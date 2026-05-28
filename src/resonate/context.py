from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Generator
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Concatenate, Final, Self, overload

import msgspec

from resonate import now_ms
from resonate.codec import deserialize_error
from resonate.durable import DurableFunction
from resonate.error import ApplicationError, ResonateError, SuspendedError
from resonate.info import Info
from resonate.types import PromiseCreateReq, TaskData, Value

if TYPE_CHECKING:
    from resonate import DependencyMap
    from resonate.effects import Effects
    from resonate.types import PromiseRecord


DEFAULT_TIMEOUT: Final[timedelta] = timedelta(days=1)

TargetResolver = Callable[[str | None], str]


class SpawnedLocal(msgspec.Struct, frozen=True, kw_only=True):
    id: str
    handle: asyncio.Task[Any]


class Opts(msgspec.Struct, frozen=True, kw_only=True):
    timeout: timedelta | None = None
    target: str | None = None
    data: Any | None = None


class ResonateFuture[T](msgspec.Struct, frozen=True, kw_only=True):
    _id: str
    _task: asyncio.Task[T]
    _created: asyncio.Event

    def __await__(self) -> Generator[Any, None, T]:
        return self._task.__await__()

    async def id(self) -> str:
        await self._created.wait()
        return self._id

    def done(self) -> bool:
        return self._task.done()


def _decode_settled(record: PromiseRecord) -> Any:
    """Map an already-settled record to its value, raising on rejection.

    Mirrors Go's ``decodeSettled`` / Rust's ``PromiseRecord::as_result``. The
    record's ``value`` has already been decoded by the codec, so a resolved
    payload is returned as-is and any rejected payload is turned back into the
    originating error.
    """
    match record.state:
        case "resolved":
            return record.value.data
        case "rejected" | "rejected_canceled" | "rejected_timedout":
            raise deserialize_error(record.value.data)
        case _:
            msg = f"future {record.id} has unexpected state {record.state!r}"
            raise ApplicationError(msg)


class Context:
    def __init__(
        self,
        id: str,
        origin_id: str,
        branch_id: str,
        parent_id: str,
        func_name: str,
        timeout_at: int,
        seq: int,
        effects: Effects,
        target_resolver: TargetResolver,
        spawned_remote: list[str],
        spawned_locals: list[SpawnedLocal],
        deps: DependencyMap,
        opts: Opts,
    ) -> None:
        self.id = id
        self.origin_id = origin_id
        self.branch_id = branch_id
        self.parent_id = parent_id

        self.func_name = func_name

        self.timeout_at = timeout_at
        self.seq = seq

        self.effects = effects
        self.target_resolver = target_resolver

        self.spawned_remote = spawned_remote
        self.spawned_locals = spawned_locals

        self.deps = deps
        self.opts = opts

        # Tail of the create-promise chain. Each ctx.run() captures this as
        # its prev-link and installs a fresh event as the new tail, so bg
        # tasks issue create_promise in ctx.run call order under concurrency.
        self._tail: asyncio.Event | None = None

    @classmethod
    def root(
        cls,
        id: str,
        timeout_at: int,
        func_name: str,
        effects: Effects,
        target_resolver: TargetResolver,
        deps: DependencyMap,
    ) -> Self:
        return cls(
            id=id,
            origin_id=id,
            branch_id=id,
            parent_id="",
            func_name=func_name,
            timeout_at=timeout_at,
            seq=0,
            effects=effects,
            target_resolver=target_resolver,
            spawned_locals=[],
            spawned_remote=[],
            deps=deps,
            opts=Opts(),
        )

    def child(self, id: str, func_name: str, timeout_at: int) -> Context:
        assert self.timeout_at >= timeout_at, (
            "child timeout_at must be bounded by parents timeout_at"
        )
        return Context(
            id=id,
            origin_id=self.origin_id,
            branch_id=id,
            parent_id=self.id,
            func_name=func_name,
            timeout_at=timeout_at,
            seq=0,
            effects=self.effects,
            target_resolver=self.target_resolver,
            spawned_locals=[],
            spawned_remote=[],
            deps=self.deps,
            opts=Opts(),
        )

    def with_opts(
        self,
        timeout: timedelta | None = None,
        target: str | None = None,
        data: Any | None = None,
    ) -> Self:
        self.opts = Opts(timeout=timeout, target=target, data=data)
        return self

    def get_dependency[T](self, type: type[T]) -> T:
        return self.deps.get(type)

    def next_id(self) -> str:
        self.seq += 1
        return f"{self.id}.{self.seq}"

    async def flush_local_work(self) -> None:
        """Wait for every eagerly spawned local task on this context to finish.

        Each spawned task merges its own remote todos into ``spawned_remote``
        before it exits, so callers wait here and then drain ``spawned_remote``
        via :meth:`take_remote_todos`. Mirrors Go's ``flushLocalWork`` (an
        unbounded ``wg.Wait()``): the structured-concurrency invariant requires
        every child's remote todos be merged before the parent decides to
        suspend, otherwise the suspend would register a partial awaited list.
        """
        tasks = self.spawned_locals
        self.spawned_locals = []
        for task in tasks:
            await task.handle

    def take_remote_todos(self) -> list[str]:
        """Drain and return all remote todos accumulated on this context.

        Mirrors Go's ``drainSpawnedRemote``.
        """
        todos = self.spawned_remote
        self.spawned_remote = []
        return todos

    def child_timeout(self, requested: timedelta | None) -> int:
        now = now_ms()
        timeout = requested if requested is not None else DEFAULT_TIMEOUT
        return min(now + int(timeout.total_seconds() * 1000), self.timeout_at)

    def info(self) -> Info:
        return Info(
            id=self.id,
            parent_id=self.parent_id,
            origin_id=self.origin_id,
            branch_id=self.branch_id,
            timeout_at=self.timeout_at,
            func_name=self.func_name,
            tags={},
            deps=self.deps,
        )

    def local_create_req(
        self, id: str, args: Any, timeout: timedelta | None
    ) -> PromiseCreateReq:
        return PromiseCreateReq(
            id=id,
            timeout_at=self.child_timeout(timeout),
            param=Value.from_serializable(args),
            tags={
                "resonate:scope": "local",
                "resonate:branch": self.branch_id,
                "resonate:parent": self.id,
                "resonate:origin": self.origin_id,
            },
        )

    def remote_create_req(
        self,
        id: str,
        func_name: str,
        args: Any,
        timeout: timedelta | None,
        target_override: str | None,
    ) -> PromiseCreateReq:
        return PromiseCreateReq(
            id=id,
            timeout_at=self.child_timeout(timeout),
            param=TaskData.into_value(func=func_name, args=args),
            tags={
                "resonate:scope": "global",
                "resonate:target": self.target_resolver(target_override),
                "resonate:branch": id,
                "resonate:parent": self.id,
                "resonate:origin": self.origin_id,
            },
        )

    def promise_create_req(
        self, id: str, timeout: timedelta | None
    ) -> PromiseCreateReq:
        return PromiseCreateReq(
            id=id,
            timeout_at=self.child_timeout(timeout),
            param=Value(),
            tags={
                "resonate:scope": "global",
                "resonate:branch": id,
                "resonate:parent": self.id,
                "resonate:origin": self.origin_id,
            },
        )

    def sleep_create_req(self, id: str, duration: timedelta) -> PromiseCreateReq:
        return PromiseCreateReq(
            id=id,
            timeout_at=self.child_timeout(duration),
            param=Value(),
            tags={
                "resonate:scope": "global",
                "resonate:branch": id,
                "resonate:parent": self.id,
                "resonate:origin": self.origin_id,
                "resonate:timer": "true",
            },
        )

    @overload
    def run[**P, T](
        self,
        fn: Callable[Concatenate[Context, P], Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> ResonateFuture[T]: ...
    @overload
    def run[**P, T](
        self,
        fn: Callable[Concatenate[Context, P], T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> ResonateFuture[T]: ...
    def run[T](
        self, fn: Callable[..., T], *args: Any, **kwargs: Any
    ) -> ResonateFuture[T]:
        # Chain promise creation: capture the previous tail
        # and install ours as the new tail.
        prev_created, created = self._advance_promise_chain()

        # Build id/req synchronously so child-id ordering matches call order
        # without relying on asyncio's task-start scheduling being FIFO.
        df = DurableFunction(fn)
        payload = df.pack_args(*args, **kwargs)
        req = self.local_create_req(
            self.next_id(),
            payload,
            self.opts.timeout,
        )
        self.opts = Opts()

        async def bg() -> T:
            try:
                if prev_created is not None:
                    await prev_created.wait()
                record = await self.effects.create_promise(req)
            finally:
                # Release the next link no matter what -- a failing bg must
                # not deadlock its successors in the chain.
                created.set()

            # Idempotent recovery: an already-settled promise short-circuits
            # execution
            if record.state != "pending":
                return _decode_settled(record)

            # Pending: execute the child locally on its own Context, which is
            # what every durable function receives as its first argument.
            child = self.child(req.id, df.name, record.timeout_at)

            suspended = False
            app_error: ResonateError | None = None
            value: Any = None
            try:
                value = await df.invoke(child, payload)
            except SuspendedError:
                suspended = True
            except ResonateError as exc:
                app_error = exc

            # Flush sub-tasks and collect remote todos. Done unconditionally so
            # a suspended child's already-registered todos are merged up too.
            await child.flush_local_work()
            child_remote = child.take_remote_todos()

            # Suspend when the child blocked on a remote dependency, or when it
            # finished but left fire-and-forget sub-work pending. Both merge the
            # child's todos up and unwind via SuspendedError -- Go reports
            # ``localResult{suspended: true}`` in exactly these two cases.
            if suspended or child_remote:
                self.spawned_remote.extend(child_remote)
                raise SuspendedError

            # Fully done: settle the child's promise (resolved or rejected) and
            # surface the result, re-raising a rejection to the caller.
            if app_error:
                assert value is None
                await self.effects.settle_promise(req.id, app_error)
                if app_error is not None:
                    raise app_error

            await self.effects.settle_promise(req.id, value)
            return value

        return ResonateFuture(
            _id=req.id,
            _task=asyncio.create_task(bg()),
            _created=created,
        )

    def rpc(self, fn: str, *args: Any, **kwargs: Any) -> ResonateFuture:
        # Chain promise creation: capture the previous tail
        # and install ours as the new tail.
        prev_created, created = self._advance_promise_chain()

        # Build id/req synchronously so child-id ordering matches call order
        # without relying on asyncio's task-start scheduling being FIFO.
        req = self.remote_create_req(
            self.next_id(),
            fn,
            {"args": list(args), "kwargs": kwargs} if args or kwargs else None,
            self.opts.timeout,
            self.opts.target,
        )
        self.opts = Opts()

        async def bg() -> Any:
            try:
                if prev_created is not None:
                    await prev_created.wait()
                record = await self.effects.create_promise(req)
            finally:
                # Release the next link no matter what -- a failing bg must
                # not deadlock its successors in the chain.
                created.set()

            # Idempotent recovery: an already-settled promise short-circuits.
            if record.state != "pending":
                return _decode_settled(record)

            # Pending remote dependency: register the child id so the parent's
            # suspend list is complete, then unwind via SuspendedError. Mirrors
            # Go's Future.Await on a futureRemote pending record (appendRemoteTodo
            # + panic(suspendSignal{})).
            self.spawned_remote.append(req.id)
            raise SuspendedError

        return ResonateFuture(
            _id=req.id,
            _task=asyncio.create_task(bg()),
            _created=created,
        )

    def _advance_promise_chain(self) -> tuple[asyncio.Event | None, asyncio.Event]:
        """Advances the creation chain tail, returning (prev_tail, new_tail)."""
        prev_tail = self._tail
        new_tail = asyncio.Event()
        self._tail = new_tail
        return prev_tail, new_tail
