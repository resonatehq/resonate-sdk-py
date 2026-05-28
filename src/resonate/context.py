from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Concatenate, Final, Self, overload

import msgspec

from resonate import now_ms
from resonate.codec import deserialize_error
from resonate.durable import durable_function_for
from resonate.error import ApplicationError, ResonateError, SuspendedError
from resonate.info import Info
from resonate.types import PromiseCreateReq, TaskData, Value

if TYPE_CHECKING:
    import asyncio

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


class Context(msgspec.Struct, kw_only=True):
    id: str
    origin_id: str
    branch_id: str
    parent_id: str
    func_name: str
    timeout_at: int
    seq: int
    effects: Effects
    target_resolver: TargetResolver
    spawned_remote: list[str]
    spawned_locals: list[SpawnedLocal]
    deps: DependencyMap
    opts: Opts

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

    def with_options(
        self,
        timeout: timedelta | None = None,
        target: str | None = None,
        data: Any | None = None,
    ) -> Self:
        self.opts = Opts(timeout=timeout, target=target, data=data)
        return self

    def child_info(self, id: str, func_name: str, timeout_at: int) -> Info:
        assert self.timeout_at >= timeout_at, (
            "child timeout_at must be bounded by parents timeout_at"
        )
        return Info(
            id=id,
            parent_id=self.id,
            origin_id=self.origin_id,
            branch_id=id,
            timeout_at=timeout_at,
            func_name=func_name,
            tags={},
            deps=self.deps,
        )

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
    async def run[**P, T](
        self,
        fn: Callable[Concatenate[Context, P], Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T: ...
    @overload
    async def run[**P, T](
        self,
        fn: Callable[Concatenate[Context, P], T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T: ...
    @overload
    async def run[**P, T](
        self,
        fn: Callable[Concatenate[Info, P], Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T: ...
    @overload
    async def run[**P, T](
        self,
        fn: Callable[Concatenate[Info, P], T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T: ...
    @overload
    async def run[**P, T](
        self,
        fn: Callable[P, Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T: ...
    @overload
    async def run[**P, T](
        self,
        fn: Callable[P, T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T: ...
    async def run[T](self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        try:
            df = durable_function_for(fn)
            child_id = self.next_id()
            # Pack once: the same envelope backs the durable promise's param and
            # the (re-)invocation, so a recovered call rebuilds the same call.
            payload = df.pack_args(*args, **kwargs)

            req = self.local_create_req(child_id, payload, self.opts.timeout)
            record = await self.effects.create_promise(req)

            # Idempotent recovery: an already-settled promise short-circuits
            # execution (Go's ``rec.State != PromiseStatePending`` branch).
            if record.state != "pending":
                return _decode_settled(record)

            # Pending: execute the child locally. A workflow receives a child
            # Context (so it can spawn sub-tasks); a leaf receives read-only Info.
            match df.kind:
                case "workflow":
                    child = self.child(child_id, df.name, record.timeout_at)
                    env: Info | Context = child
                case "function":
                    child = None
                    env = self.child_info(child_id, df.name, record.timeout_at)

            suspended = False
            app_error: ResonateError | None = None
            value: Any = None
            try:
                value = await df.invoke(env, payload)
            except SuspendedError:
                suspended = True
            except ResonateError as exc:
                app_error = exc

            # Flush sub-tasks and collect remote todos (workflows only -- a leaf
            # holds an Info and can spawn nothing). Done unconditionally so a
            # suspended child's already-registered todos are merged up too.
            child_remote: list[str] = []
            if child is not None:
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
            await self.effects.settle_promise(
                child_id, app_error if app_error is not None else value
            )
            if app_error is not None:
                raise app_error
            return value
        finally:
            self.opts = Opts()
