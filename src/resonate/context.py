from __future__ import annotations

import asyncio
import contextlib
import inspect
from collections.abc import Awaitable, Callable, Generator
from datetime import timedelta
from hashlib import blake2b
from typing import TYPE_CHECKING, Any, Concatenate, Self, TypeGuard, overload

import msgspec

from resonate import now_ms
from resonate.codec import deserialize_error
from resonate.error import ApplicationError, SuspendedError
from resonate.types import Args, Info, PromiseCreateReq, Status, TaskData, Value

if TYPE_CHECKING:
    from resonate.dependencies import DependencyMap
    from resonate.effects import Effects
    from resonate.types import PromiseRecord


DEFAULT_TIMEOUT = timedelta(days=1)

TargetResolver = Callable[[str | None], str]


class SpawnedLocal(msgspec.Struct, frozen=True, kw_only=True):
    id: str
    handle: asyncio.Task[Any]


class Opts(msgspec.Struct, frozen=True, kw_only=True):
    timeout: timedelta | None = None
    target: str | None = None
    version: int = 1


class ResonateFuture[T](msgspec.Struct, frozen=True, kw_only=True):
    _id: str
    _task: asyncio.Task[T]
    _created: asyncio.Event

    def __await__(self) -> Generator[Any, None, T]:
        return self._task.__await__()

    async def id(self) -> str:
        await self._created.wait()
        return self._id


def _hash_id(s: str) -> str:
    return blake2b(s.encode(), digest_size=8).hexdigest()


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

        # Background tasks spawned by rpc/sleep/promise (see
        # ``_spawn_remote_await``). Tracked so ``flush_local_work`` can join
        # them, which both retrieves their (almost always ``SuspendedError``)
        # result -- avoiding asyncio's "exception never retrieved" warning for
        # a future the workflow never awaited -- and makes ``spawned_remote``
        # population deterministic rather than dependent on incidental
        # event-loop scheduling.
        self.spawned_remote_tasks: list[asyncio.Task[Any]] = []

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

    def _child(self, id: str, func_name: str, timeout_at: int) -> Context:
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

    def _consume_opts(self) -> Opts:
        opts = self.opts
        self.opts = Opts()
        return opts

    def with_opts(
        self,
        *,
        timeout: timedelta | None = None,
        target: str | None = None,
        version: int = 1,
    ) -> Self:
        self.opts = Opts(
            timeout=timeout,
            target=target,
            version=version,
        )
        return self

    def get_dependency[T](self, type: type[T]) -> T:
        return self.deps.get(type)

    def _next_id(self) -> str:
        self.seq += 1
        return f"{self.id}.{self.seq}"

    async def flush_local_work(self) -> None:
        """Wait for every eagerly spawned task on this context to finish.

        Joins two task groups before the caller drains ``spawned_remote`` via
        :meth:`take_remote_todos`:

        * ``spawned_locals`` -- the ``ctx.run`` children. Each merges its own
          remote todos into ``spawned_remote`` before it exits.
        * ``spawned_remote_tasks`` -- the ``rpc``/``sleep``/``promise``
          background bodies (see :meth:`_spawn_remote_await`). Each appends its
          child id to ``spawned_remote`` and unwinds via ``SuspendedError`` when
          its record is pending. Joining them here makes that append
          deterministic for futures the workflow created but never awaited
          (e.g. it suspended on a sibling first), instead of relying on the
          event loop happening to run the task before ``take_remote_todos``.

        Mirrors Go's ``flushLocalWork`` (an unbounded ``wg.Wait()``): the
        structured-concurrency invariant requires every child's remote todos be
        merged before the parent decides to suspend, otherwise the suspend would
        register a partial awaited list.

        Per-task ``SuspendedError`` / ``ResonateError`` are swallowed: by the
        time a task ends, it has either merged its todos into our
        ``spawned_remote`` (suspended) or settled its own promise (errored).
        Either way, the error belongs to whoever holds the future. This
        matches Go's ``wg.Wait`` + channel-based result delivery and Rust's
        ``Outcome::{Done, Suspended}`` handling in ``flush_local_work``.
        """
        locals_ = self.spawned_locals
        remotes = self.spawned_remote_tasks
        self.spawned_locals = []
        self.spawned_remote_tasks = []
        for task in locals_:
            with contextlib.suppress(SuspendedError, ApplicationError):
                await task.handle
        for remote in remotes:
            with contextlib.suppress(SuspendedError, ApplicationError):
                await remote

    def take_remote_todos(self) -> list[str]:
        """Drain and return all remote todos accumulated on this context.

        Mirrors Go's ``drainSpawnedRemote``.
        """
        todos = self.spawned_remote
        self.spawned_remote = []
        return todos

    def _child_timeout(self, requested: timedelta | None) -> int:
        now = now_ms()
        timeout = requested if requested is not None else DEFAULT_TIMEOUT
        return min(now + int(timeout.total_seconds() * 1000), self.timeout_at)

    async def invoke[**P, T](
        self,
        fn: Callable[Concatenate[Context, P], T | Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:

        def _(value: T | Awaitable[T]) -> TypeGuard[Awaitable[T]]:
            return inspect.isawaitable(value)

        result = fn(self, *args, **kwargs)
        if _(result):
            result = await result
        assert not inspect.isawaitable(result)
        return result

    def info(self) -> Info:
        return Info(
            id=self.id,
            parent_id=self.parent_id,
            origin_id=self.origin_id,
            branch_id=self.branch_id,
            timeout_at=self.timeout_at,
            func_name=self.func_name,
            tags={},
        )

    def _global_req(
        self,
        id: str,
        timeout: timedelta | None,
        *,
        data: TaskData | None = None,
        target: str | None = None,
        timer: bool = False,
    ) -> PromiseCreateReq:
        """Build a global-scope promise request.

        Backs every global-scope creator -- remote dispatch (:meth:`rpc`,
        :meth:`detached`) as well as bare promises (:meth:`sleep`,
        :meth:`promise`). ``data`` carries a ``TaskData`` payload for function
        dispatch (empty otherwise); ``target`` adds the routing tag for remote
        dispatch; ``timer`` adds the ``resonate:timer`` tag that distinguishes a
        sleep from a bare promise. Tags are inserted in a fixed order so the
        serialized form matches across SDKs.
        """
        tags = {"resonate:scope": "global"}
        if target is not None:
            tags["resonate:target"] = target
        tags["resonate:branch"] = id
        tags["resonate:parent"] = self.id
        tags["resonate:origin"] = self.origin_id
        if timer:
            tags["resonate:timer"] = "true"
        return PromiseCreateReq(
            id=id,
            timeout_at=self._child_timeout(timeout),
            param=Value(data=data),
            tags=tags,
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
    def run[**P, T](
        self,
        fn: Callable[Concatenate[Context, P], T | Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> ResonateFuture[T]:
        opts = self._consume_opts()
        # Chain promise creation: capture the previous tail
        # and install ours as the new tail.
        prev_created, created = self._advance_promise_chain()

        # Build id/req synchronously so child-id ordering matches call order
        # without relying on asyncio's task-start scheduling being FIFO.
        payload = Args(args=args, kwargs=kwargs)

        req = PromiseCreateReq(
            id=self._next_id(),
            timeout_at=self._child_timeout(opts.timeout),
            param=Value(data=payload),
            tags={
                "resonate:scope": "local",
                "resonate:branch": self.branch_id,
                "resonate:parent": self.id,
                "resonate:origin": self.origin_id,
            },
        )

        async def bg() -> T:
            record = await self._create_promise_in_chain(req, prev_created, created)

            # Idempotent recovery: an already-settled promise short-circuits
            # execution
            if record.state != "pending":
                return _decode_settled(record)

            # Pending: execute the child locally on its own Context, which is
            # what every durable function receives as its first argument.
            child = self._child(
                req.id, getattr(fn, "__name__", "unknown"), record.timeout_at
            )

            outcome: Status
            value: Any | ApplicationError
            try:
                value = await child.invoke(fn, *payload.args, **payload.kwargs)
                outcome = "done"
            except SuspendedError:
                outcome = "suspended"
            except ApplicationError as exc:
                # A Resonate-typed error (e.g. ``ApplicationError`` deliberately
                # raised by the child, or one surfaced by ``ctx.rpc`` when an
                # awaited grandchild rejected) crosses the boundary verbatim.
                value = exc
                outcome = "error"
            except Exception as exc:
                # Plain Python exception from the local child -- treat as a
                # rejection (mirrors :mod:`resonate.core`'s convention on the
                # root task). Without this, the asyncio task would fail,
                # ``flush_local_work``'s ``contextlib.suppress`` would not catch
                # it (only ``SuspendedError`` / ``ResonateError`` are
                # suppressed), and the local promise would never settle.
                value = ApplicationError(str(exc))
                outcome = "error"

            # Always drain the child's sub-work before deciding: a suspended
            # child has already pushed its own todos, and a "done" child may
            # still have spawned background work that registered todos.
            await child.flush_local_work()
            child_remote = child.take_remote_todos()

            # Structured-concurrency: check suspension states first
            match (outcome, len(child_remote) > 0):
                # Matches if outcome is "suspended" OR if child_remote is not empty
                case ("suspended", _) | (_, True):
                    self.spawned_remote.extend(child_remote)
                    raise SuspendedError

                case ("error", _):
                    assert isinstance(value, ApplicationError)
                    await self.effects.settle_promise(req.id, value)
                    raise value

                case _:
                    assert not isinstance(value, ApplicationError)
                    assert not inspect.isawaitable(value)
                    await self.effects.settle_promise(req.id, value)
                    return value

        task = asyncio.create_task(bg())
        # Register for structured-concurrency flush: a fire-and-forget child
        # that suspends or errors in the background must be joined here so its
        # todos / settled state are observed before the parent decides what to
        # do. Mirrors Go's append to ``spawnedLocals`` + ``wg.Add(1)`` and
        # Rust's ``tasks.push(SpawnedLocal { id, handle })``.
        self.spawned_locals.append(SpawnedLocal(id=req.id, handle=task))
        return ResonateFuture(
            _id=req.id,
            _task=task,
            _created=created,
        )

    def rpc(self, fn: str, *args: Any, **kwargs: Any) -> ResonateFuture:
        opts = self._consume_opts()

        prev_created, created = self._advance_promise_chain()

        req = self._global_req(
            self._next_id(),
            opts.timeout,
            data=TaskData(func=fn, args=args, kwargs=kwargs, version=opts.version),
            target=self.target_resolver(opts.target),
        )

        return self._remote_future(req, prev_created, created)

    def sleep(self, duration: timedelta) -> ResonateFuture[None]:
        _ = self._consume_opts()

        prev_created, created = self._advance_promise_chain()

        req = self._global_req(self._next_id(), duration, timer=True)

        return self._remote_future(req, prev_created, created)

    def promise(self, timeout: timedelta | None = None) -> ResonateFuture[Any]:
        _ = self._consume_opts()

        prev_created, created = self._advance_promise_chain()

        req = self._global_req(self._next_id(), timeout)

        return self._remote_future(req, prev_created, created)

    def detached(self, fn: str, *args: Any, **kwargs: Any) -> ResonateFuture[str]:
        opts = self._consume_opts()
        prev_created, created = self._advance_promise_chain()

        req = self._global_req(
            f"{self.origin_id}.{_hash_id(self._next_id())}",
            opts.timeout,
            data=TaskData(func=fn, args=args, kwargs=kwargs, version=opts.version),
            target=self.target_resolver(opts.target),
        )

        async def bg() -> str:
            """Background body for :meth:`detached`.

            Defers ``create_promise`` through the creation chain like the other
            entrypoints, but -- unlike :meth:`_await_remote` -- never registers a
            remote todo and never suspends: the detached child is fire-and-forget,
            so once the durable promise has been created (idempotent on replay) its
            id is simply returned. Mirrors Go's ``Detached``, which ignores the
            record returned by ``CreatePromise``. ``created.set()`` lives in
            ``finally`` so a failing create never deadlocks the chain's successors.
            """
            await self._create_promise_in_chain(req, prev_created, created)
            return req.id

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

    async def _create_promise_in_chain(
        self,
        req: PromiseCreateReq,
        prev_created: asyncio.Event | None,
        created: asyncio.Event,
    ) -> PromiseRecord:
        """Create ``req``'s promise in creation-chain order.

        Waits on the previous chain link (so create_promise calls issue in
        ctx.run call order under concurrency), creates the promise, then
        releases the next link. ``created.set()`` lives in ``finally`` so a
        failing create never deadlocks the chain's successors.
        """
        try:
            if prev_created is not None:
                await prev_created.wait()
            return await self.effects.create_promise(req)
        finally:
            created.set()

    def _remote_future(
        self,
        req: PromiseCreateReq,
        prev_created: asyncio.Event | None,
        created: asyncio.Event,
    ) -> ResonateFuture:
        """Wrap a remote ``req`` in a future backed by a deferred-create task.

        Shared by :meth:`rpc`, :meth:`sleep`, and :meth:`promise`: the future's
        body is the :meth:`_await_remote` task spawned via
        :meth:`_spawn_remote_await`.
        """
        return ResonateFuture(
            _id=req.id,
            _task=self._spawn_remote_await(req, prev_created, created),
            _created=created,
        )

    def _spawn_remote_await(
        self,
        req: PromiseCreateReq,
        prev_created: asyncio.Event | None,
        created: asyncio.Event,
    ) -> asyncio.Task[Any]:
        """Spawn the :meth:`_await_remote` task for rpc/sleep/promise.

        Registers the task in ``spawned_remote_tasks`` so
        :meth:`flush_local_work` joins it. The join retrieves the task's result
        -- a never-awaited suspended task would otherwise trip asyncio's
        "exception never retrieved" warning -- and guarantees its
        ``spawned_remote`` append has happened before the caller drains todos.
        Awaiting the returned future still raises ``SuspendedError`` as before;
        an asyncio task delivers its result to every awaiter, so the flush join
        and the future await do not conflict.
        """
        task = asyncio.create_task(self._await_remote(req, prev_created, created))
        self.spawned_remote_tasks.append(task)
        return task

    async def _await_remote(
        self,
        req: PromiseCreateReq,
        prev_created: asyncio.Event | None,
        created: asyncio.Event,
    ) -> Any:
        """Background body shared by :meth:`rpc`, :meth:`sleep`, and :meth:`promise`.

        Defers ``create_promise`` through the creation chain, short-circuits an
        already-settled record (idempotent recovery), otherwise registers the
        child id as a remote todo and unwinds via ``SuspendedError``. Mirrors
        Go's remote ``Future.Await`` on a pending record (``appendRemoteTodo`` +
        ``panic(suspendSignal{})``).

        Concurrency: ``spawned_remote.append`` needs no lock. Unlike Go's
        goroutines -- which run in parallel and so guard the slice with
        ``c.mu`` -- these are asyncio tasks on a single event loop, and the
        append has no ``await`` between read and write, so it cannot interleave
        with a peer task. ``created.set()`` lives in ``finally`` so a failing
        create never deadlocks the successors waiting on this chain link.
        """
        record = await self._create_promise_in_chain(req, prev_created, created)

        if record.state != "pending":
            return _decode_settled(record)

        self.spawned_remote.append(req.id)
        raise SuspendedError
