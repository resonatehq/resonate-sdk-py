"""The top-level Resonate SDK entrypoint.

Ports Go's ``resonate.go`` and Rust's ``resonate.rs`` to idiomatic Python:

* Rust/Go's typed builder objects (``ResRunTask``/``RegisteredFunc``) collapse
  into plain methods. ``run``/``rpc`` mirror :meth:`~resonate.context.Context.run`
  / :meth:`~resonate.context.Context.rpc` (with the top-level ``id`` prepended)
  and are **synchronous** fire-and-forget triggers returning a
  :class:`~resonate.handle.ResonateHandle`; per-call options come from
  :meth:`Resonate.with_opts`, mirroring the ``with_opts`` on ``Context``.
* Go's ``sync.RWMutex``-guarded subscription map becomes a plain ``dict`` --
  asyncio is single-threaded and no mutation spans an ``await`` (see the same
  note in ``effects.py`` / ``heartbeat.py``). Each id maps to a single
  :class:`~resonate.handle.Subscription` (an :class:`asyncio.Event` + result
  slot, mirroring Go's ``subscription``); every handle to that id awaits the
  same subscription, so one settle wakes them all (the analogue of Rust's
  ``watch`` fan-out). The ``asyncio.Event`` supersedes the earlier
  ``loop.create_future()`` -- it is the idiomatic high-level primitive and needs
  no loop reference at construction.
* Goroutine / ``tokio::spawn`` fire-and-forget execution becomes
  :func:`asyncio.create_task`, with task references retained both so the loop
  does not garbage-collect them mid-flight and so :meth:`Resonate.stop` can join
  them.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import os
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Concatenate, Self, overload

from resonate import DependencyMap, now_ms
from resonate.codec import Codec, NoopEncryptor, encode_error
from resonate.context import Opts
from resonate.core import Core
from resonate.error import (
    ApplicationError,
    FunctionNotFoundError,
    ResonateError,
    ServerError,
)
from resonate.handle import PromiseResult, ResonateHandle, Subscription
from resonate.heartbeat import AsyncHeartbeat, NoopHeartbeat
from resonate.network import HttpNetwork, LocalNetwork
from resonate.options import is_url
from resonate.promises import Promises, Schedules
from resonate.registry import Registry
from resonate.send import Sender
from resonate.transport import ExecuteMsg, Transport, UnblockMsg
from resonate.types import Args, PromiseCreateReq, PromiseState, TaskData, Value

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from resonate.codec import Encryptor
    from resonate.context import Context
    from resonate.heartbeat import Heartbeat
    from resonate.network import Network
    from resonate.transport import Message

logger = logging.getLogger(__name__)

#: Default deadline applied to a top-level :meth:`Resonate.run` /
#: :meth:`Resonate.rpc` when the caller passes none. Matches the Go/Rust default.
DEFAULT_TOP_LEVEL_TIMEOUT = timedelta(days=1)

#: Default per-task lease duration.
DEFAULT_TTL = timedelta(minutes=1)

#: Ceiling on tasks executing concurrently in this process. A task holds a
#: server lease from acquire until it suspends or fulfills; with no bound, an
#: ``execute`` flood (e.g. a deep recursive workflow whose rpc children are all
#: routed back to this worker) acquires leases far faster than it can drain
#: them, so the per-task heartbeat can't keep every lease alive and they lapse
#: -- a self-amplifying 409 storm as the server re-delivers each lapsed task.
#: Bounding concurrent executions caps both the live-lease count (so the
#: heartbeat payload stays small) and the request burst against the connection
#: pool, leaving it headroom (see
#: :data:`~resonate.network._http._DEFAULT_CONN_LIMIT`). Safe because a task
#: never *blocks* on a child while holding its permit: durable functions
#: suspend (unwind, releasing the permit) and resume as a fresh ``execute``, so
#: bounding cannot deadlock a parent waiting on a child.
DEFAULT_MAX_CONCURRENT_TASKS = 64

#: Divisor applied to the lease TTL to derive the heartbeat interval. Three
#: beats per lease tolerates two missed/slow round-trips before a lapse (the
#: old ``ttl/2`` tolerated only one).
HEARTBEAT_INTERVAL_DIVISOR = 2

#: How often the background loop re-issues ``promise.register_listener`` for
#: still-pending subscriptions, defending against a dropped SSE connection.
_SUBSCRIPTION_REFRESH_SECS = 60

#: Settled promise states (anything else folds to ``"pending"``).
_SETTLED_STATES = frozenset(
    {"resolved", "rejected", "rejected_canceled", "rejected_timedout"}
)


class ResonateSchedule:
    """Handle to a created schedule. Mirrors Rust's ``ResonateSchedule``."""

    def __init__(self, name: str, schedules: Schedules) -> None:
        self.name = name
        self._schedules = schedules

    async def delete(self) -> None:
        """Delete this schedule."""
        await self._schedules.delete(self.name)


class Resonate:
    """The main entry point for the Resonate SDK.

    Owns the network, sender, codec, core execution engine, registry,
    heartbeat, and a subscription map that wakes handles when their promise
    settles via an ``unblock`` message from the server.

    Network selection precedence: ``url`` > ``network`` > ``RESONATE_URL`` env
    (with a ``RESONATE_HOST``/``RESONATE_PORT`` fallback) > an in-process
    :class:`~resonate.network.LocalNetwork`. A URL or explicit remote network
    gets an :class:`~resonate.heartbeat.AsyncHeartbeat`; local mode gets a
    :class:`~resonate.heartbeat.NoopHeartbeat`.
    """

    def __init__(
        self,
        *,
        url: str | None = None,
        network: Network | None = None,
        group: str | None = None,
        pid: str | None = None,
        ttl: timedelta | None = None,
        token: str | None = None,
        encryptor: Encryptor | None = None,
        heartbeat: Heartbeat | None = None,
        prefix: str | None = None,
        max_concurrent_tasks: int | None = None,
    ) -> None:
        self._ttl = ttl if ttl is not None else DEFAULT_TTL

        resolved_prefix = (
            prefix if prefix is not None else os.environ.get("RESONATE_PREFIX")
        )
        self._id_prefix = f"{resolved_prefix}:" if resolved_prefix else ""

        auth = token if token is not None else os.environ.get("RESONATE_TOKEN")

        self._network = _select_network(url, network, group, pid, auth)
        self._pid = self._network.pid()

        transport = Transport(self._network)
        self._codec = Codec(encryptor if encryptor is not None else NoopEncryptor())
        self._registry = Registry()
        self._sender = Sender(transport, auth)
        self._deps = DependencyMap()

        if heartbeat is not None:
            self._heartbeat = heartbeat
        elif isinstance(network, LocalNetwork):
            self._heartbeat = NoopHeartbeat()
        else:
            interval_ms = max(self._safe_ttl_ms() // HEARTBEAT_INTERVAL_DIVISOR, 1)
            self._heartbeat = AsyncHeartbeat(self._pid, interval_ms, self._sender)

        self._core = Core(
            self._sender,
            self._codec,
            self._registry,
            self._resolve_target,
            self._heartbeat,
            self._pid,
            self._safe_ttl_ms(),
            self._deps,
        )

        self.promises = Promises(self._sender, self._codec)
        self.schedules = Schedules(self._sender, self._codec)

        #: Transient options for the next run/rpc, set via :meth:`with_opts`.
        self._opts = Opts()
        #: Tail of the durable-promise-creation chain. Each run/rpc captures this
        #: as its prev-link and installs a fresh event as the new tail, so the
        #: background tasks issue ``promise.create``/``task.create`` in call order
        #: under concurrency. Mirrors ``Context._tail``.
        self._tail: asyncio.Event | None = None
        #: id -> settle-once subscription. A single subscription is shared by
        #: every handle to that id, so one settle wakes them all.
        self._subs: dict[str, Subscription] = {}
        #: Registered-function identity -> registered name, so :meth:`run` can
        #: resolve the registry name for the exact ``fn`` object it was given
        #: (falling back to ``__name__`` for an unrecorded callable).
        self._names: dict[Callable[..., Any], str] = {}
        #: Live fire-and-forget tasks (network start, promise creation, workflow
        #: execution), retained both so the event loop does not collect them
        #: mid-flight and so :meth:`stop` can join them.
        self._bg_tasks: set[asyncio.Task[Any]] = set()
        #: Set by :meth:`stop` to refuse new ``execute`` work. Without it, an
        #: in-flight job that fails and releases its task triggers the server to
        #: re-deliver an ``execute`` message, which would spawn a fresh job --
        #: an unbounded loop the join could never drain.
        self._stopping = False
        #: Caps tasks executing (holding a lease) at once. Acquired for the whole
        #: acquire→execute→suspend/fulfill span of every core execution, so the
        #: live-lease count never exceeds it regardless of how fast ``execute``
        #: messages arrive (see :data:`DEFAULT_MAX_CONCURRENT_TASKS`).
        self._execute_sema = asyncio.Semaphore(
            max_concurrent_tasks
            if max_concurrent_tasks is not None
            else DEFAULT_MAX_CONCURRENT_TASKS
        )

        # Wire push-message dispatch BEFORE starting the network so the initial
        # frames are not missed.
        transport.recv(self._on_message)

        # Start the network (fire-and-forget; HttpNetwork reconnects via SSE
        # backoff, LocalNetwork never fails here).
        self._spawn(self._network.start())
        self._refresh_handle: asyncio.Task[None] | None = asyncio.create_task(
            self._run_refresh()
        )

    # ── Constructors ────────────────────────────────────────────────────────

    @classmethod
    def local(
        cls,
        *,
        group: str | None = None,
        pid: str | None = None,
        ttl: timedelta | None = None,
        encryptor: Encryptor | None = None,
        prefix: str | None = None,
        max_concurrent_tasks: int | None = None,
    ) -> Resonate:
        """Local-only mode backed by an in-process :class:`LocalNetwork`.

        Always uses the in-process network regardless of ``RESONATE_URL`` --
        local must mean local. ``pid`` and ``group`` default to ``"default"``.
        """
        resolved_pid = pid if pid is not None else "default"
        resolved_group = group if group is not None else "default"
        return cls(
            network=LocalNetwork(pid=resolved_pid, group=resolved_group),
            group=resolved_group,
            pid=resolved_pid,
            ttl=ttl,
            encryptor=encryptor,
            max_concurrent_tasks=max_concurrent_tasks,
            prefix=prefix,
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def with_dependency(self, value: Any) -> Resonate:
        """Store a typed application dependency, shared with every context.

        Dependencies are keyed by their concrete type. Add them **before** the
        system starts processing tasks.
        """
        self._deps.insert(value)
        return self

    def with_opts(
        self,
        *,
        timeout: timedelta | None = None,
        target: str | None = None,
        version: int = 0,
    ) -> Self:
        """Set options for the **next** :meth:`run` / :meth:`rpc`, then chain.

        Mirrors :meth:`~resonate.context.Context.with_opts`: returns ``self`` so
        the call reads ``resonate.with_opts(timeout=...).run(...)``. The options
        are consumed -- and reset to defaults -- synchronously by that next call
        (``run``/``rpc`` are synchronous), so the value is captured the moment
        the call is made, not when its handle is awaited.
        """
        self._opts = Opts(timeout=timeout, target=target, version=version)
        return self

    @overload
    def register[**P, T](
        self,
        fn: Callable[Concatenate[Context, P], T],
        *,
        name: str | None = None,
    ) -> Callable[Concatenate[Context, P], T]: ...
    @overload
    def register[**P, T](
        self,
        fn: None = None,
        *,
        name: str | None = None,
    ) -> Callable[
        [Callable[Concatenate[Context, P], T]], Callable[Concatenate[Context, P], T]
    ]: ...
    def register(
        self, fn: Callable[..., Any] | None = None, *, name: str | None = None
    ) -> Any:
        """Register a durable function. Usable as a decorator.

        ``name`` defaults to the function's ``__name__``. Returns ``fn``
        unchanged (preserving its type) so it can be used bare
        (``@resonate.register``) or parameterized
        (``@resonate.register(name="...")``) -- and so the same object stays
        usable from a workflow via ``ctx.run(fn, ...)``. The identity->name map
        lets :meth:`run` recover the registered name even when it differs from
        ``__name__``.
        """
        if fn is None:
            return lambda f: self.register(f, name=name)
        reg_name = name if name is not None else getattr(fn, "__name__", "")
        if not reg_name:
            msg = "register: a name is required for an anonymous function"
            raise ApplicationError(msg)
        self._registry.register(reg_name, fn)
        self._names[fn] = reg_name
        return fn

    @overload
    def run[**P, T](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> ResonateHandle[T]: ...
    @overload
    def run[**P, T](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> ResonateHandle[T]: ...
    def run[T](
        self,
        id: str,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> ResonateHandle[T]:
        """Start a durable invocation of a locally registered function.

        The ``func``/``*args``/``**kwargs`` shape mirrors
        :meth:`~resonate.context.Context.run` exactly, with the top-level ``id``
        prepended: ``func`` is the registered **function object** (not a name --
        running purely by name is :meth:`rpc`'s job). Its registered name is
        recovered by identity, falling back to ``func.__name__`` for a callable
        that was never registered (which then fails fast as not-found).

        Returns immediately with a handle; the task is created -- and, if this
        process wins the create-and-acquire race, executed -- in the background,
        exactly like ``ctx.run``. ``await handle.result()`` for the value. The
        result type ``T`` is inferred from the function's return annotation.
        Calling ``run`` twice with the same id yields a handle to the same
        promise.

        Everything that must follow call order -- consuming the
        :meth:`with_opts` options, assigning the id, and advancing the
        durable-promise-creation chain -- happens synchronously here; only the
        network round-trips are deferred to the background task.

        A ParamSpec signature must end in ``*args: P.args, **kwargs: P.kwargs``
        with nothing following, so the function's own arguments own the keyword
        space -- per-call routing/timeout/tags options come from
        :meth:`with_opts` (``resonate.with_opts(...).run(...)``) instead.
        """
        opts = self._consume_opts()

        recorded = self._names.get(func)
        name = recorded if recorded is not None else getattr(func, "__name__", "")

        df = self._registry.get(name)
        if df is None:
            raise FunctionNotFoundError(name)

        prefixed_id = self._prefix_id(id)
        req = self._build_root_promise_create_req(
            prefixed_id,
            name,
            df.pack_args(*args, **kwargs),
            opts.version,
            opts.timeout,
            opts.target,
        )

        prev_created, created = self._advance_promise_chain()
        sub, is_new = self._subscribe(prefixed_id)

        async def _() -> None:
            """Background body for :meth:`run`: create the task in chain order, run it.

            Waits for the previous chain link before issuing ``task.create`` so the
            creates fire in call order, then releases the next link via
            ``created.set()`` (in ``finally``, so a failed create never deadlocks its
            successors). If this process won the create-and-acquire race, the
            workflow is executed in its own background task.
            """
            try:
                if prev_created is not None:
                    await prev_created.wait()
                outcome = await self._sender.task_create_or_conflict(
                    self._pid,
                    self._safe_ttl_ms(),
                    self._encode_create_req(req),
                )
            finally:
                created.set()

            if outcome != "conflict" and outcome.task.state == "acquired":
                decoded = self._codec.decode_promise(outcome.promise)
                self._spawn(
                    self._bounded_execute(
                        self._core.execute_until_blocked(
                            outcome.task.id,
                            outcome.task.version,
                            decoded,
                            outcome.preload,
                        )
                    )
                )

            if is_new:
                await self._register_and_settle(req.id, sub)

        self._spawn(_())
        return ResonateHandle(
            prefixed_id, sub, self._codec, _result_type(func), created
        )

    def rpc(self, id: str, fn: str, *args: Any, **kwargs: Any) -> ResonateHandle[Any]:
        """Dispatch a function remotely.

        Returns immediately with a handle; the durable promise is created in the
        background. The ``fn``/``*args``/``**kwargs`` shape mirrors
        :meth:`~resonate.context.Context.rpc`; ``id`` is the (required) top-level
        promise id. Does not require local registration of the target function --
        some worker subscribed to the target runs it. The result is untyped (use
        :meth:`with_opts` for routing).
        """
        opts = self._consume_opts()
        prefixed_id = self._prefix_id(id)
        req = self._build_root_promise_create_req(
            prefixed_id,
            fn,
            Args(args=args, kwargs=kwargs),
            opts.version,
            opts.timeout,
            opts.target,
        )

        prev_created, created = self._advance_promise_chain()
        sub, is_new = self._subscribe(prefixed_id)

        async def _() -> None:
            """Background body for :meth:`rpc`: create the promise in chain order."""
            try:
                if prev_created is not None:
                    await prev_created.wait()
                await self._sender.promise_create(self._encode_create_req(req))
            finally:
                created.set()

            if is_new:
                await self._register_and_settle(req.id, sub)

        self._spawn(_())
        return ResonateHandle(prefixed_id, sub, self._codec, Any, created)

    async def get(self, id: str) -> ResonateHandle[Any]:
        """Return a handle for an existing promise.

        Unlike :meth:`run` / :meth:`rpc`, this is ``async``: a lookup has nothing
        to fire-and-forget, and the listener registration is what surfaces a
        :class:`~resonate.error.ServerError` (code 404) when the promise does not
        exist. The result is untyped (``Any``): with no local function to read a
        return annotation from -- the source :meth:`run` uses, and which :meth:`rpc`
        likewise lacks -- there is nothing to infer a decode type from, so the
        settled value passes through undecoded rather than asking the caller for
        a type.
        """
        id = self._prefix_id(id)

        sub, is_new = self._subscribe(id)
        if is_new:
            try:
                record = await self._sender.promise_register_listener(
                    id, self._network.unicast()
                )
            except ResonateError:
                if self._subs.get(id) is sub and not sub.settled():
                    del self._subs[id]
                raise
            if record.state != "pending":
                self._settle_and_cleanup(id, sub, record.state, record.value)

        created = asyncio.Event()
        created.set()
        return ResonateHandle(id, sub, self._codec, Any, created)

    async def schedule(
        self,
        id: str,
        cron: str,
        func_name: str,
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        promise_timeout: timedelta | None = None,
        version: int = 0,
    ) -> ResonateSchedule:
        """Create a schedule for periodic function execution."""
        promise_timeout = (
            promise_timeout
            if promise_timeout is not None
            else DEFAULT_TOP_LEVEL_TIMEOUT
        )

        await self.schedules.create(
            id=id,
            cron=cron,
            promise_id=f"{self._id_prefix}{{{{.id}}}}.{{{{.timestamp}}}}",
            promise_timeout=_timeout_ms(promise_timeout),
            promise_param=Value.from_serializable(
                TaskData(
                    func=func_name,
                    args=args or (),
                    kwargs=kwargs or {},
                    version=version,
                )
            ),
        )
        return ResonateSchedule(id, self.schedules)

    async def stop(self) -> None:
        """Tear down background jobs and the network. Idempotent.

        Order matters. The network is stopped **first** -- this cancels its
        internal SSE/tick loop and clears its subscribers, which is what makes
        the subsequent join bounded: an in-flight job that re-dispatches work
        (e.g. a failed execution releasing its task, which the server would
        otherwise immediately re-deliver as a fresh ``execute`` message) finds
        nobody listening, so the spawn chain terminates instead of looping
        forever. With the source of new jobs closed, we then **join** every
        remaining in-flight background job -- promise creation, the run/rpc
        bodies, and workflow execution -- so no work is abandoned mid-flight.
        Draining iteratively covers a job that spawns a child after the
        snapshot. Finally the refresh loop is cancelled and the heartbeat shut
        down.

        Unlike Go -- which deliberately leaves execution goroutines untracked so
        ``Stop`` never waits on a function that blocks indefinitely -- this joins
        them; durable functions suspend (unwind) rather than block, so the join
        completes.
        """
        self._stopping = True

        handle = self._refresh_handle
        self._refresh_handle = None
        if handle is not None:
            handle.cancel()

        with contextlib.suppress(ResonateError):
            await self._network.stop()

        while self._bg_tasks:
            await asyncio.gather(*self._bg_tasks, return_exceptions=True)
            # Yield so each finished task's done-callback can discard itself from
            # the set (and any network dispatch settle) before we re-check --
            # otherwise the tight loop re-gathers the same already-done tasks.
            await asyncio.sleep(0)

        self._heartbeat.shutdown()

    # ── Accessors ─────────────────────────────────────────────────────────────

    @property
    def pid(self) -> str:
        return self._pid

    @property
    def ttl(self) -> timedelta:
        return self._ttl

    @property
    def id_prefix(self) -> str:
        return self._id_prefix

    @property
    def network(self) -> Network:
        return self._network

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _consume_opts(self) -> Opts:
        opts = self._opts
        self._opts = Opts()
        return opts

    def _prefix_id(self, id: str) -> str:
        return f"{self._id_prefix}{id}" if self._id_prefix else id

    def _resolve_target(self, target: str | None) -> str:
        """Resolve a routing target.

        Empty falls back to the network group; a URL passes through unchanged; a
        bare name runs through the network resolver. Doubles as the
        :class:`~resonate.context.TargetResolver` handed to :class:`Core`.
        """
        resolved = target if target is not None else self._network.group()
        if is_url(resolved):
            return resolved
        return self._network.target_resolver(resolved)

    def _safe_ttl_ms(self) -> int:
        ms = _timeout_ms(self._ttl)
        return ms if ms > 0 else (1 << 50)

    def _build_root_promise_create_req(
        self,
        prefixed_id: str,
        func_name: str,
        packed: Args,
        version: int,
        timeout: timedelta | None,
        target: str | None,
    ) -> PromiseCreateReq:
        """Build the root ``PromiseCreateReq`` for a top-level run or rpc.

        The param is a **plaintext** :class:`~resonate.types.Value` wrapping the
        :class:`~resonate.types.TaskData` -- symmetric with the requests
        :class:`~resonate.context.Context` builds for child promises. Encryption
        is deferred to a single boundary, :meth:`_encode_create_req`, applied
        immediately before the request is handed to the sender (mirroring
        :meth:`~resonate.effects.Effects.create_promise` for child promises and
        :func:`~resonate.promises.encode_value` for the public clients). Carries
        the SDK's ``resonate:origin/branch/parent/scope/target`` tags.
        """
        timeout = timeout if timeout is not None else DEFAULT_TOP_LEVEL_TIMEOUT

        return PromiseCreateReq(
            id=prefixed_id,
            timeout_at=now_ms() + _timeout_ms(timeout),
            param=Value.from_serializable(
                TaskData(
                    func=func_name,
                    args=packed.args,
                    kwargs=packed.kwargs,
                    version=version,
                )
            ),
            tags={
                "resonate:origin": prefixed_id,
                "resonate:branch": prefixed_id,
                "resonate:parent": prefixed_id,
                "resonate:scope": "global",
                "resonate:target": self._resolve_target(target),
            },
        )

    def _encode_create_req(self, req: PromiseCreateReq) -> PromiseCreateReq:
        """Encode a plaintext root req's ``param`` through the codec for the wire.

        The single symmetric encode boundary for top-level run/rpc, applied
        immediately before the request is handed to the sender. Mirrors
        :meth:`resonate.effects.ResonateEffects.create_promise` for child
        promises and :func:`resonate.promises.encode_value` for the public
        clients, keeping the :class:`~resonate.codec.Codec` the sole owner of
        encode/decode -- and so of encryption/decryption. The companion decode
        happens on the way back via :meth:`Codec.decode_promise`.
        """
        return PromiseCreateReq(
            id=req.id,
            timeout_at=req.timeout_at,
            param=self._codec.encode(req.param.data),
            tags=req.tags,
        )

    # ── Promise-creation chain ─────────────────────────────────────────────────

    def _advance_promise_chain(self) -> tuple[asyncio.Event | None, asyncio.Event]:
        """Advance the creation-chain tail, returning ``(prev_tail, new_tail)``.

        Each run/rpc captures the previous tail (which its background task waits
        on before creating its promise) and installs a fresh event as the new
        tail (which it sets once its own create returns). This serializes the
        ``promise.create``/``task.create`` requests in call order. Mirrors
        ``Context._advance_promise_chain``.
        """
        prev_tail = self._tail
        new_tail = asyncio.Event()
        self._tail = new_tail
        return prev_tail, new_tail

    # ── Subscription / handle path ─────────────────────────────────────────────

    def _subscribe(self, id: str) -> tuple[Subscription, bool]:
        """Return ``(sub, is_new)`` for ``id``, creating the subscription if new.

        Synchronous so :meth:`run`/:meth:`rpc` can register the subscription --
        and hand back a handle -- before their background task touches the
        network, keeping an early ``unblock`` from being dropped.
        """
        sub = self._subs.get(id)
        if sub is not None:
            return sub, False
        sub = Subscription()
        self._subs[id] = sub
        return sub, True

    async def _register_and_settle(self, id: str, sub: Subscription) -> None:
        """Register a unicast listener for ``id``; settle now if already done.

        Failures are logged, not raised -- the background caller has nobody to
        propagate to, and the periodic refresh re-registers any still-pending
        subscription. A 404 is the one exception: it means the durable promise
        no longer exists on the server (memory-storage restart, manual purge,
        ...), so retrying is pointless; settle the subscription with a
        synthetic rejection so :meth:`ResonateHandle.result` raises an
        :class:`ApplicationError` instead of hanging on the next ``wait``.
        """
        try:
            record = await self._sender.promise_register_listener(
                id, self._network.unicast()
            )
        except ServerError as exc:
            if exc.code == 404:
                self._settle_subscription_gone(id, sub, exc.message)
                return
            logger.warning("listener registration failed id=%s: %s", id, exc)
            return
        except ResonateError as exc:
            logger.warning("listener registration failed id=%s: %s", id, exc)
            return
        if record.state != "pending":
            self._settle_and_cleanup(id, sub, record.state, record.value)

    def _settle_subscription_gone(
        self, id: str, sub: Subscription, reason: str
    ) -> None:
        """Settle ``sub`` with a rejected state when the promise is gone.

        The server returned 404 for ``promise.register_listener`` -- the
        durable promise no longer exists (ephemeral-storage restart, manual
        purge, expired retention, ...). There is no settled value to deliver
        and no future ``unblock`` push will ever come, so fabricate a
        :class:`~resonate.error.ApplicationError` carrying the server's
        reason and route it through the regular ``rejected`` path. Without
        this, :meth:`ResonateHandle.result` waits on the subscription event
        for the rest of the process lifetime -- the user only learns the
        workflow is unrecoverable by hitting Ctrl+C.
        """
        err = ApplicationError(reason)
        encoded = self._codec.encode(encode_error(err))
        self._settle_and_cleanup(id, sub, "rejected", encoded)

    def _settle_and_cleanup(
        self,
        id: str,
        sub: Subscription,
        state: PromiseState,
        value: Value,
    ) -> None:
        """Settle ``sub`` and remove ``id`` from the subscription map.

        Holders of ``sub`` still observe the settled result; the deletion only
        keeps the map from growing without bound. ``Subscription.settle`` is
        idempotent, so a duplicate settle (e.g. the 60s refresh racing an
        ``unblock``) is safe.
        """
        wire: dict[str, Any] = {}
        if value.headers is not None:
            wire["headers"] = value.headers
        wire["data"] = value.data

        sub.settle(PromiseResult(state=state, value=wire))
        if self._subs.get(id) is sub:
            del self._subs[id]

    # ── Message dispatch ────────────────────────────────────────────────────────

    def _on_message(self, msg: Message) -> None:
        """Dispatch a push message: execute → core, unblock → subscription map.

        Once :meth:`stop` has begun, ``execute`` messages are dropped so the
        join can drain (see ``_stopping``); a still-pending ``unblock`` is always
        routed so a handle awaiting a last-moment settle is not stranded.
        """
        if isinstance(msg, ExecuteMsg):
            if not self._stopping:
                self._spawn(
                    self._bounded_execute(
                        self._core.on_message(msg.task_id(), msg.version())
                    )
                )
        elif isinstance(msg, UnblockMsg):
            raw = msg.promise()
            if not isinstance(raw, dict):
                return
            raw_state = raw.get("state")
            state = raw_state if raw_state in _SETTLED_STATES else "pending"

            if state == "pending":
                return
            id = raw.get("id")
            if not isinstance(id, str):
                return
            sub = self._subs.get(id)
            if sub is None:
                # No one waiting (already settled+cleaned, or a duplicate push).
                return

            self._settle_and_cleanup(
                id,
                sub,
                state,
                Value.from_wire(raw.get("value")),
            )

    # ── Background tasks ──────────────────────────────────────────────────────

    async def _bounded_execute(self, coro: Any) -> None:
        """Run a core execution coroutine under the concurrency semaphore.

        Holds a permit for the coroutine's whole lifetime -- which, for both
        :meth:`Core.on_message` and :meth:`Core.execute_until_blocked`, spans
        from acquiring the task's lease to suspending or fulfilling it -- so the
        number of leases held at once never exceeds the ceiling. When permits
        are exhausted, surplus ``execute`` jobs wait here *before* touching the
        network, so no extra lease is taken until one frees up. ``coro`` is an
        un-awaited coroutine, so the ``task.acquire`` round-trip inside it is
        deferred until the permit is in hand.
        """
        async with self._execute_sema:
            await coro

    def _spawn(self, coro: Any) -> None:
        """Fire-and-forget a coroutine, logging failures and retaining the task.

        Uses :func:`asyncio.create_task` (not ``ensure_future``): every caller
        hands in a freshly built coroutine, so the more general ``ensure_future``
        -- which also accepts an existing ``Future`` -- buys nothing here.
        """
        task = asyncio.create_task(coro)

        def _(task: asyncio.Task[Any]) -> None:
            self._bg_tasks.discard(task)
            if not task.cancelled():
                exc = task.exception()
                if exc is not None:
                    logger.error("background task failed: %s", exc)

        self._bg_tasks.add(task)
        task.add_done_callback(_)

    async def _run_refresh(self) -> None:
        """Re-register listeners for pending subscriptions every interval.

        Defends a handle against a dropped SSE connection. Returns when
        cancelled by :meth:`stop`. A 404 from the server means the promise
        no longer exists -- settle the subscription with a synthetic rejection
        (see :meth:`_register_and_settle`) so the awaiting handle is not
        stranded for the rest of the process lifetime.
        """
        with contextlib.suppress(asyncio.CancelledError):
            while True:
                await asyncio.sleep(_SUBSCRIPTION_REFRESH_SECS)
                addr = self._network.unicast()

                for id, sub in list(self._subs.items()):
                    if not sub.settled():
                        try:
                            record = await self._sender.promise_register_listener(
                                id, addr
                            )
                        except ServerError as exc:
                            if exc.code == 404:
                                self._settle_subscription_gone(id, sub, exc.message)
                                continue
                            logger.warning(
                                "subscription refresh failed id=%s: %s", id, exc
                            )
                            continue
                        except ResonateError as exc:
                            logger.warning(
                                "subscription refresh failed id=%s: %s", id, exc
                            )
                            continue

                        if record.state != "pending":
                            self._settle_and_cleanup(
                                id, sub, record.state, record.value
                            )


# ── Module-level helpers ───────────────────────────────────────────────────────


def _timeout_ms(timeout: timedelta) -> int:
    return int(timeout.total_seconds() * 1000)


def _result_type(func: Callable[..., Any]) -> Any:
    """Resolve a registered function's return type for decoding the result.

    The static ``T`` from :meth:`Resonate.run`'s ``[**P, T]`` signature is gone
    at runtime (the reason :class:`ResonateHandle` is handed an explicit type),
    so the durable function's *return annotation* drives coercion of the settled
    value. A missing or unresolved annotation falls back to ``Any``
    (passthrough), mirroring :func:`resonate.durable._resolve_annotation`.
    """
    try:
        ret = inspect.signature(func).return_annotation
    except (TypeError, ValueError):
        return Any
    if ret is inspect.Signature.empty:
        return Any
    if isinstance(ret, str):
        try:
            return eval(ret, getattr(func, "__globals__", {}))  # noqa: S307
        except (NameError, AttributeError, SyntaxError, TypeError):
            return Any
    return ret


def _select_network(
    url: str | None,
    network: Network | None,
    group: str | None,
    pid: str | None,
    auth: str | None,
) -> Network:
    if url is not None:
        return HttpNetwork(url=url, pid=pid, group=group, auth=auth)
    if network is not None:
        return network
    env_url = _resolve_env_url()
    if env_url is not None:
        return HttpNetwork(url=env_url, pid=pid, group=group, auth=auth)
    return LocalNetwork(pid=pid, group=group)


def _resolve_env_url() -> str | None:
    """Resolve a server URL from the environment.

    ``RESONATE_URL`` takes precedence; otherwise a ``RESONATE_HOST`` (with
    optional ``RESONATE_SCHEME``/``RESONATE_PORT``) is assembled. Mirrors Rust's
    URL resolution.
    """
    env_url = os.environ.get("RESONATE_URL")
    if env_url:
        return env_url
    host = os.environ.get("RESONATE_HOST")
    if not host:
        return None
    scheme = os.environ.get("RESONATE_SCHEME", "http")
    port = os.environ.get("RESONATE_PORT", "8001")
    return f"{scheme}://{host}:{port}"
