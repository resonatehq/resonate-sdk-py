"""One task, then exit: the entry point for a process that was started to run it.

:class:`~resonate.resonate.Resonate` is a *worker*. It listens: it opens
sources, advertises an address, registers listeners, refreshes them on a timer,
and runs whatever the server pushes at it, for as long as the process lives.
That is the right shape when the process outlives its work.

Some processes do not. A sandbox started for one promise, a container launched
per task, a batch job handed a task id on its command line -- each is told which
task it exists for *before* it starts, and has nothing to do once that task is
finished with. Pointing the listening class at that job means asking it not to
do most of what it is: no sources, no refresh loop, no client surface, and a
listener registration for an address nothing will ever deliver to.

:class:`Handler` is that job as its own class. It holds a registry, a codec, a
dependency map and one network -- and no loop, no source, no address, and no
``run``/``rpc``: a handler does not *create* work, it executes work it was
handed. :meth:`Handler.run` acquires one task, drives it to a terminal state,
tears the network down and returns how it ended::

    handler = Handler(network=StdioConnection())
    handler.register(greet)
    status = await handler.run(task_id, version)

This is the same arrangement :mod:`resonate_aws` makes for Lambda -- a passive
container of registrations that mints a :class:`~resonate.core.Core` per task --
with the transport left open, because what starts a per-task process differs
(an HTTP push there, a worker with a tunnel here) while the shape does not.

Knowing when to exit
====================

:meth:`run` returns ``"done"`` when the promise settled and ``"suspended"``
when the function unwound to await a child. Both mean *this process* is
finished, and the difference cannot be recovered from outside: a suspended
function has settled nothing, so the promise reads ``pending`` either way.

Lingering past that is not harmless. A worker that starts one process per task
generally refuses to start a second for a promise already running -- otherwise
two clients share one tunnel, both convinced they hold the lease -- so a
process still up when the resumption arrives stalls the promise until its lease
lapses. Return, then exit.
"""

from __future__ import annotations

import contextlib
import logging
import os
import uuid
from typing import TYPE_CHECKING, Any, Concatenate, overload

from resonate.codec import Codec, NoopEncryptor
from resonate.connections import LocalConnection
from resonate.core import Core
from resonate.dependencies import DependencyMap
from resonate.error import ApplicationError, ResonateError
from resonate.heartbeat import AsyncHeartbeat, NoopHeartbeat
from resonate.observability import logging_observer
from resonate.registry import Registry
from resonate.resonate import (
    DEFAULT_TTL,
    HEARTBEAT_INTERVAL_DIVISOR,
    default_resolve_target,
)
from resonate.retry import Exponential
from resonate.send import Sender, TaskAcquireResult
from resonate.timing import now_ms, sleep
from resonate.transport import Transport

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Mapping
    from datetime import timedelta

    from resonate_base.connections import Network

    from resonate.codec import Encryptor
    from resonate.context import Context
    from resonate.heartbeat import Heartbeat
    from resonate.observability import Observer
    from resonate.retry import RetryPolicy
    from resonate.timing import Clock, Sleeper
    from resonate.types import Status

logger = logging.getLogger(__name__)

#: Environment variables a worker sets on a process it starts for one task.
#: They are the wire between the two, and they are read rather than pushed
#: because a process started *for* a task needs no message to learn its name.
TASK_ID_ENV = "RESONATE_TASK_ID"
TASK_VERSION_ENV = "RESONATE_TASK_VERSION"


class Handler:
    """A registry, a network, and one task at a time.

    Construct one, register functions and dependencies on it, then await
    :meth:`run`. Everything reusable is built here; the network is started and
    stopped by :meth:`run`, so the object is inert until then and inert again
    after.

    Unlike :class:`~resonate.resonate.Resonate` this opens no source,
    advertises no address, registers no listener and spawns no background work
    beyond the heartbeat that holds the task's own lease.
    """

    def __init__(
        self,
        *,
        network: Network,
        group: str | None = None,
        pid: str | None = None,
        ttl: timedelta | None = None,
        token: str | None = None,
        encryptor: Encryptor | None = None,
        retry_policy: RetryPolicy | None = None,
        resolve_target: Callable[[str], str] | None = None,
        heartbeat: Heartbeat | None = None,
        env: Mapping[str, str] | None = None,
        clock: Clock = now_ms,
        sleeper: Sleeper = sleep,
        observer: Observer = logging_observer,
    ) -> None:
        """Build the wiring that outlives any one task.

        ``network`` is required and there is no ``url`` shorthand. A handler
        exists because something else decided how this process talks to the
        server -- a tunnel on its stdio, a url in its environment -- and
        inventing a connection from ambient configuration is exactly the
        guesswork it is here to avoid. Pass
        ``HttpConnection(url=...)`` for the ordinary case.

        ``group`` is *not* something this process listens on -- nothing is
        listening. It is the routing target a child inherits when a
        ``ctx.run``/``ctx.rpc`` names none, resolved through ``resolve_target``
        exactly as :class:`~resonate.resonate.Resonate` resolves it. A target
        that is already an address passes through untouched.

        ``ttl`` is the lease this handler holds its task under, renewed by a
        :class:`~resonate.heartbeat.AsyncHeartbeat` for as long as :meth:`run`
        is running -- a per-task *process* can beat its lease, unlike a Lambda,
        because it has an event loop for the whole task. Pass ``heartbeat=`` to
        override; a :class:`~resonate.connections.LocalConnection` gets a
        :class:`~resonate.heartbeat.NoopHeartbeat`, mirroring ``Resonate``.

        ``env`` is the mapping :meth:`run_from_env` reads, taken as a parameter
        so that path is testable with a dict.
        """
        environ: Mapping[str, str] = env if env is not None else os.environ
        self._env = environ
        self._group = group if group is not None else "default"
        self._pid = pid if pid is not None else uuid.uuid4().hex
        self._auth = token if token is not None else environ.get("RESONATE_TOKEN")
        self._resolver = (
            resolve_target if resolve_target is not None else default_resolve_target
        )

        resolved_ttl = ttl if ttl is not None else DEFAULT_TTL
        ttl_ms = int(resolved_ttl.total_seconds() * 1000)
        safe_ttl = ttl_ms if ttl_ms > 0 else 1

        # The SDK-wide default for a pure-leaf failure with no per-call
        # (``ctx.options``) or per-function (:meth:`register`) override --
        # the same ladder :class:`~resonate.resonate.Resonate` applies, so a
        # function retries identically under either.
        resolved_retry_policy = (
            retry_policy
            if retry_policy is not None
            else Exponential(delay=1, max_delay=(1 << 63) - 1, factor=2, max_retries=30)
        )

        self._network = network
        self._codec = Codec(encryptor if encryptor is not None else NoopEncryptor())
        self._registry = Registry()
        self._deps = DependencyMap()
        sender = Sender(
            Transport(network, observer=observer), self._auth, observer=observer
        )

        if heartbeat is not None:
            self._heartbeat: Heartbeat = heartbeat
        elif isinstance(network, LocalConnection):
            self._heartbeat = NoopHeartbeat()
        else:
            self._heartbeat = AsyncHeartbeat(
                self._pid,
                max(safe_ttl // HEARTBEAT_INTERVAL_DIVISOR, 1),
                sender,
                sleeper,
            )

        self._core = Core(
            # One ``Sender`` satisfies both narrow ports structurally.
            sender=sender,
            fencing=sender,
            codec=self._codec,
            registry=self._registry,
            resolver=self._resolve_target,
            heartbeat=self._heartbeat,
            pid=self._pid,
            ttl=safe_ttl,
            deps=self._deps,
            retry_policy=resolved_retry_policy,
            clock=clock,
            sleeper=sleeper,
            observer=observer,
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def with_dependency(self, value: Any) -> Handler:
        """Store a typed application dependency, shared with every context.

        Keyed by concrete type and read back inside a function through
        ``ctx.get_dependency(SomeType)``. Add them before :meth:`run`.
        """
        self._deps.insert(value)
        return self

    @overload
    def register[**P, T](
        self,
        fn: Callable[Concatenate[Context, P], T],
        *,
        name: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> Callable[Concatenate[Context, P], T]: ...
    @overload
    def register[**P, T](
        self,
        fn: None = None,
        *,
        name: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> Callable[
        [Callable[Concatenate[Context, P], T]], Callable[Concatenate[Context, P], T]
    ]: ...
    def register(
        self,
        fn: Callable[Concatenate[Context, ...], Any] | None = None,
        *,
        name: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> Any:
        """Register a durable function. Usable as a decorator.

        Identical semantics to :meth:`resonate.resonate.Resonate.register`:
        ``name`` defaults to ``fn.__name__``, ``version`` to ``1``, and the
        same name may be registered at several versions. Returns ``fn``
        unchanged, so it works bare (``@handler.register``) or parameterized
        (``@handler.register(name="...", version=2)``).
        """
        if fn is None:
            return lambda f: self.register(
                f, name=name, version=version, retry_policy=retry_policy
            )
        reg_name = name if name is not None else getattr(fn, "__name__", "")
        if not reg_name:
            msg = "register: a name is required for an anonymous function"
            raise ApplicationError(msg)
        self._registry.register(reg_name, fn, version, retry_policy)
        return fn

    async def run(self, task_id: str, version: int = 0) -> Status:
        """Claim one task by name, run it, then tear the network down.

        For a task nobody holds yet: this acquires it under the handler's own
        ``pid`` first. Hand in a task someone else already claimed with
        :meth:`run_acquired` instead -- acquiring a second time is not a no-op,
        it is a second claim on a lease that is already out.

        Returns ``"done"`` (the promise settled) or ``"suspended"`` (the
        function unwound to await a child). Either way this process is
        finished: exit.
        """
        return await self._drive(self._core.on_message(task_id, version))

    async def run_acquired(self, acquired: TaskAcquireResult) -> Status:
        """Run a task whose lease someone else already took.

        The claim carries everything the acquire would have returned -- the
        task record, the root promise, the preloaded siblings -- so this skips
        straight to executing it. Use it when the thing that decided this
        process should run the task also claimed it: a worker handing work to a
        sandbox it starts, a dispatcher farming claims to a pool. It saves a
        round trip, and more importantly it closes the window in which a task
        is dispatched but not yet held.

        A claim that arrives as JSON -- over a tunnel, in an environment
        variable, in a request body -- becomes one of these through
        :func:`~resonate.send.parse_task_acquire`::

            await handler.run_acquired(parse_task_acquire(json.loads(blob)))

        **Run under the claimer's pid, or do not beat the lease at all.**
        ``task.heartbeat`` renews only the tasks held by the pid that sends it,
        so a handler with a pid of its own renews nothing here and the task is
        redelivered mid-run. Either ``Handler(pid=<the claimer's pid>)``, or
        ``Handler(heartbeat=NoopHeartbeat())`` and let the claimer keep beating
        it. A mismatch is warned about rather than refused -- it is only wrong
        when nothing else is beating, which this cannot see.
        """
        self._warn_if_the_lease_is_not_ours(acquired)
        return await self._drive(self._core.on_acquired(acquired))

    async def run_from_env(self) -> Status:
        """Run the task this process was started for, named by its environment.

        Reads :data:`TASK_ID_ENV` and :data:`TASK_VERSION_ENV` -- what a worker
        that starts a process per task sets on it. Missing or unparseable, that
        is a deployment fault rather than a task failure: it is raised here,
        before the network is touched and before any lease is taken.
        """
        task_id = self._env.get(TASK_ID_ENV)
        if not task_id:
            msg = (
                f"{TASK_ID_ENV} is not set: a handler runs the task its process "
                f"was started for. Pass one to run() instead when it comes from "
                f"somewhere else."
            )
            raise ApplicationError(msg)
        raw = self._env.get(TASK_VERSION_ENV, "0")
        try:
            version = int(raw)
        except ValueError as exc:
            msg = f"{TASK_VERSION_ENV} is not an integer: {raw!r}"
            raise ApplicationError(msg) from exc
        return await self.run(task_id, version)

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _drive(self, coro: Coroutine[Any, Any, Status]) -> Status:
        """Run one task with the network open around exactly that.

        The network's whole lifetime is this call -- started before anything is
        sent and stopped in a ``finally``, so a handler that raises still
        leaves nothing open. That also makes it one-shot against a connection
        that cannot be restarted, which
        :class:`~resonate.connections.StdioConnection` deliberately is: the
        process's stdio is opened once.

        Failures propagate. :class:`~resonate.core.Core` has already released
        the task by then, so the server redelivers it; what is left for the
        caller is the exit code, which for a per-task process is the only
        channel it has to say the run went badly.
        """
        try:
            await self._network.start()
        except BaseException:
            # Nothing will await it now, and an un-awaited coroutine is a
            # warning on a path that already has a real error to report.
            coro.close()
            raise
        try:
            return await coro
        finally:
            self._heartbeat.shutdown()
            # The task is over either way; a network that will not close
            # cleanly must not turn a settled promise into a raised handler.
            with contextlib.suppress(ResonateError):
                await self._network.stop()

    def _warn_if_the_lease_is_not_ours(self, acquired: TaskAcquireResult) -> None:
        """Warn when this handler's heartbeat cannot renew the claim it was handed.

        Not an error: the claimer beating its own lease is a legitimate
        arrangement, and from here the two are indistinguishable. What is worth
        refusing to do silently is beating under a pid the server will ignore.
        """
        pid = acquired.task.pid
        if (
            pid is None
            or pid == self._pid
            or isinstance(self._heartbeat, NoopHeartbeat)
        ):
            return
        logger.warning(
            "handler: task %s is leased to pid %s but this handler beats under "
            "%s, which renews nothing -- construct it with pid=%s, or with "
            "heartbeat=NoopHeartbeat() if the claimer is beating the lease",
            acquired.task.id,
            pid,
            self._pid,
            pid,
        )

    def _resolve_target(self, target: str | None) -> str:
        """Resolve a child's routing target to a delivery address.

        The same three rules :meth:`resonate.resonate.Resonate._resolve_target`
        applies, so a ``ctx.rpc`` resolves identically whether the function is
        running under a worker or under a handler: no target means this
        handler's ``group``, an address passes through, a bare group name goes
        through ``resolve_target``.
        """
        resolved = target if target is not None else self._group
        if "://" in resolved:
            return resolved
        return self._resolver(resolved)
