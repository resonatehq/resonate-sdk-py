"""Orchestrates the full lifecycle of one task. Mirrors Go's ``core.go``.

A :class:`Core` acquires (via :meth:`Core.on_message`) or skip-acquires (via
:meth:`Core.execute_until_blocked`) a task, executes the registered function
inside a suspend/panic boundary, then fulfills on done, suspends on remote
work, or releases on error.

Where Go returns ``(Status, error)`` from every method, this port follows the
repo-wide convention (see codec.py / sender.py): methods **raise** a
:class:`~resonate.error.ResonateError` on failure and return the success
:data:`~resonate.types.Status` (``"done"`` / ``"suspended"``) otherwise. The
``StatusErr`` arm of Go's enum is therefore expressed as a raised exception, and
the release-on-error ``defer`` becomes an ``except`` that releases the task then
re-raises.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal

import msgspec

from resonate import DependencyMap
from resonate.codec import encode_error
from resonate.context import Context
from resonate.effects import Effects, ResonateEffects
from resonate.error import (
    ApplicationError,
    DecodingError,
    FunctionNotFoundError,
    ResonateError,
    SuspendedError,
)
from resonate.heartbeat import NoopHeartbeat
from resonate.send import Redirect
from resonate.types import (
    PromiseRegisterCallbackData,
    PromiseSettleReq,
    TaskData,
    Value,
)

if TYPE_CHECKING:
    from resonate.codec import Codec
    from resonate.context import TargetResolver
    from resonate.heartbeat import Heartbeat
    from resonate.registry import Registry
    from resonate.send import Sender
    from resonate.types import PromiseRecord, Status

logger = logging.getLogger(__name__)

#: The target state for a ``promise.settle`` request, mirroring Go's
#: ``SettleState``. Folded into the field type as a ``Literal`` per the
#: established convention; identical to
#: :class:`~resonate.types.PromiseSettleReq`'s ``state``.
SettleState = Literal["resolved", "rejected", "rejected_canceled"]


def identity_target_resolver(override: str | None) -> str:
    """Return the override unchanged, or the empty string when there is none.

    Mirrors Go's ``IdentityTargetResolver`` -- the fallback resolver used when a
    :class:`Core` is built without one.
    """
    return override if override is not None else ""


# ═══════════════════════════════════════════════════════════════
#  execOutcome -- what the inner reports back to the lifecycle loop
# ═══════════════════════════════════════════════════════════════


class _ExecFulfilled(msgspec.Struct, frozen=True, kw_only=True):
    """The workflow finished; the task should be fulfilled with these args.

    ``value`` is already codec-encoded, so the caller has a single uniform
    fulfill path. Mirrors Go's ``execOutcome{kind: execFulfill}``.
    """

    state: SettleState
    value: Value


class _ExecSuspended(msgspec.Struct, frozen=True, kw_only=True):
    """The workflow has remote dependencies; suspend on these awaiteds.

    Mirrors Go's ``execOutcome{kind: execSuspend}``.
    """

    todos: list[str]


#: Outcome reported by :meth:`Core._execute_until_blocked_inner`. Mirrors Go's
#: ``execOutcome`` discriminated by ``execOutcomeKind``.
_ExecOutcome = _ExecFulfilled | _ExecSuspended


class Core:
    """Orchestrates acquire/execute/fulfill-suspend-release for one task.

    Mirrors Go's ``Core``. ``resolver`` may be ``None`` -- it falls back to
    :func:`identity_target_resolver`. ``heartbeat`` may be ``None`` -- it falls
    back to :class:`~resonate.heartbeat.NoopHeartbeat`.

    ``deps`` has no Go analog: Go injects dependencies through the host
    ``context.Context`` (``Context.Value``), whereas the Python SDK threads an
    explicit :class:`~resonate.DependencyMap` into the root context.
    """

    def __init__(
        self,
        sender: Sender | None,
        codec: Codec,
        registry: Registry,
        resolver: TargetResolver | None,
        heartbeat: Heartbeat | None,
        pid: str,
        ttl: int,
        deps: DependencyMap | None = None,
    ) -> None:
        if resolver is None:
            resolver = identity_target_resolver
        if heartbeat is None:
            heartbeat = NoopHeartbeat()
        if deps is None:
            deps = DependencyMap()

        self.sender = sender
        self.codec = codec
        self.registry = registry
        self.resolver = resolver
        self.heartbeat = heartbeat
        self.pid = pid
        self.ttl = ttl
        self.deps = deps

    # ═══════════════════════════════════════════════════════════════
    #  Path 1: on_message -- acquire then execute
    # ═══════════════════════════════════════════════════════════════

    async def on_message(self, task_id: str, version: int) -> Status:
        """Handle an execute push message.

        Acquires the task, decodes the root promise, and runs
        :meth:`execute_until_blocked`. Mirrors Go's ``OnMessage``.
        """
        assert self.sender, "sender must be set"
        res = await self.sender.task_acquire(task_id, version, self.pid, self.ttl)
        logger.debug("core: task acquired task_id=%s", task_id)

        promise = self.codec.decode_promise(res.promise)
        return await self.execute_until_blocked(
            task_id, res.task.version, promise, res.preload
        )

    # ═══════════════════════════════════════════════════════════════
    #  Path 2: execute_until_blocked -- task already acquired
    # ═══════════════════════════════════════════════════════════════

    async def execute_until_blocked(
        self,
        task_id: str,
        task_version: int,
        promise: PromiseRecord,
        preload: list[PromiseRecord],
    ) -> Status:
        """Run an already-acquired task to completion or suspension.

        The caller owns the acquire step and must pass a ``promise`` whose
        ``param``/``value`` have been run through
        :meth:`~resonate.codec.Codec.decode_promise`.

        Owns the task lifecycle: builds :class:`~resonate.effects.Effects`,
        drives the redirect loop, fulfills or suspends on the inner's outcome,
        and releases on error. Mirrors Go's ``ExecuteUntilBlocked``.
        """
        self.heartbeat.start(task_id, task_version)
        assert self.sender, "sender must be set"
        try:
            try:
                logger.debug(
                    "core: starting execution task_id=%s promise_id=%s",
                    task_id,
                    promise.id,
                )
                current_preload = preload
                while True:
                    effects = ResonateEffects(self.sender, self.codec, current_preload)
                    outcome = await self._execute_until_blocked_inner(promise, effects)

                    match outcome:
                        case _ExecFulfilled(state=state, value=value):
                            await self.sender.task_fulfill(
                                task_id,
                                task_version,
                                PromiseSettleReq(
                                    id=promise.id, state=state, value=value
                                ),
                            )

                            logger.debug(
                                "core: task fulfilled task_id=%s promise_id=%s",
                                task_id,
                                promise.id,
                            )
                            return "done"

                        case _ExecSuspended(todos=todos):
                            logger.debug(
                                "core: attempting to suspend task task_id=%s "
                                "remote_deps=%d",
                                task_id,
                                len(todos),
                            )
                            sr = await self.sender.task_suspend(
                                task_id,
                                task_version,
                                [
                                    PromiseRegisterCallbackData(
                                        awaited=awaited, awaiter=task_id
                                    )
                                    for awaited in todos
                                ],
                            )

                            if not isinstance(sr, Redirect):
                                logger.debug("core: task suspended task_id=%s", task_id)
                                return sr
                            logger.debug(
                                "core: suspend returned redirect, re-executing task "
                                "task_id=%s preload=%d",
                                task_id,
                                len(sr.preload),
                            )
                            current_preload = sr.preload
            except ResonateError:
                logger.exception(
                    "core: execution failed, releasing task task_id=%s promise_id=%s",
                    task_id,
                    promise.id,
                )
                try:
                    await self.sender.task_release(task_id, task_version)
                except ResonateError:
                    logger.exception(
                        "core: failed to release task after error task_id=%s",
                        task_id,
                    )
                raise
        finally:
            self.heartbeat.stop(task_id)

    async def _execute_until_blocked_inner(
        self, promise: PromiseRecord, effects: Effects
    ) -> _ExecOutcome:
        """Run the workflow body once and report the outcome.

        Does not touch task lifecycle APIs (fulfill/suspend/release) -- the
        caller owns those. Encodes return values through the codec so the caller
        has a single, uniform fulfill path. Mirrors Go's
        ``executeUntilBlockedInner``.
        """
        # 1. Decode TaskData from the (already-decoded) promise param.
        try:
            task_data = msgspec.convert(promise.param.data, TaskData)
        except (TypeError, ValueError, msgspec.MsgspecError) as exc:
            msg = f"invalid task data: {exc}"
            raise DecodingError(msg) from exc

        # 2. Look up the function in the registry by (name, version). The version
        #    was persisted in TaskData at create time, so this resolves the same
        #    implementation on every replay regardless of later registrations.
        df = self.registry.get(task_data.func, task_data.version)
        if df is None:
            raise FunctionNotFoundError(task_data.func, task_data.version)

        # 3. SHORT-CIRCUIT: if the root promise is already settled, report a
        #    fulfill outcome without invoking the function.
        if promise.state != "pending":
            logger.info(
                "core: promise already settled, fulfilling task without execution "
                "promise_id=%s state=%s",
                promise.id,
                promise.state,
            )
            return _ExecFulfilled(
                state="rejected"
                if promise.state == "rejected_timedout"
                else promise.state,
                value=self.codec.encode(promise.value.data),
            )

        # 4. EXECUTE the workflow.
        root_ctx = Context.root(
            id=promise.id,
            timeout_at=promise.timeout_at,
            func_name=task_data.func,
            effects=effects,
            target_resolver=self.resolver,
            deps=self.deps,
        )

        suspended: bool = False
        run_err: ApplicationError | None = None
        try:
            res = await df.invoke(root_ctx, task_data)
        except SuspendedError:
            suspended = True
        except ApplicationError as exc:
            # A Resonate-typed error (e.g. ``ApplicationError`` deliberately
            # raised by the user, or one surfaced by ``ctx.rpc`` when an
            # awaited child rejected) crosses the boundary verbatim.
            run_err = exc
        except Exception as exc:
            # User code raised a plain Python exception (``ValueError``,
            # ``RuntimeError``, a domain-specific subclass, ...). Python has no
            # Go-style "returned error vs panic" split -- exceptions are how
            # user functions report failure -- so wrap into an
            # :class:`ApplicationError` and settle the promise ``rejected``
            # with the original message. Awaiters then see an
            # ``ApplicationError`` via :func:`~resonate.codec.deserialize_error`,
            # matching the convention documented on ``examples/saga``.
            #
            # ``BaseException`` subclasses (``SystemExit``,
            # ``KeyboardInterrupt``, ``asyncio.CancelledError`` on 3.8+...) are
            # *not* caught here -- they propagate out so the task is released
            # and the runtime can shut down.
            logger.debug(
                "core: user function raised %s in task=%s: %s",
                type(exc).__name__,
                root_ctx.id,
                exc,
                exc_info=True,
            )
            run_err = ApplicationError(str(exc))

        # Flush local work and collect remote todos.
        await root_ctx.flush_local_work()
        todos = root_ctx.take_remote_todos()

        # 5. FINALIZE: fulfill when no remote todos remain and the function did
        #    not request suspension.
        if not suspended and not todos:
            if run_err is not None:
                state: SettleState = "rejected"
                encoded = self.codec.encode(encode_error(run_err))
            else:
                state = "resolved"
                encoded = self.codec.encode(res)
            return _ExecFulfilled(state=state, value=encoded)

        # If the function returned done but there are pending todos, treat as
        # suspended (structured-concurrency rule; covers the fire-and-forget
        # child case).
        if not suspended and todos:
            logger.warning(
                "core: workflow returned done with pending remote todos -- either a "
                "fire-and-forget child suspended or the function swallowed an error "
                "func=%s id=%s todos=%s",
                task_data.func,
                promise.id,
                todos,
            )

        return _ExecSuspended(todos=todos)
