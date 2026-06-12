"""Behaviour tests for platform-error handling inside durable executions.

Once the root durable promise exists, a server failure on a durable operation
(``ctx.run`` / ``ctx.rpc`` / ``ctx.sleep`` / ``ctx.promise`` / ``ctx.detached``)
must surface as a :class:`~resonate.error.PlatformError` -- a ``BaseException``
that user code cannot swallow -- and the task must be **released** (so another
worker can resume it), never fulfilled. ``execute_until_blocked_outer`` is the
boundary: after releasing it unwraps, raising the *original*
:class:`~resonate.error.ResonateError` to its caller (with the PlatformError
chained as cause), so nothing above outer ever sees a ``BaseException``.
Before the root promise exists (top-level ``resonate.run`` / ``resonate.rpc``),
failures stay plain :class:`~resonate.error.ResonateError` instances.

Like :mod:`tests.test_core`, these run against the in-process
:class:`~resonate.network.LocalNetwork` through the real Sender/Transport;
platform failures are injected by a delegating sender wrapper that fails
``promise.create`` / ``promise.settle`` on demand.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

import pytest

from resonate.codec import Codec, NoopEncryptor
from resonate.context import Context
from resonate.core import Core, identity_target_resolver
from resonate.dependencies import DependencyMap
from resonate.effects import ResonateEffects
from resonate.error import (
    HttpError,
    PlatformError,
    ResonateError,
    SerializationError,
    ServerError,
)
from resonate.network import LocalNetwork
from resonate.registry import Registry
from resonate.resonate import Resonate
from resonate.retry import Constant
from resonate.send import Sender
from resonate.transport import Transport
from resonate.types import PromiseCreateReq, PromiseSettleReq, TaskData

if TYPE_CHECKING:
    from resonate.types import PromiseRecord

# Far-future deadline, matching tests.test_core.
FAR_FUTURE = 1 << 50
TTL = 10_000


# ── Test harness ────────────────────────────────────────────────────────


class FailingSender(Sender):
    """A :class:`Sender` that can be armed to fail specific durable-op calls.

    ``fail_promise_create`` / ``fail_promise_settle`` arm the failure.
    Everything else passes through, so task lifecycle calls
    (acquire/fulfill/suspend/release) hit the real server.
    """

    def __init__(
        self, transport: Transport, error: ResonateError | None = None
    ) -> None:
        super().__init__(transport, None)
        self.error: ResonateError = (
            error if error is not None else ServerError(503, "server unavailable")
        )
        self.fail_promise_create = False
        self.fail_promise_settle = False

    async def promise_create(self, req: PromiseCreateReq) -> PromiseRecord:
        if self.fail_promise_create:
            raise self.error
        return await super().promise_create(req)

    async def promise_settle(self, req: PromiseSettleReq) -> PromiseRecord:
        if self.fail_promise_settle:
            raise self.error
        return await super().promise_settle(req)


class PlatformFixture:
    """LocalNetwork + FailingSender + Codec + Registry + Core."""

    def __init__(self) -> None:
        self.pid = "platform-test-pid"
        self.net = LocalNetwork(pid=self.pid)
        self.sender = FailingSender(Transport(self.net))
        self.codec = Codec(NoopEncryptor())
        self.reg = Registry()
        self.core = Core(
            sender=self.sender,
            codec=self.codec,
            registry=self.reg,
            resolver=identity_target_resolver,
            pid=self.pid,
            ttl=TTL,
        )

    async def create_root_task(
        self, id: str, func_name: str, *args: Any, **kwargs: Any
    ) -> tuple[int, PromiseRecord, list[PromiseRecord]]:
        """Create a root durable promise + task atomically, acquired by us."""
        param = self.codec.encode(
            TaskData(func=func_name, args=args, kwargs=kwargs, version=1)
        )
        res = await self.sender.task_create(
            self.pid,
            TTL,
            PromiseCreateReq(
                id=id,
                timeout_at=FAR_FUTURE,
                param=param,
                tags={"resonate:branch": id, "resonate:target": "any"},
            ),
        )
        decoded = self.codec.decode_promise(res.promise)
        return res.task.version, decoded, res.preload

    async def promise_get_raw(self, id: str) -> PromiseRecord:
        return await self.sender.promise_get(id)


@pytest.fixture
def fix() -> PlatformFixture:
    return PlatformFixture()


async def assert_released_root_pending(fix: PlatformFixture, root_id: str) -> None:
    """Assert the platform-error contract: root pending, task re-acquirable."""
    root = await fix.promise_get_raw(root_id)
    assert root.state == "pending"
    result = await fix.sender.task_acquire(root_id, 0, "other-pid", 1000)
    assert result.task.state == "acquired"


def leaf(ctx: Context) -> int:
    return 1


# ── 1. create_promise failure → released task, original error raised ─────


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [ServerError(503, "server unavailable"), HttpError(ConnectionError("refused"))],
)
async def test_rpc_create_failure_releases_task(
    fix: PlatformFixture, error: ResonateError
) -> None:
    async def wf(ctx: Context) -> Any:
        return await ctx.rpc("child")

    fix.reg.register("wf", wf)
    v, promise, preload = await fix.create_root_task("pe-rpc", "wf")

    fix.sender.error = error
    fix.sender.fail_promise_create = True

    # Outer unwraps the PlatformError after releasing: the caller sees the
    # *original* ResonateError, with the PlatformError chained as its cause.
    with pytest.raises(ResonateError) as excinfo:
        await fix.core.execute_until_blocked_outer("pe-rpc", v, promise, preload)

    assert excinfo.value is error
    assert isinstance(excinfo.value.__cause__, PlatformError)
    await assert_released_root_pending(fix, "pe-rpc")


@pytest.mark.asyncio
async def test_run_create_failure_releases_task(fix: PlatformFixture) -> None:
    async def wf(ctx: Context) -> int:
        return await ctx.run(leaf)

    fix.reg.register("wf", wf)
    v, promise, preload = await fix.create_root_task("pe-run", "wf")

    fix.sender.fail_promise_create = True

    with pytest.raises(ServerError) as excinfo:
        await fix.core.execute_until_blocked_outer("pe-run", v, promise, preload)

    assert isinstance(excinfo.value.__cause__, PlatformError)
    await assert_released_root_pending(fix, "pe-run")


# ── 2. user code cannot swallow a platform error ─────────────────────────


@pytest.mark.asyncio
async def test_except_exception_does_not_swallow_platform_error(
    fix: PlatformFixture,
) -> None:
    swallowed = False

    async def wf(ctx: Context) -> str:
        nonlocal swallowed
        try:
            await ctx.rpc("child")
        except Exception:
            swallowed = True
            return "swallowed"
        return "unreachable"

    fix.reg.register("wf", wf)
    v, promise, preload = await fix.create_root_task("pe-swallow", "wf")

    fix.sender.fail_promise_create = True

    with pytest.raises(ServerError):
        await fix.core.execute_until_blocked_outer("pe-swallow", v, promise, preload)

    assert not swallowed
    await assert_released_root_pending(fix, "pe-swallow")


# ── 3. settle failure on a fire-and-forget child surfaces via flush ──────


@pytest.mark.asyncio
async def test_fire_and_forget_settle_failure_releases_task(
    fix: PlatformFixture,
) -> None:
    """A platform error with *no* awaiter must not be lost: flush re-raises it."""

    async def wf(ctx: Context) -> int:
        ctx.run(leaf)  # fire-and-forget; settle of "pe-ff.1" will fail
        return 0

    fix.reg.register("wf", wf)
    v, promise, preload = await fix.create_root_task("pe-ff", "wf")

    fix.sender.fail_promise_settle = True

    with pytest.raises(ServerError) as excinfo:
        await fix.core.execute_until_blocked_outer("pe-ff", v, promise, preload)

    assert isinstance(excinfo.value.__cause__, PlatformError)
    # The child's settle never landed.
    child = await fix.promise_get_raw("pe-ff.1")
    assert child.state == "pending"
    await assert_released_root_pending(fix, "pe-ff")


@pytest.mark.asyncio
async def test_first_platform_error_wins_with_multiple_failures(
    fix: PlatformFixture,
) -> None:
    """Concurrent platform failures release the task once, on the first error.

    The flush propagates the first PlatformError it joins; remaining siblings
    are abandoned (the released task's re-delivery retries everything), so the
    execution must neither hang nor double-release.
    """

    async def wf(ctx: Context) -> int:
        ctx.run(leaf)  # "pe-multi.1" -- its settle fails
        ctx.run(leaf)  # "pe-multi.2" -- its settle fails too
        return 0

    fix.reg.register("wf", wf)
    v, promise, preload = await fix.create_root_task("pe-multi", "wf")

    fix.sender.fail_promise_settle = True

    with pytest.raises(ServerError):
        await fix.core.execute_until_blocked_outer("pe-multi", v, promise, preload)

    await assert_released_root_pending(fix, "pe-multi")


# ── 4. creation-chain integrity: no deadlock past a failed link ──────────


@pytest.mark.asyncio
async def test_chain_failure_rejects_created_so_successors_do_not_deadlock() -> None:
    sender = FailingSender(Transport(LocalNetwork(pid="chain-pid")))
    sender.fail_promise_create = True
    effects = ResonateEffects(sender, Codec(NoopEncryptor()), [])
    ctx = Context.root(
        id="r",
        origin_id="r",
        prefix_id="r",
        timeout_at=FAR_FUTURE,
        func_name="f",
        effects=effects,
        target_resolver=identity_target_resolver,
        deps=DependencyMap(),
    )

    fut1 = ctx.rpc("a")
    fut2 = ctx.rpc("b")

    async def await_fut1() -> Any:
        return await fut1

    # Link 1's failure settles its own `created` *and* propagates down the
    # chain, so link 2's id() resolves (with the error) instead of hanging.
    with pytest.raises(PlatformError):
        await asyncio.wait_for(await_fut1(), timeout=1)
    with pytest.raises(PlatformError):
        await asyncio.wait_for(fut2.id(), timeout=1)
    # The flush surfaces the platform error rather than suppressing it.
    with pytest.raises(PlatformError):
        await ctx.flush_local_work()


# ── 6. pre-durable-world failures stay regular ResonateErrors ────────────


@pytest.mark.asyncio
async def test_top_level_create_failure_stays_plain_resonate_error() -> None:
    res = Resonate()
    failing = FailingSender(res._sender.transport)
    failing.fail_promise_create = True
    res._sender = failing

    handle = res.rpc("pe-top", "somefn")
    with pytest.raises(ServerError) as excinfo:
        await handle.id()
    assert not isinstance(excinfo.value, PlatformError)
    await res.stop()


@pytest.mark.asyncio
async def test_top_level_unserializable_param_stays_plain_resonate_error() -> None:
    res = Resonate()

    @res.register
    def myfn(ctx: Context, x: Any) -> int:
        return 1

    handle = res.run("pe-ser", myfn, object())  # object() is unserializable
    with pytest.raises(SerializationError):
        await handle.id()
    await res.stop()


# ── 7. retry interaction ─────────────────────────────────────────────────


class CountingPolicy:
    """A RetryPolicy that records every consultation and never retries."""

    def __init__(self) -> None:
        self.calls = 0

    def next(self, attempt: int) -> int | None:
        self.calls += 1
        return None


@pytest.mark.asyncio
async def test_platform_error_never_fed_to_retry_policy(fix: PlatformFixture) -> None:
    policy = CountingPolicy()
    fix.core.retry_policy = policy

    async def wf(ctx: Context) -> Any:
        return await ctx.rpc("child")

    fix.reg.register("wf", wf)
    v, promise, preload = await fix.create_root_task("pe-retry", "wf")

    fix.sender.fail_promise_create = True

    with pytest.raises(ServerError):
        await fix.core.execute_until_blocked_outer("pe-retry", v, promise, preload)

    assert policy.calls == 0


@pytest.mark.asyncio
async def test_pure_leaf_user_failure_still_retries(fix: PlatformFixture) -> None:
    attempts = {"n": 0}

    def flaky(ctx: Context) -> int:
        attempts["n"] += 1
        if attempts["n"] < 3:
            msg = "flaky"
            raise ValueError(msg)
        return 42

    fix.reg.register("flaky", flaky, 1, Constant(max_retries=5, delay=0))
    v, promise, preload = await fix.create_root_task("pe-leaf", "flaky")

    status = await fix.core.execute_until_blocked_outer("pe-leaf", v, promise, preload)
    assert status == "done"
    assert attempts["n"] == 3

    got = await fix.promise_get_raw("pe-leaf")
    assert got.state == "resolved"


# ── g. background-task plumbing: _spawn logs, stop drains ────────────────


@pytest.mark.asyncio
async def test_spawn_and_stop_tolerate_base_exception_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Defense in depth: the background-task plumbing survives a BaseException.

    Outer unwraps every PlatformError, so none should reach ``_spawn`` in a
    real flow -- but if one ever leaked, ``task.exception()`` returns custom
    BaseException subclasses (asyncio only special-cases SystemExit /
    KeyboardInterrupt / CancelledError) and ``stop()``'s
    ``gather(return_exceptions=True)`` aggregates them, so the failure is
    logged and shutdown completes instead of crashing.
    """
    res = Resonate()

    async def boom() -> None:
        raise PlatformError(ServerError(503, "server unavailable"))

    with caplog.at_level(logging.ERROR, logger="resonate.resonate"):
        res._spawn(boom())
        await res.stop()

    assert any("background task failed" in record.message for record in caplog.records)
