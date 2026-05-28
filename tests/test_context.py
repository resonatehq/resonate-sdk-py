"""Behaviour tests for :mod:`resonate.context` -- focused on ``Context.run``.

``context.rs`` has its own ``#[cfg(test)]`` module, but the Python ``run`` was
ported from Go's ``context.go`` (``Run`` + ``executeLocal`` + ``Future.Await``
fused into the inline async-await form), so these tests mirror Go's
``context_test.go`` ``Run`` cases adapted to that model: ``run`` returns the
value directly, raises on rejection, and raises
:class:`~resonate.error.SuspendedError` (instead of Go's ``suspendSignal`` panic)
when a dependency is still pending.

The harness builds a root :class:`~resonate.context.Context` over a real
:class:`~resonate.network.LocalNetwork` (as :mod:`tests.test_durable` does), so
``create_promise``/``settle_promise`` exercise the actual durability boundary.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, patch

import msgspec
import pytest

from resonate import DependencyMap, now_ms
from resonate.codec import Codec, NoopEncryptor, encode_error
from resonate.context import Context, Opts
from resonate.effects import Effects
from resonate.error import ApplicationError, SuspendedError
from resonate.network import LocalNetwork
from resonate.send import Sender
from resonate.transport import Transport
from resonate.types import PromiseRecord, Value

if TYPE_CHECKING:
    from collections.abc import Iterator

I64_MAX = 2**63 - 1


# =============================================================================
# Harness
# =============================================================================


def _codec() -> Codec:
    return Codec(NoopEncryptor())


def _root(
    preload: list[PromiseRecord] | None = None, *, timeout_at: int = I64_MAX
) -> Context:
    """Build a root ``Context`` over a fresh ``LocalNetwork``."""
    sender = Sender(Transport(LocalNetwork()), None)
    effects = Effects(sender, _codec(), preload or [])
    return Context.root(
        id="root",
        timeout_at=timeout_at,
        func_name="root",
        effects=effects,
        target_resolver=lambda target: target or "",
        deps=DependencyMap(),
    )


def _resolved(id: str, value: Any) -> PromiseRecord:
    """Build a pre-settled *resolved* record, wire-encoded for the preload cache."""
    return PromiseRecord(
        id=id,
        state="resolved",
        timeout_at=I64_MAX,
        param=Value(),
        value=_codec().encode(value),
        tags={},
        created_at=0,
        settled_at=1,
    )


def _rejected(id: str, message: str) -> PromiseRecord:
    """Build a pre-settled *rejected* record carrying an encoded error payload."""
    return PromiseRecord(
        id=id,
        state="rejected",
        timeout_at=I64_MAX,
        param=Value(),
        value=_codec().encode(encode_error(ApplicationError(message))),
        tags={},
        created_at=0,
        settled_at=1,
    )


# =============================================================================
# Durable functions under test (leaves and workflows)
# =============================================================================


async def double(ctx: Context, x: int) -> int:
    """Double ``x`` -- a leaf that ignores the injected Context."""
    return x * 2


async def add(ctx: Context, a: int, b: int) -> int:
    """Add ``a`` and ``b``, ignoring the injected Context."""
    return a + b


async def beat(ctx: Context) -> str:
    """Return a constant -- a ctx-only leaf (``pack_args`` -> ``None``)."""
    return "ok"


async def failing(ctx: Context) -> int:
    msg = "denied"
    raise ApplicationError(msg)


class Point(msgspec.Struct, frozen=True):
    x: int
    y: int


async def sum_point(ctx: Context, p: Point) -> int:
    return p.x + p.y


async def parent_workflow(ctx: Context, x: int) -> int:
    """Run nested leaves on this workflow's own child context."""
    a = await ctx.run(double, x)
    b = await ctx.run(double, a)
    return a + b


async def blocks_on_remote(ctx: Context) -> int:
    """Mimic ``ctx.rpc``/``sleep``/``promise`` on a *pending* promise.

    Those register the awaited promise id and unwind via ``SuspendedError``;
    until they exist, this stands in so ``run``'s suspension path is exercised.
    """
    ctx.spawned_remote.append("remote-dep")
    raise SuspendedError


async def fire_and_forget(ctx: Context) -> int:
    """Complete normally but leave a pending remote child registered."""
    ctx.spawned_remote.append("ff-dep")
    return 7


# =============================================================================
# Context plumbing (next_id, child linkage, child_timeout)
# =============================================================================


def test_next_id_sequential() -> None:
    ctx = _root()
    assert ctx.next_id() == "root.1"
    assert ctx.next_id() == "root.2"
    assert ctx.next_id() == "root.3"


def test_child_parent_is_current_id() -> None:
    # Regression: child.parent_id must be the *current* id (Go ``c.id`` / Rust
    # ``self.id``), not the parent's own parent_id.
    ctx = _root()
    child = ctx.child("root.1", "fn", I64_MAX)
    assert child.parent_id == "root"
    assert child.origin_id == "root"
    assert child.branch_id == "root.1"


def test_child_timeout_caps_to_parent() -> None:
    cap = now_ms() + 1_000
    ctx = _root(timeout_at=cap)
    # A requested deadline beyond the parent's is capped to the parent.
    assert ctx.child_timeout(timedelta(days=1)) == cap
    # A nearer deadline is honoured.
    assert ctx.child_timeout(timedelta(milliseconds=500)) <= cap


# =============================================================================
# run: leaves -- create, execute, settle, return
# =============================================================================


@pytest.mark.asyncio
async def test_run_leaf_returns_and_settles_resolved() -> None:
    ctx = _root()
    assert await ctx.run(double, 21) == 42
    record = ctx.effects.cache["root.1"]
    assert record.state == "resolved"
    assert record.value.data == 42


@pytest.mark.asyncio
async def test_run_ctx_only_function() -> None:
    ctx = _root()
    assert await ctx.run(beat) == "ok"
    assert ctx.effects.cache["root.1"].state == "resolved"


@pytest.mark.asyncio
async def test_run_coerces_struct_arg() -> None:
    ctx = _root()
    assert await ctx.run(sum_point, Point(x=3, y=4)) == 7


@pytest.mark.asyncio
async def test_run_sequential_child_ids() -> None:
    ctx = _root()
    assert await ctx.run(double, 2) == 4  # root.1
    assert await ctx.run(double, 3) == 6  # root.2
    assert ctx.effects.cache["root.1"].value.data == 4
    assert ctx.effects.cache["root.2"].value.data == 6


@pytest.mark.asyncio
async def test_run_rejects_non_callable() -> None:
    not_callable: Any = 42
    ctx = _root()
    with pytest.raises(ApplicationError):
        await ctx.run(not_callable)


# =============================================================================
# run: error handling
# =============================================================================


@pytest.mark.asyncio
async def test_run_function_error_propagates_and_settles_rejected() -> None:
    ctx = _root()
    with pytest.raises(ApplicationError, match="denied"):
        await ctx.run(failing)
    assert ctx.effects.cache["root.1"].state == "rejected"


# =============================================================================
# run: idempotent recovery (a pre-settled promise skips execution)
# =============================================================================


@pytest.mark.asyncio
async def test_run_presettled_resolved_skips_execution() -> None:
    calls = 0

    async def counted(ctx: Context, x: int) -> int:
        nonlocal calls
        calls += 1
        return x

    ctx = _root([_resolved("root.1", 99)])
    assert await ctx.run(counted, 1) == 99  # value from the pre-settled record
    assert calls == 0  # the function body never ran


@pytest.mark.asyncio
async def test_run_presettled_rejected_raises_without_execution() -> None:
    calls = 0

    async def counted(ctx: Context, x: int) -> int:
        nonlocal calls
        calls += 1
        return x

    ctx = _root([_rejected("root.1", "stored failure")])
    with pytest.raises(ApplicationError, match="stored failure"):
        await ctx.run(counted, 1)
    assert calls == 0


@pytest.mark.asyncio
async def test_run_replay_does_not_reinvoke() -> None:
    calls = 0

    async def counted(ctx: Context, x: int) -> int:
        nonlocal calls
        calls += 1
        return x

    ctx = _root()
    assert await ctx.run(counted, 7) == 7
    assert calls == 1
    # Replay: the same child id is recreated, but the cached settled record
    # short-circuits, so the body does not run again.
    ctx.seq = 0
    assert await ctx.run(counted, 7) == 7
    assert calls == 1


# =============================================================================
# run: nested workflow (structured concurrency, happy path)
# =============================================================================


@pytest.mark.asyncio
async def test_workflow_runs_nested_leaves() -> None:
    ctx = _root()
    assert await ctx.run(parent_workflow, 5) == 30  # a=10, b=20
    assert ctx.spawned_remote == []  # nothing pending -> no suspension
    assert ctx.effects.cache["root.1"].state == "resolved"
    # Nested children live under the workflow's own id.
    assert ctx.effects.cache["root.1.1"].value.data == 10
    assert ctx.effects.cache["root.1.2"].value.data == 20


# =============================================================================
# run: suspension (child blocks on a remote dependency)
# =============================================================================


@pytest.mark.asyncio
async def test_run_suspends_when_child_blocks_on_remote() -> None:
    ctx = _root()
    with pytest.raises(SuspendedError):
        await ctx.run(blocks_on_remote)
    # The child's todo is merged up so the task can suspend on it...
    assert ctx.spawned_remote == ["remote-dep"]
    # ...and the child promise is left pending (not settled).
    assert ctx.effects.cache["root.1"].state == "pending"


@pytest.mark.asyncio
async def test_run_suspends_when_child_completes_with_pending_remote() -> None:
    # Go reports ``localResult{suspended: true}`` when the function finished but
    # left remote todos -- the value is dropped in favour of suspension.
    ctx = _root()
    with pytest.raises(SuspendedError):
        await ctx.run(fire_and_forget)
    assert ctx.spawned_remote == ["ff-dep"]
    assert ctx.effects.cache["root.1"].state == "pending"


# =============================================================================
# run: structured-concurrency propagation under suspension
#
# When a deeply nested child blocks on a remote dependency, its todo must
# travel through every intermediate ``ctx.run`` up to the root, and every
# promise on the suspension path must be left pending (not settled). This
# mirrors Go's ``executeLocal`` recursion and Rust's ``RunTask::into_future``
# propagation: at each level, ``take_remote_todos`` drains the child and
# extends the parent before re-raising ``SuspendedError``.
# =============================================================================


async def deep_inner(ctx: Context) -> int:
    """Leaf stand-in for rpc/sleep -- registers a remote dep and suspends."""
    ctx.spawned_remote.append("deep-dep")
    raise SuspendedError


async def deep_middle(ctx: Context) -> int:
    return await ctx.run(deep_inner)


async def deep_top(ctx: Context) -> int:
    return await ctx.run(deep_middle)


async def completes_then_suspends(ctx: Context) -> int:
    """Settle one child, then suspend on a sibling."""
    a = await ctx.run(double, 21)
    b = await ctx.run(blocks_on_remote)
    return a + b


async def multi_remote(ctx: Context) -> int:
    """Register multiple remote deps before suspending -- a multi-todo leaf."""
    ctx.spawned_remote.extend(["dep-a", "dep-b", "dep-c"])
    raise SuspendedError


async def parent_with_fire_and_forget(ctx: Context) -> int:
    """Spawn a child that suspends, then return without awaiting it.

    The unawaited bg task only runs once ``flush_local_work`` joins it; if
    ``ctx.run`` didn't register the task in ``spawned_locals``, its
    ``spawned_remote.append`` would happen after the parent had already
    decided to settle, and the todo would be lost.
    """
    _ = ctx.run(blocks_on_remote)
    return 99


@pytest.mark.asyncio
async def test_run_suspension_propagates_through_intermediate_workflow() -> None:
    # A grandchild that suspends must bubble its todo up through the parent
    # workflow into the root's ``spawned_remote``.
    ctx = _root()
    with pytest.raises(SuspendedError):
        await ctx.run(deep_middle)
    assert ctx.spawned_remote == ["deep-dep"]
    # Both promises along the suspension path are left pending.
    assert ctx.effects.cache["root.1"].state == "pending"  # middle
    assert ctx.effects.cache["root.1.1"].state == "pending"  # inner


@pytest.mark.asyncio
async def test_run_suspension_propagates_through_three_levels() -> None:
    # Same property at one more level of nesting: todos travel arbitrarily deep.
    ctx = _root()
    with pytest.raises(SuspendedError):
        await ctx.run(deep_top)
    assert ctx.spawned_remote == ["deep-dep"]
    assert ctx.effects.cache["root.1"].state == "pending"  # top
    assert ctx.effects.cache["root.1.1"].state == "pending"  # middle
    assert ctx.effects.cache["root.1.1.1"].state == "pending"  # inner


@pytest.mark.asyncio
async def test_run_completed_sibling_settles_but_parent_still_suspends() -> None:
    # The first child runs to completion and is settled, but a later child
    # suspends -- the parent must surface the suspension and stay pending,
    # even though one of its sub-promises is already resolved.
    ctx = _root()
    with pytest.raises(SuspendedError):
        await ctx.run(completes_then_suspends)
    assert ctx.spawned_remote == ["remote-dep"]
    # First child got fully settled with its computed value.
    assert ctx.effects.cache["root.1.1"].state == "resolved"
    assert ctx.effects.cache["root.1.1"].value.data == 42
    # Second child remains pending -- its body raised SuspendedError.
    assert ctx.effects.cache["root.1.2"].state == "pending"
    # Parent workflow itself stays pending: ``outcome == "suspended"`` skips
    # the settle_promise call at the bottom of ``run``.
    assert ctx.effects.cache["root.1"].state == "pending"


@pytest.mark.asyncio
async def test_run_merges_multiple_todos_from_single_child() -> None:
    # ``take_remote_todos`` drains the full list, not just the first entry,
    # so a child registering N pending deps surfaces all N at the parent.
    ctx = _root()
    with pytest.raises(SuspendedError):
        await ctx.run(multi_remote)
    assert ctx.spawned_remote == ["dep-a", "dep-b", "dep-c"]


@pytest.mark.asyncio
async def test_run_fire_and_forget_child_suspension_propagates() -> None:
    # Parent spawns a child via ctx.run but doesn't await it. The child's bg
    # task only runs once flush_local_work joins it; that join must pull the
    # child's todo into the parent's spawned_remote and force the parent to
    # suspend even though the body returned 99.
    #
    # This is the exact scenario Go's executeLocal calls out with
    # ``localResult{suspended: true}`` when the function returned but left
    # remote todos (context.go:362-371) -- it depends on spawned_locals
    # being populated so flush has something to wait for.
    ctx = _root()
    with pytest.raises(SuspendedError):
        await ctx.run(parent_with_fire_and_forget)
    assert ctx.spawned_remote == ["remote-dep"]
    # Parent's value (99) was dropped in favour of suspension; both promises
    # along the suspension path are left pending.
    assert ctx.effects.cache["root.1"].state == "pending"
    assert ctx.effects.cache["root.1.1"].state == "pending"


# =============================================================================
# run: structured concurrency -- unawaited child still blocks parent settlement
#
# ``ctx.run`` appends the child to ``spawned_locals``, and the parent's
# ``flush_local_work`` joins it before deciding to settle. The invariant is
# that a fire-and-forget child must not be orphaned: the parent's
# ``settle_promise`` cannot run until every child registered on the parent's
# context has reached a terminal state (settled or merged remote todos up).
# Mirrors Go's ``wg.Wait()`` over ``spawnedLocals`` in ``executeLocal``.
# =============================================================================


async def quiet_child(ctx: Context) -> int:
    # Yield a few times so a broken impl (one that doesn't join spawned_locals)
    # would let the parent settle ahead of the child.
    for _ in range(5):
        await asyncio.sleep(0)
    return 42


async def parent_does_not_await_child(ctx: Context) -> int:
    _ = ctx.run(quiet_child)
    return 1


@pytest.mark.asyncio
async def test_run_unawaited_child_settles_before_parent() -> None:
    # Parent spawns the child via ``ctx.run`` but never awaits the returned
    # future. ``flush_local_work`` must still join the child's bg task before
    # the parent calls ``settle_promise``, so the child's settlement is
    # observed strictly before the parent's.
    ctx = _root()
    settle_order: list[str] = []
    original = ctx.effects.settle_promise

    async def recorder(id: str, value: Any) -> Any:
        settle_order.append(id)
        return await original(id, value)

    with patch.object(
        ctx.effects, "settle_promise", new=AsyncMock(side_effect=recorder)
    ):
        assert await ctx.run(parent_does_not_await_child) == 1

    # Child settled first; parent second. If ``flush_local_work`` were a no-op
    # the parent's bg would settle ahead of the still-pending child task and
    # this order would flip.
    assert settle_order == ["root.1.1", "root.1"]
    assert ctx.effects.cache["root.1.1"].state == "resolved"
    assert ctx.effects.cache["root.1.1"].value.data == 42
    assert ctx.effects.cache["root.1"].state == "resolved"
    assert ctx.effects.cache["root.1"].value.data == 1


# =============================================================================
# run: blocks until the durable promise has been created
#
# ``Context.run`` returns a Task immediately, but the underlying
# ``wait_for_signal(durable_promise_created)`` guard means that Task must not
# be observable as done -- and the function body must not run -- until
# ``effects.create_promise`` has either returned a record or raised. These
# tests pin execution at that exact boundary by gating ``create_promise`` on
# an asyncio.Event.
# =============================================================================


@contextmanager
def _gated_create_promise(ctx: Context, gate: asyncio.Event) -> Iterator[asyncio.Event]:
    entered = asyncio.Event()
    original = ctx.effects.create_promise

    async def gated(req: Any) -> Any:
        entered.set()
        await gate.wait()
        return await original(req)

    mock = AsyncMock(side_effect=gated)
    with patch.object(ctx.effects, "create_promise", new=mock):
        yield entered


@pytest.mark.asyncio
async def test_run_task_pending_while_create_promise_blocked() -> None:
    # The Task returned by ``ctx.run`` must not be ``done()`` while
    # ``create_promise`` has not yet resolved -- the ``wait_for_signal`` guard
    # parks the outer task at ``event.wait()`` until then.
    ctx = _root()
    gate = asyncio.Event()

    with _gated_create_promise(ctx, gate) as entered:
        task = ctx.run(double, 21)

        # Wait until we *know* the inner coroutine is parked inside create_promise.
        await entered.wait()

        # And the durable record is not in the cache yet.
        assert "root.1" not in ctx.effects.cache

        # Releasing the gate lets create_promise return, the event fire, and
        # the body run through to settlement.
        gate.set()
        assert await task == 42
        assert ctx.effects.cache["root.1"].state == "resolved"


@pytest.mark.asyncio
async def test_run_body_does_not_execute_before_promise_created() -> None:
    # The user-supplied function body must not be invoked until
    # ``create_promise`` has returned. If it ran earlier, a still-pending
    # promise creation could be raced by side effects in the body.
    body_ran = asyncio.Event()

    async def observed(ctx: Context, x: int) -> int:
        body_ran.set()
        return x

    ctx = _root()
    gate = asyncio.Event()

    with _gated_create_promise(ctx, gate) as entered:
        task = ctx.run(observed, 7)

        await entered.wait()
        # We're now parked inside create_promise -- the body must not have run.
        assert not body_ran.is_set()

        gate.set()
        assert await task == 7
        assert body_ran.is_set()


@pytest.mark.asyncio
async def test_run_durable_promise_visible_in_cache_before_task_resolves() -> None:
    # Once ``await ctx.run(...)`` yields a value to its caller, the durable
    # promise record is necessarily in ``effects.cache``. This is the
    # "happens-before" guarantee the rest of the SDK leans on (e.g. replay
    # picking up the cached record).
    ctx = _root()
    result = await ctx.run(double, 5)
    assert result == 10
    assert "root.1" in ctx.effects.cache
    # And it was created *before* it was settled -- the cached record reflects
    # the post-settle state by the time we observe the result.
    assert ctx.effects.cache["root.1"].state == "resolved"


@pytest.mark.asyncio
async def test_run_releases_event_when_create_promise_raises() -> None:
    # If ``create_promise`` raises, the body re-raises through the
    # ``except Exception:`` branch and the event is still set -- otherwise the
    # outer ``wait_for_signal`` would hang forever waiting on a signal that
    # never comes. The Task must surface the original error promptly.
    ctx = _root()

    failing = AsyncMock(side_effect=RuntimeError("network down"))
    with patch.object(ctx.effects, "create_promise", new=failing):
        task = ctx.run(double, 1)

        with pytest.raises(RuntimeError, match="network down"):
            await task

    failing.assert_awaited_once()
    # Nothing was cached, because creation never succeeded.
    assert "root.1" not in ctx.effects.cache


@pytest.mark.asyncio
async def test_run_does_not_settle_before_create_returns() -> None:
    # ``settle_promise`` must come strictly *after* ``create_promise``. The
    # only way to reach the settle path is through the post-event branch in
    # ``_``, so while creation is gated, no settle call can sneak through.
    ctx = _root()
    gate = asyncio.Event()

    # ``wraps=`` lets the mock both record calls *and* delegate to the real
    # ``settle_promise`` so settlement still hits the cache as usual.
    settle_mock = AsyncMock(wraps=ctx.effects.settle_promise)
    with (
        patch.object(ctx.effects, "settle_promise", new=settle_mock),
        _gated_create_promise(ctx, gate) as entered,
    ):
        task = ctx.run(double, 3)

        await entered.wait()
        settle_mock.assert_not_awaited()

        gate.set()
        assert await task == 6
        settle_mock.assert_awaited_once_with("root.1", 6)


# =============================================================================
# run: with_options(timeout=...) and per-call opts reset
# =============================================================================


@pytest.mark.asyncio
async def test_run_with_options_timeout_sets_child_deadline() -> None:
    ctx = _root()
    before = now_ms()
    assert await ctx.with_opts(timeout=timedelta(seconds=30)).run(double, 5) == 10
    after = now_ms()
    record = ctx.effects.cache["root.1"]
    assert before + 30_000 <= record.timeout_at <= after + 30_000


@pytest.mark.asyncio
async def test_run_with_options_timeout_capped_to_parent() -> None:
    cap = now_ms() + 5_000
    ctx = _root(timeout_at=cap)
    # A year-long timeout still cannot outlive the parent's deadline.
    await ctx.with_opts(timeout=timedelta(days=365)).run(double, 1)
    assert ctx.effects.cache["root.1"].timeout_at == cap


@pytest.mark.asyncio
async def test_run_consumes_options_after_one_call() -> None:
    ctx = _root()
    await ctx.with_opts(timeout=timedelta(seconds=30)).run(double, 1)  # root.1
    short = ctx.effects.cache["root.1"].timeout_at
    # The next run carries no options -> the 24h default, well past the 30s one.
    await ctx.run(double, 1)  # root.2
    assert ctx.effects.cache["root.2"].timeout_at > short
    assert ctx.opts == Opts()


@pytest.mark.asyncio
async def test_run_resets_options_even_on_error() -> None:
    ctx = _root()
    ctx.with_opts(timeout=timedelta(seconds=5))
    with pytest.raises(ApplicationError):
        await ctx.run(failing)
    assert ctx.opts == Opts()


# =============================================================================
# run: promise creation order under concurrency
#
# ``Context.run`` returns its ``ResonateFuture`` immediately and the inner bg
# task is what awaits ``create_promise``. The chain on ``Context._tail`` is the
# guarantee that those bg tasks issue ``create_promise`` in call order, no
# matter how asyncio schedules them.
# =============================================================================


@pytest.mark.asyncio
async def test_run_promise_creation_order_under_concurrency() -> None:
    # Mirror of ``test_rpc_promise_creation_order_under_concurrency`` for
    # ``ctx.run``: even with bg tasks spawned concurrently, the chain
    # serializes ``create_promise`` calls into call order.
    ctx = _root()
    seen: list[str] = []
    original = ctx.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        # Yield so a non-chained implementation would let root.2 race ahead.
        await asyncio.sleep(0)
        return await original(req)

    with patch.object(
        ctx.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.run(double, 1)
        f2 = ctx.run(double, 2)
        # Await in reverse so completion order can't accidentally line up.
        assert await f2 == 4
        assert await f1 == 2

    assert seen == ["root.1", "root.2"]


@pytest.mark.asyncio
async def test_run_chain_blocks_second_create_until_first_returns() -> None:
    # Stronger than the recorder-style test above: gate the first
    # ``create_promise`` and assert the second has NOT entered ``create_promise``
    # at all. This proves the chain parks bg #2 at ``await prev_created.wait()``
    # rather than just happening to win a scheduling race.
    ctx = _root()
    gate = asyncio.Event()
    seen: list[str] = []
    entered_second = asyncio.Event()
    original = ctx.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        if req.id == "root.1":
            await gate.wait()
        else:
            entered_second.set()
        return await original(req)

    with patch.object(
        ctx.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.run(double, 1)
        f2 = ctx.run(double, 2)
        # Yield generously: a non-chained impl would let bg #2 enter create_promise.
        for _ in range(5):
            await asyncio.sleep(0)
        assert seen == ["root.1"]
        assert not entered_second.is_set()

        gate.set()
        assert await f1 == 2
        assert await f2 == 4

    assert seen == ["root.1", "root.2"]


# =============================================================================
# rpc: pending -> register a remote todo and suspend
# =============================================================================


@pytest.mark.asyncio
async def test_rpc_pending_registers_todo_and_suspends() -> None:
    # A fresh remote promise is created pending; awaiting the future appends
    # its id to ``spawned_remote`` and raises ``SuspendedError`` (mirrors Go's
    # Future.Await for a futureRemote pending record).
    ctx = _root()
    fut = ctx.rpc("remote_fn", 1, 2)
    with pytest.raises(SuspendedError):
        await fut
    assert ctx.spawned_remote == ["root.1"]
    assert ctx.effects.cache["root.1"].state == "pending"


# =============================================================================
# rpc: idempotent recovery (pre-settled records short-circuit)
# =============================================================================


@pytest.mark.asyncio
async def test_rpc_presettled_resolved_returns_value() -> None:
    ctx = _root([_resolved("root.1", "remote-result")])
    assert await ctx.rpc("remote_fn") == "remote-result"
    assert ctx.spawned_remote == []


@pytest.mark.asyncio
async def test_rpc_presettled_rejected_raises() -> None:
    ctx = _root([_rejected("root.1", "remote failure")])
    with pytest.raises(ApplicationError, match="remote failure"):
        await ctx.rpc("remote_fn")
    assert ctx.spawned_remote == []


# =============================================================================
# rpc: request shape (tags + TaskData envelope)
# =============================================================================


@contextmanager
def _spy_create_promise(ctx: Context) -> Iterator[list[Any]]:
    """Capture every PromiseCreateReq passed to ``effects.create_promise``."""
    captured: list[Any] = []
    original = ctx.effects.create_promise

    async def spy(req: Any) -> Any:
        captured.append(req)
        return await original(req)

    with patch.object(ctx.effects, "create_promise", new=AsyncMock(side_effect=spy)):
        yield captured


@pytest.mark.asyncio
async def test_rpc_request_tags_and_param() -> None:
    ctx = _root()
    with _spy_create_promise(ctx) as captured, pytest.raises(SuspendedError):
        await ctx.rpc("remote_fn", 1, 2, k="v")

    [req] = captured
    assert req.id == "root.1"
    assert req.tags == {
        "resonate:scope": "global",
        "resonate:target": "",
        "resonate:branch": "root.1",
        "resonate:parent": "root",
        "resonate:origin": "root",
    }
    assert req.param.data == {
        "func": "remote_fn",
        "args": {"args": [1, 2], "kwargs": {"k": "v"}},
    }


@pytest.mark.asyncio
async def test_rpc_no_args_param_is_null() -> None:
    # ``pack_args``-style envelope: an empty call collapses to ``None`` so a
    # remote receiver can round-trip via ``_unpack`` -> ([], {}).
    ctx = _root()
    with _spy_create_promise(ctx) as captured, pytest.raises(SuspendedError):
        await ctx.rpc("remote_fn")

    assert captured[0].param.data == {"func": "remote_fn", "args": None}


# =============================================================================
# rpc: child id allocation matches call order
# =============================================================================


@pytest.mark.asyncio
async def test_rpc_sequential_child_ids() -> None:
    ctx = _root([_resolved("root.1", "a"), _resolved("root.2", "b")])
    assert await ctx.rpc("fn") == "a"  # root.1
    assert await ctx.rpc("fn") == "b"  # root.2


@pytest.mark.asyncio
async def test_rpc_promise_creation_order_under_concurrency() -> None:
    # The chain in ``_advance_promise_chain`` must serialize ``create_promise``
    # calls into call order even when both rpcs are spawned concurrently.
    ctx = _root([_resolved("root.1", "a"), _resolved("root.2", "b")])
    seen: list[str] = []
    original = ctx.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        # Yield so a non-chained implementation would let root.2 race ahead.
        await asyncio.sleep(0)
        return await original(req)

    with patch.object(
        ctx.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.rpc("fn")
        f2 = ctx.rpc("fn")
        # Await in reverse so completion order can't accidentally line up.
        assert await f2 == "b"
        assert await f1 == "a"

    assert seen == ["root.1", "root.2"]


# =============================================================================
# rpc: with_options(timeout=, target=) and per-call opts reset
# =============================================================================


@pytest.mark.asyncio
async def test_rpc_with_options_target_sets_tag() -> None:
    ctx = _root()
    with _spy_create_promise(ctx) as captured, pytest.raises(SuspendedError):
        await ctx.with_opts(target="worker-1").rpc("fn")
    assert captured[0].tags["resonate:target"] == "worker-1"


@pytest.mark.asyncio
async def test_rpc_with_options_timeout_sets_child_deadline() -> None:
    ctx = _root()
    before = now_ms()
    with pytest.raises(SuspendedError):
        await ctx.with_opts(timeout=timedelta(seconds=30)).rpc("fn")
    after = now_ms()
    record = ctx.effects.cache["root.1"]
    assert before + 30_000 <= record.timeout_at <= after + 30_000


@pytest.mark.asyncio
async def test_rpc_with_options_timeout_capped_to_parent() -> None:
    cap = now_ms() + 5_000
    ctx = _root(timeout_at=cap)
    with pytest.raises(SuspendedError):
        await ctx.with_opts(timeout=timedelta(days=365)).rpc("fn")
    assert ctx.effects.cache["root.1"].timeout_at == cap


@pytest.mark.asyncio
async def test_rpc_consumes_options_after_one_call() -> None:
    ctx = _root([_resolved("root.1", "a")])
    await ctx.with_opts(timeout=timedelta(seconds=30), target="x").rpc("fn")
    assert ctx.opts == Opts()


@pytest.mark.asyncio
async def test_rpc_resets_options_even_on_suspend() -> None:
    # Suspension is the common rpc terminal state; opts must still reset so the
    # next call does not inherit them.
    ctx = _root()
    ctx.with_opts(timeout=timedelta(seconds=5), target="x")
    with pytest.raises(SuspendedError):
        await ctx.rpc("fn")
    assert ctx.opts == Opts()


# =============================================================================
# rpc: blocks until the durable promise has been created
# =============================================================================


@pytest.mark.asyncio
async def test_rpc_task_pending_while_create_promise_blocked() -> None:
    # Mirrors ``test_run_task_pending_while_create_promise_blocked``: the Task
    # must not be ``done()`` until ``create_promise`` resolves.
    ctx = _root()
    gate = asyncio.Event()

    with _gated_create_promise(ctx, gate) as entered:
        task = ctx.rpc("fn")

        await entered.wait()

        assert "root.1" not in ctx.effects.cache

        gate.set()
        with pytest.raises(SuspendedError):
            await task
        assert ctx.effects.cache["root.1"].state == "pending"


@pytest.mark.asyncio
async def test_rpc_releases_event_when_create_promise_raises() -> None:
    # If ``create_promise`` raises, the chain event still fires (the ``finally``
    # in bg) and the Task surfaces the original error -- otherwise a chained
    # successor would hang forever.
    ctx = _root()

    failing_mock = AsyncMock(side_effect=RuntimeError("network down"))
    with patch.object(ctx.effects, "create_promise", new=failing_mock):
        task = ctx.rpc("fn")
        with pytest.raises(RuntimeError, match="network down"):
            await task

    failing_mock.assert_awaited_once()
    assert "root.1" not in ctx.effects.cache
    assert ctx.spawned_remote == []


# =============================================================================
# sleep: pending -> register a remote todo and suspend
#
# ``sleep`` is structurally identical to ``rpc`` (both go through the shared
# ``_await_remote`` body): a fresh timer promise is created pending, awaiting
# the future appends its id to ``spawned_remote`` and raises ``SuspendedError``.
# The differences are in the request shape (a ``resonate:timer`` tag, no param,
# no func envelope) and that ``sleep`` takes its deadline from the duration
# argument rather than from ``opts``.
# =============================================================================


@pytest.mark.asyncio
async def test_sleep_pending_registers_todo_and_suspends() -> None:
    ctx = _root()
    fut = ctx.sleep(timedelta(seconds=30))
    with pytest.raises(SuspendedError):
        await fut
    assert ctx.spawned_remote == ["root.1"]
    assert ctx.effects.cache["root.1"].state == "pending"


# =============================================================================
# sleep: idempotent recovery (a pre-settled timer short-circuits)
# =============================================================================


@pytest.mark.asyncio
async def test_sleep_presettled_resolved_returns_none() -> None:
    # An elapsed timer is recovered as a resolved record carrying no payload;
    # ``_decode_settled`` yields its (empty) value and no todo is registered.
    ctx = _root([_resolved("root.1", None)])
    assert await ctx.sleep(timedelta(seconds=1)) is None
    assert ctx.spawned_remote == []


# =============================================================================
# sleep: request shape (timer tag, empty param, no func envelope)
# =============================================================================


@pytest.mark.asyncio
async def test_sleep_request_tags_and_timer_flag() -> None:
    ctx = _root()
    with _spy_create_promise(ctx) as captured, pytest.raises(SuspendedError):
        await ctx.sleep(timedelta(seconds=30))

    [req] = captured
    assert req.id == "root.1"
    assert req.tags == {
        "resonate:scope": "global",
        "resonate:branch": "root.1",
        "resonate:parent": "root",
        "resonate:origin": "root",
        "resonate:timer": "true",
    }
    # A timer promise carries no param payload (unlike rpc's func envelope).
    assert req.param.data is None


@pytest.mark.asyncio
async def test_sleep_timeout_at_is_now_plus_duration() -> None:
    # The wake time is ``now + duration``, taken straight from the argument.
    ctx = _root()
    before = now_ms()
    with pytest.raises(SuspendedError):
        await ctx.sleep(timedelta(seconds=30))
    after = now_ms()
    record = ctx.effects.cache["root.1"]
    assert before + 30_000 <= record.timeout_at <= after + 30_000


@pytest.mark.asyncio
async def test_sleep_duration_capped_to_parent_timeout() -> None:
    # A wake time beyond the parent's deadline is capped, like every other
    # child promise (``child_timeout``).
    cap = now_ms() + 5_000
    ctx = _root(timeout_at=cap)
    with pytest.raises(SuspendedError):
        await ctx.sleep(timedelta(days=365))
    assert ctx.effects.cache["root.1"].timeout_at == cap


# =============================================================================
# sleep: duration comes from the argument, NOT from opts
#
# Unlike run/rpc, ``sleep`` does not read ``opts.timeout`` for its deadline
# (mirrors Go's ``Sleep(d)``). It still consumes any opts set via with_opts()
# so they cannot leak into the next entrypoint call.
# =============================================================================


@pytest.mark.asyncio
async def test_sleep_ignores_opts_timeout_for_duration() -> None:
    # The 30s argument wins over the 5s opt -- the opt does not touch the
    # timer's deadline at all.
    ctx = _root()
    before = now_ms()
    with pytest.raises(SuspendedError):
        await ctx.with_opts(timeout=timedelta(seconds=5)).sleep(timedelta(seconds=30))
    after = now_ms()
    record = ctx.effects.cache["root.1"]
    assert before + 30_000 <= record.timeout_at <= after + 30_000


@pytest.mark.asyncio
async def test_sleep_consumes_options_so_they_do_not_leak() -> None:
    # sleep ignores opts for its own duration but still clears them, so a
    # stray with_opts() before a sleep cannot bleed into the next entrypoint.
    ctx = _root()
    ctx.with_opts(timeout=timedelta(seconds=5), target="x")
    with pytest.raises(SuspendedError):
        await ctx.sleep(timedelta(seconds=1))
    assert ctx.opts == Opts()


# =============================================================================
# sleep: child id allocation matches call order
# =============================================================================


@pytest.mark.asyncio
async def test_sleep_sequential_child_ids() -> None:
    ctx = _root([_resolved("root.1", None), _resolved("root.2", None)])
    assert await ctx.sleep(timedelta(seconds=1)) is None  # root.1
    assert await ctx.sleep(timedelta(seconds=1)) is None  # root.2


@pytest.mark.asyncio
async def test_sleep_promise_creation_order_under_concurrency() -> None:
    # ``sleep`` joins the same ``_advance_promise_chain`` as run/rpc, so two
    # timers spawned concurrently still issue ``create_promise`` in call order.
    ctx = _root([_resolved("root.1", None), _resolved("root.2", None)])
    seen: list[str] = []
    original = ctx.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        # Yield so a non-chained implementation would let root.2 race ahead.
        await asyncio.sleep(0)
        return await original(req)

    with patch.object(
        ctx.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.sleep(timedelta(seconds=1))
        f2 = ctx.sleep(timedelta(seconds=1))
        # Await in reverse so completion order can't accidentally line up.
        assert await f2 is None
        assert await f1 is None

    assert seen == ["root.1", "root.2"]


# =============================================================================
# sleep: blocks until the durable promise has been created
# =============================================================================


@pytest.mark.asyncio
async def test_sleep_releases_event_when_create_promise_raises() -> None:
    # If ``create_promise`` raises, the shared ``_await_remote`` body still sets
    # the chain event in its ``finally`` and the Task surfaces the original
    # error -- otherwise a chained successor would hang forever.
    ctx = _root()

    failing_mock = AsyncMock(side_effect=RuntimeError("network down"))
    with patch.object(ctx.effects, "create_promise", new=failing_mock):
        task = ctx.sleep(timedelta(seconds=1))
        with pytest.raises(RuntimeError, match="network down"):
            await task

    failing_mock.assert_awaited_once()
    assert "root.1" not in ctx.effects.cache
    assert ctx.spawned_remote == []


# =============================================================================
# promise: pending -> register a remote todo and suspend
#
# ``promise`` creates a *deferred* (DI) durable promise -- one with no func
# envelope and no timer flag, meant to be resolved/rejected by some external
# party. It shares the same ``_await_remote`` body as ``rpc``/``sleep``: a fresh
# pending record appends its id to ``spawned_remote`` and raises
# ``SuspendedError``; a pre-settled record short-circuits with its value. The
# distinguishing trait is the request shape (empty param, no ``resonate:timer``,
# no ``resonate:target``) and that the deadline comes from the explicit
# ``timeout`` argument rather than from ``opts``.
# =============================================================================


@pytest.mark.asyncio
async def test_promise_pending_registers_todo_and_suspends() -> None:
    ctx = _root()
    fut = ctx.promise(timedelta(seconds=30))
    with pytest.raises(SuspendedError):
        await fut
    assert ctx.spawned_remote == ["root.1"]
    assert ctx.effects.cache["root.1"].state == "pending"


# =============================================================================
# promise: idempotent recovery (a pre-settled DI promise short-circuits)
# =============================================================================


@pytest.mark.asyncio
async def test_promise_presettled_resolved_returns_value() -> None:
    # A DI promise resolved by an external party is recovered as a resolved
    # record; ``_decode_settled`` yields its payload and no todo is registered.
    ctx = _root([_resolved("root.1", "external-result")])
    assert await ctx.promise(timedelta(seconds=1)) == "external-result"
    assert ctx.spawned_remote == []


@pytest.mark.asyncio
async def test_promise_presettled_rejected_raises() -> None:
    ctx = _root([_rejected("root.1", "external failure")])
    with pytest.raises(ApplicationError, match="external failure"):
        await ctx.promise(timedelta(seconds=1))
    assert ctx.spawned_remote == []


# =============================================================================
# promise: request shape (empty param, no timer flag, no func envelope)
# =============================================================================


@pytest.mark.asyncio
async def test_promise_request_tags_and_empty_param() -> None:
    ctx = _root()
    with _spy_create_promise(ctx) as captured, pytest.raises(SuspendedError):
        await ctx.promise(timedelta(seconds=30))

    [req] = captured
    assert req.id == "root.1"
    # A DI promise carries the standard scope/lineage tags only -- no timer
    # flag (unlike sleep) and no target (unlike rpc).
    assert req.tags == {
        "resonate:scope": "global",
        "resonate:branch": "root.1",
        "resonate:parent": "root",
        "resonate:origin": "root",
    }
    assert "resonate:timer" not in req.tags
    assert "resonate:target" not in req.tags
    # No param payload and no func envelope: the promise is filled in later.
    assert req.param.data is None


@pytest.mark.asyncio
async def test_promise_timeout_at_is_now_plus_timeout() -> None:
    ctx = _root()
    before = now_ms()
    with pytest.raises(SuspendedError):
        await ctx.promise(timedelta(seconds=30))
    after = now_ms()
    record = ctx.effects.cache["root.1"]
    assert before + 30_000 <= record.timeout_at <= after + 30_000


@pytest.mark.asyncio
async def test_promise_none_timeout_uses_default() -> None:
    # ``timeout=None`` falls back to ``DEFAULT_TIMEOUT`` (24h) via
    # ``child_timeout``, well past any explicit short timeout.
    ctx = _root()
    before = now_ms()
    with pytest.raises(SuspendedError):
        await ctx.promise(None)
    after = now_ms()
    record = ctx.effects.cache["root.1"]
    day_ms = 24 * 60 * 60 * 1000
    assert before + day_ms <= record.timeout_at <= after + day_ms


@pytest.mark.asyncio
async def test_promise_timeout_capped_to_parent() -> None:
    cap = now_ms() + 5_000
    ctx = _root(timeout_at=cap)
    with pytest.raises(SuspendedError):
        await ctx.promise(timedelta(days=365))
    assert ctx.effects.cache["root.1"].timeout_at == cap


# =============================================================================
# promise: deadline comes from the argument, NOT from opts
# =============================================================================


@pytest.mark.asyncio
async def test_promise_ignores_opts_timeout_for_deadline() -> None:
    # Like ``sleep``, ``promise`` takes its deadline from its argument; a stray
    # ``with_opts(timeout=...)`` must not touch the DI promise's deadline.
    ctx = _root()
    before = now_ms()
    with pytest.raises(SuspendedError):
        await ctx.with_opts(timeout=timedelta(seconds=5)).promise(timedelta(seconds=30))
    after = now_ms()
    record = ctx.effects.cache["root.1"]
    assert before + 30_000 <= record.timeout_at <= after + 30_000


@pytest.mark.asyncio
async def test_promise_consumes_options_so_they_do_not_leak() -> None:
    # ``promise`` ignores opts for its own deadline but still clears them, so a
    # stray ``with_opts()`` cannot bleed into the next entrypoint call.
    ctx = _root()
    ctx.with_opts(timeout=timedelta(seconds=5), target="x")
    with pytest.raises(SuspendedError):
        await ctx.promise(timedelta(seconds=1))
    assert ctx.opts == Opts()


# =============================================================================
# promise: child id allocation matches call order
# =============================================================================


@pytest.mark.asyncio
async def test_promise_sequential_child_ids() -> None:
    ctx = _root([_resolved("root.1", "a"), _resolved("root.2", "b")])
    assert await ctx.promise(timedelta(seconds=1)) == "a"  # root.1
    assert await ctx.promise(timedelta(seconds=1)) == "b"  # root.2


@pytest.mark.asyncio
async def test_promise_creation_order_under_concurrency() -> None:
    # ``promise`` joins the same ``_advance_promise_chain`` as run/rpc/sleep, so
    # two DI promises spawned concurrently still create in call order.
    ctx = _root([_resolved("root.1", "a"), _resolved("root.2", "b")])
    seen: list[str] = []
    original = ctx.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        # Yield so a non-chained implementation would let root.2 race ahead.
        await asyncio.sleep(0)
        return await original(req)

    with patch.object(
        ctx.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.promise(timedelta(seconds=1))
        f2 = ctx.promise(timedelta(seconds=1))
        # Await in reverse so completion order can't accidentally line up.
        assert await f2 == "b"
        assert await f1 == "a"

    assert seen == ["root.1", "root.2"]


# =============================================================================
# promise: blocks until the durable promise has been created
# =============================================================================


@pytest.mark.asyncio
async def test_promise_releases_event_when_create_promise_raises() -> None:
    # If ``create_promise`` raises, the shared ``_await_remote`` body still sets
    # the chain event in its ``finally`` and the Task surfaces the original
    # error -- otherwise a chained successor would hang forever.
    ctx = _root()

    failing_mock = AsyncMock(side_effect=RuntimeError("network down"))
    with patch.object(ctx.effects, "create_promise", new=failing_mock):
        task = ctx.promise(timedelta(seconds=1))
        with pytest.raises(RuntimeError, match="network down"):
            await task

    failing_mock.assert_awaited_once()
    assert "root.1" not in ctx.effects.cache
    assert ctx.spawned_remote == []


# =============================================================================
# run + rpc: shared promise-creation chain orders both code paths
# =============================================================================


@pytest.mark.asyncio
async def test_mixed_run_and_rpc_create_in_call_order() -> None:
    # ``run`` and ``rpc`` share the same ``_tail`` chain on Context. A mixed
    # sequence must see ``create_promise`` called in call order across both
    # code paths -- otherwise interleaving the two would break id allocation.
    ctx = _root(
        [
            _resolved("root.2", "remote-1"),
            _resolved("root.4", "remote-2"),
        ]
    )
    seen: list[str] = []
    original = ctx.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        # Yield so a non-chained implementation could let later calls race ahead.
        await asyncio.sleep(0)
        return await original(req)

    with patch.object(
        ctx.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.run(double, 1)  # root.1 (executes locally)
        f2 = ctx.rpc("remote_fn")  # root.2 (preloaded resolved)
        f3 = ctx.run(double, 3)  # root.3 (executes locally)
        f4 = ctx.rpc("remote_fn")  # root.4 (preloaded resolved)
        # Await in reverse so completion order is decoupled from call order.
        assert await f4 == "remote-2"
        assert await f3 == 6
        assert await f2 == "remote-1"
        assert await f1 == 2

    assert seen == ["root.1", "root.2", "root.3", "root.4"]


@pytest.mark.asyncio
async def test_mixed_run_rpc_sleep_create_in_call_order() -> None:
    # ``sleep`` shares the same ``_tail`` chain as ``run`` and ``rpc``; a mixed
    # sequence across all three must still call ``create_promise`` in call order.
    ctx = _root(
        [
            _resolved("root.2", "remote-1"),
            _resolved("root.3", None),
        ]
    )
    seen: list[str] = []
    original = ctx.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        await asyncio.sleep(0)
        return await original(req)

    with patch.object(
        ctx.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.run(double, 1)  # root.1 (executes locally)
        f2 = ctx.rpc("remote_fn")  # root.2 (preloaded resolved)
        f3 = ctx.sleep(timedelta(seconds=1))  # root.3 (preloaded resolved timer)
        # Await in reverse so completion order is decoupled from call order.
        assert await f3 is None
        assert await f2 == "remote-1"
        assert await f1 == 2

    assert seen == ["root.1", "root.2", "root.3"]


@pytest.mark.asyncio
async def test_mixed_run_rpc_sleep_promise_create_in_call_order() -> None:
    # All four entrypoints (``run``/``rpc``/``sleep``/``promise``) share the
    # same ``_tail`` chain; a mixed sequence across every code path must still
    # call ``create_promise`` in call order.
    ctx = _root(
        [
            _resolved("root.2", "remote-1"),
            _resolved("root.3", None),
            _resolved("root.4", "di-1"),
        ]
    )
    seen: list[str] = []
    original = ctx.effects.create_promise

    async def recorder(req: Any) -> Any:
        seen.append(req.id)
        await asyncio.sleep(0)
        return await original(req)

    with patch.object(
        ctx.effects, "create_promise", new=AsyncMock(side_effect=recorder)
    ):
        f1 = ctx.run(double, 1)  # root.1 (executes locally)
        f2 = ctx.rpc("remote_fn")  # root.2 (preloaded resolved)
        f3 = ctx.sleep(timedelta(seconds=1))  # root.3 (preloaded resolved timer)
        f4 = ctx.promise(timedelta(seconds=1))  # root.4 (preloaded resolved DI)
        # Await in reverse so completion order is decoupled from call order.
        assert await f4 == "di-1"
        assert await f3 is None
        assert await f2 == "remote-1"
        assert await f1 == 2

    assert seen == ["root.1", "root.2", "root.3", "root.4"]
