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

from datetime import timedelta
from typing import Any

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


async def double(x: int) -> int:
    """Double ``x`` -- a pure leaf with no env parameter."""
    return x * 2


async def add(ctx: Context, a: int, b: int) -> int:
    """Add ``a`` and ``b``, ignoring the injected Context."""
    return a + b


async def beat() -> str:
    """Return a constant -- a no-arg leaf (``pack_args`` -> ``None``)."""
    return "ok"


async def failing(x: int) -> int:
    msg = "denied"
    raise ApplicationError(msg)


class Point(msgspec.Struct, frozen=True):
    x: int
    y: int


async def sum_point(p: Point) -> int:
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
async def test_run_zero_arg_function() -> None:
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
        await ctx.run(failing, 0)
    assert ctx.effects.cache["root.1"].state == "rejected"


# =============================================================================
# run: idempotent recovery (a pre-settled promise skips execution)
# =============================================================================


@pytest.mark.asyncio
async def test_run_presettled_resolved_skips_execution() -> None:
    calls = 0

    async def counted(x: int) -> int:
        nonlocal calls
        calls += 1
        return x

    ctx = _root([_resolved("root.1", 99)])
    assert await ctx.run(counted, 1) == 99  # value from the pre-settled record
    assert calls == 0  # the function body never ran


@pytest.mark.asyncio
async def test_run_presettled_rejected_raises_without_execution() -> None:
    calls = 0

    async def counted(x: int) -> int:
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

    async def counted(x: int) -> int:
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
# run: with_options(timeout=...) and per-call opts reset
# =============================================================================


@pytest.mark.asyncio
async def test_run_with_options_timeout_sets_child_deadline() -> None:
    ctx = _root()
    before = now_ms()
    assert await ctx.with_options(timeout=timedelta(seconds=30)).run(double, 5) == 10
    after = now_ms()
    record = ctx.effects.cache["root.1"]
    assert before + 30_000 <= record.timeout_at <= after + 30_000


@pytest.mark.asyncio
async def test_run_with_options_timeout_capped_to_parent() -> None:
    cap = now_ms() + 5_000
    ctx = _root(timeout_at=cap)
    # A year-long timeout still cannot outlive the parent's deadline.
    await ctx.with_options(timeout=timedelta(days=365)).run(double, 1)
    assert ctx.effects.cache["root.1"].timeout_at == cap


@pytest.mark.asyncio
async def test_run_consumes_options_after_one_call() -> None:
    ctx = _root()
    await ctx.with_options(timeout=timedelta(seconds=30)).run(double, 1)  # root.1
    short = ctx.effects.cache["root.1"].timeout_at
    # The next run carries no options -> the 24h default, well past the 30s one.
    await ctx.run(double, 1)  # root.2
    assert ctx.effects.cache["root.2"].timeout_at > short
    assert ctx.opts == Opts()


@pytest.mark.asyncio
async def test_run_resets_options_even_on_error() -> None:
    ctx = _root()
    ctx.with_options(timeout=timedelta(seconds=5))
    with pytest.raises(ApplicationError):
        await ctx.run(failing, 0)
    assert ctx.opts == Opts()
