"""Structural tests for :meth:`Core._execute_until_blocked_inner`.

Where :mod:`tests.test_core` drives the inner through the full lifecycle against
a *real* server simulation (LocalNetwork), this module pokes the inner directly
with a **dict-backed mock** for :class:`~resonate.effects.Effects`. The point is
not the lifecycle (fulfill/suspend/release -- the caller owns those) but the
*shape of the execution tree* the run leaves behind the moment it returns, and
how that tree evolves across **replays**.

What the tree records (and what these tests assert on -- not just ids):

* **state** -- ``pending`` until the promise settles, then ``resolved`` /
  ``rejected``. A suspended run leaves its blocking leaves ``pending`` and every
  finished step ``resolved``.
* **type** -- derived from the ``resonate:scope`` / ``resonate:timer`` tags the
  Context stamps on each create (see :meth:`Context._local_create_req` etc.):
  ``run`` (local child), ``rpc`` (remote call), ``sleep`` (timer), ``promise``
  (bare external promise), ``detached`` (fire-and-forget remote). The *type*
  decides whether a pending node blocks the parent: ``rpc``/``sleep``/``promise``
  do, ``detached`` does not.
* **value** -- the settled payload, asserted where it is meaningful.

Replay: the real :class:`Effects` is seeded from a ``preload`` of records the
server already settled. :class:`MockEffects` mirrors that -- on replay,
``create_promise`` returns the cached settled record and ``ctx.run`` /
``ctx.rpc`` short-circuit via ``_decode_settled`` *without re-executing the
function body*. The replay tests below assert exactly that: settled leaves are
not re-run, orchestrators with still-pending promises are, and the tree
converges toward fully resolved as the server settles the blocking nodes.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Literal

import pytest

from resonate.codec import Codec, NoopEncryptor, _encode_error
from resonate.core import Core, _ExecFulfilled, _ExecSuspended, identity_target_resolver
from resonate.error import ApplicationError, ResonateError
from resonate.registry import Registry
from resonate.types import PromiseCreateReq, PromiseRecord, Value

if TYPE_CHECKING:
    from resonate.context import Context

# Far-future deadline, matching tests.test_core (Go's ``int64(1) << 50``).
FAR_FUTURE = 1 << 50
TTL = 10_000


# ── Dict-backed mock for Effects ─────────────────────────────────────────


class MockEffects:
    """A dict-backed stand-in for :class:`~resonate.effects.Effects`.

    Holds the execution tree as a plain ``{id: PromiseRecord}`` map and nothing
    else -- no Sender, no Codec, no network. ``create_promise`` records a fresh
    ``pending`` node (or returns the cached one, idempotent like the real
    Effects); ``settle_promise`` flips an existing node to ``resolved`` /
    ``rejected``. When :meth:`Core._execute_until_blocked_inner` returns,
    ``cache`` *is* the tree the run produced, ready to assert against.

    ``preload`` seeds the cache the way the server re-delivers a resumed task's
    already-settled promises -- the hook the replay tests drive.
    """

    def __init__(self, preload: list[PromiseRecord] | None = None) -> None:
        self.cache: dict[str, PromiseRecord] = {p.id: p for p in (preload or [])}

    async def create_promise(self, req: PromiseCreateReq) -> PromiseRecord:
        cached = self.cache.get(req.id)
        if cached is not None:
            return cached
        record = PromiseRecord(
            id=req.id,
            state="pending",
            param=req.param,
            timeout_at=req.timeout_at,
            tags=req.tags,
        )
        self.cache[req.id] = record
        return record

    async def settle_promise(self, id: str, result: Any) -> PromiseRecord:
        cached = self.cache.get(id)
        if cached is not None and cached.state != "pending":
            return cached

        state: Literal["resolved", "rejected"]
        if isinstance(result, ResonateError):
            state = "rejected"
            value = Value(data=_encode_error(result))
        else:
            state = "resolved"
            value = Value(data=result)

        record = PromiseRecord(
            id=id,
            state=state,
            param=cached.param if cached is not None else Value(),
            value=value,
            timeout_at=cached.timeout_at if cached is not None else FAR_FUTURE,
            tags=cached.tags if cached is not None else {},
        )
        self.cache[id] = record
        return record


# ── Helpers ──────────────────────────────────────────────────────────────


def _core(reg: Registry) -> Core:
    """Build a Core whose inner never touches the network.

    ``_execute_until_blocked_inner`` reads only ``self.registry`` (function
    lookup), ``self.codec`` (the fulfill / short-circuit paths), and
    ``self.resolver`` (rpc target). It never calls ``self.sender`` or the
    heartbeat, so ``sender=None`` / ``heartbeat=None`` are safe here.
    """
    return Core(
        sender=None,
        codec=Codec(NoopEncryptor()),
        registry=reg,
        resolver=identity_target_resolver,
        pid="tree-test",
        ttl=TTL,
    )


def _root(func: str, args: Any = None, *, id: str = "foo.1") -> PromiseRecord:
    """Build a pending root promise whose param carries a ``TaskData``.

    The inner decodes ``param.data`` straight into ``TaskData`` (it is handed an
    already-decoded promise), so the param data is the plain dict, not encoded.
    ``args`` is the (optional) single user positional.
    """
    return PromiseRecord(
        id=id,
        state="pending",
        param=Value(
            data={
                "func": func,
                "args": [] if args is None else [args],
                "kwargs": {},
                "version": 1,
            }
        ),
        timeout_at=FAR_FUTURE,
    )


def kind(rec: PromiseRecord) -> str:
    """Classify a promise by the tags the Context stamped at create time.

    Mirrors the create-req builders in :mod:`resonate.context`:

    * ``resonate:scope == "local"``           -> ``run``   (ctx.run)
    * ``resonate:scope == "global"`` + timer  -> ``sleep``  (ctx.sleep)
    * global + empty param                    -> ``promise`` (ctx.promise)
    * global + structured numeric id          -> ``rpc``     (ctx.rpc)
    * global + hashed id                       -> ``detached`` (ctx.detached)
    """
    tags = rec.tags
    scope = tags.get("resonate:scope")
    match scope:
        case "local":
            return "run"
        case "global":
            if tags.get("resonate:timer") == "true":
                return "sleep"
            if rec.param.data is None:
                return "promise"
            return "rpc" if rec.id.rsplit(".", 1)[-1].isdigit() else "detached"
        case _:
            msg = "promise without scope"
            raise ValueError(msg)


def describe(cache: dict[str, PromiseRecord]) -> dict[str, tuple[str, str]]:
    """Map every node to ``(type, state)`` -- the structural snapshot tests assert."""
    return {id: (kind(rec), rec.state) for id, rec in cache.items()}


def _replayed(
    cache: dict[str, PromiseRecord], resolved: dict[str, Any]
) -> list[PromiseRecord]:
    """Copy ``cache``, flipping the given ids to ``resolved`` with the given values.

    Models the server settling the promises a suspended task was waiting on and
    re-delivering the task with these records as preload -- the input to a replay
    of :meth:`Core._execute_until_blocked_inner`.
    """
    out = []
    for rec in cache.values():
        if rec.id in resolved:
            out.append(
                PromiseRecord(
                    id=rec.id,
                    state="resolved",
                    param=rec.param,
                    value=Value(data=resolved[rec.id]),
                    tags=rec.tags,
                    timeout_at=rec.timeout_at,
                )
            )
        else:
            out.append(rec)
    return out


# ── Suspend: tree shape at the moment of blocking ────────────────────────


@pytest.mark.asyncio
async def test_nested_tree_blocks_on_rpc_leaves() -> None:
    """The foo/bar/baz tree: every baz settles, every rpc stays pending.

    Tree the run leaves behind (root ``foo.1`` is created by the caller, so it
    is not in the cache)::

        foo.1.1 [run] pending          (bar #1 -- suspended on its rpc)
          foo.1.1.1 [run] resolved     (baz)
          foo.1.1.2 [rpc] pending      (rpc ".")
          foo.1.1.3 [run] resolved     (baz)
        foo.1.2 [run] pending          (bar #2 -- suspended on its rpc)
          foo.1.2.1 [run] resolved     (baz)
          foo.1.2.2 [rpc] pending      (rpc ".")
          foo.1.2.3 [run] resolved     (baz)

    blocked on foo.1.1.2, foo.1.2.2
    """
    calls = {"foo": 0, "bar": 0, "baz": 0}

    async def baz(ctx: Context) -> None:
        calls["baz"] += 1
        await asyncio.sleep(0)  # yield, mimicking the example's asyncio.sleep

    async def bar(ctx: Context) -> None:
        calls["bar"] += 1
        # Two local leaves bracket one rpc. The rpc child is created pending, so
        # ``await p2`` suspends -- but p1/p3 still settle (p3's eager task is
        # joined by flush_local_work even though bar never reaches its await).
        p1 = ctx.run(baz)
        p2 = ctx.rpc(".")
        p3 = ctx.run(baz)
        await p1
        await p2
        await p3

    async def foo(ctx: Context) -> None:
        calls["foo"] += 1
        # Both bars are spawned eagerly before either await, so bar #2 is
        # created (and joined by flush) even though ``await p1`` suspends first.
        p1 = ctx.run(bar)
        p2 = ctx.run(bar)
        await p1
        await p2

    reg = Registry()
    reg.register("foo", foo)
    effects = MockEffects()

    outcome = await _core(reg)._execute_until_blocked_inner(_root("foo"), effects)

    assert describe(effects.cache) == {
        "foo.1.1": ("run", "pending"),
        "foo.1.1.1": ("run", "resolved"),
        "foo.1.1.2": ("rpc", "pending"),
        "foo.1.1.3": ("run", "resolved"),
        "foo.1.2": ("run", "pending"),
        "foo.1.2.1": ("run", "resolved"),
        "foo.1.2.2": ("rpc", "pending"),
        "foo.1.2.3": ("run", "resolved"),
    }

    assert isinstance(outcome, _ExecSuspended)
    assert sorted(outcome.todos) == ["foo.1.1.2", "foo.1.2.2"]
    # One pass: every function ran exactly once.
    assert calls == {"foo": 1, "bar": 2, "baz": 4}


@pytest.mark.asyncio
async def test_single_level_suspends_on_rpc_between_settled_leaves() -> None:
    """One rpc between two local leaves: the leaves settle, the rpc blocks."""

    def leaf(ctx: Context) -> int:
        return 1

    async def mixed(ctx: Context) -> None:
        a = ctx.run(leaf)
        r = ctx.rpc("remote")
        b = ctx.run(leaf)
        await a
        await r
        await b

    reg = Registry()
    reg.register("mixed", mixed)
    effects = MockEffects()

    outcome = await _core(reg)._execute_until_blocked_inner(
        _root("mixed", id="m"), effects
    )

    assert describe(effects.cache) == {
        "m.1": ("run", "resolved"),
        "m.2": ("rpc", "pending"),
        "m.3": ("run", "resolved"),
    }
    # The settled leaves carry leaf()'s return value; the blocking rpc has none.
    assert effects.cache["m.1"].value.data == 1
    assert effects.cache["m.3"].value.data == 1
    assert effects.cache["m.2"].value.data is None

    assert isinstance(outcome, _ExecSuspended)
    assert outcome.todos == ["m.2"]


@pytest.mark.asyncio
async def test_sleep_creates_timer_promise_and_replay_fulfills() -> None:
    """``ctx.sleep`` blocks on a timer-scoped promise; firing it lets the run finish."""

    async def naps(ctx: Context) -> str:
        await ctx.sleep(timedelta(seconds=1))
        return "awake"

    reg = Registry()
    reg.register("naps", naps)
    core = _core(reg)
    effects = MockEffects()

    outcome = await core._execute_until_blocked_inner(_root("naps", id="s"), effects)

    # Blocked on a single timer promise.
    assert describe(effects.cache) == {"s.1": ("sleep", "pending")}
    assert isinstance(outcome, _ExecSuspended)
    assert outcome.todos == ["s.1"]

    # Replay after the server fired the timer (timers resolve with null).
    effects2 = MockEffects(_replayed(effects.cache, {"s.1": None}))
    outcome2 = await core._execute_until_blocked_inner(_root("naps", id="s"), effects2)

    assert describe(effects2.cache) == {"s.1": ("sleep", "resolved")}
    assert isinstance(outcome2, _ExecFulfilled)
    assert core.codec.decode(outcome2.value) == "awake"


@pytest.mark.asyncio
async def test_promise_creates_external_promise_and_replay_delivers_value() -> None:
    """``ctx.promise`` blocks on a bare external promise; resolving it delivers its value."""

    async def waits(ctx: Context) -> str:
        return await ctx.promise()

    reg = Registry()
    reg.register("waits", waits)
    core = _core(reg)
    effects = MockEffects()

    outcome = await core._execute_until_blocked_inner(_root("waits", id="p"), effects)

    assert describe(effects.cache) == {"p.1": ("promise", "pending")}
    assert isinstance(outcome, _ExecSuspended)
    assert outcome.todos == ["p.1"]

    # Replay after something external resolved the promise with a payload.
    effects2 = MockEffects(_replayed(effects.cache, {"p.1": "signal"}))
    outcome2 = await core._execute_until_blocked_inner(_root("waits", id="p"), effects2)

    assert describe(effects2.cache) == {"p.1": ("promise", "resolved")}
    assert isinstance(outcome2, _ExecFulfilled)
    assert core.codec.decode(outcome2.value) == "signal"


@pytest.mark.asyncio
async def test_detached_child_is_created_but_does_not_block() -> None:
    """A detached child lands in the tree as a pending node yet never blocks the parent.

    Unlike ``rpc``, ``ctx.detached`` registers no remote todo and never raises
    SuspendedError -- so the run fulfills immediately even though the detached
    promise is still pending. The future resolves to the child id.
    """

    async def fires(ctx: Context) -> str:
        return await ctx.detached("worker")

    reg = Registry()
    reg.register("fires", fires)
    core = _core(reg)
    effects = MockEffects()

    outcome = await core._execute_until_blocked_inner(_root("fires", id="d"), effects)

    # Exactly one node: a pending, detached promise -- created but not awaited-on.
    assert len(effects.cache) == 1
    (only,) = effects.cache.values()
    assert kind(only) == "detached"
    assert only.state == "pending"

    # The parent did NOT suspend on it.
    assert isinstance(outcome, _ExecFulfilled)
    assert outcome.state == "resolved"
    assert core.codec.decode(outcome.value) == only.id


@pytest.mark.asyncio
async def test_detached_id_stays_bounded_across_recursive_re_root() -> None:
    """Recursive ``detached`` ids stay bounded via the ``resonate:origin`` tag.

    A re-rooted workflow takes its lineage origin from the tag, not its own
    (grown) id. A detached workflow runs as its own root. Were its origin
    re-rooted to its
    own id, every recursion level would prepend a longer prefix and the detached
    id would grow ``+17`` chars per level without bound. Reading the origin from
    the tag the dispatcher set means each level shares the *original* top origin,
    so the id is always ``{top_origin}.{16hex}`` -- exactly one segment past the
    dotless origin, regardless of depth.
    """

    async def fires(ctx: Context) -> str:
        return await ctx.detached("fires")

    reg = Registry()
    reg.register("fires", fires)
    core = _core(reg)
    effects = MockEffects()

    # Simulate a workflow already several detached levels deep: its OWN id has
    # grown a hashed segment, but the lineage origin (the tag) is still the top.
    grown_id = "top.deadbeefdeadbeef"
    promise = PromiseRecord(
        id=grown_id,
        state="pending",
        param=Value(data={"func": "fires", "args": [], "kwargs": {}, "version": 1}),
        timeout_at=FAR_FUTURE,
        tags={"resonate:origin": "top"},
    )

    await core._execute_until_blocked_inner(promise, effects)

    (child,) = effects.cache.values()
    # Bounded shape: rooted at the ORIGINAL origin, one segment past a dotless
    # top -- NOT the grown id (which would yield two dots and grow each level).
    origin, dot, suffix = child.id.partition(".")
    assert (origin, dot) == ("top", ".")
    assert child.id.count(".") == 1
    assert len(suffix) == 16
    assert all(c in "0123456789abcdef" for c in suffix)
    # The original origin is carried forward, keeping the next level bounded too.
    assert child.tags["resonate:origin"] == "top"


@pytest.mark.asyncio
async def test_recursive_multi_detached_keeps_ids_bounded_through_core() -> None:
    """Multiple recursive detached do not grow the id -- proven end-to-end.

    Fan out FANOUT detached per workflow, DEPTH levels deep, re-rooting each
    child by feeding its created promise back through ``_execute_until_blocked_
    inner`` exactly as a worker would. The origin is resolved the production way
    (core reads ``resonate:origin`` from the promise), so this exercises the
    real fix -- not a hand-rolled re-root. ``blake2b`` flattens the ever-growing
    seqid to a constant 16 hex and the origin stays pinned to the top, so every
    detached id is ``{top}.{16hex}`` regardless of depth or fan-out.
    """
    top, fanout, depth = "top", 3, 4

    async def fires(ctx: Context) -> str:
        for _ in range(fanout):
            await ctx.detached("fires")
        return "done"

    reg = Registry()
    reg.register("fires", fires)
    core = _core(reg)
    effects = MockEffects()  # one shared store across every level

    # Seed the genuine top-level root: origin tag equals its own id.
    root = PromiseRecord(
        id=top,
        state="pending",
        param=Value(data={"func": "fires", "args": [], "kwargs": {}, "version": 1}),
        timeout_at=FAR_FUTURE,
        tags={"resonate:origin": top},
    )

    frontier = [root]
    seen = {top}
    lengths: set[int] = set()
    ids: set[str] = set()
    origins: set[str] = set()

    for _level in range(depth):
        next_frontier: list[PromiseRecord] = []
        for promise in frontier:
            await core._execute_until_blocked_inner(promise, effects)
            # Newly created detached children become the next level's roots --
            # core re-roots each one off its own resonate:origin tag.
            for rec in effects.cache.values():
                if rec.id not in seen and kind(rec) == "detached":
                    seen.add(rec.id)
                    lengths.add(len(rec.id))
                    ids.add(rec.id)
                    origins.add(rec.tags["resonate:origin"])
                    next_frontier.append(rec)
        frontier = next_frontier

    # Bounded: a single length across all depths, exactly ``{top}.{16hex}``.
    assert lengths == {len(top) + 1 + 16}
    # Origin stays pinned to the top at every level -- the mechanism that bounds it.
    assert origins == {top}
    # No collisions: every node in the tree got a distinct id.
    assert len(ids) == (fanout ** (depth + 1) - fanout) // (fanout - 1)


# ── Fulfill: everything settles in one pass ──────────────────────────────


@pytest.mark.asyncio
async def test_all_local_tree_fulfills_with_every_node_resolved() -> None:
    """No remote work: the run finishes, fulfills, and leaves a fully settled tree."""

    def leaf(ctx: Context) -> int:
        return 1

    async def all_local(ctx: Context) -> int:
        a = ctx.run(leaf)
        b = ctx.run(leaf)
        return await a + await b

    reg = Registry()
    reg.register("allLocal", all_local)
    core = _core(reg)
    effects = MockEffects()

    outcome = await core._execute_until_blocked_inner(
        _root("allLocal", id="g"), effects
    )

    assert describe(effects.cache) == {
        "g.1": ("run", "resolved"),
        "g.2": ("run", "resolved"),
    }
    assert effects.cache["g.1"].value.data == 1
    assert effects.cache["g.2"].value.data == 1

    assert isinstance(outcome, _ExecFulfilled)
    assert outcome.state == "resolved"
    # The fulfill path encodes the return value through the codec, so decode it.
    assert core.codec.decode(outcome.value) == 2


# ── Rejection: a failed child is recorded rejected in the tree ───────────


@pytest.mark.asyncio
async def test_rejected_child_recorded_and_caught_by_parent() -> None:
    """A failing run child settles ``rejected``; a parent that catches still resolves."""

    async def boom(ctx: Context) -> int:
        msg = "kaboom"
        raise ApplicationError(msg)

    async def parent(ctx: Context) -> str:
        try:
            await ctx.run(boom)
        except ResonateError:
            return "rescued"
        return "unreachable"

    reg = Registry()
    reg.register("parent", parent)
    core = _core(reg)
    effects = MockEffects()

    outcome = await core._execute_until_blocked_inner(_root("parent", id="e"), effects)

    assert describe(effects.cache) == {"e.1": ("run", "rejected")}
    assert effects.cache["e.1"].value.data == {
        "__type": "error",
        "message": "kaboom",
    }

    # The parent swallowed the rejection, so the root resolves.
    assert isinstance(outcome, _ExecFulfilled)
    assert outcome.state == "resolved"
    assert core.codec.decode(outcome.value) == "rescued"


@pytest.mark.asyncio
async def test_uncaught_rejection_propagates_to_root() -> None:
    """An unhandled child rejection bubbles up: the root fulfills as ``rejected``."""

    async def boom(ctx: Context) -> int:
        msg = "kaboom"
        raise ApplicationError(msg)

    async def explodes(ctx: Context) -> int:
        await ctx.run(boom)
        return 0

    reg = Registry()
    reg.register("explodes", explodes)
    core = _core(reg)
    effects = MockEffects()

    outcome = await core._execute_until_blocked_inner(
        _root("explodes", id="x"), effects
    )

    assert describe(effects.cache) == {"x.1": ("run", "rejected")}
    assert isinstance(outcome, _ExecFulfilled)
    assert outcome.state == "rejected"


# ── Replay: re-execution over a preloaded cache ──────────────────────────


@pytest.mark.asyncio
async def test_replay_settles_remotes_and_fulfills_without_reexecuting_leaves() -> None:
    """Run 1 suspends on the two rpcs; run 2 (both settled) fulfills.

    The orchestrators (foo, bar) replay because their own promises are still
    pending, but the resolved baz leaves are NOT re-executed -- the call counter
    proves it.
    """
    calls = {"foo": 0, "bar": 0, "baz": 0}

    async def baz(ctx: Context) -> None:
        calls["baz"] += 1
        await asyncio.sleep(0)

    async def bar(ctx: Context) -> None:
        calls["bar"] += 1
        p1 = ctx.run(baz)
        p2 = ctx.rpc(".")
        p3 = ctx.run(baz)
        await p1
        await p2
        await p3

    async def foo(ctx: Context) -> None:
        calls["foo"] += 1
        p1 = ctx.run(bar)
        p2 = ctx.run(bar)
        await p1
        await p2

    reg = Registry()
    reg.register("foo", foo)
    core = _core(reg)

    # Run 1: suspend on both rpc leaves.
    effects1 = MockEffects()
    outcome1 = await core._execute_until_blocked_inner(_root("foo"), effects1)
    assert isinstance(outcome1, _ExecSuspended)
    assert calls == {"foo": 1, "bar": 2, "baz": 4}

    # Run 2: the server settled both rpcs; replay over that preload.
    preload = _replayed(effects1.cache, {"foo.1.1.2": 11, "foo.1.2.2": 22})
    effects2 = MockEffects(preload)
    outcome2 = await core._execute_until_blocked_inner(_root("foo"), effects2)

    # Whole tree resolved now.
    assert describe(effects2.cache) == {
        "foo.1.1": ("run", "resolved"),
        "foo.1.1.1": ("run", "resolved"),
        "foo.1.1.2": ("rpc", "resolved"),
        "foo.1.1.3": ("run", "resolved"),
        "foo.1.2": ("run", "resolved"),
        "foo.1.2.1": ("run", "resolved"),
        "foo.1.2.2": ("rpc", "resolved"),
        "foo.1.2.3": ("run", "resolved"),
    }
    assert isinstance(outcome2, _ExecFulfilled)
    assert outcome2.state == "resolved"

    # foo and bar replayed (pending promises); baz did NOT (already resolved).
    assert calls == {"foo": 2, "bar": 4, "baz": 4}


@pytest.mark.asyncio
async def test_replay_partial_settle_still_suspends_on_remaining() -> None:
    """Only one rpc settled between runs: replay resolves that branch, blocks on the other."""
    calls = {"foo": 0, "bar": 0, "baz": 0}

    async def baz(ctx: Context) -> None:
        calls["baz"] += 1
        await asyncio.sleep(0)

    async def bar(ctx: Context) -> None:
        calls["bar"] += 1
        p1 = ctx.run(baz)
        p2 = ctx.rpc(".")
        p3 = ctx.run(baz)
        await p1
        await p2
        await p3

    async def foo(ctx: Context) -> None:
        calls["foo"] += 1
        p1 = ctx.run(bar)
        p2 = ctx.run(bar)
        await p1
        await p2

    reg = Registry()
    reg.register("foo", foo)
    core = _core(reg)

    effects1 = MockEffects()
    await core._execute_until_blocked_inner(_root("foo"), effects1)

    # Settle only bar #1's rpc; bar #2's rpc is still pending.
    preload = _replayed(effects1.cache, {"foo.1.1.2": 11})
    effects2 = MockEffects(preload)
    outcome2 = await core._execute_until_blocked_inner(_root("foo"), effects2)

    assert describe(effects2.cache) == {
        # bar #1 completed: its promise and rpc are resolved.
        "foo.1.1": ("run", "resolved"),
        "foo.1.1.1": ("run", "resolved"),
        "foo.1.1.2": ("rpc", "resolved"),
        "foo.1.1.3": ("run", "resolved"),
        # bar #2 still blocked on its rpc, so its own promise stays pending.
        "foo.1.2": ("run", "pending"),
        "foo.1.2.1": ("run", "resolved"),
        "foo.1.2.2": ("rpc", "pending"),
        "foo.1.2.3": ("run", "resolved"),
    }
    assert isinstance(outcome2, _ExecSuspended)
    assert outcome2.todos == ["foo.1.2.2"]

    # baz never re-ran across either pass.
    assert calls == {"foo": 2, "bar": 4, "baz": 4}
