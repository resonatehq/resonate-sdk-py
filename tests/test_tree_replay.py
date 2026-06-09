"""Replay-evolution tests for the execution :class:`~resonate.tree.Tree`.

Where :mod:`tests.test_execute_until_blocked` asserts on the *promise cache*
one inner return leaves behind, this module asserts on how the **tree** evolves
across consecutive runs of the same body -- the Python port of Go's
``tree_replay_test.go`` (``tree.md`` §13), grown into the formalization of
``tree.md`` §6-§9. Three layers:

* **Replay relations over a shared cache** -- consecutive
  ``execute_until_blocked_inner`` calls over one ``MockEffects``. An unchanged
  cache prunes to the §7 fixed point (``is_prune_of`` then ``is_equal``); a
  cache the external world advanced between runs extends past the settled await
  (``is_extension_of``, §8). A run that does both at once satisfies neither atom
  directly -- it factors into a prune followed by an extension.
* **Per-op replay semantics** -- what each durable ``Context`` op contributes
  to the tree across replays: ``run`` (Int: settles under our lease, prunes
  once settled), ``rpc`` / ``sleep`` / ``promise`` (Ext: the frontier, settled
  by another worker / the server's timer / an external ``promise.settle``),
  and ``detached`` (Det: exempt from the whole contract). The §9 phased
  workflow asserts the §8 frontier-evolution rule at every step.
* **Through the full SDK** -- the same suspend -> settle -> replay cycles
  driven through the public :class:`~resonate.resonate.Resonate` API against
  the in-process ``LocalNetwork``. The tree is internal there, but ``Core``
  asserts ``tree.well_formed`` on every inner return, so these resolve only
  if real replays keep the contract.

The inner-level tests reuse one ``MockEffects`` (hence one ``cache``) across
runs; ``_settle_external`` models the world advancing between two runs -- the
§7 "disturbance" that moves the system to a new fixed point.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Literal

import pytest

from resonate.codec import Codec, NoopEncryptor, _encode_error
from resonate.core import (
    Core,
    _ExecFulfilled,
    _ExecSuspended,
    identity_target_resolver,
)
from resonate.error import ResonateError, ServerError
from resonate.registry import Registry
from resonate.resonate import Resonate
from resonate.retry import Never
from resonate.types import PromiseCreateReq, PromiseRecord, Value

if TYPE_CHECKING:
    from resonate.context import Context

# Far-future deadline, matching tests.test_core (Go's ``int64(1) << 50``).
FAR_FUTURE = 1 << 50
TTL = 10_000


# ── Dict-backed mock for Effects (self-contained; this file stands alone) ────


class MockEffects:
    """A dict-backed stand-in for :class:`~resonate.effects.Effects`.

    Holds the execution state as a plain ``{id: PromiseRecord}`` map: no Sender,
    Codec, or network. ``create_promise`` records a fresh ``pending`` node or
    returns the cached one (idempotent, like the real Effects); ``settle_promise``
    flips an existing node terminal. Reusing one instance across inner calls is
    exactly the "same body, same cache" replay scenario.
    """

    def __init__(self) -> None:
        self.cache: dict[str, PromiseRecord] = {}

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


# ── Shared setup ─────────────────────────────────────────────────────────────


def _setup(reg: Registry) -> tuple[Core, MockEffects, PromiseRecord]:
    """Build the pieces every test drives the inner with.

    A Core whose inner never touches the network (sender/heartbeat unused), a
    fresh ``MockEffects`` (one shared cache across every iteration *within* a
    test), and a pending root promise whose param carries a ``TaskData``.
    """
    core = Core(
        sender=None,
        codec=Codec(NoopEncryptor()),
        registry=reg,
        resolver=identity_target_resolver,
        pid="tree-replay-test",
        ttl=TTL,
    )
    effects = MockEffects()
    root = PromiseRecord(
        id="f",
        state="pending",
        param=Value(data={"func": "func", "args": [], "kwargs": {}, "version": 1}),
        timeout_at=FAR_FUTURE,
    )
    return core, effects, root


def _settle_external(effects: MockEffects, id: str, data: Any = None) -> None:
    """Settle ``id`` in place in the shared cache, resolved with ``data``.

    Models the external world advancing between two inner runs -- a remote
    worker fulfilling an rpc child, the server's timer firing a sleep, or an
    external ``promise.settle`` -- the §7 "disturbance" that moves the system
    to a new fixed point. Asserts the promise is still pending so a test
    cannot accidentally re-settle (Kind is monotonic).
    """
    rec = effects.cache[id]
    assert rec.state == "pending", f"{id} is already settled"
    effects.cache[id] = PromiseRecord(
        id=rec.id,
        state="resolved",
        param=rec.param,
        value=Value(data=data),
        tags=rec.tags,
        timeout_at=rec.timeout_at,
    )


# ── Test 1: unchanged cache -- prune to a fixed point ────────────────────────


@pytest.mark.asyncio
async def test_replay_over_unchanged_cache_prunes_then_stabilizes() -> None:
    """Same body, same cache: replay prunes the completed Int subtree, then stabilizes.

    ``tree1`` (iteration 0) is the full tree, suspended on rpc ``a``::

        f (int, pending)
        ├── f.1   (int, settled)     child -- ran to completion
        │   └── f.1.1 (int, settled) grandchild
        └── f.2   (ext, pending)     rpc "a" -- the live dependency

    ``tree2`` (iteration 1) finds ``f.1`` settled, skips child's body, and never
    re-adds the grandchild -- a strict prune (``f.1.1`` gone). ``tree3``
    (iteration 2) has nothing left to prune, so it equals ``tree2`` -- the §7
    fixed point ``inner(inner(X)) = inner(X)``.
    """

    def grandchild(ctx: Context) -> int:
        return 1

    async def child(ctx: Context) -> int:
        await ctx.run(grandchild)
        return 2

    async def func(ctx: Context) -> None:
        await ctx.run(child)
        await ctx.rpc("a")
        await ctx.rpc("b")

    reg = Registry()
    reg.register("func", func)
    core, effects, root = _setup(reg)

    # The outcome carries the tree on both arms (``_ExecFulfilled.tree`` /
    # ``_ExecSuspended.tree``), so one inner call yields one ``Tree`` snapshot.
    outcome1 = await core.execute_until_blocked_inner(root, effects)
    tree1 = outcome1.tree
    assert sorted(tree1.ids()) == ["f", "f.1", "f.1.1", "f.2"]

    outcome2 = await core.execute_until_blocked_inner(root, effects)
    tree2 = outcome2.tree
    assert tree2.is_prune_of(tree1)
    assert not tree2.is_equal(tree1)  # strictly fewer nodes -- the grandchild
    assert sorted(tree2.ids()) == ["f", "f.1", "f.2"]

    outcome3 = await core.execute_until_blocked_inner(root, effects)
    tree3 = outcome3.tree
    assert tree3.is_equal(tree2)


# ── Test 2: an rpc settles between runs -- prune AND extend ──────────────────


@pytest.mark.asyncio
async def test_replay_after_settling_rpc_prunes_and_extends() -> None:
    """Settle the blocking rpc between runs: replay prunes *and* extends.

    ``tree1`` suspends on rpc ``a`` (``f.2``); ``ctx.rpc("b")`` is never reached,
    so ``f.3`` does not yet exist. After ``a`` settles in the shared cache,
    ``tree2``:

    * **prunes** ``f.1.1`` -- child is settled, its body is skipped; and
    * **extends** with ``f.3`` -- the body runs past ``await a`` and spawns
      rpc ``b``, a node ``tree1`` never had.

    Neither node set contains the other, so neither ``is_prune_of`` nor
    ``is_extension_of`` holds directly -- the replay factors into a prune of
    ``tree1`` followed by an extension (``tree.md`` §6/§8).
    """

    def grandchild(ctx: Context) -> int:
        return 1

    async def child(ctx: Context) -> int:
        await ctx.run(grandchild)
        return 2

    async def func(ctx: Context) -> None:
        await ctx.run(child)
        await ctx.rpc("a")
        await ctx.rpc("b")

    reg = Registry()
    reg.register("func", func)
    core, effects, root = _setup(reg)

    outcome1 = await core.execute_until_blocked_inner(root, effects)
    tree1 = outcome1.tree
    assert sorted(tree1.ids()) == ["f", "f.1", "f.1.1", "f.2"]

    # Settle rpc "a" (``f.2``) in place in the shared cache -- modeling the
    # server (or a remote worker) settling the promise the suspended task was
    # blocked on -- so the next inner replay sees it terminal and progresses
    # past the await.
    _settle_external(effects, "f.2")

    outcome2 = await core.execute_until_blocked_inner(root, effects)
    tree2 = outcome2.tree
    # Prune (f.1.1 gone) AND extend (f.3 is new) -- neither contains the other,
    # so neither atom holds directly; it factors into a prune then an extension
    # (see test_tree.test_mixed_replay_factors_into_prune_then_extension).
    assert sorted(tree2.ids()) == ["f", "f.1", "f.2", "f.3"]
    assert not tree2.is_prune_of(tree1)  # f.3 breaks containment
    assert not tree2.is_extension_of(tree1)  # f.1.1 was pruned away


# ── Test 3: sleep -- an Ext node settled by the server's timer ───────────────


@pytest.mark.asyncio
async def test_replay_after_timer_fires_extends_past_sleep() -> None:
    """``ctx.sleep`` contributes an Ext node whose settler is the server's timer.

    Iteration 0 suspends on the timer promise ``f.1`` -- created with the
    ``resonate:timer`` tag, which is what tells the server to *resolve* it at
    its timeout rather than reject it (the §2 "settled by something we await"
    column, timer row). The test plays the timer firing by settling ``f.1``;
    the replay then extends past the sleep and suspends on the rpc -- a pure
    §8 extension with nothing to prune.
    """

    async def func(ctx: Context) -> None:
        await ctx.sleep(timedelta(seconds=30))
        await ctx.rpc("a")

    reg = Registry()
    reg.register("func", func)
    core, effects, root = _setup(reg)

    outcome1 = await core.execute_until_blocked_inner(root, effects)
    assert isinstance(outcome1, _ExecSuspended)
    tree1 = outcome1.tree
    assert sorted(tree1.ids()) == ["f", "f.1"]
    node = tree1.get("f.1")
    assert node is not None
    assert (node.type, node.kind) == ("ext", "pending")
    assert tree1.frontier() == ["f.1"]
    assert outcome1.todos == ["f.1"]
    # The timer tag is the only thing distinguishing a sleep from a bare
    # promise on the wire -- pin it on the durable record the body created.
    assert effects.cache["f.1"].tags.get("resonate:timer") == "true"

    # The timer fires: the server resolves the promise at its timeout.
    _settle_external(effects, "f.1")

    outcome2 = await core.execute_until_blocked_inner(root, effects)
    assert isinstance(outcome2, _ExecSuspended)
    tree2 = outcome2.tree
    assert sorted(tree2.ids()) == ["f", "f.1", "f.2"]
    assert tree2.is_extension_of(tree1)
    assert not tree2.is_prune_of(tree1)  # f.2 is new -- pure extension
    node = tree2.get("f.1")
    assert node is not None
    assert node.kind == "settled"
    assert tree2.frontier() == ["f.2"]


# ── Test 4: promise -- a bare Ext node settled by an external caller ─────────


@pytest.mark.asyncio
async def test_replay_after_external_promise_settle_runs_to_done() -> None:
    """``ctx.promise`` contributes a bare Ext node only ``promise.settle`` advances.

    The created record carries no ``TaskData`` and no timer tag -- nothing will
    ever run it or fire it; only an external settle moves it. Once settled,
    the replay reads the value through the idempotent-recovery path and the
    body runs to done: same node set, kinds advanced -- a prune with nothing
    pruned (``is_prune_of`` via kind monotonicity alone) and an empty frontier
    (D1).
    """

    async def func(ctx: Context) -> Any:
        return await ctx.promise()

    reg = Registry()
    reg.register("func", func)
    core, effects, root = _setup(reg)

    outcome1 = await core.execute_until_blocked_inner(root, effects)
    assert isinstance(outcome1, _ExecSuspended)
    tree1 = outcome1.tree
    assert sorted(tree1.ids()) == ["f", "f.1"]
    assert tree1.frontier() == ["f.1"]
    # Bare promise: no function payload, no timer tag -- distinguishes it from
    # the rpc and sleep records sharing the Ext column.
    rec = effects.cache["f.1"]
    assert rec.param.data is None
    assert "resonate:timer" not in rec.tags

    _settle_external(effects, "f.1", data=42)

    outcome2 = await core.execute_until_blocked_inner(root, effects)
    assert isinstance(outcome2, _ExecFulfilled)
    assert outcome2.state == "resolved"
    tree2 = outcome2.tree
    assert tree2.is_prune_of(tree1)
    node = tree2.get("f.1")
    assert node is not None
    assert node.kind == "settled"
    assert tree2.frontier() == []


# ── Test 5: detached -- a Det node exempt from the whole contract ────────────


@pytest.mark.asyncio
async def test_detached_subtree_is_exempt_from_replay_and_done_contract() -> None:
    """``ctx.detached`` contributes a Det node outside this workflow's contract.

    Three properties pinned across three iterations:

    * the Det node never enters the frontier, so a pending detached child never
      holds the parent suspended (only the rpc ``f.2`` does);
    * replay over an unchanged cache is the exact §7 fixed point *with* the
      pending Det node in place -- nothing prunes, nothing extends;
    * once the rpc settles, the workflow is **done while the Det child is
      still pending** -- Det is exempt from D1/U3, so the fulfill is legal.
    """

    async def func(ctx: Context) -> None:
        await ctx.detached("side")
        await ctx.rpc("a")

    reg = Registry()
    reg.register("func", func)
    core, effects, root = _setup(reg)

    outcome1 = await core.execute_until_blocked_inner(root, effects)
    assert isinstance(outcome1, _ExecSuspended)
    tree1 = outcome1.tree
    root_node = tree1.get("f")
    assert root_node is not None
    det_id, rpc_id = root_node.children
    # Bounded detached id: one ``d``-marked 16-hex segment past the prefix.
    assert det_id.startswith("f.d")
    assert len(det_id) == len("f.d") + 16
    assert rpc_id == "f.2"
    det = tree1.get(det_id)
    assert det is not None
    assert (det.type, det.kind) == ("det", "pending")
    # The pending Det node is invisible to the suspension contract.
    assert tree1.frontier() == ["f.2"]
    assert outcome1.todos == ["f.2"]

    # Unchanged cache: the fixed point holds with the Det node still pending.
    outcome2 = await core.execute_until_blocked_inner(root, effects)
    assert outcome2.tree.is_equal(tree1)

    # The rpc settles; the detached child is *not* settled -- done regardless.
    _settle_external(effects, "f.2")

    outcome3 = await core.execute_until_blocked_inner(root, effects)
    assert isinstance(outcome3, _ExecFulfilled)
    assert outcome3.state == "resolved"
    tree3 = outcome3.tree
    assert tree3.is_prune_of(tree1)
    det = tree3.get(det_id)
    assert det is not None
    assert det.kind == "pending"  # still in flight
    assert effects.cache[det_id].state == "pending"  # durably, too
    assert tree3.frontier() == []  # D1 holds: Det subtrees are skipped


# ── Test 6: fan-out -- unawaited siblings, the S4 bridge ─────────────────────


@pytest.mark.asyncio
async def test_fanout_settling_unawaited_sibling_shrinks_frontier() -> None:
    """Three rpcs spawned, one awaited: the todos/frontier relation under fan-out.

    The body suspends on ``a`` having also created ``b`` and ``c`` it never
    reaches an await on. In this port every created pending remote registers
    its todo deterministically (``flush_local_work`` joins the background
    await tasks -- the S4 bridge), so ``todos`` covers the unawaited siblings
    too and S4 (``todos ⊆ frontier``) holds at equality -- unlike Go, where
    ``todos`` is the strictly-awaited subset.

    Settling the *unawaited* sibling ``b`` between runs cannot unblock the
    body -- the replay still suspends on ``a`` -- but ``b`` leaves the
    frontier: the §8 evolution rule with an empty new-spawn set.
    """

    async def func(ctx: Context) -> None:
        a = ctx.rpc("a")
        ctx.rpc("b")
        ctx.rpc("c")
        await a

    reg = Registry()
    reg.register("func", func)
    core, effects, root = _setup(reg)

    outcome1 = await core.execute_until_blocked_inner(root, effects)
    assert isinstance(outcome1, _ExecSuspended)
    tree1 = outcome1.tree
    frontier1 = tree1.frontier()
    assert frontier1 == ["f.1", "f.2", "f.3"]
    assert sorted(outcome1.todos) == ["f.1", "f.2", "f.3"]
    assert set(outcome1.todos) <= set(frontier1)  # S4, here at equality

    # Settle the sibling the body never awaits.
    _settle_external(effects, "f.2")

    outcome2 = await core.execute_until_blocked_inner(root, effects)
    assert isinstance(outcome2, _ExecSuspended)
    tree2 = outcome2.tree
    assert tree2.is_prune_of(tree1)  # same nodes, f.2's kind advanced
    node = tree2.get("f.2")
    assert node is not None
    assert node.kind == "settled"
    frontier2 = tree2.frontier()
    assert frontier2 == ["f.1", "f.3"]
    assert sorted(outcome2.todos) == ["f.1", "f.3"]
    # §8: frontier2 ⊆ (frontier_1 - resolved) | new_ext_spawns, with no spawns.
    assert set(frontier2) <= set(frontier1) - {"f.2"}


# ── Test 7: suspended-local -- an Int node kept alive by its Ext descendant ──


@pytest.mark.asyncio
async def test_suspended_local_child_settles_then_prunes_across_replays() -> None:
    """An Int child suspended on its own rpc: alive via U3, settled on replay, pruned next.

    Iteration 0::

        f (int, pending)
        └── f.1   (int, pending)     child -- suspended, not completed
            └── f.1.1 (ext, pending) the rpc keeping f.1 alive (U3)

    ``f.1`` is the *suspended local* case of §4: its durable promise stays
    pending and its remote todo folds up into the parent, so the workflow
    suspends on ``f.1.1`` alone. After the rpc settles, replay 1 re-runs the
    child body to completion -- ``f.1`` settles durably under our lease and
    the workflow fulfills (kinds advance, nothing pruned yet). Replay 2 then
    finds ``f.1`` settled and skips the body entirely, pruning ``f.1.1``;
    replay 3 is the §7 fixed point on the done path.
    """

    async def child(ctx: Context) -> int:
        await ctx.rpc("a")
        return 2

    async def func(ctx: Context) -> int:
        v = await ctx.run(child)
        return v + 1

    reg = Registry()
    reg.register("func", func)
    core, effects, root = _setup(reg)

    outcome1 = await core.execute_until_blocked_inner(root, effects)
    assert isinstance(outcome1, _ExecSuspended)
    tree1 = outcome1.tree
    assert sorted(tree1.ids()) == ["f", "f.1", "f.1.1"]
    node = tree1.get("f.1")
    assert node is not None
    assert (node.type, node.kind) == ("int", "pending")
    assert effects.cache["f.1"].state == "pending"  # suspended, not settled
    assert tree1.frontier() == ["f.1.1"]
    assert outcome1.todos == ["f.1.1"]  # the child's todo, folded up

    _settle_external(effects, "f.1.1")

    # Replay 1: the child body re-runs past the await, returns 2, and settles
    # its own promise under our lease -- the workflow fulfills.
    outcome2 = await core.execute_until_blocked_inner(root, effects)
    assert isinstance(outcome2, _ExecFulfilled)
    assert outcome2.state == "resolved"
    tree2 = outcome2.tree
    assert sorted(tree2.ids()) == ["f", "f.1", "f.1.1"]
    assert tree2.is_prune_of(tree1)  # kinds advanced, nothing pruned yet
    assert effects.cache["f.1"].state == "resolved"
    assert effects.cache["f.1"].value.data == 2

    # Replay 2: ``ctx.run`` short-circuits the settled child -- its body never
    # executes, so the rpc node is never re-added. A strict prune.
    outcome3 = await core.execute_until_blocked_inner(root, effects)
    assert isinstance(outcome3, _ExecFulfilled)
    tree3 = outcome3.tree
    assert sorted(tree3.ids()) == ["f", "f.1"]
    assert tree3.is_prune_of(tree2)
    assert not tree3.is_equal(tree2)

    # Replay 3: nothing left to prune -- the fixed point on the done path.
    outcome4 = await core.execute_until_blocked_inner(root, effects)
    assert outcome4.tree.is_equal(tree3)


# ── Test 8: the §9 phased workflow under the §8 frontier-evolution rule ──────


@pytest.mark.asyncio
async def test_phased_settlements_follow_frontier_evolution_rule() -> None:
    """Drive ``tree.md`` §9's phased workflow, asserting §8 at every step.

    The strongest cross-iteration invariant::

        frontier_{n+1} ⊆ (frontier_n - resolved_n) | new_ext_spawns_n

    Each settlement between runs is the ``resolved_n`` set; the ids appearing
    in the new tree but not the old are ``new_ext_spawns_n``. The sequence
    reproduces §9 exactly: suspend on ``a``; settle ``a`` -> spawn ``b``/``c``,
    suspend on ``b``; settle ``c`` (out of await order) -> frontier shrinks
    with no new spawns; settle ``b`` -> done, empty frontier, and the final
    tree is §9's closing diagram verbatim.
    """

    async def func(ctx: Context) -> int:
        await ctx.rpc("a")
        b = ctx.rpc("b")
        c = ctx.rpc("c")
        await b
        await c
        return 0

    reg = Registry()
    reg.register("func", func)
    core, effects, root = _setup(reg)

    # Iteration 0 -- empty cache: create rpc "a", suspend on it.
    outcome0 = await core.execute_until_blocked_inner(root, effects)
    assert isinstance(outcome0, _ExecSuspended)
    tree0 = outcome0.tree
    frontier0 = tree0.frontier()
    assert sorted(tree0.ids()) == ["f", "f.1"]
    assert frontier0 == ["f.1"]

    # External: settle "a". Iteration 1 -- past the await, spawns "b" and "c",
    # suspends on "b" ("c" is created but its await is never reached).
    _settle_external(effects, "f.1")
    outcome1 = await core.execute_until_blocked_inner(root, effects)
    assert isinstance(outcome1, _ExecSuspended)
    tree1 = outcome1.tree
    frontier1 = tree1.frontier()
    assert sorted(tree1.ids()) == ["f", "f.1", "f.2", "f.3"]
    assert frontier1 == ["f.2", "f.3"]
    assert tree1.is_extension_of(tree0)  # f.2, f.3 spawned past the settled await
    new_ext0 = set(tree1.ids()) - set(tree0.ids())
    assert set(frontier1) <= (set(frontier0) - {"f.1"}) | new_ext0  # §8

    # External: settle "c". Iteration 2 -- still blocked on "b"; the frontier
    # shrinks with no new spawns.
    _settle_external(effects, "f.3")
    outcome2 = await core.execute_until_blocked_inner(root, effects)
    assert isinstance(outcome2, _ExecSuspended)
    tree2 = outcome2.tree
    frontier2 = tree2.frontier()
    assert frontier2 == ["f.2"]
    assert tree2.is_prune_of(tree1)  # equal node set, f.3's kind advanced
    new_ext1 = set(tree2.ids()) - set(tree1.ids())
    assert new_ext1 == set()
    assert set(frontier2) <= (set(frontier1) - {"f.3"}) | new_ext1  # §8

    # External: settle "b". Iteration 3 -- the body runs to completion.
    _settle_external(effects, "f.2")
    outcome3 = await core.execute_until_blocked_inner(root, effects)
    assert isinstance(outcome3, _ExecFulfilled)
    assert outcome3.state == "resolved"
    tree3 = outcome3.tree
    assert tree3.frontier() == []  # D1 -- and §8 trivially holds
    assert tree3.is_prune_of(tree2)  # equal node set, f.2's kind advanced
    # §9's closing diagram, verbatim. The root stays pending -- it is settled
    # by ``task.fulfill`` in the outer, never the inner (U1).
    assert tree3.print() == (
        "f (int, pending)\n"
        "├── f.1 (ext, settled)\n"
        "├── f.2 (ext, settled)\n"
        "└── f.3 (ext, settled)"
    )


# ── Through the full SDK: real replays over the in-process LocalNetwork ──────
#
# The tree is internal to Core at this level -- no outcome to inspect -- but
# ``execute_until_blocked_inner`` runs ``tree.well_formed(status, todos)`` on
# every return, so each real suspend -> settle -> replay cycle below resolves
# only if the contract held at every step. These pin that the formalization
# above is the behavior of the public API, not just of the mocked inner.


@pytest.mark.asyncio
async def test_sdk_run_prunes_local_child_and_extends_past_rpc_on_replay() -> None:
    """A real suspend -> settle -> replay cycle through ``Resonate.run``.

    ``parent`` runs a local child (Int, settles under our lease), then awaits
    an rpc dispatched back to this same worker (Ext): the first execution
    suspends, the rpc child executes as its own task, and the unblock
    re-executes the parent -- which prunes the settled local child and extends
    past the await: the same §6/§8 relation the MockEffects tests pin
    directly, here driven by the real server loop.
    """
    r = Resonate.local(retry_policy=Never())
    try:

        async def double(ctx: Context, x: int) -> int:
            return x * 2

        async def parent(ctx: Context, x: int) -> int:
            local = await ctx.run(double, x)
            remote: int = await ctx.rpc("double", local)
            return remote + 1

        r.register(double)
        r.register(parent)
        assert await r.run("tree-sdk-rpc", parent, 4).result() == 17
    finally:
        await r.stop()


@pytest.mark.asyncio
async def test_sdk_rpc_workflow_unblocked_by_external_promise_settle() -> None:
    """Top-level ``rpc`` + ``ctx.promise`` + ``promises.resolve``: §7's disturbance, live.

    The workflow suspends on a bare promise nothing will ever run or fire;
    ``promises.resolve`` is the external settle that disturbs the fixed point,
    and the resulting unblock replays the body to done with the delivered
    value.
    """
    r = Resonate.local(retry_policy=Never())
    try:

        async def waiter(ctx: Context) -> Any:
            return await ctx.promise()

        r.register(waiter)
        handle = r.rpc("tree-sdk-promise", "waiter")

        # The bare promise the body suspends on has the deterministic child id
        # ``{root}.1``; wait for the background dispatch to create it.
        for _ in range(2000):
            try:
                rec = await r.promises.get("tree-sdk-promise.1")
                break
            except ServerError:
                await asyncio.sleep(0.001)
        else:
            msg = "child promise was never created"
            raise AssertionError(msg)
        assert rec.state == "pending"

        await r.promises.resolve("tree-sdk-promise.1", Value(data=42))
        assert await handle.result() == 42
    finally:
        await r.stop()


@pytest.mark.asyncio
async def test_sdk_detached_child_completes_outside_parent_contract() -> None:
    """``ctx.detached`` through the full stack: the parent's Done ignores the Det node.

    The parent fulfills the moment the detached promise is *created* -- D1
    skips Det subtrees, so a pending detached child cannot hold the parent
    open. The child then completes in its own lineage: its promise resolves
    under a fresh ``resonate:origin`` (its own id) while ``resonate:prefix``
    still points at the dispatching root, unchanged.
    """
    r = Resonate.local(retry_policy=Never())
    try:

        async def side(ctx: Context) -> str:
            return "side-done"

        async def parent(ctx: Context) -> str:
            return await ctx.detached("side")

        r.register(side)
        r.register(parent)

        det_id = await r.run("tree-sdk-det", parent).result()
        # Bounded detached id: one ``d``-marked segment past the prefix.
        assert det_id.startswith("tree-sdk-det.d")

        # The detached child runs as its own root; poll it to settlement.
        for _ in range(2000):
            rec = await r.promises.get(det_id)
            if rec.state != "pending":
                break
            await asyncio.sleep(0.001)
        assert rec.state == "resolved"
        assert rec.tags["resonate:origin"] == det_id  # fresh lineage root
        assert rec.tags["resonate:prefix"] == "tree-sdk-det"  # prefix unchanged
    finally:
        await r.stop()
