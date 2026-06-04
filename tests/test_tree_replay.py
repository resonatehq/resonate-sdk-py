"""Replay-evolution tests for the execution :class:`~resonate.tree.Tree`.

Where :mod:`tests.test_execute_until_blocked` asserts on the *promise cache*
one inner return leaves behind, this module asserts on how the **tree** evolves
across consecutive ``execute_until_blocked_inner`` calls over a *shared* cache
(the Python port of Go's ``tree_replay_test.go``, ``tree.md`` §13).

Both tests reuse one ``MockEffects`` (hence one ``cache``) across runs. They
differ only in whether the cache advances between runs:

* **Unchanged cache** -- the §7 fixed point. Replay 1 prunes the Int subtrees
  that completed in replay 0 (``tree2.is_prune_of(tree1)``); replay 2 reproduces
  the exact shape (``tree3.is_equal(tree2)``).
* **An rpc settles between runs** -- the §6/§8 general relation. Replay 1 both
  prunes (completed Int subtrees collapse) *and* extends (the body runs past the
  now-unblocked await and spawns new nodes): ``tree2.is_prune_and_extension_of
  (tree1)``.

The workflow is the same in both: ``func`` runs an all-local ``child`` (which
spawns a ``grandchild``) to completion -- a settled Int subtree, so pruning is
observable -- then awaits two rpcs *in sequence*, so the second rpc node exists
only once the first has settled (the node that "extends" the tree).
"""

from __future__ import annotations

import random
from typing import TYPE_CHECKING, Any, Literal

import pytest

from resonate.codec import Codec, NoopEncryptor, _encode_error
from resonate.core import Core, identity_target_resolver
from resonate.error import ResonateError
from resonate.registry import Registry
from resonate.types import PromiseCreateReq, PromiseRecord, Value

if TYPE_CHECKING:
    from resonate.context import Context
    from resonate.tree import Tree

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


# ── Helpers ──────────────────────────────────────────────────────────────


def _core(reg: Registry) -> Core:
    """Build a Core whose inner never touches the network (sender/heartbeat unused)."""
    return Core(
        sender=None,
        codec=Codec(NoopEncryptor()),
        registry=reg,
        resolver=identity_target_resolver,
        pid="tree-replay-test",
        ttl=TTL,
    )


def _root(func: str, *, id: str) -> PromiseRecord:
    """Build a pending root promise whose param carries a ``TaskData``."""
    return PromiseRecord(
        id=id,
        state="pending",
        param=Value(data={"func": func, "args": [], "kwargs": {}, "version": 1}),
        timeout_at=FAR_FUTURE,
    )


async def _tree(core: Core, func: str, effects: MockEffects, *, id: str) -> Tree:
    """Run the inner once over ``effects`` and hand back the tree it produced.

    The outcome carries the tree on both arms (``_ExecFulfilled.tree`` /
    ``_ExecSuspended.tree``), so one inner call yields one ``Tree`` snapshot.
    """
    outcome = await core.execute_until_blocked_inner(_root(func, id=id), effects)
    return outcome.tree


def _is_pending_rpc(rec: PromiseRecord) -> bool:
    """Classify a cache record as a still-pending rpc, mirroring ``Context``'s tags.

    An rpc is global-scope, not a timer, carries a (non-null) call param, and ends
    in a numeric segment -- distinguishing it from ``ctx.promise`` (null param)
    and ``ctx.detached`` (hashed segment).
    """
    tags = rec.tags
    return (
        rec.state == "pending"
        and tags.get("resonate:scope") == "global"
        and tags.get("resonate:timer") != "true"
        and rec.param.data is not None
        and rec.id.rsplit(".", 1)[-1].isdigit()
    )


def randomly_settle_rpc(cache: dict[str, PromiseRecord], rng: random.Random) -> str:
    """Settle one randomly-chosen pending rpc in the cache (resolved, null value).

    Models the server (or a remote worker) settling a promise the suspended task
    was blocked on, in place in the shared cache, so the next inner replay sees
    it terminal and can progress past the await. Returns the settled id.
    """
    pending = sorted(r.id for r in cache.values() if _is_pending_rpc(r))
    assert pending, "no pending rpc in the cache to settle"
    chosen = rng.choice(pending)
    rec = cache[chosen]
    cache[chosen] = PromiseRecord(
        id=rec.id,
        state="resolved",
        param=rec.param,
        value=Value(data=None),
        tags=rec.tags,
        timeout_at=rec.timeout_at,
    )
    return chosen


# ── The shared workflow ──────────────────────────────────────────────────


def _register_func(reg: Registry) -> None:
    """Register ``func``, the workflow both tests drive.

    It completes a local child-with-grandchild (a settled Int subtree), then
    awaits two rpcs *in sequence* -- so the second rpc node appears only once the
    first has settled.
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

    reg.register("func", func)


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
    reg = Registry()
    _register_func(reg)
    core = _core(reg)
    effects = MockEffects()  # one shared cache across every iteration

    tree1 = await _tree(core, "func", effects, id="f")
    assert sorted(tree1.ids()) == ["f", "f.1", "f.1.1", "f.2"]

    tree2 = await _tree(core, "func", effects, id="f")
    assert tree2.is_prune_of(tree1)
    assert not tree2.is_equal(tree1)  # strictly fewer nodes -- the grandchild
    assert sorted(tree2.ids()) == ["f", "f.1", "f.2"]

    tree3 = await _tree(core, "func", effects, id="f")
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

    Neither node set contains the other, so ``is_prune_of`` no longer applies;
    ``is_prune_and_extension_of`` is the general §6/§8 replay relation that does.
    """
    reg = Registry()
    _register_func(reg)
    core = _core(reg)
    effects = MockEffects()

    tree1 = await _tree(core, "func", effects, id="f")
    assert sorted(tree1.ids()) == ["f", "f.1", "f.1.1", "f.2"]

    randomly_settle_rpc(effects.cache, random.Random())

    tree2 = await _tree(core, "func", effects, id="f")
    assert tree2.is_prune_and_extension_of(tree1)
    # Prune (f.1.1 gone) AND extend (f.3 is new) -- neither contains the other.
    assert sorted(tree2.ids()) == ["f", "f.1", "f.2", "f.3"]
    assert not tree2.is_prune_of(tree1)  # f.3 breaks containment
