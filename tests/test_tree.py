"""Assertion-layer tests for :mod:`resonate.tree`.

The execution tree (:class:`~resonate.tree.Tree`) is a pure assertion layer: it
never drives control flow, it only *materializes* the shape an inner return left
behind so :meth:`~resonate.tree.Tree.well_formed` can check that shape against
the suspension contract (``tree.md`` §4). These tests therefore exercise the two
things that matter:

* the **primitives** that build the shape -- ``add_child`` (idempotent),
  ``settle`` (monotonic), ``get`` (defensive copy), ``frontier`` (the
  ``(ext, pending)`` cell, pruning Det subtrees);
* the **predicates** that judge the shape -- ``useful`` (U3) and ``well_formed``
  (U1/U2/U3 universal, D1 done, S1/S4 suspended), with a *valid* and an
  *invalid* state for each rule.

The headline of the suite (per the task) is suspension-time well-formedness: a
suspended outcome is valid iff the frontier is non-empty (S1), the awaited
``todos`` lie within it (S4), and no pending branch is dead (U3). Each of those
gets a passing tree and a tree that trips the corresponding ``AssertionError``.

Some invalid states (a non-Int root, an unreachable node) cannot be reached
through the public API by construction -- those are built by poking
``tree._nodes`` directly, which is exactly what an SDK bug would do to the
materialized view.
"""

from __future__ import annotations

import pytest

from resonate.tree import Node, Tree

# ── primitives ───────────────────────────────────────────────────────────


def test_new_tree_root_is_int_pending() -> None:
    """A fresh tree is just the root, classified (int, pending) -- U1's shape."""
    t = Tree("root")
    assert t.root() == "root"
    root = t.get("root")
    assert root is not None
    assert root.type == "int"
    assert root.kind == "pending"
    assert root.children == []


def test_get_unknown_returns_none() -> None:
    assert Tree("root").get("nope") is None


def test_get_returns_defensive_copy() -> None:
    """Mutating the handle must not leak back into tree state (Go's value copy)."""
    t = Tree("root")
    t.add_child("root", "root.1", "ext")

    handle = t.get("root")
    assert handle is not None
    handle.kind = "settled"
    handle.children.append("forged")

    fresh = t.get("root")
    assert fresh is not None
    assert fresh.kind == "pending"
    assert fresh.children == ["root.1"]


def test_add_child_inserts_and_links_parent() -> None:
    t = Tree("root")
    assert t.add_child("root", "root.1", "ext") is True

    child = t.get("root.1")
    assert child is not None
    assert child.type == "ext"
    assert child.kind == "pending"

    root = t.get("root")
    assert root is not None
    assert root.children == ["root.1"]


def test_add_child_is_idempotent_on_id() -> None:
    """A replay re-walking the same body must not duplicate nodes or re-append."""
    t = Tree("root")
    assert t.add_child("root", "root.1", "ext") is True
    # Second call -- same id, even a different type -- is a no-op returning False.
    assert t.add_child("root", "root.1", "int") is False

    child = t.get("root.1")
    assert child is not None
    assert child.type == "ext"  # type stable: first write wins

    root = t.get("root")
    assert root is not None
    assert root.children == ["root.1"]  # not re-appended


def test_add_child_preserves_insertion_order() -> None:
    """Children are kept in call order -- the children-as-prefix replay property."""
    t = Tree("root")
    for i in (1, 2, 3):
        t.add_child("root", f"root.{i}", "ext")
    root = t.get("root")
    assert root is not None
    assert root.children == ["root.1", "root.2", "root.3"]


def test_add_child_unknown_parent_raises() -> None:
    t = Tree("root")
    with pytest.raises(AssertionError):
        t.add_child("ghost", "root.1", "ext")


def test_settle_flips_pending_to_settled() -> None:
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.settle("root.1")
    child = t.get("root.1")
    assert child is not None
    assert child.kind == "settled"


def test_settle_is_monotonic_and_idempotent() -> None:
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.settle("root.1")
    t.settle("root.1")  # still settled, no error
    child = t.get("root.1")
    assert child is not None
    assert child.kind == "settled"


def test_settle_unknown_is_noop() -> None:
    t = Tree("root")
    t.settle("nope")  # must not raise
    assert t.get("nope") is None


# ── frontier ───────────────────────────────────────────────────────────────


def test_frontier_empty_for_bare_root() -> None:
    assert Tree("root").frontier() == []


def test_frontier_collects_pending_ext_leaf() -> None:
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    assert t.frontier() == ["root.1"]


def test_frontier_skips_settled_ext() -> None:
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.settle("root.1")
    assert t.frontier() == []


def test_frontier_is_depth_first_in_insertion_order() -> None:
    """root.1, root.2, root.3 in call order -- the §9 worked-example shape."""
    t = Tree("root")
    for i in (1, 2, 3):
        t.add_child("root", f"root.{i}", "ext")
    assert t.frontier() == ["root.1", "root.2", "root.3"]


def test_frontier_descends_through_int_parent() -> None:
    """A suspended-local Int parent folds up its child's pending Ext leaf."""
    t = Tree("root")
    t.add_child("root", "root.1", "int")  # ctx.run child, still in flight
    t.add_child("root.1", "root.1.1", "ext")  # the rpc it awaits
    assert t.frontier() == ["root.1.1"]


def test_frontier_prunes_det_subtree() -> None:
    """Det lives in another workflow's tree -- its whole subtree is skipped (§3)."""
    t = Tree("root")
    t.add_child("root", "d", "det")
    t.add_child("d", "d.1", "ext")  # would be a frontier leaf if not under Det
    assert t.frontier() == []


def test_frontier_pending_ext_prunes_own_subtree() -> None:
    """An (ext, pending) node is collected, then its subtree is not descended."""
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.add_child("root.1", "root.1.1", "ext")  # below a pending Ext -> not reached
    assert t.frontier() == ["root.1"]


# ── useful() / U3 ────────────────────────────────────────────────────────────


def test_useful_passes_when_all_settled() -> None:
    """Done shape: every non-root node settled satisfies U3 via the Settled disjunct."""
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.settle("root.1")
    t.useful()  # no raise


def test_useful_passes_with_pending_ext_descendant() -> None:
    """Suspended-local: an Int parent kept alive by a pending Ext descendant."""
    t = Tree("root")
    t.add_child("root", "root.1", "int")
    t.add_child("root.1", "root.1.1", "ext")
    t.useful()  # no raise


def test_useful_flags_dead_int_pending_leaf() -> None:
    """An Int leaf still pending with nothing below it -- U3's canonical violation."""
    t = Tree("root")
    t.add_child("root", "root.1", "int")  # never settled, no children
    with pytest.raises(AssertionError):
        t.useful()


def test_useful_flags_int_pending_with_only_det_descendant() -> None:
    """A detached child keeps nothing pending in our contract -> parent is dead."""
    t = Tree("root")
    t.add_child("root", "root.1", "int")
    t.add_child("root.1", "root.1.d", "det")
    with pytest.raises(AssertionError):
        t.useful()


def test_useful_names_every_dead_branch() -> None:
    t = Tree("root")
    t.add_child("root", "root.1", "int")
    t.add_child("root", "root.2", "int")
    with pytest.raises(AssertionError) as exc:
        t.useful()
    msg = str(exc.value)
    assert "root.1" in msg
    assert "root.2" in msg


# ── well_formed: universal rules (U1/U2/U3) ──────────────────────────────────


def test_well_formed_u1_root_must_be_int() -> None:
    t = Tree("root")
    t._nodes["root"].type = "ext"  # only an SDK bug could do this
    with pytest.raises(AssertionError):
        t.well_formed("done", [])


def test_well_formed_u1_root_must_be_pending() -> None:
    """The inner must never settle the root -- task.fulfill in the outer owns it."""
    t = Tree("root")
    t.settle("root")
    with pytest.raises(AssertionError):
        t.well_formed("done", [])


def test_well_formed_u2_flags_unreachable_node() -> None:
    t = Tree("root")
    t._nodes["orphan"] = Node(id="orphan", type="ext")  # not linked under root
    with pytest.raises(AssertionError):
        t.well_formed("suspended", [])


def test_well_formed_u3_flags_dead_branch() -> None:
    t = Tree("root")
    t.add_child("root", "root.1", "int")  # dead pending Int leaf
    with pytest.raises(AssertionError):
        t.well_formed("suspended", [])


# ── well_formed: done state (D1) ─────────────────────────────────────────────


def test_well_formed_done_valid_when_frontier_empty() -> None:
    t = Tree("root")
    t.add_child("root", "root.1", "int")
    t.settle("root.1")
    t.well_formed("done", [])  # no raise


def test_well_formed_done_rejects_non_empty_frontier() -> None:
    """A done outcome cannot still have a live remote dependency."""
    t = Tree("root")
    t.add_child("root", "root.1", "ext")  # pending -> frontier non-empty
    with pytest.raises(AssertionError):
        t.well_formed("done", [])


# ── well_formed: suspended state (S1/S4) -- valid and invalid ────────────────


def test_well_formed_suspended_valid() -> None:
    """The canonical valid suspension: a non-empty frontier, todos within it."""
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.add_child("root", "root.2", "ext")
    t.well_formed("suspended", ["root.1"])  # awaited subset of frontier


def test_well_formed_suspended_s1_rejects_empty_frontier() -> None:
    """Suspending with nothing pending remotely is a contract violation."""
    t = Tree("root")
    t.add_child("root", "root.1", "int")
    t.settle("root.1")  # frontier now empty
    with pytest.raises(AssertionError):
        t.well_formed("suspended", [])


def test_well_formed_suspended_s4_rejects_todo_outside_frontier() -> None:
    """An awaited id that is not a live frontier leaf -- todos must be a subset."""
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.add_child("root", "root.2", "ext")
    t.settle("root.2")  # root.2 leaves the frontier...
    with pytest.raises(AssertionError):
        t.well_formed("suspended", ["root.2"])  # ...but is still awaited


def test_well_formed_suspended_s4_holds_for_full_frontier_subset() -> None:
    """Todos == frontier is the trivially-valid S4 boundary."""
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.add_child("root", "root.2", "ext")
    t.well_formed("suspended", ["root.1", "root.2"])


def test_well_formed_suspended_s4_holds_for_empty_todos() -> None:
    """A non-empty frontier with no explicitly awaited todo still satisfies S1+S4."""
    t = Tree("root")
    t.add_child("root", "root.1", "ext")
    t.well_formed("suspended", [])


# ── worked example: the phased workflow under replay (tree.md §9) ────────────


def test_phased_workflow_replay_sequence() -> None:
    """Walk tree.md §9 end to end: three suspended fixed points, then done.

    Body: rpc(a); await a; rpc(b); rpc(c); await b; return await c. Each iteration
    rebuilds the tree against a richer cache and must be well-formed for the
    outcome it reports -- suspended while any leaf is pending, done once all settle.
    """
    # Iteration 0 -- cache empty: body creates root.1, awaits it, suspends.
    t0 = Tree("root")
    t0.add_child("root", "root.1", "ext")
    assert t0.frontier() == ["root.1"]
    t0.well_formed("suspended", ["root.1"])

    # External settles root.1. Iteration 1 -- a returns, b and c spawn, await b.
    t1 = Tree("root")
    t1.add_child("root", "root.1", "ext")
    t1.settle("root.1")
    t1.add_child("root", "root.2", "ext")
    t1.add_child("root", "root.3", "ext")
    assert t1.frontier() == ["root.2", "root.3"]
    t1.well_formed("suspended", ["root.2"])  # only b is awaited

    # External settles root.3. Iteration 2 -- still blocked on b.
    t2 = Tree("root")
    t2.add_child("root", "root.1", "ext")
    t2.settle("root.1")
    t2.add_child("root", "root.2", "ext")
    t2.add_child("root", "root.3", "ext")
    t2.settle("root.3")
    assert t2.frontier() == ["root.2"]
    t2.well_formed("suspended", ["root.2"])

    # External settles root.2. Iteration 3 -- body runs to completion, done.
    t3 = Tree("root")
    t3.add_child("root", "root.1", "ext")
    t3.add_child("root", "root.2", "ext")
    t3.add_child("root", "root.3", "ext")
    for leaf in ("root.1", "root.2", "root.3"):
        t3.settle(leaf)
    assert t3.frontier() == []
    t3.well_formed("done", [])
