"""The execution tree -- the in-memory model of one workflow attempt's call graph.

This is the Python port of the model specified in ``tree.md`` (Go's ``tree.go``).
It is an **assertion-only** structure: the runtime never reads it to make a
control-flow decision (the outer drives off ``Context.spawned_remote``); instead
it materializes what ``execute_until_blocked_inner`` produced -- the set of
promises the body created, who settles each, and which are still pending -- so
that :meth:`Journal.well_formed` can assert the worker's behavior matches the
suspension contract on every inner return.

Naming: the spec calls this the *Tree*; here the class is :class:`Journal` (the
SDK's name for the per-attempt call-graph model). Terminology otherwise tracks
``tree.md``: a node's :data:`NodeType` (who settles it) and :data:`NodeKind`
(whether it has settled) form the 2x3 product whose ``(ext, pending)`` cell is
the suspension :meth:`~Journal.frontier`.

Two deliberate divergences from Go, both per the porting conventions
(see the ``py-mirrors-rust-sdk`` / ``execution-tree-port`` notes):

* **No mutex.** Go guards ``t.nodes`` with a ``sync.Mutex`` because spawned-local
  goroutines run in parallel. Here every writer is an asyncio task on a single
  event loop, and no public method awaits between read and write, so the map and
  the per-node child slice cannot interleave -- the lock would add nothing.
* **Predicates raise.** Go's ``Useful()``/``WellFormed()`` return ``error``;
  following the repo-wide convention (codec.py / core.py) the equivalents here
  return ``None`` and raise :class:`AssertionError` with a multi-line message on
  violation. The tree is a pure assertion layer, so a failed check is a bug in
  the SDK, not a recoverable condition.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from resonate.types import Status

#: Who is responsible for settling a node's durable promise. Assigned at the
#: call site and never changes (``tree.md`` Type, §2):
#:
#: * ``"int"`` -- internal, created by ``ctx.run``; this worker settles it under
#:   our task lease when the local executor returns.
#: * ``"ext"`` -- external, created by ``ctx.rpc`` / ``ctx.sleep`` /
#:   ``ctx.promise``; settled by something we await (another worker, the
#:   server's timer, an external ``promise.settle`` caller).
#: * ``"det"`` -- detached, created by ``ctx.detached``; fire-and-forget, outside
#:   this workflow's contract. Det subtrees are exempt from every rule and
#:   skipped by the frontier walk.
NodeType = Literal["int", "ext", "det"]

#: Whether a node's durable promise has reached a terminal state. The five-state
#: durable lattice (``pending | resolved | rejected | rejected_canceled |
#: rejected_timedout``) collapses to this single bit -- the success/failure
#: distinction matters at ``Future.Await`` time, not for the structural
#: suspension contract. Transitions monotonically: ``pending`` -> ``settled``
#: only (``tree.md`` Kind, §2).
NodeKind = Literal["pending", "settled"]


@dataclass
class Node:
    """One promise in the call graph.

    Mutable (``kind`` flips ``pending`` -> ``settled``; ``children`` grows as the
    body spawns), so a plain ``@dataclass`` rather than a frozen ``msgspec.Struct``
    -- msgspec is reserved for immutable / wire types. ``children`` holds child
    IDs in insertion (== call) order, which is what makes the children-as-prefix
    replay property and :meth:`Journal.print` deterministic.
    """

    id: str
    type: NodeType
    kind: NodeKind = "pending"
    children: list[str] = field(default_factory=list)


class Journal:
    """The execution tree for one workflow attempt.

    Built incrementally as the body runs (``add_child`` per spawn, ``settle`` as
    promises terminate), then asserted at the inner boundary. The root is always
    ``(int, pending)`` -- it is settled by ``task.fulfill`` in the outer, never by
    the inner (invariant U1).
    """

    def __init__(self, root_id: str) -> None:
        self._root = root_id
        self._nodes: dict[str, Node] = {root_id: Node(id=root_id, type="int")}

    # ── inspection ──────────────────────────────────────────────────

    def root(self) -> str:
        """Return the root node's ID."""
        return self._root

    def get(self, id: str) -> Node | None:
        """Return a defensive copy of the node, or ``None`` if absent.

        Returns a copy so callers cannot mutate tree state through the handle
        (mirrors Go's ``Get`` returning a value copy with a fresh ``Children``
        slice).
        """
        node = self._nodes.get(id)
        if node is None:
            return None
        return Node(
            id=node.id, type=node.type, kind=node.kind, children=list(node.children)
        )

    def add_child(self, parent: str, id: str, type: NodeType) -> bool:
        """Attach a child of ``type`` under ``parent``. Idempotent on ``id``.

        Returns ``True`` if a node was inserted, ``False`` if ``id`` already
        existed (so a replay that re-walks the same body does not duplicate
        nodes or re-append to the parent's child list). The parent must already
        be in the tree.
        """
        assert parent in self._nodes, f"unknown parent {parent!r}"
        if id in self._nodes:
            return False
        self._nodes[id] = Node(id=id, type=type)
        self._nodes[parent].children.append(id)
        return True

    def settle(self, id: str) -> None:
        """Mark ``id`` settled. Monotonic; a no-op if unknown or already settled."""
        node = self._nodes.get(id)
        if node is not None:
            node.kind = "settled"

    # ── frontier ────────────────────────────────────────────────────

    def frontier(self) -> list[str]:
        """Return the ``(ext, pending)`` node IDs, skipping Det subtrees (§3).

        These are the workflow's live remote dependencies -- the promises whose
        settlement will unblock further progress. A depth-first walk in child
        insertion order: a Det node prunes its whole subtree (it lives in another
        workflow's tree); an ``(ext, pending)`` node is collected and its subtree
        pruned; everything else descends.

        Note this is a **superset** of ``outcome.todos`` (the awaited subset the
        outer registers callbacks for): the frontier is *every* pending remote
        leaf, including ones the body created but never reached an ``await`` on.
        Invariant S4 (``todos subset frontier``) connects the two.
        """
        out: list[str] = []
        self._frontier_walk(self._root, out)
        return out

    def _frontier_walk(self, id: str, out: list[str]) -> None:
        node = self._nodes[id]
        if node.type == "det":
            return
        if node.type == "ext" and node.kind == "pending":
            out.append(id)
            return
        for child in node.children:
            self._frontier_walk(child, out)

    # ── predicates ──────────────────────────────────────────────────

    def useful(self) -> None:
        """Assert U3: no dead pending branches (``tree.md`` §4).

        Every non-root, non-Det node must be either ``settled`` OR have at least
        one ``(ext, pending)`` node in its subtree (itself included). A node
        failing this is *pending with no path to further progress* -- it should
        already have settled. Raises :class:`AssertionError` listing every dead
        branch.

        U3 subsumes the older S2/S3 (no Int-pending leaf, every Int-pending has
        an Ext-pending descendant): a suspended-local Int node is kept alive only
        by the Ext-pending descendant its child folded up.
        """
        dead = [
            id
            for id, node in self._nodes.items()
            if id != self._root
            and node.type != "det"
            and node.kind != "settled"
            and not self._has_pending_ext(id)
        ]
        lines = "\n".join(f"  - {id} (dead pending branch)" for id in dead)
        assert not dead, (
            f"U3 violated: pending node(s) with no Ext-pending descendant:\n{lines}"
        )

    def _has_pending_ext(self, id: str) -> bool:
        """Whether ``id``'s subtree contains an ``(ext, pending)`` node.

        A Det subtree never counts -- a detached child keeps nothing pending in
        our contract, so an Int parent whose only pending descendant is detached
        is still a dead branch.
        """
        node = self._nodes[id]
        if node.type == "det":
            return False
        if node.type == "ext" and node.kind == "pending":
            return True
        return any(self._has_pending_ext(c) for c in node.children)

    def well_formed(self, status: Status, todos: list[str]) -> None:
        """Assert the tree matches the inner outcome (``tree.md`` §4).

        Checked on *every* inner return. ``status`` is the outcome the inner is
        about to report -- ``"done"`` (the body returned, including a rejected
        fulfill) or ``"suspended"``; ``todos`` is the awaited subset
        (``Context.take_remote_todos()``), used for S4. Universal rules apply in
        both states; the status-specific rule then pins the frontier.

        * **U1** root is ``(int, pending)``.
        * **U2** every node reachable from the root.
        * **U3** no dead pending branches (:meth:`useful`).
        * **D1** (done) frontier empty.
        * **S1** (suspended) frontier non-empty; **S4** ``todos subset frontier``.

        Raises :class:`AssertionError` on the first violated rule.
        """
        # U1 -- the root is always settled by the outer, never the inner.
        root = self._nodes[self._root]
        assert root.type == "int", (
            f"U1 violated: root {self._root!r} has type {root.type}, expected int"
        )

        assert root.kind == "pending", (
            f"U1 violated: root {self._root!r} has kind {root.kind}, expected pending"
        )

        # U2 -- every node must be reachable from the root.
        reachable: set[str] = set()
        self._reach(self._root, reachable)
        orphans = sorted(set(self._nodes) - reachable)

        assert reachable == set(self._nodes), (
            f"U2 violated: node(s) not reachable from root: {orphans}"
        )

        # U3 -- no dead pending branches.
        self.useful()

        frontier = self.frontier()
        if status == "suspended":
            # S1 -- a suspend must have something to wait on.
            assert frontier, "S1 violated: suspended outcome with an empty frontier"
            # S4 -- the awaited subset must lie within the full frontier.
            extra = [t for t in todos if t not in frontier]
            assert not extra, (
                f"S4 violated: todos not subset of frontier; "
                f"todos={todos} frontier={frontier} extra={extra}"
            )
        else:
            assert not frontier, (
                f"D1 violated: done outcome with non-empty frontier {frontier}"
            )

    def _reach(self, id: str, seen: set[str]) -> None:
        """Collect every node reachable from ``id`` (Det subtrees included).

        Reachability spans all children regardless of type -- the frontier walk
        prunes Det, but U2 wants the full graph so an orphaned Det node is still
        caught.
        """
        if id in seen:
            return
        seen.add(id)
        for child in self._nodes[id].children:
            self._reach(child, seen)
