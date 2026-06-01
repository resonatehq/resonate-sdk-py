"""The execution tree -- the in-memory model of one workflow attempt's call graph.

This is the Python port of the model specified in ``tree.md``. It is an
**assertion-only** structure: the runtime never reads it to make a control-flow
decision (the outer drives off ``Context.spawned_remote``); instead it
materializes what ``execute_until_blocked_inner`` produced -- the set of
promises the body created, who settles each, and which are still pending -- so
that :meth:`Tree.well_formed` can assert the worker's behavior matches the
suspension contract on every inner return.

Terminology tracks ``tree.md`` exactly: a node's :data:`NodeType` (who settles
it, §2 Type) and :data:`NodeKind` (whether it has settled, §2 Kind) form the
2x3 product whose ``(ext, pending)`` cell is the suspension
:meth:`~Tree.frontier`.

Two deliberate divergences from a strict reading of the spec, both documented:

* **No mutex.** ``tree.md`` §10 guards ``t.nodes`` with a ``sync.Mutex`` because
  spawned-local goroutines run in parallel. Here every writer is an asyncio
  task on a single event loop, and no public method awaits between read and
  write, so the map and the per-node child slice cannot interleave -- the lock
  would add nothing.
* **Predicates raise.** ``tree.md`` §12 has ``Useful()`` / ``WellFormed()``
  return ``error``; following the repo-wide convention (codec.py / core.py)
  the equivalents here return ``None`` and raise :class:`AssertionError` with a
  multi-line message on violation. The tree is a pure assertion layer, so a
  failed check is a bug in the SDK, not a recoverable condition.
"""

from __future__ import annotations

from typing import Literal

import msgspec

#: Who is responsible for settling a node's durable promise. Assigned at the
#: call site and never changes (``tree.md`` §2 Type):
#:
#: * ``"int"`` -- internal, created by ``ctx.run``; this worker settles it
#:   under our task lease when the local executor returns.
#: * ``"ext"`` -- external, created by ``ctx.rpc`` / ``ctx.sleep`` /
#:   ``ctx.promise``; settled by something we await (another worker, the
#:   server's timer, an external ``promise.settle`` caller).
#: * ``"det"`` -- detached, created by ``ctx.detached``; fire-and-forget,
#:   outside this workflow's contract. Det subtrees are exempt from every rule
#:   and skipped by the frontier walk.
NodeType = Literal["int", "ext", "det"]

#: Whether a node's durable promise has reached a terminal state. The
#: five-state durable lattice (``pending | resolved | rejected |
#: rejected_canceled | rejected_timedout``) collapses to this single bit -- the
#: success/failure distinction matters at ``Future.await`` time, not for the
#: structural suspension contract. Transitions monotonically:
#: ``pending`` -> ``settled`` only (``tree.md`` §2 Kind).
NodeKind = Literal["pending", "settled"]

#: The outcome status the inner can report at a tree-assertion boundary.
#: ``tree.md`` §4 enumerates only ``Done`` and ``Suspended``; an error-fulfill
#: (the body raised an :class:`~resonate.errors.ApplicationError`) is a Done
#: with a rejected settle state, not a separate tree-level status.
Status = Literal["done", "suspended"]


class Node(msgspec.Struct, kw_only=True):
    """One promise in the call graph (``tree.md`` §12 Node).

    Mutable: ``kind`` flips ``pending`` -> ``settled`` exactly once, and
    ``children`` grows as the body spawns. ``children`` holds child IDs in
    insertion (== call) order, which is what makes the children-as-prefix
    replay property and :meth:`Tree.print` deterministic.
    """

    id: str
    type: NodeType
    kind: NodeKind
    children: list[str] = msgspec.field(default_factory=list)


class Tree:
    """The execution tree for one workflow attempt (``tree.md`` §12 Tree).

    Built incrementally as the body runs (:meth:`add_child` per spawn,
    :meth:`settle` as promises terminate), then asserted at the inner boundary
    via :meth:`well_formed`. The root is always ``(int, pending)`` -- it is
    settled by ``task.fulfill`` in the outer, never by the inner (invariant
    U1).
    """

    def __init__(self, root_id: str) -> None:
        self._root = root_id
        self._nodes: dict[str, Node] = {
            root_id: Node(id=root_id, type="int", kind="pending"),
        }

    # ── inspection ──────────────────────────────────────────────────

    def root(self) -> str:
        """Return the root node's id (``tree.md`` §12 Root)."""
        return self._root

    def has(self, id: str) -> bool:
        """Whether ``id`` exists in the tree (``tree.md`` §12 Has)."""
        return id in self._nodes

    def size(self) -> int:
        """Total node count (``tree.md`` §12 Size)."""
        return len(self._nodes)

    def ids(self) -> list[str]:
        """Every node id, unordered (``tree.md`` §12 IDs)."""
        return list(self._nodes)

    def get(self, id: str) -> Node | None:
        """Return a defensive copy of the node, or ``None`` if absent (``tree.md`` §12 Get).

        Returns a copy (including a fresh ``children`` list) so callers cannot
        mutate tree state through the handle.
        """
        node = self._nodes.get(id)
        if node is None:
            return None
        return Node(
            id=node.id,
            type=node.type,
            kind=node.kind,
            children=list(node.children),
        )

    # ── mutation ────────────────────────────────────────────────────

    def add_child(self, parent: str, id: str, type: NodeType) -> bool:
        """Attach a child of ``type`` under ``parent``; idempotent on ``id`` (``tree.md`` §12 AddChild).

        Returns ``True`` if a node was inserted, ``False`` if ``id`` already
        existed (so a replay that re-walks the same body does not duplicate
        nodes or re-append to the parent's child list). The parent must already
        be in the tree.
        """
        assert parent in self._nodes, f"unknown parent {parent!r}"
        if id in self._nodes:
            return False
        self._nodes[id] = Node(id=id, type=type, kind="pending")
        self._nodes[parent].children.append(id)
        return True

    def settle(self, id: str) -> None:
        """Mark ``id`` settled; monotonic, no-op if already settled or unknown (``tree.md`` §12 Settle)."""
        node = self._nodes.get(id)
        if node is None:
            return
        node.kind = "settled"

    # ── frontier ────────────────────────────────────────────────────

    def frontier(self) -> list[str]:
        """Return the ``(ext, pending)`` node IDs, skipping Det subtrees (``tree.md`` §3).

        These are the workflow's live remote dependencies -- the promises
        whose settlement will unblock further progress. A depth-first walk in
        child insertion order: a Det node prunes its whole subtree (it lives
        in another workflow's tree); an ``(ext, pending)`` node is collected
        and its subtree pruned; everything else descends.

        Note this is a **superset** of ``outcome.todos`` (the awaited subset
        the outer registers callbacks for): the frontier is *every* pending
        remote leaf, including ones the body created but never reached an
        ``await`` on. Invariant S4 (``todos subset frontier``) connects the
        two.
        """
        out: list[str] = []
        stack: list[str] = [self._root]

        while stack:
            curr_id = stack.pop()
            node = self._nodes[curr_id]

            if node.type == "det":
                continue

            if node.type == "ext" and node.kind == "pending":
                out.append(curr_id)
                continue

            # Extend the stack with children in reverse order to
            # maintain the depth-first walk in child insertion order.
            stack.extend(reversed(node.children))

        return out

    # ── predicates ──────────────────────────────────────────────────

    def useful(self) -> None:
        """Assert U3: no dead pending branches (``tree.md`` §4).

        Every non-root, non-Det node must be either ``settled`` OR have at
        least one ``(ext, pending)`` node in its subtree (itself included). A
        node failing this is *pending with no path to further progress* -- it
        should already have settled. Raises :class:`AssertionError` listing
        every dead branch.

        U3 subsumes the older S2/S3 (no Int-pending leaf, every Int-pending
        has an Ext-pending descendant): a suspended-local Int node is kept
        alive only by the Ext-pending descendant its child folded up.
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

        A Det subtree never counts -- a detached child keeps nothing pending
        in our contract, so an Int parent whose only pending descendant is
        detached is still a dead branch.
        """
        node = self._nodes[id]
        if node.type == "det":
            return False
        if node.type == "ext" and node.kind == "pending":
            return True
        return any(self._has_pending_ext(c) for c in node.children)

    def well_formed(self, status: Status, todos: list[str]) -> None:
        """Assert the tree matches the inner outcome (``tree.md`` §4).

        Checked on *every* inner return. ``status`` is the outcome the inner
        is about to report -- ``"done"`` (the body returned, including a
        rejected fulfill) or ``"suspended"``; ``todos`` is the awaited subset
        (``Context.take_remote_todos()``), used for S4. Universal rules apply
        in both states; the status-specific rule then pins the frontier.

        * **U1** root is ``(int, pending)``.
        * **U2** every node reachable from the root.
        * **U3** no dead pending branches (:meth:`useful`).
        * **D1** (done) frontier empty.
        * **S1** (suspended) frontier non-empty; **S4**
          ``todos subset frontier``.

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
        stack: list[str] = [self._root]

        while stack:
            curr_id = stack.pop()
            if curr_id not in reachable:
                reachable.add(curr_id)
                stack.extend(self._nodes[curr_id].children)

        assert reachable == set(self._nodes), (
            f"U2 violated: node(s) not reachable from root: "
            f"{sorted(set(self._nodes) - reachable)}"
        )

        # U3 -- no dead pending branches.
        self.useful()

        frontier = self.frontier()

        # S1 / D1 -- the status-specific rule pins the frontier. Keyed on the
        # reported status, not on emptiness, so S4 below sees the suspended
        # (non-empty) frontier too.
        match status:
            case "suspended":
                assert frontier, "S1 violated: suspended outcome with an empty frontier"
            case "done":
                assert not frontier, (
                    f"D1 violated: done outcome with non-empty frontier {frontier}"
                )

        # S4 -- the awaited subset must lie within the full frontier. Holds in
        # both states: when done the frontier is empty, so this also pins
        # todos empty.
        extra = [t for t in todos if t not in frontier]
        assert not extra, (
            f"S4 violated: todos not subset of frontier; "
            f"todos={todos} frontier={frontier} extra={extra}"
        )

    # ── display ─────────────────────────────────────────────────────

    def print(self) -> str:
        """Return an ASCII tree diagram, children in insertion order (``tree.md`` §12 Print).

        Each line is ``<id> (<type>, <kind>)``. Children are listed in their
        insertion (== call) order, so the output is a deterministic function
        of the (body, cache) pair -- exactly the children-as-prefix replay
        property (``tree.md`` §6).
        """
        lines: list[str] = []
        self._print_node(self._root, line_prefix="", child_prefix="", lines=lines)
        return "\n".join(lines)

    def _print_node(
        self, id: str, *, line_prefix: str, child_prefix: str, lines: list[str]
    ) -> None:
        node = self._nodes[id]
        lines.append(f"{line_prefix}{id} ({node.type}, {node.kind})")
        n = len(node.children)
        for i, child_id in enumerate(node.children):
            last = i == n - 1
            self._print_node(
                child_id,
                line_prefix=child_prefix + ("└── " if last else "├── "),
                child_prefix=child_prefix + ("    " if last else "│   "),
                lines=lines,
            )
