from __future__ import annotations

from typing import TYPE_CHECKING, Any

from resonate.actions import LFI
from resonate.dataclasses import FnOrCoroutine
from resonate.promise import Promise

if TYPE_CHECKING:
    from resonate.context import Context


def _foo(ctx: Context) -> None: ...
def test_child_creation() -> None:
    action = LFI(exec_unit=FnOrCoroutine(_foo))
    root_promise: Promise[Any] = Promise(
        promise_id="a", action=action, parent_promise=None
    )
    child_promise = root_promise.child_promise(promise_id=None, action=action)
    assert child_promise.promise_id == "a.1"
    assert child_promise.parent_promise == root_promise
    assert child_promise.root().promise_id == root_promise.promise_id
    child_child_promise = child_promise.child_promise(None, action)
    assert child_child_promise.promise_id == "a.1.1"
    assert child_child_promise.parent_promise == child_promise
    assert child_child_promise.root().promise_id == root_promise.promise_id


def test_leaf_promises() -> None:
    action = LFI(exec_unit=FnOrCoroutine(_foo))
    root_promise: Promise[Any] = Promise(
        promise_id="a", action=action, parent_promise=None
    )
    child_promise1 = root_promise.child_promise(promise_id=None, action=action)
    child_promise2 = root_promise.child_promise(promise_id=None, action=action)

    assert (
        len(
            root_promise.leaf_promises.symmetric_difference(
                {child_promise1, child_promise2}
            )
        )
        == 0
    )

    leaf_promise1_1 = child_promise1.child_promise(promise_id=None, action=action)
    leaf_promise1_2 = child_promise1.child_promise(promise_id=None, action=action)
    leaf_promise1_3 = child_promise1.child_promise(promise_id=None, action=action)
    assert (
        len(
            root_promise.leaf_promises.symmetric_difference(
                {leaf_promise1_1, leaf_promise1_2, leaf_promise1_3, child_promise2}
            )
        )
        == 0
    )

    leaf_promise2_1 = child_promise2.child_promise(promise_id=None, action=action)
    leaf_promise2_2 = child_promise2.child_promise(promise_id=None, action=action)
    assert (
        len(
            root_promise.leaf_promises.symmetric_difference(
                {
                    leaf_promise1_1,
                    leaf_promise1_2,
                    leaf_promise1_3,
                    leaf_promise2_1,
                    leaf_promise2_2,
                }
            )
        )
        == 0
    )

    child_promise2_3 = child_promise2.child_promise(promise_id=None, action=action)
    assert (
        len(
            root_promise.leaf_promises.symmetric_difference(
                {
                    leaf_promise1_1,
                    leaf_promise1_2,
                    leaf_promise1_3,
                    leaf_promise2_1,
                    leaf_promise2_2,
                    child_promise2_3,
                }
            )
        )
        == 0
    )
    leaf_promise2_3_1 = child_promise2_3.child_promise(promise_id=None, action=action)
    leaf_promise2_3_2 = child_promise2_3.child_promise(promise_id=None, action=action)
    leaf_promise2_3_3 = child_promise2_3.child_promise(promise_id=None, action=action)

    assert (
        len(
            root_promise.leaf_promises.symmetric_difference(
                {
                    leaf_promise1_1,
                    leaf_promise1_2,
                    leaf_promise1_3,
                    leaf_promise2_1,
                    leaf_promise2_2,
                    leaf_promise2_3_1,
                    leaf_promise2_3_2,
                    leaf_promise2_3_3,
                }
            )
        )
        == 0
    )


def test_partition_root() -> None:
    action = LFI(exec_unit=FnOrCoroutine(_foo))
    p2: Promise[Any] = Promise(promise_id="p2", action=action, parent_promise=None)
    p1 = p2.child_promise(promise_id="p1", action=action)
    assert p1.root() == p2
    assert p1.partition_root() == p2
    p0 = p1.child_promise(promise_id="p0", action=action)
    assert p0.root() == p2
    assert p0.partition_root() == p2
    assert len(p2.leaf_promises.symmetric_difference({p0})) == 0
    assert p2.children_promises == [p1]
    assert p1.children_promises == [p0]
    p1.mark_as_partition()
    assert p0.root() == p2
    assert p0.partition_root() == p1
    assert len(p2.leaf_promises.symmetric_difference({p0})) == 0
    assert p2.children_promises == [p1]
    assert p1.children_promises == [p0]
