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
    assert child_promise.root_promise().promise_id == root_promise.promise_id
    child_child_promise = child_promise.child_promise(None, action)
    assert child_child_promise.promise_id == "a.1.1"
    assert child_child_promise.parent_promise == child_promise
    assert child_child_promise.root_promise().promise_id == root_promise.promise_id


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


def test_promote_as_root() -> None:
    action = LFI(exec_unit=FnOrCoroutine(_foo))
    root_promise: Promise[Any] = Promise(
        promise_id="a", action=action, parent_promise=None
    )
    assert root_promise.root_promise() == root_promise
    child_1 = root_promise.child_promise(promise_id=None, action=action)
    assert child_1.root_promise() == root_promise
    assert len(root_promise.leaf_promises.symmetric_difference({child_1})) == 0
    assert root_promise.children_promises == [child_1]
    child_1.promote_as_root()
    assert child_1.root_promise() == child_1
    assert len(root_promise.leaf_promises) == 0
    assert len(root_promise.children_promises) == 0
