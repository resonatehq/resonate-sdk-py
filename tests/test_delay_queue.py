from __future__ import annotations

from resonate.delay_queue import DelayQ


def test_delay_queue() -> None:
    dq = DelayQ[int]()
    dq.add(1, 10)
    dq.add(1, 10)
    dq.add(1, 10)
    assert dq.get(11) == [1, 1, 1]
    assert dq.get(11) == []
    dq.add(1, 2)
    dq.add(2, 1)
    assert dq.get(1) == [2]
    assert dq.get(2) == [1]
    dq.add(1, 2)
    dq.add(1, 2)
    dq.add(1, 2)
    assert dq.get(2) == [1, 1, 1]
