from __future__ import annotations

from resonate.queue import DelayQueue


def test_delay_queue() -> None:
    queue = DelayQueue[int]()
    queue.start()
    queue.put_nowait(item=3, delay=0.002)
    assert queue.dequeue() == 3  # noqa: PLR2004
    assert queue.qsize() == 0
    queue.put_nowait(item=10, delay=0.01)
    queue.put_nowait(item=3, delay=0.002)
    assert queue.dequeue() == 3  # noqa: PLR2004
    assert queue.qsize() == 1
    assert queue.dequeue() == 10  # noqa: PLR2004
    assert queue.qsize() == 0
