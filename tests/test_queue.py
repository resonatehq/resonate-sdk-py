from __future__ import annotations

from resonate.cmd_queue import CmdQ, Invoke
from resonate.queue import DelayQueue


def test_delay_queue() -> None:
    queue = DelayQueue()
    cmd_queue = CmdQ()
    queue.start(cmd_queue)
    queue.enqueue(item=Invoke("3"), delay=0.002)
    assert cmd_queue.dequeue() == Invoke("3")
    queue.enqueue(item=Invoke("10"), delay=0.01)
    queue.enqueue(item=Invoke("2"), delay=0.002)
    assert cmd_queue.dequeue() == Invoke("2")
    assert cmd_queue.dequeue() == Invoke("10")
