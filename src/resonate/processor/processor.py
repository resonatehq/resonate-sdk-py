from __future__ import annotations

from threading import Thread
from typing import TYPE_CHECKING, Any

from resonate.cmd_queue import CmdQ, Complete
from resonate.processor.traits import IProcessor
from resonate.queue import Queue
from resonate.result import Err, Ok, Result

if TYPE_CHECKING:
    from resonate.processor.traits import SQE


class Processor(IProcessor):
    def __init__(
        self,
        workers: int = 4,
    ) -> None:
        assert workers > 0, "workers must be greater than zero."

        self._workers = workers
        self._threads = set[Thread]()

        self._sq: Queue[SQE[Any]] = Queue()

    def start(self, cmd_queue: CmdQ) -> None:
        for _ in range(self._workers):
            t = Thread(target=self._run, args=(cmd_queue,), daemon=True)
            self._threads.add(t)

            t.start()

    def enqueue(self, sqe: SQE[Any]) -> None:
        self._sq.put(sqe)

    def _run(self, cmd_queue: CmdQ) -> None:
        while True:
            sqe = self._sq.get()
            result: Result[Any, Exception]

            try:
                result = Ok(sqe.thunk())
            except Exception as e:  # noqa: BLE001
                result = Err(e)

            # put on completion queue
            cmd_queue.enqueue(Complete(sqe.id, result))
