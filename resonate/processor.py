from __future__ import annotations

import os
import queue
from collections.abc import Callable
from threading import Thread
from typing import TYPE_CHECKING, Any

from resonate.models.commands import Command, Return
from resonate.models.result import Ko, Ok
from resonate.utils import exit_on_exception

if TYPE_CHECKING:
    from concurrent.futures import Future


class Processor:
    def __init__(self, cq: queue.Queue[Command | tuple[Command, Future] | None]) -> None:
        self.threads = set[Thread]()
        for _ in range(min(32, (os.cpu_count() or 1))):
            self.threads.add(Thread(target=self._run, daemon=True))

        self.sq = queue.Queue[tuple[str, str, Callable[[], Any]] | None]()
        self.cq = cq

    @exit_on_exception
    def _run(self) -> None:
        while sqe := self.sq.get():
            id, cid, func = sqe
            try:
                r = Ok(func())
            except Exception as e:
                r = Ko(e)
            self.cq.put_nowait(Return(id, cid, r))
            self.sq.task_done()

        self.sq.put(None)

    def enqueue(self, id: str, cid: str, func: Callable[[], Any]) -> None:
        self.sq.put((id, cid, func))

    def start(self) -> None:
        for t in self.threads:
            if not t.is_alive():
                t.start()

    def stop(self) -> None:
        self.sq.put(None)
        for t in self.threads:
            t.join()
