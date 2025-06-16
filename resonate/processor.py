from __future__ import annotations

import os
import queue
from collections.abc import Callable
from threading import Thread
from typing import Any

from resonate.models.result import Ko, Ok, Result
from resonate.utils import exit_on_exception


class Processor:
    def __init__(self) -> None:
        self.threads = set[Thread]()
        for _ in range(min(32, (os.cpu_count() or 1))):
            self.threads.add(Thread(target=self._run, daemon=True))

        self.sq = queue.Queue[tuple[Callable[[], Any], Callable[[Result[Any]], None]]]()

    @exit_on_exception
    def _run(self) -> None:
        while True:
            try:
                func, callback = self.sq.get()
            except queue.ShutDown:
                break

            try:
                r = Ok(func())
            except Exception as e:
                r = Ko(e)

            callback(r)
            self.sq.task_done()

    def enqueue(self, func: Callable[[], Any], callback: Callable[[Result[Any]], None]) -> None:
        self.sq.put((func, callback))

    def start(self) -> None:
        for t in self.threads:
            if not t.is_alive():
                t.start()

    def stop(self) -> None:
        self.sq.shutdown()
        for t in self.threads:
            t.join()
        self.sq.join()
