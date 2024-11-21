from __future__ import annotations

import time
from threading import Thread
from typing import TYPE_CHECKING, Any

import requests

from resonate.encoders import JsonEncoder
from resonate.logging import logger
from resonate.stores.record import TaskRecord
from resonate.task_sources.traits import ITaskSource

if TYPE_CHECKING:
    from resonate.scheduler.traits import IScheduler


class Poller(ITaskSource):
    def __init__(self, scheduler: IScheduler, pid: str, url: str, group: str) -> None:
        self._scheduler = scheduler
        self._url = url
        self._group = group
        self._pid = pid
        self._encoder = JsonEncoder()
        self._worket_thread = Thread(target=self._run, daemon=True)
        self._worket_thread.start()

    def _run(self) -> None:
        try:
            while True:
                response = requests.get(  # noqa: S113
                    url=f"{self._url}/{self._group}/{self._pid}",
                    headers={"Accept": "text/event-stream"},
                    stream=True,
                )
                for line in response.iter_lines(chunk_size=None, decode_unicode=True):
                    if not line:
                        continue
                    stripped: str = line.strip()
                    assert stripped.startswith("data:")
                    info = self._encoder.decode(stripped[5:])
                    task = TaskRecord.decode(info["task"], encoder=self._encoder)
                    logger.info("Task received %s", task)
                    self._scheduler.add_task(task)
        except requests.exceptions.ConnectionError:
            time.sleep(2)

    def start(self) -> None:
        self._worket_thread.start()

    def default_recv(self) -> dict[str, Any]:
        return {"type": "poll", "data": {"group": self._group, "id": self._pid}}
