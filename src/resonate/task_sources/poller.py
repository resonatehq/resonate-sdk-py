from __future__ import annotations

import time
from threading import Thread
from typing import TYPE_CHECKING, Any

import requests

from resonate.encoders import JsonEncoder
from resonate.stores.record import TaskRecord
from resonate.task_sources.traits import ITaskSource

if TYPE_CHECKING:
    from resonate.scheduler.traits import IScheduler


class Poller(ITaskSource):
    def __init__(self, scheduler: IScheduler, url: str, group: str) -> None:
        self._scheduler = scheduler
        self._url = url
        self._encoder = JsonEncoder()
        self._worket_thread = Thread(target=self._run, daemon=True)
        self._group = group

    def _run(self) -> None:
        try:
            while True:
                response = requests.get(  # noqa: S113
                    url=self._url,
                    headers={"Accept": "text/event-stream"},
                    stream=True,
                )
                for line in response.iter_lines(chunk_size=None, decode_unicode=True):
                    if not line:
                        continue
                    stripped: str = line.strip()
                    assert stripped.startswith("data:")
                    info = self._encoder.decode(stripped[5:])
                    self._scheduler.add_task(
                        TaskRecord.decode(info["task"], encoder=self._encoder)
                    )
        except requests.exceptions.ConnectionError:
            time.sleep(2)

    def start(self) -> None:
        self._worket_thread.start()

    def default_recv(self, pid: str) -> dict[str, Any]:
        return {"type": "poll", "data": {"group": self._group, "id": pid}}
