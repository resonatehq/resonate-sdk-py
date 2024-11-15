from __future__ import annotations

import json
import time
from threading import Thread
from typing import TYPE_CHECKING

import requests

from resonate.encoders import JsonEncoder
from resonate.stores.record import TaskRecord

if TYPE_CHECKING:
    from resonate.scheduler import Scheduler


class Poller:
    def __init__(self, scheduler: Scheduler, url: str) -> None:
        self._url = url
        self._scheduler = scheduler
        self._encoder = JsonEncoder()
        self._worket_thread = Thread(target=self._run, daemon=True)
        self._worket_thread.start()

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
                    info = json.loads(stripped[5:])
                    self._scheduler.enqueue_task_record(
                        TaskRecord.decode(info["task"], encoder=self._encoder)
                    )
        except requests.exceptions.ConnectionError:
            time.sleep(2)
