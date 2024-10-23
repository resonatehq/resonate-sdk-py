from __future__ import annotations

import json

import requests

from resonate.encoders import JsonEncoder
from resonate.record import TaskRecord
from resonate.scheduler import Scheduler
from resonate.storage import RemoteServer


class LongPoller:
    def __init__(self, url: str = "http://localhost") -> None:
        self._url = url
        storage = RemoteServer(f"{url}:8001")
        self._scheduler = Scheduler(storage)
        self._encoder = JsonEncoder()

    def run(self) -> None:
        while True:
            response = requests.get(
                f"{self._url}:8002/{self._scheduler.logic_group}/{self._scheduler.pid}",
                headers={"Accept": "text/event-stream"},
                stream=True,
            )
            for line in response.iter_lines(chunk_size=None, decode_unicode=True):
                if not line:
                    continue

                stripped: str = line.strip()
                assert stripped.startswith("data:")
                info = json.loads(stripped[5:])
                self._scheduler.enqueue_task(
                    TaskRecord.decode(info["task"], encoder=self._encoder)
                )
