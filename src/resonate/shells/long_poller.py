from __future__ import annotations

import json
from threading import Thread
from typing import TYPE_CHECKING

import requests

from resonate.encoders import JsonEncoder
from resonate.record import TaskRecord

if TYPE_CHECKING:
    from resonate.tasks import TaskHandler


class LongPoller:
    def __init__(
        self,
        task_handler: TaskHandler,
        logic_group: str,
        pid: str,
        url: str = "http://localhost",
    ) -> None:
        self._url = url
        self._login_group = logic_group
        self._pid = pid
        self._task_handler = task_handler
        self._encoder = JsonEncoder()
        self._worket_thread = Thread(target=self._run, daemon=True)
        self._worket_thread.start()

    def _run(self) -> None:
        while True:
            response = requests.get(  # noqa: S113
                f"{self._url}:8002/{self._login_group}/{self._pid}",
                headers={"Accept": "text/event-stream"},
                stream=True,
            )
            for line in response.iter_lines(chunk_size=None, decode_unicode=True):
                if not line:
                    continue
                stripped: str = line.strip()
                assert stripped.startswith("data:")
                info = json.loads(stripped[5:])
                self._task_handler.enqueue_to_claim(
                    TaskRecord.decode(info["task"], encoder=self._encoder)
                )
