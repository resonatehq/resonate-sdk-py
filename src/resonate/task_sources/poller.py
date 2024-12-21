from __future__ import annotations

import time
from threading import Thread
from typing import Any

import requests

from resonate.cmd_queue import Claim, CommandQ
from resonate.encoders import JsonEncoder
from resonate.logging import logger
from resonate.stores.record import TaskRecord
from resonate.task_sources.traits import ITaskSource


class Poller(ITaskSource):
    def __init__(
        self,
        pid: str,
        url: str = "http://localhost:8002",
        group: str = "default",
    ) -> None:
        self._url = url
        self._group = group
        self._encoder = JsonEncoder()
        self._pid = pid

    def start(self, cmd_queue: CommandQ) -> None:
        t = Thread(target=self._run, args=(cmd_queue,), daemon=True)
        t.start()

    def _run(self, cmd_queue: CommandQ) -> None:
        url = f"{self._url}/{self._group}/{self._pid}"

        while True:
            try:
                with requests.get(url, stream=True) as res:  # noqa: S113
                    if not res.ok:
                        break

                    for line in res.iter_lines(chunk_size=None, decode_unicode=True):
                        if not line:
                            continue

                        stripped = line.strip()
                        if not stripped.startswith("data:"):
                            continue

                        info = self._encoder.decode(stripped[5:])
                        if "task" not in info:
                            continue

                        # extract the task
                        task = TaskRecord.decode(info["task"], encoder=self._encoder)

                        # enqueue the task
                        cmd_queue.put(Claim(task))

            except requests.exceptions.ConnectionError:
                logger.warning("Connection to poller failed, reconnecting")

            time.sleep(1)

    def default_recv(self) -> dict[str, Any]:
        return {"type": "poll", "data": {"group": self._group, "id": self._pid}}
