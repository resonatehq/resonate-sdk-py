from __future__ import annotations

import time
from threading import Thread
from typing import TYPE_CHECKING

import requests
from typing_extensions import Any

from resonate_sdk import default, targets, utils
from resonate_sdk.encoder import IEncoder, JsonAndExceptionEncoder
from resonate_sdk.logging import logger
from resonate_sdk.models.task import TaskRecord
from resonate_sdk.task_sources.traits import ITaskSource

if TYPE_CHECKING:
    from queue import Queue


class Poller(ITaskSource):
    def __init__(
        self,
        url: str | None = None,
        group: str | None = None,
        encoder: IEncoder[Any, str] | None = None,
    ) -> None:
        self._url = url or default.url("poller")
        self.group = group or default.group()
        self._encoder = encoder or JsonAndExceptionEncoder()
        self._t: Thread | None = None

    def run(self, cq: Queue[TaskRecord], pid: str) -> None:
        if self._t is not None:
            return
        self._t = Thread(
            target=self._run,
            args=(
                cq,
                pid,
            ),
            daemon=True,
        )
        self._t.start()

    # @threading.exit_on_exception
    def _run(self, cq: Queue[TaskRecord], pid: str) -> None:
        url = f"{self._url}/{self.group}/{pid}"
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

                        task_record = utils.decode(info, self._encoder)
                        assert isinstance(task_record, TaskRecord)
                        cq.put(task_record)

            except requests.exceptions.ConnectionError:
                logger.warning("Connection to poller failed, reconnecting")

            time.sleep(1)

    def stop(self) -> None:
        return

    def recv(self, pid: str) -> str:
        return targets.poll(self.group, pid)
