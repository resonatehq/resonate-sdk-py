from __future__ import annotations

from threading import Thread
from typing import TYPE_CHECKING

import requests
from typing_extensions import Any

from resonate_sdk import default, threading, utils
from resonate_sdk.encoder import IEncoder, JsonAndExceptionEncoder
from resonate_sdk.store.models import TaskRecord
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
        self._group = group or default.group()
        self._encoder = encoder or JsonAndExceptionEncoder()
        self._t: Thread | None = None

    def run(self, cq: Queue[TaskRecord | None], pid: str) -> None:
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

    @threading.exit_on_exception
    def _run(self, cq: Queue[TaskRecord | None], pid: str) -> None:
        url = f"{self._url}/{self._group}/{pid}"

        with requests.get(url, stream=True, timeout=10) as res:
            if not res.ok:
                raise NotImplementedError

            for line in res.iter_lines(decode_unicode=True):
                if not line:
                    continue

                stripped: str = line.strip()
                if not stripped.startswith("data:"):
                    continue
                info: dict[str, Any] = self._encoder.decode(stripped[5:])
                record = utils.decode(info["task"], self._encoder)
                assert isinstance(record, TaskRecord)
                cq.put(record)

    def stop(self) -> None:
        return

    def recv(self, pid: str) -> dict[str, Any]:
        return {"type": "poll", "data": {"group": self._group, "id": pid}}
