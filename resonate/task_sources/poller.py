from __future__ import annotations

import os
from threading import Thread
from typing import TYPE_CHECKING, Any

import requests

from resonate.encoders.json import JsonEncoder

if TYPE_CHECKING:
    from resonate.models.encoder import Encoder
    from resonate.models.enqueueable import Enqueueable
    from resonate.models.message import Mesg


class Poller:
    def __init__(
        self,
        url: str | None = None,
        group: str = "default",
        timeout: int | None = None,
        encoder: Encoder[Any, str] | None = None,
    ) -> None:
        self._url = url or os.getenv("RESONATE_TASKS_URL", "http://localhost:8002")
        self.group = group
        self._encoder = encoder or JsonEncoder()
        self._thread: Thread | None = None
        self._timeout = timeout

    def start(self, cq: Enqueueable[Mesg], pid: str) -> None:
        if self._thread is not None:
            return
        self._thread = Thread(target=self.loop, args=(cq, pid), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        pass

    def url(self, pid: str) -> str:
        return f"{self._url}/{self.group}/{pid}"

    def loop(self, cq: Enqueueable[Mesg], pid: str) -> None:
        try:
            while True:
                with requests.get(self.url(pid), stream=True, timeout=self._timeout) as res:
                    while True:
                        if msg := self._step(res):
                            cq.enqueue(msg)
        except requests.exceptions.ConnectionError:
            pass

    def step(self, cq: Enqueueable[Mesg], pid: str) -> None:
        with requests.get(self.url(pid), stream=True, timeout=self._timeout) as res:
            if msg := self._step(res):
                cq.enqueue(msg)

    def _step(self, res: requests.Response) -> Any:
        for line in res.iter_lines(chunk_size=None, decode_unicode=True):
            if not line:
                continue

            stripped = line.strip()
            if not stripped.startswith("data:"):
                continue

            return self._encoder.decode(stripped[5:])

        return None
