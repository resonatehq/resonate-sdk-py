from __future__ import annotations

import os
import time
from threading import Thread
from typing import TYPE_CHECKING, Any

import requests

from resonate.encoders.json import JsonEncoder
from resonate.models.enqueuable import Enqueueable

if TYPE_CHECKING:
    from resonate.models.encoder import Encoder
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
        self._stopped = False
        self._timeout = timeout

    def start(self, cq: Enqueueable[Mesg], pid: str) -> None:
        if self._thread is not None:
            return
        self._thread = Thread(target=self._run, args=(cq, pid), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stopped = True
        assert self._thread
        self._thread.join()

    def _run(self, cq: Enqueueable[Mesg], pid: str) -> None:
        url = f"{self._url}/{self.group}/{pid}"
        while not self._stopped:
            try:
                with requests.get(url, stream=True, timeout=self._timeout) as res:
                    for line in res.iter_lines(chunk_size=None, decode_unicode=True):
                        if self._stopped:
                            break

                        if not line:
                            continue

                        stripped = line.strip()
                        if not stripped.startswith("data:"):
                            continue

                        info = self._encoder.decode(stripped[5:])
                        cq.enqueue(info)

            except requests.exceptions.ConnectionError:
                pass

            if self._stopped:
                break
            time.sleep(1)
