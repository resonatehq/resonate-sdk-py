from __future__ import annotations

import os
import time
from queue import Queue
from threading import Thread
from typing import Any, Protocol

import requests

from resonate.encoders.json import JsonEncoder
from resonate.models.encoder import Encoder
from resonate.models.message import InvokeMesg, Mesg


class Enqueueable[T](Protocol):
    def enqueue(self, item: T, /) -> None:
        ...

class Poller:
    def __init__(
        self,
        url: str | None = None,
        group: str = "default",
        encoder: Encoder[Any, str] | None = None,
    ) -> None:
        self._url = url or os.getenv("RESONATE_TASKS_URL", "http://localhost:8002")
        self.group = group
        self._encoder = encoder or JsonEncoder()
        self._thread: Thread | None = None
        self._stopped = False
        self.connection = None

    def start(self, cq: Enqueueable[Mesg], pid: str) -> None:
        if self._thread is not None:
            return
        self._thread = Thread(target=self._run, args=(cq, pid), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stopped = True

        if self.connection:
            self.connection.close()

        if self._thread is not None:
            self._thread.join()

    # @threading.exit_on_exception
    def _run(self, cq: Enqueueable[Mesg], pid: str) -> None:
        url = f"{self._url}/{self.group}/{pid}"
        while not self._stopped:
            try:
                with requests.get(url, stream=True) as res:  # noqa: S113
                    if not res.ok:
                        break

                    # fake it till you make it
                    self.connection = res.connection

                    for line in res.iter_lines(chunk_size=None, decode_unicode=True):
                        if not line:
                            continue

                        stripped = line.strip()
                        if not stripped.startswith("data:"):
                            continue

                        info = self._encoder.decode(stripped[5:])
                        # if "task" not in info:
                        #     continue

                        cq.enqueue(info)

            except requests.exceptions.ConnectionError:
                print("Connection to poller failed, reconnecting")
                # logger.warning("Connection to poller failed, reconnecting")

            time.sleep(1)

    # def recv(self, pid: str) -> str:
    #     return targets.poll(self.group, pid)
