from __future__ import annotations

import os
import time
from threading import Thread
from typing import TYPE_CHECKING, Any

import requests

from resonate.encoders.json import JsonEncoder
from resonate.models.message import InvokeMesg, NotifyMesg, ResumeMesg

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
        self._url = url or os.getenv("RESONATE_MSG_SRC_URL", "http://localhost:8002")
        self.group = group
        self._encoder = encoder or JsonEncoder()
        self._thread: Thread | None = None
        self._timeout = timeout

    def start(self, cq: Enqueueable[Mesg], pid: str) -> None:
        if self._thread is not None:
            return

        self._thread = Thread(name="poller-thread", target=self.loop, args=(cq, pid), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        # TODO(avillega): Couldn't come up with a nice way of stoping this thread
        # iter_lines is blocking and request.get is also blockig, this makes it so
        # the only way to stop it is waiting for a timeout on the request itself
        # which could never happen.
        #
        # Assuming we will have a high load on the nodes it would be possible to
        # check for a 'shutdown' flag inside the process lines function but it
        # could be that it never gets called when running tests for example.
        pass

    def url(self, pid: str) -> str:
        return f"{self._url}/{self.group}/{pid}"

    def loop(self, cq: Enqueueable[Mesg], pid: str) -> None:
        while True:
            try:
                url = self.url(pid)
                with requests.get(url, stream=True, timeout=self._timeout) as res:
                    res.raise_for_status()

                    for line in res.iter_lines(chunk_size=None, decode_unicode=True):
                        if msg := self._process_line(line):
                            cq.enqueue(msg)

                    if self._shutdown.is_set():
                        break

            except requests.exceptions.Timeout:
                print(f"Polling request timed out for group {self.group}. Retrying...")
                continue  # Go to the next iteration of the while loop
            except requests.exceptions.RequestException as e:
                print(f"Polling network error for group {self.group}: {e}. Retrying after delay...")
                time.sleep(1)
            except Exception as e:
                print(f"Unexpected error in poller loop for group {self.group}: {e}")
                if self._shutdown.wait(timeout=1):  # Short wait or until stopped
                    break

    def step(self, cq: Enqueueable[Mesg], pid: str) -> None:
        with requests.get(self.url(pid), stream=True, timeout=self._timeout) as res:
            for line in res.iter_lines(chunk_size=None, decode_unicode=True):
                if msg := self._process_line(line):
                    cq.enqueue(msg)
                    break

    def _process_line(self, line: str) -> Mesg | None:
        if not line:
            return None

        stripped = line.strip()
        if not stripped.startswith("data:"):
            return None

        d = self._encoder.decode(stripped[5:])
        match d["type"]:
            case "invoke":
                return InvokeMesg(type="invoke", task=d["task"])
            case "resume":
                return ResumeMesg(type="resume", task=d["task"])
            case "notify":
                return NotifyMesg(type="notify", promise=d["promise"])
            case _:
                # Unknown message type
                return None
