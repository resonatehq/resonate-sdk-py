from __future__ import annotations

import json
import time
from threading import Event, Thread

import requests

from resonate.logging import logger
from resonate.queue import Queue
from resonate.storage import ITaskStore, Task


class TaskHandler:
    def __init__(self, task_store: ITaskStore) -> None:
        self.claimables_queue = Queue[Task]()
        self.completables_queue = Queue[Task]()
        self.task_store: ITaskStore = task_store

        self._worker_thread = Thread(target=self._run)
        self._heartbeat_thread = Thread(target=self._heartbeat)

        self._worker_thread.start()

        self._worker_continue = Event()

    def enqueue_claimable(self, task: Task) -> None:
        pass

    def _claim_task(self, task: Task) -> None:
        try:
            message = self.task_store.claim(task)
            _ = message
            if not self._heartbeat_thread.is_alive():
                self._heartbeat_thread.start()


        except Exception:
            # TODO: handle exception, how does failing to claim looks like?
            ...

    def _complete_task(self, task: Task) -> None:
        pass

    def _run(self) -> None:
        while self._worker_continue.wait():
            claimables = self.claimables_queue.dequeue_all()
            for claimable in claimables:
                self._claim_task(claimable)

            completables = self.completables_queue.dequeue_all()
            for completable in completables:
                self._complete_task(completable)

    def _heartbeat(self) -> None:
        pass


class TaskPoller:
    def __init__(self, task_store: ITaskStore) -> None:
        self.task_handler = TaskHandler(task_store=task_store)
        self.stop = False

        self._worker_thread = Thread(target=self._run)
        self._worker_thread.start()

    def _run(self) -> None:
        while True:
            try:
                headers = {"Accept": "text/event-stream"}
                response = requests.get(  # noqa: S113
                    "http://localhost:8002/default/0",
                    headers=headers,
                    stream=True,
                    # TODO: do not hardcode this, this should be configured by the user.
                )

                logger.info("Connection to task source stablished, waiting for data")
                response.encoding = "utf-8"

                event_data = []
                for line in response.iter_lines(chunk_size=None, decode_unicode=True):
                    if line:
                        stripped = line.strip()
                        if stripped.startswith("data:"):
                            event_data.append(stripped[5:].strip())
                    else:
                        # Empty line, event is complete
                        if event_data:
                            data = "\n".join(event_data)
                            try:
                                json_data = json.loads(data)
                                self.task_handler.enqueue_claimable(
                                    ITaskStore.decode(json_data)
                                )
                            except json.JSONDecodeError:
                                logger.error(  # noqa: TRY400, Note: Not interested in the exception message itself
                                    "The received task is not valid json %s", data
                                )

                            # Reset for next event
                        event_data = []

            except Exception:  # noqa: BLE001, PERF203
                logger.info("Connection to task source lost, trying to reconnect")
                time.sleep(2)
