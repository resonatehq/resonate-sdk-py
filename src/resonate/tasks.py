from __future__ import annotations

import json
import time
from threading import Event, Thread
from typing_extensions import assert_never

import requests

from resonate.encoders import Base64Encoder
from resonate.logging import logger
from resonate.queue import Queue
from resonate.record import DurablePromiseRecord
from resonate.storage import ITaskStore, Task
from resonate.scheduler import Scheduler


class TaskHandler:
    def __init__(self, scheduler: Scheduler, task_store: ITaskStore) -> None:
        self.claimables_queue = Queue[Task]()
        self.completables_queue = Queue[Task]()
        self.task_store: ITaskStore = task_store
        self.scheduler = scheduler
        self.base_64_encoder = Base64Encoder()

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

            if message.type == "invoke":
                root = message.root
                assert (
                    root is not None
                ), "Root promise must not be None for task message invoke"

                # Params do not have a set type since it is used to put different kinds
                # of data in it for invoke task we expect it to have "func" which
                # represents the function name to call and "args" which are the args
                # to be use for the function invocation
                params = (
                    json.loads(self.base_64_encoder.decode(root.param.data))
                    if root.param.data is not None
                    else None
                )

                assert params is not None, "Params must have been set with rfc creation"

                args = params["args"]
                self.scheduler.invoke_task(params["func"], args, root, task)

            elif message.type == "resume":
                root = message.root
                leaf = message.leaf
                assert (
                    root is not None and leaf is not None
                ), "Both leaf and root promises are needed for a resume task"

                # Params do not have a set type since it is used to put different kinds
                # of data in it for invoke task we expect it to have "func" which
                # represents the function name to call and "args" which are the args
                # to be use for the function invocation
                params = (
                    json.loads(self.base_64_encoder.decode(root.param.data))
                    if root.param.data is not None
                    else None
                )

                assert params is not None, "Params must have been set with rfc creation"

                args = params["args"]
                self.scheduler.resume_task(
                    params["func"], args, task, root_promise=root, leaf_promise=leaf
                )

        except Exception:  # noqa: BLE001
            logger.debug("Couldn't claim task %v", task)

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
