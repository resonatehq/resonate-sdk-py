from __future__ import annotations

import json
import time
from threading import Event, Thread
from typing import TYPE_CHECKING, Union

import requests
from typing_extensions import TypeAlias

from resonate.encoders import Base64Encoder
from resonate.logging import logger
from resonate.queue import Queue
from resonate.record import Invoke, Resume, TaskRecord

if TYPE_CHECKING:
    from resonate.scheduler import Scheduler
    from resonate.storage import ITaskStore


Task: TypeAlias = Union[Invoke, Resume]


class TaskHandler:
    def __init__(
        self,
        scheduler: Scheduler,
        store: ITaskStore,
        heartbeat_freq: int = 10,
    ) -> None:
        self._claimables_queue = Queue[TaskRecord]()
        self._completables_queue = Queue[str]()
        self._store = store
        self._scheduler = scheduler
        self._base_64_encoder = Base64Encoder()
        self._heartbeat_freq = heartbeat_freq

        self._promise_id_to_task: dict[str, TaskRecord] = {}

        self._worker_continue = Event()

        self._worker_thread = Thread(target=self._run, daemon=True)
        self._heartbeat_thread: Thread | None = None

        self._worker_thread.start()

    def enqueue_claimable(self, task: TaskRecord) -> None:
        self._claimables_queue.put_nowait(task)
        self._signal()

    def _claim_task(self, task: TaskRecord) -> None:
        try:
            message = self._store.claim_task(
                task_id=task.id,
                counter=task.counter,
                pid=self._scheduler.pid,
                ttl=5,
            )

            self._heartbeat()

            root_promise = message.root
            assert (
                root_promise is not None
            ), "Root promise must not be None for a task message"

            self._promise_id_to_task[root_promise.promise_id] = task

            if message.type == "invoke":
                self._scheduler.enqueue_task(Invoke(root_promise))
            elif message.type == "resume":
                leaf = message.leaf
                assert leaf is not None, "Resume message must contain a leaf promise"
                self._scheduler.enqueue_task(
                    Resume(
                        root_promise=root_promise,
                        leaf_promise=leaf,
                    )
                )

        except Exception as e:  # noqa: BLE001
            logger.debug("Couldn't claim task %v, error: %v", task, e)

    def enqueue_completable(self, promise_id: str) -> None:
        self._completables_queue.put_nowait(promise_id)
        self._signal()

    def _complete_task(self, promise_id: str) -> None:
        task = self._promise_id_to_task.get(promise_id)
        assert (
            task is not None
        ), f"A task must exists for the given promise_id {promise_id}"
        self._store.complete_task(task_id=task.id, counter=task.counter)
        self._promise_id_to_task.pop(promise_id)

    def _run(self) -> None:
        while self._worker_continue.wait():
            self._worker_continue.clear()

            claimables = self._claimables_queue.dequeue_all()
            for claimable in claimables:
                self._claim_task(claimable)

            completables = self._completables_queue.dequeue_all()
            for promise_id in completables:
                self._complete_task(promise_id)

    def _signal(self) -> None:
        self._worker_continue.set()

    def _heartbeat(self) -> None:
        if self._heartbeat_thread is None or not self._heartbeat_thread.is_alive:
            self._heartbeat_thread = Thread(target=self._do_heartbeat, daemon=True)
            self._heartbeat_thread.start()

    def _do_heartbeat(self) -> None:
        active_tasks = self._store.heartbeat_tasks(pid=self._scheduler.pid)
        while active_tasks > 0:
            time.sleep(self._heartbeat_freq)
            active_tasks = self._store.heartbeat_tasks(pid=self._scheduler.pid)


class TaskPoller:
    def __init__(self, task_handler: TaskHandler) -> None:
        self.task_handler = task_handler
        self.stop = False

        self._worker_thread = Thread(target=self._run)
        self._worker_thread.start()

    def _run(self) -> None:
        while True:
            try:
                headers = {"Accept": "text/event-stream"}
                response = requests.get(  # noqa: S113
                    # TODO(avillega): do not hardcode this,  # noqa: FIX002, TD003
                    # this should be configured by the user.
                    "http://localhost:8002/default/0",
                    headers=headers,
                    stream=True,
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
                                    TaskRecord.decode(json_data["task"])
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
