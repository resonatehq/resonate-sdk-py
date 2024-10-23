from __future__ import annotations

import time
from threading import Thread
from typing import TYPE_CHECKING

from typing_extensions import assert_never

from resonate.logging import logger
from resonate.queue import Queue
from resonate.record import TaskRecord

if TYPE_CHECKING:
    from resonate.scheduler import Scheduler
    from resonate.storage import ITaskStore


class TaskHandler:
    def __init__(self, scheduler: Scheduler, storage: ITaskStore) -> None:
        self._scheduler = scheduler
        self._submission_queue = Queue[TaskRecord]()
        self._storage = storage
        self._tasks_to_complete: dict[str, TaskRecord] = {}
        self._heartbeating_thread = Thread(target=self._heartbeat, daemon=True)
        self._heartbeating_thread.start()
        self._worker_thread = Thread(target=self._run, daemon=True)
        self._worker_thread.start()

    def _heartbeat(self) -> None:
        while True:
            affected = self._storage.heartbeat_tasks(pid=self._scheduler.pid)
            logger.debug("Heatbeat affected %s tasks", affected)
            time.sleep(2)

    def enqueue(self, sqe: TaskRecord) -> None:
        self._submission_queue.put(sqe)

    def _run(self) -> None:
        while True:
            sqe = self._submission_queue.dequeue()
            if isinstance(sqe, TaskRecord):
                poll_msg = self._storage.claim_task(
                    task_id=sqe.task_id,
                    counter=sqe.counter,
                    pid=self._scheduler.pid,
                    ttl=5000,
                )
                assert (
                    poll_msg.root_promise_store.promise_id
                    not in self._tasks_to_complete
                ), "There can only be one task per top level promise at a time."
                self._tasks_to_complete[poll_msg.root_promise_store.promise_id] = sqe

                self._scheduler.enqueue_poll_msg(poll_msg)
            elif isinstance(sqe, str):
                assert (
                    sqe in self._tasks_to_complete
                ), f"Task to complete related to promise {sqe} not found."
                task_record = self._tasks_to_complete[sqe]
                self._storage.complete_task(
                    task_id=task_record.task_id, counter=task_record.counter
                )
            else:
                assert_never(sqe)
