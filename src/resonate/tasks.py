from __future__ import annotations

import time
from threading import Thread
from typing import TYPE_CHECKING

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

        self._heartbeating_thread = Thread(target=self._heartbeat, daemon=True)
        self._heartbeating_thread.start()
        self._worker_thread = Thread(target=self._run, daemon=True)
        self._worker_thread.start()

    def _heartbeat(self) -> None:
        while True:
            affected = self._storage.heartbeat_tasks(pid=self._scheduler.pid)
            logger.debug("Heatbeat affected %e tasks", affected)
            time.sleep(2)

    def enqueue(self, sqe: TaskRecord) -> None:
        self._submission_queue.put(sqe)

    def _run(self) -> None:
        while True:
            sqe = self._submission_queue.dequeue()
            self._storage.claim_task(
                task_id=sqe.task_id,
                counter=sqe.counter,
                pid=self._scheduler.pid,
                ttl=5000,
            )
