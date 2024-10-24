from __future__ import annotations

import time
from threading import Event, Thread
from typing import TYPE_CHECKING

from resonate.logging import logger
from resonate.queue import Queue
from resonate.record import TaskRecord
from resonate.shells.long_poller import LongPoller

if TYPE_CHECKING:
    from resonate.scheduler import Scheduler
    from resonate.storage.traits import ITaskStore


class TaskHandler:
    def __init__(
        self,
        scheduler: Scheduler,
        storage: ITaskStore,
        *,
        with_long_polling: bool = True,
    ) -> None:
        self._scheduler = scheduler
        self._claimables = Queue[TaskRecord]()
        self._completables = Queue[str]()
        self._storage = storage
        self._tasks_to_complete: dict[str, TaskRecord] = {}
        if with_long_polling:
            self._long_poller = LongPoller(
                task_handler=self,
                logic_group=scheduler.logic_group,
                pid=scheduler.pid,
            )
        self._heartbeating_thread = Thread(target=self._heartbeat, daemon=True)
        self._heartbeating_thread.start()
        self._continue_event = Event()
        self._worker_thread = Thread(target=self._run, daemon=True)
        self._worker_thread.start()

    def _heartbeat(self) -> None:
        while True:
            affected = self._storage.heartbeat_tasks(pid=self._scheduler.pid)
            logger.debug("Heatbeat affected %s tasks", affected)
            time.sleep(2)

    def _signal(self) -> None:
        self._continue_event.set()

    def enqueue_to_claim(self, sqe: TaskRecord) -> None:
        self._claimables.put(sqe)
        self._signal()

    def enqueue_to_complete(self, sqe: str) -> None:
        self._completables.put(sqe)
        self._signal()

    def _run(self) -> None:
        while self._continue_event.wait():
            self._continue_event.clear()
            claimables = self._claimables.dequeue_all()
            for claimable in claimables:
                poll_msg = self._storage.claim_task(
                    task_id=claimable.task_id,
                    counter=claimable.counter,
                    pid=self._scheduler.pid,
                    ttl=5000,
                )
                assert (
                    poll_msg.root_promise_store.promise_id
                    not in self._tasks_to_complete
                ), "There can only be one task per top level promise at a time"
                self._tasks_to_complete[poll_msg.root_promise_store.promise_id] = (
                    claimable
                )

                logger.info(
                    "Task related to promise %s has been claimed from worker %s/%s",
                    poll_msg.root_promise_store.promise_id,
                    self._scheduler.logic_group,
                    self._scheduler.pid,
                )
                self._scheduler.enqueue_poll_msg(poll_msg)

            completables = self._completables.dequeue_all()
            for completable in completables:
                assert (
                    completable in self._tasks_to_complete
                ), f"Task to complete related to promise {completable} not found."
                task_record = self._tasks_to_complete.pop(completable)
                self._storage.complete_task(
                    task_id=task_record.task_id, counter=task_record.counter
                )
                logger.info(
                    "Task related to promise %s has been completed from worker %s/%s",
                    completable,
                    self._scheduler.logic_group,
                    self._scheduler.pid,
                )
