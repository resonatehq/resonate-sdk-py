from __future__ import annotations

import os
from threading import Thread
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from typing_extensions import assert_never

from resonate.result import Err, Ok

if TYPE_CHECKING:
    from resonate.context import Context
    from resonate.dataclasses import FnOrCoroutine
    from resonate.queue import Queue
    from resonate.result import Result
    from resonate.scheduler import Scheduler

T = TypeVar("T")


class FnSQE(Generic[T]):
    def __init__(self, ctx: Context, func: FnOrCoroutine[T]) -> None:
        assert not func.is_generator()
        self.ctx = ctx
        self.func = func


class FnCQE(Generic[T]):
    def __init__(self, result: Result[T, Exception]) -> None:
        self.result = result


class Processor:
    def __init__(
        self,
        max_workers: int | None,
        sq: Queue[FnSQE[Any]],
        scheduler: Scheduler,
    ) -> None:
        if max_workers is None:
            max_workers = min(32, (os.cpu_count() or 1) + 4)
        assert max_workers > 0, "`max_workers` must be positive."
        self._max_workers = max_workers
        self._sq = sq
        self._scheduler = scheduler
        self._threads = set[Thread]()

    def enqueue(self, sqe: FnSQE[Any]) -> None:
        self._sq.put_nowait(sqe)
        self._adjust_thread_count()

    def _run(self) -> None:
        while True:
            sqe = self._sq.dequeue()
            if isinstance(sqe, FnSQE):
                fn_result = sqe.func.exec_unit(
                    sqe.ctx, *sqe.func.args, **sqe.func.kwargs
                )
                assert isinstance(
                    fn_result, (Ok, Err)
                ), f"{fn_result} must be a result."
                self._scheduler.enqueue_cqe(FnCQE(fn_result))
            else:
                assert_never(sqe)

    def _adjust_thread_count(self) -> None:
        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            t = Thread(target=self._run, daemon=True)
            t.start()
            self._threads.add(t)
