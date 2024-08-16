from __future__ import annotations

import asyncio
import os
import queue
from inspect import isgenerator, isgeneratorfunction
from threading import Event, Thread
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from typing_extensions import ParamSpec, assert_never

from resonate import utils
from resonate.actions import TopLevelInvoke
from resonate.context import (
    Call,
    Context,
    FnOrCoroutine,
    Invoke,
)
from resonate.dataclasses import Command, CoroAndPromise, Runnable
from resonate.dependency_injection import Dependencies
from resonate.itertools import FinalValue, iterate_coro
from resonate.promise import Promise
from resonate.result import Err, Ok

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from resonate.result import Result
    from resonate.typing import (
        Awaitables,
        DurableCoro,
        DurableFn,
        RunnableCoroutines,
    )

T = TypeVar("T")
P = ParamSpec("P")


class _SQE(Generic[T]):
    def __init__(
        self,
        promise: Promise[T],
        fn: Callable[P, T | Coroutine[Any, Any, T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.promise = promise
        self.fn = fn
        self.args = args
        self.kwargs = kwargs


class _CQE(Generic[T]):
    def __init__(self, promise: Promise[T], fn_result: Result[T, Exception]) -> None:
        self.fn_result = fn_result
        self.promise = promise


class _Processor:
    def __init__(self, max_workers: int | None, scheduler: Scheduler) -> None:
        if max_workers is None:
            max_workers = min(32, (os.cpu_count() or 1) + 4)
        assert max_workers > 0, "`max_workers` must be positive."
        self._max_workers = max_workers
        self._submission_queue = queue.Queue[_SQE[Any]]()
        self._completion_queue = queue.Queue[_CQE[Any]]()
        self._threads = set[Thread]()
        self._scheduler = scheduler

    def enqueue(self, sqe: _SQE[Any]) -> None:
        self._submission_queue.put(sqe)
        self._adjust_thread_count()

    def dequeue(self) -> _CQE[Any]:
        return utils.dequeue(self._completion_queue)

    def dequeue_batch(self, batch_size: int) -> list[_CQE[Any]]:
        return utils.dequeue_batch(self._completion_queue, batch_size)

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        while True:
            fn_result: Result[Any, Exception]
            sqe = utils.dequeue(self._submission_queue)
            if asyncio.iscoroutinefunction(sqe.fn):
                try:
                    fn_result = Ok(
                        loop.run_until_complete(sqe.fn(*sqe.args, **sqe.kwargs))
                    )
                except Exception as e:  # noqa: BLE001
                    fn_result = Err(e)
            else:
                try:
                    fn_result = Ok(sqe.fn(*sqe.args, **sqe.kwargs))
                except Exception as e:  # noqa: BLE001
                    fn_result = Err(e)
            self._completion_queue.put(
                _CQE(
                    sqe.promise,
                    fn_result,
                )
            )
            self._scheduler.signal()

    def _adjust_thread_count(self) -> None:
        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            t = Thread(target=self._run, daemon=True)
            t.start()
            self._threads.add(t)


class Scheduler:
    def __init__(self, max_workers: int | None = None) -> None:
        self._stg_queue = queue.Queue[tuple[TopLevelInvoke, Promise[Any]]]()
        self._worker_thread: Thread | None = None
        self._worker_continue = Event()

        self._deps = Dependencies()

        self.runnable_coros: RunnableCoroutines = []
        self.awaitables: Awaitables = {}

        self._processor = _Processor(max_workers, self)

    def signal(self) -> None:
        self._worker_continue.set()

    def run(
        self,
        promise_id: str,
        coro: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Promise[T]:
        top_lvl = TopLevelInvoke(coro, *args, **kwargs)
        p = Promise[T](promise_id, top_lvl.to_invoke())
        self._stg_queue.put_nowait((top_lvl, p))
        self.signal()

        if self._worker_thread is None:
            self._worker_thread = Thread(target=self._run, daemon=True)
            self._worker_thread.start()

        return p

    def _run(self) -> None:
        while self._worker_continue.wait():
            self._worker_continue.clear()

            top_lvls: list[tuple[TopLevelInvoke, Promise[Any]]] = utils.dequeue_batch(
                self._stg_queue,
                batch_size=20,
            )
            for top_lvl, p in top_lvls:
                parent_ctx = Context(
                    ctx_id=p.promise_id, seed=None, parent_ctx=None, deps=self._deps
                )
                if isgeneratorfunction(top_lvl.exec_unit):
                    # Within a try/except in case the coroutine raises an error before
                    # yielding anything.
                    try:
                        coro = top_lvl.exec_unit(
                            parent_ctx, *top_lvl.args, **top_lvl.kwargs
                        )
                    except Exception as e:  # noqa: BLE001
                        p.set_result(Err(e))
                    self._add_coro_to_runnables(
                        CoroAndPromise(coro, p, parent_ctx),
                        None,
                    )

                else:
                    # If is not a generator, but a function. Submit the execution
                    # to the processor.
                    self._processor.enqueue(
                        _SQE(
                            p,
                            top_lvl.exec_unit,
                            parent_ctx,
                            *top_lvl.args,
                            **top_lvl.kwargs,
                        )
                    )

            cqes = self._processor.dequeue_batch(batch_size=10)
            for cqe in cqes:
                cqe.promise.set_result(cqe.fn_result)
                self._unblock_coros_waiting_on_promise(cqe.promise)

            while self.runnable_coros:
                self._advance_runnable_span(self.runnable_coros.pop())

            assert not self.runnable_coros, "Runnables should have been all exhausted"

    def _add_coro_to_awaitables(
        self, p: Promise[Any], coro_and_promise: CoroAndPromise[Any]
    ) -> None:
        assert (
            not p.done()
        ), "If the promise is resolved already it makes no sense to block coroutine"
        self.awaitables.setdefault(p, []).append(coro_and_promise)

    def _add_coro_to_runnables(
        self,
        coro_and_promise: CoroAndPromise[Any],
        value_to_yield_back: Result[Any, Exception] | None,
    ) -> None:
        self.runnable_coros.append(Runnable(coro_and_promise, value_to_yield_back))

    def _unblock_coros_waiting_on_promise(self, p: Promise[Any]) -> None:
        assert p.done(), "Promise must be done to unblock waiting coroutines."

        if self.awaitables.get(p) is None:
            return

        for coro_and_promise in self.awaitables.pop(p):
            self._add_coro_to_runnables(coro_and_promise, p.safe_result())

    def _advance_runnable_span(self, runnable: Runnable[Any]) -> None:
        assert isgenerator(
            runnable.coro_and_promise.coro
        ), "Only coroutines can be advanced"

        yieldable_or_final_value: Call | Invoke | Promise[Any] | FinalValue[Any] = (
            iterate_coro(runnable=runnable)
        )

        if isinstance(yieldable_or_final_value, FinalValue):
            v = yieldable_or_final_value.v
            runnable.coro_and_promise.prom.set_result(v)
            self._unblock_coros_waiting_on_promise(runnable.coro_and_promise.prom)

        elif isinstance(yieldable_or_final_value, Call):
            called = yieldable_or_final_value
            p = self._process_invokation(called.to_invoke(), runnable)
            if not p.done():
                self._add_coro_to_awaitables(p, runnable.coro_and_promise)
            else:
                self._add_coro_to_runnables(runnable.coro_and_promise, p.safe_result())

        elif isinstance(yieldable_or_final_value, Invoke):
            invokation = yieldable_or_final_value
            p = self._process_invokation(invokation, runnable)
            self._add_coro_to_runnables(runnable.coro_and_promise, Ok(p))

        elif isinstance(yieldable_or_final_value, Promise):
            p = yieldable_or_final_value
            if p.done():
                self._add_coro_to_runnables(runnable.coro_and_promise, p.safe_result())
                self._unblock_coros_waiting_on_promise(p)
            else:
                self._add_coro_to_awaitables(p, runnable.coro_and_promise)

        else:
            assert_never(yieldable_or_final_value)

    def _process_invokation(
        self, invokation: Invoke, runnable: Runnable[Any]
    ) -> Promise[Any]:
        parent_ctx = runnable.coro_and_promise.ctx
        child_ctx = parent_ctx.new_child()
        p = Promise[Any](child_ctx.ctx_id, invokation)
        if isinstance(invokation.exec_unit, Command):
            raise NotImplementedError
        if isinstance(invokation.exec_unit, FnOrCoroutine):
            coro_or_function = invokation.exec_unit.exec_unit
            if isgeneratorfunction(coro_or_function):
                try:
                    coro = coro_or_function(
                        child_ctx,
                        *invokation.exec_unit.args,
                        **invokation.exec_unit.kwargs,
                    )
                except Exception as e:  # noqa: BLE001
                    p.set_result(Err(e))
                self._add_coro_to_runnables(
                    CoroAndPromise(coro, p, child_ctx),
                    None,
                )
            else:
                self._processor.enqueue(
                    _SQE(
                        p,
                        coro_or_function,
                        child_ctx,
                        *invokation.exec_unit.args,
                        **invokation.exec_unit.kwargs,
                    )
                )
        else:
            assert_never(invokation.exec_unit)
        return p
