from __future__ import annotations

import sys

import pytest

from resonate.dependencies import Dependencies
from resonate.models.commands import Delayed, Function, Invoke, Network, RejectPromiseReq, ResolvePromiseReq, Return
from resonate.models.durable_promise import DurablePromise
from resonate.models.options import Options
from resonate.models.result import Ko, Ok
from resonate.models.retry_policies import Constant, Exponential, Linear, Never, RetryPolicy
from resonate.models.task import Task
from resonate.registry import Registry
from resonate.resonate import Context
from resonate.scheduler import Scheduler
from resonate.stores.local import LocalStore


def foo(ctx: Context):
    return "foo"


def bar(ctx: Context):
    yield ctx.lfi(foo)


@pytest.fixture
def scheduler() -> Scheduler:
    s = Scheduler(lambda id, info: Context(id, info, Options(), Registry(), Dependencies()))
    return s


@pytest.fixture
def promise_and_task() -> tuple[DurablePromise, Task]:
    store = LocalStore()
    promise, task = store.promises.create_with_task(id="foo", timeout=sys.maxsize, pid="foo", ttl=sys.maxsize, tags={"resonate:invoke": "foo"})
    assert task is not None
    return promise, task


@pytest.mark.parametrize(
    "retry_policy",
    [
        Never(),
        Constant(),
        Linear(),
        Exponential(),
    ],
)
def test_function_happy_path(scheduler: Scheduler, promise_and_task: tuple[DurablePromise, Task], retry_policy: RetryPolicy) -> None:
    reqs = scheduler._step(Invoke("foo", "foo", foo, opts=Options(retry_policy=retry_policy), promise_and_task=promise_and_task))
    assert len(reqs) == 1
    req = reqs[0]
    assert isinstance(req, Function)
    reqs = scheduler._step(Return("foo", "foo", Ok("foo")))
    assert len(reqs) == 1
    req = reqs[0]
    assert isinstance(req, Network)
    assert isinstance(req.req, ResolvePromiseReq)


@pytest.mark.parametrize(
    ("retry_policy", "retries"),
    [
        (Never(), 0),
        (Constant(max_retries=2), 2),
        (Linear(max_retries=3), 3),
        (Exponential(max_retries=2), 2),
    ],
)
def test_function_sad_path(scheduler: Scheduler, promise_and_task: tuple[DurablePromise, Task], retry_policy: RetryPolicy, retries: int) -> None:
    reqs = scheduler._step(Invoke("foo", "foo", foo, opts=Options(retry_policy=retry_policy), promise_and_task=promise_and_task))
    assert len(reqs) == 1
    req = reqs[0]
    assert isinstance(req, Function)

    for _ in range(retries):
        reqs = scheduler._step(Return("foo", "foo", Ko(ValueError("something went wrong"))))
        assert len(reqs) == 1
        req = reqs[0]
        assert isinstance(req, Delayed)

    reqs = scheduler._step(Return("foo", "foo", Ko(ValueError("something went wrong"))))
    assert len(reqs) == 1
    req = reqs[0]
    assert isinstance(req, Network)
    assert isinstance(req.req, RejectPromiseReq)


@pytest.mark.parametrize(
    "retry_policy",
    [
        Never(),
        Constant(),
        Linear(),
        Exponential(),
    ],
)
def test_generator_happy_path(scheduler: Scheduler, promise_and_task: tuple[DurablePromise, Task], retry_policy: RetryPolicy) -> None:
    reqs = scheduler._step(Invoke("foo", "foo", bar, opts=Options(retry_policy=retry_policy), promise_and_task=promise_and_task))
    assert len(reqs) == 1
    req = reqs[0]
    print(req)
    assert isinstance(req, Function)
    reqs = scheduler._step(Return("bar", "bar", Ok("bar")))
    assert len(reqs) == 1
    req = reqs[0]
    assert isinstance(req, Network)
    assert isinstance(req.req, ResolvePromiseReq)
