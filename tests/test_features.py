from __future__ import annotations

import json
import os
import time
from typing import TYPE_CHECKING, Any

import pytest

from resonate.commands import CreateDurablePromiseReq
from resonate.retry_policy import never
from resonate.scheduler import Scheduler
from resonate.storage.resonate_server import RemoteServer

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.context import Context
    from resonate.promise import Promise
    from resonate.typing import Yieldable


@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_human_in_the_loop() -> None:
    def human_in_the_loop(ctx: Context) -> Generator[Yieldable, Any, str]:
        name: str = yield ctx.rfc(
            CreateDurablePromiseReq(
                promise_id="test-human-in-loop-question-to-answer-1"
            )
        )
        age: int = yield ctx.rfc(
            CreateDurablePromiseReq(
                promise_id="test-human-in-loop-question-to-answer-2"
            )
        )
        return f"Hi {name} with age {age}"

    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])
    s = Scheduler(store)
    s.register(human_in_the_loop, retry_policy=never())
    p: Promise[str] = s.run("test-feature-human-in-the-loop", human_in_the_loop)
    assert not p.done()
    time.sleep(2)
    store.resolve(
        promise_id="test-human-in-loop-question-to-answer-1",
        ikey=None,
        strict=False,
        headers=None,
        data=json.dumps("Peter"),
    )
    time.sleep(2)
    store.resolve(
        promise_id="test-human-in-loop-question-to-answer-2",
        ikey=None,
        strict=False,
        headers=None,
        data=json.dumps(50),
    )
    assert p.result() == "Hi Peter with age 50"


@pytest.mark.skip
@pytest.mark.skipif(
    os.getenv("RESONATE_STORE_URL") is None, reason="env variable is not set"
)
def test_factorial_same_node() -> None:
    node_group = "test-factorial-same-node"

    def factorial(ctx: Context, n: int) -> Generator[Yieldable, Any, int]:
        if n == 0:
            return 1
        return n * (
            yield ctx.rfc(factorial, n - 1).with_options(
                f"factorial-{n-1}", target=node_group
            )
        )

    store = RemoteServer(url=os.environ["RESONATE_STORE_URL"])
    s = Scheduler(store, logic_group=node_group)
    s.register(factorial)
    n = 2
    p: Promise[int] = s.run(f"factorial-{n}", factorial, n)
    assert p.result() == 2  # noqa: PLR2004
