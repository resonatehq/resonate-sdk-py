from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio

from resonate import PROTOCOL_VERSION, now_ms
from resonate.network.turso import TursoLocalDriver, TursoNetwork, origin_of
from resonate.network.turso.cron import CronError, cron_occurrences, next_cron

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

pytestmark = pytest.mark.asyncio

TARGET = "poll://any@default"

_corr = 0


def head(**extra: Any) -> dict[str, Any]:
    global _corr  # noqa: PLW0603
    _corr += 1
    return {"corrId": f"c{_corr}", "version": PROTOCOL_VERSION, **extra}


def request(kind: str, data: dict[str, Any], **head_extra: Any) -> str:
    return json.dumps({"kind": kind, "head": head(**head_extra), "data": data})


async def send(
    net: TursoNetwork, kind: str, data: dict[str, Any], **head_extra: Any
) -> dict[str, Any]:
    return json.loads(await net.send(request(kind, data, **head_extra)))


def ok(response: dict[str, Any]) -> Any:
    """Assert a 2xx and return the response data."""
    status = response["head"]["status"]
    assert 200 <= status < 300, f"expected success, got {status}: {response['data']}"
    return response["data"]


class Inbox:
    """Collects messages the network delivers, so a test can await a specific one."""

    def __init__(self, net: TursoNetwork) -> None:
        self.messages: list[dict[str, Any]] = []
        net.recv(lambda raw: self.messages.append(json.loads(raw)))

    async def next(
        self, predicate: Callable[[dict[str, Any]], bool], limit: float = 5.0
    ) -> dict[str, Any]:
        deadline = asyncio.get_running_loop().time() + limit
        while asyncio.get_running_loop().time() < deadline:
            for msg in self.messages:
                if predicate(msg):
                    return msg
            await asyncio.sleep(0.005)
        msg = f"timed out waiting for message; saw {self.messages}"
        raise AssertionError(msg)

    def clear(self) -> None:
        self.messages.clear()


def executes(inbox: Inbox) -> list[dict[str, Any]]:
    return [m for m in inbox.messages if m["kind"] == "execute"]


async def _network(**overrides: Any) -> TursoNetwork:
    """Build an in-memory network. Each database name gets its own isolated database."""
    options: dict[str, Any] = {
        "prefix": "test-",
        "tick_seconds": 0.01,
        "retry_timeout": 1000,
        **overrides,
    }
    net = TursoNetwork(TursoLocalDriver(":memory:"), **options)
    await net.start()
    return net


@pytest_asyncio.fixture
async def net() -> AsyncIterator[TursoNetwork]:
    network = await _network()
    try:
        yield network
    finally:
        await network.stop()


@pytest_asyncio.fixture
async def quiet() -> AsyncIterator[TursoNetwork]:
    """Yield a network whose background tick never fires on its own, so tests drive time."""
    network = await _network(tick_seconds=3600)
    try:
        yield network
    finally:
        await network.stop()


# =============================================================================
# ORIGIN
# =============================================================================


async def test_origin_of_is_the_id_up_to_the_first_dot() -> None:
    assert origin_of("foo") == "foo"
    assert origin_of("foo.1") == "foo"
    assert origin_of("foo.1.2") == "foo"
    assert origin_of("") == ""


# =============================================================================
# CRON
# =============================================================================


async def test_cron_next_is_strictly_after_and_in_utc() -> None:
    # 2024-01-01T00:00:00Z
    base = 1_704_067_200_000
    assert next_cron("* * * * *", base) == base + 60_000
    assert next_cron("0 0 * * *", base) == base + 86_400_000
    # Sunday 2024-01-07T00:00:00Z is the first Sunday after the base.
    assert next_cron("0 0 * * 0", base) == base + 6 * 86_400_000


async def test_cron_supports_ranges_lists_and_steps() -> None:
    base = 1_704_067_200_000
    assert next_cron("*/15 * * * *", base) == base + 15 * 60_000
    assert next_cron("5,10 0 * * *", base) == base + 5 * 60_000
    assert next_cron("0 2-4 * * *", base) == base + 2 * 3_600_000
    # Sunday is both 0 and 7.
    assert next_cron("0 0 * * 7", base) == next_cron("0 0 * * 0", base)


async def test_cron_finds_a_sparse_occurrence() -> None:
    # 2023-01-01T00:00:00Z; the next 29 February is in 2024.
    base = 1_672_531_200_000
    assert next_cron("0 0 29 2 *", base) == 1_709_164_800_000  # 2024-02-29T00:00:00Z


async def test_cron_rejects_malformed_expressions() -> None:
    for expression in (
        "* * * *",
        "60 * * * *",
        "* * * * 9",
        "*/0 * * * *",
        "5-1 * * * *",
    ):
        with pytest.raises(CronError):
            next_cron(expression, 0)


async def test_cron_occurrences_are_bounded_and_ordered() -> None:
    base = 1_704_067_200_000
    got = cron_occurrences("* * * * *", base, base + 5 * 60_000)
    assert got == [base + i * 60_000 for i in range(1, 6)]
    assert len(cron_occurrences("* * * * *", base, base + 10_000_000, cap=7)) == 7


# =============================================================================
# PROMISES
# =============================================================================


async def test_create_is_idempotent_and_get_reads_it_back(net: TursoNetwork) -> None:
    timeout_at = now_ms() + 60_000
    created = ok(
        await send(
            net,
            "promise.create",
            {
                "id": "wf",
                "timeoutAt": timeout_at,
                "param": {"data": "hello"},
                "tags": {},
            },
        )
    )
    assert created["promise"]["state"] == "pending"
    assert created["promise"]["param"]["data"] == "hello"

    # A second create with a different param returns the original.
    again = ok(
        await send(
            net,
            "promise.create",
            {
                "id": "wf",
                "timeoutAt": timeout_at,
                "param": {"data": "other"},
                "tags": {},
            },
        )
    )
    assert again["promise"]["param"]["data"] == "hello"

    got = ok(await send(net, "promise.get", {"id": "wf"}))
    assert got["promise"]["id"] == "wf"


async def test_get_of_an_unknown_promise_is_404(net: TursoNetwork) -> None:
    res = await send(net, "promise.get", {"id": "nope"})
    assert res["head"]["status"] == 404


async def test_settle_resolves_and_is_idempotent(net: TursoNetwork) -> None:
    await send(
        net,
        "promise.create",
        {"id": "wf", "timeoutAt": now_ms() + 60_000, "param": {}, "tags": {}},
    )

    settled = ok(
        await send(
            net,
            "promise.settle",
            {"id": "wf", "state": "resolved", "value": {"data": 42}},
        )
    )
    assert settled["promise"]["state"] == "resolved"
    assert settled["promise"]["value"]["data"] == 42

    twice = ok(
        await send(
            net,
            "promise.settle",
            {"id": "wf", "state": "rejected", "value": {"data": "no"}},
        )
    )
    assert twice["promise"]["state"] == "resolved"
    assert twice["promise"]["value"]["data"] == 42


async def test_a_promise_created_past_its_deadline_is_born_settled(
    net: TursoNetwork,
) -> None:
    res = ok(
        await send(
            net,
            "promise.create",
            {"id": "late", "timeoutAt": now_ms() - 1000, "param": {}, "tags": {}},
        )
    )
    assert res["promise"]["state"] == "rejected_timedout"
    assert res["promise"]["settledAt"] == res["promise"]["timeoutAt"]


async def test_a_timer_promise_past_its_deadline_is_born_resolved(
    net: TursoNetwork,
) -> None:
    res = ok(
        await send(
            net,
            "promise.create",
            {
                "id": "timer",
                "timeoutAt": now_ms() - 1000,
                "param": {},
                "tags": {"resonate:timer": "true"},
            },
        )
    )
    assert res["promise"]["state"] == "resolved"


async def test_a_pending_promise_past_its_deadline_reads_as_settled(
    quiet: TursoNetwork,
) -> None:
    now = now_ms()
    await send(
        quiet,
        "promise.create",
        {"id": "wf", "timeoutAt": now + 1000, "param": {}, "tags": {}},
        **{"resonate:debug_time": now},
    )

    res = ok(
        await send(
            quiet, "promise.get", {"id": "wf"}, **{"resonate:debug_time": now + 2000}
        )
    )
    assert res["promise"]["state"] == "rejected_timedout"
    assert res["promise"]["settledAt"] == now + 1000


async def test_create_validation_rejects_an_id_that_escapes_its_origin(
    net: TursoNetwork,
) -> None:
    res = await send(
        net,
        "promise.create",
        {
            "id": "other.1",
            "timeoutAt": now_ms() + 60_000,
            "param": {},
            "tags": {"resonate:origin": "wf"},
        },
    )
    assert res["head"]["status"] == 400


# =============================================================================
# CALLBACKS AND LISTENERS
# =============================================================================


async def test_a_callback_needs_an_external_awaited(net: TursoNetwork) -> None:
    timeout_at = now_ms() + 60_000
    # Awaiter carries a target, so it is external and addressable.
    await send(
        net,
        "promise.create",
        {
            "id": "wf",
            "timeoutAt": timeout_at,
            "param": {},
            "tags": {"resonate:target": TARGET},
        },
    )
    # Awaited is internal: no target, not a timer, not tagged external.
    await send(
        net,
        "promise.create",
        {"id": "wf.internal", "timeoutAt": timeout_at, "param": {}, "tags": {}},
    )

    res = await send(
        net, "promise.register_callback", {"awaited": "wf.internal", "awaiter": "wf"}
    )
    assert res["head"]["status"] == 422


async def test_a_callback_may_not_cross_origins(net: TursoNetwork) -> None:
    res = await send(
        net, "promise.register_callback", {"awaited": "other.1", "awaiter": "wf"}
    )
    assert res["head"]["status"] == 400


async def test_a_listener_is_unblocked_when_the_promise_settles(
    net: TursoNetwork,
) -> None:
    inbox = Inbox(net)
    await send(
        net,
        "promise.create",
        {
            "id": "wf",
            "timeoutAt": now_ms() + 60_000,
            "param": {},
            "tags": {"resonate:external": "true"},
        },
    )
    ok(
        await send(
            net,
            "promise.register_listener",
            {"awaited": "wf", "address": net.unicast()},
        )
    )
    await send(
        net,
        "promise.settle",
        {"id": "wf", "state": "resolved", "value": {"data": "done"}},
    )

    msg = await inbox.next(lambda m: m["kind"] == "unblock")
    assert msg["data"]["promise"]["id"] == "wf"
    assert msg["data"]["promise"]["state"] == "resolved"


async def test_a_listener_address_must_be_routable(net: TursoNetwork) -> None:
    await send(
        net,
        "promise.create",
        {
            "id": "wf",
            "timeoutAt": now_ms() + 60_000,
            "param": {},
            "tags": {"resonate:external": "true"},
        },
    )
    res = await send(
        net, "promise.register_listener", {"awaited": "wf", "address": "poll://default"}
    )
    assert res["head"]["status"] == 400


# =============================================================================
# TASKS
# =============================================================================


async def test_creating_a_targeted_promise_dispatches_an_execute(
    net: TursoNetwork,
) -> None:
    inbox = Inbox(net)
    await send(
        net,
        "promise.create",
        {
            "id": "wf",
            "timeoutAt": now_ms() + 60_000,
            "param": {},
            "tags": {"resonate:target": TARGET},
        },
    )

    msg = await inbox.next(lambda m: m["kind"] == "execute")
    assert msg == {
        "kind": "execute",
        "head": {},
        "data": {"task": {"id": "wf", "version": 0}},
    }


async def test_resonate_delay_holds_the_first_dispatch_back(
    quiet: TursoNetwork,
) -> None:
    inbox = Inbox(quiet)
    now = now_ms()
    await send(
        quiet,
        "promise.create",
        {
            "id": "wf",
            "timeoutAt": now + 60_000,
            "param": {},
            "tags": {"resonate:target": TARGET, "resonate:delay": str(now + 10_000)},
        },
        **{"resonate:debug_time": now},
    )

    # Nothing is dispatched while the delay is ahead...
    await send(quiet, "debug.tick", {"time": now + 1000})
    assert executes(inbox) == []

    # ...and the retry timer's first firing is the first dispatch.
    await send(quiet, "debug.tick", {"time": now + 11_000})
    msg = await inbox.next(lambda m: m["kind"] == "execute")
    assert msg["data"]["task"] == {"id": "wf", "version": 0}


async def test_acquire_fences_on_version(net: TursoNetwork) -> None:
    await send(
        net,
        "promise.create",
        {
            "id": "wf",
            "timeoutAt": now_ms() + 60_000,
            "param": {},
            "tags": {"resonate:target": TARGET},
        },
    )

    acquired = ok(
        await send(
            net, "task.acquire", {"id": "wf", "version": 0, "pid": "p1", "ttl": 30_000}
        )
    )
    assert acquired["task"]["version"] == 1
    assert acquired["task"]["state"] == "acquired"

    replay = await send(
        net, "task.acquire", {"id": "wf", "version": 0, "pid": "p2", "ttl": 30_000}
    )
    assert replay["head"]["status"] == 409


async def test_task_create_claims_a_fresh_workflow(net: TursoNetwork) -> None:
    res = ok(
        await send(
            net,
            "task.create",
            {
                "pid": "p1",
                "ttl": 30_000,
                "action": {
                    "kind": "promise.create",
                    "head": head(),
                    "data": {
                        "id": "wf",
                        "timeoutAt": now_ms() + 60_000,
                        "param": {},
                        "tags": {"resonate:target": TARGET},
                    },
                },
            },
        )
    )
    assert res["task"]["state"] == "acquired"
    assert res["task"]["version"] == 1
    assert res["promise"]["state"] == "pending"


async def test_suspend_registers_callbacks_and_settling_resumes(
    net: TursoNetwork,
) -> None:
    inbox = Inbox(net)
    timeout_at = now_ms() + 60_000

    await send(
        net,
        "promise.create",
        {
            "id": "wf",
            "timeoutAt": timeout_at,
            "param": {},
            "tags": {"resonate:target": TARGET},
        },
    )
    ok(
        await send(
            net, "task.acquire", {"id": "wf", "version": 0, "pid": "p1", "ttl": 30_000}
        )
    )
    # A child the workflow will block on. It is external, so it may be awaited.
    await send(
        net,
        "promise.create",
        {
            "id": "wf.child",
            "timeoutAt": timeout_at,
            "param": {},
            "tags": {"resonate:external": "true"},
        },
    )

    ok(
        await send(
            net,
            "task.suspend",
            {
                "id": "wf",
                "version": 1,
                "actions": [
                    {
                        "kind": "promise.register_callback",
                        "head": head(),
                        "data": {"awaited": "wf.child", "awaiter": "wf"},
                    }
                ],
            },
        )
    )
    assert ok(await send(net, "task.get", {"id": "wf"}))["task"]["state"] == "suspended"

    inbox.clear()
    await send(
        net,
        "promise.settle",
        {"id": "wf.child", "state": "resolved", "value": {"data": "child"}},
    )

    msg = await inbox.next(lambda m: m["kind"] == "execute")
    assert msg["data"]["task"] == {"id": "wf", "version": 1}

    resumed = ok(await send(net, "task.get", {"id": "wf"}))["task"]
    assert resumed["state"] == "pending"
    assert resumed["resumes"] == 1


async def test_suspending_on_a_settled_promise_returns_300(net: TursoNetwork) -> None:
    timeout_at = now_ms() + 60_000
    await send(
        net,
        "promise.create",
        {
            "id": "wf",
            "timeoutAt": timeout_at,
            "param": {},
            "tags": {"resonate:target": TARGET},
        },
    )
    await send(
        net, "task.acquire", {"id": "wf", "version": 0, "pid": "p1", "ttl": 30_000}
    )
    await send(
        net,
        "promise.create",
        {
            "id": "wf.child",
            "timeoutAt": timeout_at,
            "param": {},
            "tags": {"resonate:external": "true"},
        },
    )
    await send(
        net, "promise.settle", {"id": "wf.child", "state": "resolved", "value": {}}
    )

    res = await send(
        net,
        "task.suspend",
        {
            "id": "wf",
            "version": 1,
            "actions": [
                {
                    "kind": "promise.register_callback",
                    "head": head(),
                    "data": {"awaited": "wf.child", "awaiter": "wf"},
                }
            ],
        },
    )
    assert res["head"]["status"] == 300


async def test_fulfill_settles_the_promise_and_retires_the_task(
    net: TursoNetwork,
) -> None:
    await send(
        net,
        "promise.create",
        {
            "id": "wf",
            "timeoutAt": now_ms() + 60_000,
            "param": {},
            "tags": {"resonate:target": TARGET},
        },
    )
    await send(
        net, "task.acquire", {"id": "wf", "version": 0, "pid": "p1", "ttl": 30_000}
    )

    res = ok(
        await send(
            net,
            "task.fulfill",
            {
                "id": "wf",
                "version": 1,
                "action": {
                    "kind": "promise.settle",
                    "head": head(),
                    "data": {"id": "wf", "state": "resolved", "value": {"data": "out"}},
                },
            },
        )
    )
    assert res["promise"]["state"] == "resolved"
    assert ok(await send(net, "task.get", {"id": "wf"}))["task"]["state"] == "fulfilled"


async def test_fence_creates_a_child_and_refuses_a_stale_version(
    net: TursoNetwork,
) -> None:
    timeout_at = now_ms() + 60_000
    await send(
        net,
        "promise.create",
        {
            "id": "wf",
            "timeoutAt": timeout_at,
            "param": {},
            "tags": {"resonate:target": TARGET},
        },
    )
    await send(
        net, "task.acquire", {"id": "wf", "version": 0, "pid": "p1", "ttl": 30_000}
    )

    fenced = ok(
        await send(
            net,
            "task.fence",
            {
                "id": "wf",
                "version": 1,
                "action": {
                    "kind": "promise.create",
                    "head": head(),
                    "data": {
                        "id": "wf.child",
                        "timeoutAt": timeout_at,
                        "param": {"data": "arg"},
                        "tags": {},
                    },
                },
            },
        )
    )
    assert fenced["action"]["head"]["status"] == 200
    assert (
        ok(await send(net, "promise.get", {"id": "wf.child"}))["promise"]["param"][
            "data"
        ]
        == "arg"
    )

    stale = await send(
        net,
        "task.fence",
        {
            "id": "wf",
            "version": 0,
            "action": {
                "kind": "promise.create",
                "head": head(),
                "data": {
                    "id": "wf.other",
                    "timeoutAt": timeout_at,
                    "param": {},
                    "tags": {},
                },
            },
        },
    )
    assert stale["head"]["status"] == 409


async def test_halt_and_continue(net: TursoNetwork) -> None:
    inbox = Inbox(net)
    await send(
        net,
        "promise.create",
        {
            "id": "wf",
            "timeoutAt": now_ms() + 60_000,
            "param": {},
            "tags": {"resonate:target": TARGET},
        },
    )
    await inbox.next(lambda m: m["kind"] == "execute")

    ok(await send(net, "task.halt", {"id": "wf"}))
    assert ok(await send(net, "task.get", {"id": "wf"}))["task"]["state"] == "halted"

    inbox.clear()
    ok(await send(net, "task.continue", {"id": "wf"}))
    msg = await inbox.next(lambda m: m["kind"] == "execute")
    assert msg["kind"] == "execute"


# =============================================================================
# TIMEOUTS
# =============================================================================


async def test_a_due_promise_timeout_settles_retires_and_unblocks(
    quiet: TursoNetwork,
) -> None:
    inbox = Inbox(quiet)
    now = now_ms()

    await send(
        quiet,
        "promise.create",
        {
            "id": "wf",
            "timeoutAt": now + 1000,
            "param": {},
            "tags": {"resonate:target": TARGET},
        },
        **{"resonate:debug_time": now},
    )
    ok(
        await send(
            quiet,
            "promise.register_listener",
            {"awaited": "wf", "address": quiet.unicast()},
            **{"resonate:debug_time": now},
        )
    )

    await send(quiet, "debug.tick", {"time": now + 2000})

    promise = ok(
        await send(
            quiet, "promise.get", {"id": "wf"}, **{"resonate:debug_time": now + 2000}
        )
    )
    assert promise["promise"]["state"] == "rejected_timedout"
    task = ok(
        await send(
            quiet, "task.get", {"id": "wf"}, **{"resonate:debug_time": now + 2000}
        )
    )
    assert task["task"]["state"] == "fulfilled"

    msg = await inbox.next(lambda m: m["kind"] == "unblock")
    assert msg["data"]["promise"]["state"] == "rejected_timedout"


async def test_an_unclaimed_task_is_redispatched(quiet: TursoNetwork) -> None:
    inbox = Inbox(quiet)
    now = now_ms()

    await send(
        quiet,
        "promise.create",
        {
            "id": "wf",
            "timeoutAt": now + 60_000,
            "param": {},
            "tags": {"resonate:target": TARGET},
        },
        **{"resonate:debug_time": now},
    )
    await inbox.next(lambda m: m["kind"] == "execute")
    inbox.clear()

    await send(quiet, "debug.tick", {"time": now + 1500})
    msg = await inbox.next(lambda m: m["kind"] == "execute")
    assert msg["data"]["task"] == {"id": "wf", "version": 0}


async def test_an_expired_lease_returns_the_task_to_circulation(
    quiet: TursoNetwork,
) -> None:
    inbox = Inbox(quiet)
    now = now_ms()

    await send(
        quiet,
        "promise.create",
        {
            "id": "wf",
            "timeoutAt": now + 60_000,
            "param": {},
            "tags": {"resonate:target": TARGET},
        },
        **{"resonate:debug_time": now},
    )
    await inbox.next(lambda m: m["kind"] == "execute")
    ok(
        await send(
            quiet,
            "task.acquire",
            {"id": "wf", "version": 0, "pid": "p1", "ttl": 500},
            **{"resonate:debug_time": now},
        )
    )
    inbox.clear()

    await send(quiet, "debug.tick", {"time": now + 1000})

    task = ok(
        await send(
            quiet, "task.get", {"id": "wf"}, **{"resonate:debug_time": now + 1000}
        )
    )
    assert task["task"]["state"] == "pending"
    msg = await inbox.next(lambda m: m["kind"] == "execute")
    assert msg["data"]["task"] == {"id": "wf", "version": 1}


async def test_a_lease_on_a_logically_dead_task_is_not_returned(
    quiet: TursoNetwork,
) -> None:
    inbox = Inbox(quiet)
    now = now_ms()

    # The promise deadline lands before the lease would expire.
    await send(
        quiet,
        "promise.create",
        {
            "id": "wf",
            "timeoutAt": now + 400,
            "param": {},
            "tags": {"resonate:target": TARGET},
        },
        **{"resonate:debug_time": now},
    )
    await inbox.next(lambda m: m["kind"] == "execute")
    await send(
        quiet,
        "task.acquire",
        {"id": "wf", "version": 0, "pid": "p1", "ttl": 500},
        **{"resonate:debug_time": now},
    )
    inbox.clear()

    await send(quiet, "debug.tick", {"time": now + 1000})

    task = ok(
        await send(
            quiet, "task.get", {"id": "wf"}, **{"resonate:debug_time": now + 1000}
        )
    )
    assert task["task"]["state"] == "fulfilled"
    assert executes(inbox) == []


async def test_heartbeating_keeps_a_lease_alive(quiet: TursoNetwork) -> None:
    now = now_ms()
    await send(
        quiet,
        "promise.create",
        {
            "id": "wf",
            "timeoutAt": now + 60_000,
            "param": {},
            "tags": {"resonate:target": TARGET},
        },
        **{"resonate:debug_time": now},
    )
    await send(
        quiet,
        "task.acquire",
        {"id": "wf", "version": 0, "pid": "p1", "ttl": 500},
        **{"resonate:debug_time": now},
    )
    ok(
        await send(
            quiet,
            "task.heartbeat",
            {"pid": "p1", "tasks": [{"id": "wf", "version": 1}]},
            **{"resonate:debug_time": now + 400},
        )
    )

    await send(quiet, "debug.tick", {"time": now + 700})
    task = ok(
        await send(
            quiet, "task.get", {"id": "wf"}, **{"resonate:debug_time": now + 700}
        )
    )
    assert task["task"]["state"] == "acquired"


# =============================================================================
# PARTITIONING
# =============================================================================


async def test_each_origin_gets_its_own_database_file(tmp_path: Path) -> None:
    network = TursoNetwork(
        TursoLocalDriver(str(tmp_path)),
        prefix="acme-",
        timeout_database="timers",
        tick_seconds=3600,
    )
    await network.start()
    try:
        timeout_at = now_ms() + 60_000
        for promise_id in ("alpha", "beta.1"):
            ok(
                await send(
                    network,
                    "promise.create",
                    {
                        "id": promise_id,
                        "timeoutAt": timeout_at,
                        "param": {},
                        "tags": {},
                    },
                )
            )

        files = sorted(p.name for p in Path(tmp_path).glob("*.db"))  # noqa: ASYNC240
        assert files == ["acme-alpha.db", "acme-beta.db", "acme-timers.db"]
    finally:
        await network.stop()


async def test_promises_in_different_origins_do_not_see_each_other(
    net: TursoNetwork,
) -> None:
    timeout_at = now_ms() + 60_000
    await send(
        net,
        "promise.create",
        {
            "id": "alpha.1",
            "timeoutAt": timeout_at,
            "param": {"data": "a"},
            "tags": {"resonate:origin": "alpha"},
        },
    )
    await send(
        net,
        "promise.create",
        {
            "id": "beta.1",
            "timeoutAt": timeout_at,
            "param": {"data": "b"},
            "tags": {"resonate:origin": "beta"},
        },
    )

    # The search is scoped to the alpha database, which holds only alpha's ids.
    alpha = ok(
        await send(net, "promise.search", {"tags": {"resonate:origin": "alpha"}})
    )
    assert [p["id"] for p in alpha["promises"]] == ["alpha.1"]


async def test_a_tenant_wide_search_is_refused(net: TursoNetwork) -> None:
    res = await send(net, "promise.search", {})
    assert res["head"]["status"] == 501


async def test_a_heartbeat_spanning_origins_refreshes_every_one(
    quiet: TursoNetwork,
) -> None:
    now = now_ms()
    for promise_id in ("alpha", "beta"):
        await send(
            quiet,
            "promise.create",
            {
                "id": promise_id,
                "timeoutAt": now + 60_000,
                "param": {},
                "tags": {"resonate:target": TARGET},
            },
            **{"resonate:debug_time": now},
        )
        await send(
            quiet,
            "task.acquire",
            {"id": promise_id, "version": 0, "pid": "p1", "ttl": 500},
            **{"resonate:debug_time": now},
        )

    ok(
        await send(
            quiet,
            "task.heartbeat",
            {
                "pid": "p1",
                "tasks": [{"id": "alpha", "version": 1}, {"id": "beta", "version": 1}],
            },
            **{"resonate:debug_time": now + 400},
        )
    )

    await send(quiet, "debug.tick", {"time": now + 700})
    for promise_id in ("alpha", "beta"):
        task = ok(
            await send(
                quiet,
                "task.get",
                {"id": promise_id},
                **{"resonate:debug_time": now + 700},
            )
        )
        assert task["task"]["state"] == "acquired"


# =============================================================================
# SCHEDULES
# =============================================================================


async def test_schedule_create_get_delete(quiet: TursoNetwork) -> None:
    created = ok(
        await send(
            quiet,
            "schedule.create",
            {
                "id": "nightly",
                "cron": "0 0 * * *",
                "promiseId": "job.{{.timestamp}}",
                "promiseTimeout": 60_000,
                "promiseParam": {},
                "promiseTags": {"resonate:target": TARGET},
            },
        )
    )
    assert created["schedule"]["id"] == "nightly"
    assert created["schedule"]["nextRunAt"] > now_ms()

    got = ok(await send(quiet, "schedule.get", {"id": "nightly"}))
    assert got["schedule"]["cron"] == "0 0 * * *"

    ok(await send(quiet, "schedule.delete", {"id": "nightly"}))
    gone = await send(quiet, "schedule.get", {"id": "nightly"})
    assert gone["head"]["status"] == 404


async def test_a_schedule_without_a_target_is_refused(quiet: TursoNetwork) -> None:
    res = await send(
        quiet,
        "schedule.create",
        {
            "id": "bad",
            "cron": "* * * * *",
            "promiseId": "job.{{.timestamp}}",
            "promiseTimeout": 60_000,
            "promiseParam": {},
            "promiseTags": {},
        },
    )
    assert res["head"]["status"] == 400


async def test_a_due_schedule_fires_into_the_origin_its_id_names(
    quiet: TursoNetwork,
) -> None:
    ok(
        await send(
            quiet,
            "schedule.create",
            {
                "id": "every-minute",
                "cron": "* * * * *",
                "promiseId": "job.{{.id}}",
                "promiseTimeout": 3_600_000,
                "promiseParam": {"data": "tick"},
                "promiseTags": {"resonate:target": TARGET},
            },
        )
    )

    # Two minutes on, the schedule is due.
    await send(quiet, "debug.tick", {"time": now_ms() + 120_000})

    promise = ok(await send(quiet, "promise.get", {"id": "job.every-minute"}))[
        "promise"
    ]
    assert promise["param"]["data"] == "tick"
    assert promise["tags"]["resonate:schedule"] == "every-minute"
