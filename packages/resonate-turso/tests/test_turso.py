from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio
from resonate_base import ORIGIN_HEADER, PROTOCOL_VERSION
from resonate_turso import (
    TursoLocalDriver,
    TursoNetwork,
    hash_origin,
    origin_of,
    owner_of,
)
from resonate_turso.cron import CronError, cron_occurrences, next_cron

from resonate.resonate import Resonate
from resonate.timing import now_ms

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from resonate.context import Context

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


async def test_origin_of_is_the_id_up_to_the_first_colon() -> None:
    assert origin_of("foo") == "foo"
    assert origin_of("foo:1") == "foo"
    assert origin_of("foo:1.2") == "foo"
    assert origin_of("") == ""


async def test_a_dotted_root_id_is_one_origin_not_several() -> None:
    # '.' separates lineage segments *below* the origin, so a dotted root is a
    # single workflow. Splitting on it would scatter one workflow's state
    # across databases and break the single-database-transaction invariant.
    assert origin_of("my.app.workflow") == "my.app.workflow"
    assert origin_of("my.app.workflow:1.2") == "my.app.workflow"


async def test_the_origin_header_routes_the_request(net: TursoNetwork) -> None:
    """The header the SDK resolves wins over anything derivable from the id.

    This is the seam ``resonate-base`` defines: the connector is handed the
    origin rather than working it out, so the id format stays the SDK's
    business alone.
    """
    timeout_at = now_ms() + 60_000
    ok(
        await send(
            net,
            "promise.create",
            {"id": "wf", "timeoutAt": timeout_at, "param": {}, "tags": {}},
        )
    )

    # Routed to a different origin's database, so the promise is not there --
    # proving the header, not the id, selected the database.
    elsewhere = json.loads(
        await net.send(
            request("promise.get", {"id": "wf"}), {ORIGIN_HEADER: "somewhere-else"}
        )
    )
    assert elsewhere["head"]["status"] == 404

    # Routed correctly, it is.
    found = json.loads(
        await net.send(request("promise.get", {"id": "wf"}), {ORIGIN_HEADER: "wf"})
    )
    assert found["head"]["status"] == 200


async def test_hash_origin_matches_the_vector_shared_with_the_typescript_sdk() -> None:
    # Pinned, not computed. Every node in a fleet must agree on this --
    # including nodes running the TypeScript SDK, whose ``hashOrigin`` asserts
    # the same vector -- so a change that quietly reshuffles ownership has to
    # fail here first.
    assert hash_origin("order-0") == 713018330
    assert hash_origin("order-1") == 729795949
    assert hash_origin("order-2") == 679463092
    assert hash_origin("acme") == 1174237615
    assert hash_origin("x.y") == 3335537014
    assert hash_origin("") == 2166136261


async def test_owner_of_spreads_origins_over_the_fleet_and_is_stable() -> None:
    assert owner_of("order-0", 2) == 0
    assert owner_of("order-1", 2) == 1
    assert len({owner_of(i, 3) for i in "abcdefgh"}) > 1


async def test_a_shard_that_cannot_index_the_fleet_is_rejected() -> None:
    for shard in ((2, 2), (-1, 2), (0, 0)):
        with pytest.raises(ValueError, match="Invalid shard"):
            TursoNetwork(TursoLocalDriver(":memory:"), shard=shard)
    TursoNetwork(TursoLocalDriver(":memory:"), shard=(1, 2))


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
            "id": "other:1",
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
        {"id": "wf:internal", "timeoutAt": timeout_at, "param": {}, "tags": {}},
    )

    res = await send(
        net, "promise.register_callback", {"awaited": "wf:internal", "awaiter": "wf"}
    )
    assert res["head"]["status"] == 422


async def test_a_callback_may_not_cross_origins(net: TursoNetwork) -> None:
    res = await send(
        net, "promise.register_callback", {"awaited": "other:1", "awaiter": "wf"}
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
            "id": "wf:child",
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
                        "data": {"awaited": "wf:child", "awaiter": "wf"},
                    }
                ],
            },
        )
    )
    assert ok(await send(net, "task.get", {"id": "wf"}))["task"]["state"] == "suspended"

    await send(
        net,
        "promise.settle",
        {"id": "wf:child", "state": "resolved", "value": {"data": "child"}},
    )

    # Match the resumed version rather than "any execute": the create-time
    # dispatch of version 0 is also in this inbox, and messages are handed over a
    # turn of the event loop after their commit, so the two can interleave.
    msg = await inbox.next(
        lambda m: m["kind"] == "execute" and m["data"]["task"]["version"] == 1
    )
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
            "id": "wf:child",
            "timeoutAt": timeout_at,
            "param": {},
            "tags": {"resonate:external": "true"},
        },
    )
    await send(
        net, "promise.settle", {"id": "wf:child", "state": "resolved", "value": {}}
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
                    "data": {"awaited": "wf:child", "awaiter": "wf"},
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
                        "id": "wf:child",
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
        ok(await send(net, "promise.get", {"id": "wf:child"}))["promise"]["param"][
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
                    "id": "wf:other",
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
        for promise_id in ("alpha", "beta:1"):
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


async def test_a_second_process_picks_up_work_it_never_created(tmp_path: Path) -> None:
    """The whole recovery story now that messages are not queued.

    The creator delivers the execute to itself and does nothing with it. The task
    stays pending, its retry timer comes due, and the worker -- which has never
    heard of this workflow -- finds it in the tenant timeout index, opens the
    origin database the id names, and gets the execute delivered locally.
    """
    creator = TursoNetwork(
        TursoLocalDriver(str(tmp_path)),
        prefix="shared-",
        # Nothing swept by the creator, so the worker is the only sweeper.
        tick_seconds=3600,
        retry_timeout=100,
    )
    worker = TursoNetwork(
        TursoLocalDriver(str(tmp_path)),
        prefix="shared-",
        tick_seconds=0.005,
        retry_timeout=100,
    )
    await creator.start()
    await worker.start()
    try:
        inbox = Inbox(worker)
        creator_inbox = Inbox(creator)

        ok(
            await send(
                creator,
                "promise.create",
                {
                    "id": "handoff",
                    "timeoutAt": now_ms() + 60_000,
                    "param": {"data": "payload"},
                    "tags": {"resonate:target": creator.resolve_target("default")},
                },
            )
        )

        # The creating process gets its own message, immediately and in process.
        local = await creator_inbox.next(lambda m: m["kind"] == "execute")
        assert local["data"]["task"] == {"id": "handoff", "version": 0}

        # The worker gets it too, once the retry timer it swept comes due.
        msg = await inbox.next(lambda m: m["kind"] == "execute")
        assert msg["data"]["task"] == {"id": "handoff", "version": 0}

        # The worker opens the origin database for the first time and claims it.
        acquired = ok(
            await send(
                worker,
                "task.acquire",
                {"id": "handoff", "version": 0, "pid": "worker-1", "ttl": 30_000},
            )
        )
        assert acquired["promise"]["param"]["data"] == "payload"

        # And the creator sees the claim, because both read the same database.
        seen = ok(await send(creator, "task.get", {"id": "handoff"}))["task"]
        assert seen["state"] == "acquired"
        assert seen["pid"] == "worker-1"
    finally:
        await creator.stop()
        await worker.stop()


async def test_a_sharded_node_sweeps_only_the_origins_it_owns(
    tmp_path: Path,
) -> None:
    """The point of ``shard``.

    Both nodes read one timeout index holding both slices; each must act on its
    own and leave the other's alone, or two nodes drive one workflow.
    """
    ids = ["alpha", "beta", "gamma", "delta"]
    mine = [i for i in ids if owner_of(i, 2) == 0]
    theirs = [i for i in ids if owner_of(i, 2) == 1]
    # A useless test if the hash happens to send everything one way.
    assert mine
    assert theirs

    # Creates the work but never sweeps, so every execute below was swept.
    creator = TursoNetwork(
        TursoLocalDriver(str(tmp_path)),
        prefix="shard-",
        tick_seconds=3600,
        retry_timeout=50,
    )
    node0 = TursoNetwork(
        TursoLocalDriver(str(tmp_path)),
        prefix="shard-",
        tick_seconds=0.005,
        retry_timeout=50,
        shard=(0, 2),
    )
    await creator.start()
    await node0.start()
    try:
        inbox = Inbox(node0)
        for promise_id in ids:
            ok(
                await send(
                    creator,
                    "promise.create",
                    {
                        "id": promise_id,
                        "timeoutAt": now_ms() + 60_000,
                        "param": {},
                        "tags": {"resonate:target": creator.resolve_target("default")},
                    },
                )
            )

        # Everything this node owns arrives...
        for promise_id in mine:
            await inbox.next(
                lambda m, want=promise_id: (
                    m["kind"] == "execute" and m["data"]["task"]["id"] == want
                )
            )
        # ...and nothing else ever does, though its timer is equally overdue.
        await asyncio.sleep(0.3)
        seen = {m["data"]["task"]["id"] for m in executes(inbox)}
        assert seen == set(mine)
        assert not seen & set(theirs)
    finally:
        await node0.stop()
        await creator.stop()


async def test_the_tenant_database_can_live_on_a_driver_of_its_own(
    tmp_path: Path,
) -> None:
    # A fleet shares the timeout index but not its origins, so the two sides
    # must be able to point at different storage.
    origins = tmp_path / "origins"
    timers = tmp_path / "timers"
    origins.mkdir()
    timers.mkdir()
    network = TursoNetwork(
        TursoLocalDriver(str(origins)),
        timeout_driver=TursoLocalDriver(str(timers)),
        prefix="split-",
        timeout_database="timers",
        tick_seconds=3600,
    )
    await network.start()
    try:
        ok(
            await send(
                network,
                "promise.create",
                {"id": "wf", "timeoutAt": now_ms() + 60_000, "param": {}, "tags": {}},
            )
        )
        assert [p.name for p in origins.glob("*.db")] == ["split-wf.db"]
        assert [p.name for p in timers.glob("*.db")] == ["split-timers.db"]
    finally:
        await network.stop()


async def test_a_message_reaches_the_client_with_no_table_backing_it(
    quiet: TursoNetwork,
) -> None:
    inbox = Inbox(quiet)
    await send(
        quiet,
        "promise.create",
        {
            "id": "wf",
            "timeoutAt": now_ms() + 60_000,
            "param": {},
            "tags": {"resonate:target": TARGET},
        },
    )

    # Delivered with no tick in between -- the network's tick interval is set
    # past the life of this test, so nothing but the commit could have done it.
    await inbox.next(lambda m: m["kind"] == "execute")

    # And the snapshot confirms nothing is queued anywhere.
    snap = ok(await send(quiet, "debug.snap", {}, **{"resonate:origin": "wf"}))
    assert snap["messages"] == []


async def test_promises_in_different_origins_do_not_see_each_other(
    net: TursoNetwork,
) -> None:
    timeout_at = now_ms() + 60_000
    await send(
        net,
        "promise.create",
        {
            "id": "alpha:1",
            "timeoutAt": timeout_at,
            "param": {"data": "a"},
            "tags": {"resonate:origin": "alpha"},
        },
    )
    await send(
        net,
        "promise.create",
        {
            "id": "beta:1",
            "timeoutAt": timeout_at,
            "param": {"data": "b"},
            "tags": {"resonate:origin": "beta"},
        },
    )

    # The search is scoped to the alpha database, which holds only alpha's ids.
    alpha = ok(
        await send(net, "promise.search", {"tags": {"resonate:origin": "alpha"}})
    )
    assert [p["id"] for p in alpha["promises"]] == ["alpha:1"]


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


# =============================================================================
# END TO END
# =============================================================================
#
# Everything above drives the network directly at the protocol level. These
# drive the SDK: a registered function, invoked durably, running to completion
# against Turso databases with no server anywhere.


async def _leaf(ctx: Context, tag: str) -> str:
    return tag


async def _order(ctx: Context, customer: str, amount: int) -> str:
    ref = await ctx.run(_leaf, f"CH-{amount}")
    await ctx.sleep(timedelta(milliseconds=30))
    return f"{customer}:{ref}"


async def _job(ctx: Context) -> str:
    a = await ctx.run(_leaf, "step1")
    await ctx.sleep(timedelta(milliseconds=200))
    b = await ctx.run(_leaf, "step2")
    return f"{a}+{b}"


def _worker(
    directory: str, pid: str, prefix: str, retry_timeout: int = 2000
) -> Resonate:
    r = Resonate(
        network=TursoNetwork(
            TursoLocalDriver(directory),
            prefix=prefix,
            pid=pid,
            tick_seconds=0.02,
            retry_timeout=retry_timeout,
        ),
    )
    r.register(_order)
    r.register(_job)
    r.register(_leaf)
    return r


async def test_a_workflow_runs_to_completion_and_can_be_reattached_to(
    tmp_path: Path,
) -> None:
    a = _worker(str(tmp_path), "proc-a", "e2e-")
    b = _worker(str(tmp_path), "proc-b", "e2e-")
    try:
        handle = a.run("order-1", _order, "acme", 100)
        assert await handle.result() == "acme:CH-100"

        # A second client over the same databases sees the settled result.
        #
        # Read it rather than awaiting a handle. ``result()`` registers a
        # listener and waits to be told, and this network does not push across
        # processes -- so on the orderings where B's push never arrives the
        # assertion only passes when the SDK's 60s subscription refresh comes
        # round. That is the documented limitation, not something to measure
        # here; what this test is for is that B, which never ran the workflow,
        # reads the same settled state out of the same databases.
        record = await b.promises.get("order-1")
        assert record.state == "resolved"
        assert record.value.data == "acme:CH-100"
    finally:
        await b.stop()
        await a.stop()


async def test_a_workflow_abandoned_mid_flight_is_finished_by_another_process(
    tmp_path: Path,
) -> None:
    # The recovery claim, end to end: A dies while its workflow is asleep, and
    # B -- which has never seen this workflow -- picks it up off the tenant
    # timeout index and runs it to completion.
    a = _worker(str(tmp_path), "proc-a", "rec-", retry_timeout=300)
    b: Resonate | None = None
    try:
        a.run("job-1", _job)
        await asyncio.sleep(0.12)
        await a.stop()

        b = _worker(str(tmp_path), "proc-b", "rec-", retry_timeout=300)
        handle = await b.get("job-1")
        assert await handle.result() == "step1+step2"
    finally:
        if b is not None:
            await b.stop()
