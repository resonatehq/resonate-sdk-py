from __future__ import annotations

import asyncio
from typing import Any

import msgspec

from resonate.network import HttpNetwork, LocalNetwork

I64_MAX = 2**63 - 1


# -- helpers ------------------------------------------------------------------


async def send(net: LocalNetwork, req: Any) -> Any:
    """Encode ``req``, send it through ``net``, and decode the response."""
    resp = await net.send(msgspec.json.encode(req).decode("utf-8"))
    return msgspec.json.decode(resp)


def status(resp: Any) -> int:
    """Extract ``head.status`` from an envelope response (Rust test helper)."""
    head = resp.get("head") if isinstance(resp, dict) else None
    if isinstance(head, dict) and isinstance(head.get("status"), int):
        return head["status"]
    return 0


def data(resp: Any) -> Any:
    """Extract the ``data`` portion from an envelope response (Rust test helper)."""
    if isinstance(resp, dict) and "data" in resp:
        return resp["data"]
    return resp


# -- tests --------------------------------------------------------------------


def test_local_network_creates_and_gets_promise() -> None:
    async def run() -> None:
        net = LocalNetwork(pid="test-pid", group="default")
        req = {
            "kind": "promise.create",
            "head": {"corrId": "c1", "version": "2025-01-15"},
            "data": {
                "id": "p1",
                "timeoutAt": I64_MAX,
                "param": {"data": "test"},
                "tags": {"resonate:scope": "global"},
            },
        }
        resp = await send(net, req)
        assert status(resp) in (200, 201)
        assert data(resp)["promise"]["id"] == "p1"
        assert data(resp)["promise"]["state"] == "pending"

        get_req = {
            "kind": "promise.get",
            "head": {"corrId": "c2", "version": "2025-01-15"},
            "data": {"id": "p1"},
        }
        get_resp = await send(net, get_req)
        assert status(get_resp) == 200
        assert data(get_resp)["promise"]["id"] == "p1"

    asyncio.run(run())


def test_local_network_idempotent_promise_create() -> None:
    async def run() -> None:
        net = LocalNetwork()
        req = {
            "kind": "promise.create",
            "head": {"corrId": "c1", "version": "2025-01-15"},
            "data": {"id": "p1", "timeoutAt": I64_MAX, "param": {}, "tags": {}},
        }
        r1 = await send(net, req)
        assert status(r1) in (200, 201)

        r2 = await send(net, req)
        assert status(r2) == 200
        assert data(r2)["promise"]["id"] == "p1"

    asyncio.run(run())


def test_local_network_task_create_and_fulfill() -> None:
    async def run() -> None:
        net = LocalNetwork(pid="pid1")
        req = {
            "kind": "task.create",
            "head": {"corrId": "c1", "version": "2025-01-15"},
            "data": {
                "pid": "pid1",
                "ttl": 60000,
                "action": {
                    "kind": "promise.create",
                    "head": {"corrId": "c1a", "version": "2025-01-15"},
                    "data": {
                        "id": "p1",
                        "timeoutAt": I64_MAX,
                        "param": {"data": "test"},
                        "tags": {},
                    },
                },
            },
        }
        resp = await send(net, req)
        assert status(resp) in (200, 201)
        assert data(resp)["task"]["state"] == "acquired"
        assert data(resp)["promise"]["id"] == "p1"

        task_id = data(resp)["task"]["id"]
        fulfill = {
            "kind": "task.fulfill",
            "head": {"corrId": "c2", "version": "2025-01-15"},
            "data": {
                "id": task_id,
                "version": 0,
                "action": {
                    "kind": "promise.settle",
                    "head": {"corrId": "c2a", "version": "2025-01-15"},
                    "data": {
                        "id": "p1",
                        "state": "resolved",
                        "value": {"data": "result"},
                    },
                },
            },
        }
        f_resp = await send(net, fulfill)
        assert status(f_resp) == 200

    asyncio.run(run())


def test_local_network_identity() -> None:
    net = LocalNetwork(pid="mypid", group="mygroup")
    assert net.pid() == "mypid"
    assert net.group() == "mygroup"
    assert net.unicast() == "local://uni@mygroup/mypid"
    assert net.anycast() == "local://any@mygroup/mypid"
    assert net.target_resolver("target") == "local://any@target"


def test_promise_create_with_target_creates_task_and_dispatches_execute() -> None:
    async def run() -> None:
        net = LocalNetwork(pid="pid1")
        req = {
            "kind": "promise.create",
            "head": {"corrId": "c1", "version": "2025-01-15"},
            "data": {
                "id": "rpc-1",
                "timeoutAt": I64_MAX,
                "param": {"data": "test"},
                "tags": {
                    "resonate:target": "local://any@hello",
                    "resonate:scope": "global",
                },
            },
        }
        resp = await send(net, req)
        assert data(resp)["promise"]["state"] == "pending"

        # The task should exist in pending state.
        task = net.state.tasks["rpc-1"]
        assert task.state == "pending"
        assert task.id == "rpc-1"

    asyncio.run(run())


def test_task_suspend_registers_awaiters_and_suspends() -> None:
    async def run() -> None:
        net = LocalNetwork(pid="pid1")

        # Create a task (acquired).
        create_req = {
            "kind": "task.create",
            "head": {"corrId": "c1", "version": "2025-01-15"},
            "data": {
                "pid": "pid1",
                "ttl": 60000,
                "action": {
                    "kind": "promise.create",
                    "head": {"corrId": "c1a", "version": "2025-01-15"},
                    "data": {
                        "id": "parent",
                        "timeoutAt": I64_MAX,
                        "tags": {"resonate:target": "local://any@wf"},
                    },
                },
            },
        }
        await send(net, create_req)

        # Create a child promise (pending, represents an RPC dependency).
        child_req = {
            "kind": "promise.create",
            "head": {"corrId": "c2", "version": "2025-01-15"},
            "data": {
                "id": "child-1",
                "timeoutAt": I64_MAX,
                "tags": {"resonate:target": "local://any@hello"},
            },
        }
        await send(net, child_req)

        # Suspend the parent task waiting on child.
        suspend_req = {
            "kind": "task.suspend",
            "head": {"corrId": "c3", "version": "2025-01-15"},
            "data": {
                "id": "parent",
                "version": 0,
                "actions": [
                    {
                        "kind": "promise.register_callback",
                        "head": {"corrId": "c3a", "version": "2025-01-15"},
                        "data": {"awaited": "child-1", "awaiter": "parent"},
                    }
                ],
            },
        }
        resp = await send(net, suspend_req)
        assert status(resp) == 200

        assert net.state.tasks["parent"].state == "suspended"
        assert "parent" in net.state.promises["child-1"].awaiters

    asyncio.run(run())


def test_settling_child_resumes_suspended_parent() -> None:
    async def run() -> None:
        net = LocalNetwork(pid="pid1")

        # Create parent task.
        create_req = {
            "kind": "task.create",
            "head": {"corrId": "c1", "version": "2025-01-15"},
            "data": {
                "pid": "pid1",
                "ttl": 60000,
                "action": {
                    "kind": "promise.create",
                    "head": {"corrId": "c1a", "version": "2025-01-15"},
                    "data": {
                        "id": "parent",
                        "timeoutAt": I64_MAX,
                        "tags": {"resonate:target": "local://any@wf"},
                    },
                },
            },
        }
        await send(net, create_req)

        # Create child promise.
        child_req = {
            "kind": "promise.create",
            "head": {"corrId": "c2", "version": "2025-01-15"},
            "data": {
                "id": "child",
                "timeoutAt": I64_MAX,
                "tags": {"resonate:target": "local://any@hello"},
            },
        }
        await send(net, child_req)

        # Suspend parent on child.
        suspend_req = {
            "kind": "task.suspend",
            "head": {"corrId": "c3", "version": "2025-01-15"},
            "data": {
                "id": "parent",
                "version": 0,
                "actions": [
                    {
                        "kind": "promise.register_callback",
                        "head": {"corrId": "c3a", "version": "2025-01-15"},
                        "data": {"awaited": "child", "awaiter": "parent"},
                    }
                ],
            },
        }
        await send(net, suspend_req)

        # Acquire child task then fulfill it.
        acquire_req = {
            "kind": "task.acquire",
            "head": {"corrId": "c4", "version": "2025-01-15"},
            "data": {"id": "child", "version": 0, "pid": "pid1", "ttl": 60000},
        }
        await send(net, acquire_req)

        fulfill_req = {
            "kind": "task.fulfill",
            "head": {"corrId": "c5", "version": "2025-01-15"},
            "data": {
                "id": "child",
                "version": 0,
                "action": {
                    "kind": "promise.settle",
                    "head": {"corrId": "c5a", "version": "2025-01-15"},
                    "data": {
                        "id": "child",
                        "state": "resolved",
                        "value": {"data": "hello"},
                    },
                },
            },
        }
        await send(net, fulfill_req)

        # Parent should be resumed (pending, version incremented).
        parent_task = net.state.tasks["parent"]
        assert parent_task.state == "pending"
        assert parent_task.version == 1

    asyncio.run(run())


def test_task_suspend_redirect_when_dependency_already_settled() -> None:
    async def run() -> None:
        net = LocalNetwork(pid="pid1")

        # Create parent task.
        create_req = {
            "kind": "task.create",
            "head": {"corrId": "c1", "version": "2025-01-15"},
            "data": {
                "pid": "pid1",
                "ttl": 60000,
                "action": {
                    "kind": "promise.create",
                    "head": {"corrId": "c1a", "version": "2025-01-15"},
                    "data": {
                        "id": "parent",
                        "timeoutAt": I64_MAX,
                        "tags": {"resonate:target": "local://any@wf"},
                    },
                },
            },
        }
        await send(net, create_req)

        # Create and immediately settle child promise.
        child_req = {
            "kind": "promise.create",
            "head": {"corrId": "c2", "version": "2025-01-15"},
            "data": {"id": "child", "timeoutAt": I64_MAX, "param": {}, "tags": {}},
        }
        await send(net, child_req)

        settle_req = {
            "kind": "promise.settle",
            "head": {"corrId": "c3", "version": "2025-01-15"},
            "data": {"id": "child", "state": "resolved", "value": {"data": "ok"}},
        }
        await send(net, settle_req)

        # Suspend parent on already-settled child -> should get redirect.
        suspend_req = {
            "kind": "task.suspend",
            "head": {"corrId": "c4", "version": "2025-01-15"},
            "data": {
                "id": "parent",
                "version": 0,
                "actions": [
                    {
                        "kind": "promise.register_callback",
                        "head": {"corrId": "c4a", "version": "2025-01-15"},
                        "data": {"awaited": "child", "awaiter": "parent"},
                    }
                ],
            },
        }
        resp = await send(net, suspend_req)
        assert status(resp) == 300

    asyncio.run(run())


def test_http_network_identity() -> None:
    net = HttpNetwork(
        "http://localhost:8001",
        pid="mypid",
        group="mygroup",
    )
    assert net.pid() == "mypid"
    assert net.group() == "mygroup"
    assert net.unicast() == "poll://uni@mygroup/mypid"
    assert net.anycast() == "poll://any@mygroup/mypid"


def test_http_network_match_returns_poll_anycast() -> None:
    net = HttpNetwork("http://localhost:8001")
    assert net.target_resolver("my-target") == "poll://any@my-target"


def test_http_network_strips_trailing_slash() -> None:
    net = HttpNetwork("http://localhost:8001/", pid="pid")
    assert net._url == "http://localhost:8001"


def test_http_network_default_group() -> None:
    net = HttpNetwork("http://localhost:8001", pid="pid1")
    assert net.group() == "default"
    assert net.unicast() == "poll://uni@default/pid1"
