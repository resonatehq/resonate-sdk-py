"""The one-task entry point, against a real in-process server.

What is under test is the shape rather than the execution: that a handler runs
the task it is named, reports how it ended, opens and closes its network around
exactly that, and never listens for anything. Execution itself is
:class:`~resonate.core.Core`'s, and the rest of the suite covers it.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING

import pytest
from resonate_base.connections import Source

from resonate.connections import LocalConnection
from resonate.error import ApplicationError, ServerError
from resonate.handler import TASK_ID_ENV, TASK_VERSION_ENV, Handler
from resonate.heartbeat import NoopHeartbeat
from resonate.resonate import Resonate
from resonate.retry import Never

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from resonate.context import Context


# -- helpers ------------------------------------------------------------------


async def greet(ctx: Context, name: str) -> str:
    return f"hello, {name}!"


async def awaits_a_child(ctx: Context) -> str:
    return await ctx.rpc("nobody_runs_this")


@contextlib.asynccontextmanager
async def dispatcher(server: LocalConnection) -> AsyncIterator[Resonate]:
    """Yield a client that creates work on ``server`` and listens for nothing.

    A handler executes work; something else has to create it. ``sources=[]``
    keeps this side from picking the task up itself, which is what leaves it
    for the handler under test.
    """
    client = Resonate(
        network=server,
        sources=[],
        retry_policy=Never(),
        subscription_refresh_secs=0,
    )
    try:
        yield client
    finally:
        await client.stop()


async def wait_for_promise(client: Resonate, id: str) -> None:
    """Block until ``id`` exists, since ``rpc`` creates it in the background."""
    for _ in range(500):
        with contextlib.suppress(ServerError):
            await client.promises.get(id)
            return
        await asyncio.sleep(0)
    msg = f"promise {id} was never created"
    raise AssertionError(msg)


# -- shape --------------------------------------------------------------------


def test_a_handler_is_not_a_source() -> None:
    """It advertises no address because nothing delivers to it.

    The distinction the class exists for: a worker is reachable, a handler is
    told.
    """
    handler = Handler(network=LocalConnection())
    assert not isinstance(handler, Source)
    # No address to advertise, nothing to deliver to it, and no client surface:
    # a handler executes work, it does not create it. (``run`` is absent from
    # this list because a handler has one -- it means *execute this task*, not
    # ``Resonate.run``'s *dispatch this function*.)
    for absent in ("unicast", "resolve_target", "recv", "rpc", "get", "schedule"):
        assert not hasattr(handler, absent), absent


def test_register_works_bare_and_parameterized() -> None:
    handler = Handler(network=LocalConnection())

    @handler.register
    async def one(ctx: Context) -> None: ...

    @handler.register(name="two-by-name", version=3)
    async def two(ctx: Context) -> None: ...

    assert handler._registry.get("one") is not None
    assert handler._registry.get("two-by-name", 3) is not None
    # Returned unchanged, so the decorated name is still the function.
    assert one.__name__ == "one"
    assert two.__name__ == "two"


def test_an_anonymous_function_needs_a_name() -> None:
    handler = Handler(network=LocalConnection())

    def anonymous(ctx: Context) -> None: ...

    anonymous.__name__ = ""
    with pytest.raises(ApplicationError):
        handler.register(anonymous)


def test_a_local_network_gets_a_noop_heartbeat() -> None:
    """Mirrors ``Resonate``: there is no lease to beat against a simulation."""
    assert isinstance(Handler(network=LocalConnection())._heartbeat, NoopHeartbeat)


def test_a_child_target_resolves_as_it_would_under_a_worker() -> None:
    handler = Handler(network=LocalConnection(), group="workers")
    assert handler._resolve_target(None) == "poll://any@workers"
    assert handler._resolve_target("other") == "poll://any@other"
    # An address passes through, so a child can be sent anywhere nameable.
    assert handler._resolve_target("tensorlake://prod") == "tensorlake://prod"


# -- running one task ---------------------------------------------------------


@pytest.mark.asyncio
async def test_run_settles_the_task_it_is_named() -> None:
    server = LocalConnection()
    async with dispatcher(server) as client:
        client.rpc("h-done", "greet", "world")
        await wait_for_promise(client, "h-done")

        handler = Handler(network=server, retry_policy=Never())
        handler.register(greet)
        assert await handler.run("h-done") == "done"
        assert (await client.promises.get("h-done")).state == "resolved"


@pytest.mark.asyncio
async def test_run_says_suspended_rather_than_leaving_it_to_be_guessed() -> None:
    """The status a per-task process cannot read off the promise.

    A suspended function has settled nothing, so ``h-suspend`` is ``pending``
    either way -- which is why "waiting on a child" has to be the return value.
    """
    server = LocalConnection()
    async with dispatcher(server) as client:
        client.rpc("h-suspend", "awaits_a_child")
        await wait_for_promise(client, "h-suspend")

        handler = Handler(network=server, retry_policy=Never())
        handler.register(awaits_a_child)
        assert await handler.run("h-suspend") == "suspended"
        assert (await client.promises.get("h-suspend")).state == "pending"


@pytest.mark.asyncio
async def test_a_dependency_reaches_the_function() -> None:
    class Greeting:
        text = "howdy"

    async def uses_dep(ctx: Context, name: str) -> str:
        return f"{ctx.get_dependency(Greeting).text}, {name}!"

    server = LocalConnection()
    async with dispatcher(server) as client:
        client.rpc("h-dep", "uses_dep", "world")
        await wait_for_promise(client, "h-dep")

        handler = Handler(network=server, retry_policy=Never())
        handler.with_dependency(Greeting())
        handler.register(uses_dep)
        assert await handler.run("h-dep") == "done"


@pytest.mark.asyncio
async def test_run_propagates_a_failure_to_acquire() -> None:
    """No observer to swallow it: the caller is the process's exit code."""
    handler = Handler(network=LocalConnection(), retry_policy=Never())
    with pytest.raises(ServerError):
        await handler.run("no-such-task")


# -- the network's lifetime is the call ---------------------------------------


class RecordingNetwork:
    """A network that records its lifecycle and refuses to say anything else."""

    def __init__(self, inner: LocalConnection) -> None:
        self._inner = inner
        self.events: list[str] = []

    async def start(self) -> None:
        self.events.append("start")
        await self._inner.start()

    async def stop(self) -> None:
        self.events.append("stop")
        await self._inner.stop()

    async def send(self, req: str, headers: dict[str, str] | None = None) -> str:
        return await self._inner.send(req, headers)


@pytest.mark.asyncio
async def test_the_network_is_started_and_stopped_around_the_task() -> None:
    server = LocalConnection()
    async with dispatcher(server) as client:
        client.rpc("h-lifecycle", "greet", "world")
        await wait_for_promise(client, "h-lifecycle")

        network = RecordingNetwork(server)
        handler = Handler(network=network, retry_policy=Never())
        handler.register(greet)
        assert not network.events  # inert until run
        await handler.run("h-lifecycle")
        assert network.events == ["start", "stop"]


@pytest.mark.asyncio
async def test_the_network_is_stopped_even_when_the_task_raises() -> None:
    network = RecordingNetwork(LocalConnection())
    handler = Handler(network=network, retry_policy=Never())
    with pytest.raises(ServerError):
        await handler.run("no-such-task")
    assert network.events == ["start", "stop"]


# -- run_from_env -------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_from_env_runs_the_task_the_process_was_started_for() -> None:
    server = LocalConnection()
    async with dispatcher(server) as client:
        client.rpc("h-env", "greet", "world")
        await wait_for_promise(client, "h-env")

        handler = Handler(
            network=server,
            retry_policy=Never(),
            env={TASK_ID_ENV: "h-env", TASK_VERSION_ENV: "0"},
        )
        handler.register(greet)
        assert await handler.run_from_env() == "done"


@pytest.mark.asyncio
async def test_run_from_env_defaults_the_version() -> None:
    server = LocalConnection()
    async with dispatcher(server) as client:
        client.rpc("h-env-noversion", "greet", "world")
        await wait_for_promise(client, "h-env-noversion")

        handler = Handler(
            network=server, retry_policy=Never(), env={TASK_ID_ENV: "h-env-noversion"}
        )
        handler.register(greet)
        assert await handler.run_from_env() == "done"


@pytest.mark.asyncio
async def test_a_missing_task_id_is_a_deployment_fault_not_a_task_failure() -> None:
    """Raised before the network is touched, so no lease is taken."""
    network = RecordingNetwork(LocalConnection())
    handler = Handler(network=network, env={})
    with pytest.raises(ApplicationError):
        await handler.run_from_env()
    assert network.events == []


@pytest.mark.asyncio
async def test_an_unparseable_version_is_refused_up_front() -> None:
    network = RecordingNetwork(LocalConnection())
    handler = Handler(
        network=network, env={TASK_ID_ENV: "t", TASK_VERSION_ENV: "not a number"}
    )
    with pytest.raises(ApplicationError):
        await handler.run_from_env()
    assert network.events == []


def test_the_env_is_a_parameter_so_the_default_is_the_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(TASK_ID_ENV, "from-the-process")
    handler = Handler(network=LocalConnection())
    assert handler._env[TASK_ID_ENV] == "from-the-process"
