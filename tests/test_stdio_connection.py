"""The ``stdio://`` connector, against a pair of real pipes.

A pipe rather than a mock, because everything this connector has to get right
is a property of a pipe: a blocking ``readline``, a write that has to be whole,
an end of input that has to fail the requests waiting on it. The peer is the
test itself, playing the host that would otherwise be relaying to a server.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
from typing import IO, TYPE_CHECKING, Any

import pytest
from resonate_base.connections import Network, Source

from resonate.connections import LocalConnection, StdioConnection
from resonate.connections.stdio import (
    MARKER,
    correlation_id,
    frame,
    resolve_target,
    unframe,
    unicast,
)
from resonate.error import ConnectorError
from resonate.resonate import Resonate

if TYPE_CHECKING:
    from collections.abc import Coroutine

# -- helpers ------------------------------------------------------------------


class Pipes:
    """The connector's stdin/stdout, with the peer's ends to drive them from.

    The peer reads on a daemon thread of its own rather than through the
    default executor, so a test that ends with the connection still writing
    leaves nothing for :func:`asyncio.run` to join on the way out.
    """

    def __init__(self) -> None:
        in_r, in_w = os.pipe()
        out_r, out_w = os.pipe()
        #: What the connection reads as its stdin, and writes as its stdout.
        self.stdin: IO[bytes] = os.fdopen(in_r, "rb")
        self.stdout: IO[bytes] = os.fdopen(out_w, "wb")
        #: The peer's ends.
        self._to_conn: IO[bytes] = os.fdopen(in_w, "wb")
        self._from_conn: IO[bytes] = os.fdopen(out_r, "rb")

        self._loop = asyncio.get_running_loop()
        self._lines: asyncio.Queue[str] = asyncio.Queue()
        threading.Thread(target=self._read_loop, daemon=True).start()

    def _read_loop(self) -> None:
        for raw in iter(self._from_conn.readline, b""):
            self._loop.call_soon_threadsafe(
                self._lines.put_nowait, raw.decode("utf-8").rstrip("\r\n")
            )

    def write(self, line: str) -> None:
        """Send one raw line to the connection's stdin."""
        self._to_conn.write(f"{line}\n".encode())
        self._to_conn.flush()

    def respond(self, payload: Any) -> None:
        """Send one framed message to the connection's stdin."""
        self._to_conn.write(frame(json.dumps(payload)).encode())
        self._to_conn.flush()

    def close(self) -> None:
        """Close the connection's stdin, as a host shutting the tunnel would."""
        self._to_conn.close()

    async def read_line(self) -> str:
        """Read one line of the connection's stdout."""
        return await self._lines.get()

    async def read_request(self) -> Any:
        """Read one framed request off the connection's stdout."""
        payload = unframe(await self.read_line())
        assert payload is not None
        return json.loads(payload)


def request(corr_id: str, kind: str = "promise.get") -> str:
    return json.dumps({"kind": kind, "head": {"corrId": corr_id}, "data": {}})


def response(corr_id: str, kind: str = "promise.get") -> dict[str, Any]:
    return {"kind": kind, "head": {"corrId": corr_id, "status": 200}, "data": {}}


# -- framing ------------------------------------------------------------------


def test_a_frame_is_the_marker_the_payload_and_a_newline() -> None:
    assert frame('{"a":1}') == f'{MARKER}{{"a":1}}\n'


def test_unframe_recovers_the_payload() -> None:
    assert unframe(frame('{"a":1}').rstrip("\n")) == '{"a":1}'


def test_output_is_not_a_frame() -> None:
    for line in [
        "hello world",
        "Traceback (most recent call last):",
        '{"level":"info","msg":"starting"}',
        f"  {MARKER}{{}}",  # indented: output, not a frame
        "",
    ]:
        assert unframe(line) is None, line


def test_correlation_id_is_read_off_the_head() -> None:
    assert correlation_id(request("c1")) == "c1"


def test_a_push_message_has_no_correlation_id() -> None:
    assert correlation_id('{"kind":"execute","data":{"task":{"id":"t1"}}}') == ""


def test_an_unparseable_payload_correlates_to_nothing() -> None:
    for payload in ["not json", '{"head":"not an object"}', '{"head":{"corrId":7}}']:
        assert correlation_id(payload) == "", payload


def test_addresses() -> None:
    assert unicast("sandbox", "7f3a") == "stdio://uni@sandbox/7f3a"
    assert resolve_target("workers") == "stdio://any@workers"


# -- seams --------------------------------------------------------------------


def test_the_connection_satisfies_both_protocols() -> None:
    conn = StdioConnection()
    assert isinstance(conn, Network)
    assert isinstance(conn, Source)


def test_addresses_can_be_overridden() -> None:
    conn = StdioConnection(
        pid="7f3a",
        group="sandbox",
        unicast_address="tensorlake://acct/img?process=main",
        target_resolver=lambda t: f"tensorlake://{t}",
    )
    assert conn.unicast() == "tensorlake://acct/img?process=main"
    assert conn.resolve_target("workers") == "tensorlake://workers"


def test_addresses_default_to_the_stdio_scheme() -> None:
    conn = StdioConnection(pid="7f3a", group="sandbox")
    assert conn.unicast() == "stdio://uni@sandbox/7f3a"
    assert conn.resolve_target("workers") == "stdio://any@workers"


# -- request/response ---------------------------------------------------------


def test_a_request_goes_out_framed_and_its_response_comes_back() -> None:
    async def run() -> None:
        pipes = Pipes()
        conn = StdioConnection(stdin=pipes.stdin, stdout=pipes.stdout)
        await conn.start()
        try:
            sending = asyncio.ensure_future(conn.send(request("c1")))
            req = await pipes.read_request()
            assert req["head"]["corrId"] == "c1"
            pipes.respond(response("c1"))
            assert json.loads(await sending)["head"]["corrId"] == "c1"
        finally:
            await conn.stop()

    asyncio.run(run())


def test_responses_are_matched_by_corr_id_not_by_order() -> None:
    async def run() -> None:
        pipes = Pipes()
        conn = StdioConnection(stdin=pipes.stdin, stdout=pipes.stdout)
        await conn.start()
        try:
            first = asyncio.ensure_future(conn.send(request("c1")))
            second = asyncio.ensure_future(conn.send(request("c2")))
            await pipes.read_request()
            await pipes.read_request()
            # Answered in the opposite order to the one they were asked in.
            pipes.respond(response("c2"))
            pipes.respond(response("c1"))
            assert json.loads(await first)["head"]["corrId"] == "c1"
            assert json.loads(await second)["head"]["corrId"] == "c2"
        finally:
            await conn.stop()

    asyncio.run(run())


def test_the_peers_own_output_is_passed_over() -> None:
    async def run() -> None:
        pipes = Pipes()
        seen: list[str] = []
        conn = StdioConnection(
            stdin=pipes.stdin, stdout=pipes.stdout, on_output=seen.append
        )
        await conn.start()
        try:
            sending = asyncio.ensure_future(conn.send(request("c1")))
            await pipes.read_request()
            pipes.write("starting up")
            pipes.write('{"kind":"promise.get","head":{"corrId":"c1"}}')  # unframed
            pipes.respond(response("c1"))
            assert json.loads(await sending)["head"]["corrId"] == "c1"
            assert seen == [
                "starting up",
                '{"kind":"promise.get","head":{"corrId":"c1"}}',
            ]
        finally:
            await conn.stop()

    asyncio.run(run())


def test_a_request_with_no_corr_id_is_refused() -> None:
    async def run() -> None:
        pipes = Pipes()
        conn = StdioConnection(stdin=pipes.stdin, stdout=pipes.stdout)
        await conn.start()
        try:
            with pytest.raises(ConnectorError):
                await conn.send('{"kind":"promise.get","data":{}}')
        finally:
            await conn.stop()

    asyncio.run(run())


def test_a_request_times_out_when_the_peer_stays_silent() -> None:
    async def run() -> None:
        pipes = Pipes()
        conn = StdioConnection(
            stdin=pipes.stdin, stdout=pipes.stdout, request_timeout=0.05
        )
        await conn.start()
        try:
            with pytest.raises(ConnectorError):
                await conn.send(request("c1"))
            # The timed-out request left nothing behind: the same corrId is
            # free to be asked again.
            sending = asyncio.ensure_future(conn.send(request("c1")))
            await pipes.read_request()
            await pipes.read_request()
            pipes.respond(response("c1"))
            assert json.loads(await sending)["head"]["corrId"] == "c1"
        finally:
            await conn.stop()

    asyncio.run(run())


# -- push messages ------------------------------------------------------------


def test_a_frame_with_no_corr_id_is_a_push_message() -> None:
    async def run() -> None:
        pipes = Pipes()
        received: list[str] = []
        conn = StdioConnection(stdin=pipes.stdin, stdout=pipes.stdout)
        conn.recv(received.append)
        await conn.start()
        try:
            execute = {"kind": "execute", "data": {"task": {"id": "t1", "version": 1}}}
            pipes.respond(execute)
            for _ in range(100):
                if received:
                    break
                await asyncio.sleep(0.01)
            assert [json.loads(m) for m in received] == [execute]
        finally:
            await conn.stop()

    asyncio.run(run())


def test_one_receiver_raising_does_not_cost_another_its_message() -> None:
    async def run() -> None:
        pipes = Pipes()
        received: list[str] = []

        def boom(_: str) -> None:
            msg = "receiver is broken"
            raise RuntimeError(msg)

        conn = StdioConnection(stdin=pipes.stdin, stdout=pipes.stdout)
        conn.recv(boom)
        conn.recv(received.append)
        await conn.start()
        try:
            pipes.respond({"kind": "unblock", "data": {}})
            for _ in range(100):
                if received:
                    break
                await asyncio.sleep(0.01)
            assert len(received) == 1
        finally:
            await conn.stop()

    asyncio.run(run())


# -- end of input -------------------------------------------------------------


def test_eof_fails_every_request_in_flight() -> None:
    async def run() -> None:
        pipes = Pipes()
        conn = StdioConnection(stdin=pipes.stdin, stdout=pipes.stdout)
        await conn.start()
        try:
            sending = asyncio.ensure_future(conn.send(request("c1")))
            await pipes.read_request()
            pipes.close()
            with pytest.raises(ConnectorError):
                await sending
        finally:
            await conn.stop()

    asyncio.run(run())


def test_eof_fails_later_requests_rather_than_hanging_them() -> None:
    async def run() -> None:
        pipes = Pipes()
        conn = StdioConnection(stdin=pipes.stdin, stdout=pipes.stdout)
        await conn.start()
        try:
            pipes.close()
            await asyncio.wait_for(conn.wait_closed(), timeout=2)
            with pytest.raises(ConnectorError):
                await conn.send(request("c1"))
        finally:
            await conn.stop()

    asyncio.run(run())


def test_wait_closed_returns_when_the_peer_closes_stdin() -> None:
    async def run() -> None:
        pipes = Pipes()
        conn = StdioConnection(stdin=pipes.stdin, stdout=pipes.stdout)
        await conn.start()
        try:
            waiting = asyncio.ensure_future(conn.wait_closed())
            assert not waiting.done()
            pipes.close()
            await asyncio.wait_for(waiting, timeout=2)
        finally:
            await conn.stop()

    asyncio.run(run())


# -- lifecycle ----------------------------------------------------------------


def test_start_is_idempotent() -> None:
    async def run() -> None:
        pipes = Pipes()
        conn = StdioConnection(stdin=pipes.stdin, stdout=pipes.stdout)
        await conn.start()
        await conn.start()
        try:
            sending = asyncio.ensure_future(conn.send(request("c1")))
            await pipes.read_request()
            pipes.respond(response("c1"))
            await sending
        finally:
            await conn.stop()

    asyncio.run(run())


def test_stop_fails_what_is_in_flight_and_refuses_what_follows() -> None:
    async def run() -> None:
        pipes = Pipes()
        conn = StdioConnection(stdin=pipes.stdin, stdout=pipes.stdout)
        await conn.start()
        sending = asyncio.ensure_future(conn.send(request("c1")))
        await pipes.read_request()
        await conn.stop()
        with pytest.raises(ConnectorError):
            await sending
        with pytest.raises(ConnectorError):
            await conn.send(request("c2"))
        with pytest.raises(ConnectorError):
            await conn.start()

    asyncio.run(run())


# -- end to end ---------------------------------------------------------------


def test_a_whole_worker_runs_over_the_tunnel() -> None:
    """A ``Resonate`` whose only channel is a pipe pair does real work.

    Everything above tests the connector in isolation. This tests the claim the
    connector exists to make: that a process with no address, no url and no
    credential can acquire a task, run it and settle the promise, provided
    something on the other end of its stdio is relaying.

    The relay here is the test, standing in for the host -- it reads a frame off
    the worker's stdout, applies it against an in-process
    :class:`~resonate.connections.LocalConnection`, and writes the response
    back. The dispatching client shares that same server and listens for
    nothing itself, so its handle settles by re-reading the promise.
    """

    async def greet(ctx: Any, name: str) -> str:
        return f"hello, {name}!"

    async def run() -> None:
        pipes = Pipes()
        server = LocalConnection(group="sandbox")
        relay = _Relay(pipes, server)

        worker = Resonate(
            network=StdioConnection(
                stdin=pipes.stdin, stdout=pipes.stdout, group="sandbox"
            ),
            group="sandbox",
            autostart=False,
        )
        worker.register(greet)
        client = Resonate(
            network=server,
            sources=[],
            group="sandbox",
            subscription_refresh_secs=0.05,
            autostart=False,
        )

        server.recv(relay.push)
        pumping = asyncio.ensure_future(relay.pump())
        worker.start()
        client.start()
        try:
            handle = client.options(target="sandbox").rpc("e2e", "greet", "world")
            result = await asyncio.wait_for(handle.result(), timeout=30)
            assert result == "hello, world!"
        finally:
            pumping.cancel()
            await client.stop()
            await worker.stop()

    asyncio.run(run())


class _Relay:
    """The host end of the tunnel: worker frames in, server responses out."""

    def __init__(self, pipes: Pipes, server: LocalConnection) -> None:
        self._pipes = pipes
        self._server = server
        self._tasks: set[asyncio.Task[None]] = set()

    async def pump(self) -> None:
        """Answer every request the worker writes, until its stdout ends."""
        while True:
            payload = unframe(await self._pipes.read_line())
            if payload is not None:
                self._spawn(self._answer(payload))

    def push(self, message: str) -> None:
        """Forward one server push message to the worker."""
        self._pipes.respond(json.loads(message))

    async def _answer(self, request: str) -> None:
        self._pipes.respond(json.loads(await self._server.send(request)))

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
