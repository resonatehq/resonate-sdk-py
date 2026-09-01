r"""The ``stdio://`` connector: the Resonate protocol over a process's own stdio.

Both halves of the conversation ride on the two file descriptors every process
already has. Requests go out on **stdout**, responses and push messages come
back in on **stdin**. There is no socket, no port, no url and no credential --
which is the point: a process with no route to a Resonate server can still
speak to one, provided whoever started it is willing to relay.

The roles are inverted from the usual. This process is the *client*, and its
parent -- the thing holding the other ends of the pipes -- is the server, or a
gateway standing in for one. That is exactly the shape a sandboxed worker
needs: the sandbox writes a request, the host applies it against the real
server and writes the response back, and the code inside needs no egress at
all.

Framing
=======

A pipe carrying a protocol also carries whatever the program prints. So a
protocol message is not merely a line of JSON -- it is a line of JSON behind a
marker::

    RN8:{"kind":"promise.create","head":{"corrId":"c1",...},"data":{...}}\n

One line, one message, newline-terminated, :data:`MARKER` in front. Everything
else on the stream is the program's own output and is passed over: a log line,
a traceback, someone's structured logging that happens to have a ``kind`` field
of its own. Without the marker those are indistinguishable from protocol
traffic, and the failure mode is silent -- a stack trace answered with a
protocol error, or a request mistaken for a log and never answered.

The marker earns the interleaving guarantee, not just the discrimination one:
because the peer skips lines it does not recognise, this connector may share
stdout with ``print`` without corrupting either. What it cannot defend against
is a *partial* line -- a ``print`` with no newline, still sitting in
:data:`sys.stdout`'s buffer -- so :meth:`~StdioConnection.send` flushes that
buffer before every frame. Programs that want the guarantee outright should
write their output to stderr.

Correlation
===========

Requests are multiplexed: several may be in flight, and responses may come back
in any order, so each is matched to its request by the ``head.corrId`` the SDK
already stamps on it. This is the one place a connector reads inside ``req``
(see :class:`~resonate_base.connections.Network`), and it is unavoidable here:
a single duplex pipe has no envelope of its own to carry a correlation token
in, the way an HTTP response has its own request to belong to.

An inbound frame *without* a ``corrId`` is not a response at all -- it is a
push message (``execute``/``unblock``), and it goes to the
:class:`~resonate_base.connections.Source` receivers. That is why this class
implements both seams: one duplex pipe is one channel, and splitting it in two
would mean framing the same stream twice.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import IO, TYPE_CHECKING

import msgspec

from resonate.error import ConnectorError

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================

#: URL scheme of the delivery addresses this connector advertises. Like every
#: connector, it owns the syntax past the scheme: the server parses an address,
#: dispatches on the scheme, and hands the rest back to whoever minted it.
SCHEME = "stdio"

#: Userinfo marking an address that reaches exactly one process.
UNICAST = "uni"

#: Userinfo marking an address that reaches any one member of a group.
ANYCAST = "any"

#: What tells a protocol message from the program's own output. Chosen to be
#: short, unambiguous at the head of a line, and vanishingly unlikely to open a
#: line anyone prints on purpose.
MARKER = "RN8:"


# =============================================================================
# Addresses
# =============================================================================


def unicast(group: str, pid: str) -> str:
    """Mint the address that reaches one process alone.

    Total by design, like every connector's address minting: it only formats.
    An address is opaque to the SDK and to the server alike, and a malformed
    one is declined at delivery, which is observable.

    Worth knowing before relying on it: a ``stdio://`` address only means
    anything to a server that has a transport for the scheme. A process whose
    host reaches it by some *other* route -- a sandbox its host addresses as
    ``tensorlake://...``, say -- should advertise that address instead, via
    :class:`StdioConnection`'s ``unicast_address``.

    >>> unicast("sandbox", "7f3a")
    'stdio://uni@sandbox/7f3a'
    """
    return f"{SCHEME}://{UNICAST}@{group}/{pid}"


def resolve_target(target: str) -> str:
    """Mint the address that reaches any one member of ``target``'s group.

    No pid, and that is the asymmetry with :func:`unicast`: a unicast address
    names a concrete process, while a routing target names a *group* whose
    members whoever resolves it does not know.

    Total, for the same reason as :func:`unicast` -- and doubly so here, since
    a target is resolved lazily while a durable op builds its request. Raising
    there would be caught by the workflow's own error boundary and permanently
    reject the durable promise.

    >>> resolve_target("workers")
    'stdio://any@workers'
    """
    return f"{SCHEME}://{ANYCAST}@{target}"


# =============================================================================
# Framing -- pure, no IO
# =============================================================================


def frame(payload: str) -> str:
    r"""Wrap one protocol message for the wire: marker, payload, newline.

    >>> frame('{"kind":"promise.get"}')
    'RN8:{"kind":"promise.get"}\n'
    """
    return f"{MARKER}{payload}\n"


def unframe(line: str) -> str | None:
    """Return the payload of a framed line, or ``None`` if it is not one.

    The marker must open the line: leading whitespace makes it output, not a
    frame. That is deliberate -- a program that indents its logs should not be
    able to spoof the protocol by accident, and a peer that writes frames has
    no reason to indent them.

    >>> unframe('RN8:{"kind":"promise.get"}')
    '{"kind":"promise.get"}'
    >>> unframe('  RN8:{"kind":"promise.get"}') is None
    True
    >>> unframe("hello, world") is None
    True
    """
    if not line.startswith(MARKER):
        return None
    return line[len(MARKER) :].strip()


class _Head(msgspec.Struct, kw_only=True, rename="camel"):
    corr_id: str = ""


class _Correlated(msgspec.Struct, kw_only=True):
    """Just enough of an envelope to route it. Everything else is ignored.

    Decoding into this rather than a ``dict`` keeps the read shallow and total:
    a payload with a ``head`` that is not an object, or a ``corrId`` that is
    not a string, is a payload with no correlation id -- not an exception on
    the read path.
    """

    head: _Head = msgspec.field(default_factory=_Head)


def correlation_id(payload: str) -> str:
    """Read ``head.corrId`` out of an envelope, or ``""`` if it has none.

    Never raises. A payload this cannot parse is one nothing is waiting on,
    which is a routing answer rather than an error.

    >>> correlation_id('{"kind":"promise.get","head":{"corrId":"c1"}}')
    'c1'
    >>> correlation_id('{"kind":"execute","data":{}}')
    ''
    >>> correlation_id("not json at all")
    ''
    """
    try:
        return msgspec.json.decode(
            payload.encode("utf-8"), type=_Correlated
        ).head.corr_id
    except msgspec.MsgspecError:
        return ""


# =============================================================================
# Connection
# =============================================================================


class StdioConnection:
    """The Resonate protocol over this process's own stdin and stdout.

    Implements both :class:`~resonate_base.connections.Network` and
    :class:`~resonate_base.connections.Source`: a duplex pipe is one channel,
    and framing it twice to split the seams would buy nothing.

    A single instance serves as both, so a whole worker is::

        Resonate(network=StdioConnection(), group="sandbox")

    -- a dual-role connection passed as ``network`` doubles as the sole source.

    Reads and writes happen on threads, not on the event loop: ``readline`` on
    a pipe blocks, and so does a write to a pipe whose reader has fallen
    behind. The reader thread is a daemon and hands every line back through
    :meth:`asyncio.loop.call_soon_threadsafe`, so callbacks still run on the
    loop; writes go through a single-threaded executor, which is what makes a
    frame atomic against another frame and keeps them in order.

    **End of input ends the conversation.** When the peer closes this process's
    stdin no further response can arrive, so every request still in flight
    fails with :class:`~resonate_base.error.ConnectorError` rather than hanging
    forever, and so does every later :meth:`send`. :meth:`wait_closed` is that
    same moment as something to await -- a program's cue to shut down.
    """

    def __init__(
        self,
        stdin: IO[bytes] | None = None,
        stdout: IO[bytes] | None = None,
        pid: str | None = None,
        group: str | None = None,
        unicast_address: str | None = None,
        target_resolver: Callable[[str], str] | None = None,
        request_timeout: float | None = None,
        on_output: Callable[[str], None] | None = None,
    ) -> None:
        """Wire a connection over ``stdin``/``stdout``.

        ``stdin`` and ``stdout`` are the test seam and the embedding seam at
        once: pass a pair of pipes and this speaks the protocol over them
        instead of over the real stdio, which is how the suite exercises it
        without a subprocess. They default to :data:`sys.stdin` and
        :data:`sys.stdout`'s binary buffers, resolved at :meth:`start` so a
        program is free to rebind either beforehand.

        ``pid`` and ``group`` name this process in the addresses it advertises.
        :class:`~resonate.resonate.Resonate` passes its own.

        ``unicast_address`` and ``target_resolver`` override those addresses
        outright, for a process whose host reaches it by a route that is not
        ``stdio://`` (see :func:`unicast`).

        ``request_timeout`` bounds the wait for a response, in seconds. Off by
        default: a request may legitimately be slow, and a peer that has gone
        away closes the pipe, which fails the request without a timer. Set it
        when the peer might stay open while answering nothing.

        ``on_output`` receives every inbound line that is *not* a frame. Unset,
        those lines are logged at debug and dropped -- there is nothing else to
        do with a peer's stray output.
        """
        self._stdin_arg = stdin
        self._stdout_arg = stdout
        self._pid = pid if pid is not None else "default"
        self._group = group if group is not None else "default"
        self._unicast = (
            unicast_address
            if unicast_address is not None
            else unicast(self._group, self._pid)
        )
        self._resolve_target = (
            target_resolver if target_resolver is not None else resolve_target
        )
        self._request_timeout = request_timeout
        self._on_output = on_output

        self._stdin: IO[bytes] | None = None
        self._stdout: IO[bytes] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._reader: threading.Thread | None = None
        self._writer: ThreadPoolExecutor | None = None

        #: In-flight requests by correlation id. Only ever touched on the event
        #: loop -- ``send`` runs there, and the reader thread reaches it only
        #: through ``call_soon_threadsafe`` -- so it needs no lock.
        self._pending: dict[str, asyncio.Future[str]] = {}
        self._receivers: list[Callable[[str], None]] = []

        #: Set once stdin reaches EOF: no response can arrive after it.
        self._eof = asyncio.Event()
        #: True only after :meth:`stop`, which is distinct from "not started".
        self._stopped = False

    # -- addresses ------------------------------------------------------------

    def unicast(self) -> str:
        return self._unicast

    def resolve_target(self, target: str) -> str:
        return self._resolve_target(target)

    # -- lifecycle ------------------------------------------------------------

    async def start(self) -> None:
        """Open the streams and spawn the reader. Idempotent.

        Refuses to reopen after :meth:`stop`: the pipes are the process's own
        and a restarted reader would be a second thread on the same
        descriptor.
        """
        if self._stopped:
            msg = "connection has been stopped"
            raise ConnectorError(RuntimeError(msg))
        if self._reader is not None:
            return

        self._loop = asyncio.get_running_loop()
        self._stdin = (
            self._stdin_arg if self._stdin_arg is not None else sys.stdin.buffer
        )
        self._stdout = (
            self._stdout_arg if self._stdout_arg is not None else sys.stdout.buffer
        )
        self._writer = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="resonate-stdio-write"
        )
        self._reader = threading.Thread(
            target=self._read_loop,
            args=(self._stdin,),
            name="resonate-stdio-read",
            daemon=True,
        )
        self._reader.start()

    async def stop(self) -> None:
        """Stop writing and fail everything still in flight.

        The streams themselves are left open. They belong to the process, not
        to this object, and closing stdout would take the program's own output
        down with the protocol.

        The reader thread is not joined -- it is parked in a blocking
        ``readline`` that only the peer can end, which is why it is a daemon.
        It observes ``_stopped`` on its next line and drops it.
        """
        self._stopped = True
        self._fail_pending(RuntimeError("connection has been stopped"))
        self._receivers.clear()
        writer, self._writer = self._writer, None
        if writer is not None:
            writer.shutdown(wait=False, cancel_futures=True)

    async def wait_closed(self) -> None:
        """Block until the peer closes this process's stdin.

        The natural "run until the host is done with me" for a program whose
        only channel is its stdio: there is no port to serve and no signal to
        wait for, just an input stream that ends.
        """
        await self._eof.wait()

    # -- network --------------------------------------------------------------

    async def send(self, req: str, headers: dict[str, str] | None = None) -> str:
        """Write a request as one frame and await the response with its ``corrId``.

        The lineage origin rides in ``headers`` under ``resonate:origin`` but is
        unused here: there is exactly one peer, so there is nothing to route
        between. It exists on the seam for substrates that shard.

        Failures are not retried, and that is a property of the substrate
        rather than a simplification: a pipe that breaks does not come back,
        and the peer is the only thing that could re-open it. What would be an
        endless backoff over HTTP is an immediate
        :class:`~resonate_base.error.ConnectorError` here, which releases the
        task instead of holding its lease.
        """
        if self._stopped:
            msg = "connection has been stopped"
            raise ConnectorError(RuntimeError(msg))
        await self.start()
        if self._eof.is_set():
            msg = "peer closed stdin"
            raise ConnectorError(EOFError(msg))

        corr_id = correlation_id(req)
        if not corr_id:
            # Nothing to match a response to. Refusing beats writing a request
            # whose answer could only ever be dropped as unsolicited.
            msg = f"request has no head.corrId to correlate a response by: {req}"
            raise ConnectorError(ValueError(msg))
        if corr_id in self._pending:
            msg = f"a request with corrId {corr_id!r} is already in flight"
            raise ConnectorError(ValueError(msg))

        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        self._pending[corr_id] = future
        try:
            logger.debug("stdio_connection req: %s", req)
            await self._write(frame(req))
            if self._request_timeout is None:
                resp = await future
            else:
                resp = await asyncio.wait_for(future, self._request_timeout)
        except TimeoutError as exc:
            msg = f"no response for corrId {corr_id!r} within {self._request_timeout}s"
            raise ConnectorError(TimeoutError(msg)) from exc
        finally:
            self._pending.pop(corr_id, None)
        logger.debug("stdio_connection res: %s", resp)
        return resp

    # -- source ---------------------------------------------------------------

    def recv(self, callback: Callable[[str], None]) -> None:
        """Register a receiver for push messages. Call before :meth:`start`."""
        self._receivers.append(callback)

    # -- internals ------------------------------------------------------------

    async def _write(self, text: str) -> None:
        """Hand one frame to the writer thread and wait for it to land."""
        writer, out = self._writer, self._stdout
        if writer is None or out is None:
            msg = "connection is not started"
            raise ConnectorError(RuntimeError(msg))
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(writer, self._write_blocking, out, text)
        except (OSError, ValueError, RuntimeError) as exc:
            # A closed or broken stdout, or an executor shut down by a
            # concurrent ``stop``. Either way the request never left.
            raise ConnectorError(exc) from exc

    @staticmethod
    def _write_blocking(out: IO[bytes], text: str) -> None:
        """Write one frame, whole, on the writer thread.

        :data:`sys.stdout` is flushed first when it is the text layer over
        ``out``: a ``print`` with no newline yet is still sitting in that
        buffer, and writing past it would put the marker in the middle of the
        program's line rather than at the head of its own.
        """
        text_layer = sys.stdout
        if getattr(text_layer, "buffer", None) is out:
            text_layer.flush()
        out.write(text.encode("utf-8"))
        out.flush()

    def _read_loop(self, stdin: IO[bytes]) -> None:
        """Read lines until EOF, on the reader thread.

        Every decision about a line is left to the event loop; this does the
        blocking read, the decode, and nothing else. A line that is not valid
        UTF-8 is not dropped but replaced through -- a mangled log line is
        still worth seeing, and a mangled frame fails its own JSON parse, which
        is the more precise complaint.
        """
        try:
            for raw in iter(stdin.readline, b""):
                if self._stopped:
                    return
                line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                if not self._on_loop(self._on_line, line):
                    return
        except (OSError, ValueError) as exc:
            # stdin closed under us. Indistinguishable from EOF as far as
            # anything waiting on a response is concerned.
            logger.debug("stdio_connection read failed: %s", exc)
        self._on_loop(self._on_eof)

    def _on_loop(self, fn: Callable[..., None], *args: object) -> bool:
        """Schedule ``fn`` on the event loop; ``False`` once the loop is gone."""
        loop = self._loop
        if loop is None:
            return False
        try:
            loop.call_soon_threadsafe(fn, *args)
        except RuntimeError:
            # The loop closed while the daemon reader was still parked in
            # ``readline``. Nothing left to deliver to.
            return False
        return True

    def _on_line(self, line: str) -> None:
        """Route one inbound line: response, push message, or output."""
        payload = unframe(line)
        if payload is None:
            if self._on_output is not None:
                self._on_output(line)
            else:
                logger.debug("stdio_connection output: %s", line)
            return

        corr_id = correlation_id(payload)
        if not corr_id:
            # No correlation id, so nothing is waiting on it: a push message.
            self._dispatch(payload)
            return

        future = self._pending.pop(corr_id, None)
        if future is None:
            # A response to a request that has already been given up on: timed
            # out, cancelled, or failed by :meth:`stop` while it was in flight.
            # The last of those happens on every clean shutdown -- a heartbeat
            # is usually mid-round-trip -- so this is expected traffic, not a
            # fault, and dropping it is the whole of the handling.
            logger.debug("stdio_connection unmatched response: corrId=%s", corr_id)
            return
        if not future.done():
            future.set_result(payload)

    def _dispatch(self, payload: str) -> None:
        """Hand one push message to every receiver.

        A receiver that raises must not cost the others their message, nor take
        down the reader path -- so each is called in its own guard.
        """
        for callback in list(self._receivers):
            try:
                callback(payload)
            except Exception:
                logger.exception("stdio_connection receiver failed")

    def _on_eof(self) -> None:
        """Record that the peer closed stdin: no response can arrive after it."""
        if self._eof.is_set():
            return
        self._eof.set()
        logger.debug("stdio_connection stdin closed")
        self._fail_pending(EOFError("peer closed stdin"))

    def _fail_pending(self, cause: Exception) -> None:
        """Fail every in-flight request with ``cause``."""
        pending, self._pending = self._pending, {}
        for future in pending.values():
            if not future.done():
                future.set_exception(ConnectorError(cause))
