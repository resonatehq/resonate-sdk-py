from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from typing import TYPE_CHECKING

import aiohttp

from resonate.error import HttpError

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================

_INITIAL_BACKOFF_SECS = 1
_MAX_BACKOFF_SECS = 60

#: Total connection cap for the shared :class:`aiohttp.ClientSession`. aiohttp's
#: default ``TCPConnector`` caps at 100; under a heavy fan-out (e.g. a deep
#: recursive workflow whose rpc children are all dispatched back to this worker)
#: that pool saturates with acquire/create/suspend traffic, and the periodic
#: ``task.heartbeat`` POST queues behind it -- so leases lapse and the server
#: re-delivers them, a self-amplifying 409 storm. Raising the cap well above the
#: execution-concurrency ceiling (see
#: ``resonate.resonate.DEFAULT_MAX_CONCURRENT_TASKS``) guarantees the heartbeat
#: always finds a free connection. The long-lived SSE ``GET`` occupies one slot.
DEFAULT_CONN_LIMIT = 256


class HttpNetwork:
    """:class:`Network` that communicates with a Resonate server over HTTP.

    Mirrors Rust's ``HttpNetwork``:

    - Requests are sent via ``POST /`` (JSON envelope format).
    - Incoming messages (execute/unblock) are received via SSE on
      ``GET /poll/{group}/{pid}``.
    - Addresses use the ``poll://`` scheme: ``poll://uni@group/id`` and
      ``poll://any@group/id``.

    asyncio specifics: the SSE listener runs as an :func:`asyncio.create_task`
    (mirror of tokio::spawn) and ``aiohttp`` plays the role of Rust's
    ``reqwest`` client. ``recv`` appends the callback directly (single-threaded
    asyncio needs no write-lock spawn), and callbacks fire on the event loop as
    SSE events arrive.
    """

    def __init__(
        self,
        url: str,
        pid: str | None = None,
        group: str | None = None,
        auth: str | None = None,
        conn_limit: int | None = None,
    ) -> None:
        self._pid = pid if pid is not None else uuid.uuid4().hex
        self._group = group if group is not None else "default"
        self._unicast = f"poll://uni@{self._group}/{self._pid}"
        self._anycast = f"poll://any@{self._group}/{self._pid}"
        # Strip trailing slash(es) from url.
        self._url = url.rstrip("/")
        self._auth = auth
        self._conn_limit = conn_limit if conn_limit is not None else DEFAULT_CONN_LIMIT

        self._subscribers: list[Callable[[str], None]] = []
        self._session: aiohttp.ClientSession | None = None
        self._sse_handle: asyncio.Task[None] | None = None
        self._running: bool = False
        # Awaitable mirror of ``_running``: lets a ``send`` parked in the
        # retry backoff wake immediately on :meth:`stop`. Without it, an
        # in-flight request retrying through a server outage would block the
        # bounded join inside :meth:`~resonate.resonate.Resonate.stop`.
        self._stop_event = asyncio.Event()

    def pid(self) -> str:
        return self._pid

    def group(self) -> str:
        return self._group

    def unicast(self) -> str:
        return self._unicast

    def anycast(self) -> str:
        return self._anycast

    async def start(self) -> None:
        """Start the SSE listener for incoming messages from the server."""
        self._running = True
        self._stop_event.clear()
        self._sse_handle = asyncio.create_task(self._sse_loop())

    async def stop(self) -> None:
        self._running = False
        # Wake any ``send`` parked in the retry backoff so the bounded join in
        # :meth:`~resonate.resonate.Resonate.stop` does not stall.
        self._stop_event.set()
        handle = self._sse_handle
        self._sse_handle = None
        if handle is not None:
            handle.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await handle
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._subscribers.clear()

    async def send(self, req: str) -> str:
        """Send a request to the Resonate server via ``POST /``.

        Retries on transport-level connection failures with the same
        exponential backoff the SSE loop already uses (``1s → 60s``): the SDK
        must survive the server being down at startup, restarting mid-flight,
        or briefly unreachable. Without this, *any* request initiated while
        the server is dead -- ``task.create``/``promise.create`` from
        :class:`~resonate.resonate.Resonate.run`/``rpc``, plus every
        ``task.acquire``/``task.fulfill``/``task.suspend``/``promise.settle``
        the workflow's :class:`~resonate.core.Core` issues mid-execution --
        would propagate :class:`HttpError`, abandon the in-flight task, and
        strand the handle awaiting an ``unblock`` that never comes.
        Asymmetric resilience (only the SSE half reconnects) would be worse
        than none.

        :class:`aiohttp.ClientError` (the underlying ``HttpError`` cause) is
        the only signal retried; an HTTP response of any status still returns
        normally so :class:`~resonate.error.ServerError` semantics (404, 409,
        500, …) propagate unchanged. The loop exits when :meth:`stop` is
        called, re-raising :class:`HttpError` so the caller can log it
        cleanly during shutdown.

        Shutdown race: :meth:`stop` closes the session while requests are
        still in flight (by design -- :class:`~resonate.resonate.Resonate.stop`
        stops the network first so the bounded bg-task join can drain). A
        mid-flight ``session.post`` then raises ``RuntimeError("Session is
        closed")`` rather than a :class:`aiohttp.ClientError`, so both are
        caught here and folded into :class:`HttpError`; otherwise the bare
        ``RuntimeError`` would surface from every fire-and-forget
        :func:`asyncio.create_task` whose result nobody is awaiting (the
        ``ctx.run`` ``bg()`` chain) as a ``Task exception was never
        retrieved`` log. A new request issued *after* stop short-circuits at
        the top of the loop so no fresh session is opened post-shutdown (the
        ``Unclosed client session`` warning from aiohttp).
        """
        logger.debug("http_network http_req: %s", req)
        headers = self._auth_headers({"Content-Type": "application/json"})
        backoff: float = _INITIAL_BACKOFF_SECS
        while True:
            if not self._running:
                msg = "network has been stopped"
                raise HttpError(RuntimeError(msg))
            session = self._ensure_session()
            try:
                async with session.post(
                    f"{self._url}/", data=req, headers=headers
                ) as resp:
                    resp_str = await resp.text()
            except (aiohttp.ClientError, RuntimeError) as exc:
                # ``stop`` closing the session mid-flight raises ``RuntimeError
                # ("Session is closed")``; surface as :class:`HttpError` so the
                # caller's bg task unwinds cleanly. A spontaneous ``RuntimeError``
                # while still running is not retriable -- re-raise so a real
                # bug is not papered over by infinite backoff.
                if not self._running:
                    raise HttpError(exc) from exc
                if isinstance(exc, RuntimeError):
                    raise
                logger.warning(
                    "HTTP send failed, retrying (backoff=%ss): %s", backoff, exc
                )
                await self._sleep_or_stop(backoff)
                if not self._running:
                    raise HttpError(exc) from exc
                backoff = min(backoff * 2, _MAX_BACKOFF_SECS)
                continue
            logger.debug("http_network http_res: %s", resp_str)
            return resp_str

    def recv(self, callback: Callable[[str], None]) -> None:
        """Register a callback for incoming SSE messages."""
        self._subscribers.append(callback)

    def target_resolver(self, target: str) -> str:
        """Resolve a target name to a ``poll://`` anycast address."""
        return f"poll://any@{target}"

    # -- internals ------------------------------------------------------------

    def _ensure_session(self) -> aiohttp.ClientSession:
        """Lazily create the shared :class:`aiohttp.ClientSession`.

        Mirrors Rust building ``reqwest::Client`` in ``new``, but deferred until
        an event loop is running (``ClientSession`` must be created inside one).

        Once :meth:`stop` has run, ``_session`` is ``None`` *and*
        ``_running`` is ``False``; refuse to open a fresh session in that
        window so a retry loop racing with shutdown does not leak a
        ``ClientSession`` that nobody will close (the ``Unclosed client
        session`` aiohttp warning). :meth:`send` is responsible for the
        guard at the top of its retry loop -- this assert is defence in
        depth.
        """
        if self._session is None:
            if not self._running:
                msg = "network has been stopped"
                raise RuntimeError(msg)
            # Raise the connector cap above aiohttp's default 100 so heartbeat
            # and execution traffic never starve each other (see
            # ``_DEFAULT_CONN_LIMIT``). The session owns the connector, so the
            # existing ``session.close()`` in :meth:`stop` tears it down.
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=self._conn_limit)
            )
        return self._session

    def _auth_headers(self, headers: dict[str, str]) -> dict[str, str]:
        """Add the bearer ``Authorization`` header when auth is configured."""
        if self._auth is not None:
            headers["Authorization"] = f"Bearer {self._auth}"
        return headers

    async def _sleep_or_stop(self, secs: float) -> None:
        """Sleep for ``secs``, returning early once :meth:`stop` is called.

        Used by :meth:`send`'s retry loop -- the SSE loop, which holds its
        long-lived connection inside :meth:`asyncio.CancelledError`-safe
        :func:`asyncio.sleep`, gets unblocked by the task ``cancel`` in
        :meth:`stop` instead.
        """
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=secs)

    async def _sse_loop(self) -> None:
        """Connect to the SSE endpoint, reconnecting with exponential backoff."""
        url = f"{self._url}/poll/{self._group}/{self._pid}"
        headers = self._auth_headers({"Accept": "text/event-stream"})
        backoff = _INITIAL_BACKOFF_SECS
        with contextlib.suppress(asyncio.CancelledError):
            while self._running:
                try:
                    session = self._ensure_session()
                    async with session.get(url, headers=headers) as resp:
                        if not (200 <= resp.status < 300):
                            logger.warning(
                                "SSE endpoint returned %s, retrying (backoff=%ss)",
                                resp.status,
                                backoff,
                            )
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, _MAX_BACKOFF_SECS)
                            continue

                        # Connection succeeded, reset backoff.
                        backoff = _INITIAL_BACKOFF_SECS
                        logger.info("SSE connection established: %s", url)
                        await self._read_stream(resp)
                except asyncio.CancelledError:
                    raise
                except aiohttp.ClientError as exc:
                    logger.warning(
                        "SSE connection failed, retrying (backoff=%ss): %s",
                        backoff,
                        exc,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, _MAX_BACKOFF_SECS)
                    continue

                if not self._running:
                    break
                logger.info(
                    "SSE connection closed, reconnecting (backoff=%ss)", backoff
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF_SECS)

    async def _read_stream(self, resp: aiohttp.ClientResponse) -> None:
        """Parse the SSE byte stream and dispatch each ``data:`` line.

        SSE events are separated by a blank line; mirrors Rust's buffering by
        splitting on the blank line and dispatching every ``data:`` line in a
        block to each subscriber.
        """
        buffer = ""
        async for chunk in resp.content.iter_any():
            try:
                text = chunk.decode("utf-8")
            except UnicodeDecodeError:
                continue
            buffer += text

            while "\n\n" in buffer:
                block, buffer = buffer.split("\n\n", 1)
                for line in block.splitlines():
                    data = _strip_data_prefix(line)
                    if data is None:
                        continue
                    logger.debug("http_network sse_recv: %s", data)
                    for cb in list(self._subscribers):
                        cb(data)


def _strip_data_prefix(line: str) -> str | None:
    """Return the trimmed payload of an SSE ``data:`` line, else ``None``."""
    if line.startswith("data:"):
        data = line[len("data:") :].strip()
        return data or None
    return None
