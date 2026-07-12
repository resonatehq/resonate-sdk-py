from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import uuid
from typing import TYPE_CHECKING

import asyncpg
import msgspec

from resonate.error import PostgresError

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================

_INITIAL_BACKOFF_SECS = 1
_MAX_BACKOFF_SECS = 60

#: Connection cap for the shared :class:`asyncpg.Pool`. Postgres connections
#: are far heavier than HTTP sockets, so the cap is modest; every request is a
#: single short ``resonate_rpc()`` call, so a small pool sustains high
#: concurrency without starving the periodic ``task.heartbeat``.
DEFAULT_POOL_MAX_SIZE = 16

#: Rows fetched per ``dequeue_execute`` / ``dequeue_unblock`` call. Matches the
#: server-side default; a full batch means more may remain, so the drain loops
#: until a short batch comes back.
DEFAULT_DEQUEUE_BATCH = 100

#: Fallback drain period. ``NOTIFY`` is fire-and-forget: a notification that
#: races a reconnect, or a LISTEN connection that dies silently, would strand
#: outbox rows until the server's ~5s task-retry re-emit (which only covers
#: ``execute``, never ``unblock``). Polling on this period bounds that gap.
DEFAULT_DRAIN_INTERVAL_SECS = 5.0

_RPC_SQL = "SELECT resonate.resonate_rpc($1::jsonb)"
_DEQUEUE_EXECUTE_SQL = "SELECT task_id, version FROM resonate.dequeue_execute($1, $2)"
_DEQUEUE_UNBLOCK_SQL = "SELECT promise FROM resonate.dequeue_unblock($1, $2)"


def _outbox_channel(address: str) -> str:
    """Return the NOTIFY channel for an address.

    Python twin of the server's ``resonate.outbox_channel``: the address is
    md5-hashed (identification only, not security) so any address fits within
    Postgres's 63-byte channel-name limit. Must stay byte-for-byte in sync
    with the SQL definition or notifications are silently never received.
    """
    digest = hashlib.md5(address.encode("utf-8"), usedforsecurity=False)
    return "resonate_q_" + digest.hexdigest()


def _execute_envelope(task_id: str, version: int) -> str:
    """Build the ``execute`` message envelope from a dequeued outbox row.

    Same shape the server's ``_outbox_http_body`` produces for HTTP push, so
    :class:`~resonate.transport.Transport` decodes both identically.
    """
    return msgspec.json.encode(
        {
            "kind": "execute",
            "head": {},
            "data": {"task": {"id": task_id, "version": version}},
        }
    ).decode("utf-8")


def _unblock_envelope(promise_json: str) -> str:
    """Build the ``unblock`` message envelope from a dequeued outbox row.

    ``promise_json`` is the promise as jsonb text straight from the database;
    it is spliced in verbatim rather than decoded and re-encoded.
    """
    return '{"kind":"unblock","head":{},"data":{"promise":' + promise_json + "}}"


class PostgresNetwork:
    """:class:`Network` implementation that talks to a resonate-pg server.

    `resonate-pg <https://github.com/resonatehq/resonate-pg>`_ is a complete
    Resonate server living entirely inside Postgres: every protocol action is
    a stored procedure and timers are driven by pg_cron. There is no server
    process to connect to -- the database *is* the server.

    - Requests are single ``SELECT resonate.resonate_rpc($1::jsonb)`` calls
      over a shared :class:`asyncpg.Pool`; the JSON envelope in and out is
      identical to the HTTP transport's.
    - Incoming messages (execute/unblock) sit in the server's ``outbox``
      table addressed to this worker. A dedicated connection LISTENs on the
      per-address NOTIFY channels (see :func:`_outbox_channel`) and each
      notification -- plus a periodic fallback -- triggers a destructive
      drain via ``dequeue_execute`` / ``dequeue_unblock``.
    - Addresses use the ``pg://`` scheme: ``pg://uni@group/pid`` and
      ``pg://any@group``. The address string is opaque to the server, and the
      dequeue's ``FOR UPDATE SKIP LOCKED`` provides anycast semantics: group
      members share the *same* anycast address (no pid suffix, unlike
      ``poll://``) and each outbox row is delivered to exactly one of them.

    With ``send_only=True`` no listener or drain task is started -- for
    serverless workers (see ``resonate.faas``) that are *pushed* work over
    HTTP by the server's pg_net trigger and only need the RPC path back.
    """

    def __init__(
        self,
        url: str,
        pid: str | None = None,
        group: str | None = None,
        *,
        send_only: bool = False,
        pool_max_size: int = DEFAULT_POOL_MAX_SIZE,
        dequeue_batch: int = DEFAULT_DEQUEUE_BATCH,
        drain_interval: float = DEFAULT_DRAIN_INTERVAL_SECS,
    ) -> None:
        self._url = url
        self._pid = pid if pid is not None else uuid.uuid4().hex
        self._group = group if group is not None else "default"
        self._unicast = f"pg://uni@{self._group}/{self._pid}"
        self._anycast = f"pg://any@{self._group}"
        self._send_only = send_only
        self._pool_max_size = pool_max_size
        self._dequeue_batch = dequeue_batch
        self._drain_interval = drain_interval

        self._subscribers: list[Callable[[str], None]] = []
        self._pool: asyncpg.Pool | None = None
        self._pool_lock = asyncio.Lock()
        self._listen_handle: asyncio.Task[None] | None = None
        self._drain_handle: asyncio.Task[None] | None = None
        self._running: bool = False
        # Set on :meth:`stop` so a ``send`` parked in its retry backoff wakes
        # immediately instead of blocking shutdown.
        self._stop_event = asyncio.Event()
        # Set by NOTIFY callbacks (and on listener [re]connect) to wake the
        # drain loop ahead of its periodic fallback tick.
        self._wake = asyncio.Event()

    def pid(self) -> str:
        return self._pid

    def group(self) -> str:
        return self._group

    def unicast(self) -> str:
        return self._unicast

    def anycast(self) -> str:
        return self._anycast

    async def start(self) -> None:
        """Start the LISTEN and drain tasks for incoming messages.

        In ``send_only`` mode neither task is started -- the network only
        marks itself running so :meth:`send` is permitted -- because a
        serverless worker receives work by HTTP push, not by polling.
        """
        self._running = True
        self._stop_event.clear()
        if not self._send_only:
            self._listen_handle = asyncio.create_task(self._listen_loop())
            self._drain_handle = asyncio.create_task(self._drain_loop())

    async def stop(self) -> None:
        self._running = False
        # Wake any ``send`` parked in the retry backoff so the bounded join in
        # :meth:`~resonate.resonate.Resonate.stop` does not stall.
        self._stop_event.set()
        for handle in (self._listen_handle, self._drain_handle):
            if handle is not None:
                handle.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await handle
        self._listen_handle = None
        self._drain_handle = None
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
        self._subscribers.clear()

    async def send(self, req: str) -> str:
        """Send a request via ``SELECT resonate.resonate_rpc($1::jsonb)``.

        Connection-level failures are retried with exponential backoff
        (``1s → 60s``), so the SDK survives the database being down at
        startup, restarting, or briefly unreachable. Database errors raised
        by the call itself (missing schema, revoked EXECUTE, ...) are not
        retried: they are deliberate responses, surfaced once as
        :class:`PostgresError`. Protocol-level errors (404, 409, ...) never
        raise -- ``resonate_rpc`` reports them inside the response envelope.

        Once :meth:`stop` is called, any in-flight or new request raises
        :class:`PostgresError` instead of retrying, so shutdown is never
        blocked by the backoff loop.
        """
        logger.debug("pg_network pg_req: %s", req)
        backoff: float = _INITIAL_BACKOFF_SECS
        while True:
            if not self._running:
                msg = "network has been stopped"
                raise PostgresError(RuntimeError(msg))
            try:
                pool = await self._ensure_pool()
                resp = await pool.fetchval(_RPC_SQL, req)
            except (asyncpg.PostgresConnectionError, OSError) as exc:
                if not self._running:
                    raise PostgresError(exc) from exc
                logger.warning(
                    "Postgres send failed, retrying (backoff=%ss): %s", backoff, exc
                )
                await self._sleep_or_stop(backoff)
                if not self._running:
                    raise PostgresError(exc) from exc
                backoff = min(backoff * 2, _MAX_BACKOFF_SECS)
                continue
            except asyncpg.PostgresError as exc:
                # A non-connection database error is a deliberate response
                # (schema not installed, permission revoked, ...): retrying
                # cannot fix it, so surface it to the caller immediately.
                raise PostgresError(exc) from exc
            except asyncpg.InterfaceError as exc:
                # :meth:`stop` closing the pool mid-flight surfaces as an
                # ``InterfaceError``; wrap it so the caller unwinds cleanly.
                # While still running it is a real bug -- re-raise unhidden.
                if not self._running:
                    raise PostgresError(exc) from exc
                raise
            logger.debug("pg_network pg_res: %s", resp)
            if resp is None:
                msg = "resonate_rpc returned NULL"
                raise PostgresError(RuntimeError(msg))
            return resp

    def recv(self, callback: Callable[[str], None]) -> None:
        """Register a callback for incoming execute/unblock messages."""
        self._subscribers.append(callback)

    def target_resolver(self, target: str) -> str:
        """Resolve a target group name to its ``pg://`` anycast address."""
        return f"pg://any@{target}"

    # -- internals ------------------------------------------------------------

    async def _ensure_pool(self) -> asyncpg.Pool:
        """Lazily create the shared :class:`asyncpg.Pool`.

        ``statement_cache_size=0`` keeps the pool compatible with
        transaction-mode poolers (pgbouncer, Supabase's Supavisor), which do
        not support named prepared statements; each request is a single
        self-contained call, so transaction pooling is fine. The asyncio lock
        keeps concurrent first sends from racing two pools into existence.

        Once :meth:`stop` has run, this refuses to open a fresh pool so a
        retry loop racing with shutdown cannot leak a pool that nobody will
        close.
        """
        if self._pool is None:
            async with self._pool_lock:
                if self._pool is None:
                    if not self._running:
                        msg = "network has been stopped"
                        raise RuntimeError(msg)
                    self._pool = await asyncpg.create_pool(
                        self._url,
                        min_size=1,
                        max_size=self._pool_max_size,
                        statement_cache_size=0,
                    )
        return self._pool

    async def _sleep_or_stop(self, secs: float) -> None:
        """Sleep for ``secs``, returning early once :meth:`stop` is called.

        Used by :meth:`send`'s retry loop so a pending retry never delays
        shutdown.
        """
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=secs)

    async def _listen_loop(self) -> None:
        """Hold a dedicated LISTEN connection, reconnecting with backoff.

        The NOTIFY payload is only a wake-up signal ("execute"/"unblock");
        the messages themselves stay in the outbox until the drain loop
        destructively dequeues them, so a lost notification is latency, not
        data loss.
        """
        channels = (_outbox_channel(self._unicast), _outbox_channel(self._anycast))
        backoff: float = _INITIAL_BACKOFF_SECS
        with contextlib.suppress(asyncio.CancelledError):
            while self._running:
                conn: asyncpg.Connection | None = None
                try:
                    conn = await asyncpg.connect(self._url, statement_cache_size=0)
                    closed = asyncio.Event()
                    conn.add_termination_listener(
                        lambda _conn, _closed=closed: _closed.set()
                    )
                    for channel in channels:
                        await conn.add_listener(channel, self._on_notify)

                    # Connection succeeded, reset backoff. Wake the drain loop
                    # immediately: rows enqueued while we were not listening
                    # never re-notify.
                    backoff = _INITIAL_BACKOFF_SECS
                    logger.info("Postgres LISTEN established (channels=%s)", channels)
                    self._wake.set()
                    await closed.wait()
                except asyncio.CancelledError:
                    raise
                except (asyncpg.PostgresError, OSError) as exc:
                    logger.warning(
                        "Postgres LISTEN failed, retrying (backoff=%ss): %s",
                        backoff,
                        exc,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, _MAX_BACKOFF_SECS)
                    continue
                finally:
                    if conn is not None and not conn.is_closed():
                        with contextlib.suppress(Exception):
                            await conn.close()

                if not self._running:
                    break
                logger.info(
                    "Postgres LISTEN connection closed, reconnecting (backoff=%ss)",
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF_SECS)

    def _on_notify(
        self,
        _conn: asyncpg.Connection,
        _pid: int,
        channel: str,
        payload: str,
    ) -> None:
        """Wake the drain loop; asyncpg invokes this on the event loop."""
        logger.debug("pg_network notify: channel=%s payload=%s", channel, payload)
        self._wake.set()

    async def _drain_loop(self) -> None:
        """Drain the outbox on every wake-up and on a periodic fallback tick.

        A drain failure (database briefly unreachable) is logged and retried
        on the next tick -- the rows stay in the outbox until dequeued.
        """
        with contextlib.suppress(asyncio.CancelledError):
            while self._running:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._wake.wait(), timeout=self._drain_interval
                    )
                self._wake.clear()
                if not self._running:
                    break
                try:
                    await self._drain()
                except asyncio.CancelledError:
                    raise
                except (asyncpg.PostgresError, OSError) as exc:
                    logger.warning("Postgres outbox drain failed, will retry: %s", exc)

    async def _drain(self) -> None:
        """Destructively dequeue and dispatch all pending outbox messages.

        Both this worker's unicast address and the group's shared anycast
        address are drained; ``FOR UPDATE SKIP LOCKED`` inside the dequeue
        functions guarantees each anycast row lands on exactly one group
        member. A full batch means more may remain, so each queue loops
        until a short batch comes back.
        """
        pool = await self._ensure_pool()
        for address in (self._unicast, self._anycast):
            while True:
                rows = await pool.fetch(
                    _DEQUEUE_EXECUTE_SQL, address, self._dequeue_batch
                )
                for row in rows:
                    version = row["version"]
                    self._dispatch(
                        _execute_envelope(
                            row["task_id"], version if version is not None else 0
                        )
                    )
                if len(rows) < self._dequeue_batch:
                    break
            while True:
                rows = await pool.fetch(
                    _DEQUEUE_UNBLOCK_SQL, address, self._dequeue_batch
                )
                for row in rows:
                    self._dispatch(_unblock_envelope(row["promise"]))
                if len(rows) < self._dequeue_batch:
                    break

    def _dispatch(self, data: str) -> None:
        logger.debug("pg_network recv: %s", data)
        for cb in list(self._subscribers):
            cb(data)
