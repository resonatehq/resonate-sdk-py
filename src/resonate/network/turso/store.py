"""Connection management and the bridge between the two kinds of database.

An origin database is the authority for one workflow. The tenant database
indexes the due timers across all of them, so a process can find work in a
workflow it has never heard of without opening every database in the tenant.
Nothing in the tenant database is trusted: :meth:`TursoStore.flush` rebuilds it
from the origin after every commit, and every consumer re-validates against the
origin before acting.

``flush`` runs after the request's transaction has committed, never inside it: a
single transaction cannot span two databases, so the mirror is deliberately
eventual. Crash after commit but before flush and the origin still holds the
truth -- the next flush, or the next request touching this origin, republishes
it.
"""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from typing import TYPE_CHECKING, Any

from resonate.network.turso.schema import ORIGIN_SCHEMA, SCHEMA_VERSION, TENANT_SCHEMA
from resonate.network.turso.server import hash_origin

if TYPE_CHECKING:
    from resonate.network.turso.driver import (
        TursoConnection,
        TursoDriver,
        TursoExecutor,
        TursoRow,
    )

logger = logging.getLogger(__name__)

#: The tenant index's timeout kinds, widening the origin database's encoding.
TIMEOUT_PROMISE = 0
TIMEOUT_TASK_RETRY = 1
TIMEOUT_TASK_LEASE = 2


class SchemaVersionError(RuntimeError):
    """The database was written by a newer SDK than this one."""


class TursoStore:
    """Opens, caches, and cross-publishes the network's databases."""

    def __init__(
        self,
        driver: TursoDriver,
        prefix: str,
        timeout_database: str,
        max_open_databases: int = 64,
        timeout_driver: TursoDriver | None = None,
    ) -> None:
        self._driver = driver
        #: Opens the tenant database; defaults to ``driver``. Origins are owned
        #: by one node, while the timeout index is written by all of them, so
        #: the two can need different storage -- see ``TursoNetwork``.
        self._timeout_driver = timeout_driver or driver
        self._prefix = prefix
        self._timeout_database = timeout_database
        self._max_open = max_open_databases

        self._tenant: TursoConnection | None = None
        self._tenant_lock = asyncio.Lock()
        #: Open origin connections, in least-recently-used order.
        self._origins: OrderedDict[str, TursoConnection] = OrderedDict()
        self._open_lock = asyncio.Lock()
        #: Serializes work per origin so a request and a flush never interleave.
        self._locks: dict[str, asyncio.Lock] = {}
        #: The timer set this process last published per origin.
        #:
        #: The tenant database is the one file every node in the fleet writes,
        #: which makes it the fleet's only global write bottleneck. Most requests
        #: do not move a timer -- reads never do, and plenty of writes do not
        #: either -- so publishing unconditionally saturates that file for no
        #: reason. Comparing against what we last published turns those into no
        #: tenant write at all. A stale skip is possible if another node rewrote
        #: this origin's rows underneath us; that is the same drift the index
        #: already tolerates, and the next real change republishes the slice.
        self._published: dict[str, str] = {}
        self._closed = False

    # -------------------------------------------------------------------------
    # CONNECTIONS
    # -------------------------------------------------------------------------

    async def tenant(self) -> TursoConnection:
        if self._tenant is not None:
            return self._tenant
        async with self._tenant_lock:
            if self._tenant is None:
                conn = await self._timeout_driver.open(
                    f"{self._prefix}{self._timeout_database}"
                )
                await _migrate(conn, TENANT_SCHEMA)
                self._tenant = conn
            return self._tenant

    async def origin(self, origin: str) -> TursoConnection:
        async with self._open_lock:
            conn = self._origins.get(origin)
            if conn is not None:
                self._origins.move_to_end(origin)
                return conn

            conn = await self._driver.open(f"{self._prefix}{origin}")
            await _migrate(conn, ORIGIN_SCHEMA)
            self._origins[origin] = conn
            await self._evict()
            return conn

    async def _evict(self) -> None:
        while len(self._origins) > self._max_open:
            name, conn = self._origins.popitem(last=False)
            # Evict behind the per-origin lock so an in-flight transaction is
            # never closed out from under itself.
            async with self.lock(name):
                try:
                    await conn.close()
                except Exception:
                    logger.warning(
                        "turso close failed for origin %s", name, exc_info=True
                    )
            self._locks.pop(name, None)

    def lock(self, origin: str) -> asyncio.Lock:
        """Exclusive access to an origin within this process.

        The driver already serializes statements on a connection, but a request
        is a transaction *followed by* a flush, and those two must not interleave
        with another request's pair -- otherwise a flush could publish a partial
        view of another request's timers.
        """
        lock = self._locks.get(origin)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[origin] = lock
        return lock

    # -------------------------------------------------------------------------
    # FLUSH
    # -------------------------------------------------------------------------

    async def flush(
        self, origin: str, conn: TursoConnection, *, push_origin: bool = True
    ) -> None:
        """Publish an origin's armed timers to the tenant index.

        Called after every committed write against an origin. Reading the whole
        timeout set and replacing the origin's slice of the index -- rather than
        tracking deltas -- is deliberate: a workflow's live timer set is small,
        and a full replace cannot drift, whereas a missed delta would silently
        strand a timer forever.

        ``push_origin`` is False for requests inside a task's tenure: the lease
        already gives the workflow one writer, so intermediate durable steps
        stay on the local replica and the upload happens once, at the boundary
        (complete or suspend). An index entry published while the origin is
        still unpushed is safe -- a consumer re-validates against the origin
        and treats "not armed yet" exactly like any other stale entry -- it
        just wastes an open per sweep until the boundary push lands.
        """
        if self._closed:
            return

        # Local writes first: the tenant index should not advertise a timer whose
        # origin database the remote cannot yet serve to whoever picks it up.
        if push_origin:
            await conn.push()

        async with conn.transaction() as tx:
            timeouts = await _read_timeouts(tx)

        # Nothing moved: leave the shared file alone.
        fingerprint = "".join(sorted(f"{i} {k} {t}" for i, k, t in timeouts))
        if self._published.get(origin) == fingerprint:
            return

        tenant = await self.tenant()
        async with tenant.transaction() as tx:
            await tx.execute("DELETE FROM timeouts WHERE origin = ?", [origin])
            origin_hash = hash_origin(origin)
            for timeout_id, kind, timeout_at in timeouts:
                await tx.execute(
                    """
                    INSERT INTO timeouts (origin, origin_hash, id, kind, timeout_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (origin, id, kind) DO UPDATE SET
                      timeout_at = excluded.timeout_at,
                      origin_hash = excluded.origin_hash
                    """,
                    [origin, origin_hash, timeout_id, kind, timeout_at],
                )
        await tenant.push()
        # Recorded only after the write lands, so a failed publish is retried.
        self._published[origin] = fingerprint

    async def discard(self) -> None:
        """Close every open connection, leaving the store usable.

        The next access reopens. Distinct from :meth:`close`, which also marks
        the store shut down so a late flush cannot resurrect it.
        """
        connections: list[TursoConnection] = list(self._origins.values())
        self._origins.clear()
        self._locks.clear()
        self._published.clear()
        if self._tenant is not None:
            connections.append(self._tenant)
            self._tenant = None
        for conn in connections:
            try:
                await conn.close()
            except Exception:
                logger.debug("turso close failed", exc_info=True)

    async def close(self) -> None:
        self._closed = True
        await self.discard()


# =============================================================================
# HELPERS
# =============================================================================


async def _read_timeouts(tx: TursoExecutor) -> list[tuple[str, int, int]]:
    """Read the origin's armed timers, translated into the tenant index's kind encoding."""
    promises: list[TursoRow] = await tx.execute(
        "SELECT id, timeout_at FROM promise_timeouts"
    )
    tasks: list[TursoRow] = await tx.execute(
        "SELECT id, kind, timeout_at FROM task_timeouts"
    )
    out: list[tuple[str, int, int]] = [
        (row["id"], TIMEOUT_PROMISE, int(row["timeout_at"])) for row in promises
    ]
    out.extend(
        (
            row["id"],
            TIMEOUT_TASK_LEASE if int(row["kind"]) == 1 else TIMEOUT_TASK_RETRY,
            int(row["timeout_at"]),
        )
        for row in tasks
    )
    return out


async def _migrate(conn: TursoConnection, statements: tuple[str, ...]) -> None:
    """Create the schema if absent and record its version.

    Every statement is ``IF NOT EXISTS``, so this is safe to run on every open --
    which it must be, since a database is created the first time a workflow
    touches it and no separate migration step ever runs against it.
    """
    for sql in statements:
        await conn.execute(sql)

    # Claim the version stamp idempotently. Several processes routinely open the
    # same database for the first time at the same moment, and a
    # read-then-insert loses that race against the unique constraint on
    # ``meta.key``.
    await conn.execute(
        "INSERT INTO meta (key, value) VALUES ('schema_version', ?) ON CONFLICT (key) DO NOTHING",
        [str(SCHEMA_VERSION)],
    )

    rows: list[dict[str, Any]] = await conn.execute(
        "SELECT value FROM meta WHERE key = 'schema_version'"
    )
    found = int(rows[0]["value"]) if rows else SCHEMA_VERSION
    if found > SCHEMA_VERSION:
        msg = (
            f"Turso database is at schema version {found}, "
            f"newer than this SDK understands ({SCHEMA_VERSION})"
        )
        raise SchemaVersionError(msg)
    if found < SCHEMA_VERSION:
        # The timeout table is a pure mirror of the origin databases, so an
        # upgrade that changes its shape can simply drop and rebuild it rather
        # than migrate: the next flush of each origin republishes its slice.
        # Nothing else in either schema has changed shape, and the DDL above is
        # idempotent, so the rest of catching up is just restamping. A table
        # this version no longer uses is left in place rather than dropped --
        # an older SDK sharing the database still reads it.
        await conn.execute("DROP TABLE IF EXISTS timeouts")
        for sql in statements:
            await conn.execute(sql)
        await conn.execute(
            "UPDATE meta SET value = ? WHERE key = 'schema_version'",
            [str(SCHEMA_VERSION)],
        )
