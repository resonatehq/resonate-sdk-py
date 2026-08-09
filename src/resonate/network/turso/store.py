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
import contextlib
import logging
from collections import OrderedDict
from typing import TYPE_CHECKING, Any

from resonate import now_ms
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


_MISSING = object()


def _has_waiters(lock: asyncio.Lock) -> bool:
    """Whether anyone is blocked on ``lock``.

    ``asyncio.Lock`` exposes no public predicate, and ``locked()`` alone is not
    one: ``release()`` clears the locked flag *before* the woken waiter runs,
    so there is a real window where the lock reads unlocked while a waiter
    still holds a reference to this object. Dropping it then would hand the
    next request a different lock for the same origin and let both run.

    Every unexpected shape therefore answers True -- keeping a lock costs one
    map entry, dropping a live one costs correctness.
    """
    waiters = getattr(lock, "_waiters", _MISSING)
    if waiters is _MISSING:
        return True
    if waiters is None:
        return False
    try:
        return len(waiters) > 0
    except TypeError:  # pragma: no cover - defensive
        return True


#: How long a "nothing moved, skip the tenant write" decision stays good.
#: See ``TursoStore._published``.
REPUBLISH_AFTER_MS = 60_000

#: The tenant index's timeout kinds, widening the origin database's encoding.
TIMEOUT_PROMISE = 0
TIMEOUT_TASK_RETRY = 1
TIMEOUT_TASK_LEASE = 2


class SchemaVersionError(RuntimeError):
    """The database was written by a newer SDK than this one."""


class StoreClosedError(RuntimeError):
    """The store was closed; open a new network rather than reusing this one."""

    def __init__(self) -> None:
        super().__init__("TursoStore is closed")


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
        #: Each entry carries when it was published: "unchanged" is not a safe
        #: answer forever. If this origin's slice goes missing from the index by
        #: a route this process cannot see, the fingerprint still matches, the
        #: republish is skipped, and -- since the sweep finds work only through
        #: the index -- the timers never fire, so the fingerprint never changes.
        #: Expiring the entry bounds that strand to ``REPUBLISH_AFTER_MS``.
        self._published: dict[str, tuple[str, int]] = {}
        self._closed = False

    # -------------------------------------------------------------------------
    # CONNECTIONS
    # -------------------------------------------------------------------------

    async def tenant(self) -> TursoConnection:
        if self._closed:
            raise StoreClosedError
        if self._tenant is not None:
            return self._tenant
        async with self._tenant_lock:
            if self._tenant is None:
                conn = await self._timeout_driver.open(
                    f"{self._prefix}{self._timeout_database}"
                )
                try:
                    await _migrate(conn, TENANT_SCHEMA)
                except BaseException:
                    with contextlib.suppress(Exception):
                        await conn.close()
                    raise
                self._tenant = conn
            return self._tenant

    async def origin(self, origin: str) -> TursoConnection:
        # LOCK ORDER: a per-origin lock may be held by our caller, and closing
        # an eviction victim needs the victim's per-origin lock -- so nothing
        # here may await a per-origin lock while holding _open_lock, or two
        # requests deadlock in an AB-BA cycle (caller holds lock(Z), waits on
        # _open_lock; opener holds _open_lock, waits on lock(Z)). Victims are
        # therefore popped under _open_lock but closed after it is released.
        # Total, not just checked in `flush`: reopening after close would leak
        # a connection nothing will ever close, and would let a request that
        # raced `stop()` commit writes the mirror never learns about.
        if self._closed:
            raise StoreClosedError
        victims: list[tuple[str, TursoConnection]] = []
        async with self._open_lock:
            conn = self._origins.get(origin)
            if conn is not None:
                self._origins.move_to_end(origin)
                return conn

            conn = await self._driver.open(f"{self._prefix}{origin}")
            try:
                await _migrate(conn, ORIGIN_SCHEMA)
            except BaseException:
                with contextlib.suppress(Exception):
                    await conn.close()
                raise
            self._origins[origin] = conn
            while len(self._origins) > self._max_open:
                name, victim = self._origins.popitem(last=False)
                # `flush` only ever runs against a connection from `origin()`,
                # so an origin is open whenever it publishes: dropping the
                # fingerprint here bounds `_published` by max_open_databases
                # instead of one entry per origin ever touched. A dropped entry
                # costs one redundant republish, which the mirror tolerates.
                self._published.pop(name, None)
                victims.append((name, victim))
        for name, victim in victims:
            # Close behind the per-origin lock so an in-flight transaction is
            # never closed out from under itself. The lock object itself is
            # never removed from _locks: a waiter may already hold a reference,
            # and handing a later request a fresh lock would let two requests
            # run the same origin's transaction+flush pair concurrently.
            lock = self.lock(name)
            async with lock:
                try:
                    await victim.close()
                except Exception:
                    logger.warning(
                        "turso close failed for origin %s", name, exc_info=True
                    )
            # One database per workflow means one lock per workflow *ever
            # seen*, which is unbounded in a process running short-lived
            # workflows -- the LRU bounds connections, not this. Dropping it is
            # only safe with nobody queued: identity has to stay stable while a
            # holder or waiter exists (two requests on different lock objects
            # for one origin would interleave), and `_lock_waiters` is exactly
            # that test. A later request for a quiet origin starts a fresh
            # chain, which is fine.
            if not lock.locked() and not _has_waiters(lock):
                self._locks.pop(name, None)
        return conn

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
        last = self._published.get(origin)
        if (
            last is not None
            and last[0] == fingerprint
            and now_ms() - last[1] < REPUBLISH_AFTER_MS
        ):
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
        self._published[origin] = (fingerprint, now_ms())

    async def discard(self) -> None:
        """Close every open connection, leaving the store usable.

        The next access reopens. Distinct from :meth:`close`, which also marks
        the store shut down so a late flush cannot resurrect it.
        """
        entries = list(self._origins.items())
        self._origins.clear()
        self._published.clear()
        # The locks map is deliberately NOT cleared: a coroutine may be waiting
        # on one right now, and handing a later request a fresh lock for the
        # same origin would let two run their transaction-and-flush pairs
        # concurrently -- the interleaving the lock exists to prevent.
        for name, conn in entries:
            # Behind the per-origin lock, for the same reason eviction is: an
            # in-flight transaction must never be closed out from under itself.
            async with self.lock(name):
                try:
                    await conn.close()
                except Exception:
                    logger.debug("turso close failed", exc_info=True)
        # Tenant last: an origin's closing flush may still publish to it.
        tenant, self._tenant = self._tenant, None
        if tenant is not None:
            try:
                await tenant.close()
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
        # The timeout table is a mirror of the origin databases, but it is
        # also the fleet's only discovery mechanism: an origin whose workflows
        # are all quiescent gets no flush until something touches it, and
        # nothing touches it except a sweep -- which finds it through this
        # very table. So an upgrade must carry the rows across, not drop them:
        # stale-shaped rows are safe (every consumer re-validates against the
        # origin database), missing rows strand workflows forever. (Guarded:
        # origin databases run this same path and have no timeouts table. A
        # future version that reshapes the carried columns must adjust the
        # copy below in the same change.)
        mirror = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'timeouts'"
        )
        if mirror:
            await conn.execute("ALTER TABLE timeouts RENAME TO timeouts_old")
        for sql in statements:
            await conn.execute(sql)
        if mirror:
            await conn.execute(
                "INSERT OR IGNORE INTO timeouts (origin, origin_hash, id, kind, timeout_at) "
                "SELECT origin, origin_hash, id, kind, timeout_at FROM timeouts_old"
            )
            await conn.execute("DROP TABLE timeouts_old")
        await conn.execute(
            "UPDATE meta SET value = ? WHERE key = 'schema_version'",
            [str(SCHEMA_VERSION)],
        )
