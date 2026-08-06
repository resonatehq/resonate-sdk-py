"""Opening Turso databases by name.

The decentralized network needs one thing from Turso: the ability to open a
database *by name* and run transactions against it. That is the whole contract,
expressed below as :class:`TursoDriver`.

Keeping it this small is deliberate. "One database per workflow" means the
network opens databases the caller never named in advance, so the mapping from a
logical database name (``<prefix><origin>``) to a physical database -- a local
file, an embedded replica that syncs to Turso Cloud -- is a deployment decision,
not a protocol decision. Two drivers ship here; a caller with a third arrangement
implements the protocol.

The ``turso`` package (PyPI ``pyturso``) is an optional dependency, imported
lazily so the SDK carries no hard dependency on it unless a Turso driver is used.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from types import TracebackType

#: A row, keyed by column name.
type TursoRow = dict[str, Any]


@runtime_checkable
class TursoExecutor(Protocol):
    """The statement-running surface, shared by connections and transactions."""

    async def execute(self, sql: str, args: Sequence[Any] = ()) -> list[TursoRow]:
        """Run one statement and return its rows (empty for non-SELECT statements)."""
        ...


class TursoConnection(Protocol):
    """An open database."""

    async def execute(self, sql: str, args: Sequence[Any] = ()) -> list[TursoRow]: ...

    def transaction(self) -> TursoTransaction:
        """Return an async context manager that commits on exit, rolls back on error."""
        ...

    async def pull(self) -> bool:
        """Pull remote changes into this replica; ``False`` when it does not replicate."""
        ...

    async def push(self) -> None:
        """Push local changes to the remote; a no-op when it does not replicate."""
        ...

    async def close(self) -> None: ...


class TursoTransaction(Protocol):
    """A write transaction, entered as an async context manager."""

    async def __aenter__(self) -> TursoExecutor: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...


class TursoDriver(Protocol):
    """Opens databases by name.

    ``name`` is the fully-qualified logical database name the network computed --
    ``<prefix><origin>`` for a workflow, ``<prefix><timeout_database>`` for the
    tenant database. A driver maps that name onto physical storage.
    """

    async def open(self, name: str) -> TursoConnection: ...


# =============================================================================
# HELPERS
# =============================================================================

_UNSAFE_NAMES = frozenset({"", ".", ".."})

#: How long a connection waits for the write lock before giving up. Several
#: processes may share one origin database, and a request is a short
#: transaction, so queueing beats failing.
BUSY_TIMEOUT_MS = 5000


def database_path(directory: str, name: str, suffix: str = ".db") -> str:
    """Map a logical database name to a filesystem path.

    Origins are validated by the protocol (``resonate:origin`` must not contain a
    ``.``) but a database name still reaches the filesystem, so anything that
    could escape the directory is rejected rather than sanitized -- silently
    rewriting a name would make two distinct origins share one database.
    """
    if name in _UNSAFE_NAMES or any(c in name for c in ("/", "\\", "\0")):
        msg = f"Invalid database name: {name!r}"
        raise ValueError(msg)
    return f"{directory.rstrip('/')}/{name}{suffix}"


# =============================================================================
# CONNECTION WRAPPER
# =============================================================================


class _Connection:
    """Adapts a ``turso`` DB-API connection to :class:`TursoConnection`.

    The connection runs in autocommit mode (``isolation_level=None``) and
    transactions are opened explicitly with ``BEGIN IMMEDIATE``. Letting the
    driver manage implicit transactions would make the boundaries depend on
    which statement ran first; a request is one transaction, and the protocol's
    fencing guards are only sound if it is an isolated one.

    A per-connection lock serializes transactions within the process: the
    underlying connection is a single worker thread, so two overlapping
    ``BEGIN``s would interleave into one transaction.
    """

    def __init__(self, conn: Any, *, replicates: bool = False) -> None:
        self._conn = conn
        self._replicates = replicates
        self._lock = asyncio.Lock()

    async def execute(self, sql: str, args: Sequence[Any] = ()) -> list[TursoRow]:
        cursor = await self._conn.execute(sql, tuple(args))
        return await _rows(cursor)

    def transaction(self) -> _Transaction:
        return _Transaction(self)

    async def pull(self) -> bool:
        if not self._replicates:
            return False
        return bool(await self._conn.pull())

    async def push(self) -> None:
        if self._replicates:
            await self._conn.push()

    async def close(self) -> None:
        await self._conn.close()


class _Transaction:
    def __init__(self, conn: _Connection) -> None:
        self._conn = conn

    async def __aenter__(self) -> TursoExecutor:
        await self._conn._lock.acquire()  # noqa: SLF001
        try:
            await self._conn.execute("BEGIN IMMEDIATE")
        except BaseException:
            self._conn._lock.release()  # noqa: SLF001
            raise
        return self._conn

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            if exc_type is None:
                await self._conn.execute("COMMIT")
            else:
                # A failed rollback would mask the original error; the
                # connection is discarded by the caller either way.
                with contextlib.suppress(Exception):
                    await self._conn.execute("ROLLBACK")
        finally:
            self._conn._lock.release()  # noqa: SLF001


async def _rows(cursor: Any) -> list[TursoRow]:
    """Zip a DB-API cursor's tuples with its column names."""
    description = cursor.description
    if not description:
        return []
    columns = [d[0] for d in description]
    fetched = await cursor.fetchall()
    return [dict(zip(columns, row, strict=False)) for row in fetched]


# =============================================================================
# LOCAL DRIVER
# =============================================================================


class TursoLocalDriver:
    """Embedded, local-only databases: no replication.

    **One process only.** Turso takes an exclusive file lock when it opens a
    database, so a second process opening the same file fails outright with
    "File is locked by another process" -- a shared directory is not a way to
    run a fleet. Use it for a single node, for tests, and for ``":memory:"``,
    which keeps every database in memory while still isolating them by name.
    """

    def __init__(self, directory: str, *, suffix: str = ".db") -> None:
        self._directory = directory
        self._suffix = suffix

    async def open(self, name: str) -> TursoConnection:
        import turso.aio  # noqa: PLC0415

        path = (
            ":memory:"
            if self._directory == ":memory:"
            else database_path(self._directory, name, self._suffix)
        )
        conn = await turso.aio.connect(path, isolation_level=None)
        wrapped = _Connection(conn)
        # Busy timeout first: switching to WAL takes an exclusive lock of its
        # own, so arming the timeout afterwards leaves that very statement
        # unprotected against a concurrent opener.
        await wrapped.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
        if path != ":memory:":
            await wrapped.execute("PRAGMA journal_mode = WAL")
        return wrapped


# =============================================================================
# SYNC DRIVER
# =============================================================================


class TursoSyncDriver:
    """An embedded replica per database, syncing with Turso Cloud.

    This is the decentralized arrangement -- every process holds a local copy of
    the workflows it is working on, reads and writes it at local-disk latency,
    and converges with everyone else through the remote.

    ``url`` is either a prefix (the remote database is ``f"{url}{name}"``) or a
    callable for full control. A Turso Cloud deployment typically names one
    remote database per workflow, so the prefix form is usually enough.
    """

    def __init__(
        self,
        directory: str,
        url: str | Callable[[str], str],
        *,
        auth_token: str | Callable[[], str | None] | None = None,
        client_name: str | None = None,
        long_poll_timeout_ms: int | None = None,
        suffix: str = ".db",
        options: dict[str, Any] | None = None,
    ) -> None:
        self._directory = directory
        self._url_for: Callable[[str], str] = (
            (lambda name, prefix=url: f"{prefix}{name}")
            if isinstance(url, str)
            else url
        )
        self._auth_token = auth_token
        self._client_name = client_name
        self._long_poll_timeout_ms = long_poll_timeout_ms
        self._suffix = suffix
        self._options = options or {}

    async def open(self, name: str) -> TursoConnection:
        # `turso.aio.sync.connect` is the async wrapper around `connect_sync`.
        import turso.aio.sync  # noqa: PLC0415

        conn = await turso.aio.sync.connect(
            database_path(self._directory, name, self._suffix),
            self._url_for(name),
            auth_token=self._auth_token,
            client_name=self._client_name,
            long_poll_timeout_ms=self._long_poll_timeout_ms,
            isolation_level=None,
            **self._options,
        )
        wrapped = _Connection(conn, replicates=True)
        # Only the busy timeout: the sync engine owns this database's journal
        # mode, so setting it here would fight the replica machinery.
        await wrapped.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
        return wrapped
