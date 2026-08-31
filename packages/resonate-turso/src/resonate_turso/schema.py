"""Physical layout of the decentralized network's databases.

Two shapes of database back the network.

**Origin database** -- one per workflow. Everything the protocol reads or writes
while advancing a single workflow lives here, in four tables: its promises, the
callbacks and listeners registered against them, and each task's buffered
resumes. Every request is served by exactly one origin database, which is what
makes a request a single-database transaction.

A task is not among them, and neither is a timeout queue. **A table that only
ever recorded "this row is also in that set" is a predicate, not a table.** A
task is 1:1 with the promise it drives and shares its id, so ``tasks`` was five
columns behind a join; ``task_timeouts`` discriminated two queues with a
``kind`` column, which two nullable columns say without the row; and
``promise_timeouts`` held exactly the pending external promises, which a
partial index expresses with the same index that table carried. The Resonate
Server collapsed its SQLite schema the same way and for the same reason.

The one that stays is ``resumes``, and it is worth saying why, because the
server folded its equivalent into a ``ready`` flag on ``callbacks``. Settlement
here *deletes* the callbacks of the settled promise before the resume is
buffered, so there is no surviving row to carry the flag. That is a difference
in the transition, not in the schema, and changing it to match would change
behaviour rather than layout.

**Tenant database** -- one per tenant, shared by every origin. It holds the
things no single workflow owns: the timeout index (so a sweeper can find due
timers without opening every origin database).

Schedules were tenant-scoped too and are not implemented: ``schedule.*`` answers
501. An older database may still carry an unused ``schedules`` table, which is
left alone rather than dropped.

**Messages are not stored.** ``execute`` and ``unblock`` are handed to the local
Resonate client the moment the transaction that produced them commits; they
never touch a table. Recovery does not depend on them: an undelivered execute is
re-emitted by the task's own retry timer, which *is* durable, and the timeout
index is what lets any process in the tenant find that timer.

The timeout table is an *index*, never the authority. The origin database holds
the armed timers; the tenant rows are mirrored from it after each commit. A
stale entry is harmless -- every timeout transition re-checks its own due time
against the origin database before acting (the spec's NOT BEFORE rule). A
missing entry only delays work; the next flush restores it.

Turso does not support generated columns without an experimental flag, so the
tag-derived columns (``target``, ``branch``, ``timer``, ``external``) are written
by the caller rather than computed by the engine. ``external`` is the spec's
``PromiseObject.external``: explicitly tagged, targeted, or a timer.
"""

from __future__ import annotations

#: Bumped when the physical layout changes in a way old rows cannot satisfy.
SCHEMA_VERSION = 4

ORIGIN_SCHEMA: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS meta (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS promises (
      id TEXT PRIMARY KEY,
      state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'resolved', 'rejected', 'rejected_canceled', 'rejected_timedout')),
      param_headers TEXT,
      param_data TEXT,
      value_headers TEXT,
      value_data TEXT,
      tags TEXT NOT NULL DEFAULT '{}',
      target TEXT,
      branch TEXT,
      timer INTEGER NOT NULL DEFAULT 0,
      external INTEGER NOT NULL DEFAULT 0,
      timeout_at INTEGER NOT NULL,
      created_at INTEGER NOT NULL,
      settled_at INTEGER,

      -- was the ``tasks`` table. A task is 1:1 with the promise it drives and
      -- shares its id, so it was never a relation -- it was five columns
      -- behind a join. NULL ``task_state`` means this promise has no task,
      -- which is what the join's absence used to say.
      task_state TEXT
        CHECK (task_state IS NULL OR
               task_state IN ('pending', 'acquired', 'suspended', 'halted', 'fulfilled')),
      task_version INTEGER NOT NULL DEFAULT 0,
      pid TEXT,
      ttl INTEGER,

      -- was ``task_timeouts``, whose ``kind`` column discriminated two queues.
      -- Two nullable columns say the same thing without the row, and a task
      -- carries at most one of each.
      retry_timeout_at INTEGER,
      lease_timeout_at INTEGER
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_promises_branch ON promises (branch) WHERE branch IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_promises_state ON promises (state, id)",
    # ``promise_timeouts`` is gone. A pending external promise past its
    # ``timeout_at`` *is* the queue -- membership was never independent of the
    # promise row, because only an external promise arms a durable timeout and
    # settling one is exactly what used to delete its row. This partial index
    # is the index that table carried.
    "CREATE INDEX IF NOT EXISTS idx_promises_due ON promises (timeout_at, id) "
    "WHERE state = 'pending' AND external = 1",
    "CREATE INDEX IF NOT EXISTS idx_promises_retry_due ON promises (retry_timeout_at, id) "
    "WHERE retry_timeout_at IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_promises_lease_due ON promises (lease_timeout_at, id) "
    "WHERE lease_timeout_at IS NOT NULL",
    # The awaiter ids registered against an awaited promise. ``seq`` preserves
    # the spec's append order, which fixes the order resumes are deferred in.
    """
    CREATE TABLE IF NOT EXISTS callbacks (
      awaited_id TEXT NOT NULL,
      awaiter_id TEXT NOT NULL,
      seq INTEGER NOT NULL,
      PRIMARY KEY (awaited_id, awaiter_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_callbacks_awaiter ON callbacks (awaiter_id)",
    """
    CREATE TABLE IF NOT EXISTS listeners (
      promise_id TEXT NOT NULL,
      address TEXT NOT NULL,
      seq INTEGER NOT NULL,
      PRIMARY KEY (promise_id, address)
    )
    """,
    # A task's ``resumes`` list: awaited ids that settled while the task was not
    # suspended, buffered until it next suspends or continues.
    """
    CREATE TABLE IF NOT EXISTS resumes (
      task_id TEXT NOT NULL,
      awaited_id TEXT NOT NULL,
      seq INTEGER NOT NULL,
      PRIMARY KEY (task_id, awaited_id)
    )
    """,
)

TENANT_SCHEMA: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS meta (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )
    """,
    # The tenant-global timeout index. ``kind`` widens the origin database's own
    # encoding so promise and task timers share one due-time ordering:
    #   0 = promise timeout, 1 = task retry, 2 = task lease.
    #
    # ``origin_hash`` is ``hash_origin(origin)``, stored so a sharded fleet can
    # select its own slice in SQL — ``WHERE origin_hash % count = index`` —
    # rather than reading every due timer and discarding most of them. It is a
    # property of the origin alone, so it stays valid when the fleet is resized.
    """
    CREATE TABLE IF NOT EXISTS timeouts (
      origin TEXT NOT NULL,
      origin_hash INTEGER NOT NULL,
      id TEXT NOT NULL,
      kind INTEGER NOT NULL,
      timeout_at INTEGER NOT NULL,
      PRIMARY KEY (origin, id, kind)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_timeouts_due ON timeouts (timeout_at, origin_hash)",
)
