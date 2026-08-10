"""A :class:`~resonate.network.Network` with no server behind it.

Every other network in this SDK is a transport: it carries a request to a
Resonate Server and carries the response back. This one is not. There is no
server; the SDK *is* the server, and the durable state lives in Turso databases
the SDK reads, writes, and syncs directly.

The partition is by workflow. Every promise id is prefixed by its origin -- the
root workflow's id, everything before the first ``.`` -- and each origin gets its
own database, ``<prefix><origin>``. That is not an arbitrary sharding key: the
protocol already guarantees a callback never crosses an origin
(``promise.register_callback`` refuses), so every request touches exactly one
workflow's state and is therefore a single-database transaction. A workflow is a
unit of consistency, and now also a unit of storage -- which is what makes "one
database per workflow" work at all, and what makes each one small enough for a
process to hold as a local replica.

One database is shared: ``<prefix><timeout_database>``, the tenant database. It
holds what no single workflow owns -- the index of armed timers across all
origins, and schedules. The timer index is a mirror, republished from the origin
databases after every commit and re-validated against them before use. It is how
a process finds work in a workflow it has never seen.

**Messages are not stored.** When a transition emits an ``execute`` or
``unblock``, the message is handed to the local Resonate client as soon as the
transaction commits -- no outbox, no queue, no routing table. This is the whole
of message transport, and it means delivery is in-process: the node that ran the
transition is the node that gets the message.

Reaching a *different* process therefore goes through time, not through a queue.
A dispatched task carries a durable retry timer; if nobody claims it, the timer
comes due, and whichever process sweeps the tenant timeout index re-emits the
execute and delivers it to its own client. Recovery is the timer, which is
exactly why the timeout database is the one thing shared.

The consequence to know: ``resonate:target`` is advisory here. The address is
recorded on the promise and echoed in the message, but nothing routes by it -- a
message goes to whoever did the work. In a homogeneous fleet, where every process
registers the same functions, that is what you want. In a heterogeneous one,
where only some processes can run a given function, this network does not
deliver work to the right group.

**Known limitation, measured.** A result computed on one node does not reach the
node waiting for it. ``run(...)`` waits by registering a listener carrying its
own unicast address; if a timer resumed the workflow elsewhere, that node emits
the ``unblock`` and delivers it to itself. Two nodes, one migrating workflow:
done at +165ms, caller notified at +60004ms. ``execute`` escapes this because a
retry timer re-emits it and any node can claim it; ``unblock`` has no
equivalent. Multi-node use needs this closed first -- either by routing messages
to their address, or by having a waiter poll the promise it is blocked on rather
than waiting to be told.

**Compare-and-swap.** Every fenced action is a read-compare-write inside one
``BEGIN IMMEDIATE`` transaction -- a genuine CAS, atomic against whichever
database applies it, and no CAS at all across nodes: embedded-replica mode
applies it to each node's own copy and merges later, and two nodes racing the
same acquire through a real Turso Cloud remote both won 50 times out of 50
(measured with the TypeScript SDK; its ``remoteWrites`` flag was measured
broken too). A fleet must keep one writer per workflow -- static sharding.

**Concurrency -- read this before deploying a fleet.** Within one process this
works: requests against an origin are serialized, workflows run, and a workflow
abandoned mid-flight is recovered off the timeout index.

Across processes, shard. :class:`TursoLocalDriver` cannot be shared at all --
Turso takes an exclusive file lock, so the second process cannot even open the
database. :class:`TursoSyncDriver`, where each node holds its own replica and
they converge through Turso Cloud, is the arrangement this design is for, and
convergence itself is measured (a committed write reaches another replica in
~200ms median; a workflow abandoned by one node is recovered and completed by
another). What is *not* sound is two nodes writing one workflow -- see the
compare-and-swap note above -- so every workflow must have exactly one owner:
set ``shard`` and route with :func:`owner_of`.

The design also concentrates fleet-wide write traffic on the tenant database,
since every origin publishes its timers there. :meth:`TursoStore.flush` skips
the write when an origin's timers are unchanged, which removes most of it, but
that file remains the one global bottleneck.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid
from typing import TYPE_CHECKING, Any, Literal

from resonate import PROTOCOL_VERSION, now_ms
from resonate.error import DecodingError
from resonate.network.turso.cron import CronError, cron_occurrences, next_cron
from resonate.network.turso.driver import (
    TursoConnection,
    TursoDriver,
    TursoExecutor,
    TursoLocalDriver,
    TursoRow,
    TursoSyncDriver,
    database_path,
)
from resonate.network.turso.server import (
    DEFAULT_RETRY_TIMEOUT,
    OriginServer,
    Outcome,
    OutgoingMessage,
    ScheduleStore,
    expand_promise_id,
    hash_origin,
    origin_of,
    owner_of,
    to_schedule_record,
)
from resonate.network.turso.store import (
    TIMEOUT_PROMISE,
    TIMEOUT_TASK_LEASE,
    TIMEOUT_TASK_RETRY,
    TursoStore,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

__all__ = [
    "TIMEOUT_PROMISE",
    "TIMEOUT_TASK_LEASE",
    "TIMEOUT_TASK_RETRY",
    "TursoConnection",
    "TursoDriver",
    "TursoExecutor",
    "TursoLocalDriver",
    "TursoNetwork",
    "TursoRow",
    "TursoSyncDriver",
    "database_path",
    "hash_origin",
    "origin_of",
    "owner_of",
]

logger = logging.getLogger(__name__)


_SCHEDULE_KINDS = frozenset(
    {"schedule.get", "schedule.create", "schedule.delete", "schedule.search"}
)
_BY_ID_KINDS = frozenset(
    {
        "promise.get",
        "promise.create",
        "promise.settle",
        "task.get",
        "task.acquire",
        "task.release",
        "task.suspend",
        "task.halt",
        "task.continue",
        "task.fulfill",
        "task.fence",
    }
)

_SEARCH_UNSUPPORTED = (
    "Tenant-wide {what} search is not supported: {what}s are partitioned by origin. "
    "Narrow the search to one origin with the resonate:origin request header."
)


class TursoNetwork:
    """Runs the Resonate protocol locally against Turso databases."""

    def __init__(
        self,
        driver: TursoDriver,
        *,
        prefix: str = "resonate-",
        timeout_database: str = "timeouts",
        group: str | None = None,
        pid: str | None = None,
        tick_seconds: float = 0.25,
        retry_timeout: int = DEFAULT_RETRY_TIMEOUT,
        max_open_databases: int = 64,
        batch_size: int = 100,
        timeout_driver: TursoDriver | None = None,
        shard: tuple[int, int] | None = None,
        push_on: Literal["boundary", "request"] = "boundary",
    ) -> None:
        """Run the protocol against Turso databases.

        ``timeout_driver`` opens the tenant database when it lives somewhere
        other than the origins, and defaults to ``driver``. The two have
        opposite access patterns: origin databases are owned -- under static
        sharding exactly one node ever touches a given workflow -- whereas the
        timeout index is written by every node, because that is the point of it.
        A driver fine for the first can be wrong for the second, so they split.

        ``shard`` is this node's ``(index, count)`` slice of a sharded fleet.
        Set it and the node sweeps only the timers of origins it owns --
        ``hash_origin(origin) % count == index``. Every node must agree on
        ``count`` and on the hash, which is why the hash ships here as
        :func:`hash_origin` rather than being left to the caller.

        That turns "any node may pick up any workflow" into "each workflow has
        exactly one owner", which is worth having: a workflow stops migrating
        between nodes mid-flight, so it stops contending with itself, and one
        owner means one writer -- which is what the protocol's compare-and-swap
        wants. Routing requests to the owning node is the caller's job; use
        :func:`owner_of` for it, so routing and sweeping agree. Unset (the
        default) means this node owns everything.

        ``push_on`` decides when an origin database is uploaded to the remote,
        on drivers that replicate. ``"boundary"`` (the default) pushes at the
        points another process could ever need to read from: a task ends its
        tenure (fulfill, suspend, release, halt), work becomes visible to the
        fleet (``task.create``, a root or targeted ``promise.create``), or the
        root promise settles. The intermediate durable steps a task records
        while it holds the lease stay on the local replica -- the lease already
        guarantees one writer, so nobody else can need them before the
        boundary. The trade: a crash mid-tenure recovers from the last
        boundary, not the last step. ``"request"`` pushes after every committed
        write, the most conservative cadence.
        """
        if shard is not None:
            index, count = shard
            if count < 1 or not (0 <= index < count):
                msg = f"Invalid shard {index}/{count}: need 0 <= index < count"
                raise ValueError(msg)
        self._shard = shard

        self._pid = pid if pid is not None else uuid.uuid4().hex
        self._group = group if group is not None else "default"
        # A task targets the group; a callback or listener targets this process.
        self._unicast = f"poll://uni@{self._group}/{self._pid}"
        self._anycast = f"poll://any@{self._group}"

        self._store = TursoStore(
            driver,
            prefix,
            timeout_database,
            max_open_databases,
            timeout_driver=timeout_driver,
        )
        self._tick_seconds = tick_seconds
        self._retry_timeout = retry_timeout
        self._batch_size = batch_size
        self._push_on = push_on

        self._subscribers: list[Callable[[str], None]] = []
        self._tick_handle: asyncio.Task[None] | None = None
        self._stopped = False

    # -------------------------------------------------------------------------
    # NETWORK
    # -------------------------------------------------------------------------

    def pid(self) -> str:
        return self._pid

    def group(self) -> str:
        return self._group

    def unicast(self) -> str:
        return self._unicast

    def anycast(self) -> str:
        return self._anycast

    def target_resolver(self, target: str) -> str:
        return f"poll://any@{target}"

    async def start(self) -> None:
        self._stopped = False
        tenant = await self._store.tenant()
        self._warn_if_unsharded_replica(tenant)
        if self._tick_handle is None:
            self._tick_handle = asyncio.create_task(self._tick_loop())

    def _warn_if_unsharded_replica(self, tenant: TursoConnection) -> None:
        """Warn when this node replicates but claims every origin.

        One node owning everything is a perfectly good single-node deployment
        -- hence a warning and not an error. But a *second* node deployed the
        same way is the arrangement measured to let two nodes both win one
        ``task.acquire``, and the config gives no hint of it: ``shard`` reads
        like an optimization.
        """
        if self._shard is not None or not getattr(tenant, "replicates", False):
            return
        logger.warning(
            "turso network is replicating but unsharded: safe for a single node, but a "
            "second node sharing this tenant will double-execute fenced actions "
            "(measured). Set shard= and route with owner_of -- see turso.md."
        )

    async def stop(self) -> None:
        self._stopped = True
        handle = self._tick_handle
        self._tick_handle = None
        if handle is not None:
            handle.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await handle
        self._subscribers.clear()
        await self._store.close()

    def recv(self, callback: Callable[[str], None]) -> None:
        self._subscribers.append(callback)

    async def send(self, req: str) -> str:
        try:
            envelope = json.loads(req)
        except ValueError as exc:
            msg = f"invalid JSON request: {exc}"
            raise DecodingError(msg) from exc

        head = envelope.get("head") or {}
        now = head.get("resonate:debug_time") or now_ms()
        outcome = await self._apply(envelope, now)

        return json.dumps(
            {
                "kind": outcome.kind,
                "head": {
                    "corrId": head.get("corrId"),
                    "status": outcome.status,
                    "version": head.get("version") or PROTOCOL_VERSION,
                },
                "data": outcome.data,
            }
        )

    # -------------------------------------------------------------------------
    # ROUTING
    # -------------------------------------------------------------------------

    async def _apply(self, req: dict[str, Any], now: int) -> Outcome:
        kind = req["kind"]
        data = req.get("data") or {}
        declared = (req.get("head") or {}).get("resonate:origin")

        if kind.startswith("debug."):
            return await self._debug(kind, data, declared, now)

        if kind in _SCHEDULE_KINDS:
            return await self._in_tenant_schedules(kind, data, now)

        # The resonate:origin header may carry a full promise or task id rather
        # than a bare origin -- the engine cores stamp it with the task's id --
        # so it is normalized through origin_of before use. For a request that
        # also names an id, a header disagreeing with that id's origin is
        # refused outright: honoring it would write one workflow's state into
        # another workflow's database.
        def routed(id_: str) -> str | Outcome:
            origin = origin_of(id_)
            if declared is not None and origin_of(declared) != origin:
                return Outcome(
                    kind,
                    400,
                    f"resonate:origin {declared!r} does not match the origin of id {id_!r}",
                )
            return origin

        if kind in _BY_ID_KINDS:
            route = routed(data["id"])
            if isinstance(route, Outcome):
                return route
            return await self._in_origin_apply(route, now, req)

        if kind in {"promise.register_callback", "promise.register_listener"}:
            route = routed(data["awaited"])
            if isinstance(route, Outcome):
                return route
            return await self._in_origin_apply(route, now, req)

        if kind == "task.create":
            route = routed(data["action"]["data"]["id"])
            if isinstance(route, Outcome):
                return route
            return await self._in_origin_apply(route, now, req)

        if kind == "promise.search":
            origin = declared or (data.get("tags") or {}).get("resonate:origin")
            if origin is None:
                return Outcome(kind, 501, _SEARCH_UNSUPPORTED.format(what="promise"))
            return await self._in_origin_apply(origin_of(origin), now, req)

        if kind == "task.search":
            if declared is None:
                return Outcome(kind, 501, _SEARCH_UNSUPPORTED.format(what="task"))
            return await self._in_origin_apply(origin_of(declared), now, req)

        if kind == "task.heartbeat":
            # The only request that fans out: its task list may span workflows,
            # so it is split into one transaction per origin, always by each
            # task's own origin -- a declared header cannot be trusted to cover
            # a list that may span workflows. Each transaction is independent:
            # a heartbeat refreshes leases and can partially apply without
            # breaking any invariant.
            tasks = data.get("tasks") or []
            for origin in dict.fromkeys(origin_of(t["id"]) for t in tasks):
                scoped = {
                    **req,
                    "data": {
                        **data,
                        "tasks": [t for t in tasks if origin_of(t["id"]) == origin],
                    },
                }
                await self._in_origin_apply(origin, now, scoped)
            return Outcome(kind, 200, {})

        return Outcome(kind, 501, "Not implemented")

    # -------------------------------------------------------------------------
    # EXECUTION
    # -------------------------------------------------------------------------

    async def _in_origin_apply(
        self, origin: str, now: int, req: dict[str, Any]
    ) -> Outcome:
        async def run(server: OriginServer) -> Outcome:
            return await server.apply(req)

        return await self._in_origin(origin, now, run, push_origin=self._push_now(req))

    def _push_now(self, req: dict[str, Any]) -> bool:
        """Decide whether this request's flush uploads the origin database.

        Under ``push_on="boundary"``, only at the points another process could
        ever need to read from -- see ``push_on`` in :class:`TursoNetwork`. The
        child creates and settles that record a task's intermediate durable
        steps stay local until the task's next boundary; the lease guarantees
        nobody else is writing this workflow in the meantime, and the sweep
        transitions (which do not come through here) always push.
        """
        if self._push_on == "request":
            return True
        kind = req["kind"]
        data = req.get("data") or {}
        # task.continue is release's mirror -- a halted task re-enters
        # circulation with a fresh retry timer, outside any lease tenure -- so
        # it is a boundary for exactly the reason release is.
        if kind in {
            "task.create",
            "task.fulfill",
            "task.suspend",
            "task.release",
            "task.halt",
            "task.continue",
        }:
            return True
        if kind == "promise.create":
            tags = data.get("tags") or {}
            return data["id"] == origin_of(data["id"]) or "resonate:target" in tags
        if kind == "promise.settle":
            return data["id"] == origin_of(data["id"])
        return False

    async def _in_origin(
        self,
        origin: str,
        now: int,
        fn: Callable[[OriginServer], Awaitable[Any]],
        *,
        push_origin: bool = True,
    ) -> Any:
        """Run one transaction against an origin, publish its timers, deliver its messages.

        Both follow-ups are outside the transaction. The flush writes a
        different database, which a transaction cannot span (see
        :meth:`TursoStore.flush`). The delivery must not happen until the state
        change is durable, and must not happen while this origin's lock is held
        -- a subscriber is free to call back into :meth:`send` for the same
        workflow, and would deadlock against the lock its own message was
        delivered under.
        """
        outgoing: list[OutgoingMessage] = []
        sync_needed = False
        async with self._store.lock(origin):
            conn = await self._store.origin(origin)
            async with conn.transaction() as tx:
                server = OriginServer(tx, now, self._retry_timeout)
                result = await fn(server)
                # Read inside the transaction, delivered after it commits: a
                # rolled-back transaction raises before reaching the dispatch
                # below, so no message is ever delivered for a state change that
                # did not land.
                outgoing = server.take_messages()
                sync_needed = server.take_sync_needed()
            # The transaction has committed. Its messages describe durable
            # state and are owed to the client whether or not the mirror write
            # lands -- the index is eventual by construction and the next flush
            # republishes it, whereas a dropped ``unblock`` has nothing to
            # re-emit it (settlement already deleted the listener rows). So
            # dispatch first, then let the flush failure reach the caller.
            try:
                await self._store.flush(
                    origin, conn, push_origin=push_origin or sync_needed
                )
            except BaseException:
                self._dispatch(outgoing)
                outgoing = []
                raise
        self._dispatch(outgoing)
        return result

    def _dispatch(self, messages: list[OutgoingMessage]) -> None:
        """Hand messages to the local Resonate client.

        Deferred by a turn of the event loop so a subscriber that reacts by
        issuing another request cannot re-enter the call that produced its
        message. This is the whole of message transport: nothing is stored, and
        nothing is routed anywhere else.
        """
        if not messages or not self._subscribers:
            return
        subscribers = list(self._subscribers)
        encoded = [json.dumps(m.message) for m in messages]

        def deliver() -> None:
            if self._stopped:
                return
            for payload in encoded:
                for callback in subscribers:
                    callback(payload)

        asyncio.get_running_loop().call_soon(deliver)

    async def _in_tenant_schedules(
        self, kind: str, data: dict[str, Any], now: int
    ) -> Outcome:
        tenant = await self._store.tenant()
        async with tenant.transaction() as tx:
            schedules = ScheduleStore(tx, now)
            if kind == "schedule.get":
                outcome = await schedules.get(data["id"])
            elif kind == "schedule.create":
                outcome = await schedules.create(data)
            elif kind == "schedule.delete":
                outcome = await schedules.delete(data["id"])
            else:
                outcome = await schedules.search(
                    data.get("tags"), data.get("limit"), data.get("cursor")
                )
        await tenant.push()
        return outcome

    # -------------------------------------------------------------------------
    # TICK: timer sweep and schedule firing
    # -------------------------------------------------------------------------
    #
    # Messages are not ticked -- they go straight to the client when the
    # transaction that produced them commits. What the tick does is fire due
    # timers, and that is also how work reaches a *different* process: an
    # unclaimed task's retry timer is durable and indexed tenant-wide, so
    # whichever process sweeps it re-emits the execute and delivers it locally.

    async def _tick_loop(self) -> None:
        with contextlib.suppress(asyncio.CancelledError):
            while not self._stopped:
                await self._tick(now_ms())
                await asyncio.sleep(self._tick_seconds)

    async def _tick(self, now: int) -> None:
        try:
            await self._sweep_timeouts(now)
            await self._fire_schedules(now)
        except Exception:
            if not self._stopped:
                logger.warning("turso tick failed", exc_info=True)

    async def _sweep_timeouts(self, now: int) -> None:
        """Fire every timer the tenant index says is due.

        The index is a hint. Each transition re-reads its own armed time from the
        origin database and refuses an early firing, so a stale index entry costs
        a wasted open and nothing else.
        """
        tenant = await self._store.tenant()
        await tenant.pull()

        # A sharded node takes only its own slice, in SQL, so a crowded index
        # does not fill its batch with timers belonging to someone else.
        shard_filter = " AND origin_hash % ? = ?" if self._shard else ""
        shard_args = [self._shard[1], self._shard[0]] if self._shard else []
        due: list[TursoRow] = await tenant.execute(
            # S608: the only interpolation is the fixed literal above; the
            # shard itself is bound, not spliced.
            f"SELECT origin, id, kind FROM timeouts WHERE timeout_at <= ?{shard_filter} "  # noqa: S608
            "ORDER BY timeout_at ASC LIMIT ?",
            [now, *shard_args, self._batch_size],
        )
        if not due:
            return

        by_origin: dict[str, list[tuple[str, int]]] = {}
        for row in due:
            by_origin.setdefault(row["origin"], []).append(
                (row["id"], int(row["kind"]))
            )

        for origin, timers in by_origin.items():

            async def fire(
                server: OriginServer, timers: list[tuple[str, int]] = timers
            ) -> None:
                # Promise timeouts first: they settle promises, which fulfils
                # tasks, which makes the task timers below no-ops rather than
                # spurious redispatches of work that is already logically dead.
                for timeout_id, kind in timers:
                    if kind == TIMEOUT_PROMISE:
                        await server.on_promise_timeout(timeout_id)
                for timeout_id, kind in timers:
                    if kind == TIMEOUT_TASK_RETRY:
                        await server.on_task_retry_timeout(timeout_id)
                    elif kind == TIMEOUT_TASK_LEASE:
                        await server.on_task_lease_timeout(timeout_id)

            try:
                await self._in_origin(origin, now, fire)
            except Exception:
                logger.warning(
                    "turso timeout sweep failed for origin %s", origin, exc_info=True
                )

    async def _fire_schedules(self, now: int) -> None:
        """Create the promises due schedules should have fired.

        A schedule lives in the tenant database but the promises it fires belong
        to whichever origin their expanded id names, so this cannot be one
        transaction. It does not need to be: the expanded id is per-occurrence,
        so re-firing after a crash finds the promise already there and writes
        nothing, and the schedule is advanced only once every occurrence is in.
        """
        tenant = await self._store.tenant()
        due = await ScheduleStore(tenant, now).due()

        for row in due:
            record = to_schedule_record(row)
            since = (
                row["last_run_at"]
                if row["last_run_at"] is not None
                else row["next_run_at"] - 1
            )
            try:
                occurrences = cron_occurrences(record["cron"], since, now)
            except CronError:
                logger.warning(
                    "turso skipped schedule %s: unparseable cron expression", row["id"]
                )
                continue
            if not occurrences:
                continue

            for at in occurrences:
                promise_id = expand_promise_id(record["promiseId"], record["id"], at)
                await self._in_origin_apply(
                    origin_of(promise_id),
                    at,
                    {
                        "kind": "promise.create",
                        "head": {
                            "corrId": f"sched-{record['id']}-{at}",
                            "version": PROTOCOL_VERSION,
                        },
                        "data": {
                            "id": promise_id,
                            "timeoutAt": at + record["promiseTimeout"],
                            "param": record["promiseParam"],
                            "tags": {
                                **record["promiseTags"],
                                "resonate:schedule": record["id"],
                            },
                        },
                    },
                )

            last = occurrences[-1]
            async with tenant.transaction() as tx:
                await ScheduleStore(tx, now).advance(
                    row["id"], last, next_cron(record["cron"], last)
                )
            await tenant.push()

    # -------------------------------------------------------------------------
    # DEBUG
    # -------------------------------------------------------------------------

    async def _debug(
        self, kind: str, data: dict[str, Any], origin: str | None, now: int
    ) -> Outcome:
        if kind in {"debug.start", "debug.stop"}:
            return Outcome(kind, 200, {})

        if kind == "debug.tick":
            # Drive one full tick at the caller's clock, so a test can advance
            # time without waiting for it.
            await self._tick(data.get("time") or now)
            return Outcome(kind, 200, [])

        if kind == "debug.reset":
            await self._reset()
            return Outcome(kind, 200, {})

        if kind == "debug.snap":
            if origin is None:
                return Outcome(
                    kind,
                    501,
                    "Tenant-wide snapshots are not supported: state is partitioned by origin. "
                    "Set the resonate:origin request header.",
                )
            return await self._snap(origin, now)

        return Outcome(kind, 501, "Not implemented")

    async def _reset(self) -> None:
        """Empty every database this network has open. Test support only."""
        tenant = await self._store.tenant()
        async with tenant.transaction() as tx:
            for table in ("timeouts", "schedules"):
                await tx.execute(f"DELETE FROM {table}")  # noqa: S608
        # Origin databases are created on demand and never enumerated, so only
        # the ones this process has open can be cleared. A caller wanting a clean
        # slate across processes should point the driver at a fresh directory or
        # prefix.
        await self._store.discard()

    async def _snap(self, origin: str, now: int) -> Outcome:
        async def collect(server: OriginServer) -> dict[str, Any]:
            promises = await server.promise_search(None, None, 1000, None)
            tasks = await server.task_search(None, 1000, None)
            return {"promises": promises.data["promises"], "tasks": tasks.data["tasks"]}

        async with self._store.lock(origin):
            conn = await self._store.origin(origin)
            async with conn.transaction() as tx:
                server = OriginServer(tx, now, self._retry_timeout)
                snapshot = await collect(server)
                promise_timeouts = await tx.execute(
                    "SELECT id, timeout_at FROM promise_timeouts ORDER BY id"
                )
                task_timeouts = await tx.execute(
                    "SELECT id, kind, timeout_at FROM task_timeouts ORDER BY id"
                )
                callbacks = await tx.execute(
                    "SELECT awaited_id, awaiter_id FROM callbacks ORDER BY awaiter_id, awaited_id"
                )
                listeners = await tx.execute(
                    "SELECT promise_id, address FROM listeners ORDER BY promise_id, address"
                )

        return Outcome(
            "debug.snap",
            200,
            {
                "promises": snapshot["promises"],
                "promiseTimeouts": [
                    {"id": r["id"], "timeout": r["timeout_at"]}
                    for r in promise_timeouts
                ],
                "callbacks": [
                    {"awaiter": r["awaiter_id"], "awaited": r["awaited_id"]}
                    for r in callbacks
                ],
                "listeners": [
                    {"id": r["promise_id"], "address": r["address"]} for r in listeners
                ],
                "tasks": snapshot["tasks"],
                "taskTimeouts": [
                    {"id": r["id"], "type": r["kind"], "timeout": r["timeout_at"]}
                    for r in task_timeouts
                ],
                # Always empty: messages are handed to the client on commit
                # and never held, so there is no pending set to snapshot.
                "messages": [],
            },
        )
