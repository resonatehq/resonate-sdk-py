"""The Resonate protocol, executed against one origin database.

This is the piece that makes the network decentralized: there is no server
process to send a request to, so every SDK carries the transition relation and
runs it locally against the workflow's own database, inside one transaction.

The implementation follows `resonatehq/resonate-specification`
(``spec/03-concrete``) rather than the SQL in `resonatehq/resonate`'s
``persistence_sqlite.rs``. The two have diverged, and where they do the spec is
the authority. The differences that matter here:

* **Projection, not mutation.** A pending promise past its deadline is
  *logically* settled; the spec never mutates state to discover that. All guards
  and all responses consult the projected view (:func:`project`), and the stored
  row is converged only by the timeout transition. The SQL server instead eagerly
  settles on read. Projection is what makes a read side-effect free, which in
  turn is what lets a replica serve one without pushing.

* **``external``, not ``resonate:target``, gates durable timeouts.** A promise is
  external when it is tagged ``resonate:external``, carries a
  ``resonate:target``, or is a timer. Only external promises may be awaited
  (``register_callback``/``register_listener`` answer 422 otherwise) and only
  they arm a durable timeout -- an internal promise's deadline is
  projection-only, so an obligation recorded against one could never be
  discharged.

* **``resonate:delay`` defers the first dispatch.** A targeted promise created
  with a delay tag still ahead of ``now`` arms its retry timer at the delay and
  emits no execute message; the timer's first firing dispatches it.

* **Timeout always wins.** Retry and lease timers consult the *projected* promise
  before acting, so a logically dead task is never redispatched and never
  returned to circulation.

* **Deferred resumes.** Settlement does not resume awaiters inline; it records a
  resume obligation per awaiter, drained after the action completes. The drain
  runs in the same transaction as the settlement that scheduled it -- a suspended
  task has no armed timer, so losing a resume would strand it forever.

One deviation from the spec is imposed by the decentralized topology rather than
chosen: ``promise.register_callback`` requires awaiter and awaited to share an
origin. That is already the rule the Resonate Server enforces, and here it is
load-bearing -- a cross-origin callback would span two databases and could not be
registered in one transaction.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Literal, NamedTuple

from resonate.network.turso.cron import CronError, next_cron

if TYPE_CHECKING:
    from collections.abc import Sequence

    from resonate.network.turso.driver import TursoExecutor, TursoRow

# =============================================================================
# CONSTANTS
# =============================================================================

#: Default pending-retry interval: how long a dispatched task may go unclaimed.
DEFAULT_RETRY_TIMEOUT = 30_000

#: The states a client is allowed to settle a promise into.
SETTABLE = frozenset({"resolved", "rejected", "rejected_canceled"})

#: Timeout kinds as stored in an origin database's ``task_timeouts`` table.
TASK_TIMEOUT_RETRY = 0
TASK_TIMEOUT_LEASE = 1


class Outcome(NamedTuple):
    """What a handler returns before the network stamps the response head."""

    kind: str
    status: int
    data: Any


class _Fence(NamedTuple):
    """The result of the fence check every acquired-task action shares."""

    ok: bool
    status: int
    data: str
    task: TursoRow | None
    promise: TursoRow | None


class _Resume(NamedTuple):
    """A resume obligation: "awaited settled, wake awaiter"."""

    awaited: str
    awaiter: str


# =============================================================================
# PURE HELPERS
# =============================================================================


def origin_of(promise_id: str) -> str:
    """Return the origin a promise id belongs to.

    Everything before the first ``.``, or the whole id when it has none. This is
    the server's own rule (see ``origin()`` in ``resonate/src/types.rs``) and it
    is what partitions the tenant into databases -- one per workflow root.
    """
    head, _, _ = promise_id.partition(".")
    return head


def address_valid(address: str) -> bool:
    """Report whether an address is deliverable: HTTP, or a poll address naming a group."""
    return address.startswith(("http://", "https://")) or (
        address.startswith("poll://") and "@" in address
    )


def _parse_delay(raw: str | None) -> int | None:
    """Total decimal parse; a malformed tag yields ``None`` rather than raising."""
    if raw is None or not raw.isdigit():
        return None
    return int(raw)


def _parse_tags(raw: str) -> dict[str, str]:
    try:
        tags = json.loads(raw)
    except (ValueError, TypeError):
        return {}
    if not isinstance(tags, dict):
        return {}
    return {k: v for k, v in tags.items() if isinstance(v, str)}


def _encode_value(value: Any) -> tuple[str | None, str | None]:
    if not isinstance(value, dict):
        return (None, None)
    headers = value.get("headers")
    data = value.get("data")
    return (
        None if headers is None else json.dumps(headers),
        None if data is None else json.dumps(data),
    )


def _decode_value(headers: str | None, data: str | None) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, raw in (("headers", headers), ("data", data)):
        if raw is None:
            continue
        try:
            value[key] = json.loads(raw)
        except ValueError:
            # A corrupt blob is treated as absent rather than failing the request.
            continue
    return value


def _tag_columns(tags: dict[str, str]) -> tuple[str | None, str | None, int, int]:
    """Derive the tag columns, written explicitly because Turso has no generated columns."""
    target = tags.get("resonate:target")
    timer = 1 if tags.get("resonate:timer") == "true" else 0
    external = (
        1
        if (tags.get("resonate:external") == "true" or target is not None or timer)
        else 0
    )
    return target, tags.get("resonate:branch"), timer, external


def project(row: TursoRow, now: int) -> TursoRow:
    """Return the logical view of a promise at ``now``.

    A pending promise past its deadline is settled -- resolved for timers,
    rejected_timedout otherwise -- stamped at the deadline, so the projected
    record is identical to the one the timeout transition eventually writes.
    """
    if row["state"] != "pending" or now < row["timeout_at"]:
        return row
    return {
        **row,
        "state": "resolved" if row["timer"] else "rejected_timedout",
        "settled_at": row["timeout_at"],
    }


def live(row: TursoRow, now: int) -> bool:
    """Report whether the promise is logically live: pending and not past its deadline."""
    return row["state"] == "pending" and row["timeout_at"] > now


def to_promise_record(row: TursoRow) -> dict[str, Any]:
    record: dict[str, Any] = {
        "id": row["id"],
        "state": row["state"],
        "param": _decode_value(row["param_headers"], row["param_data"]),
        "value": _decode_value(row["value_headers"], row["value_data"]),
        "tags": _parse_tags(row["tags"]),
        "timeoutAt": row["timeout_at"],
        "createdAt": row["created_at"],
    }
    if row["settled_at"] is not None:
        record["settledAt"] = row["settled_at"]
    return record


def to_task_record(row: TursoRow, resumes: int) -> dict[str, Any]:
    record: dict[str, Any] = {
        "id": row["id"],
        "state": row["state"],
        "version": row["version"],
        "resumes": resumes,
    }
    if row.get("pid") is not None:
        record["pid"] = row["pid"]
    if row.get("ttl") is not None:
        record["ttl"] = row["ttl"]
    return record


def _message_key(address: str, msg: dict[str, Any]) -> str:
    if msg["kind"] == "execute":
        return str(msg["data"]["task"]["id"])
    return f"{msg['data']['promise']['id']}:notify:{address}"


def _tag_filter(
    tags: dict[str, str] | None, column: str
) -> tuple[list[str], list[Any]]:
    """Build an AND-chain of tag equality tests.

    Compound SELECTs are not supported inside WHERE subqueries on Turso, so the
    tag filter cannot be the server's ``EXCEPT`` formulation.
    """
    clauses: list[str] = []
    args: list[Any] = []
    for key, value in (tags or {}).items():
        clauses.append(f"json_extract({column}, ?) = ?")
        args.extend([f"$.{json.dumps(key)}", value])
    return clauses, args


# =============================================================================
# ORIGIN SERVER
# =============================================================================


class OriginServer:
    """One origin database's view of the protocol, for the duration of one transaction."""

    def __init__(
        self, tx: TursoExecutor, now: int, retry_timeout: int = DEFAULT_RETRY_TIMEOUT
    ) -> None:
        self._tx = tx
        self._now = now
        self._retry_timeout = retry_timeout
        self._deferred: list[_Resume] = []

    async def apply(self, req: dict[str, Any]) -> Outcome:
        """Apply one request, then drain the resume obligations it scheduled.

        The drain is the spec's ``step``: settlement records obligations rather
        than resuming inline, and every action is followed by exactly one drain
        pass. A resume never settles anything, so one pass always suffices.
        """
        outcome = await self._dispatch(req)
        await self._drain()
        return outcome

    async def _dispatch(self, req: dict[str, Any]) -> Outcome:
        kind = req["kind"]
        data = req.get("data") or {}

        match kind:
            case "promise.get":
                return await self._promise_get(data["id"])
            case "promise.create":
                return await self._promise_create(data)
            case "promise.settle":
                return await self._promise_settle(data)
            case "promise.register_callback":
                return await self._register_callback(data["awaited"], data["awaiter"])
            case "promise.register_listener":
                return await self._register_listener(data["awaited"], data["address"])
            case "promise.search":
                return await self.promise_search(
                    data.get("state"),
                    data.get("tags"),
                    data.get("limit"),
                    data.get("cursor"),
                )
            case "task.get":
                return await self._task_get(data["id"])
            case "task.create":
                return await self._task_create(data["pid"], data["ttl"], data["action"])
            case "task.acquire":
                return await self._task_acquire(
                    data["id"], data["version"], data["pid"], data["ttl"]
                )
            case "task.release":
                return await self._task_release(data["id"], data["version"])
            case "task.suspend":
                return await self._task_suspend(
                    data["id"], data["version"], data["actions"]
                )
            case "task.halt":
                return await self._task_halt(data["id"])
            case "task.continue":
                return await self._task_continue(data["id"])
            case "task.fulfill":
                return await self._task_fulfill(
                    data["id"], data["version"], data["action"]
                )
            case "task.fence":
                return await self._task_fence(
                    data["id"], data["version"], data["action"]
                )
            case "task.heartbeat":
                return await self._task_heartbeat(data["pid"], data["tasks"])
            case "task.search":
                return await self.task_search(
                    data.get("state"), data.get("limit"), data.get("cursor")
                )
            case _:
                return Outcome(kind, 501, "Not implemented")

    # =========================================================================
    # PROMISE
    # =========================================================================

    async def _promise_get(self, promise_id: str) -> Outcome:
        row = await self._get_promise(promise_id)
        if row is None:
            return Outcome("promise.get", 404, "Promise not found")
        return Outcome(
            "promise.get", 200, {"promise": to_promise_record(project(row, self._now))}
        )

    async def _promise_create(self, data: dict[str, Any]) -> Outcome:
        return await self._create_promise(data)

    async def _create_promise(self, data: dict[str, Any]) -> Outcome:
        """Run the ``promise.create`` transition, shared with ``task.fence``."""
        kind = "promise.create"
        promise_id = data["id"]
        timeout_at = data["timeoutAt"]
        tags: dict[str, str] = data.get("tags") or {}

        # Validation precedes the lookup: the server validates on deserialize, so
        # a malformed request is a 400 whether or not the promise happens to exist.
        invalid = validate_create(promise_id, timeout_at, tags)
        if invalid is not None:
            return Outcome(kind, 400, invalid)

        existing = await self._get_promise(promise_id)
        if existing is not None:
            return Outcome(
                kind, 200, {"promise": to_promise_record(project(existing, self._now))}
            )

        target, _branch, timer, external = _tag_columns(tags)

        if timeout_at > self._now:
            row = await self._insert_promise(
                promise_id,
                "pending",
                data.get("param"),
                tags,
                timeout_at,
                self._now,
                None,
            )

            # Only external promises arm a durable timeout -- an internal
            # promise's deadline is projection-only, and nothing may await it.
            if external:
                await self._set_promise_timeout(promise_id, timeout_at)

            if target is not None:
                await self._set_task(promise_id, "pending", 0, None, None)
                delay = _parse_delay(tags.get("resonate:delay"))
                if delay is not None and delay > self._now:
                    # Deferred dispatch: arm the retry timer at the delay and
                    # emit nothing. The timer's first firing is the first
                    # dispatch.
                    await self._set_task_timeout(promise_id, TASK_TIMEOUT_RETRY, delay)
                else:
                    await self._set_task_timeout(
                        promise_id, TASK_TIMEOUT_RETRY, self._now + self._retry_timeout
                    )
                    await self._set_message(target, _execute(promise_id, 0))

            return Outcome(kind, 200, {"promise": to_promise_record(row)})

        # Born settled: created at or after its own deadline.
        row = await self._insert_promise(
            promise_id,
            "resolved" if timer else "rejected_timedout",
            data.get("param"),
            tags,
            timeout_at,
            timeout_at,
            timeout_at,
        )
        if target is not None:
            await self._set_task(promise_id, "fulfilled", 0, None, None)
        return Outcome(kind, 200, {"promise": to_promise_record(row)})

    async def _promise_settle(self, data: dict[str, Any]) -> Outcome:
        return await self._settle_promise(data)

    async def _settle_promise(self, data: dict[str, Any]) -> Outcome:
        """Run the ``promise.settle`` transition, shared with ``task.fence``."""
        kind = "promise.settle"
        if data["state"] not in SETTABLE:
            return Outcome(kind, 400, "Promise cannot be settled into that state")

        row = await self._get_promise(data["id"])
        if row is None:
            return Outcome(kind, 404, "Promise not found")

        if not live(row, self._now):
            return Outcome(
                kind, 200, {"promise": to_promise_record(project(row, self._now))}
            )

        settled = await self._settle(row, data["state"], data.get("value"), self._now)
        return Outcome(kind, 200, {"promise": to_promise_record(settled)})

    async def _register_callback(self, awaited: str, awaiter: str) -> Outcome:
        kind = "promise.register_callback"
        if awaited == awaiter:
            return Outcome(kind, 400, "Awaited and awaiter must be different promises")
        # Imposed by the topology: a cross-origin callback would span databases.
        if origin_of(awaited) != origin_of(awaiter):
            return Outcome(
                kind, 400, "Awaiter and awaited must belong to the same origin"
            )

        awaited_row = await self._get_promise(awaited)
        if awaited_row is None:
            return Outcome(kind, 404, "Awaited promise not found")

        awaiter_row = await self._get_promise(awaiter)
        if awaiter_row is None:
            return Outcome(kind, 422, "Awaiter promise not found")
        if awaiter_row["target"] is None:
            return Outcome(kind, 422, "Awaiter has no address")
        if not awaited_row["external"]:
            return Outcome(kind, 422, "Awaited promise is not external")

        if live(awaited_row, self._now) and live(awaiter_row, self._now):
            await self._add_callback(awaited, awaiter)

        return Outcome(
            kind, 200, {"promise": to_promise_record(project(awaited_row, self._now))}
        )

    async def _register_listener(self, awaited: str, address: str) -> Outcome:
        kind = "promise.register_listener"
        if not address_valid(address):
            return Outcome(kind, 400, "Invalid listener address")

        row = await self._get_promise(awaited)
        if row is None:
            return Outcome(kind, 404, "Promise not found")
        if not row["external"]:
            return Outcome(kind, 422, "Awaited promise is not external")

        if live(row, self._now):
            await self._add_listener(awaited, address)

        return Outcome(
            kind, 200, {"promise": to_promise_record(project(row, self._now))}
        )

    async def promise_search(
        self,
        state: str | None,
        tags: dict[str, str] | None,
        limit: int | None,
        cursor: str | None,
    ) -> Outcome:
        """Search within this origin.

        A tenant-wide scan is not offered: promises are partitioned across one
        database per origin, so "every promise" is not a query any single
        database can answer. The network routes a search carrying a
        ``resonate:origin`` tag filter here and answers everything else with 501
        rather than returning a partial result that reads like a complete one.
        """
        size = min(max(limit or 100, 1), 1000)
        clauses: list[str] = []
        args: list[Any] = []
        if state is not None:
            clauses.append("state = ?")
            args.append(state)
        if cursor is not None:
            clauses.append("id > ?")
            args.append(cursor)
        tag_clauses, tag_args = _tag_filter(tags, "tags")
        clauses.extend(tag_clauses)
        args.extend(tag_args)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = await self._tx.execute(
            f"SELECT * FROM promises {where} ORDER BY id ASC LIMIT ?",  # noqa: S608
            [*args, size],
        )

        data: dict[str, Any] = {
            "promises": [to_promise_record(project(row, self._now)) for row in rows]
        }
        if len(rows) == size:
            data["cursor"] = rows[-1]["id"]
        return Outcome("promise.search", 200, data)

    # =========================================================================
    # TASK
    # =========================================================================

    async def _task_get(self, task_id: str) -> Outcome:
        kind = "task.get"
        task = await self._get_task(task_id)
        if task is None:
            return Outcome(kind, 404, "Task not found")
        promise = await self._get_promise(task_id)
        if promise is None:
            return Outcome(kind, 404, "Promise not found")

        if not live(promise, self._now):
            fulfilled = {**task, "state": "fulfilled", "pid": None, "ttl": None}
            return Outcome(kind, 200, {"task": to_task_record(fulfilled, 0)})
        return Outcome(
            kind,
            200,
            {"task": to_task_record(task, await self._count_resumes(task_id))},
        )

    async def _task_create(self, pid: str, ttl: int, action: dict[str, Any]) -> Outcome:
        kind = "task.create"
        a = action["data"]
        tags: dict[str, str] = a.get("tags") or {}
        if not tags.get("resonate:target"):
            return Outcome(kind, 400, "Action must have a resonate:target tag")
        if "resonate:delay" in tags:
            return Outcome(kind, 400, "Action must not have a resonate:delay tag")
        if ttl <= 0:
            return Outcome(kind, 400, "TTL must be a positive integer")

        promise_id = a["id"]
        timeout_at = a["timeoutAt"]
        invalid = validate_create(promise_id, timeout_at, tags)
        if invalid is not None:
            return Outcome(kind, 400, invalid)

        existing = await self._get_promise(promise_id)

        if existing is None:
            _target, _, timer, _external = _tag_columns(tags)

            if timeout_at > self._now:
                promise = await self._insert_promise(
                    promise_id,
                    "pending",
                    a.get("param"),
                    tags,
                    timeout_at,
                    self._now,
                    None,
                )
                await self._set_promise_timeout(promise_id, timeout_at)
                task = await self._set_task(promise_id, "acquired", 1, pid, ttl)
                await self._set_task_timeout(
                    promise_id, TASK_TIMEOUT_LEASE, self._now + ttl
                )
                return Outcome(
                    kind,
                    200,
                    {
                        "task": to_task_record(task, 0),
                        "promise": to_promise_record(promise),
                        "preload": await self._preload(promise_id),
                    },
                )

            # Born settled -- serve the fulfilled pair, arm nothing.
            promise = await self._insert_promise(
                promise_id,
                "resolved" if timer else "rejected_timedout",
                a.get("param"),
                tags,
                timeout_at,
                timeout_at,
                timeout_at,
            )
            task = await self._set_task(promise_id, "fulfilled", 0, None, None)
            return Outcome(
                kind,
                200,
                {
                    "task": to_task_record(task, 0),
                    "promise": to_promise_record(promise),
                    "preload": [],
                },
            )

        if existing["target"] is None:
            return Outcome(kind, 422, "Promise has no address")

        task = await self._get_task(promise_id)
        if task is None:
            return Outcome(kind, 409, "Task not found")

        # Re-acquisition is gated on the PROJECTED promise: no lease is ever
        # armed on a logically dead task.
        if not live(existing, self._now):
            fulfilled = {**task, "state": "fulfilled", "pid": None, "ttl": None}
            return Outcome(
                kind,
                200,
                {
                    "task": to_task_record(fulfilled, 0),
                    "promise": to_promise_record(project(existing, self._now)),
                    "preload": await self._preload(promise_id),
                },
            )

        if task["state"] == "fulfilled":
            return Outcome(
                kind,
                200,
                {
                    "task": to_task_record(task, await self._count_resumes(promise_id)),
                    "promise": to_promise_record(existing),
                    "preload": await self._preload(promise_id),
                },
            )

        if task["state"] != "pending":
            return Outcome(kind, 409, "Task already exists")

        acquired = await self._set_task(
            promise_id, "acquired", task["version"] + 1, pid, ttl
        )
        await self._clear_resumes(promise_id)
        await self._del_task_timeout(promise_id)
        await self._set_task_timeout(promise_id, TASK_TIMEOUT_LEASE, self._now + ttl)
        return Outcome(
            kind,
            200,
            {
                "task": to_task_record(acquired, 0),
                "promise": to_promise_record(existing),
                "preload": await self._preload(promise_id),
            },
        )

    async def _task_acquire(
        self, task_id: str, version: int, pid: str, ttl: int
    ) -> Outcome:
        kind = "task.acquire"
        task = await self._get_task(task_id)
        if task is None:
            return Outcome(kind, 404, "Task not found")
        promise = await self._get_promise(task_id)
        if promise is None:
            return Outcome(kind, 409, "Promise not found")

        if task["state"] != "pending":
            return Outcome(kind, 409, "Task not in pending state")
        if not live(promise, self._now):
            return Outcome(kind, 409, "Task logically fulfilled")
        if task["version"] != version:
            return Outcome(kind, 409, "Version mismatch")

        acquired = await self._set_task(
            task_id, "acquired", task["version"] + 1, pid, ttl
        )
        await self._clear_resumes(task_id)
        await self._del_task_timeout(task_id)
        await self._set_task_timeout(task_id, TASK_TIMEOUT_LEASE, self._now + ttl)

        return Outcome(
            kind,
            200,
            {
                "task": to_task_record(acquired, 0),
                "promise": to_promise_record(promise),
                "preload": await self._preload(task_id),
            },
        )

    async def _task_release(self, task_id: str, version: int) -> Outcome:
        kind = "task.release"
        fence = await self._fenced(task_id, version)
        if not fence.ok:
            return Outcome(kind, fence.status, fence.data)
        assert fence.task is not None
        assert fence.promise is not None

        released = await self._set_task(
            task_id, "pending", fence.task["version"], None, None
        )
        await self._del_task_timeout(task_id)
        await self._set_task_timeout(
            task_id, TASK_TIMEOUT_RETRY, self._now + self._retry_timeout
        )
        await self._dispatch_task(fence.promise, released)

        return Outcome(kind, 200, {})

    async def _task_suspend(
        self, task_id: str, version: int, actions: Sequence[dict[str, Any]]
    ) -> Outcome:
        kind = "task.suspend"
        if not actions:
            return Outcome(kind, 400, "Actions list must not be empty")
        awaited_ids = [a["data"]["awaited"] for a in actions]
        if any(a["data"]["awaiter"] != task_id for a in actions):
            return Outcome(kind, 400, "Awaiter must be the suspending task")
        if task_id in awaited_ids:
            return Outcome(kind, 400, "Task cannot await its own promise")
        if len(set(awaited_ids)) != len(awaited_ids):
            return Outcome(kind, 400, "Awaited promises must be distinct")

        fence = await self._fenced(task_id, version)
        if not fence.ok:
            return Outcome(kind, fence.status, fence.data)
        assert fence.task is not None

        # Callbacks are registered only when EVERY awaited promise is still
        # logically pending; one settled awaited resumes the task immediately.
        settled = False
        for awaited in awaited_ids:
            row = await self._get_promise(awaited)
            if row is None:
                return Outcome(kind, 422, "Awaited promise not found")
            if not row["external"]:
                return Outcome(kind, 422, "Awaited promise is not external")
            if not live(row, self._now):
                settled = True

        if settled:
            await self._clear_resumes(task_id)
            return Outcome(kind, 300, {"preload": await self._preload(task_id)})

        for awaited in awaited_ids:
            await self._add_callback(awaited, task_id)
        await self._set_task(task_id, "suspended", fence.task["version"], None, None)
        await self._clear_resumes(task_id)
        await self._del_task_timeout(task_id)

        return Outcome(kind, 200, {})

    async def _task_halt(self, task_id: str) -> Outcome:
        kind = "task.halt"
        task = await self._get_task(task_id)
        if task is None:
            return Outcome(kind, 404, "Task not found")
        promise = await self._get_promise(task_id)
        if promise is None:
            return Outcome(kind, 404, "Promise not found")

        # A task whose promise is logically settled has no circulation left to
        # take it out of; branching on the stored state here would make the
        # stored-vs-projected divergence observable.
        if not live(promise, self._now):
            return Outcome(kind, 409, "Task is fulfilled")
        if task["state"] == "fulfilled":
            return Outcome(kind, 409, "Task is fulfilled")
        if task["state"] == "halted":
            return Outcome(kind, 200, {})

        await self._set_task(task_id, "halted", task["version"], None, None)
        await self._del_task_timeout(task_id)
        return Outcome(kind, 200, {})

    async def _task_continue(self, task_id: str) -> Outcome:
        kind = "task.continue"
        task = await self._get_task(task_id)
        if task is None:
            return Outcome(kind, 404, "Task not found")
        if task["state"] != "halted":
            return Outcome(kind, 409, "Task is not halted")
        promise = await self._get_promise(task_id)
        if promise is None:
            return Outcome(kind, 404, "Promise not found")
        if not live(promise, self._now):
            return Outcome(kind, 409, "Task is fulfilled")

        resumed = await self._set_task(
            task_id, "pending", task["version"], task.get("pid"), task.get("ttl")
        )
        await self._set_task_timeout(
            task_id, TASK_TIMEOUT_RETRY, self._now + self._retry_timeout
        )
        await self._dispatch_task(promise, resumed)

        return Outcome(kind, 200, {})

    async def _task_fulfill(
        self, task_id: str, version: int, action: dict[str, Any]
    ) -> Outcome:
        kind = "task.fulfill"
        a = action["data"]
        if a["id"] != task_id:
            return Outcome(kind, 400, "Promise ID must match task ID")
        if a["state"] not in SETTABLE:
            return Outcome(kind, 400, "Promise cannot be settled into that state")

        fence = await self._fenced(task_id, version)
        if not fence.ok:
            return Outcome(kind, fence.status, fence.data)
        assert fence.promise is not None

        settled = await self._settle(
            fence.promise, a["state"], a.get("value"), self._now
        )
        return Outcome(kind, 200, {"promise": to_promise_record(settled)})

    async def _task_fence(
        self, task_id: str, version: int, action: dict[str, Any]
    ) -> Outcome:
        kind = "task.fence"
        a = action["data"]
        if a["id"] == task_id:
            return Outcome(kind, 400, "Fenced action must target another promise")
        # A fenced action lands in the fencing task's own database, so it has to
        # belong to the same origin.
        if origin_of(a["id"]) != origin_of(task_id):
            return Outcome(kind, 400, "Fenced action must belong to the same origin")

        fence = await self._fenced(task_id, version)
        if not fence.ok:
            message = fence.data if fence.status == 404 else "Fence check failed"
            return Outcome(kind, fence.status, message)

        inner = (
            await self._create_promise(a)
            if action["kind"] == "promise.create"
            else await self._settle_promise(a)
        )

        head = action.get("head") or {}
        return Outcome(
            kind,
            200,
            {
                "action": {
                    "kind": action["kind"],
                    "head": {
                        "corrId": head.get("corrId"),
                        "status": inner.status,
                        "version": head.get("version"),
                    },
                    "data": inner.data,
                },
                "preload": [],
            },
        )

    async def _task_heartbeat(
        self, pid: str, tasks: Sequence[dict[str, Any]]
    ) -> Outcome:
        for ref in tasks:
            task = await self._get_task(ref["id"])
            if (
                task is None
                or task["state"] != "acquired"
                or task["version"] != ref["version"]
                or task["pid"] != pid
            ):
                continue
            promise = await self._get_promise(ref["id"])
            if promise is None or not live(promise, self._now):
                continue

            await self._del_task_timeout(ref["id"])
            await self._set_task_timeout(
                ref["id"], TASK_TIMEOUT_LEASE, self._now + (task["ttl"] or 0)
            )
        return Outcome("task.heartbeat", 200, {})

    async def task_search(
        self, state: str | None, limit: int | None, cursor: str | None
    ) -> Outcome:
        size = min(max(limit or 100, 1), 1000)
        clauses: list[str] = []
        args: list[Any] = []
        if state is not None:
            clauses.append("state = ?")
            args.append(state)
        if cursor is not None:
            clauses.append("id > ?")
            args.append(cursor)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = await self._tx.execute(
            f"SELECT * FROM tasks {where} ORDER BY id ASC LIMIT ?",  # noqa: S608
            [*args, size],
        )

        tasks = [
            to_task_record(row, await self._count_resumes(row["id"])) for row in rows
        ]
        data: dict[str, Any] = {"tasks": tasks}
        if len(rows) == size:
            data["cursor"] = rows[-1]["id"]
        return Outcome("task.search", 200, data)

    # =========================================================================
    # TIMEOUTS
    # =========================================================================
    #
    # Every timeout transition re-checks its own due time before acting: an armed
    # timer means NOT BEFORE, and the machine enforces the contract rather than
    # trusting whoever fired it. That is what makes the tenant-global timeout
    # index safe to treat as a hint.

    async def on_promise_timeout(self, promise_id: str) -> None:
        """Settle a promise whose deadline has passed, converging stored to logical state."""
        row = await self._get_promise(promise_id)
        if row is None or row["state"] != "pending" or row["timeout_at"] > self._now:
            return

        listeners = await self._get_listeners(promise_id)
        callbacks = await self._get_callbacks(promise_id)
        state = "resolved" if row["timer"] else "rejected_timedout"

        await self._tx.execute(
            "UPDATE promises SET state = ?, settled_at = ? WHERE id = ?",
            [state, row["timeout_at"], promise_id],
        )
        await self._tx.execute(
            "DELETE FROM callbacks WHERE awaited_id = ?", [promise_id]
        )
        await self._tx.execute(
            "DELETE FROM listeners WHERE promise_id = ?", [promise_id]
        )
        await self._del_promise_timeout(promise_id)

        task = await self._get_task(promise_id)
        if task is not None:
            await self._set_task(promise_id, "fulfilled", task["version"], None, None)
            await self._clear_resumes(promise_id)
            await self._del_task_timeout(promise_id)

        settled = {**row, "state": state, "settled_at": row["timeout_at"]}
        for address in listeners:
            await self._set_message(address, _unblock(to_promise_record(settled)))
        for awaiter in callbacks:
            self._defer(_Resume(promise_id, awaiter))

        await self._drain()

    async def on_task_retry_timeout(self, task_id: str) -> None:
        """Redispatch a pending task whose retry interval elapsed."""
        armed = await self._get_task_timeout(task_id, TASK_TIMEOUT_RETRY)
        if armed is None or armed > self._now:
            return

        task = await self._get_task(task_id)
        if task is None or task["state"] != "pending":
            return

        promise = await self._get_promise(task_id)
        # Timeout always wins: a logically dead task creates no new work -- its
        # cleanup belongs to the promise-timeout transition.
        if promise is None or not live(promise, self._now):
            return

        await self._del_task_timeout(task_id)
        await self._set_task_timeout(
            task_id, TASK_TIMEOUT_RETRY, self._now + self._retry_timeout
        )
        await self._dispatch_task(promise, task)

    async def on_task_lease_timeout(self, task_id: str) -> None:
        """Return an acquired task whose lease expired to circulation."""
        armed = await self._get_task_timeout(task_id, TASK_TIMEOUT_LEASE)
        if armed is None or armed > self._now:
            return

        task = await self._get_task(task_id)
        if task is None or task["state"] != "acquired":
            return

        promise = await self._get_promise(task_id)
        # An expired lease on a logically dead task is not returned to
        # circulation -- there is no circulation left.
        if promise is None or not live(promise, self._now):
            return

        released = await self._set_task(task_id, "pending", task["version"], None, None)
        await self._del_task_timeout(task_id)
        await self._set_task_timeout(
            task_id, TASK_TIMEOUT_RETRY, self._now + self._retry_timeout
        )
        await self._dispatch_task(promise, released)

    # =========================================================================
    # RESUMES
    # =========================================================================

    def _defer(self, resume: _Resume) -> None:
        if resume not in self._deferred:
            self._deferred.append(resume)

    async def _drain(self) -> None:
        """Discharge every recorded resume obligation.

        A resume settles nothing, so one pass suffices.
        """
        while self._deferred:
            await self._on_resume(self._deferred.pop(0))

    async def _on_resume(self, resume: _Resume) -> None:
        task = await self._get_task(resume.awaiter)
        if task is None:
            return
        promise = await self._get_promise(resume.awaiter)
        if promise is None or self._now >= promise["timeout_at"]:
            return  # the awaiter is itself expired

        if task["state"] == "suspended":
            await self._set_task(
                resume.awaiter,
                "pending",
                task["version"],
                task.get("pid"),
                task.get("ttl"),
            )
            await self._clear_resumes(resume.awaiter)
            await self._add_resume(resume.awaiter, resume.awaited)
            await self._set_task_timeout(
                resume.awaiter, TASK_TIMEOUT_RETRY, self._now + self._retry_timeout
            )
            await self._dispatch_task(promise, task)
            return

        if task["state"] in {"pending", "acquired", "halted"}:
            # Buffer: the task is already awake, so record the resume for the
            # next time it suspends or continues.
            await self._add_resume(resume.awaiter, resume.awaited)

    # =========================================================================
    # SHARED TRANSITIONS
    # =========================================================================

    async def _fenced(self, task_id: str, version: int) -> _Fence:
        """Run the fence check every acquired-task action shares.

        The task exists, its promise is logically live, the task is acquired, and
        the version matches.
        """
        task = await self._get_task(task_id)
        if task is None:
            return _Fence(
                ok=False, status=404, data="Task not found", task=None, promise=None
            )
        promise = await self._get_promise(task_id)
        if promise is None:
            return _Fence(
                ok=False, status=409, data="Promise not found", task=None, promise=None
            )
        if task["state"] != "acquired":
            return _Fence(
                ok=False, status=409, data="Task not acquired", task=None, promise=None
            )
        if not live(promise, self._now):
            return _Fence(
                ok=False, status=409, data="Task not acquired", task=None, promise=None
            )
        if task["version"] != version:
            return _Fence(
                ok=False, status=409, data="Version mismatch", task=None, promise=None
            )
        return _Fence(ok=True, status=200, data="", task=task, promise=promise)

    async def _settle(
        self,
        row: TursoRow,
        state: str,
        value: Any,
        settled_at: int,
    ) -> TursoRow:
        """Settle a live promise.

        Writes the terminal state, fulfils the task bound to it, notifies
        listeners, and records a resume obligation per awaiter.
        """
        listeners = await self._get_listeners(row["id"])
        callbacks = await self._get_callbacks(row["id"])
        headers, data = _encode_value(value)

        await self._tx.execute(
            "UPDATE promises SET state = ?, value_headers = ?, value_data = ?, settled_at = ? WHERE id = ?",
            [state, headers, data, settled_at, row["id"]],
        )
        await self._tx.execute(
            "DELETE FROM callbacks WHERE awaited_id = ?", [row["id"]]
        )
        await self._tx.execute(
            "DELETE FROM listeners WHERE promise_id = ?", [row["id"]]
        )
        await self._del_promise_timeout(row["id"])

        task = await self._get_task(row["id"])
        if task is not None:
            await self._set_task(row["id"], "fulfilled", task["version"], None, None)
            await self._clear_resumes(row["id"])
            await self._del_task_timeout(row["id"])

        settled = {
            **row,
            "state": state,
            "value_headers": headers,
            "value_data": data,
            "settled_at": settled_at,
        }

        for address in listeners:
            await self._set_message(address, _unblock(to_promise_record(settled)))
        for awaiter in callbacks:
            self._defer(_Resume(row["id"], awaiter))

        return settled

    async def _dispatch_task(self, promise: TursoRow, task: TursoRow) -> None:
        """Emit the execute message for a task whose promise carries a target."""
        if promise["target"] is None:
            return
        await self._set_message(
            promise["target"], _execute(task["id"], task["version"])
        )

    async def _preload(self, promise_id: str) -> list[dict[str, Any]]:
        """Promises sharing this one's ``resonate:branch``, so the caller can skip round trips."""
        row = await self._get_promise(promise_id)
        if row is None or row["branch"] is None:
            return []
        rows = await self._tx.execute(
            "SELECT * FROM promises WHERE branch = ? AND id != ? ORDER BY id ASC",
            [row["branch"], promise_id],
        )
        return [to_promise_record(project(r, self._now)) for r in rows]

    # =========================================================================
    # STORAGE
    # =========================================================================

    async def _get_promise(self, promise_id: str) -> TursoRow | None:
        rows = await self._tx.execute(
            "SELECT * FROM promises WHERE id = ?", [promise_id]
        )
        return rows[0] if rows else None

    async def _insert_promise(
        self,
        promise_id: str,
        state: str,
        param: Any,
        tags: dict[str, str],
        timeout_at: int,
        created_at: int,
        settled_at: int | None,
    ) -> TursoRow:
        headers, data = _encode_value(param)
        target, branch, timer, external = _tag_columns(tags)
        row: TursoRow = {
            "id": promise_id,
            "state": state,
            "param_headers": headers,
            "param_data": data,
            "value_headers": None,
            "value_data": None,
            "tags": json.dumps(tags),
            "target": target,
            "branch": branch,
            "timer": timer,
            "external": external,
            "timeout_at": timeout_at,
            "created_at": created_at,
            "settled_at": settled_at,
        }
        await self._tx.execute(
            """
            INSERT INTO promises
              (id, state, param_headers, param_data, value_headers, value_data, tags,
               target, branch, timer, external, timeout_at, created_at, settled_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            list(row.values()),
        )
        return row

    async def _get_task(self, task_id: str) -> TursoRow | None:
        rows = await self._tx.execute("SELECT * FROM tasks WHERE id = ?", [task_id])
        return rows[0] if rows else None

    async def _set_task(
        self,
        task_id: str,
        state: str,
        version: int,
        pid: str | None,
        ttl: int | None,
    ) -> TursoRow:
        await self._tx.execute(
            """
            INSERT INTO tasks (id, state, version, pid, ttl) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET state = excluded.state, version = excluded.version,
              pid = excluded.pid, ttl = excluded.ttl
            """,
            [task_id, state, version, pid, ttl],
        )
        return {
            "id": task_id,
            "state": state,
            "version": version,
            "pid": pid,
            "ttl": ttl,
        }

    async def _get_callbacks(self, awaited_id: str) -> list[str]:
        rows = await self._tx.execute(
            "SELECT awaiter_id FROM callbacks WHERE awaited_id = ? ORDER BY seq ASC",
            [awaited_id],
        )
        return [row["awaiter_id"] for row in rows]

    async def _add_callback(self, awaited_id: str, awaiter_id: str) -> None:
        await self._tx.execute(
            """
            INSERT INTO callbacks (awaited_id, awaiter_id, seq)
            VALUES (?, ?, (SELECT COALESCE(MAX(seq), 0) + 1 FROM callbacks))
            ON CONFLICT (awaited_id, awaiter_id) DO NOTHING
            """,
            [awaited_id, awaiter_id],
        )

    async def _get_listeners(self, promise_id: str) -> list[str]:
        rows = await self._tx.execute(
            "SELECT address FROM listeners WHERE promise_id = ? ORDER BY seq ASC",
            [promise_id],
        )
        return [row["address"] for row in rows]

    async def _add_listener(self, promise_id: str, address: str) -> None:
        await self._tx.execute(
            """
            INSERT INTO listeners (promise_id, address, seq)
            VALUES (?, ?, (SELECT COALESCE(MAX(seq), 0) + 1 FROM listeners))
            ON CONFLICT (promise_id, address) DO NOTHING
            """,
            [promise_id, address],
        )

    async def _count_resumes(self, task_id: str) -> int:
        rows = await self._tx.execute(
            "SELECT COUNT(*) AS n FROM resumes WHERE task_id = ?", [task_id]
        )
        return int(rows[0]["n"]) if rows else 0

    async def _add_resume(self, task_id: str, awaited_id: str) -> None:
        await self._tx.execute(
            """
            INSERT INTO resumes (task_id, awaited_id, seq)
            VALUES (?, ?, (SELECT COALESCE(MAX(seq), 0) + 1 FROM resumes))
            ON CONFLICT (task_id, awaited_id) DO NOTHING
            """,
            [task_id, awaited_id],
        )

    async def _clear_resumes(self, task_id: str) -> None:
        await self._tx.execute("DELETE FROM resumes WHERE task_id = ?", [task_id])

    async def _set_promise_timeout(self, promise_id: str, timeout_at: int) -> None:
        await self._tx.execute(
            """
            INSERT INTO promise_timeouts (id, timeout_at) VALUES (?, ?)
            ON CONFLICT (id) DO UPDATE SET timeout_at = excluded.timeout_at
            """,
            [promise_id, timeout_at],
        )

    async def _del_promise_timeout(self, promise_id: str) -> None:
        await self._tx.execute(
            "DELETE FROM promise_timeouts WHERE id = ?", [promise_id]
        )

    async def _get_task_timeout(self, task_id: str, kind: int) -> int | None:
        rows = await self._tx.execute(
            "SELECT timeout_at FROM task_timeouts WHERE id = ? AND kind = ?",
            [task_id, kind],
        )
        return int(rows[0]["timeout_at"]) if rows else None

    async def _set_task_timeout(self, task_id: str, kind: int, timeout_at: int) -> None:
        await self._tx.execute(
            """
            INSERT INTO task_timeouts (id, kind, timeout_at) VALUES (?, ?, ?)
            ON CONFLICT (id, kind) DO UPDATE SET timeout_at = excluded.timeout_at
            """,
            [task_id, kind, timeout_at],
        )

    async def _del_task_timeout(self, task_id: str) -> None:
        """Disarm every timer on a task, matching the spec's ``delTaskTimeout``."""
        await self._tx.execute("DELETE FROM task_timeouts WHERE id = ?", [task_id])

    async def _set_message(self, address: str, msg: dict[str, Any]) -> None:
        """Append to the outbox, collapsing on the message key.

        A newer execute for a task supersedes the older one, so a receiver never
        sees a stale version.
        """
        await self._tx.execute(
            """
            INSERT INTO outbox (msg_key, address, payload, created_at) VALUES (?, ?, ?, ?)
            ON CONFLICT (msg_key) DO UPDATE SET address = excluded.address,
              payload = excluded.payload, created_at = excluded.created_at
            """,
            [_message_key(address, msg), address, json.dumps(msg), self._now],
        )


def _execute(task_id: str, version: int) -> dict[str, Any]:
    return {
        "kind": "execute",
        "head": {},
        "data": {"task": {"id": task_id, "version": version}},
    }


def _unblock(promise: dict[str, Any]) -> dict[str, Any]:
    return {"kind": "unblock", "head": {}, "data": {"promise": promise}}


# =============================================================================
# SCHEDULES
# =============================================================================
#
# Schedules live in the tenant database: a schedule's promise id is a template,
# so the promises it fires may land in any number of origins, and the schedule
# itself belongs to none of them.


def to_schedule_record(row: TursoRow) -> dict[str, Any]:
    record: dict[str, Any] = {
        "id": row["id"],
        "cron": row["cron"],
        "promiseId": row["promise_id"],
        "promiseTimeout": row["promise_timeout"],
        "promiseParam": _decode_value(
            row["promise_param_headers"], row["promise_param_data"]
        ),
        "promiseTags": _parse_tags(row["promise_tags"]),
        "createdAt": row["created_at"],
        "nextRunAt": row["next_run_at"],
    }
    if row["last_run_at"] is not None:
        record["lastRunAt"] = row["last_run_at"]
    return record


def expand_promise_id(template: str, schedule_id: str, timestamp: int) -> str:
    """Expand a schedule's promise-id template against one occurrence."""
    return template.replace("{{.id}}", schedule_id).replace(
        "{{.timestamp}}", str(timestamp)
    )


class ScheduleStore:
    """Schedule storage, backed by the tenant database."""

    def __init__(self, tx: TursoExecutor, now: int) -> None:
        self._tx = tx
        self._now = now

    async def get(self, schedule_id: str) -> Outcome:
        row = await self._row(schedule_id)
        if row is None:
            return Outcome("schedule.get", 404, "Schedule not found")
        return Outcome("schedule.get", 200, {"schedule": to_schedule_record(row)})

    async def create(self, data: dict[str, Any]) -> Outcome:
        kind = "schedule.create"
        tags: dict[str, str] = data.get("promiseTags") or {}
        # Every fired promise needs a routing target, so a schedule without one
        # is rejected up front rather than firing promises nobody will ever run.
        if not tags.get("resonate:target"):
            return Outcome(kind, 400, "promiseTags must include a resonate:target tag")

        existing = await self._row(data["id"])
        if existing is not None:
            return Outcome(kind, 200, {"schedule": to_schedule_record(existing)})

        try:
            next_run_at = next_cron(data["cron"], self._now)
        except CronError:
            return Outcome(kind, 400, "Invalid cron expression")

        headers, blob = _encode_value(data.get("promiseParam"))
        row: TursoRow = {
            "id": data["id"],
            "cron": data["cron"],
            "promise_id": data["promiseId"],
            "promise_timeout": data["promiseTimeout"],
            "promise_param_headers": headers,
            "promise_param_data": blob,
            "promise_tags": json.dumps(tags),
            "created_at": self._now,
            "next_run_at": next_run_at,
            "last_run_at": None,
        }
        await self._tx.execute(
            """
            INSERT INTO schedules (id, cron, promise_id, promise_timeout, promise_param_headers,
              promise_param_data, promise_tags, created_at, next_run_at, last_run_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            list(row.values()),
        )
        return Outcome(kind, 200, {"schedule": to_schedule_record(row)})

    async def delete(self, schedule_id: str) -> Outcome:
        row = await self._row(schedule_id)
        if row is None:
            return Outcome("schedule.delete", 404, "Schedule not found")
        await self._tx.execute("DELETE FROM schedules WHERE id = ?", [schedule_id])
        return Outcome("schedule.delete", 200, {})

    async def search(
        self, tags: dict[str, str] | None, limit: int | None, cursor: str | None
    ) -> Outcome:
        size = min(max(limit or 100, 1), 1000)
        clauses: list[str] = []
        args: list[Any] = []
        if cursor is not None:
            clauses.append("id > ?")
            args.append(cursor)
        tag_clauses, tag_args = _tag_filter(tags, "promise_tags")
        clauses.extend(tag_clauses)
        args.extend(tag_args)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = await self._tx.execute(
            f"SELECT * FROM schedules {where} ORDER BY id ASC LIMIT ?",  # noqa: S608
            [*args, size],
        )
        data: dict[str, Any] = {"schedules": [to_schedule_record(row) for row in rows]}
        if len(rows) == size:
            data["cursor"] = rows[-1]["id"]
        return Outcome("schedule.search", 200, data)

    async def due(self) -> list[TursoRow]:
        """Schedules whose next run is due at or before ``now``."""
        return await self._tx.execute(
            "SELECT * FROM schedules WHERE next_run_at <= ? ORDER BY id ASC",
            [self._now],
        )

    async def advance(
        self, schedule_id: str, last_run_at: int, next_run_at: int
    ) -> None:
        await self._tx.execute(
            "UPDATE schedules SET last_run_at = ?, next_run_at = ? WHERE id = ?",
            [last_run_at, next_run_at, schedule_id],
        )

    async def _row(self, schedule_id: str) -> TursoRow | None:
        rows = await self._tx.execute(
            "SELECT * FROM schedules WHERE id = ?", [schedule_id]
        )
        return rows[0] if rows else None


# =============================================================================
# VALIDATION
# =============================================================================


def validate_create(
    promise_id: str, timeout_at: Any, tags: dict[str, str]
) -> str | None:
    """Create-time validation, mirroring the Resonate Server.

    The prefix rules are not cosmetic here: ``resonate:origin`` is what selects
    the database, so an id that does not sit under its own origin would be
    written to a database that cannot be found again from the id.
    """
    if not promise_id:
        return "Promise ID is required"
    if "\0" in promise_id:
        return "Promise ID must not contain null bytes"
    if (
        not isinstance(timeout_at, int)
        or isinstance(timeout_at, bool)
        or timeout_at < 0
    ):
        return "TimeoutAt must be a non-negative integer"

    origin = tags.get("resonate:origin")
    if origin is not None:
        if "." in origin:
            return "resonate:origin must not contain '.'"
        if promise_id != origin and not promise_id.startswith(f"{origin}."):
            return "Promise ID must be prefixed by resonate:origin"

    for tag, label in (
        ("resonate:branch", "resonate:branch"),
        ("resonate:parent", "resonate:parent"),
    ):
        prefix = tags.get(tag)
        if (
            prefix is not None
            and promise_id != prefix
            and not promise_id.startswith(f"{prefix}.")
        ):
            return f"Promise ID must be prefixed by {label}"

    if "." in (tags.get("resonate:prefix") or ""):
        return "resonate:prefix must not contain '.'"

    raw_delay = tags.get("resonate:delay")
    if raw_delay is not None:
        delay = _parse_delay(raw_delay)
        if delay is None:
            return "resonate:delay must be a non-negative integer"
        if delay >= timeout_at:
            return "resonate:delay must be less than timeoutAt"
        if "resonate:target" not in tags:
            return "resonate:delay requires a resonate:target tag"

    return None


#: Re-exported for the network's routing table.
PromiseState = Literal[
    "pending", "resolved", "rejected", "rejected_canceled", "rejected_timedout"
]
