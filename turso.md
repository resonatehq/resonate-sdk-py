# Decentralized Resonate on Turso

`TursoNetwork` is a `Network` with no server behind it.

## Production readiness (reviewed 2026-08-08)

An adversarial review — seven independent dimensions over both SDKs, every
finding re-verified against the code by a refuter before it counted — preceded
this release. Twelve findings were confirmed (two critical, among them an
AB-BA deadlock in this SDK's connection eviction that could wedge a whole
node); all are fixed on this branch, with regression tests for the criticals.
Status by concern:

| Concern | Status |
|---|---|
| Single-node correctness (full suites) | ✅ tested |
| Sharded fleet — one owner per workflow | ✅ measured |
| Convergence through Turso Cloud | ✅ measured (with the TS SDK's identical arrangement) |
| Turso Cloud provisioning (no auto-create; API create ~384ms) | ✅ measured |
| Boundary uploads (`push_on="boundary"`) | ✅ implemented, reviewed, tested |
| Origin routing (`resonate:origin` header normalized and validated) | ✅ fixed under review, regression-tested |
| **Cross-node CAS without sharding** | ⚠️ **answered, not yet shipped** — a guard trigger makes the push a real CAS (measured 20/20 single-winner unsharded in the TS repo); needs server-side provisioning, a replica-reset path, and a push per write. Static sharding remains the shipped recommendation. |
| Detached (re-rooted) lineages | ✖ unsupported by design (see below) |

Deployment checklist: shard the fleet (`shard`, route with `owner_of`) — one
writer per workflow is the correctness boundary until the CAS question closes;
create databases before first connect (Turso Cloud does not auto-create);
configure Python logging (this module logs failures through the standard
`logging` module, logger name `resonate.network.turso`); rotate group tokens
(`auth_token` accepts a callable).

Every other network in this SDK is a transport: it carries a request to a
Resonate Server and carries the response back. This one is not. There is no
server; the SDK *is* the server, and the durable state lives in Turso databases
the SDK reads, writes, and syncs directly.

```python
from resonate import Resonate
from resonate.network.turso import TursoNetwork, TursoSyncDriver

resonate = Resonate.remote(
    network=TursoNetwork(
        TursoSyncDriver(
            "/var/lib/resonate",
            # A Turso Cloud database lives at `<name>-<org>.<region>.turso.io`,
            # so the flat prefix form cannot address it -- pass a callable.
            lambda name: f"libsql://{name}-acme.aws-us-west-2.turso.io",
            auth_token=os.environ["TURSO_AUTH_TOKEN"],
        ),
        prefix="acme-",                    # local database is `acme-<origin>`
        timeout_database="timeouts",       # tenant-global: `acme-timeouts`
        group="default",
    ),
)
```

Install the optional client with `pip install resonate-sdk[turso]`.

## Turso Cloud provisioning (measured)

Measured against a real Turso Cloud account (`aws-us-west-2`, free plan,
August 2026), with the TypeScript SDK's identical driver arrangement:

* **Databases are not auto-created.** A sync connect to a name that does not
  exist fails with `status=404, body=Host not found`. Every origin database —
  and the tenant database — must be created first, via the platform API
  (`POST /v1/organizations/<org>/databases`) or the `turso` CLI.
* **Creation is cheap and immediate:** 359–558ms per database (median 384ms
  over 16 creates), and a fresh database accepted a sync connect 528ms after
  the create call, first attempt. A create-if-missing step can wrap the
  driver, at the price of one platform round trip on a workflow's first touch.
* **Plan limits are the real constraint:** the free plan caps an organization
  at 100 databases; paid plans advertise unlimited databases and meter
  storage, rows, and sync traffic instead — and sync traffic (every replica
  bootstrap, push, and pull) is the axis this design leans on.

Fallbacks if per-workflow creation is unacceptable: a pool of N pre-created
databases with origins hashed into them (`hash_origin` is exported), or one
database per tenant with an `origin` column — each giving up some of the
isolation that makes per-workflow CAS cheap.

## One database per workflow

Every promise id is prefixed by its **origin** — the root workflow's id,
everything before the first `.`. `TursoNetwork` gives each origin its own
database, named `<prefix><origin>`.

That is not an arbitrary sharding key. The protocol already guarantees a
callback never crosses an origin (`promise.register_callback` refuses one, and
the Resonate Server enforces the same rule), so every request touches exactly
one workflow's state — which makes every request a single-database
transaction. A workflow is a unit of consistency, and here it becomes a unit of
storage too. That is what makes each database small enough for a process to
hold as an embedded replica.

An origin database holds everything about one workflow: its promises, the
callbacks and listeners registered against them, its tasks, and the armed
timeouts.

## One tenant-global timeout database

`<prefix><timeout_database>` (default `<prefix>timeouts`) holds what no single
workflow owns:

| Table | Purpose |
|-------|---------|
| `timeouts` | `(origin, id, kind, timeout_at)` — the index of armed timers across every origin, so a sweeper can find due work without opening every workflow |
| `schedules` | schedules, which are tenant-scoped by definition — a schedule's promise id is a template, so the promises it fires belong to many origins |

The timeout table is a **mirror, never the authority**. The origin database
holds the armed timers; `TursoStore.flush` republishes the origin's slice after
every commit. Nothing trusts the index: every timeout transition re-reads its
own armed time from the origin database and refuses an early firing (the spec's
NOT BEFORE rule), so a stale entry costs a wasted database open and nothing
else. A *missing* entry only delays work — the origin still holds the truth,
and the next flush restores it.

## Messages are not stored

When a transition emits an `execute` or `unblock`, the message is handed to the
local Resonate client as soon as the transaction commits. No outbox, no queue,
no routing table. Delivery is deferred by one turn of the event loop
(`loop.call_soon`) so the response reaches the caller first and a subscriber
cannot re-enter the call that produced its message.

Reaching a **different** process therefore goes through time, not through a
queue. A dispatched task carries a durable retry timer; if nobody claims it,
the timer comes due, and whichever process sweeps the tenant timeout index
re-emits the execute and delivers it to its own client. Recovery is the timer —
which is exactly why the timeout database is the one thing shared.

Three consequences worth knowing:

* **`resonate:target` is advisory.** The address is recorded on the promise and
  echoed in the message, but nothing routes by it: a message goes to whoever
  did the work. In a homogeneous fleet — every process registering the same
  functions — that is what you want. In a heterogeneous one, where only some
  processes can run a given function, this network will not deliver work to the
  right group.

* **A result computed on one node does not reach the node waiting for it.**
  This is the sharpest edge of in-process delivery, and it is measured, not
  theoretical. `resonate.run(...)` waits by registering a listener carrying the
  caller's *unicast* address. If the workflow is finished by a different node —
  which happens whenever a timer resumed it elsewhere — that node emits the
  `unblock` and delivers it to **itself**. The waiting node never sees it and
  falls back on its own slow path.

  Measured with two nodes and one workflow that migrates: the work is done at
  `+165ms`, and the caller learns about it at `+60004ms`. One node alone: 812ms
  for four workflows. Two nodes: 60 seconds. Correctness is unaffected — the
  results are right, nothing runs twice — but the latency is not usable.

  `execute` does not have this problem because a task's retry timer re-emits it
  and any node can pick it up. `unblock` has no equivalent: nothing re-emits it,
  and nothing routes it. Closing this needs either address-routed delivery (a
  shared queue again) or a waiter that polls the promise it is blocked on
  instead of waiting to be told.

  **Static sharding avoids it** — see below. If every workflow has one owner and
  callers are routed to that owner, the node that finishes a workflow is the node
  that was waiting on it, and the message never has to cross a process.

* **First dispatch is local.** Creating a targeted promise hands the execute to
  the creating process. If that process is a client that cannot run the
  function, the task simply stays pending until its retry timer hands it to a
  sweeper that can.

## Drivers

A driver maps a logical database name to physical storage. Two ship:

| Driver | Use |
|--------|-----|
| `TursoSyncDriver` | embedded replica per workflow, syncing with Turso Cloud — the decentralized arrangement |
| `TursoLocalDriver` | a local directory of databases, or `":memory:"` |

Both use the optional `pyturso` package, imported lazily. Implement the
`TursoDriver` protocol for anything else — it has one method, `open(name)`.

`TursoLocalDriver` takes an **exclusive file lock** on open: a second process
opening the same file fails outright. It is a single-process driver — right for
a node's own origin databases, wrong for anything the fleet shares.

Origins and the tenant database need not use the same driver. `timeout_driver`
opens the tenant database when it lives elsewhere:

```python
TursoNetwork(
    TursoLocalDriver("/var/lib/resonate/node-0"),      # mine alone
    timeout_driver=TursoSyncDriver(..., "libsql://acme-"),  # shared by the fleet
)
```

## Sharding a fleet

`shard=(index, count)` gives a node a fixed slice of the workflows:

```python
TursoNetwork(driver, shard=(0, 2))
```

The node then sweeps only timers whose origin it owns —
`hash_origin(origin) % count == index` — filtered in SQL against the shared
index, not in memory after reading everyone's.

This turns "any node may pick up any workflow" into "every workflow has exactly
one owner", which buys three things: a workflow stops migrating mid-flight, one
owner means one writer (which is what the CAS fences want), and the `unblock`
problem above stops mattering, because the node that finishes a workflow is the
node that was waiting on it.

**The count is fleet state, not node config.** Ownership is
`hash_origin(origin) % count`, so a node using a different `count` from its
peers is silently destructive in both directions: origins no node claims keep
their timers due forever (parked workflows never resume, and nothing logs), and
origins two nodes claim recreate the unsound multi-writer arrangement. A sharded
node therefore stamps its count in the tenant database at startup and raises
`ShardCountMismatchError` if the fleet records a different one.

Resharding is consequently not a rolling operation. Stop the fleet, relocate or
share the origin databases if they are node-local (a resize moves workflows to
nodes that hold no copy of them), start one node with `reshard=True` to claim
the new count, then start the rest normally.

The caller must route requests to the owning node using the same function —
`owner_of(origin_of(promise_id), count)`, exported for exactly this. The hash
lives in the SDK rather than in the caller so that routing and sweeping cannot
disagree; the TypeScript SDK's `hashOrigin` computes the same values (it is
FNV-1a over UTF-16 code units in both), and both suites pin the same vector. A
mixed-language fleet shards identically.

## Running more than one node

Nodes do not share a disk for their origins — each has its own directory. What
they must share is the timer index, since that is the one place a node learns
that work exists at all. That is the arrangement `TursoSyncDriver` is for.

**Convergence through a real remote works — measured** (with the TypeScript
SDK's identical arrangement against Turso Cloud, `aws-us-west-2`): a committed
write is visible to another replica's pull in ~190–220ms median (p90
270–400ms, worst observed 1.1s); a workflow parked on a 4s durable sleep by a
node that then stopped was discovered through the tenant index, resumed, and
completed by the surviving node with ~2s overhead over the sleep; workflows
abandoned by killed processes were recovered by later unrelated nodes; and at
quiescence the tenant index exactly matched the union of origin databases —
no missing entries, no stale entries.

**Protocol correctness under contention holds.** Three nodes, each with its own
network, racing on the same workflows with timer-driven migration between them:
all workflows completed with correct results, work spread across all three
nodes, **zero** durable steps executed by more than one node, and **zero**
disagreement between nodes on any result. The version fences do their job.

**Unsharded, liveness does not.** See the `unblock` bullet above: a workflow
finished by one node does not notify the node waiting for it, so a fleet pays 60
seconds where a single node pays milliseconds. Static sharding sidesteps this
rather than fixing it — the owner is the waiter — so a fleet that rebalances or
steals work still meets it.

Two more things a fleet meets:

* **The tenant database is the fleet's one global write bottleneck.** Every
  origin publishes its timers there. `TursoStore.flush` skips the write when an
  origin's timers have not moved, which removes most of the traffic, but the
  bottleneck is structural.

* **Compare-and-swap does not survive replication — measured, and worse than
  feared.** Every fenced action (`task.acquire` and friends) is a
  read-compare-write inside a single `BEGIN IMMEDIATE` transaction — a genuine
  CAS against the one database that applies it, and no CAS at all across
  nodes. Measured with the TypeScript SDK against Turso Cloud: two nodes
  racing `task.acquire` for the same `{id, version: 0}` through the same
  remote both won **50 times out of 50**. With each CAS applied to its own
  replica there is nothing to contend with, so a simultaneous race *always*
  double-wins.

  Remote writes are not an escape hatch either. `pyturso` does not expose the
  option, and the TypeScript client's `remoteWrites: true` was measured
  broken: still 50/50 double wins, 11–13s per acquire, and an independent
  replica read the remote as untouched afterwards — the flag neither
  serializes the transaction remotely nor lands its writes
  (`@tursodatabase/sync` 0.7.2). The earlier revision of this document held
  the TypeScript flag up as the fix; that was wrong.

  **A guard trigger makes the push itself the compare-and-swap — measured.**
  A version row whose trigger rejects any update that is not exactly +1:

  ```sql
  CREATE TABLE cas_table (key TEXT PRIMARY KEY, value);
  CREATE TRIGGER cas_table_conflict
  BEFORE UPDATE ON cas_table
  WHEN CAST(NEW.value AS INTEGER) != CAST(OLD.value AS INTEGER) + 1
  BEGIN SELECT RAISE(ROLLBACK, 'unexpected existing row state'); END;
  ```

  A writer bumps the row inside its transaction and pushes; the remote applies
  pushes one at a time, so a node that staked a version another already landed
  trips the trigger and its push is rejected with `SQLITE_CONSTRAINT`, whole
  and atomically. Two unsharded nodes racing `task.acquire` then produce
  exactly one winner, 20 times out of 20 (measured in the TypeScript repo,
  `experiments/exp-e.ts`; the same arrangement without the guard double-wins
  50/50). Caveats: the trigger must be installed **server-side** at
  provisioning (DDL pushed from a replica does not register one on the
  remote), a rejected push wedges the replica until its local files are
  deleted and re-bootstrapped (~420ms; there is no revert API), and the fence
  costs a remote round trip per write, so it is in tension with
  `push_on="boundary"`.

  Two sound arrangements, then, trading opposite ways: **static sharding**
  keeps writes at local-disk latency and fixes one owner per workflow; **a
  guard trigger** lets any node write any workflow at a round trip per write.
  Sharding stays the default; the guard is what would make work-stealing and
  unsharded fleets possible.

  **A sound CAS does exist one protocol over — measured.** The same Turso
  Cloud database also serves Hrana, the server-side transaction protocol, and
  a `BEGIN IMMEDIATE` read-compare-write raced from two clients there yields
  exactly one winner, 20/20, with the loser observing the winner's write
  (~150–190ms per winning acquire; measured with `@libsql/client` and
  `@tursodatabase/serverless` in the TypeScript repo, `experiments/exp-c.ts`).
  A driver that runs origin transactions server-side — e.g. over
  `libsql-client` for Python — would make a fleet sound in any topology, no
  sharding required, at ~150ms per transition; sharding becomes a latency
  optimization rather than a correctness requirement.

## What the schema follows

The schema and transitions follow
[`resonatehq/resonate-specification`](https://github.com/resonatehq/resonate-specification)
(`spec/03-concrete`), not the SQL in `resonatehq/resonate`'s
`persistence_sqlite.rs`. The two have diverged; where they do, the spec wins.
The differences that show up here:

* **Projection, not mutation.** A pending promise past its deadline is
  *logically* settled, and nothing mutates state to discover that. Every guard
  and every response consults the projected view; only the timeout transition
  converges the stored row. This is why a read is side-effect free — and so why
  a replica can serve one without pushing.

* **`external` gates durable timeouts**, not `resonate:target`. A promise is
  external when it is tagged `resonate:external`, carries a `resonate:target`,
  or is a timer. Only external promises may be awaited (`register_callback` and
  `register_listener` answer `422` otherwise) and only they arm a durable
  timeout.

* **`resonate:delay` defers the first dispatch.** A targeted promise created
  with a delay still ahead of `now` arms its retry timer at the delay and emits
  no execute message; the timer's first firing is the first dispatch.

* **Timeout always wins.** Retry and lease timers consult the projected promise
  before acting, so a logically dead task is neither redispatched nor returned
  to circulation.

* **Deferred resumes.** Settlement records a resume obligation per awaiter
  rather than resuming inline, and the drain runs in the same transaction — a
  suspended task has no armed timer, so a lost resume would strand it forever.

Turso does not support generated columns without an experimental flag, so the
tag-derived columns (`target`, `branch`, `timer`, `external`) are written by the
SDK rather than computed by the engine. This schema is therefore **not** the
Resonate Server's SQLite schema and the two are not interchangeable.

## Schedules and cron

Cron expressions are evaluated in **UTC** (`resonate/network/turso/cron.py`), a
self-contained five-field evaluator supporting `*`, lists, ranges, and steps.
A schedule is fired by whichever process sweeps it first and those processes
need not share a timezone, so a local-time interpretation would make the same
schedule fire at different instants depending on which machine woke up. The
TypeScript SDK pins `tz: "UTC"` for the same reason, so a mixed fleet agrees.

## What is not supported

* **Tenant-wide `promise.search` / `task.search`** answer `501`. Promises are
  partitioned across one database per origin, so "every promise" is not a query
  any single database can answer. Narrow the search with a `resonate:origin`
  tag (or the `resonate:origin` request header) and it is served from that
  workflow's database.

* **Tenant-wide `debug.snap`** answers `501` for the same reason; set the
  `resonate:origin` header to snapshot one workflow.

* **`http://` listener addresses.** An `unblock` for one is emitted and handed
  to the local client like any other message, but nothing makes the HTTP call —
  there is no server to make it. Treat these as unsupported.

* **Detached (re-rooted) lineages.** A detached child's `resonate:origin` tag
  is its own dotted id, re-rooting the lineage. The origin partition cannot
  represent a root whose id contains `.` — database selection is
  `origin_of(id)`, which would split the detached workflow and its children
  across databases — so `promise.create` refuses the tag with a 400 naming
  this limitation. Start detached work as a genuine root instead (a fresh
  un-dotted id).

## When the cloud is written: `push_on`

A task lease gives a workflow one writer for the span of a tenure, which makes
per-request uploads mostly wasted motion: nobody else may read the workflow's
intermediate steps before the tenure ends. `push_on` on `TursoNetwork` makes
that explicit:

* `"boundary"` (default) — writes stay on the local replica until a moment
  another process could ever need to read from: the task fulfills, suspends,
  releases, or halts; work becomes visible to the fleet (`task.create`, a root
  or targeted `promise.create`); or the root promise settles. Sweep-driven
  recovery transitions always push. The trade is recovery granularity: a node
  that crashes mid-tenure is recovered from its last boundary, and the durable
  steps since then are re-executed — at-least-once per tenure segment instead
  of per step.

* `"request"` — the old behavior: push after every committed write. Recovery
  loses at most one request; the cloud sees every durable step as it lands.

A timer index entry may briefly advertise state the remote cannot serve yet
(the entry is published before the boundary push). That is safe by the same
rule that makes every index entry safe: a consumer re-validates against the
origin database and treats "not armed yet" like any stale entry — it costs a
wasted open per sweep until the boundary push lands, and nothing else.

## Operational gotchas (all paid for)

* **`close()` on a replica can hang** while a pull is in flight, which makes
  `network.stop()` hang with it. A supervisor with a kill timeout will SIGKILL
  the process on every deploy; race `stop()` against a timeout if you need a
  prompt exit. The network marks itself stopped and cancels its tick before
  closing connections, so the node is already inert when the hang happens.

* **A swept origin can lose its connection mid-fire** (`turso close failed for
  origin ...`, or a sweep warning) when the store evicts it. The sweep retries
  next tick and the fleet self-heals; the warning is noise unless it repeats
  for the same origin indefinitely.

* **The store does not reopen after `stop()`.** Requests arriving after a stop
  raise `StoreClosedError` rather than silently reopening databases nothing
  will ever close. Build a new `TursoNetwork` to restart.

* **Failures are reported through the standard `logging` module**, logger name
  `resonate.network.turso`. A fleet whose sweeper is broken looks exactly like
  a fleet with no work to do, so configure logging at WARNING or below before
  concluding that a quiet node is an idle one.

## Tests

```shell
uv run pytest tests/test_turso.py
```
