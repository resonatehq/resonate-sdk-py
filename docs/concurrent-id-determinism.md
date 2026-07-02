# Nondeterministic durable ids under concurrency

**Status:** FIXED — via task-lineage-scoped id generation (`src/resonate/lineage.py`),
a variant of the "implicit task-lineage namespacing" idea §5 rejected, made viable
by intercepting `create_task` with a loop task factory (`loop.set_task_factory`).
Every asyncio task gets a lineage path (parent path + spawn index) assigned
*synchronously at creation time* — creation order within a task is program order,
hence replay-stable — and `Context._next_id` keys its seq counter per path
relative to the state's anchor task: `{id}.t{i}...t{j}.{seq}`. Sequential code
keeps flat `{id}.{seq}` ids; bare `asyncio.create_task`/`gather`/`TaskGroup` all
work with no new API. The spawn counter is re-armed at the user-code boundary
(`invoke_with_retry`) so SDK/network-internal spawns can't skew user indices.
The analysis below is kept for the record.
**Constraint:** the fix must **not** require users to define promise ids by hand.
**Scope:** `ctx.run` / `ctx.rpc` / `ctx.sleep` / `ctx.promise` child-id generation
inside a single workflow execution.

---

## 1. TL;DR

A workflow that runs two durable chains concurrently returns the **wrong
values after replay** — deterministically wrong, every run:

```
expected ['s2:slow-result', 'f2:fast-result']
got      ['f2:fast-result', 's2:slow-result']
```

The durable child id is minted from a **shared per-execution sequence counter**
(`Context._next_id`, `src/resonate/context.py:360`) advanced at `ctx.run` **call
time**. Under concurrency the *order in which the two chains reach their calls*
depends on which sibling future settled first. That order is set by wall-clock
on the live pass and by instant promise recovery on the replay pass — the two
disagree, so the same logical operation gets a **different id** on replay and
recovers **another operation's stored value**.

This is not caused by `asyncio.gather`; it reproduces with plain
`asyncio.create_task`. The creation-chain (`src/resonate/chain.py`) cannot fix
it, because ids already follow call order and the defect is that *call order
itself* is not replay-stable.

Because explicit user ids are off the table, the viable fixes are:

- **B — structured concurrency combinator** (recommended): the SDK owns the
  fan-out and derives each branch's id namespace from its **positional index**,
  which is replay-stable.
- **C — deterministic replay / order-preserving resolution**: record sibling
  completion order and reproduce it on replay so call order is stable. Keeps
  bare `asyncio` concurrency working, but is a much larger, server-dependent
  change.

---

## 2. Reproduction

```python
async def slow(ctx):  await asyncio.sleep(0.1); return "slow-result"  # loses the race
async def fast(ctx):  return "fast-result"                            # wins the race
async def mark(ctx, tag, value): return f"{tag}:{value}"

async def wf(ctx):
    timer = ctx.sleep(timedelta(seconds=0.2))   # pending timer -> forces a suspend

    async def chain_a():
        a = await ctx.run(slow)
        return await ctx.run(mark, "s2", a)

    async def chain_b():
        b = await ctx.run(fast)
        return await ctx.run(mark, "f2", b)

    task_a = asyncio.create_task(chain_a())      # no gather -- create_task
    task_b = asyncio.create_task(chain_b())
    r1, r2 = await task_a, await task_b
    await timer                                   # suspend, resume, REPLAY from top
    return [r1, r2]
```

Instrumenting each `mark` call to print the durable id it receives (via
`await fut.id()`) prints the id **twice** — once per pass — and the ids swap:

```
live pass    [chain_b] mark('f2', 'fast-result') -> id=…4     # fast wins -> chain_b's mark is 4th call
             [chain_a] mark('s2', 'slow-result') -> id=…5     # slow +0.1s -> chain_a's mark is 5th call
replay pass  [chain_a] mark('s2', 'slow-result') -> id=…4     # no sleep -> chain_a's mark is 4th call
             [chain_b] mark('f2', 'fast-result') -> id=…5     # chain_b's mark is 5th call
```

`chain_a`'s `mark('s2', …)` is id `…5` on the live pass and `…4` on replay. Id
`…4` was durably resolved to `"f2:fast-result"` on the live pass, so on replay
`create_promise` hits the already-settled short-circuit
(`src/resonate/context.py:600`) and hands `chain_a` back `"f2:fast-result"` —
the value that belongs to `chain_b`. Symmetric for `chain_b`. Hence the swap.

It reproduces every run because replay ordering is itself deterministic; this is
a **correctness** bug, not a flake.

---

## 3. Root cause

### 3.1 The id scheme

```python
# src/resonate/context.py:360
def _next_id(self) -> str:
    self._state.seq += 1
    return f"{self._state.id}.{self._state.seq}"
```

Every durable op on a context mints its child id from a single monotonic `seq`
counter living on the **shared** `_State` (`src/resonate/context.py:74`). One
workflow body = one `_State` = one `seq`, no matter how many concurrent
coroutines call into it.

The id a call gets is therefore a pure function of **the position of that call
in the global call sequence** of the execution.

### 3.2 The invariant this silently assumes

> A given logical operation must map to the same promise id on every replay.

With `{parent}.{seq}` this holds **iff the sequence of `ctx.run` calls is
identical on every replay.** For sequential code it always is: statement order
*is* call order, and statement order does not change between runs.

### 3.3 Why concurrency breaks the assumption

Under concurrency the sequence of calls is no longer fixed by the source; it is
fixed by **which coroutine reaches its next `ctx.run` first**, which is gated on
**which sibling future settled first**:

| | first-stage settle order | second-stage (`mark`) call order | ids |
|---|---|---|---|
| **live pass** | `fast` (0s) before `slow` (0.1s) | `chain_b.mark`, then `chain_a.mark` | `…4=f2`, `…5=s2` |
| **replay pass** | both recovered instantly; `slow`'s task was created first | `chain_a.mark`, then `chain_b.mark` | `…4=s2`, `…5=f2` |

The first stage is stable (`slow=…2`, `fast=…3` — those calls are reached in a
fixed order). Only the **second stage flips**, because it is *data-dependent on
a racing first stage*. Live order is decided by wall-clock; replay order by the
event loop draining instantly-recovered promises. They disagree → the id ↔
operation mapping is not stable → operations recover each other's values.

### 3.4 Minimal conditions

All three are required; remove any one and the bug disappears:

1. **Concurrency** — ≥2 coroutines in flight sharing one `_State`/`seq`.
2. **A data-dependent follow-on** — a `ctx.run` whose *invocation* is gated on
   the completion of another concurrent `ctx.run`.
3. **Completion order that differs between passes** — live (wall-clock) vs
   replay (instant recovery) resolve siblings in different orders.

Corollaries:
- **Not a `gather` bug.** `asyncio.create_task`, `TaskGroup`, `asyncio.wait`,
  manual `ensure_future` all trigger it identically. (Verified with
  `create_task` — same swap.)
- **A single fan-out stage is safe.**
  `asyncio.gather(ctx.run(slow), ctx.run(fast))` mints `…1`/`…2` in call order
  every pass (no dependent follow-on).
- **Sequential chains are safe.** `await chain_a(); await chain_b()` fixes call
  order across passes.

---

## 4. Why the creation-chain (`chain.py`) does not fix it

A natural proposal: "we already serialize concurrent promise *creation* into
call order with `Chain`; do the same for id generation." It doesn't help, for a
concrete reason — **id generation already works the way the Chain does.**

In `run` both are acquired at the same synchronous call site, adjacently:

```python
# src/resonate/context.py:534-568 (elided)
link = self._state.chain.link()                       # chain position N
...
req = PromiseCreateReq(id=self._next_id(), ...)       # seq N
```

So chain-link order and `_next_id` order are **the same order** — both equal the
order in which control *reaches* the `ctx.run` statement. The Chain's job is to
make the background *create* network calls execute in that call order rather
than in whatever order the bg tasks get scheduled. It anchors **create-order to
call-order**. Ids are *already* anchored to call-order.

The two mechanisms are already consistent with each other. The defect is one
level upstream of both: **call-order itself flips between passes.** The flip is
decided when a sibling `await` completes — which happens *before* either
`link()` or `_next_id()` runs. The Chain lives on the **create side** (after the
call site); the divergence is on the **await/resume side** (before the call
site). Nothing sitting where the Chain sits can observe it, so nothing there can
correct it.

The only way to keep the id tied to call order *and* be correct is to make the
**await-completion order** deterministic on replay — that is not the Chain, that
is a deterministic scheduler (option C).

---

## 5. Design space

Requirement: a **replay-stable** map from logical operation → promise id that
survives concurrency, **without users writing ids**.

The candidate approaches, then the trade-offs.

### B. Structured concurrency combinator  ✅ recommended

The SDK owns the fan-out. Instead of raw `asyncio.gather`/`create_task` over
coroutines that close over the shared `ctx`, users hand branch functions to a
combinator that forks a **child context per branch**, keyed by the branch's
**positional index**:

```python
async def chain_a(ctx): a = await ctx.run(slow); return await ctx.run(mark, "s2", a)
async def chain_b(ctx): b = await ctx.run(fast); return await ctx.run(mark, "f2", b)

r1, r2 = await ctx.gather(chain_a, chain_b)     # positional -> deterministic
```

Id derivation (extends the existing `_child` mechanism,
`src/resonate/context.py:254`):

```
wf.1               ctx.sleep(timer)          # root seq, unchanged
wf.2               the gather group           # root seq slot for the combinator
wf.2.0             chain_a's sub-context       # branch index 0 (positional)
wf.2.0.1  slow
wf.2.0.2  mark("s2", …)
wf.2.1             chain_b's sub-context       # branch index 1 (positional)
wf.2.1.1  fast
wf.2.1.2  mark("f2", …)
```

Every id is positional. **Branch index comes from argument order** (fixed by the
source), and **within a branch seq is local and sequential** (each branch is
sequential code). Replay reproduces all of them exactly — the swap is
impossible.

- **Determinism:** total, by construction.
- **Ergonomics:** users switch from `asyncio.gather(chain_a(), chain_b())` to
  `ctx.gather(chain_a, chain_b)`; branch functions take a `ctx` parameter. Small
  syntactic cost.
- **Composability:** nests cleanly — a branch's own `ctx.gather` forks
  sub-sub-contexts (`wf.2.0.3.0…`). Loops are fine:
  `await ctx.gather(*[worker for _ in range(n)])` indexes by position.
- **Cross-deploy stability:** ids depend on structure (call sites + branch
  positions), not timing. Stable across restarts and redeploys as long as the
  structure is unchanged — same guarantee sequential code already has.
- **Cost:** medium. New public API (`gather`, likely a `spawn`/nursery form),
  child-context plumbing (mostly reuses `_child`), tree/`well_formed` updates
  for the group node, and docs.
- **Limitation:** it fixes concurrency *that goes through the combinator*. Raw
  `create_task`/`gather` over durable ops remains unsafe — see §6 on guarding
  against misuse.

### C. Deterministic replay (order-preserving resolution)

Make the **await-completion order** itself replay-stable, so call order — and
thus `_next_id` — reproduces without any API change. Record, durably, the order
in which sibling promises settled on the live pass; on replay, do not resolve a
recovered future until every future that settled before it (per the recorded
order) has resolved. Continuations then run in the recorded order, so the
second-stage calls are issued in the recorded order, so ids match.

- **Determinism:** total, if completion order is faithfully recorded and
  reproduced.
- **Ergonomics:** best possible — bare `asyncio.gather`/`create_task` "just
  works," no new API, existing user code becomes correct.
- **Cost:** high, and the blast radius is the runtime core. Requires:
  - a durable, monotonic **settlement-order** signal per sibling group
    (depends on what the server exposes: a settle sequence / timestamp);
  - a **resolution gate** that buffers recovered futures and releases them in
    recorded order (the SDK controls future resolution in the bg tasks, so the
    gate is feasible — but it fights asyncio's natural scheduling and must be
    airtight);
  - careful handling of ops still pending at suspend (e.g. the timer) and of
    partial groups.
- **Risk:** this is re-implementing a slice of a deterministic event loop. Easy
  to get subtly wrong; hard to test exhaustively. Also couples correctness to a
  server capability.

### Rejected / non-viable

- **A. Explicit user-supplied ids** (`ctx.options(id=…)`) — **excluded by
  constraint.** (For the record it is the smallest change and makes bare
  `asyncio` concurrency correct, but it puts uniqueness+determinism on the user.)
- **Call-site-addressed ids** (`hash(file:line)` + per-site counter) — brittle.
  Works for the example (the two `mark` calls are on different lines) but breaks
  when a shared helper does the `ctx.run` from concurrent branches (same
  call-site → per-site counter races again), and the ids change whenever code
  moves, breaking durability across refactors/deploys.
- **Content-addressed ids** (`hash(func, args, occurrence)`) — identical calls
  collide, and the disambiguating occurrence counter reintroduces the exact
  ordering problem.
- **Implicit task-lineage namespacing** (key `seq` by the current `asyncio`
  task via `contextvars`) — the SDK does not intercept `create_task`, so it
  cannot assign task branches a deterministic index without the user routing
  through a combinator. This collapses into option B.

### Comparison

| | B: combinator | C: deterministic replay |
|---|---|---|
| Deterministic ids | ✅ by construction | ✅ if completion order recorded |
| No user-defined ids | ✅ | ✅ |
| Bare `asyncio` concurrency works | ❌ (must use `ctx.gather`) | ✅ |
| Composes / nests / loops | ✅ | ✅ |
| Cross-deploy stability | ✅ structural | ✅ structural (order-anchored) |
| Server support needed | none | settlement-order signal |
| Implementation cost | medium | high |
| Blast radius | new API + child ctx | runtime core |
| Guards against misuse | needs §6 guard | inherent |

---

## 6. Recommendation

Adopt **B (structured concurrency combinator)** as the primary fix: it is
deterministic by construction, respects the "no user ids" constraint, reuses the
existing child-context machinery, and stays within the SDK's structured-
concurrency model (`flush_local_work`, the tree contract). Its one weakness is
that raw `create_task`/`gather` over durable ops stays silently unsafe.

Pair B with a **misuse guard** so the failure mode is loud, not corrupting:
detect concurrent durable ops issued on the *same* context from *different*
asyncio tasks that were not forked by the combinator, and raise a clear error
("concurrent durable operations must go through `ctx.gather`") instead of
minting an unstable id. Tag each context with the task that owns it; a durable
op called from a foreign task is the signal. This converts today's silent
value-swap into an actionable error at the call site.

Consider **C** only if transparent support for bare `asyncio` concurrency
becomes a hard requirement and the server can expose a durable settlement-order
signal — and treat it as a runtime-core project, not an incremental patch.

Whichever is chosen, mirror the decision in the Rust SDK (this port tracks it
1:1) so both SDKs share one concurrency contract.
```
