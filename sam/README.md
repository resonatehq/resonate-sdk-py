# sam-app — Resonate on AWS Lambda

Running **durable Resonate functions serverless** on AWS Lambda, using the
`resonate.faas.aws` shim. Each example is one Lambda hosting a Resonate
workflow that the Resonate server drives one task at a time.

- `hello-world/app.py` — the classic `foo` → `bar` → `baz` workflow.
- one directory per migrated example (see **Examples** below).
- `template.yaml` — the SAM template wiring every function, its URLs, and API.
- `tests/` — SAM's stock unit/integration test scaffolding.

Every function directory has the same shape: `app.py` (registered functions +
exported `lambda_handler`), a `requirements.txt`, and a `resonate` symlink to
the local SDK (`../../src/resonate`) so `sam build` bundles it.

## How the shim works

`resonate.faas.aws.Resonate` is the **serverless twin** of
`resonate.resonate.Resonate`. It keeps the construction-time surface you already
know — `register`, `with_dependency`, codec/registry/retry wiring — but drops
everything a serverless worker cannot do: no event loop, no SSE poll connection,
no background heartbeat, and no client-side `run`/`rpc`/`get`.

Instead of *polling* for work, the shim is **pushed** work:

1. A client calls `resonate invoke` (or `ctx.run`/`ctx.rpc` from another
   function). This creates a durable promise + task on the **Resonate server**.
2. The server **POSTs** a single `execute` message to your function's URL.
3. The handler decodes it, builds a per-invocation `Core` + a send-only
   `HttpNetwork` back to the server, drives the task to `done` or `suspended`,
   and returns a JSON status.
4. Child tasks (`ctx.run`, `ctx.rpc`) are routed back to **this same function's
   URL**, so the whole call graph runs on Lambda — each hop is its own
   invocation.

Because a Lambda cannot heartbeat a lease in the background, the per-task lease
`ttl` is held for the whole window (default 5 min); keep it comfortably above
the function `Timeout` so the server does not re-deliver a task still running.

## Writing a function

`hello-world/app.py` is the whole contract — register functions on a
module-level `Resonate()` and export `resonate.handler()` as the Lambda entry
point:

```python
from resonate.faas.aws import Resonate
from resonate.context import Context

resonate = Resonate()

@resonate.register
async def foo(ctx: Context, name: str) -> str:
    return await ctx.run(bar, name)      # local child, re-invokes this Lambda

@resonate.register
async def bar(ctx: Context, name: str) -> str:
    return await ctx.rpc("baz", name)    # remote-by-name child, also this Lambda

@resonate.register
async def baz(ctx: Context, name: str) -> str:
    return f"hello, {name}!"

lambda_handler = resonate.handler()
```

The handler is a plain (non-async) callable; it runs the workflow under its own
`asyncio.run` per invocation. Register your functions and dependencies
**before** exporting `handler()`.

## The server URL

The shim needs the Resonate server's address to talk *back* to it (send
acquire/fulfill/suspend). Set it via `RESONATE_URL` (or the `url=` constructor
arg):

| Env var / setting | Direction | Local value |
| --- | --- | --- |
| `RESONATE_URL` | **Lambda → server** — where the shim sends acquire/fulfill/suspend | `http://host.docker.internal:8001` |

Under `sam local` the Lambda runs in a Docker container, so `localhost` means
*the container*. To reach a `resonate dev` server on your host it must use
`host.docker.internal`.

`RESONATE_URL` always wins over the `serverUrl` the server advertises in the
pushed message: the server only knows its own local view of its address, the
deployment knows the routable one.

### What about the function's own URL?

The shim also needs **its own** public URL — the target the server pushes child
tasks (`ctx.run` / `ctx.rpc`) back to.

- **On real AWS** it reconstructs this automatically from the request's `host`
  header (with `x-forwarded-proto` for the scheme), so you set nothing — remove
  `RESONATE_FUNCTION_URL` from the template on deploy.
- **Under `sam local`** the emulated event does *not* carry a usable host
  (`headers.host` is absent and `requestContext.domainName` is `localhost` with
  no port), so the shim cannot derive a reachable URL. Set it explicitly with
  `RESONATE_FUNCTION_URL` — this is the `http://127.0.0.1:3000/hello` value in
  `template.yaml`.

It is `http://127.0.0.1:3000/hello` because the **server** (on your host) pushes
child tasks to it, and that is where SAM-local serves the function.

## Template essentials

A few settings in `template.yaml` are load-bearing for the shim (not the SAM
defaults):

```yaml
Globals:
  Function:
    Timeout: 30          # cold-start import of resonate needs headroom
    MemorySize: 512      # more memory = more CPU = faster cold start

Resources:
  HelloWorldFunction:
    Properties:
      Architectures:
        - arm64          # match Apple Silicon; avoids slow QEMU emulation locally
      Events:
        HelloWorld:
          Type: HttpApi  # payload format 2.0 — the shim reads requestContext.http.method
          Properties:
            Path: /hello
            Method: post  # the server delivers execute messages via POST
```

`Type: HttpApi` (not `Type: Api`) matters: the REST API (`Api`) emits payload
**v1.0**, where the method lives at `event.httpMethod`; the shim reads the HTTP
API **v2.0** shape (`event.requestContext.http.method`) and would otherwise
reject every request with `405`.

## Run it locally

You need Docker, the [SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html),
and the Resonate CLI.

```bash
# 1. Start a Resonate server (in-memory, ephemeral) on :8001
resonate dev

# 2. Build and serve the Lambda locally on :3000
sam-app$ sam build
sam-app$ sam local start-api

# 3. From a third shell, invoke the workflow through the server
resonate invoke hello.1 \
  --func foo \
  --arg world \
  --target http://127.0.0.1:3000/hello
# -> "hello, world!"
```

`resonate invoke` flags:

| Flag | Meaning |
| --- | --- |
| `<ID>` | Promise id — any unique string per invocation (`hello.1`) |
| `--func` | Registered function name (`foo`) |
| `--arg` | One positional arg per flag; parsed as JSON if it looks like JSON, else a plain string |
| `--target` | **Where the server pushes the execute message** — the function's HTTP URL, not the default `poll://any@default` (that is for long-running poll workers) |
| `--server` | The Resonate server the CLI talks to (default `http://localhost:8001`) |

Inspect the run:

```bash
resonate tree hello.1            # the call graph, foo → bar → baz
resonate promises get hello.1    # the durable promise + result
```

### Local gotchas

- **Intermittent `500 timed out` on cold starts** — raise `Timeout`, bump
  `MemorySize`, and use `arm64` so SAM-local does not emulate x86_64 under QEMU.
- **`405` on POST** — you are on `Type: Api` (payload v1). Switch to
  `Type: HttpApi`.
- **Child tasks hang / `500 ... no host header or domainName`** — under
  `sam local` the function URL is not derivable; `RESONATE_FUNCTION_URL` must be
  set in the template.
- **`Cannot connect to host localhost:8001`** — `RESONATE_URL` is unset or being
  overridden by the head; it must be `host.docker.internal:8001` from inside the
  container.

## Examples

Every example is invoked the same way: `sam local start-api` serves them all on
`:3000`, and you `resonate invoke` against each one's path. IDs below are
arbitrary — use a fresh one per run. Inspect any run with `resonate tree <id>`
and `resonate promises get <id>`.

### hello-world — `/hello`

```bash
resonate invoke hello.1 --func foo --arg world \
  --target http://127.0.0.1:3000/hello
# -> "hello, world!"
```

### detached — `/detached`

Fire-and-forget: `place_order` runs `reserve_stock` + `charge_card`, then
`ctx.detached`-dispatches `write_audit_log` as its own workflow and returns
without waiting. The audit runs as a separate pushed task on its own timeline.

```bash
resonate invoke order.1 --func place_order \
  --arg alice --arg WIDGET-7 --arg 2 --arg 199 \
  --target http://127.0.0.1:3000/detached
```

The result includes the detached audit's promise id — `resonate promises get`
that id to see the audit workflow resolve independently.

### fibonacci — `/fibonacci`

Recursive durable invocations. Pick the composition style by function name:
`fib_rpc` (all `ctx.rpc`), `fib_run` (all `ctx.run`), or `fib_mix`.

```bash
resonate invoke fib.1 --func fib_rpc --arg 10 \
  --target http://127.0.0.1:3000/fibonacci
# -> 55
```

### error-handling — `/error-handling`

Failures raised in a durable child cross the boundary and are re-raised (with
the original type when picklable) for the awaiter. `foo` takes a `mode`
(`run` | `rpc`) selecting how it calls `bar`.

```bash
# success
resonate invoke eh.1 --func foo --arg alice --arg 25 --arg run \
  --target http://127.0.0.1:3000/error-handling
# UsernameTakenError
resonate invoke eh.2 --func foo --arg admin --arg 25 --arg run \
  --target http://127.0.0.1:3000/error-handling
# ValueError
resonate invoke eh.3 --func foo --arg alice --arg -5 --arg rpc \
  --target http://127.0.0.1:3000/error-handling
```

### pipeline — `/pipeline`

A DAG: `download → parse → (transform_a ∥ transform_b) → merge → emit`. Each
stage is its own durable, resumable task.

```bash
resonate invoke pipe.1 --func run_pipeline --arg example.com/doc \
  --target http://127.0.0.1:3000/pipeline
```

### pydantic — `/pydantic`

Pydantic models round-trip across the durability boundary and are rebuilt as
typed models. Pass the line items as a JSON arg. (Its `requirements.txt`
includes `pydantic`.)

```bash
resonate invoke pyd.1 --func create_order \
  --json-args '["ord-42", [{"sku":"A1","qty":2,"unit_price":9.5},{"sku":"B2","qty":1,"unit_price":4.0}]]' \
  --target http://127.0.0.1:3000/pydantic
```

### pydantic-ai — `/pydantic-ai`

A **durable Pydantic AI agent running serverless**: `ResonateAgent` wraps a
multi-step support-triage agent and registers its run loop as the durable
function `support.run` on the FaaS shim (its constructor only needs
`register`, which the shim provides). The agent is deliberately multi-step —
three model round-trips, each checkpointed as its own durable promise:

1. Model call 1 fans out `lookup_order` + `check_inventory` as **parallel tool
   calls** (checkpointed in deterministic order).
2. Model call 2 calls `escalate_refund`, a tool that reaches the ambient
   durable `Context` (`workflow_context()`): `ctx.sleep` — a durable timer,
   the invocation returns *suspended* and the Lambda holds no state until the
   server re-pushes the task — then `ctx.rpc(process_refund, ...)`, a child
   task pushed back to this same function as its own invocation.
3. Model call 3 emits the final structured `Triage` output (a Pydantic model,
   rebuilt across the durability boundary).

One agent run therefore spans **four Lambda invocations** (suspend on the
timer, suspend awaiting the rpc child, the child's own invocation, the final
resume), and every resume replays from the journal — checkpointed model
requests are *not* re-executed. The example uses Pydantic AI's offline
`FunctionModel` (scripted, no API key); swap in a real model at agent
construction for the real thing. Its `requirements.txt` adds
`pydantic-ai-slim`.

The durable run takes one JSON object — the run's `RunParams` (only
`user_prompt` here; `message_history`, `deps`, etc. also fit):

```bash
resonate invoke triage.1 --func support.run \
  --json-args '[{"user_prompt": "Triage order A-1"}]' \
  --target http://127.0.0.1:3000/pydantic-ai
```

```bash
resonate tree triage.1
# triage.1
# ├── triage.1.1 🟢 (run)   — model request 1 (parallel tool calls)
# ├── triage.1.2 🟢 (run)   — model request 2 (escalate)
# ├── triage.1.3 🟢 (sleep) — durable cooldown inside the tool
# ├── triage.1.4 🟢 (rpc process_refund)
# └── triage.1.5 🟢 (run)   — model request 3 (final structured output)
resonate promises get triage.1   # value.output is the structured Triage
```

Because a lease cannot be heartbeated, everything between two suspension
points (model calls + inline tools) must finish within one invocation — keep
the function `Timeout` sized for your slowest model step, and the shim's
`ttl` above it. Reattaching by id also works serverless: a second
`resonate invoke triage.1` returns the same settled promise without re-running
the agent.

### structured-concurrency — `/structured-concurrency`

`foo` spawns two `ctx.run(bar)` children and returns `5` without awaiting
them; the runtime still joins both before settling `foo`. Confirm via the
child promises `sc.1.1` and `sc.1.2`.

```bash
resonate invoke sc.1 --func foo \
  --target http://127.0.0.1:3000/structured-concurrency
resonate promises get sc.1.1   # -> 10
resonate promises get sc.1.2   # -> 20
```

### versioning — `/versioning`

The name `charge` is registered at two versions; select with `--version`.

```bash
resonate invoke chg.1 --func charge --version 1 --arg 100 \
  --target http://127.0.0.1:3000/versioning   # -> 100.0
resonate invoke chg.2 --func charge --version 2 --arg 100 \
  --target http://127.0.0.1:3000/versioning   # -> 103.0 (3% fee)
```

### Not migrated: rpc

The `examples/rpc` example was **not** migrated. Its entire point is two
long-running **poll-worker groups** sharing a server, where a `frontend` group
dispatches to a `backend` group by group anycast (`poll://any@backend`). The
FaaS shim has no poll workers and no groups — work is *pushed* to a single URL
per function, never pulled by a group of subscribers. A single-Lambda rendering
would collapse to plain name-dispatch, which `fibonacci --func fib_rpc` already
demonstrates (a `ctx.rpc` routed back to the same function). Nothing about the
multi-group model has a serverless analog, so it is left out.

## Deploy to AWS

```bash
sam build
sam deploy --guided
```

The guided deploy prompts for stack name, region, and IAM capability. After it
completes, take the `HelloWorldApi` output URL and set `RESONATE_URL` to your
reachable Resonate server URL. **Remove `RESONATE_FUNCTION_URL`** from the
template on deploy — on real API Gateway the shim derives its own URL from the
request headers.

Then invoke with `--target <HelloWorldApi URL>`.

Fetch logs from a deployed function:

```bash
sam logs -n HelloWorldFunction --stack-name "sam-app" --tail
```

## Cleanup

```bash
sam delete --stack-name "sam-app"
```

## Resources

- [Resonate](https://www.resonatehq.io) — durable promises and functions.
- [AWS SAM developer guide](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html).
