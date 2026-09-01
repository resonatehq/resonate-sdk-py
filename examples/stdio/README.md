# stdio: a worker with no network

A Resonate program normally needs a route to a server. `StdioConnection`
removes that requirement: requests leave on the process's **stdout** behind an
`RN8:` marker, responses and messages arrive on its **stdin**, and whoever
started the process relays. No url, no port, no credential.

```
RN8:{"kind":"promise.create","head":{"corrId":"c1",...},"data":{...}}
```

Run the example, which stands up both ends locally:

```
uv run python examples/stdio                    # --mode push
uv run python examples/stdio --mode sandbox
```

## Two shapes, two programs

Which one you deploy depends on how the host tells the process what to run.

| | `worker.py` (push) | `sandbox.py` (per task) |
|---|---|---|
| Told what to run by | an `execute` message on stdin | `RESONATE_TASK_ID` in its environment |
| Class | `Resonate` — listens | `Handler` — is told |
| Wiring | `Resonate(network=StdioConnection())` | `Handler(network=StdioConnection())` |
| Runs | until stdin closes | one task, then exits |
| Entry point | `await stdio.wait_closed()` | `await handler.run_from_env()` |

`Resonate` is a worker: it opens sources, advertises an address, registers
listeners and refreshes them on a timer. A per-task process does none of that —
it was told which task it exists for before it started — so `Handler` is that
job as its own class: a registry, a codec, a dependency map and one network, no
loop and no `run`/`rpc`. See `resonate/handler.py`.

The Tensorlake worker uses the second shape. **`--mode sandbox` is that
contract in miniature** — the host below is a stand-in, but what it holds the
process to is the real thing, so the deployed shape is exercised rather than
described.

## Deploying to Tensorlake

The `tensorlake://` worker (in `resonatehq/resonate`) provisions a sandbox per
promise, starts your program in it, and carries the protocol over that
process's stdio. It never acquires the task and never settles anything — your
code does, through the tunnel.

### 1. The program

`sandbox.py` is the template. The parts that matter:

```python
handler = Handler(network=StdioConnection(), group="sandbox")

@handler.register
async def greet(ctx, name: str) -> str:
    return f"hello, {name}!"

async def main() -> None:
    status = await handler.run_from_env()   # RESONATE_TASK_ID, RESONATE_TASK_VERSION
```

Four rules, each of which the worker will otherwise hold you to:

* **Exit when `run_from_env` returns.** It returns `"done"` when the promise
  settled and `"suspended"` when the function unwound to await a child. Both
  mean *this* process is finished, and the difference is not recoverable from
  outside — a suspended function has settled nothing, so the promise reads
  `pending` either way. A process that lingers is still holding the tunnel when
  the resumption arrives, and the worker refuses to start a second process for a
  promise already running, so the promise stalls until the lease lapses.
* **Application output goes to stderr.** stdout is the protocol channel. The
  marker means a stray `print` cannot be mistaken for a frame — the worker logs
  unframed lines as sandbox output — but keeping the channel clean keeps the
  logs readable.
* **`group` is not something you listen on.** Nothing is listening. It is the
  target a child inherits when `ctx.rpc` names none, resolved through
  `resolve_target` — by default the server's own `poll://` groups, so an
  ordinary worker picks it up. Pass `Handler(resolve_target=...)` to send
  children elsewhere, a sandbox of their own included.
* **No credentials.** Deliberately: the whole point is that the sandbox cannot
  reach anything. `Handler` takes a `network` and has no `url` shorthand, so
  there is no ambient configuration for it to pick a connection out of.

### 2. The image

Build a Tensorlake image with Python, `resonate-sdk`, and your program, and
note the absolute path of the executable to start. The worker starts an
**executable, not a command line**: there is no quoting convention in a URL
path that survives a filename with a space, so extra arguments go in
`args` below, never in the address.

A wrapper script (`#!/usr/bin/env python3` … `exec python -u /app/sandbox.py`)
is the usual way to name one path and keep the arguments in the image.

### 3. The server

In the Resonate server's config:

```toml
[transports.tensorlake]
enabled = true
image = "your-org/your-image:tag"       # absent = Tensorlake's default environment
process = "/app/sandbox.py"             # the executable to start
args = []
working_dir = "/app"
sandbox_timeout = 600                   # seconds idle before a named sandbox suspends
keep_pending = true                     # keep the sandbox while the promise is pending

[transports.tensorlake.accounts.default]
api_key_env = "TENSORLAKE_API_KEY"      # the default; prefer this to an inline api_key
# api_url   = "https://api.tensorlake.ai/sandboxes"
# proxy_host = "sandbox.tensorlake.ai"
```

`accounts` is not a Tensorlake concept — the API key implies the project. It
exists so one server can address several, and so a self-hosted endpoint is a
config entry rather than a code change. `default` needs no entry at all.

### 4. Dispatching to it

An address, not a group. A target containing `://` passes through target
resolution untouched, so any caller can name one:

```python
handle = client.options(target="tensorlake://").rpc(id, "greet", "world")
```

`tensorlake://[account[/image[/process]]]`, every part optional and filled in
from the config above. Use `?image=` and `?process=` for image names that
contain a slash (`tensorlake://?image=tensorlake/ubuntu-minimal`).

**One sharp edge:** the address parser trims slashes off each part, so a
process named *in an address* is always relative — `tensorlake://prod/img/app/run.py`
and `?process=/app/run.py` both yield `app/run.py`, resolved against
`working_dir` (or `PATH`). An absolute path has to come from
`transports.tensorlake.process` in the config, which is a plain string and is
not parsed. Set `working_dir` and keep addresses relative, or name the
executable once in the config and leave it out of the address.

To route a whole group there, give the dispatching client a resolver:

```python
Resonate(url=..., resolve_target=lambda group: f"tensorlake://prod/img/bin/{group}")
```

## What the worker does around your process

Worth knowing, because it explains what you will see in the logs.

* **The sandbox is the promise.** It is named for the task id — a task id *is*
  a promise id — so every later message finds the same sandbox: a retry after a
  crash, a redispatch after a lease expired, a resumption after your function
  suspended. Tensorlake suspends a named sandbox when it goes idle with its
  filesystem, memory and processes intact, so resuming one is the normal path.
* **An `unblock` is not delivered.** The process it concerns has already
  exited. The server follows an unblock with a task for the awaiting promise,
  and the `execute` for that starts a fresh process in the same sandbox.
* **Nothing is settled on failure.** A sandbox that will not start is a
  delivery failure, and the task is left for the server to dispatch again. A
  tunnel that drops leaves the sandbox alone — the process may still hold the
  lease — but closes its stdin, so a process waiting on a reply that will never
  come reads EOF instead of waiting out the lease. `StdioConnection` turns that
  EOF into a `ConnectorError` on everything in flight rather than a hang.
* **Your exit code is a log line.** A non-zero exit or a signal is reported as
  a process failure. It settles nothing either way — the promise's state is the
  only thing that decides what happens next.
