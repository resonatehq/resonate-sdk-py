# Examples

Runnable examples for the Resonate Python SDK. Mirrors the Go SDK's
`examples/` directory.

Each example is a single `main.py`. Unlike the Go examples — which give every
example its own `go.mod` — these share the SDK project's environment, so there
is nothing extra to install: `uv run` resolves them against the project's
virtualenv (with the local `resonate` checkout installed editable).

| Example      | What it shows                                                       |
|--------------|---------------------------------------------------------------------|
| `hello`      | Minimal: register a function, run it durably, read the result.      |
| `fibonacci`  | Recursive composition via `ctx.run` / `ctx.rpc` / a mix.            |
| `concat`     | Divide-and-conquer recursion; deterministic left-to-right join.     |
| `saga`       | Multi-step workflow with compensation on failure.                   |
| `pipeline`   | DAG-shaped pipeline with parallel transforms and a merge stage.     |
| `human`      | Human-in-the-loop: suspend on `ctx.promise` until externally resolved. |
| `recurring`  | Periodic "continue as new" via `ctx.sleep` + `ctx.detached`.        |

## Run an example

Every example talks to a Resonate server. Start one with in-memory storage in
one terminal:

```sh
resonate dev
```

It listens on `localhost:8001`. Then, from the repository root:

```sh
uv run python examples/hello
```

Expected output:

```
hello, world!
```

The server URL defaults to `http://localhost:8001`; override it with the
`RESONATE_URL` environment variable.

## A note on durable replay

A durable orchestrator (a function that awaits `ctx.run` / `ctx.rpc` futures)
**re-executes from the top** every time it awaits a future that has not settled
yet — it suspends, the awaited work runs, and the orchestrator replays. Only
the leaf functions it dispatches settle once and are never re-run.

The practical rule: **put side effects (logging, I/O, external calls) in leaf
functions, never in the orchestrator.** Every `print` in the `saga` and
`pipeline` examples lives in a step/stage function for exactly this reason,
which is why each log line appears exactly once.

## Add a new example

1. Create `examples/<name>`.
2. Register your functions on a `Resonate(url=...)` instance and drive them
   with `r.run(id, fn, ...)`.
3. `uv run python examples/<name>`.
