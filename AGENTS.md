# resonate-sdk-py — Agent Orientation

> **Read the README first for the user-facing overview.** This file is the agent orientation: what to know before editing the SDK source. The Python SDK is currently the lagging member of the SDK trio — see Status.

The Python SDK for [Resonate](https://resonatehq.io). Published as [`resonate-sdk`](https://pypi.org/project/resonate-sdk/) on PyPI.

## Status

- **Latest published:** `resonate-sdk` 0.6.7 on PyPI (2025-10-14)
- **Server compatibility:** **legacy server only** — the Python SDK targets the [legacy Resonate server](https://github.com/resonatehq/resonate-legacy-server). Compatibility with the v0.9.x Rust server is on the roadmap and not yet shipped.
- **Cadence:** stalled. Releases lag behind sibling SDKs; the API surface has not yet absorbed the protocol changes that landed in the v0.10.x TypeScript line and the Rust SDK.
- **Ownership:** PRs land incidentally; no full-time owner today.

## Stack

| | |
|---|---|
| Runtime | **Python ≥3.12** |
| Package mgr | **uv** recommended; pip works (`pyproject.toml` is the source of truth) |
| Linter / formatter | **ruff** (`ruff.toml`) |
| Tests | **pytest** (`tests/`) |
| Type checker | **pyright** (configured in `pyproject.toml`) |
| Build | hatchling (`pyproject.toml [build-system]`) |
| License | Apache-2.0 |

See the README for the user-facing setup + install context; this file's table is the agent-at-a-glance subset.

## Run

```bash
uv sync                          # create venv + install deps from uv.lock
uv run pytest                    # run the test suite
uv run ruff check                # lint
uv run ruff format               # format
uv run pyright                   # type check
uv build                         # build sdist + wheel
```

If `uv` isn't available, the equivalents are `python -m venv .venv && pip install -e ".[dev]"`, `pytest`, `ruff check`, etc. — but `uv.lock` is the canonical lockfile; mixing pip resolution will drift the dep tree.

Run one-shot commands (test, lint, format, type-check) freely; don't auto-start a server or watcher — the operator runs long-lived processes themselves.

## Architecture notes

- **Entry point** is `resonate/resonate.py` — the `Resonate` class auto-detects mode from its constructor: `Resonate()` runs local with in-memory state, `Resonate(url=...)` connects to a server. `RESONATE_URL` / `RESONATE_HOST` env vars are also picked up. `@resonate.register` decorates a registered function (typically a generator) and adds it to the registry in `resonate/registry.py`. Note: the published `0.6.7` wheel still exposes the older `Resonate.remote(...)` / `Resonate.local()` classmethod surface; `main` has folded those into the constructor — match what the source on the working tree shows, not the released wheel.
- **Generator-driven execution.** Resonate Python functions are generators that yield `ctx.run(...)`, `ctx.sleep(...)`, etc. The coroutine driver is `resonate/coroutine.py`; the scheduler is `resonate/scheduler.py`.
- **Bridge / processor:** `resonate/bridge.py` plus `resonate/processor.py` form the runtime that polls the server for tasks, dispatches generator steps, and persists results. `Event().wait()` keeps the main thread alive in the README example because the work happens on a background thread.
- **Stores** (`resonate/stores/`) are the persistence backends — local in-memory store and remote-server store. The remote store currently speaks the **legacy server protocol**, not the new Rust server's protocol; that's the gating issue blocking server-version parity.
- **DST simulator** lives in `resonate/simulator.py` and is exercised by `tests/test_dst.py`. CI runs DST on every push.
- **Encoders / codecs** (`resonate/encoders/`) are pluggable for at-rest encryption and custom serialization.

## Rules

1. **No PyPI publish without explicit maintainer approval** for that specific publish, in the same session. `scripts/new-release.py` handles the release flow but agents draft, don't ship.
2. **No pushing to `main` directly.** Open a PR; CI runs pytest + DST + ruff + pyright on every push.
3. **Don't add Rust-server protocol behavior on the legacy code path.** Server-protocol parity is a planned migration, not an incremental drip — partial protocol assumptions will break the legacy server and confuse the migration.
4. **Type-checked first, dynamic second.** `pyright` runs in CI; the codebase uses `from __future__ import annotations` (enforced by ruff isort config) and full type annotations. New code should be type-clean.
5. **Voice in user-visible strings = Echo.** Exception messages, log lines, and docstrings all use the Echo voice (technical, precise, friendly-but-not-casual).

## Pointers

- [README](./README.md) — quickstart and external-facing overview
- [CONTRIBUTING.md](./CONTRIBUTING.md) — issue-first workflow + Discord coordination channel
- [Python SDK guide on docs.resonatehq.io](https://docs.resonatehq.io/develop/python) — user-facing docs (note: page currently flags the legacy-server-only state)
- [Resonate Server (legacy, Go)](https://github.com/resonatehq/resonate-legacy-server) — the server this SDK currently targets
- [Resonate Server (new, Rust)](https://github.com/resonatehq/resonate) — the server compatibility target on the roadmap
- [Example apps](https://github.com/resonatehq-examples) — `example-*-py` repos demonstrate end-to-end patterns
- [PyPI package](https://pypi.org/project/resonate-sdk/) — release listing
- [Distributed Async Await](https://www.distributed-async-await.io/) — the underlying programming model

## Privacy

- The SDK accepts bearer tokens via `RESONATE_TOKEN` (env) or the `token` constructor arg, plus basic auth via `RESONATE_USERNAME` / `RESONATE_PASSWORD`. Tokens flow through to the message source / store; never log them, never echo them back to users in error messages.
- Application-level secret handling belongs in the encoder layer (`resonate/encoders/`), not in ad-hoc plumbing across the SDK.

## Known gaps

- **No v0.9.x Rust-server compatibility.** The remote store speaks the legacy protocol; targeting the Rust server is a planned migration, not in flight.
- **Release cadence has slowed.** Sibling SDKs (TS, Rust) shipped multiple releases between Python 0.6.6 and 0.6.7; the Python line is awaiting an active owner.
- **No public API reference site.** Generated reference (pydoctor) builds locally but is not currently published.
