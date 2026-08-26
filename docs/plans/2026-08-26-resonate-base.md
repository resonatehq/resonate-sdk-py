# `resonate-base` Extraction Implementation Plan

> **For agentic workers:** implement this plan task-by-task, in order. Steps use checkbox (`- [ ]`) syntax for tracking. Every task ends in a green test suite and a commit; do not start task N+1 with task N red.

**Goal:** Extract the protocol/plumbing layer of the SDK into a standalone `resonate-base` distribution so that connector packages (NATS today; GCP, Cloudflare, others later) can be built against a small, stable core without depending on the full `resonate-sdk`.

**Architecture:** `resonate-base` (module `resonate_base`) owns the pieces that describe *the Resonate protocol and its seams* — errors, injectable time, backoff/retry policies, the observability event stream, id format, wire records, the `Network`/`Source` protocols, and the `Transport` that frames JSON over them. `resonate-sdk` (module `resonate`) keeps everything that describes *executing durable functions* — codec, context, core, tree, effects, registry, sender, handles, and the `HttpConnection`/`SSEConnection`/`LocalConnection` implementations that are the SDK's default transports. `resonate-nats` (module `resonate_nats`) becomes the first out-of-tree connector and proves the seam.

```
resonate-base ──► resonate-sdk ──► resonate-sdk-aws  (and future gcp / cloudflare)
      │
      ├────────► resonate-nats
      │
      └────────► (future connectors: gcp pubsub, cloudflare queues, ...)

http + sse + local stay inside resonate-sdk: they are the default transports.
```

**Tech Stack:** Python ≥3.12, uv workspace (`uv_build` backend), msgspec, pytest, ruff (`select = ["ALL"]`), ty, GitHub Actions (`ci.yml` / `cd.yml`).

---

## ⚠ Amendments — what actually shipped

> **This plan is executed and superseded in three places.** Read this section before treating anything below as current; the module inventory, the codemod, and Task 8's test moves describe intermediate states, not the tree.

### A. Base was narrowed to the connector seam

Tasks 4–7 moved `timing`, `retry`, `observability`, `transport` and the wire records into base. That was reversed: those describe *executing durable functions*, not the seam, and they went back to `src/resonate/`. Base ships **`connections.py`, `error.py`, `ids.py`, `addresses.py`** and has **zero** third-party dependencies (not `msgspec` — the guard test's allow-list is empty).

The error tree was narrowed the same way, and for the same reason. Base's `error.py` is now **three classes**: `ResonateError` (the root the SDK's outermost catch depends on), `ConnectorError` (the only error a connector raises), and `InvalidIdError` (raised by `ids` and `addresses`). Everything else — `ServerError`, `DecodingError`, `SerializationError`, `Base64DecodeError`, `StoppedError`, `FunctionNotFoundError`, `AlreadyRegisteredError`, `ApplicationError`, `ResonateTimeoutError`, `PlatformError`, `Suspended`, and the three union aliases with `_pin_unions` — lives in **`src/resonate/error.py`**, which re-exports base's three so SDK users still have one import path.

A side benefit worth noting in the release notes: this **restores** the pre-0.8 pickle module path (`resonate.error`) for eleven of the fourteen error classes, so the rolling-upgrade fidelity loss the plan warned about now applies only to `ConnectorError`, `InvalidIdError` and `ResonateError`.

### B. Decision 2 is reversed: `NatsError` is owned by `resonate-nats`

The original reasoning was sound but the conclusion followed from an avoidable constraint. `SenderError` was a closed union *over implementations*:

```python
type SenderError = ServerError | DecodingError | HttpError | NatsError
```

`_pin_unions` makes `ty` enforce that exhaustively, so base could not name a transport failure without naming every transport — which meant base had to know about every connector that would ever exist, and cut a release per connector.

The union is now closed over a **category** instead, with exactly one open point:

```python
class ConnectorError(ResonateError):
    label: ClassVar[str] = "connector"   # "{label} error: {cause}"

type SenderError = ServerError | DecodingError | ConnectorError
```

Subclassing is **optional**, and the SDK's own transport does not bother: `HttpConnection` raises `ConnectorError(exc)` directly, and `HttpError` no longer exists. A subclass earns its keep only when a connector ships as its own distribution and its users want a name to catch specifically — which is exactly `resonate_nats.NatsError` (`label = "nats"`). Message shape, `args`, and the pickle round-trip are inherited; `label` is the entire extension point.

This *serves* the original rationale better than keeping `NatsError` in base did: user code catching a connection failure writes `except ConnectorError` and covers connectors that did not exist when it was written, with no connector package installed.

### C. The `resonate-sdk[nats]` extra is removed

An extra puts `resonate-nats` in `resonate-sdk`'s **published dependency metadata**. There is no import edge — `Resonate`'s URL dispatch only knows `http`/`local` — but at the packaging layer the arrow becomes `resonate-sdk → resonate-nats → resonate-base`, which is what the split exists to prevent, and every future connector would want its own extra. Install a connector directly: `pip install resonate-nats`. It stays in the root `dev` dependency-group so the workspace suite still exercises it.

### D. Base gained the rest of the connector kit

| Added | Why |
|---|---|
| `resonate_base.addresses` | Helpers for the `{scheme}://{kind}@{group}/{pid}` delivery format the server parses. Was an f-string duplicated in `sse.py`, `local.py` and `nats`; a malformed address raises nowhere and simply never delivers. `sse.py` and `local.py` mint through it now. Offered as a convenience, not a requirement — NATS keeps addressing its own subject namespace. |
| `PROTOCOL_VERSION` moved into `resonate_base` | It describes the wire, not durable execution. `resonate.PROTOCOL_VERSION` is gone. |
| A curated `resonate_base.__init__` | `from resonate_base import Network, Source, ConnectorError, origin_of` — Decision 3 said no façade for the *SDK*; base is a different package whose entire audience is connector authors. |

**Considered and rejected:** a `resonate_base.conformance` module shipping inheritable `LayeringSuite` / `NetworkSuite` / `SourceSuite` test suites. It was built and removed. A connector author should be able to write a connector without satisfying a suite we authored, and it hard-coded our own dependency policy (`DEFAULT_FORBIDDEN = {"resonate", "resonate_aws", "resonate_testing"}`) into a package other people depend on. The two layering guards stay as ordinary test files in the packages that chose to write them — duplicated, and that is fine: they are each a decision that package made, not a rule base imposes.

**Current suite: 1023 passed.**

**Baseline to preserve:** `uv run pytest -q` currently reports **1001 passed** in ~2s. That number must not go down; tasks that move tests keep the total identical, tasks that add guard tests raise it.

---

## Open decisions (confirm before Task 1)

These three were judgement calls made while writing the plan. The plan implements the recommendation; flag now if you disagree, because reversing them later is expensive.

| # | Question | Decision taken | Why |
|---|---|---|---|
| 1 | Where does `LocalConnection` (1334-line in-process server simulation) live? | **Stays in `resonate-sdk`** (`resonate/connections/local.py`) | It is a *server* simulation, not a shared definition. It backs `Resonate.local()` and `resonate.testing`. A connector implementing `Network` gains nothing from it; keeping it out holds base at ~1200 lines of pure definitions. |
| 2 | Does `NatsError` follow `NatsConnection` into `resonate-nats`? | ~~**No — the whole error tree stays in `resonate_base.error`**~~ **Reversed — see Amendment B.** `NatsError` is owned by `resonate-nats`, `HttpError` by `resonate.connections.http`, and base owns the `ConnectorError` category both subclass. | The original *why* (`tests/test_error_vocabulary.py` asserts a closed vocabulary; users should not need the nats package to catch a connection failure) is preserved — `except ConnectorError` catches every transport, including ones base has never heard of. |
| 3 | Do user-facing conveniences (`Value`, `Never`, `Exponential`) keep a short import path? | **No façade in this plan** — examples/README move to `from resonate_base.retry import Never` | Consistent with the "hard move, no shims" decision. A curated `resonate/__init__.py` façade re-exporting the ~10 names users actually type is a reasonable *follow-up*, but it is a public-API design question, not part of the extraction. |

**Accepted breaking changes** (this is a 0.8.0, not a 0.7.5):

- `resonate.error`, `resonate.timing`, `resonate.retry`, `resonate.ids`, `resonate.observability`, `resonate.transport` no longer exist; import from `resonate_base.*`.
- `resonate.types` keeps only SDK-level types (`Args`, `TaskData`, `Status`, `Info`, `DurableRegistry`); wire records move to `resonate_base.types`.
- `resonate.connections` keeps only the concrete SDK transports; `Network` / `Source` move to `resonate_base.connections`.
- `resonate.connections.nats` is gone; use `resonate_nats.NatsConnection` (~~installable via the unchanged `resonate-sdk[nats]` extra~~ — the extra is removed, see Amendment C: `pip install resonate-nats`).
- `NatsError` and `HttpError` are gone from the shared error module. Catch `resonate.error.ConnectorError` (re-exported from `resonate_base.error`); HTTP failures now *are* `ConnectorError`, and NATS failures are `resonate_nats.NatsError`, a subclass (Amendment B).
- `resonate.error` exists again and is the SDK's single error import path; only `ResonateError`, `ConnectorError` and `InvalidIdError` are defined in `resonate_base.error` (Amendment A).
- `resonate.PROTOCOL_VERSION` / `resonate.now_ms` etc. move to `resonate_base`.
- **Pickled exceptions cross-version:** `codec._encode_error` pickles exceptions into rejected promise values, and pickle records the class's module path. A promise rejected by a 0.7.x worker with, say, `resonate.error.ApplicationError` cannot be unpickled by a 0.8 worker. This degrades *gracefully* — `_deserialize_error` catches the `ModuleNotFoundError` and falls back to `ApplicationError(message)` — so it is a fidelity loss on in-flight promises during a rolling upgrade, not a crash. Call it out in the release notes.

---

## Module inventory

Exactly what moves. Line counts are from the current tree.

### Into `packages/resonate-base/src/resonate_base/`

| New path | Source | LOC | Notes |
|---|---|---|---|
| `__init__.py` | `src/resonate/__init__.py` | 8 | Keeps `PROTOCOL_VERSION`; drops the timing re-exports. |
| `error.py` | `src/resonate/error.py` | 257 | Whole file, unchanged. |
| `timing.py` | `src/resonate/timing.py` | 43 | Whole file, unchanged. |
| `retry.py` | `src/resonate/retry.py` | 102 | Whole file. `RetryPolicy` family + `Backoff` family. |
| `observability.py` | `src/resonate/observability.py` | 126 | Whole file. The `"resonate.validation"` logger name is a wire contract — **do not** rename it to `resonate_base.validation`. |
| `ids.py` | `src/resonate/ids.py` | 97 | Whole file. |
| `types.py` | part of `src/resonate/types.py` | ~90 | `PromiseState`, `Value`, `PromiseRecord`, `TaskRecord`, `ScheduleRecord`, `PromiseCreateReq`, `PromiseSettleReq`, `PromiseRegisterCallbackData`. |
| `connections.py` | protocols from `src/resonate/connections/__init__.py` | ~55 | `Network`, `Source`. A flat module, not a package: base ships no implementations. |
| `transport.py` | `src/resonate/transport.py` | 181 | Whole file: `Transport`, `TaskRef`, `ExecuteData`, `ExecuteMsg`, `UnblockData`, `UnblockMsg`, `Message`, `ResponseHead`, `Response`. |

Base's only runtime dependency is `msgspec`. It must never import `resonate`, `resonate_nats`, or `aiohttp` — Task 2 installs a test that enforces this.

### Into `packages/resonate-nats/src/resonate_nats/`

| New path | Source | LOC |
|---|---|---|
| `__init__.py` | `src/resonate/connections/nats.py` | 283 |

Its only `resonate` couplings today are `error.NatsError` and `ids.origin_of` — both land in base. Nothing in `resonate/resonate.py` constructs a `NatsConnection` (the URL dispatch only knows `http`/`local`), so the split needs no dispatch changes.

### Staying in `resonate-sdk`

`codec`, `chain`, `context`, `core`, `dependencies`, `durable`, `effects`, `handle`, `heartbeat`, `promises`, `registry`, `resonate`, `schedules`, `send`, `testing`, `tree`, `ext/**`, `connections/{http,sse,local}.py`, and the SDK half of `types.py` (`Args`, `TaskData`, `Status`, `Info`, `DurableRegistry`).

---

## Task 1: Scaffold the `resonate-base` package

**Files:**
- Create: `packages/resonate-base/pyproject.toml`
- Create: `packages/resonate-base/README.md`
- Create: `packages/resonate-base/src/resonate_base/__init__.py`
- Create: `packages/resonate-base/src/resonate_base/py.typed`
- Modify: `pyproject.toml` (root: dependency, source, coverage)
- Modify: `ruff.toml` (isort first-party list)

- [ ] **Step 1: Create the package manifest**

`packages/resonate-base/pyproject.toml`:

```toml
[project]
name = "resonate-base"
version = "0.7.4"
description = "Shared protocol definitions for the Resonate SDK and its connectors, by Resonate HQ, Inc"
readme = "README.md"
authors = [{ name = "Resonate HQ, Inc", email = "contact@resonatehq.io" }]
requires-python = ">=3.12"
dependencies = ["msgspec>=0.21.1,<1"]

[project.urls]
Documentation = "https://github.com/resonatehq/resonate-sdk-py#readme"
Issues = "https://github.com/resonatehq/resonate-sdk-py/issues"
Source = "https://github.com/resonatehq/resonate-sdk-py"

[build-system]
requires = ["uv_build>=0.11.7,<0.12.0"]
build-backend = "uv_build"

[tool.uv.build-backend]
module-name = "resonate_base"
```

- [ ] **Step 2: Write the package README**

`packages/resonate-base/README.md`:

```markdown
# resonate-base

The shared protocol layer behind the Resonate Python SDK and every Resonate
connector.

`resonate-base` holds the definitions a connector needs and nothing else: the
error vocabulary, injectable time (`Clock` / `Sleeper`), retry and backoff
policies, the observability event stream, the promise id format, the wire
records exchanged with the Resonate server, the `Network` and `Source`
protocols a connector implements, and the `Transport` that frames JSON over
them.

It depends only on `msgspec`. It never imports the SDK, so a connector built
on it stays independent of the SDK's release cadence.

```python
from resonate_base.connections import Network, Source
from resonate_base.error import ServerError
from resonate_base.retry import ExponentialBackoff
from resonate_base.timing import Sleeper, sleep


class MyConnection:
    """A Network implementation for some new substrate."""

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, req: str) -> str: ...
```

Application code should depend on [`resonate-sdk`](https://pypi.org/project/resonate-sdk/),
which depends on this package.
```

- [ ] **Step 3: Create the module entry point**

`packages/resonate-base/src/resonate_base/__init__.py`:

```python
"""Shared protocol definitions for the Resonate SDK and its connectors."""

from __future__ import annotations

#: Protocol version string sent in all requests to the Resonate server.
PROTOCOL_VERSION = "2026-04-01"

__all__ = ["PROTOCOL_VERSION"]
```

Create an empty `packages/resonate-base/src/resonate_base/py.typed`:

```bash
touch packages/resonate-base/src/resonate_base/py.typed
```

- [ ] **Step 4: Wire the package into the workspace**

In the root `pyproject.toml`, add the runtime dependency and the workspace source. `[tool.uv.workspace] members = ["packages/*"]` already picks the directory up, so only these three hunks change:

```toml
dependencies = [
    "aiohttp>=3.13.5,<4",
    "msgspec>=0.21.1,<1",
    "resonate-base>=0.7.4,<0.8",
]
```

```toml
[tool.uv.sources]
resonate-base = { workspace = true }
resonate-testing = { path = "packages/resonate-testing", editable = true }
```

```toml
[tool.coverage.run]
source = ["resonate", "resonate_aws", "resonate_base"]
```

- [ ] **Step 5: Teach ruff that `resonate_base` is first-party**

Without this, isort files `resonate_base` under third-party and every touched
file grows a second import block. In `ruff.toml`:

```toml
[lint.isort]
combine-as-imports = true
known-first-party = ["resonate", "resonate_aws", "resonate_base", "resonate_nats", "resonate_testing"]
required-imports = ["from __future__ import annotations"]
```

- [ ] **Step 6: Sync and verify the package resolves**

```bash
uv sync --locked --all-extras --all-packages || uv sync --all-extras --all-packages
uv run python -c "import resonate_base; print(resonate_base.PROTOCOL_VERSION)"
```

Expected: `2026-04-01`. The first `uv sync` will fail the `--locked` check because the lockfile has no `resonate-base` yet; the fallback regenerates it.

- [ ] **Step 7: Confirm nothing else broke**

```bash
uv run pytest -q
uv run ruff check
uv run ty check
```

Expected: `1001 passed`, and clean ruff/ty.

- [ ] **Step 8: Commit**

```bash
git add packages/resonate-base pyproject.toml ruff.toml uv.lock
git commit -m "build: scaffold the resonate-base workspace package"
```

---

## Task 2: Install the layering guard (before anything moves)

The whole point of the split is a dependency direction. Encode it as a test now, so every subsequent task is checked by CI rather than by review.

**Files:**
- Create: `packages/resonate-base/tests/test_layering.py`

- [ ] **Step 1: Write the guard test**

`packages/resonate-base/tests/test_layering.py`:

```python
"""resonate-base must not depend on anything above it.

The value of the package is the direction of the arrow: the SDK and every
connector may import base, and base may import nothing but the standard
library and msgspec. A single stray ``from resonate.codec import Codec``
silently turns base into a second copy of the SDK, so the rule is asserted
rather than documented.
"""

from __future__ import annotations

import ast
import pathlib
import sys

SRC = pathlib.Path(__file__).parent.parent / "src" / "resonate_base"

#: Everything base is allowed to import at module scope, beyond the stdlib.
ALLOWED_THIRD_PARTY = {"msgspec"}

#: Packages that sit above base in the dependency graph.
FORBIDDEN_ROOTS = {"resonate", "resonate_aws", "resonate_nats", "resonate_testing"}


def modules() -> list[pathlib.Path]:
    return sorted(SRC.rglob("*.py"))


def imported_roots(path: pathlib.Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and not node.level and node.module:
            roots.add(node.module.split(".")[0])
    return roots


def test_source_files_exist() -> None:
    """Guard the guard: a bad SRC path would make every test below vacuous."""
    assert modules(), f"no modules found under {SRC}"


def test_base_does_not_import_the_layers_above_it() -> None:
    offenders = {
        path.name: sorted(imported_roots(path) & FORBIDDEN_ROOTS)
        for path in modules()
        if imported_roots(path) & FORBIDDEN_ROOTS
    }
    assert not offenders, f"base imports the layers above it: {offenders}"


def test_base_pulls_in_no_unexpected_third_party() -> None:
    offenders = {}
    for path in modules():
        third_party = {
            root
            for root in imported_roots(path)
            if root not in sys.stdlib_module_names and root != "resonate_base"
        }
        extra = third_party - ALLOWED_THIRD_PARTY
        if extra:
            offenders[path.name] = sorted(extra)
    assert not offenders, f"base grew a dependency: {offenders}"
```

The tests iterate internally rather than parametrizing over files on purpose:
the suite's test count then stays fixed at 3 as modules land in base over the
next several tasks, so the "expected: N passed" line in every later task stays
meaningful.

- [ ] **Step 2: Run it**

```bash
uv run pytest packages/resonate-base -q
```

Expected: `3 passed`. Total suite is now `1004 passed`.

- [ ] **Step 3: Commit**

```bash
git add packages/resonate-base/tests
git commit -m "test: enforce the resonate-base dependency direction"
```

---

## Task 3: Add the import codemod

Roughly 100 files import the modules that are about to move. Hand-editing them is where this refactor goes wrong. This script is symbol-aware (it splits `resonate.types` and `resonate.connections` imports across two statements) and has been validated against the current tree: it rewrites 63 files and leaves the rest alone.

**Files:**
- Create: `scripts/codemod_base.py` (deleted again in Task 11)

- [ ] **Step 1: Write the codemod**

`scripts/codemod_base.py`:

```python
"""Rewrite ``resonate.*`` imports onto ``resonate_base`` / ``resonate_nats``.

Symbol-aware: ``resonate.types`` and ``resonate.connections`` are *split*
between packages, so a single ``from ... import a, b`` may fan out into two
statements. Every other moved module is a whole-module rename.

Temporary tooling for the resonate-base extraction; delete once the move
lands.

Usage:
    uv run python scripts/codemod_base.py --check   # report, change nothing
    uv run python scripts/codemod_base.py           # rewrite in place
"""

from __future__ import annotations

import argparse
import ast
import pathlib
import sys

ROOTS = ("src", "tests", "packages", "examples", "scripts")

# Whole-module renames: every symbol imported from the key moves to the value.
WHOLE: dict[str, str] = {
    "resonate.error": "resonate_base.error",
    "resonate.timing": "resonate_base.timing",
    "resonate.retry": "resonate_base.retry",
    "resonate.ids": "resonate_base.ids",
    "resonate.observability": "resonate_base.observability",
    "resonate.transport": "resonate_base.transport",
    "resonate.connections.nats": "resonate_nats",
}

# Split modules: symbol -> new module. Symbols absent from the table stay put.
SPLIT: dict[str, dict[str, str]] = {
    "resonate.types": {
        "Value": "resonate_base.types",
        "PromiseRecord": "resonate_base.types",
        "TaskRecord": "resonate_base.types",
        "ScheduleRecord": "resonate_base.types",
        "PromiseCreateReq": "resonate_base.types",
        "PromiseSettleReq": "resonate_base.types",
        "PromiseRegisterCallbackData": "resonate_base.types",
        "PromiseState": "resonate_base.types",
    },
    "resonate.connections": {
        "Network": "resonate_base.connections",
        "Source": "resonate_base.connections",
        "NatsConnection": "resonate_nats",
    },
    "resonate": {
        "PROTOCOL_VERSION": "resonate_base",
        "Clock": "resonate_base.timing",
        "Sleeper": "resonate_base.timing",
        "now_ms": "resonate_base.timing",
        "sleep": "resonate_base.timing",
    },
}


def target_module(module: str, name: str) -> str:
    if module in WHOLE:
        return WHOLE[module]
    return SPLIT.get(module, {}).get(name, module)


def render(indent: str, module: str, aliases: list[ast.alias]) -> str:
    names = ", ".join(
        a.name if a.asname is None else f"{a.name} as {a.asname}" for a in aliases
    )
    line = f"{indent}from {module} import {names}"
    if len(line) <= 88:
        return line + "\n"
    body = "".join(
        f"{indent}    {a.name if a.asname is None else f'{a.name} as {a.asname}'},\n"
        for a in aliases
    )
    return f"{indent}from {module} import (\n{body}{indent})\n"


def rewrite(path: pathlib.Path) -> str | None:
    src = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return None

    lines = src.splitlines(keepends=True)
    edits: list[tuple[int, int, str]] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or node.level:
            continue
        module = node.module or ""
        if module != "resonate" and not module.startswith("resonate."):
            continue
        if module not in WHOLE and module not in SPLIT:
            continue

        buckets: dict[str, list[ast.alias]] = {}
        for alias in node.names:
            buckets.setdefault(target_module(module, alias.name), []).append(alias)
        if list(buckets) == [module]:
            continue

        indent = " " * node.col_offset
        text = "".join(
            render(indent, mod, aliases) for mod, aliases in sorted(buckets.items())
        )
        edits.append((node.lineno - 1, node.end_lineno or node.lineno, text))

    if not edits:
        return None

    for start, end, text in sorted(edits, reverse=True):
        lines[start:end] = [text]
    return "".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    changed = 0
    for root in ROOTS:
        for path in sorted(pathlib.Path(root).rglob("*.py")):
            new = rewrite(path)
            if new is None:
                continue
            changed += 1
            if args.check:
                print(f"would rewrite {path}")
            else:
                path.write_text(new, encoding="utf-8")
                print(f"rewrote {path}")
    print(f"{changed} file(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Dry-run it against the untouched tree**

```bash
uv run python scripts/codemod_base.py --check | tail -1
```

Expected: `63 file(s)`. If the count differs, the tree has drifted from this plan — read the list before continuing.

- [ ] **Step 3: Commit**

```bash
git add scripts/codemod_base.py
git commit -m "chore: add temporary import codemod for the base extraction"
```

---

## Task 4: Move the leaf modules (`error`, `timing`, `retry`, `ids`, `observability`)

These five have no SDK dependencies at all (`error`, `timing`, `retry`, `observability` import nothing internal; `ids` imports `error`), so they move as whole files with no edits to their contents.

**Files:**
- Move: `src/resonate/{error,timing,retry,ids,observability}.py` → `packages/resonate-base/src/resonate_base/`
- Modify: `src/resonate/__init__.py`
- Modify: ~60 files, mechanically, via the codemod

- [ ] **Step 1: Move the files with git so history follows**

```bash
git mv src/resonate/error.py         packages/resonate-base/src/resonate_base/error.py
git mv src/resonate/timing.py        packages/resonate-base/src/resonate_base/timing.py
git mv src/resonate/retry.py         packages/resonate-base/src/resonate_base/retry.py
git mv src/resonate/ids.py           packages/resonate-base/src/resonate_base/ids.py
git mv src/resonate/observability.py packages/resonate-base/src/resonate_base/observability.py
```

- [ ] **Step 2: Fix the one internal import among them**

`packages/resonate-base/src/resonate_base/ids.py` line 27:

```python
from resonate_base.error import InvalidIdError
```

(was `from resonate.error import InvalidIdError`)

- [ ] **Step 3: Empty out the SDK's root module**

`src/resonate/__init__.py` becomes:

```python
"""Distributed Async Await by Resonate HQ, Inc.

The protocol layer this SDK is built on -- errors, injectable time, retry and
backoff policies, the observability event stream, the id format, the wire
records, and the ``Network``/``Source``/``Transport`` seams -- lives in
:mod:`resonate_base`, which connectors depend on without depending on the SDK.
"""

from __future__ import annotations
```

- [ ] **Step 4: Run the codemod**

```bash
uv run python scripts/codemod_base.py
uv run ruff check --fix
```

`ruff --fix` re-sorts the import blocks; with the `known-first-party` entry from Task 1 the moved imports merge into the existing first-party block instead of forming a second one.

- [ ] **Step 5: Verify no stale references remain**

```bash
grep -rn "from resonate\.\(error\|timing\|retry\|ids\|observability\) import" --include=*.py src tests packages examples
grep -rn "from resonate import " --include=*.py src tests packages examples
```

Expected: no output from either. (Docstring cross-references like ``:class:`~resonate.error.InvalidIdError` `` are still stale — Task 9 sweeps those.)

- [ ] **Step 6: Run the suite**

```bash
uv run pytest -q
uv run ruff check
uv run ty check
```

Expected: `1004 passed`, clean ruff, clean ty.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor: move error, timing, retry, ids and observability to resonate-base"
```

---

## Task 5: Split `types.py` into wire records and SDK types

`Value`, `PromiseRecord`, `TaskRecord`, `ScheduleRecord` and the three request structs describe the server's wire format — base. `Args`, `TaskData`, `Status`, `Info` and `DurableRegistry` describe how the SDK encodes *durable function invocations* into that format, and `DurableRegistry` references `Context` — SDK.

**Files:**
- Create: `packages/resonate-base/src/resonate_base/types.py`
- Modify: `src/resonate/types.py`

- [ ] **Step 1: Create the base wire types**

`packages/resonate-base/src/resonate_base/types.py`:

```python
"""The records exchanged with the Resonate server, in wire order."""

from __future__ import annotations

from typing import Any, Literal

import msgspec

PromiseState = Literal[
    "pending",
    "resolved",
    "rejected",
    "rejected_canceled",
    "rejected_timedout",
]


class Value(msgspec.Struct, omit_defaults=True, kw_only=True, frozen=True):
    """The wire format for data crossing the durability boundary.

    On the wire, ``data`` is a base64-encoded JSON string (or omitted).
    Internally, after decoding by the Codec, ``data`` holds the deserialized
    value; before encoding, it holds the plaintext value the
    :class:`~resonate.codec.Codec` will serialize.

    Both fields default to ``None`` and, with ``omit_defaults=True``, are left
    out of the encoded output entirely.

    Note: Python uses ``None`` for JSON ``null``, so an absent ``data`` field
    and an explicit ``null`` collapse to the same value.
    """

    headers: dict[str, str] | None = None
    data: Any | None = None


class PromiseRecord(msgspec.Struct, rename="camel", kw_only=True, frozen=True):
    """A durable promise record as stored by the server.

    ``kw_only=True`` lets ``timeout_at`` stay required even though it is
    declared after fields with defaults, keeping the declaration order aligned
    with the wire format. Defaulted fields are filled in when the server omits
    them.
    """

    id: str
    state: PromiseState
    param: Value = msgspec.field(default_factory=Value)
    value: Value = msgspec.field(default_factory=Value)
    tags: dict[str, str] = msgspec.field(default_factory=dict)
    timeout_at: int
    created_at: int = msgspec.field(default=0)
    settled_at: int | None = msgspec.field(default=None)


class TaskRecord(msgspec.Struct, rename="camel", kw_only=True, frozen=True):
    id: str
    state: Literal["pending", "acquired", "suspended", "halted", "fulfilled"]
    version: int
    resumes: list[str] | int | bool | None = msgspec.field(default=None)
    ttl: int | None = msgspec.field(default=None)
    pid: str | None = msgspec.field(default=None)


class ScheduleRecord(msgspec.Struct, rename="camel", kw_only=True, frozen=True):
    id: str
    cron: str
    promise_id: str
    promise_timeout: int
    promise_param: Value = msgspec.field(default_factory=Value)
    promise_tags: dict[str, str] = msgspec.field(default_factory=dict)
    created_at: int = msgspec.field(default=0)


class PromiseCreateReq(msgspec.Struct, rename="camel", kw_only=True, frozen=True):
    id: str
    timeout_at: int = msgspec.field(default=0)
    param: Value = msgspec.field(default_factory=Value)
    tags: dict[str, str] = msgspec.field(default_factory=dict)


class PromiseSettleReq(msgspec.Struct, rename="camel", kw_only=True, frozen=True):
    id: str
    state: Literal["resolved", "rejected", "rejected_canceled"]
    value: Value


class PromiseRegisterCallbackData(
    msgspec.Struct, rename="camel", kw_only=True, frozen=True
):
    awaited: str
    awaiter: str
```

- [ ] **Step 2: Reduce the SDK's `types.py` to the SDK-level types**

`src/resonate/types.py` becomes exactly:

```python
"""How the SDK encodes a durable function invocation into the wire records.

The records themselves -- :class:`~resonate_base.types.Value`,
:class:`~resonate_base.types.PromiseRecord` and friends -- live in
:mod:`resonate_base.types`. What is here is the layer above: the shape the SDK
packs into a promise's ``param`` (:class:`Args`, :class:`TaskData`), what it
reports back about a running invocation (:class:`Status`, :class:`Info`), and
the registration surface a worker exposes (:class:`DurableRegistry`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Protocol, overload

import msgspec

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Concatenate

    from resonate_base.retry import RetryPolicy

    from resonate.context import Context


class Args(msgspec.Struct, kw_only=True, frozen=True):
    args: tuple[Any, ...] = msgspec.field(default_factory=tuple)
    kwargs: dict[str, Any] = msgspec.field(default_factory=dict)


class TaskData(Args, kw_only=True, frozen=True):
    func: str
    version: int = msgspec.field(default=1)


Status = Literal["done", "suspended", "error"]


class Info(msgspec.Struct, frozen=True, kw_only=True):
    id: str
    parent_id: str
    origin_id: str
    branch_id: str
    timeout_at: int
    func_name: str
    tags: dict[str, str]


class DurableRegistry(Protocol):
    """Anywhere a durable function can be registered.

    The structural contract shared by :class:`resonate.resonate.Resonate`
    (the full client) and the serverless worker shims (e.g.
    :class:`resonate_aws.Resonate`). Code that only needs to *register*
    durable functions -- making them executable by pushed tasks, on a
    long-running worker and a serverless one alike -- accepts this protocol
    instead of a concrete client. Dispatching new runs (``run``/``rpc``)
    remains a full-client capability.

    The overloads mirror the implementations exactly (bare decorator and
    parameterized decorator), so both classes satisfy the protocol verbatim.
    """

    @overload
    def register[**P, T](
        self,
        fn: Callable[Concatenate[Context, P], T],
        *,
        name: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> Callable[Concatenate[Context, P], T]: ...
    @overload
    def register[**P, T](
        self,
        fn: None = None,
        *,
        name: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> Callable[
        [Callable[Concatenate[Context, P], T]], Callable[Concatenate[Context, P], T]
    ]: ...
```

- [ ] **Step 3: Run the codemod and re-sort imports**

```bash
uv run python scripts/codemod_base.py
uv run ruff check --fix
```

- [ ] **Step 4: Verify the split landed cleanly**

```bash
grep -rn "from resonate.types import" --include=*.py src tests packages examples \
  | grep -E "Value|PromiseRecord|TaskRecord|ScheduleRecord|PromiseCreateReq|PromiseSettleReq|PromiseRegisterCallbackData|PromiseState"
```

Expected: no output — every wire-record import now names `resonate_base.types`.

- [ ] **Step 5: Run the suite**

```bash
uv run pytest -q && uv run ruff check && uv run ty check
```

Expected: `1004 passed`, clean.

`tests/test_types.py` now imports from both modules; that is correct and expected — it covers both halves.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: move the wire records to resonate_base.types"
```

---

## Task 6: Move the `Network` / `Source` protocols

**Files:**
- Create: `packages/resonate-base/src/resonate_base/connections.py`
- Modify: `src/resonate/connections/__init__.py`

- [ ] **Step 1: Create the protocols module in base**

`packages/resonate-base/src/resonate_base/connections.py`:

```python
"""The two seams a Resonate connector implements."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["Network", "Source"]


@runtime_checkable
class Network(Protocol):
    """The request/response channel to the server.

    Every request Resonate issues (promise/task/schedule operations) flows
    through :meth:`send` as a JSON string and returns the server's JSON
    response. Methods raise on error.

    A :class:`Resonate <resonate.resonate.Resonate>` instance uses exactly one
    network, paired with one or more :class:`Source` push channels:
    ``Resonate(network=network, sources=[source, ...])``.

    Implementations ship separately from this package: the SDK's default
    transports (:class:`~resonate.connections.HttpConnection`,
    :class:`~resonate.connections.LocalConnection`) in ``resonate-sdk``, and
    one per connector elsewhere (:class:`resonate_nats.NatsConnection`, which
    is also a :class:`Source`).
    """

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, req: str) -> str: ...


@runtime_checkable
class Source(Protocol):
    """A push-message channel from the server.

    The server delivers ``execute``/``unblock`` messages to the addresses a
    source advertises: :meth:`unicast` reaches this process alone and
    :meth:`anycast` any member of its group. :meth:`target_resolver` maps a
    bare group name to an anycast address in this source's scheme, and
    :meth:`pid`/:meth:`group` expose the identity those addresses embed.
    Register receivers via :meth:`recv` **before** :meth:`start`; messages
    arrive as JSON strings.

    A :class:`Resonate <resonate.resonate.Resonate>` instance may listen on
    several sources at once; the first is the *primary* source, whose
    addresses are advertised for listener registration and target routing.

    Implementations ship separately from this package:
    :class:`~resonate.connections.SSEConnection` and
    :class:`~resonate.connections.LocalConnection` in ``resonate-sdk``,
    :class:`resonate_nats.NatsConnection` in ``resonate-nats``.
    """

    def pid(self) -> str: ...
    def group(self) -> str: ...
    def unicast(self) -> str: ...
    def anycast(self) -> str: ...
    def target_resolver(self, target: str) -> str: ...
    def recv(self, callback: Callable[[str], None]) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
```

- [ ] **Step 2: Reduce the SDK's connections package to its implementations**

`src/resonate/connections/__init__.py` becomes exactly:

```python
"""The transports the SDK ships with.

Each implements :class:`~resonate_base.connections.Network`,
:class:`~resonate_base.connections.Source`, or both. Connectors for other
substrates live in their own packages (``resonate-nats``) and depend only on
``resonate-base``.
"""

from __future__ import annotations

from resonate.connections.http import HttpConnection
from resonate.connections.local import LocalConnection
from resonate.connections.sse import SSEConnection

__all__ = [
    "HttpConnection",
    "LocalConnection",
    "SSEConnection",
]
```

Note this drops the `NatsConnection` import too; Task 8 moves that file. Between now and then, `src/resonate/connections/nats.py` is orphaned but still importable — that is fine and intentional (one concern per commit).

- [ ] **Step 3: Run the codemod and re-sort**

```bash
uv run python scripts/codemod_base.py
uv run ruff check --fix
```

- [ ] **Step 4: Verify**

```bash
grep -rn "from resonate.connections import" --include=*.py src tests packages examples | grep -E "\bNetwork\b|\bSource\b"
uv run pytest -q && uv run ruff check && uv run ty check
```

Expected: no grep output; `1004 passed`; clean.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: move the Network and Source protocols to resonate-base"
```

---

## Task 7: Move `transport.py`

With `connections`, `error`, `observability` and the wire types already in base, `transport.py` has no remaining SDK dependency and moves whole.

**Files:**
- Move: `src/resonate/transport.py` → `packages/resonate-base/src/resonate_base/transport.py`

- [ ] **Step 1: Move it**

```bash
git mv src/resonate/transport.py packages/resonate-base/src/resonate_base/transport.py
```

- [ ] **Step 2: Fix its own imports**

The header of `packages/resonate-base/src/resonate_base/transport.py` becomes:

```python
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import msgspec

from resonate_base.error import DecodingError, ServerError
from resonate_base.observability import Dropped, logging_observer
from resonate_base.types import PromiseRecord

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from resonate_base.connections import Network, Source
    from resonate_base.observability import Observer
```

- [ ] **Step 3: Update the one docstring that names the SDK**

In the `Transport` class docstring, `:class:`~resonate.connections.Network`` becomes `:class:`~resonate_base.connections.Network``. Same for the `Observer`/`Dropped` references in that docstring block.

- [ ] **Step 4: Run the codemod and re-sort**

```bash
uv run python scripts/codemod_base.py
uv run ruff check --fix
```

- [ ] **Step 5: Verify**

```bash
grep -rn "resonate.transport" --include=*.py src tests packages examples
uv run pytest -q && uv run ruff check && uv run ty check
```

Expected: no grep output; `1004 passed`; clean.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: move Transport and the message envelopes to resonate-base"
```

---

## Task 8: Relocate the base-only tests

Three test files now exercise only base code. They move to the package so `pytest packages/resonate-base` is a real, self-contained gate. Everything else stays in `tests/` — those files test SDK behaviour that happens to touch base types, and splitting them would fragment the suite for no benefit.

**Files:**
- Move: `tests/test_retry.py` → `packages/resonate-base/tests/test_retry.py`
- Move: `tests/test_error_pickle.py` → `packages/resonate-base/tests/test_error_pickle.py`
- Move: `tests/test_transport.py` → `packages/resonate-base/tests/test_transport.py`

- [ ] **Step 1: Confirm they really are base-only**

```bash
grep -n "^from\|^import" tests/test_retry.py tests/test_error_pickle.py tests/test_transport.py \
  | grep -v resonate_base | grep resonate
```

Expected: no output. If a line appears, that file is not base-only — leave it in `tests/` and note the deviation.

- [ ] **Step 2: Move them**

```bash
git mv tests/test_retry.py        packages/resonate-base/tests/test_retry.py
git mv tests/test_error_pickle.py packages/resonate-base/tests/test_error_pickle.py
git mv tests/test_transport.py    packages/resonate-base/tests/test_transport.py
```

- [ ] **Step 3: Verify the counts are conserved**

```bash
uv run pytest packages/resonate-base -q | tail -1
uv run pytest -q | tail -1
```

Expected: the package suite grows by exactly the number of tests those three files hold, and the overall total stays `1004 passed`. `testpaths = ["tests", "packages"]` in the root `pyproject.toml` already collects package tests, so no config change is needed.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test: move the base-only suites into packages/resonate-base"
```

---

## Task 9: Split out `resonate-nats`

**Files:**
- Create: `packages/resonate-nats/pyproject.toml`
- Create: `packages/resonate-nats/README.md`
- Create: `packages/resonate-nats/src/resonate_nats/py.typed`
- Create: `packages/resonate-nats/tests/test_layering.py`
- Move: `src/resonate/connections/nats.py` → `packages/resonate-nats/src/resonate_nats/__init__.py`
- Move: the NATS cases out of `tests/test_connection_internals.py` and `tests/test_id_format.py`
- Modify: root `pyproject.toml` (extra, source, coverage)

- [ ] **Step 1: Create the manifest**

`packages/resonate-nats/pyproject.toml`:

```toml
[project]
name = "resonate-nats"
version = "0.7.4"
description = "NATS connector for the Resonate SDK by Resonate HQ, Inc"
readme = "README.md"
authors = [{ name = "Resonate HQ, Inc", email = "contact@resonatehq.io" }]
requires-python = ">=3.12"
dependencies = [
    "nats-py>=2.6.0,<3",
    "resonate-base>=0.7.4,<0.8",
]

[project.urls]
Documentation = "https://github.com/resonatehq/resonate-sdk-py#readme"
Issues = "https://github.com/resonatehq/resonate-sdk-py/issues"
Source = "https://github.com/resonatehq/resonate-sdk-py"

[tool.uv.sources]
resonate-base = { workspace = true }

[build-system]
requires = ["uv_build>=0.11.7,<0.12.0"]
build-backend = "uv_build"

[tool.uv.build-backend]
module-name = "resonate_nats"
```

Note it depends on `resonate-base`, **not** on `resonate-sdk`. That is the entire point of the exercise: verifying this package installs without the SDK is the proof the seam works.

- [ ] **Step 2: Write the README**

`packages/resonate-nats/README.md`:

```markdown
# resonate-nats

NATS connector for Resonate.

`NatsConnection` is both a `Network` (request/reply to the Resonate server)
and a `Source` (subscriptions carrying `execute` / `unblock` messages), so it
can serve as a Resonate client's only connection.

```python
from resonate.resonate import Resonate
from resonate_nats import NatsConnection

conn = NatsConnection(servers=["nats://localhost:4222"], group="workers")
resonate = Resonate(network=conn, sources=[conn])
```

Install it with the SDK extra:

```bash
pip install resonate-nats
```

The package itself depends only on [`resonate-base`](https://pypi.org/project/resonate-base/)
and `nats-py`, never on `resonate-sdk`.
```

- [ ] **Step 3: Move the module**

```bash
mkdir -p packages/resonate-nats/src/resonate_nats packages/resonate-nats/tests
git mv src/resonate/connections/nats.py packages/resonate-nats/src/resonate_nats/__init__.py
touch packages/resonate-nats/src/resonate_nats/py.typed
```

Its two internal imports were already rewritten by the codemod in Task 4 and now read:

```python
from resonate_base.error import NatsError
from resonate_base.ids import origin_of
```

Add `__all__` at the top of the new `__init__.py`, below the module docstring, since it is now a package entry point:

```python
__all__ = ["NatsConnection"]
```

Leave `DEFAULT_API_PREFIX = "resonate.requests"` and `DEFAULT_RECV_PREFIX = "resonate.recv"` **unchanged** — those are NATS subject names the server subscribes to, not Python module paths.

- [ ] **Step 4: Point the SDK extra at the new package**

In the root `pyproject.toml`:

```toml
[project.optional-dependencies]
nats = ["resonate-nats>=0.7.4,<0.8"]
pydantic = ["pydantic>=2.0.0,<3"]
pydantic-ai = [
    "pydantic-ai-slim>=2.8,<3",
    "pydantic>=2.0.0,<3",
]
```

```toml
[tool.uv.sources]
resonate-base = { workspace = true }
resonate-nats = { workspace = true }
resonate-testing = { path = "packages/resonate-testing", editable = true }
```

```toml
[tool.coverage.run]
source = ["resonate", "resonate_aws", "resonate_base", "resonate_nats"]
```

**Superseded by Amendment C:** the extra is removed entirely; `pip install resonate-nats`.

- [ ] **Step 5: Move the NATS tests**

Two root test files mix NATS cases with non-NATS ones. The NATS material is contiguous in both, so this is a cut-and-paste, not a rewrite — preserve every test name and body verbatim so the suite total is conserved.

**From `tests/test_connection_internals.py`:** lines 278 to EOF (549) — the whole `#  NATS -- the client seam` section, comprising the `_FakeNatsClient` / `_FakeNatsSubscription` fakes and these 13 tests:

```
test_routing_origin_covers_every_request_kind
test_pid_group_and_addresses_derive_from_the_recv_prefix
test_start_without_a_registered_receiver_opens_no_subscriptions
test_start_with_a_receiver_subscribes_unicast_and_queued_anycast
test_stop_unsubscribes_every_subscription_and_clears_receivers
test_stop_swallows_an_unsubscribe_failure
test_send_publishes_the_origin_routed_subject_and_reply_header
test_send_before_start_raises_nats_error
test_send_after_stop_raises_nats_error_without_touching_the_client
test_send_wraps_a_publish_failure_in_nats_error
test_send_wraps_a_reply_timeout_in_nats_error
test_on_msg_dispatches_the_decoded_payload_to_every_subscriber
test_on_msg_drops_a_non_utf8_payload_without_raising
```

Then drop the now-unused `from resonate_nats import NatsConnection, _publish_subject, _routing_origin` and `from resonate_base.error import NatsError` lines from the top of that file, and delete the NATS bullet from its module docstring (currently lines 14-19).

**From `tests/test_id_format.py`:** the single test at the end of the file, `test_nats_routing_origin_splits_on_colon` (line 247), plus its `_id_to_origin` import.

The receiving file `packages/resonate-nats/tests/test_nats.py` starts:

```python
"""NATS connector internals -- the client seam, subject mapping, and headers.

``NatsConnection`` takes a structural ``NatsClient`` rather than a concrete
``nats.aio.client.Client``, so every branch below is reachable with in-memory
fakes and no broker.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from resonate_base.error import NatsError
from resonate_nats import (
    NatsConnection,
    _id_to_origin,
    _publish_subject,
    _routing_origin,
)
```

The copied `test_nats_routing_origin_splits_on_colon` compares `_id_to_origin` against a local port of the server's rule, defined at `tests/test_id_format.py:40`. Copy that helper into the new file rather than importing it across packages:

```python
def origin(id: str) -> str:
    """Return the origin, per the server's ``origin()``: text before the first ``:``."""
    return id.split(":", 1)[0]
```

- [ ] **Step 6: Add the connector layering guard**

`packages/resonate-nats/tests/test_layering.py`:

```python
"""resonate-nats depends on resonate-base, never on resonate-sdk.

This is the assertion that makes the base package worth having: a connector
must be buildable and releasable without the SDK. If this test ever fails, the
seam has leaked and the next connector will inherit the leak.
"""

from __future__ import annotations

import ast
import pathlib

SRC = pathlib.Path(__file__).parent.parent / "src" / "resonate_nats"

FORBIDDEN_ROOTS = {"resonate", "resonate_aws", "resonate_testing"}


def modules() -> list[pathlib.Path]:
    return sorted(SRC.rglob("*.py"))


def imported_roots(path: pathlib.Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and not node.level and node.module:
            roots.add(node.module.split(".")[0])
    return roots


def test_source_files_exist() -> None:
    assert modules(), f"no modules found under {SRC}"


def test_connector_does_not_import_the_sdk() -> None:
    offenders = {
        path.name: sorted(imported_roots(path) & FORBIDDEN_ROOTS)
        for path in modules()
        if imported_roots(path) & FORBIDDEN_ROOTS
    }
    assert not offenders, f"the connector reaches into the SDK: {offenders}"
```

- [ ] **Step 7: Sync and run**

```bash
uv sync --all-extras --all-packages
uv run pytest -q && uv run ruff check && uv run ty check
```

Expected: `1006 passed` (1004 plus the two new nats guard tests; the moved NATS cases keep their identity), clean ruff and ty.

- [ ] **Step 8: Prove the connector installs without the SDK**

This is the acceptance test for the whole plan.

```bash
uv build --package resonate-base
uv build --package resonate-nats
uv run --isolated --no-project \
  --with dist/resonate_nats-0.7.4-py3-none-any.whl \
  python -c "import resonate_nats, importlib.util; \
             assert importlib.util.find_spec('resonate') is None, 'sdk leaked in'; \
             print('resonate-nats installs standalone:', resonate_nats.NatsConnection)"
```

Expected: `resonate-nats installs standalone: <class 'resonate_nats.NatsConnection'>`. A failure here means something in `resonate_nats` or `resonate_base` still reaches into the SDK.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "refactor: extract the NATS connector into resonate-nats"
```

---

## Task 10: Sweep the docstring cross-references

73 Sphinx-style cross-references in `src/` still point at moved modules (``:class:`~resonate.error.ServerError` ``, ``:mod:`resonate.ids` ``, ...). This repo's docstrings are load-bearing documentation; stale refs are broken links.

**Files:**
- Modify: docstrings across `src/`, `packages/*/src/`

- [ ] **Step 1: Rewrite the unambiguous whole-module references**

```bash
grep -rl "resonate\.\(error\|timing\|retry\|ids\|observability\|transport\)" --include=*.py src packages \
  | xargs sed -i '' -E 's/\bresonate\.(error|timing|retry|ids|observability|transport)\b/resonate_base.\1/g'
```

(On Linux use `sed -i` without the `''`.)

This is safe because no *code* references those paths as strings — verified: the only runtime string constants that look like module paths are `logging.getLogger("resonate.validation")` in `observability.py` (which the pattern above does not match, since `validation` is not in the alternation) and the NATS subject prefixes.

- [ ] **Step 2: Re-check the validation logger survived**

```bash
grep -rn "resonate.validation" packages/resonate-base/src/resonate_base/observability.py
```

Expected: two hits — the comment and `logging.getLogger("resonate.validation")`. If either changed, restore them; the external validation harness greps that exact logger name.

- [ ] **Step 3: Hand-fix the split modules**

`resonate.types` and `resonate.connections` references must be judged one at a time, because both names still exist:

```bash
grep -rn "resonate\.types\.\|resonate\.connections\." --include=*.py src packages
```

For each hit: wire records → `resonate_base.types.X`; `Network`/`Source` → `resonate_base.connections.X`; `NatsConnection` → `resonate_nats.NatsConnection`; `Args`/`TaskData`/`Info`/`Status`/`DurableRegistry` and `HttpConnection`/`SSEConnection`/`LocalConnection` stay as they are.

- [ ] **Step 4: Verify and commit**

```bash
uv run pytest -q && uv run ruff check && uv run ty check
git add -A
git commit -m "docs: repoint docstring cross-references at resonate-base"
```

---

## Task 11: Update the user-facing docs and retire the codemod

**Files:**
- Modify: `README.md`
- Modify: `examples/**/__main__.py` (already rewritten by the codemod; verify)
- Modify: `packages/resonate-sdk-aws/README.md` (if it names moved paths)
- Delete: `scripts/codemod_base.py`

- [ ] **Step 1: Check the examples still run against a real server**

```bash
grep -rn "^from resonate" examples | sort -u
```

Expected: `resonate.resonate`, `resonate.context`, `resonate_base.retry`, `resonate_base.types` — the codemod handled `Never`/`Constant`/`Exponential`/`Value`.

```bash
# requires a local server; see .github/workflows/ci.yml
resonate serve --server-port 8080 &
RESONATE_URL=http://localhost:8080 just examples
```

Expected: every example completes without a traceback.

- [ ] **Step 2: Update the root README**

Add a short "Packages" section after the intro, and fix any import snippets that name moved modules:

```markdown
## Packages

| Package | Module | What it is |
|---|---|---|
| `resonate-sdk` | `resonate` | The SDK: durable functions, context, the HTTP/SSE/local transports. |
| `resonate-base` | `resonate_base` | Shared protocol layer: errors, timing, retry, wire records, `Network`/`Source`/`Transport`. Depends only on `msgspec`. |
| `resonate-nats` | `resonate_nats` | NATS connector. Install with `pip install resonate-nats`. |
| `resonate-sdk-aws` | `resonate_aws` | AWS Lambda worker shim. |

Connectors depend on `resonate-base` alone, never on `resonate-sdk`.
```

- [ ] **Step 3: Delete the codemod**

```bash
git rm scripts/codemod_base.py
```

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "docs: document the package split and drop the codemod"
```

---

## Task 12: Full release verification

Nothing in `.github/workflows/` needs editing — `ci.yml` runs `uv sync --all-extras --all-packages` and `cd.yml` runs `uv build --all-packages` with a `dist/*.whl` glob, both of which pick up new members automatically. This task proves that.

- [ ] **Step 1: Lockfile is current**

```bash
uv lock --check
```

Expected: no error. If it fails, run `uv lock` and commit the result.

- [ ] **Step 2: Full local CI**

```bash
uv sync --locked --all-extras --all-packages
uv run ruff check
uv run ty check
uv run pytest --cov --cov-branch -q
```

Expected: clean lint, clean types, `1006 passed`, and the coverage report listing `resonate`, `resonate_aws`, `resonate_base`, `resonate_nats`.

- [ ] **Step 3: Build every wheel and test against the artifacts**

This mirrors the `cd.yml` step that catches bad inter-package version ranges.

```bash
uv build --all-packages
ls dist/
uv pip install --reinstall dist/*.whl
uv run --no-sync pytest -q
```

Expected: `dist/` holds six sdists+wheels pairs (`resonate_sdk`, `resonate_base`, `resonate_nats`, `resonate_sdk_aws`), the install resolves without reaching PyPI for a workspace member, and the suite passes.

- [ ] **Step 4: Version bump for the breaking release**

The moved import paths are breaking, so all four packages go to `0.8.0` together and the ranges widen accordingly:

- root `pyproject.toml`: `version = "0.8.0"`, `resonate-base>=0.8.0,<0.9`, `nats = ["resonate-nats>=0.8.0,<0.9"]`
- `packages/resonate-base/pyproject.toml`: `version = "0.8.0"`
- `packages/resonate-nats/pyproject.toml`: `version = "0.8.0"`, `resonate-base>=0.8.0,<0.9`
- `packages/resonate-sdk-aws/pyproject.toml`: `version = "0.8.0"`, `resonate-sdk>=0.8.0,<0.9`

```bash
uv lock
uv run pytest -q
```

`scripts/new-release.py` reads the root version only and needs no change; `cd.yml` verifies the root version matches the tag and `--check-url` skips any member already on PyPI.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "build: release 0.8.0 with the resonate-base package split"
```

---

## Verification matrix

| Property | Enforced by |
|---|---|
| base imports nothing above it | `packages/resonate-base/tests/test_layering.py` |
| base pulls in no unexpected third-party dep | same file, second test |
| the NATS connector never imports the SDK | `packages/resonate-nats/tests/test_layering.py` |
| the connector installs and works without the SDK | Task 9 Step 8 (`uv run --isolated --no-project`) |
| behaviour is unchanged | 1001 pre-existing tests, green after every task |
| inter-package version ranges are satisfiable | Task 12 Step 3 (`uv pip install dist/*.whl`), mirrored in `cd.yml` |
| import blocks stay tidy | `ruff check` with `known-first-party` from Task 1 |

## Risks

| Risk | Mitigation |
|---|---|
| Cross-version pickled exceptions lose their type during a rolling upgrade | Already degrades to `ApplicationError(message)`; document in the 0.8.0 release notes. |
| A future connector quietly imports `resonate` and re-couples the layers | The per-package layering guard tests; copy `test_layering.py` into each new connector package. |
| `resonate_base` drifts into a dumping ground | The third-party allow-list in the guard test is a natural chokepoint: anything needing a new dependency does not belong in base. |
| Downstream users break on the moved import paths | Deliberate; ship as 0.8.0 with a release-note table of old path → new path. Decision 3 (a curated `resonate` façade) remains available as a follow-up if the churn proves unpopular. |
