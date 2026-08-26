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
