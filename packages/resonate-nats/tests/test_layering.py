"""resonate-nats depends on resonate-base, never on resonate-sdk.

This is the assertion that makes the base package worth having: a connector
must be buildable and releasable without the SDK. If it ever fails, the seam
has leaked.

It is a test *this* package chose to write, not a rule base imposes -- a
connector is free to depend on whatever it likes, including the SDK, if that is
the right trade for it.
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
        path.name: sorted(roots)
        for path in modules()
        if (roots := imported_roots(path) & FORBIDDEN_ROOTS)
    }
    assert not offenders, f"the connector reaches into the SDK: {offenders}"
