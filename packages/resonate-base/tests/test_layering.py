"""resonate-base must not depend on anything above it.

The value of the package is the direction of the arrow: the SDK and every
connector may import base, and base may import nothing but the standard
library. A single stray ``from resonate.codec import Codec`` silently turns
base into a second copy of the SDK, so the rule is asserted rather than
documented.

This guards *base*, which is the only package whose dependency set is closed by
design. A connector is free to depend on whatever its substrate needs, and
nothing here is imposed on one.
"""

from __future__ import annotations

import ast
import pathlib
import sys

SRC = pathlib.Path(__file__).parent.parent / "src" / "resonate_base"

#: Packages that sit above base in the dependency graph.
FORBIDDEN_ROOTS = {"resonate", "resonate_aws", "resonate_nats", "resonate_testing"}


def modules() -> list[pathlib.Path]:
    return sorted(SRC.rglob("*.py"))


def imported_roots(path: pathlib.Path) -> set[str]:
    """Return the top-level package of every absolute import in ``path``.

    Parsed rather than imported: a module reaching into the SDK inside a
    lazily-executed branch is still a violation, and importing it to find out
    would need the SDK installed -- the very thing being disproved.
    """
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
        path.name: sorted(roots)
        for path in modules()
        if (roots := imported_roots(path) & FORBIDDEN_ROOTS)
    }
    assert not offenders, f"base imports the layers above it: {offenders}"


def test_base_has_no_third_party_dependencies_at_all() -> None:
    """Base's ``pyproject.toml`` declares ``dependencies = []``. Keep it true.

    An empty dependency set is what lets a connector install base without
    inheriting an opinion about serialization, HTTP, or anything else.
    """
    offenders = {}
    for path in modules():
        extra = {
            root
            for root in imported_roots(path)
            if root not in sys.stdlib_module_names and root != "resonate_base"
        }
        if extra:
            offenders[path.name] = sorted(extra)
    assert not offenders, f"base grew a dependency: {offenders}"
