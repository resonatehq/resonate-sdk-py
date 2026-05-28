from __future__ import annotations

import msgspec
import pytest

from resonate import DependencyMap
from resonate.info import Info


class Config(msgspec.Struct, frozen=True, kw_only=True):
    value: str


class Counter(msgspec.Struct, frozen=True, kw_only=True):
    count: int


# -- DependencyMap ------------------------------------------------------------


def test_insert_and_get() -> None:
    deps = DependencyMap()
    deps.insert(Config(value="hello"))
    assert deps.get(Config).value == "hello"


def test_insert_overwrites_same_type() -> None:
    deps = DependencyMap()
    deps.insert(Config(value="first"))
    deps.insert(Config(value="second"))
    assert deps.get(Config).value == "second"


def test_multiple_dependencies_keyed_by_type() -> None:
    deps = DependencyMap()
    deps.insert(Config(value="multi"))
    deps.insert(Counter(count=42))
    assert deps.get(Config).value == "multi"
    assert deps.get(Counter).count == 42


def test_get_missing_raises() -> None:
    deps = DependencyMap()
    with pytest.raises(KeyError, match="with_dependency"):
        deps.get(Config)


def test_repr_reports_len() -> None:
    deps = DependencyMap()
    assert repr(deps) == "DependencyMap(len=0)"
    deps.insert(Config(value="x"))
    assert repr(deps) == "DependencyMap(len=1)"


# -- Info ---------------------------------------------------------------------


def info(deps: DependencyMap | None = None) -> Info:
    return Info(
        id="id",
        parent_id="parent",
        origin_id="origin",
        branch_id="branch",
        timeout_at=123,
        func_name="func",
        tags={"k": "v"},
    )


def test_info_fields() -> None:
    i = info()
    assert i.id == "id"
    assert i.parent_id == "parent"
    assert i.origin_id == "origin"
    assert i.branch_id == "branch"
    assert i.timeout_at == 123
    assert i.func_name == "func"
    assert i.tags == {"k": "v"}
