"""Tests for :class:`resonate.registry.Registry`.

Mirrors Go's ``registry_test.go``: explicit-name registration backed by
reflection-built :class:`~resonate.durable.DurableFunction` entries. The lookup
*name* is supplied by the caller (so it stays stable across renames of the
Python function); the registered callable must follow the Python SDK
convention of accepting a :class:`Context` as its first argument.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from resonate.error import AlreadyRegisteredError, ApplicationError
from resonate.registry import Registry

if TYPE_CHECKING:
    from resonate.context import Context


async def leaf(ctx: Context, x: int) -> int:
    return x


async def flow(ctx: Context, x: int) -> int:
    return x


def test_register_and_get() -> None:
    r = Registry()
    r.register("leaf", leaf)
    df = r.get("leaf")
    assert df is not None
    assert df.name == "leaf"


def test_custom_name_is_independent_of_fn_name() -> None:
    r = Registry()
    r.register("custom", leaf)
    assert r.contains("custom")
    assert not r.contains("leaf")
    df = r.get("custom")
    assert df is not None
    assert df.name == "leaf"  # the entry still remembers its source name


def test_get_unknown_returns_none() -> None:
    assert Registry().get("missing") is None


def test_contains_unknown_is_false() -> None:
    assert not Registry().contains("missing")


def test_empty_name_rejected() -> None:
    with pytest.raises(ApplicationError, match="name is required"):
        Registry().register("", leaf)


def test_duplicate_name_rejected() -> None:
    r = Registry()
    r.register("dup", leaf)
    with pytest.raises(AlreadyRegisteredError, match="dup"):
        r.register("dup", flow)


def test_register_non_callable_rejected() -> None:
    not_callable: Any = 123
    with pytest.raises(ApplicationError, match="expected a callable"):
        Registry().register("bad", not_callable)


def test_names_and_len() -> None:
    r = Registry()
    r.register("a", leaf)
    r.register("b", flow)
    assert sorted(r.names()) == ["a", "b"]
    assert len(r) == 2


def test_empty_registry_len_is_zero() -> None:
    assert len(Registry()) == 0
