from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from resonate.errors import ResonateValidationError
from resonate.models.options import Options
from resonate.registry import Registry
from resonate.resonate import Resonate

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.models.context import Context


def foo(ctx: Context) -> None: ...


def _register_function_twice() -> None:
    registry = Registry()
    registry.add(lambda: 2, "foo", 1)
    registry.add(lambda: 2, "foo", 1)


def _get_function_with_negative_version() -> None:
    registry = Registry()
    registry.get("foo", -1)


def _get_unregistered_function() -> None:
    registry = Registry()
    registry.get("foo", 1)


def _get_non_existing_version() -> None:
    registry = Registry()
    registry.add(lambda: 2, "foo", 1)
    registry.get("foo", 3)


@pytest.mark.parametrize(
    "check",
    [
        lambda: Options(version=-1),
        lambda: Options(timeout=-1),
        lambda: Options().merge(version=-1),
        lambda: Options().merge(timeout=-1),
        lambda: Resonate().register(lambda _: 2),
        lambda: Resonate().register(lambda _: 2, name="foo", version=-1),
        lambda: Resonate().register(foo, version=-1),
        lambda: Registry().add(lambda: 2, "foo", -1),
        _register_function_twice,
        _get_unregistered_function,
        _get_non_existing_version,
    ],
)
def test_validations(check: Callable[[], None]) -> None:
    with pytest.raises(ResonateValidationError):
        check()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"timeout": 1, "version": -1},
        {"timeout": -1, "version": 1},
    ],
)
def test_instantiate_options_invalid_args(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ResonateValidationError):
        Options(**kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"timeout": 1, "version": -1},
        {"timeout": -1, "version": 1},
    ],
)
def test_merge_options_invalid_args(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ResonateValidationError):
        Options().merge(**kwargs)

def test_register_lambda_without_name() -> None:
    with pytest.raises(ResonateValidationError):
        Resonate().register(lambda _: 2)


def test_register_lambda_with_name() -> None:
    Resonate().register(lambda _: 2, name="foo")

def test_register_with_negative_version() -> None:
    with pytest.raises(ResonateValidationError):
        Resonate().register(lambda _: 2, name="foo", version=-1)

def test_run_unregistered_function() -> None:
    def foo(ctx: Context) -> None:...
    resonate = Resonate()
    with pytest.raises(ResonateValidationError):
        resonate.run("foo", foo)


def test_run_missing_version() -> None:
    def foo(ctx: Context) -> None:...
    resonate = Resonate()
    resonate.register(foo, version=1)
    # resonate.run("foo", foo)


def test_rpc_unregistered_function() -> None:
    def foo(ctx: Context) -> None:...
    resonate = Resonate()
    with pytest.raises(ResonateValidationError):
        resonate.rpc("foo", foo)
