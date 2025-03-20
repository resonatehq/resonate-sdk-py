from __future__ import annotations

from typing import TYPE_CHECKING

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
