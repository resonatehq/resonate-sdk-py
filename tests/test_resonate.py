from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from resonate import Context, Resonate
from resonate.models.commands import Invoke, Listen
from resonate.models.options import Options
from resonate.registry import Registry

if TYPE_CHECKING:
    from collections.abc import Generator


def foo(ctx: Context, a: int, b: int) -> int: ...
def bar(a: int, b: int) -> int: ...
def baz(ctx: Context, a: int, b: int) -> Generator[Any, Any, int]: ...


@pytest.fixture
def registry() -> Registry:
    registry = Registry()
    registry.add("foo", foo)
    registry.add("bar", bar)
    registry.add("baz", baz)
    return registry


@pytest.fixture
def scheduler() -> MagicMock:
    mock_scheduler = MagicMock()

    def enqueue_side_effect(*args: Any, **kwargs: Any) -> None:
        if futures := kwargs.get("futures"):
            futures[0].set_result(None)  # Unblock future

    mock_scheduler.enqueue.side_effect = enqueue_side_effect
    return mock_scheduler


@pytest.fixture
def resonate(registry: Registry, scheduler: MagicMock) -> Resonate:
    return Resonate(registry=registry, scheduler=scheduler)


# Helper to validate Invoke parameters
def cmd(mock_scheduler: MagicMock) -> None:
    mock_scheduler.enqueue.assert_called_once()
    args, kwargs = mock_scheduler.enqueue.call_args

    mock_scheduler.reset_mock()
    return args[0]


# Parametrized tests


@pytest.mark.parametrize("func", [foo, bar, baz])
@pytest.mark.parametrize("name", ["foo", "bar", "baz", None])
def test_register(func: Callable, name: str | None) -> None:
    registry = Registry()
    resonate = Resonate(registry=registry)

    resonate.register(func, name=name)
    assert registry.get(name or func.__name__) == func
    assert registry.reverse_lookup(func) == name or func.__name__


@pytest.mark.parametrize("send_to", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("version", [1, 2, 3, None])
@pytest.mark.parametrize("timeout", [3, 2, 1, None])
@pytest.mark.parametrize(
    ("func", "args", "kwargs", "expected_name", "expected_func"),
    [
        (foo, (1, 2), {}, "foo", foo),
        (bar, (1, 2), {}, "bar", bar),
        (baz, (1, 2), {}, "baz", baz),
        (foo, (), {"1": 1, "2": 2}, "foo", foo),
        (bar, (), {"1": 1, "2": 2}, "bar", bar),
        (baz, (), {"1": 1, "2": 2}, "baz", baz),
        ("foo", (1, 2), {}, "foo", foo),
        ("bar", (1, 2), {}, "bar", bar),
        ("baz", (1, 2), {}, "baz", baz),
        ("foo", (), {"1": 1, "2": 2}, "foo", foo),
        ("bar", (), {"1": 1, "2": 2}, "bar", bar),
        ("baz", (), {"1": 1, "2": 2}, "baz", baz),
    ],
)
def test_run(
    resonate: Resonate,
    scheduler: MagicMock,
    send_to: str | None,
    version: int | None,
    timeout: int | None,
    func: Callable | str,
    args: tuple,
    kwargs: dict,
    expected_name: str,
    expected_func: Callable,
) -> None:
    opts = Options()
    if send_to is not None:
        opts = opts.merge(send_to=send_to)
    if version is not None:
        opts = opts.merge(version=version)

    invoke = Invoke(id="f", name=expected_name, func=expected_func, args=args, kwargs=kwargs)
    invoke_with_opts = Invoke(id="f", name=expected_name, func=expected_func, args=args, kwargs=kwargs, opts=opts)

    resonate.run("f", func, *args, **kwargs)
    assert cmd(scheduler) == invoke

    resonate.options(**opts.to_dict()).run("f", func, *args, **kwargs)
    assert cmd(scheduler) == invoke_with_opts

    if isinstance(func, Callable):
        f = resonate.register(func)
        f.run("f", *args, **kwargs)
        assert cmd(scheduler) == invoke

        f = resonate.register(func, **opts.to_dict())
        f.run("f", *args, **kwargs)
        assert cmd(scheduler) == invoke_with_opts

        f = resonate.register(func)
        f.options(**opts.to_dict()).run("f", *args, **kwargs)
        assert cmd(scheduler) == invoke_with_opts


@pytest.mark.parametrize("send_to", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("version", [1, 2, 3, None])
@pytest.mark.parametrize(
    ("func", "args", "kwargs", "expected_name", "expected_func"),
    [
        (foo, (1, 2), {}, "foo", None),
        (bar, (1, 2), {}, "bar", None),
        (baz, (1, 2), {}, "baz", None),
        (foo, (), {"1": 1, "2": 2}, "foo", None),
        (bar, (), {"1": 1, "2": 2}, "bar", None),
        (baz, (), {"1": 1, "2": 2}, "baz", None),
        ("foo", (1, 2), {}, "foo", None),
        ("bar", (1, 2), {}, "bar", None),
        ("baz", (1, 2), {}, "baz", None),
        ("foo", (), {"1": 1, "2": 2}, "foo", None),
        ("bar", (), {"1": 1, "2": 2}, "bar", None),
        ("baz", (), {"1": 1, "2": 2}, "baz", None),
    ],
)
def test_rpc(
    resonate: Resonate,
    scheduler: MagicMock,
    send_to: str | None,
    version: int | None,
    func: Callable | str,
    args: tuple,
    kwargs: dict,
    expected_name: str,
    expected_func: Callable,
) -> None:
    opts = Options()
    if send_to is not None:
        opts = opts.merge(send_to=send_to)
    if version is not None:
        opts = opts.merge(version=version)

    invoke = Invoke(id="f", name=expected_name, func=expected_func, args=args, kwargs=kwargs)
    invoke_with_opts = Invoke(id="f", name=expected_name, func=expected_func, args=args, kwargs=kwargs, opts=opts)

    resonate.rpc("f", func, *args, **kwargs)
    assert cmd(scheduler) == invoke

    resonate.options(**opts.to_dict()).rpc("f", func, *args, **kwargs)
    assert cmd(scheduler) == invoke_with_opts

    if isinstance(func, Callable):
        f = resonate.register(func)
        f.rpc("f", *args, **kwargs)
        assert cmd(scheduler) == invoke

        f = resonate.register(func, **opts.to_dict())
        f.rpc("f", *args, **kwargs)
        assert cmd(scheduler) == invoke_with_opts

        f = resonate.register(func)
        f.options(**opts.to_dict()).rpc("f", *args, **kwargs)
        assert cmd(scheduler) == invoke_with_opts


@pytest.mark.parametrize("id", ["foo", "bar", "baz"])
def test_get(resonate: Resonate, scheduler: MagicMock, id: str) -> None:
    resonate.get(id)
    assert cmd(scheduler) == Listen(id=id)


@pytest.mark.parametrize(
    "func",
    [foo, bar, baz],
)
def test_signatures(resonate: Resonate, func: Callable) -> None:
    f = resonate.register(func)
    assert f.rpc.__annotations__ == f.run.__annotations__
