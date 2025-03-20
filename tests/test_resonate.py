from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from resonate import Context, Resonate
from resonate.models.commands import Invoke, Listen
from resonate.models.options import Options
from resonate.registry import Registry

if TYPE_CHECKING:
    from collections.abc import Callable, Generator


def foo(ctx: Context, a: int, b: int) -> int: ...
def bar(a: int, b: int) -> int: ...
def baz(ctx: Context, a: int, b: int) -> Generator[Any, Any, int]: ...


@pytest.fixture
def scheduler() -> MagicMock:
    mock_scheduler = MagicMock()

    def enqueue_side_effect(*args: Any, **kwargs: Any) -> None:
        if futures := kwargs.get("futures"):
            futures[0].set_result(None)  # Unblock future

    mock_scheduler.enqueue.side_effect = enqueue_side_effect
    return mock_scheduler


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
    assert registry.get(name or func.__name__) == (func, 1)
    assert registry.reverse_lookup(func) == (name or func.__name__, 1)


@pytest.mark.parametrize("send_to", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("version", [1, 2, 3, None])
@pytest.mark.parametrize("timeout", [3, 2, 1, None])
@pytest.mark.parametrize(
    ("func", "name", "args", "kwargs"),
    [
        (foo, "foo", (1, 2), {}),
        (bar, "bar", (1, 2), {}),
        (baz, "baz", (1, 2), {}),
        (foo, "foo", (), {"1": 1, "2": 2}),
        (bar, "bar", (), {"1": 1, "2": 2}),
        (baz, "baz", (), {"1": 1, "2": 2}),
    ],
)
def test_run(
    scheduler: MagicMock,
    send_to: str | None,
    version: int | None,
    timeout: int | None,
    func: Callable,
    name: str,
    args: tuple,
    kwargs: dict,
) -> None:
    registry = Registry()
    resonate = Resonate(registry=registry, scheduler=scheduler)

    f1 = resonate.register(func, name=name, version=version or 1)

    opts = Options(version=1)
    if send_to is not None:
        opts = opts.merge(send_to=send_to)
    if version is not None:
        opts = opts.merge(version=version)
    if timeout is not None:
        opts = opts.merge(timeout=timeout)

    invoke = Invoke(id="f", name=name, func=func, args=args, kwargs=kwargs, opts=Options(version=version or 1))
    invoke_with_opts = Invoke(id="f", name=name, func=func, args=args, kwargs=kwargs, opts=opts)

    resonate.run("f", func, *args, **kwargs)
    assert cmd(scheduler) == invoke

    resonate.run("f", name, *args, **kwargs)
    assert cmd(scheduler) == invoke

    resonate.options(**opts.to_dict()).run("f", func, *args, **kwargs)
    assert cmd(scheduler) == invoke_with_opts

    resonate.options(**opts.to_dict()).run("f", name, *args, **kwargs)
    assert cmd(scheduler) == invoke_with_opts

    f1.run("f", *args, **kwargs)
    assert cmd(scheduler) == invoke

    f1.options(**opts.to_dict()).run("f", *args, **kwargs)
    assert cmd(scheduler) == invoke_with_opts

    version = (version or 1) + 1
    f2 = resonate.register(func, name=name, send_to=send_to, version=version, timeout=timeout)
    invoke_with_opts.opts = invoke_with_opts.opts.merge(version=version)

    f2.run("f", *args, **kwargs)
    assert cmd(scheduler) == invoke_with_opts

    f2.options(**opts.to_dict()).run("f", *args, **kwargs)
    assert cmd(scheduler) == invoke_with_opts


@pytest.mark.parametrize("send_to", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("version", [1, 2, 3, None])
@pytest.mark.parametrize("timeout", [3, 2, 1, None])
@pytest.mark.parametrize(
    ("func", "name", "args", "kwargs"),
    [
        (foo, "foo", (1, 2), {}),
        (bar, "bar", (1, 2), {}),
        (baz, "baz", (1, 2), {}),
        (foo, "foo", (), {"1": 1, "2": 2}),
        (bar, "bar", (), {"1": 1, "2": 2}),
        (baz, "baz", (), {"1": 1, "2": 2}),
    ],
)
def test_rpc(
    scheduler: MagicMock,
    send_to: str | None,
    version: int | None,
    timeout: int | None,
    func: Callable,
    name: str,
    args: tuple,
    kwargs: dict,
) -> None:
    registry = Registry()
    resonate = Resonate(registry=registry, scheduler=scheduler)

    f1 = resonate.register(func, name=name, version=version or 1)

    opts = Options(version=1)
    if send_to is not None:
        opts = opts.merge(send_to=send_to)
    if version is not None:
        opts = opts.merge(version=version)
    if timeout is not None:
        opts = opts.merge(timeout=timeout)

    invoke = Invoke(id="f", name=name, func=None, args=args, kwargs=kwargs, opts=Options(version=version or 1))
    invoke_with_opts = Invoke(id="f", name=name, func=None, args=args, kwargs=kwargs, opts=opts)

    resonate.rpc("f", func, *args, **kwargs)
    assert cmd(scheduler) == invoke

    resonate.rpc("f", name, *args, **kwargs)
    assert cmd(scheduler) == invoke

    resonate.options(**opts.to_dict()).rpc("f", func, *args, **kwargs)
    assert cmd(scheduler) == invoke_with_opts

    resonate.options(**opts.to_dict()).rpc("f", name, *args, **kwargs)
    assert cmd(scheduler) == invoke_with_opts

    f1.rpc("f", *args, **kwargs)
    assert cmd(scheduler) == invoke

    f1.options(**opts.to_dict()).rpc("f", *args, **kwargs)
    assert cmd(scheduler) == invoke_with_opts

    version = (version or 1) + 1
    f2 = resonate.register(func, name=name, send_to=send_to, version=version, timeout=timeout)
    invoke_with_opts.opts = invoke_with_opts.opts.merge(version=version)

    f2.rpc("f", *args, **kwargs)
    assert cmd(scheduler) == invoke_with_opts

    f2.options(**opts.to_dict()).rpc("f", *args, **kwargs)
    assert cmd(scheduler) == invoke_with_opts


@pytest.mark.parametrize("id", ["foo", "bar", "baz"])
def test_get(scheduler: MagicMock, id: str) -> None:
    resonate = Resonate(scheduler=scheduler)
    resonate.get(id)
    assert cmd(scheduler) == Listen(id=id)


@pytest.mark.parametrize(
    "func",
    [foo, bar, baz],
)
def test_signatures(func: Callable) -> None:
    resonate = Resonate()
    f = resonate.register(func, send_to="foo", version=1, timeout=2)
    assert f.rpc.__annotations__ == f.run.__annotations__
