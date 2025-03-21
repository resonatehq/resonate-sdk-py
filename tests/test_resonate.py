from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, assert_type
from unittest.mock import MagicMock

import pytest

from resonate import Context, Resonate
from resonate.errors import ResonateValidationError
from resonate.models.commands import Invoke, Listen
from resonate.models.handle import Handle
from resonate.models.options import Options
from resonate.registry import Registry
from resonate.resonate import Function

if TYPE_CHECKING:
    from collections.abc import Generator


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
    assert registry.get(func) == (name or func.__name__, 1)


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

    opts = opts.merge(version=version)
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

    opts = opts.merge(version=version)
    f2.options(**opts.to_dict()).rpc("f", *args, **kwargs)
    assert cmd(scheduler) == invoke_with_opts


@pytest.mark.parametrize("id", ["foo", "bar", "baz"])
def test_get(scheduler: MagicMock, id: str) -> None:
    resonate = Resonate(scheduler=scheduler)
    resonate.get(id)
    assert cmd(scheduler) == Listen(id=id)


def test_type_annotations() -> None:
    # The following are "tests", if there is an issue it will be found by pright, at runtime
    # assert_type is effectively a noop.

    resonate = Resonate()

    # foo
    def foo(ctx: Context, a: int, b: int, /) -> int: ...

    f = resonate.register(foo)
    assert_type(f, Function[[int, int], int])
    assert_type(f.run, Callable[[str, int, int], Handle[int]])
    assert_type(f.rpc, Callable[[str, int, int], Handle[int]])

    # bar
    def bar(ctx: Context, a: str, b: str, /) -> str: ...

    f = resonate.register(bar)
    assert_type(f, Function[[str, str], str])
    assert_type(f.run, Callable[[str, str, str], Handle[str]])
    assert_type(f.rpc, Callable[[str, str, str], Handle[str]])

    # baz
    def baz(ctx: Context, a: int, b: str, /) -> int | str: ...

    f = resonate.register(baz)
    assert_type(f, Function[[int, str], int | str])
    assert_type(f.run, Callable[[str, int, str], Handle[int | str]])
    assert_type(f.rpc, Callable[[str, int, str], Handle[int | str]])


# Input validation tests


@pytest.mark.parametrize(
    "kwargs",
    [
        {"timeout": 1, "version": -1},
        {"timeout": -1, "version": 1},
    ],
)
def test_instantiate_options_invalid_args(kwargs: dict) -> None:
    with pytest.raises(ResonateValidationError):
        Options(**kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"timeout": 1, "version": -1},
        {"timeout": -1, "version": 1},
    ],
)
def test_merge_options_invalid_args(kwargs: dict) -> None:
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
    def foo(ctx: Context) -> None: ...

    resonate = Resonate()
    with pytest.raises(ResonateValidationError):
        resonate.run("foo", foo)


def test_run_missing_version() -> None:
    def foo(ctx: Context) -> None: ...

    resonate = Resonate()
    resonate.register(foo, version=1)
    with pytest.raises(ResonateValidationError):
        resonate.options(version=2).run("foo", foo)


def test_rpc_unregistered_function() -> None:
    def foo(ctx: Context) -> None: ...

    resonate = Resonate()
    with pytest.raises(ResonateValidationError):
        resonate.rpc("foo", foo)


def test_rpc_missing_version() -> None:
    def foo(ctx: Context) -> None: ...

    resonate = Resonate()
    resonate.register(foo, version=1)
    with pytest.raises(ResonateValidationError):
        resonate.options(version=2).rpc("foo", foo)


def test_reregister_function_same_version() -> None:
    resonate = Resonate()

    @resonate.register(version=1)
    def foo(ctx: Context) -> None: ...

    with pytest.raises(ResonateValidationError):
        resonate.register(foo, version=1)


def test_reregister_function_different_version() -> None:
    resonate = Resonate()

    @resonate.register(version=1)
    def foo(ctx: Context) -> None: ...

    resonate.register(foo, version=2)


def test_register_same_function_with_different_name() -> None:
    resonate = Resonate()

    def foo(ctx: Context) -> None: ...

    resonate.register(foo, name="bar", version=1)
    with pytest.raises(ResonateValidationError):
        resonate.register(foo, name="baz", version=2)
