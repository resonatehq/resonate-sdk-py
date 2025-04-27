from __future__ import annotations

import sys
from collections.abc import Callable
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, assert_type

import pytest

from resonate import Context, Resonate
from resonate.dependencies import Dependencies
from resonate.errors import ResonateValidationError
from resonate.models.commands import Command, Invoke, Listen
from resonate.models.handle import Handle
from resonate.options import Options
from resonate.registry import Registry
from resonate.resonate import Function
from resonate.retry_policies import Constant, Exponential, Linear, Never
from resonate.scheduler import Info

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.models.retry_policy import RetryPolicy


def foo(ctx: Context, a: int, b: int) -> int: ...
def bar(ctx: Context, a: int, b: int) -> int: ...
def baz(ctx: Context, a: int, b: int) -> Generator[Any, Any, int]:
    yield ctx.lfc(bar, a, b)
    raise NotImplementedError


# Fixtures


@pytest.fixture
def resonate() -> Resonate:
    resonate = Resonate()
    resonate._started = True  # noqa: SLF001
    return resonate


@pytest.fixture
def registry() -> Registry:
    registry = Registry()
    registry.add(foo, "foo", version=1)
    registry.add(foo, "foo", version=2)
    registry.add(bar, "bar", version=1)
    return registry


# Helper functions


def cmd(resonate: Resonate) -> Command:
    item = resonate._bridge._cq.get_nowait()  # noqa: SLF001
    assert isinstance(item, tuple)

    cmd, _ = item
    return cmd


# Tests


@pytest.mark.parametrize("func", [foo, bar, baz, lambda x: x])
@pytest.mark.parametrize("name", ["foo", "bar", "baz", None])
def test_register(func: Callable, name: str | None) -> None:
    # skip lambda functions without name, validation tests will cover this
    if func.__name__ == "<lambda>" and name is None:
        return

    registry = Registry()
    resonate = Resonate(registry=registry)

    resonate.register(func, name=name)
    assert registry.get(name or func.__name__) == (func, 1)
    assert registry.get(func) == (name or func.__name__, 1)

    resonate.register(func, name=name, version=2)
    assert registry.get(name or func.__name__) == (func, 2)
    assert registry.get(func) == (name or func.__name__, 2)


@pytest.mark.parametrize("idempotency_key", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("send_to", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("version", [1, 2, 3, None])
@pytest.mark.parametrize("timeout", [3, 2, 1, None])
@pytest.mark.parametrize("tags", [{"a": "1"}, {"2": "foo"}, {"a": "foo"}, None])
@pytest.mark.parametrize("retry_policy", [Constant(), Linear(), Exponential()])
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
    resonate: Resonate,
    idempotency_key: str | None,
    send_to: str | None,
    version: int | None,
    timeout: int | None,
    tags: dict[str, str] | None,
    retry_policy: RetryPolicy | None,
    func: Callable,
    name: str,
    args: tuple,
    kwargs: dict,
) -> None:
    opts = Options(version=1).merge(
        idempotency_key=idempotency_key,
        send_to=send_to,
        version=version,
        timeout=timeout,
        tags=tags,
        retry_policy=retry_policy,
    )

    opts_dict = {k: v for k, v in vars(opts).items() if k not in ("id", "durable")}

    f1 = resonate.register(func, name=name, version=version or 1)

    def invoke(id: str) -> Invoke:
        return Invoke(id=id, func=func, args=args, kwargs=kwargs, opts=Options(id=id, version=version or 1))

    def invoke_with_opts(id: str) -> Invoke:
        return Invoke(id=id, func=func, args=args, kwargs=kwargs, opts=opts.merge(id=id))

    resonate.run("f1", func, *args, **kwargs)
    assert cmd(resonate) == invoke("f1")

    resonate.run("f2", name, *args, **kwargs)
    assert cmd(resonate) == invoke("f2")

    resonate.options(**opts_dict).run("f3", func, *args, **kwargs)
    assert cmd(resonate) == invoke_with_opts("f3")

    resonate.options(**opts_dict).run("f4", name, *args, **kwargs)
    assert cmd(resonate) == invoke_with_opts("f4")

    f1.run("f5", *args, **kwargs)
    assert cmd(resonate) == invoke("f5")

    f1.options(**opts_dict).run("f6", *args, **kwargs)
    assert cmd(resonate) == invoke_with_opts("f6")

    version = (version or 1) + 1
    opts_dict["version"] = version

    f2 = resonate.register(func, name=name, version=version)
    opts = opts.merge(version=version)

    f2.options(**opts_dict).run("f7", *args, **kwargs)
    assert cmd(resonate) == invoke_with_opts("f7")


@pytest.mark.parametrize("send_to", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("version", [1, 2, 3, None])
@pytest.mark.parametrize("timeout", [3, 2, 1, None])
@pytest.mark.parametrize("tags", [{"a": "1"}, {"2": "foo"}, {"a": "foo"}, None])
@pytest.mark.parametrize("retry_policy", [Constant(), Linear(), Exponential()])
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
    resonate: Resonate,
    send_to: str | None,
    version: int | None,
    timeout: int | None,
    tags: dict[str, str] | None,
    retry_policy: RetryPolicy | None,
    func: Callable,
    name: str,
    args: tuple,
    kwargs: dict,
) -> None:
    opts = Options(version=1).merge(send_to=send_to, version=version, timeout=timeout, tags=tags, retry_policy=retry_policy)
    opts_dict = {k: v for k, v in vars(opts).items() if k not in ("id", "durable")}
    f1 = resonate.register(func, name=name, version=version or 1)

    resonate.rpc("f1", func, *args, **kwargs)
    assert cmd(resonate) == Listen(id="f1")

    resonate.rpc("f2", name, *args, **kwargs)
    assert cmd(resonate) == Listen(id="f2")

    resonate.options(**opts_dict).rpc("f3", func, *args, **kwargs)
    assert cmd(resonate) == Listen(id="f3")

    resonate.options(**opts_dict).rpc("f4", name, *args, **kwargs)
    assert cmd(resonate) == Listen(id="f4")

    f1.rpc("f5", *args, **kwargs)
    assert cmd(resonate) == Listen(id="f5")

    f1.options(**opts_dict).rpc("f6", *args, **kwargs)
    assert cmd(resonate) == Listen(id="f6")


@pytest.mark.parametrize("id", ["foo", "bar", "baz"])
def test_get(resonate: Resonate, id: str) -> None:
    resonate.get(id)
    assert cmd(resonate) == Listen(id=id)


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


@pytest.mark.parametrize(
    ("func", "kwargs"),
    [
        (lambda x: x, {"name": "foo", "version": -1}),
        (lambda x: x, {"name": "foo", "version": 1}),
        (lambda x: x, {"version": 1}),
        (foo, {"version": 1}),
        (foo, {"version": 2}),
        (bar, {"version": 1}),
        (foo, {"name": "bar"}),
        (bar, {"name": "foo"}),
    ],
)
def test_register_validations(registry: Registry, func: Callable, kwargs: dict) -> None:
    resonate = Resonate(registry=registry)
    with pytest.raises(ResonateValidationError):
        resonate.register(func, **kwargs)

    with pytest.raises(ResonateValidationError):
        resonate.register(**kwargs)(func)


@pytest.mark.parametrize(
    ("func", "kwargs"),
    [
        (foo, {"version": 3}),
        (bar, {"version": 2}),
        (baz, {}),
        ("foo", {"version": 3}),
        ("bar", {"version": 2}),
        ("baz", {}),
    ],
)
def test_run_and_rpc_validations(registry: Registry, func: Callable | str, kwargs: dict) -> None:
    resonate = Resonate(registry=registry)

    with pytest.raises(ResonateValidationError):
        resonate.options(**kwargs).run("f", func)

    if callable(func):
        with pytest.raises(ResonateValidationError):
            resonate.options(**kwargs).rpc("f", func)


@pytest.mark.parametrize("func", [foo, bar, baz])
@pytest.mark.parametrize("timeout", [1, 2, 3, 101, 102, 103, None])
@pytest.mark.parametrize("version", [1, 2, 3, None])
@pytest.mark.parametrize("send_to", ["foo", "bar", "baz", None])
@pytest.mark.parametrize("retry_policy", [Constant(), Exponential(), Linear(), Never(), None])
def test_propagation(func: Callable, timeout: int | None, version: int | None, send_to: str | None, retry_policy: RetryPolicy | None) -> None:
    registry = Registry()
    registry.add(func, "func", version or 1)
    registry.add(func, "func", 100)

    opts = Options(timeout=100)
    ctx = Context("f", Info("", 0, {}), opts, registry, Dependencies())

    counter = 0

    for f in (func, "func"):
        for lf in (ctx.lfi, ctx.lfc):
            counter += 1

            cmd = lf(f, 1, 2)
            assert cmd.conv.id == f"f.{counter}"
            assert cmd.conv.func == func
            assert cmd.conv.args == (1, 2)
            assert cmd.conv.kwargs == {}
            assert cmd.conv.opts.id == f"f.{counter}"
            assert cmd.conv.opts.timeout == opts.timeout
            assert cmd.conv.opts.version == 100
            assert cmd.conv.opts.tags == opts.tags
            assert callable(cmd.conv.opts.retry_policy)
            assert isinstance(cmd.conv.opts.retry_policy(func), Never if isgeneratorfunction(func) else Exponential)

            # update the command
            cmd = cmd.options(timeout=timeout, version=version, retry_policy=retry_policy)
            if timeout:
                assert cmd.conv.opts.timeout == min(100, timeout)
            if version:
                assert cmd.conv.opts.version == version
            if retry_policy:
                assert isinstance(cmd.conv.opts.retry_policy, retry_policy.__class__)

        for rf in (ctx.rfi, ctx.rfc, ctx.detached):
            counter += 1

            cmd = rf(f, 1, 2)
            assert cmd.conv.id == f"f.{counter}"
            assert cmd.conv.headers is None
            assert cmd.conv.data == {"func": "func", "args": (1, 2), "kwargs": {}, "version": 100}
            assert cmd.conv.timeout == sys.maxsize if rf == ctx.detached else opts.timeout
            assert cmd.conv.tags == {**opts.tags, "resonate:invoke": opts.send_to}

            cmd = cmd.options(timeout=timeout, version=version, send_to=send_to)
            if timeout:
                assert cmd.conv.timeout == timeout if rf == ctx.detached else min(100, timeout)
            if version and isinstance(f, Callable):
                assert cmd.conv.data == {"func": "func", "args": (1, 2), "kwargs": {}, "version": version}
            if send_to:
                assert cmd.conv.tags["resonate:invoke"] == send_to
