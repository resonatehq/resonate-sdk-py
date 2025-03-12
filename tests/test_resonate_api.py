from typing import Generator
from typing_extensions import Any
from resonate.registry import Registry
from resonate import Resonate, Context


def test_api_register_and_run() -> None:
    registry = Registry()
    resonate = Resonate(registry=registry)

    @resonate.register(name="foo.name")
    def foo() -> int:
        return 1

    assert registry.get("foo.name") == foo.func

    foo.run("foo")
    resonate.run("foo", foo)
    foo.options(send_to="abc").run("foo")

    def bar(ctx: Context) -> Generator[Any, Any, int]:
        return (yield ctx.lfc(foo))

    resonate.register(bar, name="bar.name")
    assert registry.get("bar.name") == bar
    resonate.run("bar", bar)
    resonate.options(send_to="abc").rpc("bar", bar)

    @resonate.register
    def baz(ctx: Context) -> None: ...

    assert registry.get("baz") == baz.func

    baz.run("baz")
    resonate.run("baz", baz)

def test_run_and_register_as_method() -> None:
    def foo(ctx: Context, a: int, b: int) -> int:
        return a+b

    def bar(a: int, b: int) -> int:
        return a+b
    resonate= Resonate()
    resonate.register(foo)
    resonate.register(bar)

    resonate.run("foo.run", foo, 1, 2)
    resonate.run("bar.run", bar, 1, 2)
    resonate.options(send_to="a", version=2).run("foo.options.run", foo, 1, 2)
    resonate.options(send_to="a", version=2).run("bar.options.run", bar, 1, 2)
    resonate.rpc("foo.rpc", foo, 1, 2)
    resonate.rpc("bar.rpc", bar, 1, 2)
    resonate.options(send_to="a", version=2).rpc("foo.rpc", foo, 1, 2)
    resonate.options(send_to="a", version=2).rpc("bar.rpc", bar, 1, 2)

def test_run_and_register_as_decorator() -> None:
    resonate= Resonate()
    @resonate.register
    def foo(ctx: Context, a: int, b: int) -> int:
        return a+b

    @resonate.register
    def bar(a: int, b: int) -> int:
        return a+b

    foo.run("foo.run", 1, 2)
    bar.run("bar.run", 1, 2)
    foo.options(send_to="a", version=2).run("foo.options.run", 1, 2)
    bar.options(send_to="a", version=2).run("bar.options.run", 1, 2)
    foo.rpc("foo.rpc", 1, 2)
    bar.rpc("bar.rpc", 1, 2)
    foo.options(send_to="a", version=2).rpc("foo.rpc", 1, 2)
    bar.options(send_to="a", version=2).rpc("bar.rpc", 1, 2)
