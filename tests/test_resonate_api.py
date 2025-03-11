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
