# run with uv run --with 'resonate-sdk>=0.6.2' foo.py
from __future__ import annotations

from resonate import Resonate

resonate = Resonate.remote()


@resonate.register
def foo(ctx, greeting):
    print("running foo")
    # async
    promise = yield ctx.begin_run(bar, greeting)
    print("P", promise)
    # await
    greeting = yield promise
    print("R", greeting)
    # sync
    greeting = yield ctx.run(baz, greeting)
    print("R", greeting)
    return greeting


def bar(_, greeting):
    print(f"bar called with: {greeting}")
    return f"bar: {greeting}"


def baz(_, greeting):
    print(f"baz called with: {greeting}")
    return f"baz: {greeting}"


def main():
    resonate.start()

    handles = [resonate.begin_run(f"test-foo-lfi-{i}", "foo", f"hello-{i}") for i in range(5)]

    results = [handle.result() for handle in handles]

    print(results)


if __name__ == "__main__":
    main()
