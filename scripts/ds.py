from __future__ import annotations

import random
from functools import partial


def bar():
    return random.randint(1, 10)


def foo(n: int):
    if n == 1:
        yield partial(bar)
        return None
    return None


def main(n: int) -> None:
    coro = foo(n)
    final_value = None
    try:
        v = next(coro)
        try:
            while True:
                coro.send(v())
        except StopIteration as e:
            final_value = e.value
    except StopIteration as e:
        final_value = e.value

    assert final_value is None


if __name__ == "__main__":
    for _ in range(10000):
        main(random.randint(0, 1000))
