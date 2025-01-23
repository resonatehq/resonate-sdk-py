from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Generic, TypeVar

from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from collections.abc import Generator

T = TypeVar("T")
P = ParamSpec("P")


class Executor(Generic[P, T]):
    def __init__(self, fn: Callable[P, Generator[None, None, T]]) -> None:
        self.fn = fn

    def run(self, *args: P.args, **kwargs: P.kwargs) -> Generator[None, None, T]:
        return self.fn(*args, **kwargs)


def decorator(
    opt1: int = 1,
) -> Callable[[Callable[P, Generator[None, None, T]]], Executor[P, T]]:
    def inner(func: Callable[P, Generator[None, None, T]]) -> Executor[P, T]:
        return Executor[P, T](func)

    return inner


@decorator()
def add(a: int, b: int) -> Generator[None, None, int]:
    yield
    return a + b


add.run(1, 2)
