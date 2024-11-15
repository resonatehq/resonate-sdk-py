from __future__ import annotations

from inspect import isgenerator
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from typing_extensions import ParamSpec, assert_never

from resonate.result import Err, Ok, Result

if TYPE_CHECKING:
    from collections.abc import Generator

    from resonate.record import Record
    from resonate.typing import DurableCoro, DurableFn, Yieldable

T = TypeVar("T")
P = ParamSpec("P")


class ResonateCoro(Generic[T]):
    def __init__(self, record: Record[T], coro: Generator[Yieldable, Any, T]) -> None:
        self.id = id
        self._coro = coro
        self._coro_active: bool = True
        self._final_value: Result[T, Exception] | None = None
        self._first_error_used = False
        self._next_child_to_yield = 0
        self._record = record

    def _pending_children(self) -> list[Record[Any]]:
        return self._record.children[self._next_child_to_yield :]

    def next(self) -> Yieldable:
        assert self._coro_active
        return next(self._coro)

    def send(self, v: T) -> Yieldable:
        if self._coro_active:
            try:
                return self._coro.send(v)
            except StopIteration as e:
                assert self._final_value is None, "return value can only be set once."
                self._final_value = Ok(e.value)
                self._coro_active = False

        for child in self._pending_children():
            self._next_child_to_yield += 1
            if child.done():
                continue
            return child.promise

        assert all(child.done() for child in self._record.children)
        assert not self._coro_active
        assert isinstance(self._final_value, Ok)
        raise StopIteration(self._final_value.unwrap())

    def throw(self, error: Exception) -> Yieldable:
        if self._coro_active:
            try:
                return self._coro.throw(error)
            except StopIteration as e:
                assert self._final_value is None, "return value can only be set once."
                self._final_value = Ok(e.value)
                self._coro_active = False

        if not self._first_error_used:
            self._final_value = Err(error)
            self._first_error_used = True

        for child in self._pending_children():
            self._next_child_to_yield += 1
            if child.done():
                continue
            return child.promise
        assert all(
            child.done() for child in self._record.children
        ), "All children promise must have been resolved."

        assert not self._coro_active
        assert self._final_value is not None
        if isinstance(self._final_value, Ok):
            raise StopIteration(self._final_value)
        if isinstance(self._final_value, Err):
            raise self._final_value.err()
        assert_never(self._final_value)


class FnOrCoroutine(Generic[T]):
    def __init__(
        self,
        exec_unit: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.exec_unit = exec_unit
        self.args = args
        self.kwargs = kwargs

    def is_generator(self) -> bool:
        return isgenerator(self.exec_unit)
