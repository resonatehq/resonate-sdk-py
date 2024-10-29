from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from typing_extensions import assert_never

from resonate.dataclasses import ResonateCoro, Runnable

if TYPE_CHECKING:
    from resonate.promise import Promise
    from resonate.result import Result
    from resonate.typing import AwaitingFor

V = TypeVar("V")
K = TypeVar("K")


class Runnables:
    def __init__(self) -> None:
        self.available = deque[tuple[Runnable[Any], bool]]()

    def nothing_in_available(self) -> bool:
        return len(self.available) == 0

    def clear(self) -> None:
        self.available.clear()

    def appendleft(
        self,
        coro: ResonateCoro[Any],
        next_value: Result[Any, Exception] | None,
        *,
        was_awaited: bool,
    ) -> None:
        self.available.appendleft((Runnable(coro, next_value), was_awaited))

    def append(
        self,
        coro: ResonateCoro[Any],
        next_value: Result[Any, Exception] | None,
        *,
        was_awaited: bool,
    ) -> None:
        self.available.append((Runnable(coro, next_value), was_awaited))


class Awaiting:
    def __init__(self) -> None:
        self._local: dict[Promise[Any], list[ResonateCoro[Any]]] = {}
        self._remote: dict[Promise[Any], list[ResonateCoro[Any]]] = {}

    def clear(self) -> None:
        self._local.clear()
        self._remote.clear()

    def waiting_for(
        self, key: Promise[Any], awaiting_for: AwaitingFor | None = None
    ) -> bool:
        if awaiting_for is None:
            return key in self._local or key in self._remote
        if awaiting_for == "local":
            return key in self._local
        if awaiting_for == "remote":
            return key in self._remote

        assert_never(awaiting_for)

    def append(
        self, key: Promise[Any], value: ResonateCoro[Any], awaiting_for: AwaitingFor
    ) -> None:
        awaiting: dict[Promise[Any], list[ResonateCoro[Any]]]
        if awaiting_for == "local":
            awaiting = self._local
        elif awaiting_for == "remote":
            awaiting = self._remote
        else:
            assert_never(awaiting_for)
        awaiting.setdefault(key, []).append(value)

    def get(
        self, key: Promise[Any], awaiting_for: AwaitingFor
    ) -> list[ResonateCoro[Any]] | None:
        awaiting: dict[Promise[Any], list[ResonateCoro[Any]]]
        if awaiting_for == "local":
            awaiting = self._local
        elif awaiting_for == "remote":
            awaiting = self._remote
        else:
            assert_never(awaiting_for)
        return awaiting.get(key)

    def pop(
        self, key: Promise[Any], awaiting_for: AwaitingFor
    ) -> list[ResonateCoro[Any]]:
        awaiting: dict[Promise[Any], list[ResonateCoro[Any]]]
        if awaiting_for == "local":
            awaiting = self._local
        elif awaiting_for == "remote":
            awaiting = self._remote
        else:
            assert_never(awaiting_for)
        return awaiting.pop(key)


class EphemeralMemo(Generic[K, V]):
    def __init__(self) -> None:
        self._memo: dict[K, V] = {}
        self.roots: set[V] = set()

    def add(self, key: K, value: V, *, as_root: bool) -> None:
        if as_root:
            assert key not in self.roots, f"There's alredy a root for key={key}"
            self.roots.add(value)

        assert key not in self._memo, f"There's already a value for key={key}"
        self._memo[key] = value

    def add_to_roots(self, key: K) -> None:
        value = self._memo.get(key)
        assert value is not None, f"There's not a value for key={key}"
        assert value not in self.roots, "Value already in roots"
        self.roots.add(value)

    def get(self, key: K) -> V | None:
        return self._memo.get(key)

    def pop(self, key: K) -> V:
        assert key in self._memo, f"There's not value for key={key}"
        value = self._memo.pop(key)
        self.roots.discard(value)
        return value

    def has(self, key: K) -> bool:
        return key in self._memo

    def clear(self) -> None:
        self._memo.clear()
        self.roots.clear()

    def is_a_root(self, key: K) -> bool:
        assert key in self._memo, f"There's not value for key={key}"
        value = self._memo[key]
        return value in self.roots


class DoubleDict(Generic[K, V]):
    def __init__(self, data: dict[K, V] | None = None) -> None:
        self.data = data if data is not None else {}
        self._index = self._data_to_index()

    def _data_to_index(self) -> dict[V, K]:
        index = {}
        for k, v in self.data.items():
            assert v not in index, "Value cannot be duplicated."
            index[v] = k
        return index

    def add(self, key: K, value: V) -> None:
        assert key not in self.data
        assert value not in self._index
        self.data[key] = value
        self._index[value] = key

    def pop(self, key: K) -> V:
        assert key in self.data
        assert self.data[key] in self._index
        value = self.data.pop(key)
        self._index.pop(value)
        return value

    def get(self, key: K) -> V | None:
        return self.data.get(key)

    def get_from_value(self, value: V) -> K | None:
        return self._index.get(value)
