from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, TypeVar

from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from resonate.record import Handle
    from resonate.stores.record import TaskRecord
    from resonate.typing import DurableCoro, DurableFn

P = ParamSpec("P")
T = TypeVar("T")


class IScheduler(ABC):
    @abstractmethod
    def lfi(
        self,
        id: str,
        func: DurableCoro[P, T] | DurableFn[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[T]: ...

    @abstractmethod
    def rfi(
        self,
        id: str,
        func: str,
        /,
        *args: Any,  # noqa: ANN401
        **kwargs: Any,  # noqa: ANN401
    ) -> Handle[Any]: ...

    @abstractmethod
    def add_task(self, task: TaskRecord) -> None: ...
    @abstractmethod
    def set_default_recv(self, recv: dict[str, Any]) -> None: ...
