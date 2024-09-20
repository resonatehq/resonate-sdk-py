from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, final

from typing_extensions import ParamSpec, Self

from resonate.options import Options

if TYPE_CHECKING:
    from resonate.dataclasses import FnOrCoroutine
    from resonate.retry_policy import RetryPolicy
from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar, final

from typing_extensions import ParamSpec, override

from resonate.options import Options
from resonate.result import Err, Ok, Result

if TYPE_CHECKING:
    from resonate.promise import Promise
    from resonate.typing import ExecutionUnit

P = ParamSpec("P")
T = TypeVar("T")


@final
@dataclass
class Call:
    exec_unit: ExecutionUnit
    opts: Options = field(default=Options())

    def with_options(
        self,
        *,
        durable: bool = True,
        promise_id: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Self:
        self.opts = Options(
            durable=durable, promise_id=promise_id, retry_policy=retry_policy
        )
        return self

    def to_invocation(self) -> Invocation:
        return Invocation(self.exec_unit, opts=self.opts)


@final
@dataclass
class DeferredInvocation:
    """
    Dataclass that contains all required information to do a
    deferred invocation.
    """

    promise_id: str
    coro: FnOrCoroutine
    opts: Options = field(default=Options())

    def with_options(self, *, retry_policy: RetryPolicy | None = None) -> Self:
        self.opts = Options(
            durable=True, promise_id=self.promise_id, retry_policy=retry_policy
        )
        return self


@final
@dataclass
class Invocation:
    exec_unit: ExecutionUnit
    opts: Options = field(default=Options())

    def with_options(
        self,
        *,
        durable: bool = True,
        promise_id: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Self:
        self.opts = Options(
            durable=durable, promise_id=promise_id, retry_policy=retry_policy
        )
        return self


@final
@dataclass(frozen=True)
class Sleep:
    seconds: int


class Combinator(Generic[T]):
    opts: Options

    @abstractmethod
    def done(self) -> bool:
        pass

    @abstractmethod
    def result(self) -> Result[T, Exception]:
        pass


class All(Combinator[list[Any]]):
    def __init__(self, promises: list[Promise[Any]]) -> None:
        self.promises = promises
        self.opts = Options(durable=False)

    @override
    def done(self) -> bool:
        return all(p.done() for p in self.promises)

    @override
    def result(self) -> Result[list[Any], Exception]:
        res: Result[list[Any], Exception]
        try:
            res = Ok([p.result() for p in self.promises])
        except Exception as e:  # noqa: BLE001
            res = Err(e)
        return res


class Race(Combinator[Any]):
    def __init__(self, promises: list[Promise[Any]]) -> None:
        self.promises = promises
        self.opts = Options(durable=False)

    @override
    def done(self) -> bool:
        return any(p.done() for p in self.promises)

    @override
    def result(self) -> Result[Any, Exception]:
        res: Result[Any, Exception]
        try:
            res = Ok(next(p.result() for p in self.promises if p.done()))
        except Exception as e:  # noqa: BLE001
            res = Err(e)
        return res
