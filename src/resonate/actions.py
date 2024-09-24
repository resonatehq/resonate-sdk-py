from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, final

from typing_extensions import ParamSpec, Self

from resonate.options import Options

if TYPE_CHECKING:
    from resonate.dataclasses import FnOrCoroutine
    from resonate.retry_policy import RetryPolicy
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from resonate.result import Err, Ok, Result
from resonate.retry_policy import never

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
    opts: Options = field(default=Options())

    def __init__(self) -> None:
        self.opts = Options(retry_policy=never())

    def with_options(
        self,
        *,
        durable: bool = True,
        promise_id: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> Self:
        """
        Set options for the combinator.

        Args:
            durable (bool): Whether the promise is durable. Defaults to True.
            promise_id (str | None): An optional identifier for the promise.
            retry_policy (RetryPolicy | None): An optional retry policy for the promise.

        Returns:
            Self: The combinator instance with updated options.
        """
        self.opts = Options(
            durable=durable,
            promise_id=promise_id,
            retry_policy=retry_policy if retry_policy is not None else never(),
        )
        return self

    @abstractmethod
    def done(self) -> bool:
        pass

    @abstractmethod
    def result(self) -> Result[T, Exception]:
        pass


class All(Combinator[list[Any]]):
    """
    A combinator that waits for all promises to complete.

    Attributes:
        promises (list[Promise[Any]]): A list of promises to be combined.
    """

    def __init__(self, promises: list[Promise[Any]]) -> None:
        super().__init__()
        self.promises = promises

    def done(self) -> bool:
        """
        Check if all promises are complete.

        Returns:
            bool: True if all promises are complete, False otherwise.
        """
        if len(self.promises) == 0:
            return True

        return all(p.done() for p in self.promises)

    def result(self) -> Result[list[Any], Exception]:
        """
        Get the results of all promises.

        Returns:
            Result[list[Any], Exception]: A Result containing either a list of all
            promise results (Ok) or an Exception if any promise failed (Err).
        """
        if len(self.promises) == 0:
            return Ok([])

        res: Result[list[Any], Exception]
        try:
            res = Ok([p.result() for p in self.promises])
        except Exception as e:  # noqa: BLE001
            res = Err(e)
        return res


class AllSettled(Combinator[list[Any]]):
    """
    A combinator that waits for all promises to complete and returns a list of results
    or Errors.

    Attributes:
        promises (list[Promise[Any]]): A list of promises to be combined.
    """

    def __init__(self, promises: list[Promise[Any]]) -> None:
        super().__init__()
        self.promises = promises

    def done(self) -> bool:
        """
        Check if all promises are settled.

        Returns:
            bool: True if all promises are settled, False otherwise.
        """
        if len(self.promises) == 0:
            return True

        return all(p.done() for p in self.promises)

    def result(self) -> Result[list[Any], Exception]:
        """
        Get the results of all promises.

        Returns:
            Result[list[any]], Exception]: A Result containing
            a list of all promise results and rejections (Ok or Err).
        """
        if len(self.promises) == 0:
            return Ok([])

        res = []
        for p in self.promises:
            try:
                ok = p.result()
                res.append(ok)
            except Exception as err:  # noqa: BLE001, PERF203
                res.append(err)

        return Ok(res)


class Race(Combinator[Any]):
    """
    A combinator that completes when any of the promises completes.

    Attributes:
        promises (list[Promise[Any]]): A list of promises to race.
    """

    def __init__(self, promises: list[Promise[Any]]) -> None:
        assert (
            len(promises) > 0
        ), "Race combinator requires a non empty list of promises"
        super().__init__()
        self.promises = promises

    def done(self) -> bool:
        """
        Check if any promise is complete.

        Returns:
            bool: True if any promise is complete, False otherwise.
        """
        if not self.promises:
            return True

        return any(p.done() for p in self.promises)

    def result(self) -> Result[Any, Exception]:
        """
        Get the result of the first completed promise.

        Returns:
            Result[Any, Exception]: A Result containing either the value of the first
            completed promise (Ok) or an Exception if no promise completed successfully
            (Err).
        """
        if not self.promises:
            return Ok(None)

        res: Result[Any, Exception]
        try:
            res = Ok(next(p.result() for p in self.promises if p.done()))
        except Exception as e:  # noqa: BLE001
            res = Err(e)
        return res
