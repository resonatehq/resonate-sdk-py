from __future__ import annotations

from concurrent.futures import Future
from typing import Generic, TypeVar, final

from typing_extensions import assert_never

from resonate.actions import Invoke, Sleep
from resonate.result import Err, Ok, Result

T = TypeVar("T")


@final
class Promise(Generic[T]):
    def __init__(
        self,
        promise_id: str,
        action: Invoke | Sleep,
    ) -> None:
        self.promise_id = promise_id
        self.f = Future[T]()
        self.action = action
        if isinstance(action, Invoke):
            self.durable = action.opts.durable
        elif isinstance(action, Sleep):
            raise NotImplementedError
        else:
            assert_never(action)

    def result(self, timeout: float | None = None) -> T:
        return self.f.result(timeout=timeout)

    def safe_result(self, timeout: float | None = None) -> Result[T, Exception]:
        res: Result[T, Exception]
        try:
            res = Ok(self.f.result(timeout=timeout))
        except Exception as e:  # noqa: BLE001
            res = Err(e)
        return res

    def set_result(self, result: Result[T, Exception]) -> None:
        if isinstance(result, Ok):
            self.f.set_result(result.unwrap())
        elif isinstance(result, Err):
            self.f.set_exception(result.err())
        else:
            assert_never(result)

    def done(self) -> bool:
        return self.f.done()

    def success(self) -> bool:
        if not self.done():
            return False
        try:
            self.result()
        except Exception:  # noqa: BLE001
            return False
        else:
            return True

    def failure(self) -> bool:
        return not self.success()
