from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, final


class Retriable(Protocol):
    def calculate_delay(self, attempt: int) -> float | None: ...


type RetryPolicy = Exponential | Linear | Constant | Never


@final
@dataclass(frozen=True)
class Exponential:
    base_delay: float
    factor: float
    max_delay: float
    max_retries: int

    def calculate_delay(self, attempt: int) -> float | None:
        assert attempt > 0, "attempt must be positive"
        if attempt > self.max_retries and self.max_retries >= 0:
            return None
        return min(self.base_delay * (self.factor**attempt), self.max_delay)


def exponential(base_delay: float, factor: float, max_delay: float, max_retries: int) -> Exponential:
    return Exponential(base_delay=base_delay, factor=factor, max_delay=max_delay, max_retries=max_retries)


@final
@dataclass(frozen=True)
class Linear:
    delay: float
    max_retries: int

    def calculate_delay(self, attempt: int) -> float | None:
        assert attempt > 0, "attempt must be positive"
        if attempt > self.max_retries and self.max_retries >= 0:
            return None
        return self.delay * attempt


def linear(delay: float, max_retries: int) -> Linear:
    return Linear(delay=delay, max_retries=max_retries)


@final
@dataclass(frozen=True)
class Constant:
    delay: float
    max_retries: int

    def calculate_delay(self, attempt: int) -> float | None:
        assert attempt > 0, "attempt must be positive"
        if attempt > self.max_retries and self.max_retries >= 0:
            return None
        return self.delay


def constant(delay: float, max_retries: int) -> Constant:
    return Constant(delay=delay, max_retries=max_retries)


@final
@dataclass(frozen=True)
class Never: ...


def never() -> Never:
    return Never()
