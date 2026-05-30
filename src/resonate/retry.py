from __future__ import annotations

from typing import Protocol

import msgspec


class RetryPolicy(Protocol):
    def next(self, attempt: int) -> float | None:
        """Return seconds to sleep before ``attempt``, or ``None`` to stop retrying.

        ``attempt`` is the *upcoming* attempt number: the initial execution is
        attempt 0 and never consults the policy, the first retry is attempt 1,
        the second retry is attempt 2, and so on. A policy that wants to allow
        ``N`` retries (so ``1 + N`` total executions) returns a delay for
        ``attempt`` in ``1..=N`` and ``None`` for ``attempt > N``.

        A returned delay of ``0`` means "retry immediately, no sleep".
        """
        ...


class Exponential(msgspec.Struct, frozen=True, kw_only=True):
    delay: float = 1
    factor: float = 2
    max_delay: float = 60
    max_retries: int = 5

    def next(self, attempt: int) -> float | None:
        if attempt > self.max_retries:
            return None
        return min(self.delay * self.factor**attempt, self.max_delay)


class Never(msgspec.Struct, frozen=True, kw_only=True):
    def next(self, _: int) -> float | None:
        return None
