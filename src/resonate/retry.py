from __future__ import annotations

from typing import Protocol

import msgspec


class RetryPolicy(Protocol):
    def next(self, attempt: int) -> int | None:
        """Return seconds to sleep before ``attempt``, or ``None`` to stop retrying.

        ``attempt`` is the *upcoming* attempt number: the initial execution is
        attempt 0 and never consults the policy, the first retry is attempt 1,
        the second retry is attempt 2, and so on. A policy that wants to allow
        ``N`` retries (so ``1 + N`` total executions) returns a delay for
        ``attempt`` in ``1..=N`` and ``None`` for ``attempt > N``.

        A returned delay of ``0`` means "retry immediately, no sleep".
        """


class Exponential(msgspec.Struct, frozen=True, kw_only=True):
    # Defaults define the SDK-wide default policy (``DEFAULT_RETRY_POLICY``):
    # 1s base, doubling, capped at 30s, effectively unbounded retries. Fields are
    # ``float`` so ``factor ** attempt`` saturates to ``inf`` (then clamps to
    # ``max_delay``) instead of building an ever-larger ``int`` as ``attempt``
    # grows toward ``max_retries``.
    delay: int
    max_retries: int
    factor: int
    max_delay: int

    def next(self, attempt: int) -> int | None:
        if attempt > self.max_retries:
            return None

        return min(self.delay * self.factor**attempt, self.max_delay)


class Linear(msgspec.Struct, frozen=True, kw_only=True):
    max_retries: int
    delay: int

    def next(self, attempt: int) -> int | None:
        if attempt > self.max_retries:
            return None
        return self.delay * attempt


class Constant(msgspec.Struct, frozen=True, kw_only=True):
    max_retries: int
    delay: int

    def next(self, attempt: int) -> int | None:
        if attempt > self.max_retries:
            return None
        return self.delay


class Never(msgspec.Struct, frozen=True, kw_only=True):
    def next(self, attempt: int) -> int | None:
        return None
