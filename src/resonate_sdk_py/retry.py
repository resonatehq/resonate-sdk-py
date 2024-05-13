"""Retry module."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Union

from typing_extensions import Self, TypeAlias

from resonate_sdk_py.traits import Default


@dataclass(frozen=True)
class ExponentialRetry(Default):
    """Exponential retry policy."""

    initial_delay: int
    backoff_factor: int
    max_attempts: int | None
    max_delay: int

    @classmethod
    def default(cls) -> Self:
        """Use default exponential retry policy."""
        return cls(
            initial_delay=100, backoff_factor=2, max_attempts=None, max_delay=60_000
        )


@dataclass(frozen=True)
class LinearRetry(Default):
    """Linear retry policy."""

    delay: int
    max_attempts: int | None

    @classmethod
    def default(cls) -> Self:
        """Use default linear retry policy."""
        return cls(delay=1_000, max_attempts=None)


RetryPolicy: TypeAlias = Union[ExponentialRetry, LinearRetry]
