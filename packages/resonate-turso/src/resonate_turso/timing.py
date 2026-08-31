"""The clock, defined here rather than imported.

This package depends on ``resonate-base``, which names the connector seams and
nothing else, so ``resonate.timing`` is out of reach -- a connector must not
import the SDK that calls it. The definition is one line and matches the SDK's
exactly, which is what matters: both stamp the same wall-clock milliseconds
onto the same durable rows.
"""

from __future__ import annotations

import time

__all__ = ["now_ms"]


def now_ms() -> int:
    """Return the current time in milliseconds since the UNIX epoch."""
    return time.time_ns() // 1_000_000
