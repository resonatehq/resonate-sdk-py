"""Shared vocabulary for the SDK's own test suite.

Most of what tests need is public and lives in :mod:`resonate.testing` -- the
same helpers an application gets. This package adds only the pieces that are
specific to testing the SDK *internals*: the network stand-ins, and the golden
file comparison.

The rule from :mod:`resonate.testing` holds here too: **a helper never returns
an error, it fails directly.** No call site should have to check a return value
before getting on with the test.
"""

from __future__ import annotations

from tests.support.golden import assert_golden, golden_path
from tests.support.network import (
    FailingNetwork,
    FakeNetwork,
    FakeSource,
    RecordingNetwork,
    ScriptedNetwork,
    SendOnlyNetwork,
    StubNetwork,
    envelope,
)

__all__ = [
    "FailingNetwork",
    "FakeNetwork",
    "FakeSource",
    "RecordingNetwork",
    "ScriptedNetwork",
    "SendOnlyNetwork",
    "StubNetwork",
    "assert_golden",
    "envelope",
    "golden_path",
]
