"""Distributed Async Await by Resonate HQ, Inc.

The protocol layer this SDK is built on -- errors, injectable time, retry and
backoff policies, the observability event stream, the id format, the wire
records, and the ``Network``/``Source``/``Transport`` seams -- lives in
:mod:`resonate_base`, which connectors depend on without depending on the SDK.
"""

from __future__ import annotations
