"""Distributed Async Await by Resonate HQ, Inc.

The connector seam this SDK is built on -- the ``Network``/``Source``
protocols, the error vocabulary, and the promise id format -- lives in
:mod:`resonate_base`, which connectors depend on without depending on the SDK.
"""

from __future__ import annotations

#: Protocol version string sent in all requests to the Resonate server.
PROTOCOL_VERSION = "2026-04-01"

__all__ = ["PROTOCOL_VERSION"]
