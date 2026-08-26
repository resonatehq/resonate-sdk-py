from __future__ import annotations

from resonate_base.connections import Network, Source
from resonate_base.error import ConnectorError, ResonateError

#: Protocol version string sent in the head of every request to the Resonate
#: server. It lives here rather than in the SDK because it describes the *wire*,
#: which a connector may need to stamp or assert on without the SDK installed.
PROTOCOL_VERSION = "2026-04-01"

__all__ = [
    "PROTOCOL_VERSION",
    "ConnectorError",
    "Network",
    "ResonateError",
    "Source",
]
