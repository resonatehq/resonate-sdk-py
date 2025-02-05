from __future__ import annotations

import os
from typing import Literal


def url(system: Literal["store", "poller"]) -> str:
    match system:
        case "store":
            return os.getenv("RESONATE_STORE_URL", "http://localhost:8001")
        case "poller":
            return os.getenv("RESONATE_POLLER_URL", "http://localhost:8002")


def group() -> str:
    return "default"
