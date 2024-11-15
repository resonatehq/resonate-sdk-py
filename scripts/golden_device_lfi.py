from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context

logging.basicConfig(level="DEBUG")
resonate = Resonate(url="http://localhost:8001", polling_url="http://localhost:8002")


def foo(ctx: Context, n: str) -> str:  # noqa: ARG001
    return n


resonate.register(foo)
p = resonate.lfi("foo", foo, "hi")
assert p.result() == "hi"
