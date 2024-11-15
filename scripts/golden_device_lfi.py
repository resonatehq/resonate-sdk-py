from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from resonate.resonate import Resonate
from resonate.stores.remote import RemoteStore

if TYPE_CHECKING:
    from resonate.context import Context

logging.basicConfig(level="DEBUG")
resonate = Resonate(store=RemoteStore(url="http://localhost:8001"))


def foo(ctx: Context, n: str) -> str:  # noqa: ARG001
    return n


resonate.register(foo)
p = resonate.lfi("foo", foo, "hi")
assert p.result() == "hi"
