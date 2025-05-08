from __future__ import annotations

import random
import uuid
from typing import TYPE_CHECKING, Any

from resonate import Context, Resonate, Yieldable

if TYPE_CHECKING:
    from collections.abc import Generator

resonate = Resonate.remote(host="http://localhost", group="general")


@resonate.register
def workflow(ctx: Context) -> Generator[Yieldable, Any, list[int]]:
    v = yield ctx.rfc(
        "vectorized-sum",
        [random.randint(0, 100) for _ in range(1_000)],
        [random.randint(0, 100) for _ in range(1_000)],
    ).options(
        target="poll://ml",
    )
    return v


if __name__ == "__main__":
    h = resonate.run(f"workflow-{uuid.uuid4().hex}", workflow)
    v = h.result()
    assert isinstance(v, list)
    assert len(v) == 1_000
