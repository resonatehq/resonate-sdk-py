# type: ignore[missing-import]
from __future__ import annotations

from threading import Event

import numpy as np

from resonate import Context, Resonate

resonate = Resonate.remote(host="http://localhost", group="ml")


@resonate.register(name="vectorized-sum")
def vectorized_sum(ctx: Context, a: list[int], b: list[int]) -> list[int]:
    return (np.array(a) + np.array(b)).tolist()


if __name__ == "__main__":
    resonate.start()
    Event().wait()  # just to keep the worker alive
