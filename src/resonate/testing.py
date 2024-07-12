from __future__ import annotations

import os

from typing_extensions import assert_never

from resonate.contants import ENV_VARIABLE_PIN_SEED
from resonate.scheduler.dst import DSTScheduler


def dst(seeds: list[range | int]) -> list[DSTScheduler]:
    schedulers: list[DSTScheduler] = []

    pin_seed = os.environ.get(ENV_VARIABLE_PIN_SEED)
    if pin_seed is not None:
        seeds = [int(pin_seed)]

    for seed in seeds:
        if isinstance(seed, range):
            schedulers.extend(DSTScheduler(i) for i in seed)
        elif isinstance(seed, int):
            schedulers.append(DSTScheduler(seed=seed))
        else:
            assert_never(seed)

    return schedulers


def dsds(s: DSTScheduler) -> str:
    return f"{s.seed} cause the test to fail. Re run with `PIN_RANDOM_SEED={s.seed}`"
