from __future__ import annotations

import logging
import os
import random
import sys
from typing import TYPE_CHECKING

import pytest

from resonate.stores.local import LocalStore
from resonate.stores.remote import RemoteStore

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.models.store import Store


def pytest_configure() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--seed", action="store")
    parser.addoption("--steps", action="store")


# DST fixtures


@pytest.fixture
def seed(request: pytest.FixtureRequest) -> str:
    seed = request.config.getoption("--seed")

    if not isinstance(seed, str):
        return str(random.randint(0, sys.maxsize))

    return seed


@pytest.fixture
def steps(request: pytest.FixtureRequest) -> int:
    steps = request.config.getoption("--steps")

    if isinstance(steps, str):
        try:
            return int(steps)
        except ValueError:
            pass

    return 10000


# Store fixtures


def stores() -> list[Callable[[], Store]]:
    s: list[Callable[[], Store]] = [lambda: LocalStore()]

    if "RESONATE_STORE_URL" in os.environ:
        s.append(lambda: RemoteStore(os.environ["RESONATE_STORE_URL"]))

    return s


@pytest.fixture(scope="module", params=stores())
def store(request: pytest.FixtureRequest) -> Store:
    return request.param()
