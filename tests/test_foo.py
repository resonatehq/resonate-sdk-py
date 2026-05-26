from __future__ import annotations

from resonate import hello


def test_foo() -> None:
    assert hello() == "Hello from resonate!"
