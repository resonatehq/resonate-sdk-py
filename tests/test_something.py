from __future__ import annotations

import pytest
from resonate_sdk_py import Context


def greet(ctx: Context, name: str) -> str:  # noqa: ARG001
    return f"Hi {name}"


def count(ctx: Context, up_to: int) -> int:  # noqa: ARG001
    return up_to


def test_run_function_from_context() -> None:
    ctx = Context()
    x = ctx.run(greet, name="tomas")
    y = ctx.run(count, up_to=10)
    assert x == "Hi tomas"
    assert y == 10  # noqa: PLR2004


def test_bad_params_run_function_from_context() -> None:
    ctx = Context()
    with pytest.raises(TypeError):
        ctx.run(greet, age=30)  # type: ignore[call-arg]
