from __future__ import annotations

from dataclasses import dataclass

from resonate_sdk_py import Context, InputParams, OutputParams


@dataclass(frozen=True)
class GreetParams(InputParams):
    name: str


@dataclass(frozen=True)
class GreetOut(OutputParams):
    text: str


def greet(ctx: Context, params: GreetParams) -> GreetOut:  # noqa: ARG001
    return GreetOut(text=f"Hi {params.name}")


@dataclass(frozen=True)
class CountParams(InputParams):
    up_to: int


@dataclass(frozen=True)
class CountOut(OutputParams):
    counted: list[int]


def count(ctx: Context, params: CountParams) -> CountOut:  # noqa: ARG001
    return CountOut(counted=list(range(1, params.up_to + 1)))


def test_run_function_from_context() -> None:
    ctx = Context()
    x = ctx.run(greet, params=GreetParams(name="tomas"))
    y = ctx.run(count, params=CountParams(up_to=10))
    assert x == GreetOut(text="Hi tomas")
    assert y == CountOut(counted=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
