"""Behaviour tests for :mod:`resonate.durable`.

``durable.rs`` has no ``#[cfg(test)]`` block, so there is no Rust test module to
mirror here. The closest analog is Go's ``durable_test.go`` (reflection-based
detection and arg coercion); these tests pin the Python contract: every durable
function -- workflow or leaf -- receives a :class:`Context` as its first
positional argument, the runtime never inspects annotations, and the
``*args``/``**kwargs`` round trip through
:meth:`~resonate.durable.DurableFunction.pack_args` /
:meth:`~resonate.durable.DurableFunction.invoke` preserves the call across the
durability boundary.
"""

from __future__ import annotations

from typing import Any

import msgspec
import pytest

from resonate import DependencyMap
from resonate.codec import Codec, NoopEncryptor
from resonate.context import Context
from resonate.durable import DurableFunction
from resonate.effects import Effects
from resonate.error import ApplicationError, SerializationError
from resonate.network import LocalNetwork
from resonate.send import Sender
from resonate.transport import Transport

I64_MAX = 2**63 - 1


# =============================================================================
# Fixtures: a root ``Context`` (the only env a durable function ever sees)
# =============================================================================


def _context() -> Context:
    sender = Sender(Transport(LocalNetwork()), None)
    effects = Effects(sender, Codec(NoopEncryptor()), [])
    return Context.root(
        id="root",
        timeout_at=I64_MAX,
        func_name="root",
        effects=effects,
        target_resolver=lambda target: target or "",
        deps=DependencyMap(),
    )


# =============================================================================
# Registration: ctx-first convention, no annotation inspection
# =============================================================================


async def leaf(ctx: Context, x: int) -> int:
    return x * 2


async def workflow(ctx: Context, x: int) -> str:
    return f"{ctx.id}:{x}"


async def ctx_only(ctx: Context) -> int:
    return 42


def test_leaf_registered_without_inspecting_annotations() -> None:
    df = DurableFunction(leaf)
    assert df.name == "leaf"


def test_workflow_registered_without_inspecting_annotations() -> None:
    df = DurableFunction(workflow)
    assert df.name == "workflow"


def test_ctx_only_function_registered() -> None:
    df = DurableFunction(ctx_only)
    assert df.name == "ctx_only"


def test_non_callable_rejected() -> None:
    not_callable: Any = 42
    with pytest.raises(ApplicationError, match="expected a callable"):
        DurableFunction(not_callable)


def test_zero_arg_function_rejected() -> None:
    async def no_args() -> int:
        return 42

    with pytest.raises(ApplicationError, match="must accept a Context"):
        DurableFunction(no_args)


def test_first_param_treated_as_ctx_regardless_of_annotation() -> None:
    # No Context annotation, no Info annotation -- the runtime does not care.
    async def fn(anything: int, x: int) -> int:
        return x

    # Builds without error: first param is reserved for the runtime-injected ctx.
    df = DurableFunction(fn)
    # Only ``x`` shows up as a user-facing argument.
    assert df.pack_args(7) == {"args": [7], "kwargs": {}}
    # Passing a second positional overflows the env-stripped signature.
    with pytest.raises(ApplicationError):
        df.pack_args(1, 2)


# =============================================================================
# invoke: ctx is always injected as the first positional argument
# =============================================================================


@pytest.mark.asyncio
async def test_invoke_injects_context() -> None:
    df = DurableFunction(workflow)
    ctx = _context()
    assert await df.invoke(ctx, df.pack_args(7)) == "root:7"


@pytest.mark.asyncio
async def test_invoke_ctx_only_no_user_args() -> None:
    df = DurableFunction(ctx_only)
    assert df.pack_args() is None
    assert await df.invoke(_context(), None) == 42


@pytest.mark.asyncio
async def test_leaf_can_ignore_ctx() -> None:
    df = DurableFunction(leaf)
    assert await df.invoke(_context(), df.pack_args(21)) == 42


# =============================================================================
# invoke: sync functions, errors
# =============================================================================


@pytest.mark.asyncio
async def test_sync_function_supported() -> None:
    def sync_leaf(ctx: Context, x: int) -> int:
        return x + 1

    df = DurableFunction(sync_leaf)
    assert await df.invoke(_context(), df.pack_args(41)) == 42


@pytest.mark.asyncio
async def test_raising_function_propagates() -> None:
    async def boom(ctx: Context, x: int) -> int:
        msg = "boom"
        raise ApplicationError(msg)

    df = DurableFunction(boom)
    with pytest.raises(ApplicationError, match="boom"):
        await df.invoke(_context(), df.pack_args(1))


# =============================================================================
# pack_args / invoke: *args / **kwargs round trip
# =============================================================================


async def variadic(ctx: Context, *args: int, **kwargs: int) -> int:
    return ctx.seq + sum(args) + sum(kwargs.values())


async def keyword_only(ctx: Context, *, a: int, b: int = 10) -> int:
    return a + b


def test_pack_args_validates_arity() -> None:
    df = DurableFunction(leaf)
    with pytest.raises(ApplicationError):
        df.pack_args(1, 2)  # leaf takes a single user positional


def test_pack_args_envelope_shape() -> None:
    df = DurableFunction(variadic)
    payload = df.pack_args(1, 2, 3, k=4)
    assert payload == {"args": [1, 2, 3], "kwargs": {"k": 4}}


@pytest.mark.asyncio
async def test_variadic_round_trip() -> None:
    df = DurableFunction(variadic)
    payload = df.pack_args(1, 2, 3, k=4)
    # Simulate the durability boundary: payload is JSON-decoded back to builtins.
    decoded = msgspec.json.decode(msgspec.json.encode(payload))
    assert await df.invoke(_context(), decoded) == 1 + 2 + 3 + 4  # ctx.seq == 0


@pytest.mark.asyncio
async def test_keyword_only_round_trip() -> None:
    df = DurableFunction(keyword_only)
    assert await df.invoke(_context(), df.pack_args(a=5)) == 15  # b defaults to 10
    assert await df.invoke(_context(), df.pack_args(a=5, b=6)) == 11


# =============================================================================
# invoke: argument coercion on recovery (JSON builtins -> declared types)
# =============================================================================


class Point(msgspec.Struct, frozen=True):
    x: int
    y: int


async def sum_point(ctx: Context, p: Point) -> int:
    return p.x + p.y


@pytest.mark.asyncio
async def test_struct_arg_coerced_from_builtins() -> None:
    df = DurableFunction(sum_point)
    # On recovery the arg arrives as a plain dict, not a Point.
    payload = {"args": [{"x": 3, "y": 4}], "kwargs": {}}
    assert await df.invoke(_context(), payload) == 7


@pytest.mark.asyncio
async def test_coercion_failure_raises_serialization_error() -> None:
    df = DurableFunction(sum_point)
    payload = {"args": [{"x": "not-an-int", "y": 4}], "kwargs": {}}
    with pytest.raises(SerializationError):
        await df.invoke(_context(), payload)


# =============================================================================
# Replay parity: fresh in-process objects vs JSON-recovered builtins
# =============================================================================


def _roundtrip(payload: Any) -> Any:
    """Simulate the durability boundary: JSON-encode the payload and decode it back."""
    return msgspec.json.decode(msgspec.json.encode(payload))


class Line(msgspec.Struct, frozen=True):
    start: Point
    end: Point


async def manhattan(ctx: Context, line: Line) -> int:
    return abs(line.start.x - line.end.x) + abs(line.start.y - line.end.y)


@pytest.mark.asyncio
async def test_replay_parity_positional() -> None:
    df = DurableFunction(leaf)
    fresh = df.pack_args(21)
    assert await df.invoke(_context(), fresh) == await df.invoke(
        _context(), _roundtrip(fresh)
    )


@pytest.mark.asyncio
async def test_replay_parity_struct() -> None:
    df = DurableFunction(sum_point)
    fresh = df.pack_args(Point(x=3, y=4))
    # Fresh dispatch carries a real Point; recovery carries a dict -- both -> 7.
    assert await df.invoke(_context(), fresh) == 7
    assert await df.invoke(_context(), _roundtrip(fresh)) == 7


@pytest.mark.asyncio
async def test_replay_parity_nested_struct() -> None:
    df = DurableFunction(manhattan)
    fresh = df.pack_args(Line(start=Point(x=0, y=0), end=Point(x=3, y=4)))
    assert await df.invoke(_context(), _roundtrip(fresh)) == 7


@pytest.mark.asyncio
async def test_replay_is_idempotent_across_repeats() -> None:
    df = DurableFunction(variadic)
    payload = _roundtrip(df.pack_args(1, 2, 3, k=4))
    results = [await df.invoke(_context(), payload) for _ in range(5)]
    assert results == [10, 10, 10, 10, 10]
    # The DurableFunction carries no per-call state: its metadata is unchanged.
    assert df.name == "variadic"


@pytest.mark.asyncio
async def test_distinct_payloads_do_not_interfere() -> None:
    df = DurableFunction(leaf)
    a = df.pack_args(1)
    b = df.pack_args(100)
    assert await df.invoke(_context(), a) == 2
    assert await df.invoke(_context(), b) == 200
    assert await df.invoke(_context(), a) == 2  # re-running A stays stable


def test_pack_args_does_not_alias_caller() -> None:
    df = DurableFunction(variadic)
    payload = df.pack_args(1, 2, 3)
    payload["args"].append(999)  # mutate the returned envelope
    # A fresh pack is unaffected -- pack_args copies args into a new envelope.
    assert df.pack_args(1, 2, 3) == {"args": [1, 2, 3], "kwargs": {}}


# =============================================================================
# Registration: callables, methods, lambdas
# =============================================================================


class _Adder:
    """A callable object (not a function) used as a durable function."""

    def __call__(self, ctx: Context, x: int) -> int:
        return x + 100


class _Service:
    async def step(self, ctx: Context, x: int) -> str:
        return f"{ctx.id}:{x}"


@pytest.mark.asyncio
async def test_callable_instance_supported() -> None:
    df = DurableFunction(_Adder())
    assert df.name == "unknown"  # an instance has no __name__
    assert await df.invoke(_context(), df.pack_args(1)) == 101


@pytest.mark.asyncio
async def test_bound_method_drops_self() -> None:
    # The bound method's first user-visible param is ``ctx`` -- ``self`` is
    # already bound, so the convention still holds.
    df = DurableFunction(_Service().step)
    assert df.name == "step"
    assert await df.invoke(_context(), df.pack_args(9)) == "root:9"


def test_lambda_supported() -> None:
    df = DurableFunction(lambda _ctx, x: x)
    assert df.name == "<lambda>"


# =============================================================================
# pack_args: signature validation
# =============================================================================


def test_pack_args_missing_required_raises() -> None:
    df = DurableFunction(leaf)
    with pytest.raises(ApplicationError, match="leaf"):
        df.pack_args()


def test_pack_args_unexpected_keyword_raises() -> None:
    df = DurableFunction(leaf)
    with pytest.raises(ApplicationError):
        df.pack_args(1, nope=2)


def test_pack_args_positional_as_keyword() -> None:
    df = DurableFunction(leaf)
    assert df.pack_args(x=21) == {"args": [], "kwargs": {"x": 21}}


@pytest.mark.asyncio
async def test_pack_args_positional_as_keyword_invokes() -> None:
    df = DurableFunction(leaf)
    assert await df.invoke(_context(), df.pack_args(x=21)) == 42


def test_pack_args_excludes_ctx_param() -> None:
    df = DurableFunction(workflow)
    # ``workflow`` is (ctx, x); only ``x`` is a user arg, so passing two
    # positionals (as if including ctx) overflows the env-stripped signature.
    with pytest.raises(ApplicationError):
        df.pack_args(_context(), 7)


# =============================================================================
# *args / **kwargs / positional-only: empties and round trips
# =============================================================================


async def star_args(ctx: Context, *args: int) -> int:
    return sum(args)


async def star_kwargs(ctx: Context, **kwargs: int) -> int:
    return sum(kwargs.values())


async def posonly(ctx: Context, x: int, /, y: int) -> int:
    return x - y


def test_empty_variadic_packs_to_none() -> None:
    assert DurableFunction(star_args).pack_args() is None
    assert DurableFunction(star_kwargs).pack_args() is None


@pytest.mark.asyncio
async def test_star_args_round_trip() -> None:
    df = DurableFunction(star_args)
    assert await df.invoke(_context(), _roundtrip(df.pack_args(1, 2, 3))) == 6
    assert await df.invoke(_context(), None) == 0  # no args supplied


@pytest.mark.asyncio
async def test_star_kwargs_round_trip() -> None:
    df = DurableFunction(star_kwargs)
    assert await df.invoke(_context(), _roundtrip(df.pack_args(a=1, b=2))) == 3


@pytest.mark.asyncio
async def test_positional_only_round_trip() -> None:
    df = DurableFunction(posonly)
    assert await df.invoke(_context(), _roundtrip(df.pack_args(10, 3))) == 7


# =============================================================================
# invoke: payload shapes (envelope vs bare value, cross-language RPC)
# =============================================================================


@pytest.mark.asyncio
async def test_bare_value_payload_is_single_positional() -> None:
    # A non-Python caller (RPC) may send a bare value, not an envelope.
    df = DurableFunction(leaf)
    assert await df.invoke(_context(), 21) == 42


@pytest.mark.asyncio
async def test_bare_struct_dict_payload_coerced() -> None:
    df = DurableFunction(sum_point)
    # A bare dict (no envelope) is treated as one positional and coerced.
    assert await df.invoke(_context(), {"x": 3, "y": 4}) == 7


@pytest.mark.asyncio
async def test_user_dict_shaped_like_envelope_round_trips() -> None:
    # A function taking a single dict whose keys happen to be {"args","kwargs"}:
    # pack_args nests it inside the outer envelope, so it round-trips safely.
    async def echo(ctx: Context, d: dict[str, Any]) -> dict[str, Any]:
        return d

    df = DurableFunction(echo)
    tricky = {"args": [1, 2], "kwargs": {"z": 9}}
    payload = df.pack_args(tricky)
    assert payload == {"args": [tricky], "kwargs": {}}
    assert await df.invoke(_context(), _roundtrip(payload)) == tricky


@pytest.mark.asyncio
async def test_bare_envelope_shaped_dict_is_ambiguous() -> None:
    # KNOWN, documented behavior: a *bare* dict with only "args"/"kwargs" keys is
    # read as an envelope, not as a single dict argument. Foreign callers must
    # avoid that exact shape for a lone dict arg (Python callers are safe -- their
    # pack_args always nests the user dict one level deeper).
    df = DurableFunction(leaf)  # (ctx, x: int)
    assert await df.invoke(_context(), {"args": [5]}) == 10  # -> leaf(ctx, 5)


# =============================================================================
# invoke: coercion edge cases
# =============================================================================


@pytest.mark.asyncio
async def test_unannotated_param_passes_through() -> None:
    def fn(ctx: Context, x: int) -> object:
        return x

    del fn.__annotations__["x"]  # a genuinely unannotated parameter
    df = DurableFunction(fn)
    assert await df.invoke(_context(), {"args": [{"keep": "me"}], "kwargs": {}}) == {
        "keep": "me"
    }


@pytest.mark.asyncio
async def test_any_annotation_passes_through() -> None:
    async def any_leaf(ctx: Context, x: Any) -> Any:
        return x

    df = DurableFunction(any_leaf)
    assert await df.invoke(_context(), {"args": [{"a": 1}], "kwargs": {}}) == {"a": 1}


@pytest.mark.asyncio
async def test_optional_annotation_accepts_none() -> None:
    async def maybe(ctx: Context, x: int | None) -> bool:
        return x is None

    df = DurableFunction(maybe)
    assert await df.invoke(_context(), df.pack_args(None)) is True
    assert await df.invoke(_context(), df.pack_args(5)) is False


@pytest.mark.asyncio
async def test_list_annotation_coerces_elements() -> None:
    async def total(ctx: Context, xs: list[int]) -> int:
        return sum(xs)

    df = DurableFunction(total)
    assert await df.invoke(_context(), {"args": [[1, 2, 3]], "kwargs": {}}) == 6


@pytest.mark.asyncio
async def test_var_positional_struct_coercion() -> None:
    async def sum_points(ctx: Context, *points: Point) -> int:
        return sum(p.x + p.y for p in points)

    df = DurableFunction(sum_points)
    payload = {"args": [{"x": 1, "y": 2}, {"x": 3, "y": 4}], "kwargs": {}}
    assert await df.invoke(_context(), payload) == 10


@pytest.mark.asyncio
async def test_var_keyword_struct_coercion() -> None:
    async def gather(ctx: Context, **points: Point) -> int:
        return sum(p.x for p in points.values())

    df = DurableFunction(gather)
    payload = {"args": [], "kwargs": {"a": {"x": 5, "y": 0}, "b": {"x": 6, "y": 0}}}
    assert await df.invoke(_context(), payload) == 11


# =============================================================================
# invoke: defaults are not coerced (sentinel-default regression)
# =============================================================================


_SENTINEL: Any = object()


@pytest.mark.asyncio
async def test_unprovided_default_is_not_coerced() -> None:
    # Regression: a sentinel default whose type does not match its annotation
    # must survive untouched when the caller omits the argument (it never
    # crossed the serialization boundary, so it needs no coercion).
    async def with_sentinel(ctx: Context, x: int = _SENTINEL) -> bool:
        return x is _SENTINEL

    df = DurableFunction(with_sentinel)
    assert await df.invoke(_context(), None) is True  # default kept as-is
    assert await df.invoke(_context(), df.pack_args(7)) is False  # provided value used


@pytest.mark.asyncio
async def test_provided_value_coerced_with_defaults_present() -> None:
    async def add(ctx: Context, a: int, b: int = 100) -> int:
        return a + b

    df = DurableFunction(add)
    assert await df.invoke(_context(), {"args": [1], "kwargs": {}}) == 101  # b default
    assert await df.invoke(_context(), {"args": [1, 2], "kwargs": {}}) == 3


# =============================================================================
# invoke: resilience to incompatible / corrupt payloads (signature drift)
# =============================================================================


@pytest.mark.asyncio
async def test_invoke_too_many_args_raises() -> None:
    df = DurableFunction(leaf)  # (ctx, x: int)
    with pytest.raises(ApplicationError, match="leaf"):
        await df.invoke(_context(), {"args": [1, 2], "kwargs": {}})


@pytest.mark.asyncio
async def test_invoke_unexpected_keyword_raises() -> None:
    df = DurableFunction(leaf)
    with pytest.raises(ApplicationError):
        await df.invoke(_context(), {"args": [1], "kwargs": {"nope": 9}})


@pytest.mark.asyncio
async def test_coercion_error_notes_function_name() -> None:
    df = DurableFunction(sum_point)
    payload = {"args": [{"x": "bad", "y": 1}], "kwargs": {}}
    with pytest.raises(SerializationError) as excinfo:
        await df.invoke(_context(), payload)
    notes = getattr(excinfo.value, "__notes__", [])
    assert any("sum_point" in note for note in notes)


# =============================================================================
# invoke: sync/async parity and None results
# =============================================================================


@pytest.mark.asyncio
async def test_sync_function_raising_propagates() -> None:
    def boom(ctx: Context, x: int) -> int:
        msg = "sync boom"
        raise ApplicationError(msg)

    df = DurableFunction(boom)
    with pytest.raises(ApplicationError, match="sync boom"):
        await df.invoke(_context(), df.pack_args(1))


@pytest.mark.asyncio
async def test_function_returning_none() -> None:
    captured: list[int] = []

    async def sink(ctx: Context, x: int) -> None:
        captured.append(x)

    df = DurableFunction(sink)
    assert await df.invoke(_context(), df.pack_args(5)) is None
    assert captured == [5]
