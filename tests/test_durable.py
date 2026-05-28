"""Behaviour tests for :mod:`resonate.durable`.

``durable.rs`` has no ``#[cfg(test)]`` block, so there is no Rust test module to
mirror here. The closest analog is Go's ``durable_test.go`` (reflection-based
kind detection and arg coercion); these tests pin the same behaviour for the
Python port: the ``ExecutionEnv`` discriminators (:func:`~resonate.durable.into_context`
/ :func:`~resonate.durable.into_info`, mirroring the Rust ``panic!`` arms), the
first-parameter kind detection (``Context`` -> workflow, ``Info`` -> leaf with
metadata, anything else -> pure leaf), and the ``*args``/``**kwargs`` round trip
through :meth:`~resonate.durable.DurableFunction.pack_args` /
:meth:`~resonate.durable.DurableFunction.invoke`.
"""

from __future__ import annotations

from typing import Any

import msgspec
import pytest

from resonate import DependencyMap
from resonate.codec import Codec, NoopEncryptor
from resonate.context import Context
from resonate.durable import durable_function_for, into_context, into_info
from resonate.effects import Effects
from resonate.error import ApplicationError, SerializationError
from resonate.info import Info
from resonate.network import LocalNetwork
from resonate.send import Sender
from resonate.transport import Transport

I64_MAX = 2**63 - 1


# =============================================================================
# Fixtures: a Workflow ``Context`` and a leaf ``Info``
# =============================================================================


def _context() -> Context:
    """Build a root ``Context`` (the ``Workflow`` arm of ``ExecutionEnv``)."""
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


def _info() -> Info:
    """Build an ``Info`` (the ``Function`` arm of ``ExecutionEnv``)."""
    return Info(
        id="leaf",
        parent_id="root",
        origin_id="root",
        branch_id="leaf",
        timeout_at=123,
        func_name="leaf",
        tags={},
        deps=DependencyMap(),
    )


# =============================================================================
# ExecutionEnv: into_context / into_info
# =============================================================================


def test_into_context_returns_workflow_context() -> None:
    ctx = _context()
    assert into_context(ctx) is ctx


def test_into_context_on_function_raises() -> None:
    with pytest.raises(
        AssertionError, match="expected Workflow ExecutionEnv, got Function"
    ):
        into_context(_info())


def test_into_info_returns_function_info() -> None:
    info = _info()
    assert into_info(info) is info


def test_into_info_on_workflow_raises() -> None:
    with pytest.raises(
        AssertionError, match="expected Function ExecutionEnv, got Workflow"
    ):
        into_info(_context())


# =============================================================================
# Kind detection: Context -> workflow, Info -> leaf+info, else -> pure leaf
# =============================================================================


async def pure_leaf(x: int) -> int:
    return x * 2


async def info_leaf(info: Info, x: int) -> str:
    return f"{info.id}:{x}"


async def workflow(ctx: Context, x: int) -> str:
    return f"{ctx.id}:{x}"


async def no_args() -> int:
    return 42


def test_pure_leaf_detected_as_function() -> None:
    df = durable_function_for(pure_leaf)
    assert df.kind == "function"
    assert df.name == "pure_leaf"


def test_info_leaf_detected_as_function() -> None:
    df = durable_function_for(info_leaf)
    assert df.kind == "function"


def test_workflow_detected_as_workflow() -> None:
    df = durable_function_for(workflow)
    assert df.kind == "workflow"


def test_no_args_detected_as_pure_leaf() -> None:
    df = durable_function_for(no_args)
    assert df.kind == "function"


def test_non_callable_rejected() -> None:
    not_callable: Any = 42
    with pytest.raises(ApplicationError, match="expected a callable"):
        durable_function_for(not_callable)


# =============================================================================
# invoke: env injection per kind
# =============================================================================


@pytest.mark.asyncio
async def test_pure_leaf_ignores_env() -> None:
    df = durable_function_for(pure_leaf)
    # Even handed a Context, a pure leaf neither receives nor touches the env.
    assert await df.invoke(_context(), df.pack_args(21)) == 42


@pytest.mark.asyncio
async def test_info_leaf_receives_info() -> None:
    df = durable_function_for(info_leaf)
    info = _info()
    assert await df.invoke(info, df.pack_args(7)) == "leaf:7"


@pytest.mark.asyncio
async def test_workflow_receives_context() -> None:
    df = durable_function_for(workflow)
    ctx = _context()
    assert await df.invoke(ctx, df.pack_args(7)) == "root:7"


@pytest.mark.asyncio
async def test_info_leaf_rejects_workflow_env() -> None:
    df = durable_function_for(info_leaf)
    with pytest.raises(
        AssertionError, match="expected Function ExecutionEnv, got Workflow"
    ):
        await df.invoke(_context(), df.pack_args(1))


@pytest.mark.asyncio
async def test_workflow_rejects_function_env() -> None:
    df = durable_function_for(workflow)
    with pytest.raises(
        AssertionError, match="expected Workflow ExecutionEnv, got Function"
    ):
        await df.invoke(_info(), df.pack_args(1))


# =============================================================================
# invoke: sync functions, errors, and no-arg functions
# =============================================================================


@pytest.mark.asyncio
async def test_sync_function_supported() -> None:
    def sync_leaf(x: int) -> int:
        return x + 1

    df = durable_function_for(sync_leaf)
    assert await df.invoke(_info(), df.pack_args(41)) == 42


@pytest.mark.asyncio
async def test_no_arg_function_packs_to_none() -> None:
    df = durable_function_for(no_args)
    assert df.pack_args() is None
    assert await df.invoke(_info(), None) == 42


@pytest.mark.asyncio
async def test_raising_function_propagates() -> None:
    async def boom(x: int) -> int:
        msg = "boom"
        raise ApplicationError(msg)

    df = durable_function_for(boom)
    with pytest.raises(ApplicationError, match="boom"):
        await df.invoke(_info(), df.pack_args(1))


# =============================================================================
# pack_args / invoke: *args / **kwargs round trip
# =============================================================================


async def variadic(ctx: Context, *args: int, **kwargs: int) -> int:
    return ctx.seq + sum(args) + sum(kwargs.values())


async def keyword_only(*, a: int, b: int = 10) -> int:
    return a + b


def test_pack_args_validates_arity() -> None:
    df = durable_function_for(pure_leaf)
    with pytest.raises(ApplicationError):
        df.pack_args(1, 2)  # pure_leaf takes a single positional


def test_pack_args_envelope_shape() -> None:
    df = durable_function_for(variadic)
    payload = df.pack_args(1, 2, 3, k=4)
    assert payload == {"args": [1, 2, 3], "kwargs": {"k": 4}}


@pytest.mark.asyncio
async def test_variadic_round_trip() -> None:
    df = durable_function_for(variadic)
    payload = df.pack_args(1, 2, 3, k=4)
    # Simulate the durability boundary: payload is JSON-decoded back to builtins.
    decoded = msgspec.json.decode(msgspec.json.encode(payload))
    assert await df.invoke(_context(), decoded) == 1 + 2 + 3 + 4  # ctx.seq == 0


@pytest.mark.asyncio
async def test_keyword_only_round_trip() -> None:
    df = durable_function_for(keyword_only)
    assert await df.invoke(_info(), df.pack_args(a=5)) == 15  # b defaults to 10
    assert await df.invoke(_info(), df.pack_args(a=5, b=6)) == 11


# =============================================================================
# invoke: argument coercion on recovery (JSON builtins -> declared types)
# =============================================================================


class Point(msgspec.Struct, frozen=True):
    x: int
    y: int


async def sum_point(p: Point) -> int:
    return p.x + p.y


@pytest.mark.asyncio
async def test_struct_arg_coerced_from_builtins() -> None:
    df = durable_function_for(sum_point)
    # On recovery the arg arrives as a plain dict, not a Point.
    payload = {"args": [{"x": 3, "y": 4}], "kwargs": {}}
    assert await df.invoke(_info(), payload) == 7


@pytest.mark.asyncio
async def test_coercion_failure_raises_serialization_error() -> None:
    df = durable_function_for(sum_point)
    payload = {"args": [{"x": "not-an-int", "y": 4}], "kwargs": {}}
    with pytest.raises(SerializationError):
        await df.invoke(_info(), payload)


# =============================================================================
# Replay parity: fresh in-process objects vs JSON-recovered builtins
# =============================================================================


def _roundtrip(payload: Any) -> Any:
    """Simulate the durability boundary: JSON-encode the payload and decode it back."""
    return msgspec.json.decode(msgspec.json.encode(payload))


class Line(msgspec.Struct, frozen=True):
    start: Point
    end: Point


async def manhattan(line: Line) -> int:
    return abs(line.start.x - line.end.x) + abs(line.start.y - line.end.y)


@pytest.mark.asyncio
async def test_replay_parity_positional() -> None:
    df = durable_function_for(pure_leaf)
    fresh = df.pack_args(21)
    assert await df.invoke(_info(), fresh) == await df.invoke(
        _info(), _roundtrip(fresh)
    )


@pytest.mark.asyncio
async def test_replay_parity_struct() -> None:
    df = durable_function_for(sum_point)
    fresh = df.pack_args(Point(x=3, y=4))
    # Fresh dispatch carries a real Point; recovery carries a dict -- both -> 7.
    assert await df.invoke(_info(), fresh) == 7
    assert await df.invoke(_info(), _roundtrip(fresh)) == 7


@pytest.mark.asyncio
async def test_replay_parity_nested_struct() -> None:
    df = durable_function_for(manhattan)
    fresh = df.pack_args(Line(start=Point(x=0, y=0), end=Point(x=3, y=4)))
    assert await df.invoke(_info(), _roundtrip(fresh)) == 7


@pytest.mark.asyncio
async def test_replay_is_idempotent_across_repeats() -> None:
    df = durable_function_for(variadic)
    payload = _roundtrip(df.pack_args(1, 2, 3, k=4))
    results = [await df.invoke(_context(), payload) for _ in range(5)]
    assert results == [10, 10, 10, 10, 10]
    # The DurableFunction carries no per-call state: its metadata is unchanged.
    assert df.kind == "workflow"
    assert df.name == "variadic"


@pytest.mark.asyncio
async def test_distinct_payloads_do_not_interfere() -> None:
    df = durable_function_for(pure_leaf)
    a = df.pack_args(1)
    b = df.pack_args(100)
    assert await df.invoke(_info(), a) == 2
    assert await df.invoke(_info(), b) == 200
    assert await df.invoke(_info(), a) == 2  # re-running A stays stable


def test_pack_args_does_not_alias_caller() -> None:
    df = durable_function_for(variadic)
    payload = df.pack_args(1, 2, 3)
    payload["args"].append(999)  # mutate the returned envelope
    # A fresh pack is unaffected -- pack_args copies args into a new envelope.
    assert df.pack_args(1, 2, 3) == {"args": [1, 2, 3], "kwargs": {}}


# =============================================================================
# Kind detection: callables, methods, lambdas, identity vs name
# =============================================================================


class _Adder:
    """A callable object (not a function) used as a pure leaf."""

    def __call__(self, x: int) -> int:
        return x + 100


class _Service:
    async def step(self, ctx: Context, x: int) -> str:
        return f"{ctx.id}:{x}"


@pytest.mark.asyncio
async def test_callable_instance_supported() -> None:
    df = durable_function_for(_Adder())
    assert df.kind == "function"  # first param `x` -> pure leaf
    assert df.name == "unknown"  # an instance has no __name__
    assert await df.invoke(_info(), df.pack_args(1)) == 101


@pytest.mark.asyncio
async def test_bound_method_drops_self() -> None:
    df = durable_function_for(_Service().step)
    assert df.kind == "workflow"  # first non-self param is Context
    assert df.name == "step"
    assert await df.invoke(_context(), df.pack_args(9)) == "root:9"


def test_lambda_is_pure_leaf() -> None:
    df = durable_function_for(lambda x: x)
    assert df.kind == "function"
    assert df.name == "<lambda>"


async def str_first(name: str, x: int) -> str:
    return f"{name}:{x}"


def test_unrelated_first_param_is_pure_leaf() -> None:
    # An annotated-but-unrelated first parameter is a real user arg, not env.
    assert durable_function_for(str_first).kind == "function"


@pytest.mark.asyncio
async def test_unrelated_first_param_received_as_arg() -> None:
    df = durable_function_for(str_first)
    assert await df.invoke(_info(), df.pack_args("hello", 1)) == "hello:1"


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        ("Context", "workflow"),
        ("  Context  ", "workflow"),
        ("some.module.Context", "workflow"),
        ("Info", "function"),
        ("pkg.Info", "function"),
        ("int", "function"),
        ("Contextual", "function"),  # only an exact trailing segment matches
    ],
)
def test_string_annotation_fallback(annotation: str, expected: str) -> None:
    # Source annotations stay lint-clean; we overwrite them at runtime with an
    # unresolvable second annotation so ``inspect.signature(eval_str=True)``
    # raises and the name-based fallback in ``_env_param_of`` takes over.
    def fn(a: int, b: int) -> int:
        return a + b

    fn.__annotations__ = {"a": annotation, "b": "Unresolvable_XYZ"}
    assert durable_function_for(fn).kind == expected


# =============================================================================
# pack_args: signature validation
# =============================================================================


def test_pack_args_missing_required_raises() -> None:
    df = durable_function_for(pure_leaf)
    with pytest.raises(ApplicationError, match="pure_leaf"):
        df.pack_args()


def test_pack_args_unexpected_keyword_raises() -> None:
    df = durable_function_for(pure_leaf)
    with pytest.raises(ApplicationError):
        df.pack_args(1, nope=2)


def test_pack_args_positional_as_keyword() -> None:
    df = durable_function_for(pure_leaf)
    assert df.pack_args(x=21) == {"args": [], "kwargs": {"x": 21}}


@pytest.mark.asyncio
async def test_pack_args_positional_as_keyword_invokes() -> None:
    df = durable_function_for(pure_leaf)
    assert await df.invoke(_info(), df.pack_args(x=21)) == 42


def test_pack_args_excludes_env_param() -> None:
    df = durable_function_for(workflow)
    # ``workflow`` is (ctx, x); only ``x`` is a user arg, so passing two
    # positionals (as if including ctx) overflows the env-stripped signature.
    with pytest.raises(ApplicationError):
        df.pack_args(_context(), 7)


# =============================================================================
# *args / **kwargs / positional-only: empties and round trips
# =============================================================================


async def star_args(*args: int) -> int:
    return sum(args)


async def star_kwargs(**kwargs: int) -> int:
    return sum(kwargs.values())


async def posonly(x: int, /, y: int) -> int:
    return x - y


def test_empty_variadic_packs_to_none() -> None:
    assert durable_function_for(star_args).pack_args() is None
    assert durable_function_for(star_kwargs).pack_args() is None


@pytest.mark.asyncio
async def test_star_args_round_trip() -> None:
    df = durable_function_for(star_args)
    assert await df.invoke(_info(), _roundtrip(df.pack_args(1, 2, 3))) == 6
    assert await df.invoke(_info(), None) == 0  # no args supplied


@pytest.mark.asyncio
async def test_star_kwargs_round_trip() -> None:
    df = durable_function_for(star_kwargs)
    assert await df.invoke(_info(), _roundtrip(df.pack_args(a=1, b=2))) == 3


@pytest.mark.asyncio
async def test_positional_only_round_trip() -> None:
    df = durable_function_for(posonly)
    assert await df.invoke(_info(), _roundtrip(df.pack_args(10, 3))) == 7


# =============================================================================
# invoke: payload shapes (envelope vs bare value, cross-language RPC)
# =============================================================================


@pytest.mark.asyncio
async def test_bare_value_payload_is_single_positional() -> None:
    # A non-Python caller (RPC) may send a bare value, not an envelope.
    df = durable_function_for(pure_leaf)
    assert await df.invoke(_info(), 21) == 42


@pytest.mark.asyncio
async def test_bare_struct_dict_payload_coerced() -> None:
    df = durable_function_for(sum_point)
    # A bare dict (no envelope) is treated as one positional and coerced.
    assert await df.invoke(_info(), {"x": 3, "y": 4}) == 7


@pytest.mark.asyncio
async def test_user_dict_shaped_like_envelope_round_trips() -> None:
    # A leaf taking a single dict whose keys happen to be {"args","kwargs"}:
    # pack_args nests it inside the outer envelope, so it round-trips safely.
    async def echo(d: dict[str, Any]) -> dict[str, Any]:
        return d

    df = durable_function_for(echo)
    tricky = {"args": [1, 2], "kwargs": {"z": 9}}
    payload = df.pack_args(tricky)
    assert payload == {"args": [tricky], "kwargs": {}}
    assert await df.invoke(_info(), _roundtrip(payload)) == tricky


@pytest.mark.asyncio
async def test_bare_envelope_shaped_dict_is_ambiguous() -> None:
    # KNOWN, documented behavior: a *bare* dict with only "args"/"kwargs" keys is
    # read as an envelope, not as a single dict argument. Foreign callers must
    # avoid that exact shape for a lone dict arg (Python callers are safe -- their
    # pack_args always nests the user dict one level deeper).
    df = durable_function_for(pure_leaf)  # (x: int)
    assert await df.invoke(_info(), {"args": [5]}) == 10  # -> pure_leaf(5)


# =============================================================================
# invoke: coercion edge cases
# =============================================================================


@pytest.mark.asyncio
async def test_unannotated_param_passes_through() -> None:
    def fn(x: int) -> object:
        return x

    del fn.__annotations__["x"]  # a genuinely unannotated parameter
    df = durable_function_for(fn)
    assert await df.invoke(_info(), {"args": [{"keep": "me"}], "kwargs": {}}) == {
        "keep": "me"
    }


@pytest.mark.asyncio
async def test_any_annotation_passes_through() -> None:
    async def any_leaf(x: Any) -> Any:
        return x

    df = durable_function_for(any_leaf)
    assert await df.invoke(_info(), {"args": [{"a": 1}], "kwargs": {}}) == {"a": 1}


@pytest.mark.asyncio
async def test_optional_annotation_accepts_none() -> None:
    async def maybe(x: int | None) -> bool:
        return x is None

    df = durable_function_for(maybe)
    assert await df.invoke(_info(), df.pack_args(None)) is True
    assert await df.invoke(_info(), df.pack_args(5)) is False


@pytest.mark.asyncio
async def test_list_annotation_coerces_elements() -> None:
    async def total(xs: list[int]) -> int:
        return sum(xs)

    df = durable_function_for(total)
    assert await df.invoke(_info(), {"args": [[1, 2, 3]], "kwargs": {}}) == 6


@pytest.mark.asyncio
async def test_var_positional_struct_coercion() -> None:
    async def sum_points(info: Info, *points: Point) -> int:
        return sum(p.x + p.y for p in points)

    df = durable_function_for(sum_points)
    payload = {"args": [{"x": 1, "y": 2}, {"x": 3, "y": 4}], "kwargs": {}}
    assert await df.invoke(_info(), payload) == 10


@pytest.mark.asyncio
async def test_var_keyword_struct_coercion() -> None:
    async def gather(**points: Point) -> int:
        return sum(p.x for p in points.values())

    df = durable_function_for(gather)
    payload = {"args": [], "kwargs": {"a": {"x": 5, "y": 0}, "b": {"x": 6, "y": 0}}}
    assert await df.invoke(_info(), payload) == 11


# =============================================================================
# invoke: defaults are not coerced (sentinel-default regression)
# =============================================================================


_SENTINEL: Any = object()


@pytest.mark.asyncio
async def test_unprovided_default_is_not_coerced() -> None:
    # Regression: a sentinel default whose type does not match its annotation
    # must survive untouched when the caller omits the argument (it never
    # crossed the serialization boundary, so it needs no coercion).
    async def with_sentinel(x: int = _SENTINEL) -> bool:
        return x is _SENTINEL

    df = durable_function_for(with_sentinel)
    assert await df.invoke(_info(), None) is True  # default kept as-is
    assert await df.invoke(_info(), df.pack_args(7)) is False  # provided value used


@pytest.mark.asyncio
async def test_provided_value_coerced_with_defaults_present() -> None:
    async def add(a: int, b: int = 100) -> int:
        return a + b

    df = durable_function_for(add)
    assert await df.invoke(_info(), {"args": [1], "kwargs": {}}) == 101  # b default
    assert await df.invoke(_info(), {"args": [1, 2], "kwargs": {}}) == 3


# =============================================================================
# invoke: resilience to incompatible / corrupt payloads (signature drift)
# =============================================================================


@pytest.mark.asyncio
async def test_invoke_too_many_args_raises() -> None:
    df = durable_function_for(pure_leaf)  # (x: int)
    with pytest.raises(ApplicationError, match="pure_leaf"):
        await df.invoke(_info(), {"args": [1, 2], "kwargs": {}})


@pytest.mark.asyncio
async def test_invoke_unexpected_keyword_raises() -> None:
    df = durable_function_for(pure_leaf)
    with pytest.raises(ApplicationError):
        await df.invoke(_info(), {"args": [1], "kwargs": {"nope": 9}})


@pytest.mark.asyncio
async def test_coercion_error_notes_function_name() -> None:
    df = durable_function_for(sum_point)
    payload = {"args": [{"x": "bad", "y": 1}], "kwargs": {}}
    with pytest.raises(SerializationError) as excinfo:
        await df.invoke(_info(), payload)
    notes = getattr(excinfo.value, "__notes__", [])
    assert any("sum_point" in note for note in notes)


# =============================================================================
# invoke: sync/async parity and None results
# =============================================================================


@pytest.mark.asyncio
async def test_sync_function_raising_propagates() -> None:
    def boom(x: int) -> int:
        msg = "sync boom"
        raise ApplicationError(msg)

    df = durable_function_for(boom)
    with pytest.raises(ApplicationError, match="sync boom"):
        await df.invoke(_info(), df.pack_args(1))


@pytest.mark.asyncio
async def test_function_returning_none() -> None:
    captured: list[int] = []

    async def sink(x: int) -> None:
        captured.append(x)

    df = durable_function_for(sink)
    assert await df.invoke(_info(), df.pack_args(5)) is None
    assert captured == [5]
