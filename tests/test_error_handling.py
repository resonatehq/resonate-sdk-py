"""Pins how exceptions cross the durable-promise boundary.

What this file pins
-------------------

A step function may ``raise`` any Python exception, but every caller that
awaits a rejected promise -- via ``ctx.run``, ``ctx.rpc``, or top-level
``handle.result()`` -- observes a plain
:class:`~resonate.error.ApplicationError` carrying only the original
``str(exc)``. The exception's *class* does not survive the durability
boundary.

Why we test this
----------------

A rejected promise is stored on the server as JSON (see
:func:`resonate.codec.encode_error`: ``{"__type": "error", "message": ...}``)
and used to **deduplicate** recoveries. The worker that resumes a workflow may
be a different process, on a different binary, weeks after the rejection was
recorded -- a user-defined exception class may not exist there. So
:func:`resonate.codec.deserialize_error` always rebuilds the rejection as a
plain ``ApplicationError``, and both the local (``ctx.run``,
``handle.result``) and remote (``ctx.rpc``) paths funnel through it via
:func:`resonate.context._decode_settled`. The tests below pin that contract
at three layers:

1. **Codec** -- :func:`encode_error` + :func:`deserialize_error` collapse
   every exception class to :class:`ApplicationError` with the message
   preserved verbatim. This is the proof shared by both dispatch paths.
2. **Local dispatch (``ctx.run`` / ``handle.result``)** -- end-to-end against
   the in-process :class:`~resonate.network.LocalNetwork`.
3. **Discrimination patterns** -- "by position" (one ``await`` per ``try``)
   and "by message tag" (``raise ApplicationError('E_CODE: ...')``), which
   are the only two patterns that survive the boundary.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import pytest

from resonate.codec import deserialize_error, encode_error
from resonate.error import ApplicationError
from resonate.resonate import Resonate

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from resonate.context import Context


# ── Harness ────────────────────────────────────────────────────────────────


@contextlib.asynccontextmanager
async def local() -> AsyncIterator[Resonate]:
    """Yield a local-mode Resonate, stopping it on exit."""
    r = Resonate.local()
    try:
        yield r
    finally:
        await r.stop()


# ── Domain exceptions used by the workflow library ─────────────────────────
#
# These are deliberately defined *in this test module* so we can prove that
# even when the class is in scope on the worker, the orchestrator side never
# sees it -- it gets a plain ApplicationError.


class PaymentDeclinedError(Exception):
    """A custom domain exception. Invisible across the boundary."""


class FraudSuspectedError(PaymentDeclinedError):
    """Subclass of a custom exception. Also invisible."""


# ── Leaf step functions (settle once) ──────────────────────────────────────


async def step_raises_value_error(ctx: Context) -> str:
    msg = "bad input from step_raises_value_error"
    raise ValueError(msg)


async def step_raises_domain_error(ctx: Context) -> str:
    msg = "card declined for $42"
    raise PaymentDeclinedError(msg)


async def step_raises_domain_subclass(ctx: Context) -> str:
    msg = "suspected fraud on card 4242"
    raise FraudSuspectedError(msg)


async def step_raises_tagged_application_error(ctx: Context) -> str:
    # The canonical way to let callers discriminate: a parseable prefix in
    # the message text.
    msg = "E_NOT_FOUND: user 42 does not exist"
    raise ApplicationError(msg)


async def step_returns_ok(ctx: Context, x: int) -> int:
    return x * 2


# ── Orchestrators (no side effects: replay-safe) ───────────────────────────


async def orchestrator_lets_value_error_escape(ctx: Context) -> str:
    # No try/except: the rejection bubbles all the way out so we can pin what
    # ``handle.result()`` re-raises to the top-level caller.
    return await ctx.run(step_raises_value_error)


async def orchestrator_catches_value_error_as_application_error(
    ctx: Context,
) -> tuple[str, str]:
    try:
        await ctx.run(step_raises_value_error)
    except ApplicationError as exc:
        # Returns (caught_class_name, message) so the test can pin both.
        return type(exc).__name__, exc.message
    msg = "step unexpectedly succeeded"
    raise AssertionError(msg)


async def orchestrator_catches_domain_error_as_application_error(
    ctx: Context,
) -> tuple[str, str]:
    try:
        await ctx.run(step_raises_domain_error)
    except ApplicationError as exc:
        return type(exc).__name__, exc.message
    msg = "step unexpectedly succeeded"
    raise AssertionError(msg)


async def orchestrator_cannot_catch_domain_class(ctx: Context) -> str:
    # If the boundary preserved the class, ``except PaymentDeclined`` would
    # catch the failure. It does not -- the ApplicationError below escapes.
    try:
        await ctx.run(step_raises_domain_error)
    except PaymentDeclinedError:
        return "caught as PaymentDeclined"
    return "not caught as PaymentDeclined"


async def orchestrator_cannot_catch_domain_subclass(ctx: Context) -> str:
    # Same story for a *subclass* of a custom exception: the rejection is an
    # ApplicationError, which is not a PaymentDeclined, so the except does
    # not match even though FraudSuspected is a PaymentDeclined locally.
    try:
        await ctx.run(step_raises_domain_subclass)
    except PaymentDeclinedError:
        return "caught as PaymentDeclined"
    return "not caught as PaymentDeclined"


async def orchestrator_parses_tagged_application_error(
    ctx: Context,
) -> tuple[str, str]:
    try:
        await ctx.run(step_raises_tagged_application_error)
    except ApplicationError as exc:
        code, _, detail = exc.message.partition(": ")
        return code, detail
    msg = "step unexpectedly succeeded"
    raise AssertionError(msg)


async def orchestrator_positional_discrimination(ctx: Context) -> str:
    # The "by position" pattern: one await per try block, so *which* block
    # raised tells you which step failed -- no class introspection needed.
    # This is the only pattern that works without coordinating message
    # contents between the step and the orchestrator.
    await ctx.run(step_returns_ok, 1)  # step A: succeeds

    try:
        await ctx.run(step_raises_value_error)  # step B: fails
    except ApplicationError:
        return "step B failed"

    try:
        await ctx.run(step_raises_domain_error)  # step C: never reached
    except ApplicationError:
        return "step C failed"

    return "no failure"


async def orchestrator_raises_application_error_directly(ctx: Context) -> None:
    # The orchestrator itself raises ApplicationError without ever awaiting a
    # leaf. The top-level handle should still see it verbatim.
    msg = "orchestrator-level failure"
    raise ApplicationError(msg)


async def orchestrator_raises_value_error_directly(ctx: Context) -> None:
    # The orchestrator raises a plain Python exception. It is wrapped into
    # ApplicationError by the core executor before settling the run-level
    # promise, identically to a step.
    msg = "orchestrator-level plain failure"
    raise ValueError(msg)


# =============================================================================
# Codec layer: encode_error + deserialize_error collapse every class to
# ApplicationError. This is the single decoder used by both ctx.run and
# ctx.rpc, so pinning it here proves the "same on either dispatch path"
# claim once and for all.
# =============================================================================


def test_codec_encode_error_drops_class_keeps_message() -> None:
    err = PaymentDeclinedError("card declined for $42")
    encoded = encode_error(err)
    # On the wire there is no class identity -- just the rendered message.
    assert encoded == {"__type": "error", "message": "card declined for $42"}
    assert "PaymentDeclined" not in encoded["message"]


def test_codec_deserialize_error_rebuilds_as_application_error() -> None:
    # Whatever class was on the worker, the recovering side always rebuilds
    # an ApplicationError. This is what ``_decode_settled`` calls for both
    # ctx.run and ctx.rpc rejections.
    rebuilt = deserialize_error({"__type": "error", "message": "card declined"})
    assert isinstance(rebuilt, ApplicationError)
    assert rebuilt.message == "card declined"


@pytest.mark.parametrize(
    "exc",
    [
        ValueError("bad input"),
        RuntimeError("kaboom"),
        PaymentDeclinedError("declined"),
        FraudSuspectedError("fraud"),
        ApplicationError("already an app error"),
    ],
)
def test_codec_roundtrip_any_class_becomes_application_error(
    exc: Exception,
) -> None:
    rebuilt = deserialize_error(encode_error(exc))
    assert isinstance(rebuilt, ApplicationError)
    assert rebuilt.message == str(exc)


def test_codec_deserialize_error_unknown_shape_still_yields_application_error() -> None:
    # Defensive: a malformed/legacy payload still rehydrates as
    # ApplicationError so callers never face a non-ApplicationError surface.
    rebuilt = deserialize_error("just a string")
    assert isinstance(rebuilt, ApplicationError)
    assert "unknown error" in rebuilt.message


# =============================================================================
# End-to-end: ctx.run inside a workflow. The orchestrator sees
# ApplicationError regardless of what the step raised.
# =============================================================================


@pytest.mark.asyncio
async def test_ctx_run_value_error_surfaces_as_application_error() -> None:
    async with local() as r:
        r.register(orchestrator_catches_value_error_as_application_error)
        r.register(step_raises_value_error)

        caught, message = await r.run(
            "vh",
            orchestrator_catches_value_error_as_application_error,
        ).result()

    assert caught == "ApplicationError"
    assert message == "bad input from step_raises_value_error"


@pytest.mark.asyncio
async def test_ctx_run_domain_exception_surfaces_as_application_error() -> None:
    async with local() as r:
        r.register(orchestrator_catches_domain_error_as_application_error)
        r.register(step_raises_domain_error)

        caught, message = await r.run(
            "dh",
            orchestrator_catches_domain_error_as_application_error,
        ).result()

    # The custom PaymentDeclined raised by the step was *defined in this
    # very module*, but the orchestrator still only sees ApplicationError.
    assert caught == "ApplicationError"
    assert message == "card declined for $42"


@pytest.mark.asyncio
async def test_ctx_run_except_custom_class_does_not_match() -> None:
    """``except PaymentDeclined`` cannot catch a cross-boundary rejection."""
    async with local() as r:
        r.register(orchestrator_cannot_catch_domain_class)
        r.register(step_raises_domain_error)

        with pytest.raises(ApplicationError, match="card declined for"):
            # Because ``except PaymentDeclined`` did not match, the rejection
            # bubbles out and the top-level handle re-raises it.
            await r.run("nc", orchestrator_cannot_catch_domain_class).result()


@pytest.mark.asyncio
async def test_ctx_run_except_custom_subclass_does_not_match() -> None:
    # FraudSuspected is a PaymentDeclined locally, but the boundary drops
    # the entire chain: the orchestrator's ``except PaymentDeclined`` does
    # not catch the FraudSuspected raised by the step either.
    async with local() as r:
        r.register(orchestrator_cannot_catch_domain_subclass)
        r.register(step_raises_domain_subclass)

        with pytest.raises(ApplicationError, match="suspected fraud"):
            await r.run("ns", orchestrator_cannot_catch_domain_subclass).result()


@pytest.mark.asyncio
async def test_ctx_run_explicit_application_error_is_forwarded_verbatim() -> None:
    """``ApplicationError`` raised in a step is forwarded with its message intact."""
    async with local() as r:
        r.register(orchestrator_parses_tagged_application_error)
        r.register(step_raises_tagged_application_error)

        code, detail = await r.run(
            "tg",
            orchestrator_parses_tagged_application_error,
        ).result()

    # The tagged-message discrimination pattern -- the cross-boundary
    # equivalent of ``except NotFoundError``.
    assert code == "E_NOT_FOUND"
    assert detail == "user 42 does not exist"


@pytest.mark.asyncio
async def test_ctx_run_positional_discrimination_identifies_failing_step() -> None:
    # Knowing *which* step failed without class introspection: each step has
    # its own try block, so the block that raised tells you the step.
    async with local() as r:
        r.register(orchestrator_positional_discrimination)
        r.register(step_returns_ok)
        r.register(step_raises_value_error)
        r.register(step_raises_domain_error)

        result = await r.run("pd", orchestrator_positional_discrimination).result()

    assert result == "step B failed"


# =============================================================================
# Top-level: handle.result() re-raises ApplicationError, regardless of where
# the failure originated.
# =============================================================================


@pytest.mark.asyncio
async def test_handle_result_re_raises_uncaught_step_failure() -> None:
    # A leaf raises; the orchestrator does not catch it; ``handle.result()``
    # surfaces it to ``main`` as ApplicationError, identical message.
    async with local() as r:
        r.register(orchestrator_lets_value_error_escape)
        r.register(step_raises_value_error)

        with pytest.raises(ApplicationError) as excinfo:
            await r.run("ue", orchestrator_lets_value_error_escape).result()

    assert isinstance(excinfo.value, ApplicationError)
    assert excinfo.value.message == "bad input from step_raises_value_error"


@pytest.mark.asyncio
async def test_handle_result_re_raises_orchestrator_application_error() -> None:
    # The orchestrator itself raises ApplicationError (no step involved).
    # ``handle.result()`` sees it verbatim.
    async with local() as r:
        r.register(orchestrator_raises_application_error_directly)

        with pytest.raises(ApplicationError, match="orchestrator-level failure"):
            await r.run("oa", orchestrator_raises_application_error_directly).result()


@pytest.mark.asyncio
async def test_handle_result_wraps_orchestrator_plain_exception() -> None:
    # The orchestrator raises a plain Python exception. The core executor
    # wraps it into ApplicationError before settling the top-level promise,
    # so the caller still only ever observes ApplicationError.
    async with local() as r:
        r.register(orchestrator_raises_value_error_directly)

        with pytest.raises(ApplicationError) as excinfo:
            await r.run("op", orchestrator_raises_value_error_directly).result()

    assert isinstance(excinfo.value, ApplicationError)
    assert excinfo.value.message == "orchestrator-level plain failure"
