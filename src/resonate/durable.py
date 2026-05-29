"""Runtime representation of user functions registered as durable.

Where Rust derives a ``Durable`` impl at compile time (the
``#[resonate_sdk::function]`` proc macro inspects the AST) and Go reflects on the
function value at runtime (``durableFunctionFor``), Python takes neither path:
we cannot rely on type annotations because Python's annotations are *optional*,
so a user-supplied ``def fn(ctx, x):`` carries no information that says ``ctx``
is a :class:`Context`. Instead we adopt a single convention: **every** durable
function -- workflow or leaf -- receives a :class:`Context` as its first
positional argument. The runtime never inspects annotations; it just strips the
first parameter from the user-facing signature and injects the ``Context`` on
each invocation.

The earlier three-way Info/Context/none split, mirrored from Rust, was dropped
together with the :class:`Info` type: under the new convention there is no
"leaf with metadata" arm, no "pure leaf with no env" arm, and no need for a
``DurableKind`` discriminator.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

import msgspec

from resonate.error import ApplicationError, SerializationError
from resonate.types import Args

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.context import Context


class DurableFunction:
    """A user function adapted for durable execution.

    Built once per function (at registration / ``ctx.run`` time, matching Go's
    "build fresh, reflection is cheap next to the network round-trip" note). It
    remembers how to re-bind serialized arguments on every (re-)invocation so a
    suspended-then-resumed call reconstructs the same Python call it was first
    dispatched with. The :class:`Context` is always injected as the first
    positional argument; only the *user* arguments round-trip through the
    durable promise's ``param`` field.
    """

    def __init__(self, fn: Callable[..., Any]) -> None:
        if not callable(fn):
            msg = f"expected a callable, got {type(fn).__name__}"
            raise ApplicationError(msg)

        name = getattr(fn, "__name__", "unknown")
        sig = _signature(fn)
        params = list(sig.parameters.values())
        if not params:
            msg = (
                f"{name}: durable function must accept a Context as its first argument"
            )
            raise ApplicationError(msg)

        self._fn = fn
        #: Signature of the *user* arguments -- the leading ``ctx`` parameter
        #: removed. The runtime owns the ``ctx`` slot and never lets the caller
        #: address it through ``pack_args``.
        self._user_sig = sig.replace(parameters=params[1:])
        #: Source name of the function (used for child context ``func_name`` and
        #: error messages). The registry key is supplied separately at register
        #: time, mirroring Go.
        self.name: str = name

    def pack_args(self, *args: Any, **kwargs: Any) -> Args:
        """Validate a call and pack it into a serializable :class:`Args`.

        Called at dispatch (``ctx.run``) time. Returns an :class:`Args` -- the
        single slot able to round-trip Python ``*args``/``**kwargs`` through the
        durable promise's one ``param`` field. An empty call yields ``Args()``
        (both fields default to empty). The injected ``Context`` is never part of
        the payload.

        Raises :class:`ApplicationError` if the call does not match the signature.
        """
        try:
            self._user_sig.bind(*args, **kwargs)
        except TypeError as exc:
            msg = f"{self.name}: {exc}"
            raise ApplicationError(msg) from exc

        return Args(args=args, kwargs=kwargs)

    async def invoke(self, ctx: Context, packed: Args) -> Any:
        """Re-bind ``packed`` to the signature and execute the function.

        ``packed`` is the :class:`Args` :meth:`pack_args` produced (for a local
        child) or the :class:`~resonate.types.TaskData` Core decoded from the root
        promise param (for the root) -- ``TaskData`` is an ``Args``, so both reach
        here through the same typed slot. On recovery its ``args``/``kwargs`` hold
        JSON builtins, so each argument is coerced back to its declared type before
        the call, mirroring Go's ``coerceArgs`` JSON round-trip. ``ctx`` is always
        injected as the first positional argument. Synchronous and ``async`` user
        functions are both supported.
        """
        try:
            bound = self._user_sig.bind(*packed.args, **packed.kwargs)
        except TypeError as exc:
            msg = f"{self.name}: {exc}"
            raise ApplicationError(msg) from exc
        # Snapshot which arguments the payload actually supplied *before*
        # ``apply_defaults`` fills the rest -- only the supplied ones need coercion.
        provided = set(bound.arguments)
        bound.apply_defaults()
        self._coerce(bound, provided)

        result = self._fn(ctx, *bound.args, **bound.kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    def _coerce(self, bound: inspect.BoundArguments, provided: set[str]) -> None:
        """Coerce each *supplied* argument to its declared type, in place.

        Only arguments named in ``provided`` are coerced. Defaults filled by
        ``apply_defaults`` are live Python objects that never crossed the
        serialization boundary, so they already have the right type -- coercing
        them would wrongly reject legitimate sentinel defaults (e.g.
        ``_MISSING = object()``) against their declared annotation.
        """
        for name, value in list(bound.arguments.items()):
            if name not in provided:
                continue
            param = self._user_sig.parameters[name]
            annotation = param.annotation
            # No annotation, ``Any``, or an unresolved string: pass through.
            if (
                annotation is inspect.Parameter.empty
                or annotation is Any
                or isinstance(annotation, str)
            ):
                continue

            if param.kind is inspect.Parameter.VAR_POSITIONAL:
                bound.arguments[name] = tuple(
                    _convert(v, annotation, self.name) for v in value
                )
            elif param.kind is inspect.Parameter.VAR_KEYWORD:
                bound.arguments[name] = {
                    k: _convert(v, annotation, self.name) for k, v in value.items()
                }
            else:
                bound.arguments[name] = _convert(value, annotation, self.name)


def _signature(fn: Callable[..., Any]) -> inspect.Signature:
    """Resolve ``fn``'s signature, evaluating string annotations per parameter.

    ``from __future__ import annotations`` turns every annotation into a string;
    :meth:`DurableFunction._coerce` needs the real class to hand to
    :func:`msgspec.convert`. ``inspect.signature(fn, eval_str=True)`` would do
    that, but it resolves *all* annotations or *none*: a single name unavailable
    at runtime aborts the whole resolution. That is a footgun for the standard
    convention of importing :class:`~resonate.context.Context` only under
    ``TYPE_CHECKING`` -- a user who writes ``def fn(ctx: Context, x: int)`` would
    find ``Context`` unresolvable at runtime and silently lose coercion of ``x``
    as collateral.

    So resolve each annotation independently: an unresolvable one is left as its
    raw string (which :meth:`DurableFunction._coerce` treats as pass-through),
    while its resolvable siblings still drive coercion. The leading ``ctx``
    parameter is stripped before coercion anyway, so its annotation never has to
    resolve.
    """
    sig = inspect.signature(fn)
    globalns = getattr(fn, "__globals__", {})
    params = [
        p.replace(annotation=_resolve_annotation(p.annotation, globalns))
        for p in sig.parameters.values()
    ]
    return sig.replace(parameters=params)


def _resolve_annotation(annotation: Any, globalns: dict[str, Any]) -> Any:
    """Evaluate one string annotation, leaving it as-is if it cannot resolve.

    Mirrors what ``eval_str=True`` does internally (``eval`` against the
    function's globals) but tolerant per annotation: a name missing at runtime
    yields the raw string -- :meth:`DurableFunction._coerce` then skips it, the
    same as a genuinely unannotated parameter.
    """
    if not isinstance(annotation, str):
        return annotation
    try:
        return eval(annotation, globalns)  # noqa: S307
    except (NameError, AttributeError, SyntaxError, TypeError):
        return annotation


def _convert[T](value: Any, annotation: type[T], fn_name: str) -> Any:
    """Coerce ``value`` to ``annotation`` via msgspec, as the codec does elsewhere."""
    try:
        return msgspec.convert(value, annotation)
    except (TypeError, ValueError, msgspec.MsgspecError) as exc:
        error = SerializationError(exc)
        error.add_note(f"while binding arguments for {fn_name}")
        raise error from exc
