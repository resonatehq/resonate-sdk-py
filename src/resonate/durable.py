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

    def pack_args(self, *args: Any, **kwargs: Any) -> Any:
        """Validate a call and pack it into a serializable promise param.

        Called at dispatch (``ctx.run``) time. Returns ``None`` when the function
        takes no user arguments, otherwise an ``{"args": [...], "kwargs": {...}}``
        envelope -- the single slot able to round-trip Python ``*args``/
        ``**kwargs`` through the durable promise's one ``param`` field. The
        injected ``Context`` is never part of the payload.

        Raises :class:`ApplicationError` if the call does not match the signature.
        """
        try:
            self._user_sig.bind(*args, **kwargs)
        except TypeError as exc:
            msg = f"{self.name}: {exc}"
            raise ApplicationError(msg) from exc

        if not args and not kwargs:
            return None
        return {"args": list(args), "kwargs": kwargs}

    async def invoke(self, ctx: Context, payload: Any) -> Any:
        """Re-bind ``payload`` to the signature and execute the function.

        ``payload`` is whatever :meth:`pack_args` produced, after a round trip
        through the durability boundary -- so on recovery its contents are JSON
        builtins. Each argument is coerced back to its declared type before the
        call, mirroring Go's ``coerceArgs`` JSON round-trip. ``ctx`` is always
        injected as the first positional argument. Synchronous and ``async``
        user functions are both supported.
        """
        args, kwargs = _unpack(payload)
        try:
            bound = self._user_sig.bind(*args, **kwargs)
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
    """Resolve ``fn``'s signature, evaluating string annotations when possible.

    ``eval_str=True`` turns the string annotations produced by ``from __future__
    import annotations`` back into real classes, so :meth:`DurableFunction._coerce`
    can pass them straight to :func:`msgspec.convert`. If a name cannot be
    resolved we fall back to the raw (string) annotations -- coercion then
    silently skips them, which matches the "unannotated -> pass through" rule.
    """
    try:
        return inspect.signature(fn, eval_str=True)
    except (NameError, TypeError):
        return inspect.signature(fn)


def _unpack(payload: Any) -> tuple[list[Any], dict[str, Any]]:
    """Reverse :meth:`DurableFunction.pack_args`.

    Recognizes the ``{"args", "kwargs"}`` envelope; any other value is treated as
    a single positional argument (e.g. a bare value dispatched by a non-Python
    caller via RPC).
    """
    if payload is None:
        return [], {}
    if isinstance(payload, dict) and payload and set(payload) <= {"args", "kwargs"}:
        return list(payload.get("args") or []), dict(payload.get("kwargs") or {})
    return [payload], {}


def _convert[T](value: Any, annotation: type[T], fn_name: str) -> Any:
    """Coerce ``value`` to ``annotation`` via msgspec, as the codec does elsewhere."""
    try:
        return msgspec.convert(value, annotation)
    except (TypeError, ValueError, msgspec.MsgspecError) as exc:
        error = SerializationError(exc)
        error.add_note(f"while binding arguments for {fn_name}")
        raise error from exc
