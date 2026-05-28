"""Runtime representation of user functions registered as durable.

Where Rust derives a ``Durable`` impl at compile time (the
``#[resonate_sdk::function]`` proc macro inspects the AST) and Go reflects on the
function value at runtime (``durableFunctionFor``), Python takes Go's path: we
introspect the signature once with :mod:`inspect` and classify the function by
its first parameter's type.

Kind detection mirrors the Rust macro doc comment exactly:

* ``Context`` -> Workflow
* ``Info``    -> leaf with metadata
* anything else -> pure leaf

Unlike Go (which only distinguishes "has ``*Context``" from "leaf"), we keep the
Rust three-way split so leaf functions can opt into read-only :class:`Info`
metadata. As in Rust, ``KIND`` still collapses to two values -- both leaf shapes
are ``"function"`` -- and only the env injection differs.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Literal

import msgspec

from resonate.error import ApplicationError, SerializationError
from resonate.info import Info

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.context import Context
    from resonate.types import DurableKind


# The execution environment passed to a durable function.
#
# - ``Info`` -- leaf functions receive read-only metadata.
# - ``Context`` -- workflow functions receive a full context for sub-tasks.
#
# Mirrors Rust's ``ExecutionEnv`` enum. Where Rust wraps each reference in a
# newtype variant (``Function(&Info)`` / ``Workflow(&Context)``), Python folds
# the two into a union: ``Info`` and ``Context`` are already distinct types, so
# no wrapper is needed to tell them apart.
type ExecutionEnv = Info | Context


def into_context(env: ExecutionEnv) -> Context:
    """Extract the ``Context`` reference, raising if this is a ``Function`` env."""
    # ``ExecutionEnv`` is exactly ``Info | Context``, so "not an Info" is the
    # Context arm. Checked this way to avoid importing Context at runtime --
    # context.py imports this module for ``run``, so a runtime import here would
    # close an import cycle.
    assert not isinstance(env, Info), "expected Workflow ExecutionEnv, got Function"
    return env


def into_info(env: ExecutionEnv) -> Info:
    """Extract the ``Info`` reference, raising if this is a ``Workflow`` env."""
    assert isinstance(env, Info), "expected Function ExecutionEnv, got Workflow"
    return env


type _EnvParam = Literal["info", "context"] | None


class DurableFunction:
    """A user function adapted for durable execution.

    Built once per function (at registration / ``ctx.run`` time, matching Go's
    "build fresh, reflection is cheap next to the network round-trip" note). It
    remembers how to inject the env and how to re-bind serialized arguments on
    every (re-)invocation, so a suspended-then-resumed call reconstructs the same
    Python call it was first dispatched with.
    """

    def __init__(self, fn: Callable[..., Any]) -> None:
        if not callable(fn):
            msg = f"expected a callable, got {type(fn).__name__}"
            raise ApplicationError(msg)

        sig = _signature(fn)
        env_param, user_sig = _classify(sig)

        self._fn = fn
        self._env_param = env_param
        #: Signature of the *user* arguments -- the env parameter, if any, removed.
        self._user_sig = user_sig
        #: Source name of the function (used for child context ``func_name`` and
        #: error messages). The registry key is supplied separately at register
        #: time, mirroring Go.
        self.name: str = getattr(fn, "__name__", "unknown")
        #: ``"workflow"`` when the first parameter is ``Context``, else
        #: ``"function"`` -- both pure and ``Info`` leaves are functions.
        self.kind: DurableKind = "workflow" if env_param == "context" else "function"

    def pack_args(self, *args: Any, **kwargs: Any) -> Any:
        """Validate a call and pack it into a serializable promise param.

        Called at dispatch (``ctx.run``) time. Returns ``None`` when the function
        takes no user arguments, otherwise an ``{"args": [...], "kwargs": {...}}``
        envelope -- the single slot able to round-trip Python ``*args``/
        ``**kwargs`` through the durable promise's one ``param`` field. The
        injected env parameter is never part of the payload.

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

    async def invoke(self, env: ExecutionEnv, payload: Any) -> Any:
        """Re-bind ``payload`` to the signature and execute the function.

        ``payload`` is whatever :meth:`pack_args` produced, after a round trip
        through the durability boundary -- so on recovery its contents are JSON
        builtins. Each argument is coerced back to its declared type before the
        call, mirroring Go's ``coerceArgs`` JSON round-trip. Synchronous and
        ``async`` user functions are both supported.
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

        match self._env_param:
            case "context":
                result = self._fn(into_context(env), *bound.args, **bound.kwargs)
            case "info":
                result = self._fn(into_info(env), *bound.args, **bound.kwargs)
            case None:
                result = self._fn(*bound.args, **bound.kwargs)

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


def durable_function_for(fn: Callable[..., Any]) -> DurableFunction:
    """Validate ``fn`` and build its :class:`DurableFunction`.

    Mirrors Go's ``durableFunctionFor``. Raises :class:`ApplicationError` if
    ``fn`` is not callable.
    """
    return DurableFunction(fn)


def _signature(fn: Callable[..., Any]) -> inspect.Signature:
    """Resolve ``fn``'s signature, evaluating string annotations when possible.

    ``eval_str=True`` turns the string annotations produced by ``from __future__
    import annotations`` back into real classes, so the kind check can match
    ``Info`` by identity. If a name cannot be resolved we fall back to the raw
    (string) annotations; either way :func:`_env_param_of` matches ``Context`` by
    name.
    """
    try:
        return inspect.signature(fn, eval_str=True)
    except (NameError, TypeError):
        return inspect.signature(fn)


def _classify(sig: inspect.Signature) -> tuple[_EnvParam, inspect.Signature]:
    """Detect the env parameter and return it with the user-facing signature."""
    params = list(sig.parameters.values())
    if params:
        env_param = _env_param_of(params[0].annotation)
        if env_param is not None:
            return env_param, sig.replace(parameters=params[1:])
    return None, sig


def _env_param_of(annotation: Any) -> _EnvParam:
    """Classify a first-parameter annotation (mirrors the Rust macro).

    ``Info`` is matched by identity. ``Context`` is matched by *name*: this
    module cannot import ``Context`` at runtime without closing an import cycle
    (context.py imports it for ``run``), so the trailing-path-segment check --
    the same name-based test Rust's ``is_reference_to`` performs -- covers both
    a resolved ``Context`` class and an unresolved string annotation.
    """
    if annotation is Info:
        return "info"
    name = (
        annotation
        if isinstance(annotation, str)
        else getattr(annotation, "__name__", None)
    )
    if not isinstance(name, str):
        return None
    tail = name.rsplit(".", maxsplit=1)[-1].strip()
    if tail == "Context":
        return "context"
    if tail == "Info":
        return "info"
    return None


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
