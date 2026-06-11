from __future__ import annotations

from typing import TYPE_CHECKING, Concatenate

from resonate.durable import DurableFunction
from resonate.error import AlreadyRegisteredError

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

    from resonate.context import Context
    from resonate.retry import RetryPolicy


class Registry:
    """Maps a ``(name, version)`` pair to a validated :class:`DurableFunction`.

    The version is explicit -- never "latest" -- so a lookup is deterministic
    regardless of what is registered afterwards: a task records its version in
    ``TaskData`` at create time and resolves the *same* implementation on every replay.

    Names stay explicit -- passed at register time, not derived from the Python
    function -- so they remain stable across renames. The callable is wrapped in
    a :class:`DurableFunction` at register time, which validates the ctx-first
    convention and owns the symmetric serialize/deserialize of its arguments and
    return value.
    """

    def __init__(self) -> None:
        self._by_key: dict[tuple[str, int], DurableFunction] = {}
        #: Per-function retry policy *override*, parallel to ``_by_key``. Read by
        #: Core when it dispatches a root task -- a remote dispatch carries no
        #: policy on the wire, so the worker resolves it here by ``(name,
        #: version)``. ``None`` (or absent) means "no override" -- Core falls back
        #: to its SDK-wide default.
        self._policy_by_key: dict[tuple[str, int], RetryPolicy | None] = {}
        #: Reverse of ``_by_key``: function identity -> its registered
        #: ``(name, version)``. Lets a caller holding the function *object*
        #: recover what to dispatch by -- the name for a by-object ``rpc`` and the
        #: version for a by-object ``run`` (both at the top level and on
        #: ``Context``). The same object registered under several keys keeps the
        #: last, matching last-writer semantics for a given identity.
        self._by_fn: dict[
            Callable[Concatenate[Context, ...], Any], tuple[str, int]
        ] = {}

    def register(
        self,
        name: str,
        fn: Callable[Concatenate[Context, ...], Any],
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        """Validate ``fn`` and store it under ``(name, version)``.

        ``retry_policy`` is a per-function *override*, applied to a *pure-leaf*
        failure when this function runs as a root task. ``None`` (the default)
        means no override -- Core falls back to its SDK-wide default. A workflow
        body never retries regardless of the policy.
        """
        if not name:
            msg = "name is required"
            raise ValueError(msg)
        if version < 1:
            msg = "version must be >= 1"
            raise ValueError(msg)
        key = (name, version)
        if key in self._by_key:
            raise AlreadyRegisteredError(name, version)
        self._by_key[key] = DurableFunction(fn)
        self._policy_by_key[key] = retry_policy
        self._by_fn[fn] = (name, version)

    def get(self, name: str, version: int = 1) -> DurableFunction | None:
        return self._by_key.get((name, version))

    def reverse(
        self, fn: Callable[Concatenate[Context, ...], Any]
    ) -> tuple[str, int] | None:
        """Return the ``(name, version)`` ``fn`` was registered under, or ``None``.

        The inverse of :meth:`get`: a caller holding the function *object* rather
        than its name (a by-object ``rpc`` dispatch, a by-object ``run``) uses
        this to recover what to dispatch by. ``None`` means the object was never
        registered, leaving the caller to fall back (e.g. to ``__name__``).
        """
        return self._by_fn.get(fn)

    def get_policy(self, name: str, version: int = 1) -> RetryPolicy | None:
        """Return the per-function policy override, or ``None`` if there is none."""
        return self._policy_by_key.get((name, version))
