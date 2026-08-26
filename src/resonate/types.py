"""How the SDK encodes a durable function invocation into the wire records.

The records themselves -- :class:`~resonate_base.types.Value`,
:class:`~resonate_base.types.PromiseRecord` and friends -- live in
:mod:`resonate_base.types`. What is here is the layer above: the shape the SDK
packs into a promise's ``param`` (:class:`Args`, :class:`TaskData`), what it
reports back about a running invocation (:class:`Status`, :class:`Info`), and
the registration surface a worker exposes (:class:`DurableRegistry`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Protocol, overload

import msgspec

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Concatenate

    from resonate.context import Context
    from resonate_base.retry import RetryPolicy


class Args(msgspec.Struct, kw_only=True, frozen=True):
    args: tuple[Any, ...] = msgspec.field(default_factory=tuple)
    kwargs: dict[str, Any] = msgspec.field(default_factory=dict)


class TaskData(Args, kw_only=True, frozen=True):
    func: str
    version: int = msgspec.field(default=1)


Status = Literal["done", "suspended", "error"]


class Info(msgspec.Struct, frozen=True, kw_only=True):
    id: str
    parent_id: str
    origin_id: str
    branch_id: str
    timeout_at: int
    func_name: str
    tags: dict[str, str]


class DurableRegistry(Protocol):
    """Anywhere a durable function can be registered.

    The structural contract shared by :class:`resonate.resonate.Resonate`
    (the full client) and the serverless worker shims (e.g.
    :class:`resonate_aws.Resonate`). Code that only needs to *register*
    durable functions -- making them executable by pushed tasks, on a
    long-running worker and a serverless one alike -- accepts this protocol
    instead of a concrete client. Dispatching new runs (``run``/``rpc``)
    remains a full-client capability.

    The overloads mirror the implementations exactly (bare decorator and
    parameterized decorator), so both classes satisfy the protocol verbatim.
    """

    @overload
    def register[**P, T](
        self,
        fn: Callable[Concatenate[Context, P], T],
        *,
        name: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> Callable[Concatenate[Context, P], T]: ...
    @overload
    def register[**P, T](
        self,
        fn: None = None,
        *,
        name: str | None = None,
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> Callable[
        [Callable[Concatenate[Context, P], T]], Callable[Concatenate[Context, P], T]
    ]: ...
