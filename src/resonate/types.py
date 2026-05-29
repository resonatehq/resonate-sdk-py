from __future__ import annotations

from typing import Any, Literal

import msgspec

from resonate.error import SerializationError

PromiseState = Literal[
    "pending",
    "resolved",
    "rejected",
    "rejected_canceled",
    "rejected_timedout",
]


class Value(msgspec.Struct, omit_defaults=True, kw_only=True, frozen=True):
    """The wire format for data crossing the durability boundary.

    On the wire, ``data`` is a base64-encoded JSON string (or omitted).
    Internally, after decoding by the Codec, ``data`` holds the deserialized
    value.

    Both fields default to ``None`` and, with ``omit_defaults=True``, are left
    out when encoding -- the equivalent of serde's ``skip_serializing_if``.

    Note: Python uses ``None`` for JSON ``null``, so an absent ``data`` field
    and an explicit ``null`` collapse to the same value. This matches the Rust
    accessors, which all fall back to ``null``.
    """

    headers: dict[str, str] | None = msgspec.field(default=None)
    data: Any | None = msgspec.field(default=None)

    @classmethod
    def from_serializable(cls, val: Any) -> Value:
        """Build a ``Value`` whose data is ``val`` converted to JSON builtins.

        Mirrors ``serde_json::to_value``: ``val`` is normalized into JSON-shaped
        builtins (dicts, lists, scalars). Raises :class:`SerializationError` if
        it cannot be serialized, matching Rust's ``serde_json::to_value(val)?``.
        """
        try:
            data = msgspec.to_builtins(val)
        except (TypeError, ValueError, msgspec.MsgspecError) as exc:
            raise SerializationError(exc) from exc
        return cls(headers=None, data=data)

    def decode[T](self, type: type[T]) -> T:
        """Deserialize the data field into ``type``.

        Decoding ``None`` (an absent or null ``data``) into a non-optional type
        raises :class:`SerializationError`, matching the Rust behaviour of
        deserializing ``null`` (a ``serde_json::Error`` becomes
        ``Error::SerializationError``).
        """
        try:
            return msgspec.convert(self.data, type)
        except (TypeError, ValueError, msgspec.MsgspecError) as exc:
            raise SerializationError(exc) from exc

    @classmethod
    def from_wire(cls, raw: Any) -> Value:
        """Construct a ``Value`` from a parsed-JSON value.

        Mirrors the Rust custom ``Deserialize``:

        * ``None`` (JSON null) -> an empty ``Value``.
        * a mapping -> ``headers`` and ``data`` are read from the map; headers
          that are not a ``str -> str`` map are dropped.
        * any other value -> treated as the raw ``data``.
        """
        match raw:
            case None:
                return cls()
            case dict():
                headers: dict[str, str] | None
                try:
                    headers = (
                        msgspec.convert(raw["headers"], dict[str, str])
                        if "headers" in raw
                        else None
                    )
                except (TypeError, ValueError, msgspec.MsgspecError):
                    headers = None
                return cls(headers=headers, data=raw.get("data"))
            case _:
                return cls(headers=None, data=raw)


class PromiseRecord(msgspec.Struct, rename="camel", kw_only=True, frozen=True):
    """A durable promise record as stored by the server.

    ``kw_only=True`` lets the defaulted fields keep their Rust declaration
    order (and hence wire order) while leaving ``timeout_at`` required even
    though it follows defaulted fields. The defaults mirror Rust's
    ``#[serde(default)]``.
    """

    id: str
    state: Literal[
        "pending",
        "resolved",
        "rejected",
        "rejected_canceled",
        "rejected_timedout",
    ]
    param: Value = msgspec.field(default_factory=Value)
    value: Value = msgspec.field(default_factory=Value)
    tags: dict[str, str] = msgspec.field(default_factory=dict)
    timeout_at: int
    created_at: int = msgspec.field(default=0)
    settled_at: int | None = msgspec.field(default=None)


class TaskRecord(msgspec.Struct, rename="camel", kw_only=True, frozen=True):
    """A task record as returned by the server.

    ``resumes`` mirrors Rust's ``#[serde(default)] serde_json::Value`` (defaults
    to ``null`` when absent); the union narrows it to the shapes the protocol
    actually uses (``string[] | number | boolean``).
    """

    id: str
    state: Literal["pending", "acquired", "suspended", "halted", "fulfilled"]
    version: int
    resumes: list[str] | int | bool | None = msgspec.field(default=None)
    ttl: int | None = msgspec.field(default=None)
    pid: str | None = msgspec.field(default=None)


class ScheduleRecord(msgspec.Struct, rename="camel", kw_only=True, frozen=True):
    """A schedule record as returned by the server."""

    id: str
    cron: str
    promise_id: str
    promise_timeout: int
    promise_param: Value = msgspec.field(default_factory=Value)
    promise_tags: dict[str, str] = msgspec.field(default_factory=dict)
    created_at: int = msgspec.field(default=0)


class PromiseCreateReq(msgspec.Struct, rename="camel", kw_only=True, frozen=True):
    id: str
    timeout_at: int = msgspec.field(default=0)
    param: Value = msgspec.field(default_factory=Value)
    tags: dict[str, str] = msgspec.field(default_factory=dict)


class PromiseSettleReq(msgspec.Struct, rename="camel", kw_only=True, frozen=True):
    id: str
    state: Literal["resolved", "rejected", "rejected_canceled"]
    value: Value


class PromiseRegisterCallbackData(
    msgspec.Struct, rename="camel", kw_only=True, frozen=True
):
    awaited: str
    awaiter: str


# =============================================================================
# SDK-INTERNAL TYPES (not part of the wire protocol)
# =============================================================================


class Args(msgspec.Struct, kw_only=True, frozen=True):
    """The packed user arguments of a durable call: positional + keyword.

    Produced by :meth:`~resonate.durable.DurableFunction.pack_args`, this is the
    single serializable slot able to round-trip Python ``*args`` / ``**kwargs``
    through a durable promise's one ``param`` field. Stored verbatim as a local
    child's promise param and embedded (flattened) into :class:`TaskData` for
    root / remote dispatch. Both fields default to empty when absent.
    """

    args: tuple[Any, ...] = msgspec.field(default_factory=tuple)
    kwargs: dict[str, Any] = msgspec.field(default_factory=dict)


class TaskData(Args, kw_only=True, frozen=True):
    """Parsed task data from the root promise param.

    The wire shape is ``{"func": ..., "args": [...], "kwargs": {...}}``: ``func``
    is the registered function name and ``args`` / ``kwargs`` carry the call,
    each defaulting to empty when absent.

    NOTE: unlike most of the SDK this intentionally does **not** mirror the Rust
    SDK, where the dispatch params are a single untyped ``serde_json::Value``.
    Python instead enforces the ``args: list`` / ``kwargs: dict`` shape so a
    malformed payload is rejected at decode time rather than at bind time.
    """

    func: str
    version: int = msgspec.field(default=1)


# Execution status returned from Core methods. Mirrors Rust's ``Status`` enum.
Status = Literal["done", "suspended", "error"]
