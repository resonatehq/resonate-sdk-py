from __future__ import annotations

from typing import Any, Literal

import msgspec

from resonate.error import ResonateError, SerializationError


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

    def headers_or_empty(self) -> dict[str, str]:
        """Return the headers, defaulting to an empty map if absent."""
        return {} if self.headers is None else self.headers

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
    next_run_at: int = msgspec.field(default=0)
    last_run_at: int | None = msgspec.field(default=None)


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


class Done[T](msgspec.Struct, frozen=True, kw_only=True):
    """``Outcome`` variant: the function completed, successfully or with an error.

    Mirrors Rust's ``Outcome::Done(Result<T>)``. ``result`` holds either the
    success value (a ``T``) or a :class:`~resonate.error.ResonateError`, the two
    arms of ``Result[T] = T | ResonateError``.
    """

    result: T | ResonateError


class Suspended(msgspec.Struct, frozen=True, kw_only=True):
    """``Outcome`` variant: the function cannot proceed.

    Mirrors Rust's ``Outcome::Suspended { remote_todos }`` -- the function has
    unresolved remote dependencies, listed by id in ``remote_todos``.
    """

    remote_todos: list[str]


# The result of executing a durable function. Mirrors the Rust enum
# ``Outcome<T> { Done(Result<T>), Suspended { remote_todos: Vec<String> } }``.
type Outcome[T] = Done[T] | Suspended


class TaskData(msgspec.Struct, kw_only=True, frozen=True):
    """Parsed task data from the root promise param.

    Rust declares no ``rename_all``, so the field names stay ``func`` / ``args``
    on the wire. ``args`` mirrors Rust's ``#[serde(default)] serde_json::Value``:
    it defaults to JSON ``null`` (``None``) when absent and is still emitted
    when ``None``.
    """

    func: str
    args: Any | None = msgspec.field(default=None)

    @staticmethod
    def into_value(func: str, args: Any) -> Value:
        """Encode ``{"func": ..., "args": ...}`` into a :class:`Value` for dispatch.

        Mirrors ``TaskData::into_value``. Raises :class:`SerializationError` if
        ``args`` cannot be serialized, matching Rust's ``serde_json::to_value``.
        """
        return Value.from_serializable({"func": func, "args": args})


# Execution status returned from Core methods. Mirrors Rust's ``Status`` enum.
Status = Literal["done", "suspended", "error"]
