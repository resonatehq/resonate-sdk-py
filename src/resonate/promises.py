from __future__ import annotations

from typing import TYPE_CHECKING

from resonate.send import PromiseSearchResult, ScheduleCreateReq
from resonate.types import PromiseCreateReq, PromiseSettleReq, Value

if TYPE_CHECKING:
    from resonate.codec import Codec
    from resonate.core import SettleState
    from resonate.send import ScheduleSearchResult, Sender
    from resonate.types import PromiseRecord, ScheduleRecord


def encode_value(codec: Codec, value: Value) -> Value:
    """Encode a plaintext :class:`Value` for the wire via the codec.

    The single symmetric counterpart to the codec decode applied on the way back
    (:meth:`Codec.decode_promise`): a caller hands a *plaintext* ``Value`` (built
    with :meth:`~resonate.types.Value.from_serializable`) and the codec
    serializes, encrypts, and base64-encodes its ``data``. Keeps the
    :class:`~resonate.codec.Codec` the sole owner of encode/decode -- and so of
    encryption/decryption -- exactly mirroring
    :meth:`resonate.effects.ResonateEffects.create_promise`'s encode step for
    child promises and :meth:`Resonate._encode_create_req` for top-level
    run/rpc. ``data`` of ``None`` round-trips to an empty wire value.
    """
    return codec.encode(value.data)


class Promises:
    """Sub-client for promise operations. Mirrors Rust's ``Promises``."""

    def __init__(self, sender: Sender, codec: Codec) -> None:
        self.sender = sender
        self.codec = codec

    async def get(self, id: str) -> PromiseRecord:
        """Get a promise by ID."""
        record = await self.sender.promise_get(id)
        return self.codec.decode_promise(record)

    async def create(
        self,
        id: str,
        timeout_at: int,
        param: Value,
        tags: dict[str, str],
    ) -> PromiseRecord:
        """Create a promise."""
        record = await self.sender.promise_create(
            PromiseCreateReq(
                id=id,
                timeout_at=timeout_at,
                param=encode_value(self.codec, param),
                tags=tags,
            )
        )
        return self.codec.decode_promise(record)

    async def resolve(self, id: str, value: Value) -> PromiseRecord:
        """Resolve a promise."""
        return await self._settle(id, "resolved", value)

    async def reject(self, id: str, value: Value) -> PromiseRecord:
        """Reject a promise."""
        return await self._settle(id, "rejected", value)

    async def cancel(self, id: str, value: Value) -> PromiseRecord:
        """Cancel a promise (settles as ``rejected_canceled``)."""
        return await self._settle(id, "rejected_canceled", value)

    async def _settle(
        self,
        id: str,
        state: SettleState,
        value: Value,
    ) -> PromiseRecord:
        record = await self.sender.promise_settle(
            PromiseSettleReq(id=id, state=state, value=encode_value(self.codec, value))
        )
        return self.codec.decode_promise(record)

    async def register_listener(self, awaited: str, address: str) -> PromiseRecord:
        """Register a listener on a promise so ``address`` is notified when it settles."""
        record = await self.sender.promise_register_listener(awaited, address)
        return self.codec.decode_promise(record)

    async def search(
        self,
        state: str | None,
        tags: dict[str, str] | None,
        limit: int | None,
        cursor: str | None,
    ) -> PromiseSearchResult:
        """Search for promises matching optional state/tags filters."""
        result = await self.sender.promise_search(state, tags, limit, cursor)
        promises = [self.codec.decode_promise(p) for p in result.promises]
        return PromiseSearchResult(promises=promises, cursor=result.cursor)


class Schedules:
    """Sub-client for schedule operations. Mirrors Rust's ``Schedules``."""

    def __init__(self, sender: Sender, codec: Codec) -> None:
        self.sender = sender
        self.codec = codec

    async def create(
        self,
        id: str,
        cron: str,
        promise_id: str,
        promise_timeout: int,
        promise_param: Value,
    ) -> ScheduleRecord:
        """Create a schedule."""
        return await self.sender.schedule_create(
            ScheduleCreateReq(
                id=id,
                cron=cron,
                promise_id=promise_id,
                promise_timeout=promise_timeout,
                promise_param=encode_value(self.codec, promise_param),
                promise_tags={},
            )
        )

    async def get(self, id: str) -> ScheduleRecord:
        """Get a schedule by ID."""
        return await self.sender.schedule_get(id)

    async def delete(self, id: str) -> None:
        """Delete a schedule."""
        await self.sender.schedule_delete(id)

    async def search(
        self,
        tags: dict[str, str] | None,
        limit: int | None,
        cursor: str | None,
    ) -> ScheduleSearchResult:
        """Search for schedules matching optional tag filter."""
        return await self.sender.schedule_search(tags, limit, cursor)
