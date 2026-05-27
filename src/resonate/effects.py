from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal

from resonate.codec import encode_error
from resonate.error import ResonateError
from resonate.types import PromiseCreateReq, PromiseSettleReq

if TYPE_CHECKING:
    from resonate.codec import Codec
    from resonate.send import Sender
    from resonate.types import PromiseRecord

# Mirrors Rust's ``tracing::info!(target: "resonate::validation", ...)``: the
# validation harness keys off this logger name rather than the module name.
logger = logging.getLogger("resonate.validation")


class Effects:
    """The two durable operations the SDK needs, built from a Sender and Codec.

    Maintains an internal cache of decoded :class:`PromiseRecord`s. Mirrors
    Rust's ``Effects``; the Rust ``DashMap`` becomes a plain ``dict`` because the
    SDK runs single-threaded on asyncio -- individual ``dict`` reads and writes
    are atomic and no operation holds the cache across an ``await`` (matching the
    way Rust never holds the ``DashMap`` lock across the network round-trip).
    """

    def __init__(
        self, sender: Sender, codec: Codec, preload: list[PromiseRecord]
    ) -> None:
        """Build Effects from a Sender, Codec, and optional preloaded promises.

        Each preloaded record is decoded into the cache; a record that fails to
        decode is silently skipped, mirroring Rust's ``if let Ok(decoded)``.
        """
        self.sender = sender
        self.codec = codec
        self.cache: dict[str, PromiseRecord] = {}
        for p in preload:
            try:
                decoded = codec.decode_promise(p)
            except ResonateError:
                continue
            self.cache[decoded.id] = decoded

    async def create_promise(self, req: PromiseCreateReq) -> PromiseRecord:
        """Create a durable promise, returning the decoded record.

        Idempotent: a cached record (from preload or a prior call) is returned
        without touching the network.
        """
        cached = self.cache.get(req.id)
        if cached is not None:
            return cached

        encoded_param = self.codec.encode(req.param.data)
        encoded_req = PromiseCreateReq(
            id=req.id,
            timeout_at=req.timeout_at,
            param=encoded_param,
            tags=req.tags,
        )

        # validation tracing
        scope = encoded_req.tags.get("resonate:scope")
        if scope == "local":
            invocation = "run"
        elif scope == "global":
            invocation = "rpc"
        else:
            invocation = "unknown"
        logger.info(
            "promise_create_request promise_id=%s invocation=%s",
            encoded_req.id,
            invocation,
        )

        record = await self.sender.promise_create(encoded_req)

        decoded = self.codec.decode_promise(record)
        self.cache[decoded.id] = decoded
        logger.info(
            "promise_create_response promise_id=%s invocation=%s state=%s",
            decoded.id,
            invocation,
            decoded.state,
        )
        return decoded

    async def settle_promise[T](
        self, id: str, result: T | ResonateError
    ) -> PromiseRecord:
        """Settle a durable promise with a result.

        Idempotent: a cached non-pending record is returned without touching the
        network. ``result`` is the ``Result[T]`` to settle with -- a plain value
        resolves the promise, a :class:`ResonateError` rejects it.
        """
        cached = self.cache.get(id)
        if cached is not None and cached.state != "pending":
            return cached

        state: Literal["resolved", "rejected"]
        value_data: Any
        if isinstance(result, ResonateError):
            state = "rejected"
            value_data = encode_error(result)
        else:
            state = "resolved"
            value_data = result

        encoded_value = self.codec.encode(value_data)
        req = PromiseSettleReq(id=id, state=state, value=encoded_value)

        logger.info("promise_settle_request promise_id=%s state=%s", req.id, req.state)
        record = await self.sender.promise_settle(req)

        decoded = self.codec.decode_promise(record)
        logger.info(
            "promise_settle_response promise_id=%s state=%s", decoded.id, decoded.state
        )
        self.cache[decoded.id] = decoded
        return decoded
