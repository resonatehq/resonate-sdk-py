from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import msgspec

from resonate.error import DecodingError, ServerError

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.network import Network

logger = logging.getLogger(__name__)


# =============================================================================
# Incoming messages (recv path)
# =============================================================================


class TaskRef(msgspec.Struct, kw_only=True, frozen=True):
    """A task reference inside an execute message.

    ``version`` mirrors Rust's ``#[serde(default)]`` (defaults to ``0``).
    """

    id: str
    version: int = msgspec.field(default=0)


class ExecuteData(msgspec.Struct, kw_only=True, frozen=True):
    task: TaskRef


class ExecuteMsg(
    msgspec.Struct, tag="execute", tag_field="kind", kw_only=True, frozen=True
):
    """Execute message -- server tells this worker to run a task.

    JSON shape: ``{ kind: "execute", data: { task: { id, version } } }``.
    """

    data: ExecuteData

    def task_id(self) -> str:
        """Task ID -- shorthand for ``data.task.id``."""
        return self.data.task.id

    def version(self) -> int:
        """Task version -- shorthand for ``data.task.version``."""
        return self.data.task.version


class UnblockData(msgspec.Struct, kw_only=True, frozen=True):
    promise: Any


class UnblockMsg(
    msgspec.Struct, tag="unblock", tag_field="kind", kw_only=True, frozen=True
):
    """Unblock message -- a promise this worker is waiting on has been settled.

    JSON shape: ``{ kind: "unblock", data: { promise: PromiseRecord } }``.
    """

    data: UnblockData

    def promise(self) -> Any:
        """Return the settled promise -- shorthand for ``data.promise``."""
        return self.data.promise


# A parsed incoming message from the network. Mirrors Rust's internally tagged
# ``Message`` enum (``#[serde(tag = "kind")]``).
Message = ExecuteMsg | UnblockMsg


# =============================================================================
# Envelope helpers
# =============================================================================


def _nested_str(value: Any, *keys: str) -> str:
    """Walk nested mapping ``keys`` and return the final string, or ``""``.

    Mirrors the Rust chains of ``.get(..).and_then(|v| v.as_str()).unwrap_or("")``:
    any missing key, non-mapping node, or non-string leaf collapses to ``""``.
    """
    for key in keys:
        if not isinstance(value, dict):
            return ""
        value = value.get(key)
    return value if isinstance(value, str) else ""


# =============================================================================
# Transport
# =============================================================================


class Transport:
    """Wrap a :class:`~resonate.network.Network` with JSON and correlation.

    Adds JSON serialization, deserialization, and correlation validation.
    Resonate and its sub-components use the transport -- never the raw network.

    Mirrors Rust's ``Transport``.
    """

    def __init__(self, network: Network) -> None:
        self._network = network

    async def send(self, kind: str, corr_id: str, body: str) -> Any:
        """Send an already-serialized request, returning the parsed response.

        Validates that ``response.kind == kind`` and
        ``response.head.corrId == corr_id``, raising :class:`ServerError`
        (code 500) on a mismatch and :class:`DecodingError` on invalid JSON.
        """
        logger.debug("transport send_req: %s", body)

        resp_str = await self._network.send(body)
        logger.debug("transport send_res: %s", resp_str)

        try:
            response = msgspec.json.decode(resp_str)
        except msgspec.DecodeError as exc:
            msg = f"invalid response JSON: {exc}, resp: {resp_str}"
            raise DecodingError(msg) from exc

        resp_kind = _nested_str(response, "kind")
        if resp_kind != kind:
            msg = f"response kind mismatch: expected '{kind}', got '{resp_kind}'"
            raise ServerError(500, msg)

        resp_corr = _nested_str(response, "head", "corrId")
        if resp_corr != corr_id:
            msg = f"response corrId mismatch: expected '{corr_id}', got '{resp_corr}'"
            raise ServerError(500, msg)

        return response

    def recv(self, callback: Callable[[Message], None]) -> None:
        """Register a callback for incoming messages.

        Parses JSON into a :class:`Message`, discards invalid messages (logging
        a warning), and forwards valid ones. Mirrors Rust's ``Transport::recv``.
        """

        def on_raw(raw: str) -> None:
            try:
                msg = msgspec.json.decode(raw, type=Message)
            except msgspec.MsgspecError as exc:
                logger.warning(
                    "failed to parse incoming message: %s; raw: %s", exc, raw
                )
                return
            logger.debug("transport recv: %s", raw)
            callback(msg)

        self._network.recv(on_raw)

    def network(self) -> Network:
        """Access the underlying network."""
        return self._network
