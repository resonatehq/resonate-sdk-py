from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from resonate.network.http import HttpNetwork
from resonate.network.local import LocalNetwork
from resonate.network.nats import NatsNetwork
from resonate.network.postgres import PostgresNetwork

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["HttpNetwork", "LocalNetwork", "NatsNetwork", "Network", "PostgresNetwork"]


class Network(Protocol):
    """The transport abstraction for all server communication.

    All communication between Resonate and the server (local or remote) flows
    through it as JSON strings. Methods raise on error.
    """

    def pid(self) -> str: ...
    def group(self) -> str: ...
    def unicast(self) -> str: ...
    def anycast(self) -> str: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, req: str) -> str: ...
    def recv(self, callback: Callable[[str], None]) -> None: ...
    def target_resolver(self, target: str) -> str: ...
