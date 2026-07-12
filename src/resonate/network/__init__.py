from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from resonate.network.http import HttpNetwork
from resonate.network.local import LocalNetwork
from resonate.network.nats import NatsNetwork
from resonate.network.postgres import PostgresNetwork

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = [
    "HttpNetwork",
    "LocalNetwork",
    "NatsNetwork",
    "Network",
    "PostgresNetwork",
    "network_for_url",
]


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


def network_for_url(
    url: str,
    pid: str | None = None,
    group: str | None = None,
    auth: str | None = None,
    *,
    send_only: bool = False,
) -> Network:
    """Build the network implied by a URL's scheme.

    A ``postgres://`` / ``postgresql://`` URL is a resonate-pg DSN and selects
    :class:`PostgresNetwork` (credentials travel inside the DSN, so ``auth``
    does not apply); anything else is a Resonate server URL for
    :class:`HttpNetwork`. This is the single place scheme dispatch happens:
    the full client (:class:`resonate.resonate.Resonate`) and the serverless
    shims (``resonate.faas``) both select their network here, so a URL means
    the same thing everywhere.

    ``send_only`` builds a network that never listens for incoming messages,
    for serverless workers that are *pushed* work over HTTP.
    """
    if url.startswith(("postgres://", "postgresql://")):
        return PostgresNetwork(url, pid=pid, group=group, send_only=send_only)
    return HttpNetwork(url=url, pid=pid, group=group, auth=auth, send_only=send_only)
