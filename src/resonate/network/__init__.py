from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from resonate.network._http import HttpNetwork
from resonate.network._local import LocalNetwork

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["HttpNetwork", "LocalNetwork", "Network"]


@runtime_checkable
class Network(Protocol):
    """The transport abstraction for all server communication.

    All communication between Resonate and the server (local or remote) flows
    through it as JSON strings.

    Mirrors Rust's ``Network`` trait. Methods that return ``Result<()>`` /
    ``Result<String>`` in Rust raise on error and return ``None`` / ``str``
    here. :class:`LocalNetwork` (in ``local.py``) is the in-process
    implementation backed by the :class:`ServerState` simulation;
    :class:`HttpNetwork` (in ``http.py``) talks to a Resonate server over HTTP.
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
