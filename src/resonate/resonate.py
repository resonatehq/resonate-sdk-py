from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import timedelta

    from resonate.codec import Encryptor
    from resonate.heartbeat import Heartbeat
    from resonate.network import Network


class Resonate:
    def __init__(
        self,
        url: str | None = None,
        network: Network | None = None,
        heartbeat: Heartbeat | None = None,
        encryptor: Encryptor | None = None,
        ttl: timedelta | None = None,
        id_prefix: str | None = None,
        token: str | None = None,
    ) -> None:
        pass
