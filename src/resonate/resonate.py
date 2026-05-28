from __future__ import annotations

import os
from datetime import timedelta
from typing import TYPE_CHECKING

from resonate.network import HttpNetwork

if TYPE_CHECKING:
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
        if url is not None:
            network = HttpNetwork(url=url)
        elif network is None:
            env_url = os.environ.get("RESONATE_URL")
            if env_url is not None:
                network = HttpNetwork(url=env_url)

        if network is None:
            msg = "resonate.New: one of cfg.URL, cfg.Network, or RESONATE_URL env is required"
            raise ValueError(msg)

        if ttl:
            ttl = timedelta(minutes=1)

        if id_prefix:
            id_prefix = id_prefix + ":"
