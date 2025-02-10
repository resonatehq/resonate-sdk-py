from __future__ import annotations

import base64

from resonate.models.encoder import Encoder


class Base64Encoder(Encoder[str | None, str | None]):
    def encode(self, obj: str | None) -> str | None:
        return base64.b64encode(obj.encode()).decode() if obj is not None else None

    def decode(self, obj: str | None) -> str | None:
        return base64.b64decode(obj).decode() if obj is not None else None
