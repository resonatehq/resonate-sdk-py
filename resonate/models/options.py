from __future__ import annotations

from typing import NotRequired, TypedDict


class Options(TypedDict):
    send_to: NotRequired[str]
    version: NotRequired[int]
