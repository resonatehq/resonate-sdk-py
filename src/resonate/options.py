from __future__ import annotations


def is_url(s: str) -> bool:
    return "://" in s
