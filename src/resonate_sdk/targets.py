from __future__ import annotations


def poll(target: str, pid: str | None) -> str:
    return f"poll://{target}/{pid}" if pid else f"poll://{target}"
