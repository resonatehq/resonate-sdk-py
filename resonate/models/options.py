from dataclasses import dataclass


@dataclass(frozen=True)
class RunOptions:
    send_to: str = "default"
    version: int = 1
