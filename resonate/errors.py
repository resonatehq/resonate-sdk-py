from __future__ import annotations

from typing import Literal


class ResonateError(Exception):
    def __init__(self, msg: str, code: int) -> None:
        super().__init__(msg, code)


class UnknownError(ResonateError):
    def __init__(self, msg: str) -> None:
        super().__init__(msg, 0)


class StoreError(ResonateError):
    def __init__(
        self,
        msg: str,
        code: Literal[
            "STORE_UNAUTHORIZED",
            "STORE_PAYLOAD",
            "STORE_FORBIDDEN",
            "STORE_NOT_FOUND",
            "STORE_ALREADY_EXISTS",
            "STORE_INVALID_STATE",
            "STORE_ENCODER",
        ],
    ) -> None:
        match code:
            case "STORE_UNAUTHORIZED":
                num_code = 41
            case "STORE_PAYLOAD":
                num_code = 42
            case "STORE_FORBIDDEN":
                num_code = 43
            case "STORE_NOT_FOUND":
                num_code = 44
            case "STORE_ALREADY_EXISTS":
                num_code = 45
            case "STORE_INVALID_STATE":
                num_code = 46
            case "STORE_ENCODER":
                num_code = 47
        super().__init__(msg, num_code)


class ValidationError(ResonateError):
    def __init__(self, msg: str) -> None:
        super().__init__(msg, 100)
