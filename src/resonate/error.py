from __future__ import annotations


class ResonateError(Exception):
    """Top-level error type for the Resonate SDK."""


class FunctionNotFoundError(ResonateError):
    def __init__(self, name: str, version: int = 1) -> None:
        self.name = name
        self.version = version
        super().__init__(f"function not found: {name} (version {version})")


class AlreadyRegisteredError(ResonateError):
    def __init__(self, name: str, version: int = 1) -> None:
        self.name = name
        self.version = version
        super().__init__(
            f"function '{self.name}' (version {self.version}) is already registered"
        )


class ServerError(ResonateError):
    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"server error (code={self.code}): {self.message}")


class EncodingError(ResonateError):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(f"encoding error: {self.message}")


class DecodingError(ResonateError):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(f"decoding error: {self.message}")


class SerializationError(ResonateError):
    def __init__(self, error: Exception) -> None:
        self.error = error
        super().__init__(f"serialization error: {error}")


class HttpError(ResonateError):
    def __init__(self, error: Exception) -> None:
        self.error = error
        super().__init__(f"http error: {error}")


class Base64DecodeError(ResonateError):
    def __init__(self, error: Exception) -> None:
        self.error = error
        super().__init__(f"base64 decode error: {error}")


class Utf8Error(ResonateError):
    def __init__(self, error: Exception) -> None:
        self.error = error
        super().__init__(f"utf8 error: {error}")


class IoError(ResonateError):
    def __init__(self, error: Exception) -> None:
        self.error = error
        super().__init__(f"io error: {error}")


class SuspendedError(BaseException):
    """Signals that an execution has suspended.

    Extends ``BaseException`` so that a ``try/except Exception`` does not
    swallow it.
    """

    def __init__(self) -> None:
        super().__init__("execution suspended")


class AlreadySettledError(ResonateError):
    def __init__(self) -> None:
        super().__init__("promise already settled")


class JoinError(ResonateError):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(f"task join error: {self.message}")


class ApplicationError(ResonateError):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class TimeoutError(ResonateError):
    def __init__(self) -> None:
        super().__init__("timeout")
