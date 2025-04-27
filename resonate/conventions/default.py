from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from resonate.models.retry_policy import RetryPolicy
    from resonate.options import Options
    from resonate.registry import Registry


@dataclass
class Local:
    func_: Callable | str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    opts: Options
    registry: Registry

    def __post_init__(self) -> None:
        # Initially, timeout is set to the parent context timeout. This is the upper bound for the timeout.
        self._max_timeout = self.opts.timeout

        # Set the func and version based on the type of func_.
        match self.func_:
            case str():
                self.func, version = self.registry.get(self.func_, self.opts.version)
            case Callable():
                self.func, version = self.func_, self.registry.latest(self.func_) or 1

        assert version > 0
        self.opts = self.opts.merge(version=version)

    @property
    def id(self) -> str:
        return self.opts.id

    def options(
        self,
        durable: bool | None,
        id: str | None,
        idempotency_key: str | Callable[[str], str] | None,
        retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] | None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Local:
        if timeout:
            timeout = min(timeout, self._max_timeout)
        if version and isinstance(self.func_, str):
            self.func, version = self.registry.get(self.func_, version)

        self.opts = self.opts.merge(durable=durable, id=id, idempotency_key=idempotency_key, retry_policy=retry_policy, tags=tags, timeout=timeout, version=version)
        return self


@dataclass
class Remote:
    func: Callable | str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    opts: Options
    registry: Registry

    def __post_init__(self) -> None:
        # Initially, timeout is set to the parent context timeout. This is the upper bound for the timeout.
        self._max_timeout = self.opts.timeout

        # Set the func and version based on the type of func.
        match self.func:
            case str():
                func, version = self.func, self.registry.latest(self.func) or 1
            case Callable():
                func, version = self.registry.get(self.func, self.opts.version)

        assert version > 0
        self.data = {"func": func, "args": self.args, "kwargs": self.kwargs, "version": version}

    @property
    def id(self) -> str:
        return self.opts.id

    @property
    def idempotency_key(self) -> str:
        return self.opts.idempotency_key(self.id) if callable(self.opts.idempotency_key) else self.opts.idempotency_key

    @property
    def headers(self) -> None:
        return None

    @property
    def timeout(self) -> int:
        return min(self.opts.timeout, self._max_timeout)

    @property
    def tags(self) -> dict[str, str]:
        return {**self.opts.tags, "resonate:invoke": self.opts.send_to}

    def options(
        self,
        id: str | None,
        idempotency_key: str | Callable[[str], str] | None,
        send_to: str | None,
        tags: dict[str, str] | None,
        timeout: int | None,
        version: int | None,
    ) -> Remote:
        if version and isinstance(self.func, Callable):
            func, version = self.registry.get(self.func, version)
            self.data = {"func": func, "args": self.args, "kwargs": self.kwargs, "version": version}

        self.opts = self.opts.merge(id=id, idempotency_key=idempotency_key, send_to=send_to, tags=tags, timeout=timeout, version=version)
        return self
