from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any

from resonate.retry_policies.exponential import Exponential
from resonate.retry_policies.never import Never

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
        # print(self.opts)
        # assert self.opts.version == 0
        # assert isinstance(self.opts.retry_policy, Never)

        # Initially, timeout is set to the parent context timeout. This is the upper bound for the timeout.
        self._max_timeout = self.opts.timeout

        # Set the func and version based on the type of func_.
        match self.func_:
            case str():
                self.func, version = self.registry.get(self.func_, self.opts.version)
            case Callable():
                self.func, version = self.func_, self.registry.latest(self.func_) or 1

        # TODO(dfarr): we need a better way to check if the retry policy
        # has not been set manually
        retry_policy = (Never() if isgeneratorfunction(self.func) else Exponential()) if isinstance(self.opts.retry_policy, Never) else None

        assert version > 0
        self.opts = self.opts.merge(version=version, retry_policy=retry_policy)

    @property
    def id(self) -> str:
        return self.opts.id

    def options(
        self,
        id: str | None,
        durable: bool | None,
        retry_policy: RetryPolicy | None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Local:
        if timeout:
            timeout = min(timeout, self._max_timeout)
        if version and isinstance(self.func_, str):
            self.func, version = self.registry.get(self.func_, version)

            # TODO(dfarr): we need a better way to check if the retry policy
            # has not been set manually
            if not retry_policy and isinstance(self.opts.retry_policy, Never):
                retry_policy = Never() if isgeneratorfunction(self.func) else Exponential()

        self.opts = self.opts.merge(id=id, durable=durable, retry_policy=retry_policy, tags=tags, timeout=timeout, version=version)
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
    def headers(self) -> None:
        return None

    @property
    def timeout(self) -> int:
        return min(self.opts.timeout, self._max_timeout)

    @property
    def tags(self) -> dict[str, str]:
        return {**self.opts.tags, "resonate:invoke": self.opts.send_to}

    def options(self, id: str | None, send_to: str | None, tags: dict[str, str] | None, timeout: int | None, version: int | None) -> Remote:
        if version and isinstance(self.func, Callable):
            func, version = self.registry.get(self.func, version)

            assert version > 0
            self.data = {"func": func, "args": self.args, "kwargs": self.kwargs, "version": version}

        self.opts = self.opts.merge(id=id, send_to=send_to, tags=tags, timeout=timeout, version=version)
        return self
