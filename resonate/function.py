from __future__ import annotations

from typing import TYPE_CHECKING, Any, Concatenate, overload

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from resonate.models.handle import Handle
    from resonate.models.retry_policy import RetryPolicy
    from resonate.options import Options
    from resonate.resonate import Context, Resonate


class Function[**P, R]:
    @overload
    def __init__(self, resonate: Resonate, name: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]], opts: Options) -> None: ...
    @overload
    def __init__(self, resonate: Resonate, name: str, func: Callable[Concatenate[Context, P], R], opts: Options) -> None: ...
    def __init__(self, resonate: Resonate, name: str, func: Callable[Concatenate[Context, P], Generator[Any, Any, R]] | Callable[Concatenate[Context, P], R], opts: Options) -> None:
        self._resonate = resonate
        self._name = name
        self._func = func
        self._opts = opts

    @property
    def name(self) -> str:
        return self._name

    @property
    def func(self) -> Callable:
        return self._func

    @property
    def __name__(self) -> str:
        return self._name

    def __call__(self, ctx: Context, *args: P.args, **kwargs: P.kwargs) -> Generator[Any, Any, R] | R:
        return self._func(ctx, *args, **kwargs)

    def options(
        self,
        *,
        idempotency_key: str | Callable[[str], str] | None = None,
        retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] | None = None,
        send_to: str | None = None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Function[P, R]:
        self._opts = self._opts.merge(
            idempotency_key=idempotency_key,
            retry_policy=retry_policy,
            send_to=send_to,
            tags=tags,
            timeout=timeout,
            version=version,
        )
        return self

    def run(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[R]:
        resonate = self._resonate.options(
            idempotency_key=self._opts.idempotency_key,
            retry_policy=self._opts.retry_policy,
            send_to=self._opts.send_to,
            tags=self._opts.tags,
            timeout=self._opts.timeout,
            version=self._opts.version,
        )
        return resonate.run(id, self._name, *args, **kwargs)

    def rpc(self, id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[R]:
        resonate = self._resonate.options(
            idempotency_key=self._opts.idempotency_key,
            retry_policy=self._opts.retry_policy,
            send_to=self._opts.send_to,
            tags=self._opts.tags,
            timeout=self._opts.timeout,
            version=self._opts.version,
        )
        return resonate.rpc(id, self._name, *args, **kwargs)
