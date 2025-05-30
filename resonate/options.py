from __future__ import annotations

from dataclasses import dataclass, field
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any

from resonate.encoders import HeaderEncoder, JsonEncoder, JsonPickleEncoder, NoopEncoder, PairEncoder
from resonate.retry_policies import Exponential, Never

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.models.encoder import Encoder
    from resonate.models.retry_policy import RetryPolicy


@dataclass(frozen=True)
class Options:
    durable: bool = True
    encoder: Encoder[Any, str | None] | None = None
    id: str | None = None
    idempotency_key: str | Callable[[str], str] | None = lambda id: id
    non_retryable_exceptions: tuple[type[Exception], ...] = ()
    retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] = lambda f: Never() if isgeneratorfunction(f) else Exponential()
    target: str = "default"
    tags: dict[str, str] = field(default_factory=dict)
    timeout: float = 31536000  # relative time in seconds, default 1 year
    version: int = 0

    def __post_init__(self) -> None:
        if not (self.version >= 0):
            msg = "version must be greater than or equal to zero"
            raise ValueError(msg)
        if not (self.timeout >= 0):
            msg = "timeout must be greater than or equal to zero"
            raise ValueError(msg)

    def merge(
        self,
        *,
        durable: bool | None = None,
        encoder: Encoder[Any, str | None] | None = None,
        id: str | None = None,
        idempotency_key: str | Callable[[str], str] | None = None,
        non_retryable_exceptions: tuple[type[Exception], ...] | None = None,
        retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] | None = None,
        target: str | None = None,
        tags: dict[str, str] | None = None,
        timeout: float | None = None,
        version: int | None = None,
    ) -> Options:
        return Options(
            durable=durable if durable is not None else self.durable,
            encoder=encoder if encoder is not None else self.encoder,
            id=id if id is not None else self.id,
            idempotency_key=idempotency_key if idempotency_key is not None else self.idempotency_key,
            non_retryable_exceptions=non_retryable_exceptions if non_retryable_exceptions is not None else self.non_retryable_exceptions,
            retry_policy=retry_policy if retry_policy is not None else self.retry_policy,
            target=target if target is not None else self.target,
            tags=tags if tags is not None else self.tags,
            timeout=timeout if timeout is not None else self.timeout,
            version=version if version is not None else self.version,
        )

    def get_encoder(self) -> Encoder[Any, tuple[dict[str, str] | None, str | None]]:
        l = NoopEncoder() if self.encoder else HeaderEncoder("resonate:format-py", JsonPickleEncoder())
        r = self.encoder or JsonEncoder()
        return PairEncoder(l, r)

    def get_idempotency_key(self, id: str) -> str | None:
        return self.idempotency_key(id) if callable(self.idempotency_key) else self.idempotency_key

    def get_retry_policy(self, func: Callable) -> RetryPolicy:
        return self.retry_policy(func) if callable(self.retry_policy) else self.retry_policy
