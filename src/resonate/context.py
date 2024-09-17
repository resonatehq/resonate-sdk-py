from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar, final

from typing_extensions import ParamSpec

from resonate.actions import Call, Invoke, Sleep
from resonate.dataclasses import Command, FnOrCoroutine
from resonate.dependency_injection import Dependencies

if TYPE_CHECKING:
    from resonate.retry_policy import RetryPolicy
    from resonate.typing import ExecutionUnit, Invokable

P = ParamSpec("P")
T = TypeVar("T")


def _wrap_into_execution_unit(
    invokable: Invokable[P],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ExecutionUnit:
    if isinstance(invokable, Command):
        return invokable
    return FnOrCoroutine(invokable, *args, **kwargs)


@final
@dataclass
class Context:
    ctx_id: str
    retry_policy: RetryPolicy
    seed: int | None
    parent_ctx: Context | None = None
    deps: Dependencies = field(default=Dependencies())
    _num_children: int = field(init=False, default=0)

    def parent_promise_id(self) -> str | None:
        return self.parent_ctx.ctx_id if self.parent_ctx is not None else None

    def new_child(self, ctx_id: str | None, retry_policy: RetryPolicy) -> Context:
        self._num_children += 1
        if ctx_id is None:
            ctx_id = f"{self.ctx_id}.{self._num_children}"
        return Context(
            seed=self.seed,
            retry_policy=retry_policy,
            parent_ctx=self,
            deps=self.deps,
            ctx_id=ctx_id,
        )

    def assert_statement(self, stmt: bool, msg: str) -> None:  # noqa: FBT001
        if self.seed is None:
            return
        assert stmt, msg

    def get_dependency(self, key: str) -> Any:  # noqa: ANN401
        return self.deps.get(key)

    def invoke(
        self,
        invokable: Invokable[P],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Invoke:
        return self.call(invokable, *args, **kwargs).to_invoke()

    def call(
        self,
        invokable: Invokable[P],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Call:
        return Call(_wrap_into_execution_unit(invokable, *args, **kwargs))

    def sleep(self, seconds: int) -> Sleep:
        return Sleep(seconds)
