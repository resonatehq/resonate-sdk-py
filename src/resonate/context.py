from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar, Union

from typing_extensions import ParamSpec, TypeAlias

from resonate.dependency_injection import Dependencies

if TYPE_CHECKING:
    from resonate.typing import DurableCoro, DurableFn

P = ParamSpec("P")
T = TypeVar("T")


class Command:
    def __call__(self, ctx: Context) -> None:
        # This is not meant to be call. We are making the type system happy.
        _ = ctx
        msg = "You should never be here!"
        raise AssertionError(msg)


class FnOrCoroutine:
    def __init__(
        self,
        exec_unit: DurableCoro[P, Any] | DurableFn[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.exec_unit = exec_unit
        self.args = args
        self.kwargs = kwargs


ExecutionUnit: TypeAlias = Union[Command, FnOrCoroutine]


def _wrap_into_execution_unit(
    exec_unit: DurableCoro[P, Any] | DurableFn[P, Any] | Command,
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> ExecutionUnit:
    if isinstance(exec_unit, Command):
        return exec_unit
    return FnOrCoroutine(exec_unit, *args, **kwargs)


class TopLevelInvoke:
    def __init__(
        self,
        exec_unit: DurableCoro[P, Any] | DurableFn[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self.exec_unit = exec_unit
        self.args = args
        self.kwargs = kwargs

    def to_invocation(self) -> Invoke:
        return Invoke(FnOrCoroutine(self.exec_unit, *self.args, **self.kwargs))


class Call:
    def __init__(self, exec_unit: ExecutionUnit) -> None:
        self.exec_unit = exec_unit

    def to_invoke(self) -> Invoke:
        return Invoke(self.exec_unit)


class Invoke:
    def __init__(self, exec_unit: ExecutionUnit) -> None:
        self.exec_unit = exec_unit


def _new_deps() -> Dependencies:
    return Dependencies()


@dataclass
class Context:
    ctx_id: str
    seed: int | None
    parent_ctx: Context | None = None
    deps: Dependencies = field(default_factory=_new_deps)
    _num_children: int = field(init=False, default=0)

    def new_child(self) -> Context:
        self._num_children += 1
        return Context(
            seed=self.seed,
            parent_ctx=self,
            deps=self.deps,
            ctx_id=f"{self.ctx_id}.{self._num_children}",
        )

    def assert_statement(self, stmt: bool, msg: str) -> None:  # noqa: FBT001
        if self.seed is None:
            return
        assert stmt, msg

    def get_dependency(self, key: str) -> Any:  # noqa: ANN401
        return self.deps.get(key)

    def invoke(
        self,
        exec_unit: DurableCoro[P, Any] | DurableFn[P, Any] | Command,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Invoke:
        return Invoke(_wrap_into_execution_unit(exec_unit, *args, **kwargs))

    def call(
        self,
        exec_unit: DurableCoro[P, Any] | DurableFn[P, Any] | Command,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Call:
        return Call(_wrap_into_execution_unit(exec_unit, *args, **kwargs))
