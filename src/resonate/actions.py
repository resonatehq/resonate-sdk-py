from __future__ import annotations

from typing import TYPE_CHECKING, final

from typing_extensions import ParamSpec

from resonate.options import Options

if TYPE_CHECKING:
    from resonate.typing import ExecutionUnit

P = ParamSpec("P")


@final
class Call:
    def __init__(self, exec_unit: ExecutionUnit, opts: Options | None) -> None:
        self.exec_unit = exec_unit
        self.opts = opts or Options()

    def to_invoke(self) -> Invoke:
        return Invoke(self.exec_unit, is_top_lvl=False, opts=self.opts)


@final
class Invoke:
    def __init__(
        self,
        exec_unit: ExecutionUnit,
        *,
        is_top_lvl: bool,
        opts: Options | None,
    ) -> None:
        self.exec_unit = exec_unit
        self.is_top_lvl = is_top_lvl
        self.opts = opts or Options()


@final
class Sleep:
    def __init__(self, seconds: int) -> None:
        self.seconds = seconds
