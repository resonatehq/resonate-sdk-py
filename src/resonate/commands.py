from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from resonate.context import Context


class Command:
    def __call__(self, ctx: Context) -> None:
        # This is not meant to be call. We are making the type system happy.
        _ = ctx
        msg = "You should never be here!"
        raise AssertionError(msg)


class CreateDurablePromiseReq(Command):
    def __init__(
        self,
        promise_id: str | None,
        func_name: str | None = None,
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        self.promise_id = promise_id
        self.data: dict[str, Any] | None = None
        if func_name is not None:
            self.data = {
                "func": func_name,
                "args": args if args is not None else (),
                "kwargs": kwargs if kwargs is not None else {},
            }
        self.tags = tags
