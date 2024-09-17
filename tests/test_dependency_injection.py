from __future__ import annotations

from resonate.context import Context
from resonate.retry_policy import never


def test_inherit_deps() -> None:
    ctx = Context(ctx_id="1", seed=10, retry_policy=never())
    ctx.deps.set(key="number", obj=1)
    child = ctx.new_child(ctx_id=None, retry_policy=never())
    assert child.parent_ctx is not None, "Child must have a parent."
    assert child.parent_ctx == ctx
    assert child.deps.get("number") == 1
    assert child.seed == child.parent_ctx.seed


def test_set_name() -> None:
    ctx = Context(ctx_id="1", seed=None, retry_policy=never())
    child_1 = ctx.new_child(ctx_id=None, retry_policy=never())
    assert child_1.ctx_id == "1.1"
    child_2 = child_1.new_child("my-name", retry_policy=never())
    assert child_2.ctx_id == "my-name"
