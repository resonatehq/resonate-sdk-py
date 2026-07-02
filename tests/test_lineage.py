"""Behaviour tests for :mod:`resonate.lineage`.

The module's contract is small: once the factory is installed and a spawn
counter armed, every ``asyncio.create_task`` assigns the child a path equal to
its parent's path extended by the parent's spawn index -- decided
synchronously at the *call site*, so paths reflect program order, never
event-loop scheduling. Everything outside an armed task tree stays untracked
and degrades to the empty path.

The end-to-end consequence (concurrent durable chains recover their own
values across a replay) is asserted in ``test_resonate.py``.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from resonate.lineage import (
    _LineageTaskFactory,
    capture_anchor,
    install_task_lineage,
    reset_spawn_counter,
    task_path,
)

# =============================================================================
# install: idempotent, wraps rather than replaces
# =============================================================================


@pytest.mark.asyncio
async def test_install_is_idempotent() -> None:
    install_task_lineage()
    factory = asyncio.get_running_loop().get_task_factory()
    install_task_lineage()
    assert asyncio.get_running_loop().get_task_factory() is factory


@pytest.mark.asyncio
async def test_install_wraps_an_existing_factory() -> None:
    loop = asyncio.get_running_loop()
    calls: list[str] = []

    def inner(
        loop: asyncio.AbstractEventLoop, coro: Any, **kwargs: Any
    ) -> asyncio.Task[Any]:
        calls.append("inner")
        return asyncio.Task(coro, loop=loop, **kwargs)

    loop.set_task_factory(inner)
    try:
        install_task_lineage()
        factory = loop.get_task_factory()
        assert isinstance(factory, _LineageTaskFactory)

        async def child() -> None:
            return None

        await asyncio.create_task(child())
        assert calls == ["inner"]
    finally:
        loop.set_task_factory(None)


# =============================================================================
# paths: untracked default, creation-order indices, nesting
# =============================================================================


@pytest.mark.asyncio
async def test_untracked_task_has_empty_path() -> None:
    # No spawn counter armed: the factory leaves the task untracked, so its
    # path relative to any anchor is empty -- the old flat-id behavior.
    install_task_lineage()

    async def child() -> tuple[int, ...]:
        return task_path(capture_anchor())

    assert await asyncio.create_task(child()) == ()


@pytest.mark.asyncio
async def test_spawn_indices_follow_creation_order() -> None:
    install_task_lineage()
    reset_spawn_counter()
    anchor = capture_anchor()

    async def child() -> tuple[int, ...]:
        return task_path(anchor)

    task_a = asyncio.create_task(child())
    task_b = asyncio.create_task(child())
    assert await task_a == (1,)
    assert await task_b == (2,)


@pytest.mark.asyncio
async def test_gather_indices_follow_argument_order() -> None:
    install_task_lineage()
    reset_spawn_counter()
    anchor = capture_anchor()

    async def child() -> tuple[int, ...]:
        return task_path(anchor)

    assert await asyncio.gather(child(), child(), child()) == [(1,), (2,), (3,)]


@pytest.mark.asyncio
async def test_nested_spawns_extend_the_path() -> None:
    install_task_lineage()
    reset_spawn_counter()
    anchor = capture_anchor()

    async def leaf() -> tuple[int, ...]:
        return task_path(anchor)

    async def branch() -> list[tuple[int, ...]]:
        first = asyncio.create_task(leaf())
        second = asyncio.create_task(leaf())
        return [await first, await second]

    assert await asyncio.create_task(branch()) == [(1, 1), (1, 2)]


@pytest.mark.asyncio
async def test_inline_await_shares_the_callers_path() -> None:
    # A coroutine awaited without ``create_task`` is the same logical thread:
    # no index is consumed and the path is unchanged.
    install_task_lineage()
    reset_spawn_counter()
    anchor = capture_anchor()

    async def inline() -> tuple[int, ...]:
        return task_path(anchor)

    assert await inline() == ()

    async def child() -> tuple[int, ...]:
        return await inline()

    assert await asyncio.create_task(child()) == (1,)


# =============================================================================
# reset: the user-code boundary restarts indices; foreign anchors degrade
# =============================================================================


@pytest.mark.asyncio
async def test_reset_spawn_counter_restarts_indices() -> None:
    # The user-code boundary arms a fresh cell, so indices restart at 1 on
    # every attempt regardless of what was spawned from this task before.
    install_task_lineage()
    reset_spawn_counter()
    anchor = capture_anchor()

    async def child() -> tuple[int, ...]:
        return task_path(anchor)

    assert await asyncio.create_task(child()) == (1,)
    reset_spawn_counter()
    assert await asyncio.create_task(child()) == (1,)


@pytest.mark.asyncio
async def test_foreign_anchor_falls_back_to_empty_path() -> None:
    # A durable op reached from a task that is not a lineage descendant of the
    # anchor degrades to the empty path instead of failing.
    install_task_lineage()
    reset_spawn_counter()

    async def child() -> tuple[int, ...]:
        return task_path((99,))

    assert await asyncio.create_task(child()) == ()
