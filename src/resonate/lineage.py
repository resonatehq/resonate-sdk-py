"""Deterministic asyncio-task lineage for durable id generation.

Child-promise ids must map to the same logical operation on every replay.
A single per-execution sequence counter guarantees that only while the
*global order* of durable-op calls is replay-stable -- true for sequential
code, false under concurrency: which coroutine reaches its next ``ctx.run``
first depends on which sibling future settled first, and live (wall-clock)
completion order need not match replay (instant recovery) order.

The replay-stable thing is not *call order* but *task structure*:

* ``asyncio.create_task`` (and everything built on it -- ``gather``,
  ``TaskGroup``, ``ensure_future``, ``wait_for``) is called synchronously
  from the parent task, so within one task, spawn order is program order.
* Within one task, code runs sequentially, so its durable-op order is
  program order too.

Both facts hold inductively on every replay (given the usual determinism
assumption sequential code already relies on). So this module assigns every
task a **lineage path** -- the chain of spawn indices from an untracked
ancestor down to the task -- at creation time, via a loop task factory:

* :data:`_lineage` carries the current task's path; a coroutine awaited
  inline shares its caller's path (same logical thread).
* :data:`_spawn_counter` is a mutable per-task cell counting that task's
  spawns. The factory increments the *parent's* cell synchronously inside
  ``create_task`` and seeds the child with the extended path and a fresh
  cell of its own.

:class:`~resonate.context.Context` then keys its child-id sequence per
lineage path (relative to the task its state was created in), so the id a
durable op mints is a pure function of (task path, per-path call index) --
both replay-stable -- instead of the global call sequence.

Tracking is opt-in per task tree: a task created while the current
:data:`_spawn_counter` is ``None`` (anything outside user workflow code) is
left untracked, and :func:`reset_spawn_counter` arms a fresh cell at the
user-code boundary (:meth:`Context.invoke_with_retry`). The reset is what
keeps user spawn indices immune to tasks the SDK's own machinery -- or the
network stack under it -- happens to spawn earlier on the same task: those
consume a cell that is thrown away before user code runs. (An SDK-internal
task spawned *from* user code, e.g. ``ctx.run``'s background body, consumes
an index like any other spawn -- at a fixed call site, so deterministically.)

Without the factory installed every path is empty and id generation
degrades to exactly the old single-counter behavior.
"""

from __future__ import annotations

import asyncio
import contextvars
from typing import Any

#: The current task's lineage path. Empty for untracked tasks (the default),
#: so everything outside a workflow degrades gracefully.
_lineage: contextvars.ContextVar[tuple[int, ...]] = contextvars.ContextVar(
    "resonate_task_lineage", default=()
)

#: The current task's spawn-counter cell, or ``None`` when the task is not
#: tracked. A one-element list so the factory can increment it in place --
#: a ``ContextVar`` re-``set`` would be invisible to context copies that
#: already captured the old value.
_spawn_counter: contextvars.ContextVar[list[int] | None] = contextvars.ContextVar(
    "resonate_task_spawn_counter", default=None
)


def capture_anchor() -> tuple[int, ...]:
    """Return the current task's lineage path, to anchor a new execution state.

    Called from :class:`~resonate.context._State`'s constructor, which runs in
    the same task the state's user body will run in (the core execution task
    for a root, ``ctx.run``'s background task for a child), so relative paths
    computed against it via :func:`task_path` start at that body.
    """
    return _lineage.get()


def reset_spawn_counter() -> None:
    """Arm a fresh spawn-counter cell for the current task.

    Called at the user-code boundary (each :meth:`Context.invoke_with_retry`
    attempt), so the indices user spawns receive always start at 1 there --
    regardless of any tasks the SDK or the network stack spawned from this
    task beforehand. Deliberately does not touch :data:`_lineage`: the body
    still runs *in* this task, so its path is unchanged.
    """
    _spawn_counter.set([0])


def task_path(anchor: tuple[int, ...]) -> tuple[int, ...]:
    """Return the current task's path relative to ``anchor``.

    The empty path for the anchor task itself (sequential code -- ids keep
    their flat ``{id}.{seq}`` shape), and for any task that is not a lineage
    descendant of the anchor (untracked task, factory not installed, or a
    durable op smuggled into a foreign task tree): those fall back to the old
    shared-counter behavior rather than failing.
    """
    lineage = _lineage.get()
    if lineage[: len(anchor)] == anchor:
        return lineage[len(anchor) :]
    return ()


def _enter_task(lineage: tuple[int, ...]) -> None:
    """Seed a child task's context: its own path and a fresh spawn cell."""
    _lineage.set(lineage)
    _spawn_counter.set([0])


class _LineageTaskFactory:
    """Loop task factory that stamps each tracked task with its lineage path.

    Runs synchronously inside ``create_task``, in the parent's context, which
    is exactly what makes the assigned index deterministic: it reflects the
    position of the ``create_task`` *call site* in the parent's sequential
    execution, decided before the event loop has any say in scheduling.

    A task created while the parent's :data:`_spawn_counter` is ``None`` is
    untracked and built exactly as the default factory would build it. Wraps
    any previously installed factory (``inner``) rather than replacing it.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def __call__(
        self,
        loop: asyncio.AbstractEventLoop,
        coro: Any,
        **kwargs: Any,
    ) -> asyncio.Task[Any]:
        counter = _spawn_counter.get()
        context: contextvars.Context | None = kwargs.pop("context", None)
        if counter is not None:
            counter[0] += 1
            child = (*_lineage.get(), counter[0])
            if context is None:
                # The same copy the default path would take, made explicit so
                # the child's lineage vars can be seeded before it first runs
                # (including an eager start, which runs inside ``Task()``).
                context = contextvars.copy_context()
            context.run(_enter_task, child)
        if self._inner is not None:
            # Only forward ``context`` when there is one to forward: an
            # old-style two-argument factory would reject the keyword.
            if context is not None:
                kwargs["context"] = context
            return self._inner(loop, coro, **kwargs)
        return asyncio.Task(coro, loop=loop, context=context, **kwargs)


def install_task_lineage() -> None:
    """Install the lineage factory on the running loop. Idempotent.

    Called at execution time (:meth:`Core.execute_until_blocked_inner`), so
    any loop that runs workflows tracks lineage before the body can spawn.
    An already-installed foreign factory is wrapped, not replaced.
    """
    loop = asyncio.get_running_loop()
    current = loop.get_task_factory()
    if isinstance(current, _LineageTaskFactory):
        return
    loop.set_task_factory(_LineageTaskFactory(current))
