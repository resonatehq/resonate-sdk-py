from __future__ import annotations

from typing import TYPE_CHECKING

from opentelemetry import context, trace
from resonate.events import (
    ExecutionAwaited,
    ExecutionInvoked,
    ExecutionResumed,
    ExecutionTerminated,
    PromiseCompleted,
    PromiseCreated,
    SchedulerEvents,
)
from resonate.opentelemetry import tracer
from resonate.result import Ok

if TYPE_CHECKING:
    from collections.abc import Iterable

    from opentelemetry.trace.span import Span


def stream_events() -> Iterable[SchedulerEvents]:
    yield from [
        PromiseCreated(promise_id="execution-seq-1", parent_promise_id=None, tick=0),
        ExecutionInvoked(
            promise_id="execution-seq-1",
            parent_promise_id=None,
            tick=0,
            fn_name="only_call",
            args=(),
            kwargs={"n": 1},
        ),
        PromiseCreated(
            promise_id="execution-seq-1.1", parent_promise_id="execution-seq-1", tick=1
        ),
        ExecutionInvoked(
            promise_id="execution-seq-1.1",
            parent_promise_id="execution-seq-1",
            tick=1,
            fn_name="number",
            args=(),
            kwargs={"n": 1},
        ),
        ExecutionAwaited(
            promise_id="execution-seq-1.1", parent_promise_id="execution-seq-1", tick=1
        ),
        PromiseCompleted(
            promise_id="execution-seq-1.1",
            parent_promise_id="execution-seq-1",
            tick=2,
            value=Ok(1),
        ),
        ExecutionResumed(promise_id="execution-seq-1", parent_promise_id=None, tick=2),
        PromiseCompleted(
            promise_id="execution-seq-1", parent_promise_id=None, tick=3, value=Ok(1)
        ),
        ExecutionTerminated(
            promise_id="execution-seq-1", parent_promise_id=None, tick=3
        ),
        PromiseCreated(promise_id="execution-seq-2", parent_promise_id=None, tick=3),
        ExecutionInvoked(
            promise_id="execution-seq-2",
            parent_promise_id=None,
            tick=3,
            fn_name="only_call",
            args=(),
            kwargs={"n": 2},
        ),
        PromiseCreated(
            promise_id="execution-seq-2.1", parent_promise_id="execution-seq-2", tick=4
        ),
        ExecutionInvoked(
            promise_id="execution-seq-2.1",
            parent_promise_id="execution-seq-2",
            tick=4,
            fn_name="number",
            args=(),
            kwargs={"n": 2},
        ),
        ExecutionAwaited(
            promise_id="execution-seq-2.1", parent_promise_id="execution-seq-2", tick=4
        ),
        PromiseCompleted(
            promise_id="execution-seq-2.1",
            parent_promise_id="execution-seq-2",
            tick=5,
            value=Ok(2),
        ),
        ExecutionResumed(promise_id="execution-seq-2", parent_promise_id=None, tick=5),
        PromiseCompleted(
            promise_id="execution-seq-2", parent_promise_id=None, tick=6, value=Ok(2)
        ),
        ExecutionTerminated(
            promise_id="execution-seq-2", parent_promise_id=None, tick=6
        ),
        PromiseCreated(promise_id="execution-seq-3", parent_promise_id=None, tick=6),
        ExecutionInvoked(
            promise_id="execution-seq-3",
            parent_promise_id=None,
            tick=6,
            fn_name="only_call",
            args=(),
            kwargs={"n": 3},
        ),
        PromiseCreated(
            promise_id="execution-seq-3.1", parent_promise_id="execution-seq-3", tick=7
        ),
        ExecutionInvoked(
            promise_id="execution-seq-3.1",
            parent_promise_id="execution-seq-3",
            tick=7,
            fn_name="number",
            args=(),
            kwargs={"n": 3},
        ),
        ExecutionAwaited(
            promise_id="execution-seq-3.1", parent_promise_id="execution-seq-3", tick=7
        ),
        PromiseCompleted(
            promise_id="execution-seq-3.1",
            parent_promise_id="execution-seq-3",
            tick=8,
            value=Ok(3),
        ),
        ExecutionResumed(promise_id="execution-seq-3", parent_promise_id=None, tick=8),
        PromiseCompleted(
            promise_id="execution-seq-3", parent_promise_id=None, tick=9, value=Ok(3)
        ),
        ExecutionTerminated(
            promise_id="execution-seq-3", parent_promise_id=None, tick=9
        ),
        PromiseCreated(promise_id="execution-seq-4", parent_promise_id=None, tick=9),
        ExecutionInvoked(
            promise_id="execution-seq-4",
            parent_promise_id=None,
            tick=9,
            fn_name="only_call",
            args=(),
            kwargs={"n": 4},
        ),
        PromiseCreated(
            promise_id="execution-seq-4.1", parent_promise_id="execution-seq-4", tick=10
        ),
        ExecutionInvoked(
            promise_id="execution-seq-4.1",
            parent_promise_id="execution-seq-4",
            tick=10,
            fn_name="number",
            args=(),
            kwargs={"n": 4},
        ),
        ExecutionAwaited(
            promise_id="execution-seq-4.1", parent_promise_id="execution-seq-4", tick=10
        ),
        PromiseCompleted(
            promise_id="execution-seq-4.1",
            parent_promise_id="execution-seq-4",
            tick=11,
            value=Ok(4),
        ),
        ExecutionResumed(promise_id="execution-seq-4", parent_promise_id=None, tick=11),
        PromiseCompleted(
            promise_id="execution-seq-4", parent_promise_id=None, tick=12, value=Ok(4)
        ),
        ExecutionTerminated(
            promise_id="execution-seq-4", parent_promise_id=None, tick=12
        ),
        PromiseCreated(promise_id="execution-seq-5", parent_promise_id=None, tick=12),
        ExecutionInvoked(
            promise_id="execution-seq-5",
            parent_promise_id=None,
            tick=12,
            fn_name="only_call",
            args=(),
            kwargs={"n": 5},
        ),
        PromiseCreated(
            promise_id="execution-seq-5.1", parent_promise_id="execution-seq-5", tick=13
        ),
        ExecutionInvoked(
            promise_id="execution-seq-5.1",
            parent_promise_id="execution-seq-5",
            tick=13,
            fn_name="number",
            args=(),
            kwargs={"n": 5},
        ),
        ExecutionAwaited(
            promise_id="execution-seq-5.1", parent_promise_id="execution-seq-5", tick=13
        ),
        PromiseCompleted(
            promise_id="execution-seq-5.1",
            parent_promise_id="execution-seq-5",
            tick=14,
            value=Ok(5),
        ),
        ExecutionResumed(promise_id="execution-seq-5", parent_promise_id=None, tick=14),
        PromiseCompleted(
            promise_id="execution-seq-5", parent_promise_id=None, tick=15, value=Ok(5)
        ),
        ExecutionTerminated(
            promise_id="execution-seq-5", parent_promise_id=None, tick=15
        ),
    ]


def _new_span(parent: Span | None, name: str) -> tuple[Span, object]:
    parent_ctx = trace.set_span_in_context(parent) if parent is not None else None
    token = context.attach(parent_ctx) if parent_ctx is not None else None
    return tracer.start_span(name=name, context=parent_ctx), token


def _end_span(span: Span, token: object | None) -> None:
    if token:
        context.detach(token=token)
    span.end()


class OpenTelemetryAdapter:
    def __init__(self) -> None:
        self._spans: dict[str, tuple[Span, object]] = {}

    def create_span(self, promise_id: str, parent_promise_id: str | None) -> None:
        parent_span_and_token = (
            self._spans[parent_promise_id] if parent_promise_id is not None else None
        )
        parent_span: Span | None = None
        if parent_span_and_token is not None:
            parent_span = parent_span_and_token[0]
        new_span = _new_span(parent=parent_span, name=promise_id)

        assert (
            promise_id not in self._spans
        ), "There should not be two spans with the same name at the same time."
        self._spans[promise_id] = new_span

    def close_span(self, promise_id: str) -> None:
        assert (
            promise_id in self._spans
        ), "There should be an span associated with the promise id."
        span, token = self._spans.pop(promise_id)
        _end_span(span, token)


def main() -> None:
    adapter = OpenTelemetryAdapter()
    for event in stream_events():
        if isinstance(event, PromiseCreated):
            adapter.create_span(
                event.promise_id,
                parent_promise_id=event.parent_promise_id,
            )
        elif isinstance(event, PromiseCompleted):
            adapter.close_span(event.promise_id)
        else:
            continue


if __name__ == "__main__":
    main()
