from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

if TYPE_CHECKING:
    from opentelemetry.trace import Tracer


@cache
def _tracer() -> Tracer:
    assert __package__ is not None
    provider = TracerProvider(resource=Resource.create({SERVICE_NAME: __package__}))
    exporter = OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces")
    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)

    return trace.get_tracer(
        __package__,
    )


tracer = _tracer()
