import os
import sys
from opentelemetry import trace as trace_api
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

tracer_provider = None


def init_tracing():
    global tracer_provider
    if tracer_provider is not None:
        print("Tracing already initialized", file=sys.stderr)
        return
    print("Initializing tracing", file=sys.stderr)

    span_exporter = OTLPSpanExporter(
        endpoint=os.getenv("OTEL_ENDPOINT"),
        timeout=1,
    )

    tracer_provider = TracerProvider(
        resource=Resource.create({SERVICE_NAME: os.getenv("OTEL_SERVICE_NAME")})
    )
    trace_api.set_tracer_provider(tracer_provider)

    span_processor = BatchSpanProcessor(span_exporter)
    tracer_provider.add_span_processor(span_processor)
    return True

