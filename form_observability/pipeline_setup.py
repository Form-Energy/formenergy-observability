"""OpenTelemetry tracing setup.

Example usage for Honeycomb:

pipeline_setup.configure(
    "my-dataset",
    "x-honeycomb-team=<API key for desired environment>",
)
"""
import os

import opentelemetry
from opentelemetry.instrumentation.requests import RequestsInstrumentor

# Initialize tracing and an exporter that can send data to Honeycomb, New Relic, LightStep
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace import TracerProvider, SpanLimits


def configure(
    service_name: str,
    otlp_headers: str,
    otlp_endpoint: str = "https://api.honeycomb.io",
    instrument_requests: bool = False,
) -> None:
    """Set up a trace provider and processor, so tracing data gets exported.

    This sets options which are tuned for a multithreaded data pipeline which should
    export all data (without sampling) via OTLP (not a collector).

    This should be called once before program execution within each Python interpreter.
    For example, this can be invoked as a Dagster resource for each @op.

    Optionally auto-instruments the Python requests library. (SqlAlchemy instrumentation
    is added to each engine as it is created, so it is handled in application space.)
    """
    # Set standard values for OpenTelemetry configuration. We don't expect to change
    # these for different pipeline environments, and they must be env vars, so they
    # are published here instead of via a .env file for example.
    for key, default_value in (
        # Upload all traces (do not downsample).
        ("OTEL_TRACES_SAMPLER", "traceidratio"),
        ("OTEL_TRACES_SAMPLER_ARG", "1.0"),
        ("OTEL_RESOURCE_ATTRIBUTES", "SampleRate=1"),
    ):
        if key not in os.environ:
            os.environ[key] = default_value

    if instrument_requests:
        RequestsInstrumentor().instrument()

    trace_provider = TracerProvider(
        # The name of what we're tracing; in Honeycomb, the dataset.
        resource=Resource(
            attributes={
                SERVICE_NAME: service_name,
            }
        ),
        # Increase the default event limit from 128 per span, since we emit logs as
        # events. Otherwise logs get lost.
        # https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.html#opentelemetry.sdk.trace.SpanLimits
        span_limits=SpanLimits(
            max_events=SpanLimits.UNSET,  # no limit (v. None which gets the default)
        ),
    )
    # Use SimpleSpanProcessor to work with Dagster's start_method:forkserver, since
    # BatchSpanProcessor is not fork-safe. See:
    # https://opentelemetry-python.readthedocs.io/en/latest/examples/fork-process-model/README.html
    # If our span volume gets too high we may want to switch back to BatchSpanProcessor,
    # if only for full pipeline runs or deployed jobs.
    processor = SimpleSpanProcessor(
        OTLPSpanExporter(
            endpoint=otlp_endpoint,
            headers=otlp_headers,
        )
    )
    trace_provider.add_span_processor(processor)
    opentelemetry.trace.set_tracer_provider(trace_provider)
