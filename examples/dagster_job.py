"""Dagster job with OpenTelemetry trace propagation.

Run with:
    dagster job execute --python-file examples/dagster_job.py
"""
import os

from dagster import (
    Output,
    job,
    op,
    resource,
)

from form_observability.dagster_otel import (
    CONFIGURE_OBSERVABILITY_RESOURCE_KEY,
    otel_op,
    publish_current_trace_context,
)
from form_observability import (
    ContextAwareTracer,
    SpanName,
    ctx,
    configure,
)


_trace = ContextAwareTracer(__name__)


@resource
def configure_observability_resource(init_context):
    """This resource sets up the span exporter (before each op executes)."""
    header_env = "OTEL_EXPORTER_OTLP_HEADERS"
    endpoint_env = "OTEL_EXPORTER_OTLP_ENDPOINT"
    if header_env not in os.environ:
        init_context.log.error(
            f"To upload traces, you must set {header_env}, see README.md."
        )
    configure(
        "form_observability_example",
        otlp_headers=os.environ.get(header_env),
        otlp_endpoint=os.environ.get(endpoint_env, "https://api.honeycomb.io"),
    )


@op(
    required_resource_keys={CONFIGURE_OBSERVABILITY_RESOURCE_KEY},
)
def root_trace_context_op(context):
    """Each job/subgraph should have one regular op to start and publish the OTel trace."""
    with _trace.start_as_current_span(SpanName.ROOT):
        publish_current_trace_context(context)
        return "TS123"


@_trace.traced
def _do_work(my_data: str) -> None:
    """This throws an exception which should be caught by the on_exception_return."""
    raise RuntimeError("Catch me, one test had an error but the pipeline is OK.")


@otel_op(
    # If anything in this op raises an exception, catch it and report it using
    # OpenTelemetry, but do not let it cause the Dagster op to fail. This is useful
    # in cases where the overall pipeline can continue running, and you have
    # monitoring in place to investigate specific errors.
    on_exception_return=Output(5, "result"),
)
def test_op(context, my_data: str) -> None:
    """Other ops in the graph use @otel_op to pick up that trace context.

    This op looks for a trace context and will error if it can't find it. It should
    find the trace context published by the above op.
    """
    with ctx.set({"my_data": my_data}, update_current_span=True):
        _do_work(my_data)


@job(
    # The job will use this resource to set up the span exporter. Every @otel_op
    # requires the observability resource.
    resource_defs={
        CONFIGURE_OBSERVABILITY_RESOURCE_KEY: configure_observability_resource,
    },
)
def otel_graph():
    test_op(root_trace_context_op())
