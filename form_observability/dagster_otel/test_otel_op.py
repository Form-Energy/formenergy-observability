import pytest
import time
from unittest.mock import MagicMock, patch

from dagster import (
    OpExecutionContext,
    Output,
    graph,
    op,
)
import opentelemetry

from form_observability.dagster_otel.otel_ops import (
    CONFIGURE_OBSERVABILITY_RESOURCE_KEY,
    otel_op,
    publish_current_trace_context,
    _set_trace_context,
)
from form_observability.dagster_otel.testing import noop_resource


def test_publish_trace_context_logs_asset(mock_get_current_span):
    mock_context = MagicMock(spec=OpExecutionContext)
    mock_context.op_handle.path = ["main_graph", "subgraph", "my_op"]

    publish_current_trace_context(mock_context)

    mock_context.log_event.assert_called_once()
    first_call = mock_context.log_event.call_args_list[0]
    materialization = first_call[0][0]  # first arg from the call's args list
    assert "traceparent" in materialization.metadata["trace_context"].data
    assert materialization.asset_key.path[0] == "main_graph.subgraph"


def test_otel_op_finds_trace_context_and_catches_exception():
    @op
    def root_trace_context_op(context):
        publish_current_trace_context(context)
        return 1

    # This op looks for a trace context and will error if it can't find it. It should
    # find the trace context published by the above op.
    @otel_op(
        on_exception_return=Output(5, "result"),
    )
    def test_op(context, v):
        # This exception should be caught because of the on_exception_return.
        raise RuntimeError("Catch me, one test had an error but the pipeline is OK.")

    @graph
    def otel_graph():
        test_op(root_trace_context_op())

    result = otel_graph.execute_in_process(
        resources={
            CONFIGURE_OBSERVABILITY_RESOURCE_KEY: noop_resource,
        }
    )
    assert result.success
    assert result.output_for_node("test_op") == 5


@pytest.mark.no_mock_get_current_span
def test_after_set_trace_context_trace_id_matches_passed_context():
    trace_id = 0x4A084C716BBAB165CBC4471FB5A1DEF0
    assert (
        opentelemetry.trace.get_current_span().get_span_context().trace_id != trace_id
    )
    carrier = {
        "traceparent": f"00-{trace_id:x}-d1ade6c2175f72d5-01",
    }
    _set_trace_context(carrier)
    assert (
        opentelemetry.trace.get_current_span().get_span_context().trace_id == trace_id
    )
