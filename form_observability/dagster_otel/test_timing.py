import pytest
import time
from unittest.mock import MagicMock, patch

from dagster import (
    OpExecutionContext,
    Output,
    graph,
    op,
)

from form_observability.dagster_otel.timing import (
    get_timing_since_step_started,
)
from form_observability.dagster_otel.testing import noop_resource


def test_get_timing():
    def later():
        """Returns a time 10s later than now, to simulate more time having passed."""
        return time.time() + 10

    @op
    def start_op():
        return 1

    @op
    def end_op(context, v):
        # This op should be able to find the previous op and return details about the
        # elapsed time.
        return get_timing_since_step_started(context, "start_op", seconds_fn=later)

    @graph
    def timing_graph():
        end_op(start_op())

    result = timing_graph.execute_in_process()
    assert result.success
    assert result.output_for_node("end_op").dt_s == pytest.approx(10, 0.1)
