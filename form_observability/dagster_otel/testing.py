import pytest
from unittest import mock

from dagster import resource


@resource
def noop_resource():
    """A Dagster resource to fulfill cases where the resource will not be used.

    For the observablity resource when we don't want to record spans, in tests.
    """


@pytest.fixture
def skip_otel_op_trace_context():
    """Do not try to find a trace context in Dagster @otel_op s.

    This is necessary when running single ops in unit tests, because there will be no
    published trace context, and ops are also not provided handles (used in looking up
    the trace context).
    """
    with mock.patch(
        "form_observability.dagster_otel.otel_ops._find_trace_context",
        return_value={},
    ):
        yield
