import pytest
from types import SimpleNamespace
from unittest import mock

import opentelemetry
from opentelemetry.trace.span import NonRecordingSpan


@pytest.fixture(autouse=True)
def mock_get_current_span(request):
    """Provide a standin current span for unit tests.

    Since there is no active OpenTelemetry configuration in tests, there will never
    be a real span with a trace/span ID otherwise.

    Allow this autouse fixture to be disabled using
    @pytest.mark.no_mock_get_current_span, for the observability test that
    explicitly verifies trace IDs in trace propagation.
    """
    if "no_mock_get_current_span" in request.keywords:
        yield
    else:
        span = NonRecordingSpan(
            SimpleNamespace(trace_id=11, span_id=22, trace_flags=0, trace_state=False)
        )
        with mock.patch.object(
            opentelemetry.trace, "get_current_span", return_value=span
        ):
            yield
