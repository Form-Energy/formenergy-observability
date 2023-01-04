from unittest.mock import MagicMock, patch
import pytest

import opentelemetry
from opentelemetry import context
from opentelemetry.trace.span import Span

from form_observability.context_aware import ContextAwareTracer, ctx
from form_observability.otel_value import EventAttrKey, EventAttrValue


class _TestAttrValue(EventAttrValue):
    UNITTEST = "unit_test"


@patch.object(opentelemetry.trace, "get_current_span")
def test_add_duration_event(mock_get_current_span):
    mock_span = MagicMock(spec=Span)
    mock_get_current_span.return_value = mock_span
    dt = 12.5

    tracer = ContextAwareTracer("unittest")
    tracer.add_duration_event(_TestAttrValue.UNITTEST, dt, {"test": True})

    mock_span.add_event.assert_called_once()
    mock_call = mock_span.add_event.call_args_list[0]
    assert mock_call.args[0] == _TestAttrValue.UNITTEST
    event_attrs = mock_call.kwargs.get("attributes")
    assert event_attrs.get(EventAttrKey.TYPE) == EventAttrValue.DURATION
    assert event_attrs.get(EventAttrKey.DURATION_SECONDS) == dt
    assert event_attrs.get(EventAttrKey.DURATION_NAME) == _TestAttrValue.UNITTEST
    assert event_attrs.get("test") == True


def test_context():
    assert ctx.get("a") is None

    with ctx.set({"a": 1}), ctx.set({"b": 2}):
        assert ctx.get("a") == 1
        assert ctx.get("b") == 2

        with ctx.set({"a": 2}):
            assert ctx.get("a") == 2
            assert ctx.get("b") == 2

        assert ctx.get("a") == 1
        assert ctx.get("b") == 2

    assert ctx.get("a") is None
    assert ctx.get("b") is None
