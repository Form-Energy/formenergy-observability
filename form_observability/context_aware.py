"""OpenTelemetry tracing setup.

This module handles spans and events, but should not be aware of any implementation
under other pipeline modules (including as orchestration/ where Dagster/OTel
integration lives).

Usage:

_tracer = ContextAwareTracer(__name__)
with _tracer.start_as_current_span(
    "some work",
    attributes={
        # Attributes added to EventAttrKey/Value...
        EventAttrKey.TEST_ID: "TST22",
        # ...or just use one-off keys.
        "foo": 42,
    },
):
    # This event will have the test_id attribute, same as the parent span.
    _tracer.add_event(EventAttrValue.TEST_SKIPPED)

    # If you get more details after opening the span...
    metadata = db.get...

    # This adds cell_id as an attribute to the parent span since update_current_span=True.
    with log_ctx.set({EventAttrKey.CELL_ID: metadata["cell_id"]}, update_current_span=True):
        # Any further spans/events/logs in helper() will have test_id and cell_id.
        helper()
"""
from contextlib import contextmanager
import functools
import platform
import threading
from typing import Optional, Any, Dict

import opentelemetry

from form_observability.otel_value import (
    EventAttrKey,
    EventAttrValue,
    SpanName,
)


class _ObservabilityContext:
    """Information about the execution context, for each thread.

    This class is thread-safe and meant to be used by sharing a single instance of it
    with the whole application.

    Use the global ctx object created below.
    """

    #: Contains the isolated stacks for each thread.
    #: Each dict in a list is a new context on a stack, extending the previous context:
    #: Thread 1: [{}]
    #: Thread 2: [{}, {'a': 2}, {'a': 2, 'b': 0}]
    #: Thread 3: [{}, {'a': 3}]
    _ctx = threading.local()

    @property
    def _stack(self):
        """Lazily create and return the stack for the current thread.

        This makes sure that if the ctx is created in one thread but then used from
        another a per-thread stack is still created.
        """
        if not hasattr(self._ctx, "stack"):
            # Add these attrs globally.
            self._ctx.stack = [
                {
                    "host.name": platform.node(),
                    "os.type": platform.system().lower(),
                },
            ]
        return self._ctx.stack

    @property
    def current_ctx(self) -> dict:
        return self._stack[-1]

    @contextmanager
    def set(
        self,
        attributes: Dict[str, Any],
        update_current_span=False,
    ):
        """
        Create a new context valid for a block of code and add a key to it.

        After the block of code has ended, the previous context is restored.
        Because manually managing start and end of context is tedious, this method is
        only meant to be used with a `with` statement.

        :param attributes: Key-value pairs to add to the context.
        """
        # TODO If updating to Python 3.9, use | to update dicts more concisely.
        new_attrs = self.current_ctx.copy()
        filtered = _filter_attributes(attributes)
        new_attrs.update(filtered)
        self._stack.append(new_attrs)
        if update_current_span:
            opentelemetry.trace.get_current_span().set_attributes(filtered)
        try:
            yield
        finally:
            self._stack.pop()

    def get(self, *args, **kwargs):
        """
        Get the value for a key from the current context.
        """
        return self.current_ctx.get(*args, **kwargs)


#: Object to use throughout the application to attach context to logs and traces.
#:
#: Usage:
#:      with ctx.set({EventAttrKey.TEST_ID: "TST22"}):
#:          # test_id is added to the generated LogRecord.
#:          _log.info(f"Doing some work.")
#:          # test_id can also be read back explicitly.
#:          print("Working on test_id", ctx.get("test_id"))
ctx = _ObservabilityContext()


def _convert_types(v: Any) -> Any:
    """Converts numpy int64s to ints."""
    if type(v).__name__ in ("int8", "int32", "int64"):
        return int(v)
    return v


def _filter_attributes(d: Dict) -> Dict:
    """Pre-process span/event attributes to convert complex types and remove None values.

    Spans warn if the attribute value is None, so filter None values out. This
    filtering is not strictly necessary for span events, which accept None values.
    """
    filtered = {}
    for k, v in d.items():
        if v is None:
            continue
        filtered[_convert_types(k)] = _convert_types(v)
    return filtered


class ContextAwareTracer:
    """Main entry point for starting spans and adding events. See usage in module doc.

    Wrapper for OpenTelemetry interfaces with helpers that automatically pick up
    ctx.current_ctx and/or update it.
    """

    def __init__(self, library_name):
        self._tracer = opentelemetry.trace.get_tracer(library_name)

    def _get_required_current_span(self):
        span = opentelemetry.trace.get_current_span()
        if span is None or span.get_span_context().span_id == 0:
            raise RuntimeError(f"No current span.")
        return span

    def add_duration_event(
        self,
        duration_name: str,
        dt: float,
        attributes: Dict = {},
    ) -> None:
        """Records a duration value (duration_s) on the current span.

        :param duration_name: The value to use for the duration_name attribute on the event.
            Queries are expected to reference this value.
        :param attributes: Additional event attributes (merged with ctx.current_ctx).
        """
        if dt is None:
            raise ValueError("Bad dt value for add_duration_event: {dt}.")

        event_attrs = ctx.current_ctx.copy()
        event_attrs.update(
            {
                EventAttrKey.TYPE: EventAttrValue.DURATION,
                EventAttrKey.DURATION_NAME: duration_name,
                EventAttrKey.DURATION_SECONDS: dt,
            }
        )
        event_attrs.update(attributes)
        self._get_required_current_span().add_event(
            # Use the duration name as the event name too, for UI visibility.
            duration_name,
            attributes=_filter_attributes(event_attrs),
        )

    def add_event(self, event_type: str, attributes: Dict = {}) -> None:
        """Records an event on the current span.

        :param event_type: Used as the span name, as well as the "type" attribute.
        :param attributes: Additional event attributes (merged with ctx.current_ctx).
        """
        event_attrs = ctx.current_ctx.copy()
        event_attrs.update({EventAttrKey.TYPE: event_type})
        event_attrs.update(attributes)
        self._get_required_current_span().add_event(
            _convert_types(event_type),
            attributes=_filter_attributes(event_attrs),
        )

    def traced(self, wrapped):
        """Decorator to trace a function.

        Usage:
            _tracer = ContextAwareTracer(..)
            class Me:
                @_tracer.traced
                def my_fn(self, arg):
                    pass

        This is similar to OTel's builtin decorator, but uses the function name and
        integrates with context.
        https://opentelemetry.io/docs/instrumentation/python/manual/#creating-spans-with-decorators
        """

        @functools.wraps(wrapped)
        def wrapper(*args, **kwargs):
            with self.start_as_current_span(wrapped.__name__):
                return wrapped(*args, **kwargs)

        return wrapper

    @contextmanager
    def start_as_current_span(
        self,
        name: str,
        attributes: Dict[str, Any] = {},
        **kwargs,
    ):
        """Starts a span with the given name, merging ctx.current_ctx with attributes."""
        span_attrs = ctx.current_ctx.copy()
        span_attrs.update(attributes)
        with ctx.set(attributes or {}), self._tracer.start_as_current_span(
            _convert_types(name),
            attributes=_filter_attributes(span_attrs),
            **kwargs,
        ) as span:
            yield span
