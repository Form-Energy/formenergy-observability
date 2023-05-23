"""OpenTelemetry tracing with context-scoped attributes.

Usage:

_tracer = ContextAwareTracer(__name__)

with _tracer.start_as_current_span(
    "some work",
    attributes={
        # You can use attribute names that are added to EventAttrKey/Value, which
        # may be useful so you can reference them in monitoring, or...
        EventAttrKey.DURATION_SECONDS: 12.5,
        # ...just use one-off keys.
        "foo": 42,
    },
):
    # This event will have the duration_seconds and foo attributes, same as the
    # parent span.
    _tracer.add_event(EventAttrValue.MY_CUSTOM_EVENT_NAME)

    # If you get more details after opening the span...
    var_value = db.get...
    # This adds bar as an attribute to the parent span since update_current_span=True.
    with log_ctx.set({"bar": bar_value, update_current_span=True):
        # Any further spans/events/logs in helper() will have all the attributes:
        # EventAttrKey.DURATION_SECONDS, foo, and bar.
        helper()
"""
from contextlib import contextmanager
import functools
import platform
import threading
from typing import Optional, Any, Dict

import opentelemetry
from opentelemetry.trace import Context, Link

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
#:      with ctx.set({"attr_name": attr_value}):
#:          # attr_name is added to the generated LogRecord.
#:          _log.info("Doing some work.")
#:          # attr_value can also be read back explicitly.
#:          print(f"Doing some work with attr_name={ctx.get('attr_value')}.")
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

    @contextmanager
    def start_new_linked_trace(
        self,
        name: str,
        **kwargs,
    ):
        """Starts a new root span with start_as_current_span, linked to the current trace.

        The new root span will have a link back to the previously active trace, but will
        not appear as a child span. This helps avoid very large/complex traces.

        This does not reset the _ObservabilityContext.

        :return: The trace context of the new trace.
        """
        if "context" in kwargs:
            raise ValueError("Cannot specify context when starting new trace.")
        # Record the current context, to be added as a link.
        orig_context = opentelemetry.trace.get_current_span().get_span_context()
        # Include a link back to the old trace. Allow other links to be passed in too.
        links = kwargs.get("links", [])
        links.append(Link(orig_context))
        kwargs["links"] = links
        with self.start_as_current_span(
            name,
            # Start a fresh trace context.
            context=Context(),
            **kwargs,
        ) as child_span:
            child_context = child_span.get_span_context()
            yield child_context
