import logging
import traceback

import opentelemetry
from opentelemetry.trace import Status, StatusCode
from opentelemetry.semconv.trace import SpanAttributes

from form_observability.context_aware import ctx
from form_observability.otel_value import EventAttrKey, EventAttrValue


class OtelSpanEventHandler(logging.Handler):
    """Attach log messages as events to the current span. Noop if no current span.

    Also sets the span error status if the log is ERROR or above.

    Following Honeycomb advice:
    https://www.honeycomb.io/blog/uniting-tracing-logs-open-telemetry-span-events/
    """

    def emit(self, record: logging.LogRecord) -> None:
        span = opentelemetry.trace.get_current_span()
        # Records logged when no trace is active will have no trace ID; in practice
        # get_current_span seems to always return a span but when it's not real it
        # has 0 values for IDs. This will happen for Dagster framework logs before
        # and after op execution, b/c span context is only activated during an otel_op.
        if not (span and span.get_span_context().trace_id):
            return

        # Where possible, follow the semantic conventions of span attributes. See
        # https://github.com/open-telemetry/opentelemetry-python/blob/main/opentelemetry-semantic-conventions/src/opentelemetry/semconv/trace/__init__.py
        attributes = {
            EventAttrKey.TYPE: EventAttrValue.LOG_MESSAGE,
            "logger.name": record.name,
            "level": record.levelname,
            SpanAttributes.CODE_FILEPATH: record.pathname,
            "code.filename": record.filename,
            SpanAttributes.CODE_LINENO: record.lineno,
            SpanAttributes.CODE_NAMESPACE: record.module,
            SpanAttributes.CODE_FUNCTION: record.funcName,
            # These are conceptually resources, and could be set on the span when it
            # is started instead of logged repeatedly with messages.
            "process.pid": record.process,
            "process.name": record.processName,
            SpanAttributes.THREAD_ID: record.thread,
            SpanAttributes.THREAD_NAME: record.threadName,
        }

        if record.exc_info is not None:
            exc_type, exc_value, tb = record.exc_info
            # Record similar attributes as span.record_exception, but don't use it
            # to avoid having two events.
            attributes.update(
                {
                    SpanAttributes.EXCEPTION_TYPE: exc_value.__class__.__name__,
                    SpanAttributes.EXCEPTION_MESSAGE: str(exc_value),
                    SpanAttributes.EXCEPTION_STACKTRACE: "".join(
                        traceback.format_exception(*record.exc_info)
                    ),
                    SpanAttributes.EXCEPTION_ESCAPED: False,
                }
            )

        if record.levelno >= logging.ERROR:
            # Set the span's error status, which triggers special UI treatment. Also
            # set the log message as the span's status_message, but note that if there
            # are additional error logs the span will only show the latest (but events
            # will exist for all of the error logs).
            span.set_status(Status(StatusCode.ERROR, record.getMessage()))
            # Setting error status on a span adds the "error" attribute. Mimic that
            # for the event too.
            attributes["error"] = True

        attributes.update(ctx.current_ctx)  # TODO if updating to Python 3.9, use | .
        span.add_event(
            # Use the log message as the event name, for Honeycomb UI visibility.
            record.getMessage(),
            attributes=attributes,
        )
