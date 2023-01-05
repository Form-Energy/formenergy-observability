#!/usr/bin/env python3
"""Demonstrate a basic program that generates OpenTelemetry tracing."""
import logging
import os
import random
import time

import opentelemetry

from form_observability import (
    ContextAwareTracer,
    OtelSpanEventHandler,
    configure,
    ctx,
)


_trace = ContextAwareTracer(__name__)
_log = logging.getLogger(__name__)


def _helper_that_sometimes_catches_exceptions_internally(my_data: str) -> None:
    sleep_s = random.random()
    err_f = random.random()
    # Set some attributes that will be added to any spans and events in this context.
    with ctx.set(
        {
            "data_size": len(my_data),
            "sleep_s": sleep_s,
            "err_f": err_f,
        },
        # Also set these attributes on the active span.
        update_current_span=True,
    ):
        try:
            time.sleep(sleep_s)
            if err_f < 0.1:
                raise RuntimeError("Demo error to show exception treatment.")
            # This success event will have the data_size attribute set on it from
            # the context above, as well as the my_data attribute set in main().
            _trace.add_event("success")
            return True
        except:
            _trace.add_event("failure")
            _log.exception(f"Error during processing of {my_data=}.")
            return False


@_trace.traced  # Open a span for this function call. In this case it's the root span.
def main():
    _log.info("Preparing to process tests.")
    num_errors = 0
    for my_data in ("one", "two", "three", "four", "five", "six"):
        with _trace.start_as_current_span("work", attributes={"my_data": my_data}):
            if _helper_that_sometimes_catches_exceptions_internally(my_data):
                num_errors += 1
    opentelemetry.trace.get_current_span().set_attributes({"num_errors": num_errors})
    _log.info("Demo processing complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    header_env = "OTEL_EXPORTER_OTLP_HEADERS"
    endpoint_env = "OTEL_EXPORTER_OTLP_ENDPOINT"
    if header_env not in os.environ:
        _log.error(f"To upload traces, you must set {header_env}, see README.md.")
    # Set up a simple span exporter with no sampling, suitable for data pipelines.
    configure(
        "form_observability_example",  # demo dataset name
        otlp_headers=os.environ.get(header_env),
        otlp_endpoint=os.environ.get(endpoint_env, "https://api.honeycomb.io"),
    )
    _log.addHandler(OtelSpanEventHandler())  # Add span events for log messages.
    main()
