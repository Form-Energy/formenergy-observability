#!/usr/bin/env python3
"""Demonstrate a multithreaded program which connects child traces to a parent.

This shows how to set up simple trace context propagation.
"""
import logging
import os
import random
import threading
import time

import opentelemetry

from form_observability import (
    ContextAwareTracer,
    OtelSpanEventHandler,
    configure,
)


_trace = ContextAwareTracer(__name__)
_log = logging.getLogger(__name__)


def _thread_worker(my_data: str, otel_ctx) -> None:
    """Worker which re-attaches the OpenTelemetry trace context.

    OpenTelemetry does not automatically pick up a parent thread's trace context in a
    child thread. So this worker must be explicitly passed the value from
    context.get_current() which it can then context.attach(..). Thus the child's span
    is parented under the main span.
    """
    opentelemetry.context.attach(otel_ctx)
    sleep_s = random.random() * 4
    with _trace.start_as_current_span(my_data, attributes={"sleep_s": sleep_s}):
        time.sleep(sleep_s)
        _trace.add_event("success")


@_trace.traced
def main():
    _log.info("Preparing to process in threads.")
    otel_ctx = opentelemetry.context.get_current()
    threads = []
    for my_data in ("one", "two", "three", "four", "five", "six"):
        thread = threading.Thread(target=_thread_worker, args=(my_data, otel_ctx))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    _log.info("Demo processing complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    header_env = "OTEL_EXPORTER_OTLP_HEADERS"
    endpoint_env = "OTEL_EXPORTER_OTLP_ENDPOINT"
    if header_env not in os.environ:
        _log.error(f"To upload traces, you must set {header_env}, see README.md.")
    configure(
        "form_observability_example",
        otlp_headers=os.environ.get(header_env),
        otlp_endpoint=os.environ.get(endpoint_env, "https://api.honeycomb.io"),
    )
    _log.addHandler(OtelSpanEventHandler())
    main()
