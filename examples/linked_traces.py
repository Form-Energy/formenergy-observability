#!/usr/bin/env python3
"""Demonstrate linked traces with context carried over."""
import logging
import os
import random
import time

import opentelemetry
from opentelemetry.trace import Link

from form_observability import (
    ContextAwareTracer,
    OtelSpanEventHandler,
    configure,
    ctx,
)


_tracer = ContextAwareTracer(__name__)
_log = logging.getLogger(__name__)


@_tracer.traced
def _helper() -> None:
    """Do something in a child span of the linked trace."""
    _tracer.add_event("helped", attributes={"value": random.random()})


@_tracer.traced  # Open a span for this function call. In this case it's the root span.
def main():
    child_links = []
    for my_data in ("one", "two", "three"):
        with ctx.set({"my_data": my_data}):
            with _tracer.start_new_linked_trace("work") as child_ctx:
                child_links.append(Link(child_ctx))
                _helper()
    # Add forward links to the child traces.
    # OTEL doesn't support adding links to an existing span, but it's requested at
    # https://github.com/open-telemetry/opentelemetry-specification/issues/454 .
    with _tracer.start_as_current_span("children", links=child_links):
        pass


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
    main()
