"""OTel tracing/logging tools, emphasizing data pipeline use cases with Dagster."""

#: Semantic versioning number. The three dotted numbers are:
#: *   Major: breaking API changes, significant feature additions.
#: *   Minor: standard feature additions and improvements. No breaking changes.
#: *   Micro: small bug fixes.
__version__ = "0.3.0"

# Make commonly used symbols importable from form_observability.
from .context_aware import ContextAwareTracer, ctx
from .log import OtelSpanEventHandler
from .otel_value import EventAttrKey, EventAttrValue, SpanName
from .pipeline_setup import configure
