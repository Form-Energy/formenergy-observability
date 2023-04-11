from collections import namedtuple
from functools import wraps
import inspect
import resource
import platform
import time
from typing import Optional, Dict, Any

import dagster
from dagster import AssetKey
from dagster import AssetMaterialization
from dagster import DagsterEventType
from dagster import Output
from dagster import op
import opentelemetry
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from form_observability.context_aware import ContextAwareTracer, ctx
from form_observability.otel_value import SpanName
from form_observability.dagster_otel.util import get_run_id_and_ancestors


#: A resource that configures OpenTelemetry exporting should be registered under
#: this key. For example:
#:
#: @dagster.resource
#: def configure_observability_resource():
#:     """Run observability configuration setup as a Dagster resource.
#:
#:     This ensures span processing is set up before each op runs, because resources are
#:     initialized once per execution context. So this will be called by Dagster once in
#:     each Python interpreter.
#:     """
#:     pipeline_setup.configure(..)
CONFIGURE_OBSERVABILITY_RESOURCE_KEY = "configure_observability"


_tracer = ContextAwareTracer(__name__)


def publish_current_trace_context(context: dagster.OpExecutionContext) -> None:
    """Stores the current trace context, to be picked up by @otel_op.

    Should be called exactly once per graph.

    The context published here is automatically picked up by @otel_op
    using _set_trace_context.

    The trace context is published as JSON metadata on an AssetMaterializationEvent,
    see Dagster event docs:
    https://docs.dagster.io/concepts/ops-jobs-graphs/op-events#op-events-and-exceptions
    This is just using Dagster events as a shared db that does not require further
    setup, but trace contexts could instead be stored in any other custom store. See
    https://github.com/dagster-io/dagster/issues/7581 which requests a way to pipe
    one op's output to all downstream ops.
    """
    span = opentelemetry.trace.get_current_span()
    if not span:
        raise RuntimeError("No active span, cannot publish trace context.")
    carrier = {}
    TraceContextTextMapPropagator().inject(carrier)

    # If this is in a subgraph, use the subgraph name as the trace key. Otherwise,
    # it's not in a subgraph, use the root span name.
    # Dagster's internal convention is to use dotted namespaces for subgraphs.
    op_path = context.op_handle.path
    trace_key = SpanName.ROOT if len(op_path) <= 1 else ".".join(op_path[:-1])

    context.log_event(
        AssetMaterialization(
            asset_key=AssetKey(trace_key),
            description="OpenTelemetry trace context.",
            metadata={
                "trace_context": carrier,
            },
        ),
    )


def _find_trace_context(context):
    # Try to find a trace context for this subgraph, but fall back to parent graphs
    # up to the root.
    trace_keys = []
    remaining_path = context.op_handle.path[:-1]
    while remaining_path:
        key = ".".join(remaining_path)
        remaining_path.pop()
        trace_keys.append(key)
    trace_keys.append(SpanName.ROOT)

    run_ids = get_run_id_and_ancestors(context)
    all_searched_logs = []
    for run_id in run_ids:
        logs = context.instance.all_logs(run_id, DagsterEventType.ASSET_MATERIALIZATION)
        all_searched_logs += logs

        for trace_key in trace_keys:
            for event_log_entry in logs:
                materialization = (
                    event_log_entry.dagster_event.event_specific_data.materialization
                )
                event_asset_key = materialization.asset_key.path[0]
                if event_asset_key == trace_key:
                    return materialization.metadata["trace_context"].data

    raise RuntimeError(
        f"Could not find trace context, searched for any of {trace_keys} in"
        f" {len(all_searched_logs)} events from run IDs {run_ids}."
    )


def _set_trace_context(trace_context):
    """Sets the current trace context from the given propagated trace context carrier.

    See Python trace context propagation docs:
    https://opentelemetry.io/docs/instrumentation/python/cookbook/#manually-setting-span-context
    and conceptual docs:
    https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/context/api-propagators.md
    """
    ctx = TraceContextTextMapPropagator().extract(trace_context)
    opentelemetry.context.attach(ctx)


def otel_op(
    *decorator_args,
    span_name: Optional[str] = None,
    on_exception_return: Optional[Output] = None,
    ctx_attributes: Dict[str, Any] = {},
    **decorator_kwargs,
):
    """A wrapper for @op which automatically activates OpenTelemetry trace contexts.

    This requires the configure_observability resource key for every @otel_op, to
    ensure span processing is set up. (In tests, provide a noop resource.)

    This looks for the published root trace context or any appropriate subgraph context,
    and activates it.

    Then it also starts a span named for the current function, or the provided name.

    :param span_name: A specific span name to use, for ease of viewing in observability
        services. If not given, the span name falls back to (a) any explicit Dagster
        @op name, or (b) the decorated function's name.
    :param on_exception_return: If the @op raises an exception, swallow it and yield
        this value from the op. This should be used for per-test processing steps which
        should not halt overall graph execution. Typically Output(None, "result").
    :param ctx_attributes: Span / context attributes for this op.
    """
    op_name = decorator_kwargs.get("name")

    rkeys = "required_resource_keys"
    if rkeys not in decorator_kwargs:
        decorator_kwargs[rkeys] = set()
    decorator_kwargs[rkeys].add(CONFIGURE_OBSERVABILITY_RESOURCE_KEY)

    def wrapper(func):
        @op(*decorator_args, **decorator_kwargs)
        @wraps(func)
        # If the wrapped op is missing a context argument, this will get an error like
        # "raw_op() missing 1 required positional argument: 'context'". But we need
        # the context below, and want Dagster to pass args according to the wrapped
        # functions args so just require all @otel_ops to take context.
        def raw_op(context, *func_args, **func_kwargs):
            # Check for possible/occasional OpenTelemetry shutdown hangs.
            # https://github.com/open-telemetry/opentelemetry-python/issues/2284

            trace_context = _find_trace_context(context)
            _set_trace_context(trace_context)
            name = span_name or op_name or func.__name__
            with ctx.set(ctx_attributes), _tracer.start_as_current_span(name) as span:
                try:
                    ret = func(context, *func_args, **func_kwargs)
                    if inspect.isgenerator(ret):
                        # Yield from the generator here rather than just returning it so
                        # that the generator executes within the context manager.
                        yield from ret
                    else:
                        yield Output(ret, "result")
                except Exception as e:
                    if on_exception_return:
                        # Logging will indirectly record a span exception.
                        context.log.exception(str(e))
                        yield on_exception_return
                    else:
                        # Span context managers automatically record exceptions exiting
                        # their context.
                        raise
                finally:
                    # Record the maximum resident set size (peak RAM usage) of this
                    # process. For a Python multiprocessing executor (or executor using
                    # a container per op) this will indicate the op's max memory usage.
                    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                    maxrss_b = (
                        maxrss
                        * {
                            # Linux documents maxrss units as kilobytes.
                            "linux": 1000,
                            # MacOS X doesn't document units but they seem to be bytes,
                            # for example a fresh Python session might report 10989568.
                            "darwin": 1,
                        }[platform.system().lower()]
                    )
                    span.set_attributes(
                        {
                            "process.resource.self.maxrss_b": maxrss_b,
                        }
                    )

        return raw_op

    return wrapper
