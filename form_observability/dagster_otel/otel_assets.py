"""OpenTelemetry tracing for Dagster software-defined assets.

Op graphs have a natural root op which publishes a trace context (see
publish_current_trace_context) for the rest of the graph. Asset runs usually have
no such root: each selected asset is its own step, steps may run in separate
processes, and runs are often started by schedules, sensors or auto-materialization.

So @otel_asset / @otel_multi_asset pick a trace context for the run like this:

1.  If a trace context was published in this run (or an ancestor run) with
    publish_current_trace_context, use it, exactly like @otel_op.
2.  Otherwise derive the trace context from the run's root run ID. Every asset step
    in the run, and in any re-execution of it, computes the same trace ID and parent
    span ID, so all of their spans land in one trace without coordination.
"""
from contextlib import contextmanager
from functools import wraps
import hashlib
import inspect
from typing import Any, Callable, Dict, Optional
import uuid

import dagster
import opentelemetry
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from form_observability.context_aware import ctx
from form_observability.dagster_otel.otel_ops import (
    CONFIGURE_OBSERVABILITY_RESOURCE_KEY,
    _find_trace_context,
    _set_span_maxrss,
    _tracer,
)
from form_observability.otel_value import SpanAttrKey


def run_trace_context(context) -> Dict[str, str]:
    """Returns a W3C trace context carrier derived from the Dagster run.

    The trace ID is the root run's ID (a UUID, which is 128 bits like a trace ID), so
    re-executions of a run continue the original run's trace, and the trace can be
    found from the run ID shown in the Dagster UI. The parent span ID is a hash of the
    same root run ID.
    """
    # `context.run` replaced `context.dagster_run` in newer Dagster versions.
    run = getattr(context, "run", None) or getattr(context, "dagster_run", None)
    root_run_id = getattr(run, "root_run_id", None) or context.run_id
    try:
        trace_id = uuid.UUID(root_run_id).int
    except ValueError:
        trace_id = int.from_bytes(
            hashlib.sha256(root_run_id.encode()).digest()[:16], "big"
        )
    span_id = int.from_bytes(
        hashlib.sha256(f"span:{root_run_id}".encode()).digest()[:8], "big"
    )
    # All-zero IDs are invalid in W3C trace context.
    trace_id = trace_id or 1
    span_id = span_id or 1
    return {"traceparent": f"00-{trace_id:032x}-{span_id:016x}-01"}


def _asset_trace_context(context) -> Dict[str, str]:
    try:
        return _find_trace_context(context)
    except RuntimeError:
        return run_trace_context(context)


@contextmanager
def _asset_span(context, span_name: Optional[str], ctx_attributes: Dict[str, Any]):
    # Newer Dagster versions deprecate run/op accessors on AssetExecutionContext in
    # favor of the wrapped OpExecutionContext.
    context = getattr(context, "op_execution_context", context)
    trace_context = _asset_trace_context(context)
    parent = TraceContextTextMapPropagator().extract(trace_context)
    token = opentelemetry.context.attach(parent)
    try:
        selected_keys = sorted(
            key.to_user_string() for key in context.selected_asset_keys
        )
        if span_name:
            name = span_name
        elif len(selected_keys) == 1:
            name = selected_keys[0]
        else:
            name = context.op_handle.name
        attributes = {SpanAttrKey.DAGSTER_RUN_ID: context.run_id}
        if len(selected_keys) == 1:
            attributes[SpanAttrKey.DAGSTER_ASSET_KEY] = selected_keys[0]
        else:
            attributes[SpanAttrKey.DAGSTER_ASSET_KEYS] = selected_keys
        if context.has_partition_key:
            attributes[SpanAttrKey.DAGSTER_PARTITION_KEY] = context.partition_key

        with ctx.set(ctx_attributes), _tracer.start_as_current_span(
            name, attributes=attributes
        ) as span:
            try:
                yield span
            finally:
                _set_span_maxrss(span)
    finally:
        opentelemetry.context.detach(token)


def _check_takes_context(func: Callable, decorator_name: str) -> None:
    params = list(inspect.signature(func).parameters)
    if not params or params[0] != "context":
        raise TypeError(
            f"@{decorator_name} function {func.__name__!r} must take `context` as its"
            " first argument, it is used to find the run's trace context."
        )


def _traced(func: Callable, span_name: Optional[str], ctx_attributes: Dict[str, Any]):
    """Wraps an asset compute function so it runs inside a span.

    Generator functions stay generators (so yielded Outputs/MaterializeResults pass
    through), and plain functions keep returning their value to Dagster.
    """
    if inspect.isgeneratorfunction(func):

        @wraps(func)
        def traced_generator(context, *args, **kwargs):
            with _asset_span(context, span_name, ctx_attributes):
                yield from func(context, *args, **kwargs)

        return traced_generator

    @wraps(func)
    def traced_function(context, *args, **kwargs):
        with _asset_span(context, span_name, ctx_attributes):
            return func(context, *args, **kwargs)

    return traced_function


def _add_observability_resource(decorator_kwargs: Dict[str, Any]) -> None:
    rkeys = "required_resource_keys"
    decorator_kwargs[rkeys] = set(decorator_kwargs.get(rkeys) or set())
    decorator_kwargs[rkeys].add(CONFIGURE_OBSERVABILITY_RESOURCE_KEY)


def otel_asset(
    *decorator_args,
    span_name: Optional[str] = None,
    ctx_attributes: Dict[str, Any] = {},
    **decorator_kwargs,
):
    """A wrapper for @asset which runs the asset's computation in an OpenTelemetry span.

    The span joins the run's trace (see the module docs), is named for the asset key
    (or span_name), and has the run ID, asset key and partition key (if any) as
    attributes, plus the process's peak memory like @otel_op.

    Like @otel_op, this requires the configure_observability resource, and the
    decorated function must take `context` as its first argument. Other arguments
    (upstream assets, config) work as with @asset.

    Usage:
        @otel_asset()
        def my_asset(context, upstream_asset):
            ...

    :param span_name: A specific span name to use. Defaults to the asset key.
    :param ctx_attributes: Span / context attributes for this asset's computation.
    """
    _add_observability_resource(decorator_kwargs)

    def wrapper(func):
        _check_takes_context(func, "otel_asset")
        return dagster.asset(*decorator_args, **decorator_kwargs)(
            _traced(func, span_name, ctx_attributes)
        )

    return wrapper


def otel_multi_asset(
    *decorator_args,
    span_name: Optional[str] = None,
    ctx_attributes: Dict[str, Any] = {},
    **decorator_kwargs,
):
    """A wrapper for @multi_asset, see otel_asset.

    The span is named span_name, or the op name, and records the selected asset keys.
    """
    _add_observability_resource(decorator_kwargs)

    def wrapper(func):
        _check_takes_context(func, "otel_multi_asset")
        return dagster.multi_asset(*decorator_args, **decorator_kwargs)(
            _traced(func, span_name, ctx_attributes)
        )

    return wrapper
