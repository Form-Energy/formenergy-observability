import uuid
from types import SimpleNamespace

import pytest
from dagster import (
    AssetKey,
    AssetOut,
    DailyPartitionsDefinition,
    MaterializeResult,
    Output,
    materialize,
)
import opentelemetry
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import StatusCode

from form_observability import ContextAwareTracer, SpanAttrKey, SpanName
from form_observability.dagster_otel import (
    CONFIGURE_OBSERVABILITY_RESOURCE_KEY,
    otel_asset,
    otel_multi_asset,
    publish_current_trace_context,
    run_trace_context,
)
from form_observability.dagster_otel import otel_ops
from form_observability.dagster_otel.testing import noop_resource

# Every test here checks real spans, so don't replace get_current_span.
pytestmark = pytest.mark.no_mock_get_current_span

_trace = ContextAwareTracer(__name__)
RESOURCES = {CONFIGURE_OBSERVABILITY_RESOURCE_KEY: noop_resource}


@pytest.fixture
def spans(monkeypatch):
    """Records spans from the library's tracer (and this module's) in memory.

    Uses a local TracerProvider rather than setting the global one, so other tests
    are unaffected.
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer(__name__)
    monkeypatch.setattr(otel_ops._tracer, "_tracer", tracer)
    monkeypatch.setattr(_trace, "_tracer", tracer)
    yield exporter
    provider.shutdown()


def _by_name(exporter):
    return {s.name: s for s in exporter.get_finished_spans()}


def _parent_ids(carrier):
    _, trace_id, span_id, _ = carrier["traceparent"].split("-")
    return int(trace_id, 16), int(span_id, 16)


def test_assets_in_a_run_share_a_trace_derived_from_the_run_id(spans):
    @otel_asset()
    def upstream(context):
        return 2

    @otel_asset(key_prefix=["lab"])
    def downstream(context, upstream):
        _trace.add_event("computed", {"value": upstream * 3})
        return upstream * 3

    result = materialize([upstream, downstream], resources=RESOURCES)
    assert result.success
    assert result.output_for_node("lab__downstream") == 6

    by_name = _by_name(spans)
    assert set(by_name) == {"upstream", "lab/downstream"}
    expected_trace_id = uuid.UUID(result.run_id).int
    parent_span_ids = set()
    for span in by_name.values():
        assert span.context.trace_id == expected_trace_id
        assert span.parent is not None and span.parent.is_remote
        parent_span_ids.add(span.parent.span_id)
        assert span.attributes[SpanAttrKey.DAGSTER_RUN_ID] == result.run_id
        assert span.attributes["process.resource.self.maxrss_b"] > 0
    # Both steps computed the same synthetic parent span on their own.
    assert len(parent_span_ids) == 1
    assert (
        by_name["lab/downstream"].attributes[SpanAttrKey.DAGSTER_ASSET_KEY]
        == "lab/downstream"
    )
    assert [e.name for e in by_name["lab/downstream"].events] == ["computed"]


def test_generator_asset_and_materialize_result(spans):
    @otel_asset(span_name="custom-name", ctx_attributes={"test_id": "T1"})
    def gen_asset(context):
        _trace.add_event("inside")
        yield Output(5, metadata={"rows": 5})

    @otel_asset()
    def reported_asset(context):
        return MaterializeResult(metadata={"rows": 7})

    result = materialize([gen_asset, reported_asset], resources=RESOURCES)
    assert result.success
    assert result.output_for_node("gen_asset") == 5

    by_name = _by_name(spans)
    assert set(by_name) == {"custom-name", "reported_asset"}
    # ctx_attributes are inherited by events recorded inside the asset.
    event = by_name["custom-name"].events[0]
    assert event.attributes["test_id"] == "T1"
    materializations = result.asset_materializations_for_node("reported_asset")
    assert materializations[0].metadata["rows"].value == 7


def test_partitioned_asset_records_partition_key(spans):
    @otel_asset(partitions_def=DailyPartitionsDefinition(start_date="2024-01-01"))
    def daily(context):
        return context.partition_key

    result = materialize([daily], resources=RESOURCES, partition_key="2024-03-05")
    assert result.success
    (span,) = spans.get_finished_spans()
    assert span.attributes[SpanAttrKey.DAGSTER_PARTITION_KEY] == "2024-03-05"


def test_failing_asset_records_exception_on_span(spans):
    @otel_asset()
    def broken(context):
        raise ValueError("bad battery data")

    result = materialize([broken], resources=RESOURCES, raise_on_error=False)
    assert not result.success

    (span,) = spans.get_finished_spans()
    assert span.status.status_code == StatusCode.ERROR
    assert any(
        e.name == "exception"
        and "bad battery data" in e.attributes["exception.message"]
        for e in span.events
    )


def test_multi_asset_records_selected_keys(spans):
    @otel_multi_asset(outs={"a": AssetOut(), "b": AssetOut()})
    def pair(context):
        return 1, 2

    result = materialize([pair], resources=RESOURCES)
    assert result.success

    (span,) = spans.get_finished_spans()
    assert span.name == "pair"
    assert tuple(span.attributes[SpanAttrKey.DAGSTER_ASSET_KEYS]) == ("a", "b")


def test_published_trace_context_takes_precedence(spans):
    @otel_asset()
    def root(context):
        with _trace.start_as_current_span(SpanName.ROOT) as root_span:
            publish_current_trace_context(context)
            return root_span.get_span_context().span_id

    @otel_asset()
    def child(context, root):
        return root

    result = materialize([root, child], resources=RESOURCES)
    assert result.success

    by_name = _by_name(spans)
    root_span = by_name[SpanName.ROOT]
    assert by_name["child"].context.trace_id == root_span.context.trace_id
    assert by_name["child"].parent.span_id == root_span.context.span_id


def test_run_trace_context_uses_root_run_for_reexecutions():
    root_run_id = str(uuid.uuid4())
    reexecution = SimpleNamespace(
        run_id=str(uuid.uuid4()), run=SimpleNamespace(root_run_id=root_run_id)
    )
    original = SimpleNamespace(
        run_id=root_run_id, run=SimpleNamespace(root_run_id=None)
    )
    carrier = run_trace_context(reexecution)
    assert carrier == run_trace_context(original)
    trace_id, span_id = _parent_ids(carrier)
    assert trace_id == uuid.UUID(root_run_id).int
    assert span_id != 0


def test_run_trace_context_accepts_non_uuid_run_ids():
    context = SimpleNamespace(
        run_id="not-a-uuid", run=SimpleNamespace(root_run_id=None)
    )
    trace_id, span_id = _parent_ids(run_trace_context(context))
    assert trace_id != 0 and span_id != 0


def test_otel_asset_requires_context_argument():
    with pytest.raises(TypeError, match="must take `context`"):

        @otel_asset()
        def no_context(upstream):
            return upstream


def test_otel_asset_adds_observability_resource_requirement():
    @otel_asset(required_resource_keys={"other"})
    def needs_resources(context):
        return 1

    assert {"other", CONFIGURE_OBSERVABILITY_RESOURCE_KEY} <= set(
        needs_resources.required_resource_keys
    )
