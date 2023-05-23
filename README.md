# Form Energy Observability Python Library

OTel tracing and logging tools, emphasizing data pipeline use cases with Dagster.

## Using the Library

### [Install from PyPI](https://pypi.org/project/formenergy-observability/)

`pip install formenergy-observability`

### Features

See the [examples/](examples/) directory for examples you can run and screenshots of the traces they produce.

Some key features demonstrated in the examples:

Context-aware tracing, so that spans and events can inherit attributes from parents:

```py
from form_observability import ContextAwareTracer, ctx

_trace = ContextAwareTracer(__name__)

with ctx.set({"my_attribute_name": my_attribute_value}):
    # There could be many intervening function calls and spans.
    # Spans and events in this context automatically have the my_attribute_name
    # attribute set on them.
    _trace.add_event("my_event")
```

Trace context propagation for Dagster ops:

```py
from form_observability.dagster_otel import otel_op, publish_current_trace_context
from form_observability import ContextAwareTracer

_trace = ContextAwareTracer(__name__)

@op
def root_trace_context_op(context):
    with _trace.start_as_current_span("root"):
        # writes the trace context (as an asset materialization event) for later ops
        publish_current_trace_context(context)

@otel_op()  # picks up the trace context and starts a new span for this op
def my_op(context):
    _trace.add_event("my_event")

@graph
def my_graph()
    # In the graph, publish a root trace using a regular op, and then subsequent
    # @otel_op ops will pick it up and contribute to the same trace.
    setup_done = root_trace_context_op()
    my_op(start=setup_done)
```

### Running the Examples

Get an API key for uploading traces to your observability service (preferably in a development environment), set it in your environment, and run the examples. This should publish traces that you can then view.

```bash
export OTEL_EXPORTER_OTLP_HEADERS="x-honeycomb-team=<API key>"
export OTEL_EXPORTER_OTLP_ENDPOINT="your observability service, defaults to Honeycomb"
python examples/basics.py
dagster job execute --python-file examples/dagster_job.py
```

## License

This library is provided under the MIT license, see [LICENSE](LICENSE).

## Release Notes

*   0.3.0 Add `ContextAwareTracer.start_new_linked_trace`.
*   0.2.0 Switch to `context.op_handle` and `materialization.metadata` as dict for Dagster 1.2.
*   0.1.0 Initial external release, including `ContextAwareTracer`, `ctx`, and `@otel_op`.

## Development

If you contribute, please ensure you and your employer accept [the license](LICENSE) for any of your contributions.

### Setup

Create and activate a virtual environment with your favorite tool, such as [direnv](https://github.com/direnv/direnv/wiki/Python). This project requires Python 3.8.

```bash
pip install flit
flit install --deps all --symlink [--python /path/to/venv/python]
```

By default, `flit` installs into the current virtual environment. Using the `--python` argument can let you install into a specific venv other than the currently active one. For example you can activate some other venv, run `which python`, and then pass the output of that command to `flit`'s `--python` argument when installing from the `form-observability` repo.

Initialize pre-commit hooks:

```bash
pre-commit install --hook-type pre-commit --hook-type pre-push
```

### Running Tests

From the project root directory:

```bash
pytest
```
