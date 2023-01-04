from collections import namedtuple
import time

from dagster import DagsterEventType

from form_observability.dagster_otel.util import get_run_id_and_ancestors


TimingInfo = namedtuple("TimingInfo", ["start_s", "end_s", "dt_s"])


def get_timing_since_step_started(
    context,
    step_key: str,
    seconds_fn=time.time,
) -> TimingInfo:
    """Returns timing information relative to the provided Dagster step key.

    :return: A tuple of timing information: start_s (when the named step started in
        epoch seconds), end_s (now), and dt_s (the time delta in seconds).
    """
    t = seconds_fn()
    run_ids = get_run_id_and_ancestors(context)
    all_searched_logs = []
    for run_id in run_ids:
        logs = context.instance.all_logs(run_id, DagsterEventType.STEP_START)
        all_searched_logs += logs
        for event_log_entry in logs:
            if event_log_entry.dagster_event.step_handle.key == step_key:
                timestamp_s = event_log_entry.timestamp
                return TimingInfo(timestamp_s, t, t - timestamp_s)
    raise RuntimeError(
        f"Cannot add elapsed time, step key {step_key} not found in STEP_START"
        f" events: {[e.dagster_event.step_handle.key for e in all_searched_logs]}."
        f" Searched run IDs: {run_ids}."
    )
