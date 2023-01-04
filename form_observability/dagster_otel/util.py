from typing import List


def get_run_id_and_ancestors(context) -> List[str]:
    """Returns a list starting with the current run's ID and then any ancestor runs.

    This allows searching for events when a run as been retried, and events might
    be attached to a parent run instead of this one.
    """
    run_id = context.run_id
    run_ids = [run_id]
    while True:
        run = context.instance.get_run_by_id(run_id)
        if run.parent_run_id:
            run_id = run.parent_run_id
            run_ids.append(run_id)
        else:
            break
    return run_ids
