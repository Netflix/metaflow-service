import pytest
from .utils import (
    cli, db,
    add_flow, add_run, add_artifact,
    add_step, add_task, add_metadata,
    _test_single_resource, get_heartbeat_ts
)
pytestmark = [pytest.mark.integration_tests]

# NOTE: For Runs which don’t have heartbeat enabled, for heartbeat checks fall back on using the timestamp of the latest metadata entry for the task as a proxy and set x and Y to 2 weeks.
# NOTE: For Runs which don’t have attempt_ok in metadata, utilize the value of attempt specific task_ok in s3 (IMPORTANT: For the time being this won't affect Run context)

# Run should have "Completed" status when:
#   1. End task has succeeded
#
# Sart time: created_at(ts_epoch) column value in the run table
# End time: End time for End task

@pytest.mark.skip("Test failing due to refactor. TODO: fix later if applicable")
async def test_run_status_completed(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="end", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    # Should not affect the status at all anymore
    _artifact = (await add_artifact(
        db,
        flow_id=_task.get("flow_id"),
        run_number=_task.get("run_number"),
        step_name="end",
        task_id=_task.get("task_id"),
        artifact={
            "name": "_task_ok",
            "location": "location",
            "ds_type": "ds_type",
            "sha": "sha",
            "type": "type",
            "content_type": "content_type",
                            "attempt_id": 0
        })).body

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200, None)

    assert data["status"] == "running"
    assert data["ts_epoch"] == _run["ts_epoch"]
    assert data["finished_at"] == None

    _metadata = (await add_metadata(db,
                                    flow_id=_task.get("flow_id"),
                                    run_number=_task.get("run_number"),
                                    run_id=_task.get("run_id"),
                                    step_name=_task.get("step_name"),
                                    task_id=_task.get("task_id"),
                                    task_name=_task.get("task_name"),
                                    tags=["attempt_id:0"],
                                    metadata={
                                        "field_name": "attempt_ok",
                                        "value": "True",
                                        "type": "internal_attempt_status"})).body

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200, None)

    assert data["status"] == "completed"
    assert data["ts_epoch"] == _run["ts_epoch"]
    assert data["finished_at"] == _metadata["ts_epoch"]
    assert data["duration"] == _metadata["ts_epoch"] - _run["ts_epoch"]


# Run should have "Running" status when all of the following apply:
#   1. No failed task
#   2. Run has not succeeded
#   3. Has logged a heartbeat in the last Y minutes for some task
#
# Sart time: created_at(ts_epoch) column value in the run table
# End time: Does not apply
@pytest.mark.skip("Test failing due to refactor. TODO: fix later if applicable")
async def test_run_status_running_no_failed_task(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="start", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    _artifact = (await add_artifact(
        db,
        flow_id=_task.get("flow_id"),
        run_number=_task.get("run_number"),
        step_name="start",
        task_id=_task.get("task_id"),
        artifact={
            "name": "_task_ok",
            "location": "location",
            "ds_type": "ds_type",
            "sha": "sha",
            "type": "type",
            "content_type": "content_type",
                            "attempt_id": 0
        })).body

    _metadata = (await add_metadata(db,
                                    flow_id=_task.get("flow_id"),
                                    run_number=_task.get("run_number"),
                                    run_id=_task.get("run_id"),
                                    step_name=_task.get("step_name"),
                                    task_id=_task.get("task_id"),
                                    task_name=_task.get("task_name"),
                                    tags=["attempt_id:0"],
                                    metadata={
                                        "field_name": "attempt_ok",
                                        "value": "True",
                                        "type": "internal_attempt_status"})).body

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200, None)

    assert data["status"] == "running"
    assert data["ts_epoch"] == _run["ts_epoch"]
    assert data["finished_at"] == None

@pytest.mark.skip("Test failing due to refactor. TODO: fix later if applicable")
async def test_run_status_running_run_not_succeeded(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="start", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200, None)

    assert data["status"] == "running"
    assert data["ts_epoch"] == _run["ts_epoch"]
    assert data["finished_at"] == None


async def test_run_status_running_with_heartbeat(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    _heartbeat = get_heartbeat_ts()

    _run = (await add_run(db, flow_id=_flow.get("flow_id"), last_heartbeat_ts=_heartbeat)).body

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200)

    assert data["status"] == "running"
    assert data["ts_epoch"] == _run["ts_epoch"]
    assert data["last_heartbeat_ts"] == _heartbeat
    assert data["duration"] == _run["last_heartbeat_ts"] * 1000 - _run["ts_epoch"]
    assert data["finished_at"] == None

@pytest.mark.skip("Test failing due to refactor. TODO: fix later if applicable")
async def test_run_status_failed_with_heartbeat_expired_and_failed_task(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    _heartbeat = get_heartbeat_ts() - 65  # old heartbeat, enough to count as expired, but not older than cutoff

    _run = (await add_run(db, flow_id=_flow.get("flow_id"), last_heartbeat_ts=_heartbeat)).body
    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200)

    # Should count as running if no tasks have failed, as tasks might be stuck in scheduler.
    assert data["status"] == "running"
    assert data["ts_epoch"] == _run["ts_epoch"]
    assert data["last_heartbeat_ts"] == _heartbeat
    assert data["finished_at"] == None

    # even when a run has a heartbeat, it still requires a task that has failed via attempt_ok=false OR by an expired heartbeat.
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="end", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"),
                            last_heartbeat_ts=1)).body

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200)

    assert data["status"] == "failed"
    assert data["ts_epoch"] == _run["ts_epoch"]
    assert data["last_heartbeat_ts"] == _heartbeat
    assert data["duration"] == _run["last_heartbeat_ts"] * 1000 - _run["ts_epoch"]
    assert data["finished_at"] == _run["last_heartbeat_ts"] * 1000


async def test_run_status_failed_with_heartbeat_expired_and_no_failed_tasks(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    _heartbeat = get_heartbeat_ts() - 60 * 60 * 24 * 7  # heartbeat older than threshold to count run as stuck, even with no failed tasks

    _run = (await add_run(db, flow_id=_flow.get("flow_id"), last_heartbeat_ts=_heartbeat)).body
    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200)

    # Should count as running if no tasks have failed, as tasks might be stuck in scheduler.
    assert data["status"] == "failed"
    assert data["ts_epoch"] == _run["ts_epoch"]
    assert data["last_heartbeat_ts"] == _heartbeat
    assert data["finished_at"] == _run["last_heartbeat_ts"] * 1000

    # even if the run has no failed tasks, it should count as failed at this point due to not receiving heartbeats
    # on the run level for long enough (should count as stuck, ie. 'failed')
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="end", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"),
                            last_heartbeat_ts=get_heartbeat_ts())).body

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200)

    assert data["status"] == "failed"
    assert data["ts_epoch"] == _run["ts_epoch"]
    assert data["last_heartbeat_ts"] == _heartbeat
    assert data["duration"] == _run["last_heartbeat_ts"] * 1000 - _run["ts_epoch"]
    assert data["finished_at"] == _run["last_heartbeat_ts"] * 1000

    # Run should be possibly to be bumped to 'completed' with an eventually successful end-task
    _metadata = (await add_metadata(db,
                                    flow_id=_task.get("flow_id"),
                                    run_number=_task.get("run_number"),
                                    run_id=_task.get("run_id"),
                                    step_name=_task.get("step_name"),
                                    task_id=_task.get("task_id"),
                                    task_name=_task.get("task_name"),
                                    tags=["attempt_id:0"],
                                    metadata={
                                        "field_name": "attempt_ok",
                                        "value": "True",
                                        "type": "internal_attempt_status"})).body

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200)

    assert data["status"] == "completed"
    assert data["ts_epoch"] == _run["ts_epoch"]
    assert data["last_heartbeat_ts"] == _heartbeat
    assert data["duration"] == _metadata["ts_epoch"] - _run["ts_epoch"]
    assert data["finished_at"] == _metadata["ts_epoch"]

# Run should have "Failed" status when any of the following apply:
#   1. A task has failed
#   2. No heartbeat has been logged for the task in the last Y minutes for any task
#
# Sart time: created_at(ts_epoch) column value in the run table
# End time: Latest end time for all tasks

@pytest.mark.skip("Test failing due to refactor. TODO: fix later if applicable")
async def test_run_status_failed_with_retrying_task(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    _expired_heartbeat = get_heartbeat_ts() - 610

    _run = (await add_run(db, flow_id=_flow.get("flow_id"), last_heartbeat_ts=_expired_heartbeat)).body
    # even when a run has a heartbeat, it still requires a task that has failed via attempt_ok=false OR by an expired heartbeat.
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="any_step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"),
                            last_heartbeat_ts=get_heartbeat_ts())).body

    # task does not count as failed yet, so expired run heartbeat should not fail the run either.
    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200)

    assert data["status"] == "running"
    # assert data["last_heartbeat_ts"] == _heartbeat
    # assert data["duration"] == _run["last_heartbeat_ts"] * 1000 - _run["ts_epoch"]
    # assert data["finished_at"] == _run["last_heartbeat_ts"] * 1000

    await db.task_table_postgres.update_row(
        filter_dict={
            "flow_id": _task.get("flow_id"),
            "run_number": _task.get("run_number"),
            "step_name": _task.get("step_name"),
            "task_id": _task.get("task_id")
        },
        update_dict={
            "last_heartbeat_ts": _expired_heartbeat
        }
    )

    # Task counts as failed now, run should also be failed
    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200)

    assert data["status"] == "failed"

    await db.task_table_postgres.update_row(
        filter_dict={
            "flow_id": _task.get("flow_id"),
            "run_number": _task.get("run_number"),
            "step_name": _task.get("step_name"),
            "task_id": _task.get("task_id")
        },
        update_dict={
            "last_heartbeat_ts": get_heartbeat_ts()
        }
    )

    # Task counts as running again, run should also count as running.
    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200)

    assert data["status"] == "running"

    _metadata = (await add_metadata(db,
                                    flow_id=_task.get("flow_id"),
                                    run_number=_task.get("run_number"),
                                    run_id=_task.get("run_id"),
                                    step_name=_task.get("step_name"),
                                    task_id=_task.get("task_id"),
                                    task_name=_task.get("task_name"),
                                    tags=["attempt_id:0"],
                                    metadata={
                                        "field_name": "attempt_ok",
                                        "value": "False",
                                        "type": "internal_attempt_status"})).body

    # Task counts as failed again, run should count as failed once task heartbeat expires
    # (no successive attempt of the task is updating the heartbeat).
    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200)

    assert data["status"] == "running"

    await db.task_table_postgres.update_row(
        filter_dict={
            "flow_id": _task.get("flow_id"),
            "run_number": _task.get("run_number"),
            "step_name": _task.get("step_name"),
            "task_id": _task.get("task_id")
        },
        update_dict={
            "last_heartbeat_ts": _expired_heartbeat
        }
    )

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200)

    assert data["status"] == "failed"
