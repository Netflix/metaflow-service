import pytest
from .utils import (
    init_app, init_db, clean_db,
    add_flow, add_run, add_artifact,
    add_step, add_task, add_metadata,
    _test_list_resources, _test_single_resource, get_heartbeat_ts
)
pytestmark = [pytest.mark.integration_tests]

# Fixtures begin


@pytest.fixture
def cli(loop, aiohttp_client):
    return init_app(loop, aiohttp_client)


@pytest.fixture
async def db(cli):
    async_db = await init_db(cli)
    yield async_db
    await clean_db(async_db)

# Fixtures end

# NOTE: For Runs which don’t have heartbeat enabled, for heartbeat checks fall back on using the timestamp of the latest metadata entry for the task as a proxy and set x and Y to 2 weeks.
# NOTE: For Runs which don’t have attempt_ok in metadata, utilize the value of attempt specific task_ok in s3 (IMPORTANT: For the time being this won't affect Run context)

# Run should have "Completed" status when:
#   1. End task has succeeded
#
# Sart time: created_at(ts_epoch) column value in the run table
# End time: End time for End task


async def test_run_status_completed(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="end", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

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

    assert data["status"] == "completed"
    assert data["ts_epoch"] == _run["ts_epoch"]
    assert data["finished_at"] == _artifact["ts_epoch"]

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
    assert data["finished_at"] == _artifact["ts_epoch"]


# Run should have "Running" status when all of the following apply:
#   1. No failed task
#   2. Run has not succeeded
#   3. Has logged a heartbeat in the last Y minutes for some task
#
# Sart time: created_at(ts_epoch) column value in the run table
# End time: Does not apply

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


# Run should have "Failed" status when any of the following apply:
#   1. A task has failed
#   2. No heartbeat has been logged for the task in the last Y minutes for any task
#
# Sart time: created_at(ts_epoch) column value in the run table
# End time: Latest end time for all tasks

async def test_run_status_failed_failed_task(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="end", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

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

    # create a failed last attempt (max attempts for a task is 4, before this we might still be retrying a task, so run should not count as failed)
    _metadata = (await add_metadata(db,
                                    flow_id=_task.get("flow_id"),
                                    run_number=_task.get("run_number"),
                                    run_id=_task.get("run_id"),
                                    step_name=_task.get("step_name"),
                                    task_id=_task.get("task_id"),
                                    task_name=_task.get("task_name"),
                                    tags=["attempt_id:4"],
                                    metadata={
                                        "field_name": "attempt_ok",
                                        "value": "False",
                                        "type": "internal_attempt_status"})).body

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200, None)

    assert data["status"] == "failed"
    assert data["ts_epoch"] == _run["ts_epoch"]
    assert data["finished_at"] == _artifact["ts_epoch"]


async def test_run_status_failed_with_heartbeat_expired(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    _heartbeat = get_heartbeat_ts()

    _run = (await add_run(db, flow_id=_flow.get("flow_id"), last_heartbeat_ts=1)).body

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200)

    assert data["status"] == "failed"
    assert data["ts_epoch"] == _run["ts_epoch"]
    assert data["last_heartbeat_ts"] == 1
    assert data["duration"] == _run["last_heartbeat_ts"] * 1000 - _run["ts_epoch"]
    assert data["finished_at"] == _run["last_heartbeat_ts"] * 1000
