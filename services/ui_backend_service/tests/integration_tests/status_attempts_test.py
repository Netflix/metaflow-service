import pytest
from unittest import mock
from .utils import (
    init_app, init_db, clean_db,
    add_flow, add_run, add_artifact,
    add_step, add_task, add_metadata,
    _test_list_resources, _test_single_resource
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

# NOTE: For Attempts which don’t have attempt_ok in metadata, utilize the value of attempt specific task_ok in s3

# Attempt is considered "Successful" when:
#   1. attempt_ok in task metadata for the attempt is set to True
#
# Sart time: created_at(ts_epoch) property for attempt attribute for the attempt in task metadata
# End time: created_at(ts_epoch) property for attempt_ok attribute for the attempt in task metadata


async def __test_attempt_status_completed(cli, db):
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

    _metadata_attempt = (await add_metadata(db,
                                            flow_id=_task.get("flow_id"),
                                            run_number=_task.get("run_number"),
                                            run_id=_task.get("run_id"),
                                            step_name=_task.get("step_name"),
                                            task_id=_task.get("task_id"),
                                            task_name=_task.get("task_name"),
                                            metadata={
                                                "field_name": "attempt",
                                                "value": "0",
                                                "type": "attempt"})).body

    _metadata_attempt_ok = (await add_metadata(db,
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

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task), 200)

    assert data["status"] == "completed"
    assert data["ts_epoch"] == _task["ts_epoch"]
    assert data["started_at"] == _metadata_attempt["ts_epoch"]  # metadata.attempt is present
    assert data["finished_at"] == _metadata_attempt_ok["ts_epoch"]


# Attempt is considered "Running" when all of the following apply:
#   1. Has a start time
#   2. attempt_ok does not exist in the task metadata
#   3. Has logged a heartbeat in the last x minutes
#   4. No subsequent attempt exists
#
# Sart time: created_at(ts_epoch) property for attempt attribute for the attempt in task metadata
# End time: Does not apply

async def __test_attempt_status_running(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="end", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task), 200)

    assert data["status"] == "running"
    assert data["ts_epoch"] == _task["ts_epoch"]
    assert data["started_at"] == None
    assert data["finished_at"] == None


async def __test_attempt_status_running_second_attempt(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="end", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task), 200)

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

    _metadata_attempt_ok = (await add_metadata(db,
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

    _artifact_second = (await add_artifact(
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
                            "attempt_id": 1
        })).body

    assert data["status"] == "running"
    assert data["ts_epoch"] == _task["ts_epoch"]
    assert data["started_at"] == None
    assert data["finished_at"] == None


# Attempt is considered "Failed" when any of the following apply:
#   1. attempt_ok in task metadata for the attempt is set to False
#   2. No heartbeat has been logged for the task in the last x minutes and no new attempt has started
#   3. A newer attempt exists
#
# Sart time: created_at(ts_epoch) property for attempt attribute for the attempt in task metadata
# End time: Either of (in priority)
#   1. created_at property for attempt_ok attribute for the attempt in task metadata
#   2. The timestamp in the heartbeat column for the task if no subsequent attempt is detected
#   3. If a subsequent attempt exists, use the start time of the subsequent attempt

async def test_attempt_status_failed_attempt_ok(cli, db):
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

    _metadata_attempt = (await add_metadata(db,
                                            flow_id=_task.get("flow_id"),
                                            run_number=_task.get("run_number"),
                                            run_id=_task.get("run_id"),
                                            step_name=_task.get("step_name"),
                                            task_id=_task.get("task_id"),
                                            task_name=_task.get("task_name"),
                                            metadata={
                                                "field_name": "attempt",
                                                "value": "0",
                                                "type": "attempt"})).body

    _metadata_attempt_ok = (await add_metadata(db,
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

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task), 200)

    assert data["status"] == "failed"
    assert data["ts_epoch"] == _task["ts_epoch"]
    assert data["started_at"] == _metadata_attempt["ts_epoch"]  # metadata.attempt is present
    assert data["finished_at"] == _metadata_attempt_ok["ts_epoch"]


async def test_attempt_status_failed_attempt_ok_s3(cli, db):
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

    _metadata_attempt = (await add_metadata(db,
                                            flow_id=_task.get("flow_id"),
                                            run_number=_task.get("run_number"),
                                            run_id=_task.get("run_id"),
                                            step_name=_task.get("step_name"),
                                            task_id=_task.get("task_id"),
                                            task_name=_task.get("task_name"),
                                            metadata={
                                                "field_name": "attempt",
                                                "value": "0",
                                                "type": "attempt"})).body

    async def _fetch_data(self, targets, event_stream=None, invalidate_cache=False):
        return {
            "{flow_id}/{run_number}/{step_name}/{task_id}/0".format(**_task): [True, {'_task_ok': False}]
        }

    with mock.patch(
        "services.ui_backend_service.data.refiner.task_refiner.Refinery.fetch_data",
        new=_fetch_data
    ):
        _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}?postprocess=true".format(**_task), 200)

        assert data["status"] == "failed"
        assert data["ts_epoch"] == _task["ts_epoch"]
        assert data["started_at"] == _metadata_attempt["ts_epoch"]  # metadata.attempt is present
        assert data["finished_at"] == _artifact["ts_epoch"]


async def test_attempt_status_failed_heartbeat(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="end", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"),
                            last_heartbeat_ts=1)).body

    _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task), 200)

    assert data["status"] == "failed"
    assert data["ts_epoch"] == _task["ts_epoch"]
    assert data["started_at"] == None
    assert data["finished_at"] == 1000  # last heartbeat in this case
