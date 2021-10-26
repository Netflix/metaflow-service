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

# NOTE: For Tasks which don’t have heartbeat enabled, for heartbeat checks fall back on using the timestamp of the latest metadata entry for the task as a proxy and set x and Y to 2 weeks.
# NOTE: For Tasks which don’t have attempt_ok in metadata, utilize the value of attempt specific task_ok in s3

# Task is considered "Successful" when:
#   1. If the latest attempt is successful
#
# Sart time: created_at(ts_epoch) column value in the task table
# End time: ts_epoch for the successful attempt


async def __test_task_status_completed(cli, db):
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


# Task is considered "Running" when all of the following apply:
#   1. Has a running attempt or the task hasn’t failed
#
# Sart time: created_at(ts_epoch) column value in the task table
# End time: Does not apply


async def __test_task_status_running(cli, db):
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


# Task is considered "Failed" when all of the following apply:
#   1. Y minutes have expired after the End time for the task
#   2. the latest attempt has failed (if one exists)
#
# Sart time: created_at(ts_epoch) column value in the task table
# End time: The latest of
#   1. The timestamp in the heartbeat column for the task
#   2. End time of the latest attempt (if one exists)

async def test_task_status_failed_attempt_ok(cli, db):
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


async def test_task_status_failed_attempt_ok_s3(cli, db):
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


async def test_task_status_failed_attempt_ok_s3_artifact_ts_epoch(cli, db):
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
                            "attempt_id": 1
        })).body

    async def _fetch_data(self, targets, event_stream=None, invalidate_cache=False):
        return {
            "{flow_id}/{run_number}/{step_name}/{task_id}/1".format(**_task): [True, {'_task_ok': False}]
        }

    with mock.patch(
        "services.ui_backend_service.data.refiner.task_refiner.Refinery.fetch_data",
        new=_fetch_data
    ):
        _, data = await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}?postprocess=true".format(**_task), 200)

        assert data["status"] == "failed"
        assert data["ts_epoch"] == _task["ts_epoch"]
        assert data["started_at"] == None  # metadata.attempt not present
        assert data["finished_at"] == _artifact["ts_epoch"]
