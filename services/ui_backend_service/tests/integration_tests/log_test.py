from ...api.log import get_metadata_log, get_metadata_log_assume_path
import pytest
import os
from .utils import (
    init_app, init_db, clean_db,
    add_flow, add_run, add_step, add_task, add_metadata,
    _test_single_resource, _test_list_resources, set_env
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


@pytest.fixture
async def ds_root_env():
    os.environ["MF_DATASTORE_ROOT"] = "s3://ds_base"
    yield
    del(os.environ["MF_DATASTORE_ROOT"])  # cleanup afterwards
# Fixtures end


async def test_list_logs_without_assume(db):

    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    _metadata_first = (await add_metadata(db,
                                          flow_id=_task.get("flow_id"),
                                          run_number=_task.get("run_number"),
                                          run_id=_task.get("run_id"),
                                          step_name=_task.get("step_name"),
                                          task_id=_task.get("task_id"),
                                          task_name=_task.get("task_name"),
                                          metadata={
                                              "field_name": "log_location_stdout",
                                              "value": '{"ds_type": "s3", "location": "s3://bucket/Flow/1/end/2/0.stdout.log", "attempt": 0}',
                                              "type": "log_path"})).body

    bucket, path, attempt_id = \
        await get_metadata_log(
            db.metadata_table_postgres.find_records,
            _task.get("flow_id"), _task.get("run_number"), _task.get("step_name"), _task.get("task_id"),
            0, "log_location_stdout")

    assert bucket == "bucket"
    assert path == "Flow/1/end/2/0.stdout.log"
    assert attempt_id == 0

    bucket, path, attempt_id = \
        await get_metadata_log(
            db.metadata_table_postgres.find_records,
            _task.get("flow_id"), _task.get("run_number"), _task.get("step_name"), _task.get("task_id"),
            1, "log_location_stdout")

    assert bucket == None
    assert path == None
    assert attempt_id == None


async def test_list_logs_assume_path(db):

    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    _metadata_first = (await add_metadata(db,
                                          flow_id=_task.get("flow_id"),
                                          run_number=_task.get("run_number"),
                                          run_id=_task.get("run_id"),
                                          step_name=_task.get("step_name"),
                                          task_id=_task.get("task_id"),
                                          task_name=_task.get("task_name"),
                                          metadata={
                                              "field_name": "log_location_stdout",
                                              "value": '{"ds_type": "s3", "location": "s3://bucket/Flow/1/end/2/0.stdout.log", "attempt": 0}',
                                              "type": "log_path"})).body

    bucket, path, attempt_id = \
        await get_metadata_log_assume_path(
            db.metadata_table_postgres.find_records,
            _task.get("flow_id"), _task.get("run_number"), _task.get("step_name"), _task.get("task_id"),
            0, "log_location_stdout")

    assert bucket == "bucket"
    assert path == "Flow/1/end/2/0.stdout.log"
    assert attempt_id == 0

    bucket, path, attempt_id = \
        await get_metadata_log_assume_path(
            db.metadata_table_postgres.find_records,
            _task.get("flow_id"), _task.get("run_number"), _task.get("step_name"), _task.get("task_id"),
            1, "log_location_stdout")

    assert bucket == "bucket"
    assert path == "Flow/1/end/2/1.stdout.log"
    assert attempt_id == 1


async def test_list_mflogs_assume_path_fail_with_missing_DS_ROOT(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"), run_id="mli_123")).body
    # workaround to fetch run_number, as add_run does not return it correctly.
    # TODO: fix add_run serialization
    _run = (await db.run_table_postgres.find_records(
        conditions=["flow_id = %s", "run_id=%s"],
        values=[_run.get("flow_id"), _run.get("run_number")],
        fetch_single=True,
        expanded=True
    )
    )[0].body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="regular_step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            run_number=_run.get("run_number"),
                            run_id=_run.get("run_id"),
                            step_name=_step.get("step_name"),
                            task_name="mli_456")).body

    _task = (await db.task_table_postgres.find_records(
        conditions=["task_name=%s"],
        values=[_task.get("task_id")],
        fetch_single=True,
        expanded=True
    )
    )[0].body

    # MFLOG lookup requires at least something to exist in the metadata table for a task.
    _metadata_first = (await add_metadata(db,
                                          flow_id=_task.get("flow_id"),
                                          run_number=_task.get("run_number"),
                                          run_id=_task.get("run_id"),
                                          step_name=_task.get("step_name"),
                                          task_id=_task.get("task_id"),
                                          task_name=_task.get("task_name"),
                                          metadata={
                                              "field_name": "attempt",
                                              "value": '0',
                                              "type": "attempt"})).body

    resp = await cli.get("/flows/{flow_id}/runs/{run_id}/steps/{step_name}/tasks/{task_name}/logs/out".format(**_task))
    body = await resp.json()

    assert resp.status == 500
    assert body['id'] == 'log-error'
    assert 'MF_DATASTORE_ROOT' in body['detail']

    # Check that choice of resource primary key does not affect return.
    resp2 = await cli.get("/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/logs/out".format(**_task))
    body2 = await resp2.json()

    assert body == body2

async def test_list_mflogs_assume_path_with_DS_ROOT(ds_root_env, cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"), run_id="mli_123")).body
    # workaround to fetch run_number, as add_run does not return it correctly.
    # TODO: fix add_run serialization
    _run = (await db.run_table_postgres.find_records(
        conditions=["flow_id = %s", "run_id=%s"],
        values=[_run.get("flow_id"), _run.get("run_number")],
        fetch_single=True,
        expanded=True
    )
    )[0].body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="regular_step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            run_number=_run.get("run_number"),
                            run_id=_run.get("run_id"),
                            step_name=_step.get("step_name"),
                            task_name="mli_456")).body

    _task = (await db.task_table_postgres.find_records(
        conditions=["task_name=%s"],
        values=[_task.get("task_id")],
        fetch_single=True,
        expanded=True
    )
    )[0].body

    # MFLOG lookup requires at least something to exist in the metadata table for a task.
    # Check that choice of resource primary key does not affect return.
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_id}/steps/{step_name}/tasks/{task_name}/logs/out".format(**_task), 404, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/logs/out".format(**_task), 404, [])

    _metadata_first = (await add_metadata(db,
                                          flow_id=_task.get("flow_id"),
                                          run_number=_task.get("run_number"),
                                          run_id=_task.get("run_id"),
                                          step_name=_task.get("step_name"),
                                          task_id=_task.get("task_id"),
                                          task_name=_task.get("task_name"),
                                          metadata={
                                              "field_name": "attempt",
                                              "value": '0',
                                              "type": "attempt"})).body

    # S3 client not configured so actual log fetch should fail
    # Check that choice of resource primary key does not affect return.
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_id}/steps/{step_name}/tasks/{task_name}/logs/out".format(**_task), 500, None)
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/logs/out".format(**_task), 500, None)
