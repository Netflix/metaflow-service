from ...api.log import get_metadata_log, get_metadata_log_assume_path
import pytest
from .utils import (
    init_app, init_db, clean_db,
    add_flow, add_run, add_step, add_task, add_metadata,
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
