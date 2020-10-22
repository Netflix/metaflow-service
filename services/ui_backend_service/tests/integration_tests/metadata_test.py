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


async def test_list_metadata(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/metadata".format(**_task), 200, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(**_task), 200, [])

    _metadata = (await add_metadata(db,
                                    flow_id=_task.get("flow_id"),
                                    run_number=_task.get("run_number"),
                                    run_id=_task.get("run_id"),
                                    step_name=_task.get("step_name"),
                                    task_id=_task.get("task_id"),
                                    task_name=_task.get("task_name"),
                                    metadata={
                                        "field_name": "field_name",
                                        "value": "value",
                                        "type": "type"})).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/metadata".format(**_task), 200, [_metadata])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(**_metadata), 200, [_metadata])


async def test_list_metadata_field_names(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/metadata".format(**_task), 200, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(**_task), 200, [])

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

    _metadata_second = (await add_metadata(db,
                                           flow_id=_task.get("flow_id"),
                                           run_number=_task.get("run_number"),
                                           run_id=_task.get("run_id"),
                                           step_name=_task.get("step_name"),
                                           task_id=_task.get("task_id"),
                                           task_name=_task.get("task_name"),
                                           metadata={
                                               "field_name": "log_location_stdout",
                                               "value": '{"ds_type": "s3", "location": "s3://bucket/Flow/1/end/2/1.stdout.log", "attempt": 1}',
                                               "type": "log_path"})).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(**_task), 200, [_metadata_second, _metadata_first])
