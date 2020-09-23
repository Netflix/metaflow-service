import pytest
from .utils import (
    init_app, init_db, clean_db,
    add_flow, add_run, add_step, add_task,
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


async def test_list_tasks(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/tasks".format(**_step), 200, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks".format(**_step), 200, [])

    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/tasks".format(**_task), 200, [_task])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks".format(**_task), 200, [_task])


async def test_single_task(cli, db):
    await _test_single_resource(cli, db, "/flows/HelloFlow/runs/404/steps/none/tasks/5", 404, {})

    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    _task = (await add_task(db,
                            flow_id=_step.get("flow_id"),
                            step_name=_step.get("step_name"),
                            run_number=_step.get("run_number"),
                            run_id=_step.get("run_id"))).body

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task), 200, _task)
