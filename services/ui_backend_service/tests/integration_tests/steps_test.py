import pytest
from .utils import (
    init_app, init_db, clean_db,
    add_flow, add_run, add_step,
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


async def test_list_steps(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps".format(**_run), 200, [])

    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body

    await _test_list_resources(cli, db, "/flows/{flow_id}/runs/{run_number}/steps".format(**_step), 200, [_step])


async def test_single_step(cli, db):
    await _test_single_resource(cli, db, "/flows/HelloFlow/runs/404/steps/none", 404, {})

    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}".format(**_step), 200, _step)
