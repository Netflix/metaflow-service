import pytest
from .utils import (
    init_app, init_db, add_flow, clean_db, _test_list_resources, add_run, add_step
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


async def test_flows_autocomplete(cli, db):
    await _test_list_resources(cli, db, '/flows/autocomplete', 200, [])
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    await _test_list_resources(cli, db, '/flows/autocomplete', 200, ['HelloFlow'])


async def test_runs_autocomplete(cli, db):
    await _test_list_resources(cli, db, '/flows/HelloFlow/runs/autocomplete', 200, [])
    await add_flow(db, flow_id="HelloFlow")
    _run = (await add_run(db, flow_id="HelloFlow", run_id="HelloRun")).body
    await _test_list_resources(cli, db, '/flows/HelloFlow/runs/autocomplete', 200, ['HelloRun'])


async def test_steps_autocomplete(cli, db):
    await _test_list_resources(cli, db, '/flows/HelloFlow/runs/HelloRun/steps/autocomplete', 200, [])
    await add_flow(db, flow_id="HelloFlow")
    await add_run(db, flow_id="HelloFlow", run_id="HelloRun")
    _step = (await add_step(db, flow_id="HelloFlow", run_id="HelloRun", step_name="HelloStep")).body
    await _test_list_resources(cli, db, '/flows/HelloFlow/runs/HelloRun/steps/autocomplete', 200, ['HelloStep'])
