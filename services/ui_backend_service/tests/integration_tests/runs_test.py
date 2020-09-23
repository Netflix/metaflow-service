import pytest
from .utils import (
    init_app, init_db, clean_db,
    add_flow, add_run,
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


async def test_list_runs(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body

    await _test_list_resources(cli, db, "/runs", 200, [])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs".format(**_flow), 200, [])

    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _run["status"] = "running"

    await _test_list_resources(cli, db, "/runs", 200, [_run])
    await _test_list_resources(cli, db, "/flows/{flow_id}/runs".format(**_flow), 200, [_run])


async def test_single_run(cli, db):
    await _test_single_resource(cli, db, "/flows/HelloFlow/runs/404", 404, {})

    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _run["status"] = "running"

    await _test_single_resource(cli, db, "/flows/{flow_id}/runs/{run_number}".format(**_run), 200, _run)
