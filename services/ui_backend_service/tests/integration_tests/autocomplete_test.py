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
    await add_flow(db, flow_id="HelloFlow")
    await add_flow(db, flow_id="HelloFlow2")
    await add_flow(db, flow_id="HelloFlow3")

    await _test_list_resources(cli, db, '/flows/autocomplete', 200, ['HelloFlow', 'HelloFlow2', 'HelloFlow3'])

    # Partial matches
    await _test_list_resources(cli, db, '/flows/autocomplete?flow_id:co=low2', 200, ['HelloFlow2'])

    # no-match
    await _test_list_resources(cli, db, '/flows/autocomplete?flow_id:co=test', 200, [])



async def test_runs_autocomplete(cli, db):
    await _test_list_resources(cli, db, '/flows/HelloFlow/runs/autocomplete', 200, [])
    await add_flow(db, flow_id="HelloFlow")
    await add_run(db, flow_id="HelloFlow", run_id="HelloRun")
    await add_run(db, flow_id="HelloFlow", run_id="HelloRun2")
    await add_run(db, flow_id="HelloFlow", run_id="HelloRun3")

    await _test_list_resources(cli, db, '/flows/HelloFlow/runs/autocomplete', 200, ['HelloRun', 'HelloRun2', 'HelloRun3'])

    # Partial matches
    await _test_list_resources(cli, db, '/flows/HelloFlow/runs/autocomplete?run:co=Run2', 200, ['HelloRun2'])

    # no-match
    await _test_list_resources(cli, db, '/flows/HelloFlow/runs/autocomplete?run:co=test', 200, [])


async def test_steps_autocomplete(cli, db):
    _flow = (await add_flow(db, flow_id="HelloFlow")).body
    _run = (await add_run(db, flow_id=_flow.get("flow_id"))).body
    _step = (await add_step(db, flow_id=_run.get("flow_id"), step_name="regular_step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))).body
    await add_step(db, flow_id=_run.get("flow_id"), step_name="another_step", run_number=_run.get("run_number"), run_id=_run.get("run_id"))
    await add_step(db, flow_id=_run.get("flow_id"), step_name="step3", run_number=_run.get("run_number"), run_id=_run.get("run_id"))

    await _test_list_resources(cli, db, '/flows/{flowid}/runs/{runid}/steps/autocomplete'.format(flowid=_step.get('flow_id'), runid=_step.get('run_number')), 200, ["another_step", "regular_step", "step3"])
    
    # Partial match
    await _test_list_resources(cli, db, '/flows/{flowid}/runs/{runid}/steps/autocomplete?step_name:co=lar'.format(flowid=_step.get('flow_id'), runid=_step.get('run_number')), 200, ["regular_step"])

    # no-match
    await _test_list_resources(cli, db, '/flows/{flowid}/runs/{runid}/steps/autocomplete?step_name:co=test'.format(flowid=_step.get('flow_id'), runid=_step.get('run_number')), 200, [])


async def test_tags_autocomplete(cli, db):
    await _test_list_resources(cli, db, '/tags/autocomplete', 200, [])
    await add_flow(db, flow_id="HelloFlow")
    await add_run(db, flow_id="HelloFlow", run_id="HelloRun", tags=["tag:something"])
    # Note that runtime:dev tags gets assigned automatically
    await _test_list_resources(cli, db, '/tags/autocomplete', 200, ['runtime:dev', 'tag:something'])
    
    # Partial match
    await _test_list_resources(cli, db, '/tags/autocomplete?tag:co=thing', 200, ['tag:something'])

    # no-match
    await _test_list_resources(cli, db, '/tags/autocomplete?tag:co=nothing', 200, [])

    # Custom match
    await _test_list_resources(cli, db, '/tags/autocomplete?tag:li=tag:%25thing', 200, ['tag:something'])
