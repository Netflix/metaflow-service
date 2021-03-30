from .utils import (
    init_app, init_db, clean_db,
    assert_api_get_response, assert_api_post_response, compare_partial,
    add_flow, add_run, add_step
)
import pytest
import json
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


async def test_step_post(cli, db):
    # create flow and run to add steps for.
    _flow = (await add_flow(db)).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body

    payload = {
        "user_name": "test_user",
        "tags": ["a_tag", "b_tag"],
        "system_tags": ["runtime:test"]
    }
    _step = await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/test_step/step".format(**_run),
        payload=payload,
        status=200  # why 200 instead of 201?
    )

    # Record should be found in DB
    _found = (await db.step_table_postgres.get_step(_step["flow_id"], _step["run_number"], _step["step_name"])).body

    compare_partial(_found, {"step_name": "test_step", **payload})
    
    # Duplicate step names should not be accepted for a run
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/test_step/step".format(**_run),
        payload=payload,
        status=409
    )

    # Posting on a non-existent flow_id should result in error
    await assert_api_post_response(
        cli,
        path="/flows/NonExistentFlow/runs/{run_number}/steps/test_step/step".format(**_run),
        payload=payload,
        status=500
    )

    # posting on a non-existent run number should result in an error
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/1234/steps/test_step/step".format(**_run),
        payload=payload,
        status=500
    )


async def test_steps_get(cli, db):
    # create a flow and run for the test
    _flow = (await add_flow(db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body

    # add steps to the run
    _first_step = (await add_step(db, flow_id=_run["flow_id"], run_number=_run["run_number"], step_name="first_step")).body
    _second_step = (await add_step(db, flow_id=_run["flow_id"], run_number=_run["run_number"], step_name="second_step")).body

    # try to get all the created steps
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps".format(**_first_step), data=[_first_step, _second_step])

    # getting steps for non-existent flow should return empty list
    await assert_api_get_response(cli, "/flows/NonExistentFlow/runs/{run_number}/steps".format(**_first_step), status=200, data=[])

    # getting steps for non-existent run should return empty list
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/1234/steps".format(**_first_step), status=200, data=[])


async def test_step_get(cli, db):
    # create flow for test
    _flow = (await add_flow(db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body

    # add step to run for testing
    _step = (await add_step(db, flow_id=_run["flow_id"], run_number=_run["run_number"], step_name="first_step")).body

    # try to get created step
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}".format(**_step), data=_step)

    # non-existent flow, run, or step should return 404
    await assert_api_get_response(cli, "/flows/NonExistentFlow/runs/{run_number}/steps/{step_name}".format(**_step), status=404)
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/1234/steps/{step_name}".format(**_step), status=404)
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/nonexistent_step".format(**_step), status=404)
