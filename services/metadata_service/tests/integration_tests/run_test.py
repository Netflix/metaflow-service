from .utils import (
    init_app, init_db, clean_db,
    assert_api_get_response, assert_api_post_response, compare_partial,
    add_flow, add_run
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


async def test_run_post(cli, db):
    # create flow to add runs for.
    _flow = (await add_flow(db)).body

    payload = {
        "user_name": "test_user",
        "tags": ["a_tag", "b_tag"],
        "system_tags": ["runtime:test"]
    }
    _run = await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/run".format(**_flow),
        payload=payload,
        status=200  # why 200 instead of 201?
    )

    # Record should be found in DB
    _found = (await db.run_table_postgres.get_run(_run["flow_id"], _run["run_number"])).body

    compare_partial(_found, payload)

    # Posting on a non-existent flow_id should result in error
    await assert_api_post_response(
        cli,
        path="/flows/NonExistentFlow/run",
        payload=payload,
        status=404
    )


async def test_run_heartbeat_post(cli, db):
    # create flow to add runs for.
    _flow = (await add_flow(db)).body
    # create run to update heartbeat on.
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    assert _run["last_heartbeat_ts"] == None

    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/heartbeat".format(**_run),
        status=200   # why 200 instead of 201?
    )

    # Record should be found in DB
    _found = (await db.run_table_postgres.get_run(_run["flow_id"], _run["run_number"])).body

    assert _found["last_heartbeat_ts"] is not None

    # should get 404 for non-existent run
    await assert_api_post_response(
        cli,
        path="/flows/NonExistentFlow/runs/{run_number}/heartbeat".format(**_run),
        status=404
    )

    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/1234/heartbeat".format(**_run),
        status=404
    )


async def test_runs_get(cli, db):
    # create a flow for the test
    _flow = (await add_flow(db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])).body

    # add runs to the flow
    _first_run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _second_run = (await add_run(db, flow_id=_flow["flow_id"])).body

    # try to get all the created runs
    await assert_api_get_response(cli, "/flows/{flow_id}/runs".format(**_first_run), data=[_second_run, _first_run])

    # getting runs for non-existent flow should return empty list
    await assert_api_get_response(cli, "/flows/NonExistentFlow/runs", status=200, data=[])


async def test_run_get(cli, db):
    # create flow for test
    _flow = (await add_flow(db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])).body

    # add run to flow for testing
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body

    # try to get created flow
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}".format(**_run), data=_run)

    # non-existent flow or run should return 404
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/1234".format(**_run), status=404)
    await assert_api_get_response(cli, "/flows/NonExistentFlow/runs/{run_number}".format(**_run), status=404)
