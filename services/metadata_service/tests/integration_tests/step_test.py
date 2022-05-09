import copy

from .utils import (
    cli, db,
    assert_api_get_response, assert_api_post_response, compare_partial,
    add_flow, add_run, add_step, update_objects_with_run_tags
)
import pytest

pytestmark = [pytest.mark.integration_tests]


async def test_step_post(cli, db):
    # create flow and run to add steps for.
    _flow = (await add_flow(db)).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body

    payload = {
        "user_name": "test_user",
        "tags": ["a_tag", "b_tag"],
        "system_tags": ["runtime:test"]
    }

    # Check all fields from payload match what we get back from POST,
    # except for tags, which should match run tags instead.
    def _check_response_body(body):
        payload_cp = copy.deepcopy(payload)
        payload_cp["tags"] = _run["tags"]
        payload_cp["system_tags"] = _run["system_tags"]
        compare_partial(body, payload_cp)

    _step = await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/test_step/step".format(**_run),
        payload=payload,
        status=200,  # why 200 instead of 201?
        expected_body_check_fn=_check_response_body
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

    # expect steps' tags to be overridden by tags of their ancestral run
    update_objects_with_run_tags('step', [_first_step, _second_step], _run)

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

    # expect step's tags to be overridden by tags of their ancestral run
    update_objects_with_run_tags('step', [_step], _run)

    # try to get created step
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}".format(**_step), data=_step)

    # non-existent flow, run, or step should return 404
    await assert_api_get_response(cli, "/flows/NonExistentFlow/runs/{run_number}/steps/{step_name}".format(**_step), status=404)
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/1234/steps/{step_name}".format(**_step), status=404)
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/nonexistent_step".format(**_step), status=404)
