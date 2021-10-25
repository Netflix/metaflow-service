from .utils import (
    init_app, init_db, clean_db,
    assert_api_get_response, assert_api_post_response, compare_partial,
    add_flow, add_run, add_step,
    add_task, add_artifact
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

# Shared Artifact test data
ARTIFACT_A = {
    "user_name": "test_user",
    "name": "artifact-A",
    "content_type": "text/plain",
    "location": "/test-location-a",
    "ds_type": "local",
    "sha": "1234abcd",
    "type": "test-artifact",
    "attempt_id": 0,
    "tags": ["a_tag", "b_tag"],
    "system_tags": ["runtime:test"]
}
ARTIFACT_B = {
    "user_name": "test_user",
    "name": "artifact-B",
    "content_type": "text/plain",
    "location": "/test-location-b",
    "ds_type": "local",
    "sha": "1234dcba",
    "type": "test-artifact",
    "attempt_id": 0,
    "tags": ["a_tag", "b_tag"],
    "system_tags": ["runtime:test"]
}


async def test_artifact_post(cli, db):
    # create flow, run, step and task to add artifacts for.
    _flow = (await add_flow(db)).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _step = (await add_step(db, flow_id=_run["flow_id"], run_number=_run["run_number"])).body
    _task = (await add_task(db, flow_id=_step["flow_id"], run_number=_step["run_number"], step_name=_step["step_name"])).body

    # artifacts
    _first_artifact = ARTIFACT_A
    _second_artifact = ARTIFACT_B
    payload = [_first_artifact, _second_artifact]

    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifact".format(**_task),
        payload=payload,
        status=200,
        expected_body={"artifacts_created": 2}  # api responds with only the number of artifacts created.
    )

    # Records should be found in DB
    _first_found = (await db.artifact_table_postgres.get_artifact(_task["flow_id"], _task["run_number"], _task["step_name"], _task["task_id"], _first_artifact["name"])).body
    _second_found = (await db.artifact_table_postgres.get_artifact(_task["flow_id"], _task["run_number"], _task["step_name"], _task["task_id"], _second_artifact["name"])).body

    compare_partial(_first_found, _first_artifact)
    compare_partial(_second_found, _second_artifact)

    # Posting the same artifacts twice should not add anything due to key constraints
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifact".format(**_task),
        payload=payload,
        status=200,  # NOTE: why 200 instead of an actual error when all of the inserts failed?
        expected_body={"artifacts_created": 0}  # NOTE: error gives no info on which inserts failed.
    )

    # Posting with an incremented attempt_id should succeed
    _first_artifact_second_attempt = dict(_first_artifact)
    _first_artifact_second_attempt["attempt_id"] = 1
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifact".format(**_task),
        payload=[_first_artifact_second_attempt],
        status=200,
        expected_body={"artifacts_created": 1}
    )

    # Posting on a non-existent flow_id should result in error
    await assert_api_post_response(
        cli,
        path="/flows/NonExistentFlow/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifact".format(**_task),
        payload=payload,
        status=400,
        expected_body={"message": "need to register run_id and task_id first"}
    )

    # posting on a non-existent run number should result in an error
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/1234/steps/{step_name}/tasks/{task_id}/artifact".format(**_task),
        payload=payload,
        status=400,
        expected_body={"message": "need to register run_id and task_id first"}
    )

    # posting on a non-existent step_name should result in an error
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/nonexistent/tasks/{task_id}/artifact".format(**_task),
        payload=payload,
        status=400,
        expected_body={"message": "need to register run_id and task_id first"}
    )

    # posting on a non-existent task_id should result in an error
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/1234/artifact".format(**_task),
        payload=payload,
        status=400,
        expected_body={"message": "need to register run_id and task_id first"}
    )


async def test_run_artifacts_get(cli, db):
    # create a flow, run, step and task for the test
    _flow = (await add_flow(db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _step = (await add_step(db, flow_id=_run["flow_id"], run_number=_run["run_number"], step_name="first_step")).body
    _task = (await add_task(db, flow_id=_step["flow_id"], run_number=_step["run_number"], step_name=_step["step_name"])).body

    # add artifacts to the task
    _first_artifact = (await add_artifact(db, flow_id=_task["flow_id"], run_number=_task["run_number"], step_name=_task["step_name"], task_id=_task["task_id"], artifact=ARTIFACT_A)).body
    _second_artifact = (await add_artifact(db, flow_id=_task["flow_id"], run_number=_task["run_number"], step_name=_task["step_name"], task_id=_task["task_id"], artifact=ARTIFACT_B)).body

    # try to get all the created artifacts
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/artifacts".format(**_task), data=[_first_artifact, _second_artifact])

    # getting artifacts for non-existent flow should return empty list
    await assert_api_get_response(cli, "/flows/NonExistentFlow/runs/{run_number}/artifacts".format(**_task), status=200, data=[])

    # getting artifacts for non-existent run should return empty list
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/1234/artifacts".format(**_task), status=200, data=[])


async def test_step_artifacts_get(cli, db):
    # create a flow, run, step and task for the test
    _flow = (await add_flow(db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _step = (await add_step(db, flow_id=_run["flow_id"], run_number=_run["run_number"], step_name="first_step")).body
    _task = (await add_task(db, flow_id=_step["flow_id"], run_number=_step["run_number"], step_name=_step["step_name"])).body

    # add artifacts to the task
    _first_artifact = (await add_artifact(db, flow_id=_task["flow_id"], run_number=_task["run_number"], step_name=_task["step_name"], task_id=_task["task_id"], artifact=ARTIFACT_A)).body
    _second_artifact = (await add_artifact(db, flow_id=_task["flow_id"], run_number=_task["run_number"], step_name=_task["step_name"], task_id=_task["task_id"], artifact=ARTIFACT_B)).body

    # try to get all the created artifacts
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/artifacts".format(**_task), data=[_first_artifact, _second_artifact])

    # getting artifacts for non-existent flow should return empty list
    await assert_api_get_response(cli, "/flows/NonExistentFlow/runs/{run_number}/steps/{step_name}/artifacts".format(**_task), status=200, data=[])

    # getting artifacts for non-existent run should return empty list
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/1234/steps/{step_name}/artifacts".format(**_task), status=200, data=[])

    # getting artifacts for non-existent step should return empty list
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/nonexistent/artifacts".format(**_task), status=200, data=[])


async def test_task_artifacts_get(cli, db):
    # create a flow, run, step and task for the test
    _flow = (await add_flow(db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _step = (await add_step(db, flow_id=_run["flow_id"], run_number=_run["run_number"], step_name="first_step")).body
    _task = (await add_task(db, flow_id=_step["flow_id"], run_number=_step["run_number"], step_name=_step["step_name"])).body

    # add artifacts to the task
    _first_artifact = (await add_artifact(db, flow_id=_task["flow_id"], run_number=_task["run_number"], step_name=_task["step_name"], task_id=_task["task_id"], artifact=ARTIFACT_A)).body
    _second_artifact = (await add_artifact(db, flow_id=_task["flow_id"], run_number=_task["run_number"], step_name=_task["step_name"], task_id=_task["task_id"], artifact=ARTIFACT_B)).body

    # try to get all the created artifacts
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts".format(**_task), data=[_second_artifact, _first_artifact])

    # getting artifacts for non-existent flow should return empty list
    await assert_api_get_response(cli, "/flows/NonExistentFlow/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts".format(**_task), status=200, data=[])

    # getting artifacts for non-existent run should return empty list
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/1234/steps/{step_name}/tasks/{task_id}/artifacts".format(**_task), status=200, data=[])

    # getting artifacts for non-existent step should return empty list
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/nonexistent/tasks/{task_id}/artifacts".format(**_task), status=200, data=[])

    # getting artifacts for non-existent task should return empty list
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/1234/artifacts".format(**_task), status=200, data=[])


async def test_task_get(cli, db):
    # create flow, run, step and task for test
    _flow = (await add_flow(db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _step = (await add_step(db, flow_id=_run["flow_id"], run_number=_run["run_number"], step_name="first_step")).body
    _task = (await add_task(db, flow_id=_step["flow_id"], run_number=_step["run_number"], step_name=_step["step_name"])).body

    # add artifact to task for testing
    _artifact = (await add_artifact(db, flow_id=_task["flow_id"], run_number=_task["run_number"], step_name=_task["step_name"], task_id=_task["task_id"], artifact=ARTIFACT_A)).body

    # try to get created artifact
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts/{name}".format(**_artifact), data=_artifact)

    # non-existent flow, run, step, task or name should return 404
    await assert_api_get_response(cli, "/flows/NonExistentFlow/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts/{name}".format(**_artifact), status=404)
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/1234/steps/{step_name}/tasks/{task_id}/artifacts/{name}".format(**_artifact), status=404)
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/nonexistent_step/tasks/{task_id}/artifacts/{name}".format(**_artifact), status=404)
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/1234/artifacts/{name}".format(**_artifact), status=404)
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts/noname".format(**_artifact), status=404)
