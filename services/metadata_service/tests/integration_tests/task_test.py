import copy

from .utils import (
    cli, db,
    assert_api_get_response, assert_api_post_response, compare_partial,
    add_flow, add_run, add_step, add_task, update_objects_with_run_tags
)
import pytest

pytestmark = [pytest.mark.integration_tests]


async def test_task_post(cli, db):
    # create flow, run and step to add tasks for.
    _flow = (await add_flow(db)).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _step = (await add_step(db, flow_id=_run["flow_id"], run_number=_run["run_number"])).body

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

    _task = await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/{step_name}/task".format(**_step),
        payload=payload,
        status=200,  # why 200 instead of 201?
        check_fn=_check_response_body
    )

    # Record should be found in DB
    _found = (await db.task_table_postgres.get_task(_task["flow_id"], _task["run_number"], _task["step_name"], _task["task_id"])).body

    compare_partial(_found, payload)

    # Posting on a non-existent flow_id should result in error
    await assert_api_post_response(
        cli,
        path="/flows/NonExistentFlow/runs/{run_number}/steps/{step_name}/task".format(**_task),
        payload=payload,
        status=500
    )

    # posting on a non-existent run number should result in an error
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/1234/steps/{step_name}/task".format(**_task),
        payload=payload,
        status=500
    )

    # posting on a non-existent step_name should result in a 404 due to foreign key constraint
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/nonexistent/task".format(**_task),
        payload=payload,
        status=404
    )


async def test_task_post_has_initial_heartbeat_with_supported_version(cli, db):
    # create flow, run and step to add tasks for.
    _flow = (await add_flow(db)).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    # client version not passed so run hb should be empty
    assert _run["last_heartbeat_ts"] is None

    _step = (await add_step(db, flow_id=_run["flow_id"], run_number=_run["run_number"])).body

    _task = await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/{step_name}/task".format(**_step),
        payload={
            "user_name": "test_user",
            "tags": ["a_tag", "b_tag"],
            "system_tags": ["runtime:test", "metaflow_version:2.0.5"]
        },
        status=200  # why 200 instead of 201?
    )

    # tasks should not have a heartbeat when it is created
    # with a known version that does not support heartbeats.
    assert _task['last_heartbeat_ts'] is None

    _task = await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/{step_name}/task".format(**_step),
        payload={
            "user_name": "test_user",
            "tags": ["a_tag", "b_tag"],
            "system_tags": ["runtime:test", "metaflow_version:2.2.12"]
        },
        status=200  # why 200 instead of 201?
    )

    # tasks should have a heartbeat when it is created
    # with a heartbeat-enabled client.
    assert _task['last_heartbeat_ts'] is not None

    # Run heartbeat should have been updated as well
    _found = (await db.run_table_postgres.get_run(_run["flow_id"], _run["run_number"])).body
    assert _found['last_heartbeat_ts'] is not None


async def test_task_heartbeat_post(cli, db):
    # create flow, run and step to add tasks for.
    _flow = (await add_flow(db)).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _step = (await add_step(db, flow_id=_run["flow_id"], run_number=_run["run_number"])).body

    # create task to update heartbeat on.
    _task = (await add_task(db, flow_id=_step["flow_id"], run_number=_step["run_number"], step_name=_step["step_name"])).body
    assert _task["last_heartbeat_ts"] == None

    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/heartbeat".format(**_task),
        status=200  # why 200 instead of 201?
    )

    # Record should be found in DB
    _found = (await db.task_table_postgres.get_task(_task["flow_id"], _task["run_number"], _task["step_name"], _task["task_id"])).body

    assert _found["last_heartbeat_ts"] is not None

    # should get 404 for non-existent flow, run, step and task
    await assert_api_post_response(
        cli,
        path="/flows/NonExistentFlow/runs/{run_number}/steps/{step_name}/tasks/{task_id}/heartbeat".format(**_task),
        status=404
    )

    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/1234/steps/{step_name}/tasks/{task_id}/heartbeat".format(**_task),
        status=404
    )

    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/nonexistent/tasks/{task_id}/heartbeat".format(**_task),
        status=404
    )

    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/1234/heartbeat".format(**_task),
        status=404
    )


async def test_tasks_get(cli, db):
    # create a flow, run and step for the test
    _flow = (await add_flow(db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _step = (await add_step(db, flow_id=_run["flow_id"], run_number=_run["run_number"], step_name="first_step")).body

    # add tasks to the step
    _first_task = (await add_task(db, flow_id=_step["flow_id"], run_number=_step["run_number"], step_name=_step["step_name"])).body
    _second_task = (await add_task(db, flow_id=_step["flow_id"], run_number=_step["run_number"], step_name=_step["step_name"])).body

    # expect tasks' tags to be overridden by tags of their ancestral run
    update_objects_with_run_tags('task', [_first_task, _second_task], _run)

    # try to get all the created tasks
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks".format(**_first_task),
                                  data=[_second_task, _first_task], data_is_unordered_list_of_dicts=True)

    # getting tasks for non-existent flow should return empty list
    await assert_api_get_response(cli, "/flows/NonExistentFlow/runs/{run_number}/steps/{step_name}/tasks".format(**_first_task), status=200, data=[])

    # getting tasks for non-existent run should return empty list
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/1234/steps/{step_name}/tasks".format(**_first_task), status=200, data=[])

    # getting tasks for non-existent step should return empty list
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/nonexistent/tasks".format(**_first_task), status=200, data=[])


async def test_task_get(cli, db):
    # create flow, run and step for test
    _flow = (await add_flow(db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body  # set run-level tags
    _step = (await add_step(db, flow_id=_run["flow_id"], run_number=_run["run_number"], step_name="first_step")).body

    # add task to run for testing
    _task = (await add_task(db, flow_id=_step["flow_id"], run_number=_step["run_number"], step_name=_step["step_name"])).body

    # expect task's tags to be overridden by tags of their ancestral run
    update_objects_with_run_tags('task', [_task], _run)

    # try to get created task
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task), data=_task)

    # non-existent flow, run, step, or task should return 404
    await assert_api_get_response(cli, "/flows/NonExistentFlow/runs/{run_number}/steps/{step_name}/tasks/{task_id}".format(**_task), status=404)
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/1234/steps/{step_name}/tasks/{task_id}".format(**_task), status=404)
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/nonexistent_step/tasks/{task_id}".format(**_task), status=404)
    await assert_api_get_response(cli, "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/1234".format(**_task), status=404)
