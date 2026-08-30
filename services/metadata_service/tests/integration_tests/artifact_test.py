from .utils import (
    cli,
    db,
    assert_api_get_response,
    assert_api_post_response,
    compare_partial,
    add_flow,
    add_run,
    add_step,
    add_task,
    add_artifact,
    update_objects_with_run_tags,
    assert_paginated_api_get_response,
)
import asyncio

import pytest

pytestmark = [pytest.mark.integration_tests]


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
    "system_tags": ["runtime:test"],
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
    "system_tags": ["runtime:test"],
}
ARTIFACT_C = {
    "user_name": "test_user",
    "name": "artifact-C",
    "content_type": "text/plain",
    "location": "/test-location-c",
    "ds_type": "local",
    "sha": "1234efgh",
    "type": "test-artifact",
    "attempt_id": 0,
    "tags": ["a_tag", "b_tag"],
    "system_tags": ["runtime:test"],
}


def artifact_for_attempt(artifact: dict, attempt_id: int) -> dict:
    "Copy of an artifact definition for a specific attempt, with a distinguishable location."
    return dict(
        artifact,
        attempt_id=attempt_id,
        location="{location}-attempt-{attempt_id}".format(
            location=artifact["location"], attempt_id=attempt_id
        ),
    )


async def add_artifacts_for_attempts(db, attempt_ids=(0, 1)):
    """
    Create a flow, run, step and task, and write ARTIFACT_A, ARTIFACT_B and ARTIFACT_C
    for every attempt in attempt_ids.

    Returns a (task, artifacts_by_attempt) tuple, where artifacts_by_attempt maps an
    attempt_id to the list of created artifacts, in creation order, with their tags
    already replaced by the ancestral run's tags (which is what the read endpoints return).
    """
    _flow = (
        await add_flow(
            db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"]
        )
    ).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _step = (
        await add_step(
            db,
            flow_id=_run["flow_id"],
            run_number=_run["run_number"],
            step_name="first_step",
        )
    ).body
    _task = (
        await add_task(
            db,
            flow_id=_step["flow_id"],
            run_number=_step["run_number"],
            step_name=_step["step_name"],
        )
    ).body

    artifacts_by_attempt = {}
    for index, attempt_id in enumerate(attempt_ids):
        if index > 0:
            # The paginated queries resolve the latest attempt of an artifact by ts_epoch,
            # which only has millisecond resolution. Keep the attempts apart in time so that
            # the newer attempt always wins.
            await asyncio.sleep(0.01)

        _artifacts = [
            (
                await add_artifact(
                    db,
                    flow_id=_task["flow_id"],
                    run_number=_task["run_number"],
                    step_name=_task["step_name"],
                    task_id=_task["task_id"],
                    artifact=artifact_for_attempt(artifact, attempt_id),
                )
            ).body
            for artifact in [ARTIFACT_A, ARTIFACT_B, ARTIFACT_C]
        ]
        # expect artifacts' tags to be overridden by tags of their ancestral run
        update_objects_with_run_tags("artifact", _artifacts, _run)
        artifacts_by_attempt[attempt_id] = _artifacts

    return _task, artifacts_by_attempt


# Artifact listing endpoints, in the order (run, step, task) of decreasing scope.
ARTIFACT_LIST_PATHS = [
    "/flows/{flow_id}/runs/{run_number}/artifacts",
    "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/artifacts",
    "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts",
]

# The only listing endpoint that is scoped to a single attempt.
ARTIFACT_ATTEMPT_LIST_PATH = (
    "/flows/{flow_id}/runs/{run_number}/steps/{step_name}"
    "/tasks/{task_id}/attempt/{attempt_id}/artifacts"
)


async def test_artifact_post(cli, db):
    # create flow, run, step and task to add artifacts for.
    _flow = (await add_flow(db)).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _step = (
        await add_step(db, flow_id=_run["flow_id"], run_number=_run["run_number"])
    ).body
    _task = (
        await add_task(
            db,
            flow_id=_step["flow_id"],
            run_number=_step["run_number"],
            step_name=_step["step_name"],
        )
    ).body

    # artifacts
    _first_artifact = ARTIFACT_A
    _second_artifact = ARTIFACT_B
    payload = [_first_artifact, _second_artifact]

    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifact".format(
            **_task
        ),
        payload=payload,
        status=200,
        expected_body={
            "artifacts_created": 2
        },  # api responds with only the number of artifacts created.
    )

    # Records should be found in DB
    _first_found = (
        await db.artifact_table_postgres.get_artifact(
            _task["flow_id"],
            _task["run_number"],
            _task["step_name"],
            _task["task_id"],
            _first_artifact["name"],
        )
    ).body
    _second_found = (
        await db.artifact_table_postgres.get_artifact(
            _task["flow_id"],
            _task["run_number"],
            _task["step_name"],
            _task["task_id"],
            _second_artifact["name"],
        )
    ).body

    compare_partial(_first_found, _first_artifact)
    compare_partial(_second_found, _second_artifact)

    # Posting the same artifacts twice should not add anything due to key constraints
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifact".format(
            **_task
        ),
        payload=payload,
        status=200,  # NOTE: why 200 instead of an actual error when all of the inserts failed?
        expected_body={
            "artifacts_created": 0
        },  # NOTE: error gives no info on which inserts failed.
    )

    # Posting with an incremented attempt_id should succeed
    _first_artifact_second_attempt = dict(_first_artifact)
    _first_artifact_second_attempt["attempt_id"] = 1
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifact".format(
            **_task
        ),
        payload=[_first_artifact_second_attempt],
        status=200,
        expected_body={"artifacts_created": 1},
    )

    # Posting on a non-existent flow_id should result in error
    await assert_api_post_response(
        cli,
        path="/flows/NonExistentFlow/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifact".format(
            **_task
        ),
        payload=payload,
        status=400,
        expected_body={"message": "need to register run_id and task_id first"},
    )

    # posting on a non-existent run number should result in an error
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/1234/steps/{step_name}/tasks/{task_id}/artifact".format(
            **_task
        ),
        payload=payload,
        status=400,
        expected_body={"message": "need to register run_id and task_id first"},
    )

    # posting on a non-existent step_name should result in an error
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/nonexistent/tasks/{task_id}/artifact".format(
            **_task
        ),
        payload=payload,
        status=400,
        expected_body={"message": "need to register run_id and task_id first"},
    )

    # posting on a non-existent task_id should result in an error
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/1234/artifact".format(
            **_task
        ),
        payload=payload,
        status=400,
        expected_body={"message": "need to register run_id and task_id first"},
    )


async def test_run_artifacts_get(cli, db):
    # create a flow, run, step and task for the test
    _flow = (
        await add_flow(
            db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"]
        )
    ).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _step = (
        await add_step(
            db,
            flow_id=_run["flow_id"],
            run_number=_run["run_number"],
            step_name="first_step",
        )
    ).body
    _task = (
        await add_task(
            db,
            flow_id=_step["flow_id"],
            run_number=_step["run_number"],
            step_name=_step["step_name"],
        )
    ).body

    # add artifacts to the task
    _first_artifact = (
        await add_artifact(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            artifact=ARTIFACT_A,
        )
    ).body
    _second_artifact = (
        await add_artifact(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            artifact=ARTIFACT_B,
        )
    ).body

    # expect artifacts' tags to be overridden by tags of their ancestral run
    update_objects_with_run_tags("artifact", [_first_artifact, _second_artifact], _run)

    # try to get all the created artifacts
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/artifacts".format(**_task),
        data=[_first_artifact, _second_artifact],
        data_is_unordered_list_of_dicts=True,
    )

    # getting artifacts for non-existent flow should return empty list
    await assert_api_get_response(
        cli,
        "/flows/NonExistentFlow/runs/{run_number}/artifacts".format(**_task),
        status=200,
        data=[],
    )

    # getting artifacts for non-existent run should return empty list
    await assert_api_get_response(
        cli, "/flows/{flow_id}/runs/1234/artifacts".format(**_task), status=200, data=[]
    )


async def test_run_artifacts_pagination_get(cli, db):
    # create a flow, run, step and task for the test
    _flow = (
        await add_flow(
            db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"]
        )
    ).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _step = (
        await add_step(
            db,
            flow_id=_run["flow_id"],
            run_number=_run["run_number"],
            step_name="first_step",
        )
    ).body
    _task = (
        await add_task(
            db,
            flow_id=_step["flow_id"],
            run_number=_step["run_number"],
            step_name=_step["step_name"],
        )
    ).body

    # add artifacts to the task
    _first_artifact = (
        await add_artifact(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            artifact=ARTIFACT_A,
        )
    ).body
    _second_artifact = (
        await add_artifact(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            artifact=ARTIFACT_B,
        )
    ).body
    _third_artifact = (
        await add_artifact(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            artifact=ARTIFACT_C,
        )
    ).body

    # expect artifacts' tags to be overridden by tags of their ancestral run
    update_objects_with_run_tags(
        "artifact", [_first_artifact, _second_artifact, _third_artifact], _run
    )

    # first page
    next_cursor = await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/artifacts".format(**_task),
        data=[_third_artifact, _second_artifact],
        params={"_limit": 2},
    )

    # continue with cursor
    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/artifacts".format(**_task),
        data=[_first_artifact],
        params={"_limit": 2, "_cursor": next_cursor},
        status=200,
        has_next_cursor=False,
    )

    # invalid cursor
    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/artifacts".format(**_task),
        params={"_cursor": "garbage1234"},
        status=400,
    )

    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/artifacts".format(**_task),
        data=[_third_artifact, _second_artifact, _first_artifact],
        params={"_limit": 1000},
        has_next_cursor=False,
    )
    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/artifacts".format(**_task),
        data=[_third_artifact, _second_artifact, _first_artifact],
        params={"_limit": 3},
        has_next_cursor=False,
    )


async def test_step_artifacts_get(cli, db):
    # create a flow, run, step and task for the test
    _flow = (
        await add_flow(
            db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"]
        )
    ).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _step = (
        await add_step(
            db,
            flow_id=_run["flow_id"],
            run_number=_run["run_number"],
            step_name="first_step",
        )
    ).body
    _task = (
        await add_task(
            db,
            flow_id=_step["flow_id"],
            run_number=_step["run_number"],
            step_name=_step["step_name"],
        )
    ).body

    # add artifacts to the task
    _first_artifact = (
        await add_artifact(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            artifact=ARTIFACT_A,
        )
    ).body
    _second_artifact = (
        await add_artifact(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            artifact=ARTIFACT_B,
        )
    ).body

    # expect artifacts' tags to be overridden by tags of their ancestral run
    update_objects_with_run_tags("artifact", [_first_artifact, _second_artifact], _run)

    # try to get all the created artifacts
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/artifacts".format(
            **_task
        ),
        data=[_first_artifact, _second_artifact],
        data_is_unordered_list_of_dicts=True,
    )

    # getting artifacts for non-existent flow should return empty list
    await assert_api_get_response(
        cli,
        "/flows/NonExistentFlow/runs/{run_number}/steps/{step_name}/artifacts".format(
            **_task
        ),
        status=200,
        data=[],
    )

    # getting artifacts for non-existent run should return empty list
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/1234/steps/{step_name}/artifacts".format(**_task),
        status=200,
        data=[],
    )

    # getting artifacts for non-existent step should return empty list
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/nonexistent/artifacts".format(
            **_task
        ),
        status=200,
        data=[],
    )


async def test_step_artifacts_pagination_get(cli, db):
    # create a flow, run, step and task for the test
    _flow = (
        await add_flow(
            db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"]
        )
    ).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _step = (
        await add_step(
            db,
            flow_id=_run["flow_id"],
            run_number=_run["run_number"],
            step_name="first_step",
        )
    ).body
    _task = (
        await add_task(
            db,
            flow_id=_step["flow_id"],
            run_number=_step["run_number"],
            step_name=_step["step_name"],
        )
    ).body

    # add artifacts to the task
    _first_artifact = (
        await add_artifact(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            artifact=ARTIFACT_A,
        )
    ).body
    _second_artifact = (
        await add_artifact(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            artifact=ARTIFACT_B,
        )
    ).body
    _third_artifact = (
        await add_artifact(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            artifact=ARTIFACT_C,
        )
    ).body

    # expect artifacts' tags to be overridden by tags of their ancestral run
    update_objects_with_run_tags(
        "artifact", [_first_artifact, _second_artifact, _third_artifact], _run
    )

    # first page
    next_cursor = await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/artifacts".format(
            **_task
        ),
        data=[_third_artifact, _second_artifact],
        params={"_limit": 2},
    )

    # continue with cursor
    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/artifacts".format(
            **_task
        ),
        data=[_first_artifact],
        params={"_limit": 2, "_cursor": next_cursor},
        status=200,
        has_next_cursor=False,
    )

    # invalid cursor
    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/artifacts".format(
            **_task
        ),
        params={"_cursor": "garbage1234"},
        status=400,
    )

    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/artifacts".format(
            **_task
        ),
        data=[_third_artifact, _second_artifact, _first_artifact],
        params={"_limit": 1000},
        has_next_cursor=False,
    )
    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/artifacts".format(
            **_task
        ),
        data=[_third_artifact, _second_artifact, _first_artifact],
        params={"_limit": 3},
        has_next_cursor=False,
    )


async def test_task_artifacts_get(cli, db):
    # create a flow, run, step and task for the test
    _flow = (
        await add_flow(
            db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"]
        )
    ).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _step = (
        await add_step(
            db,
            flow_id=_run["flow_id"],
            run_number=_run["run_number"],
            step_name="first_step",
        )
    ).body
    _task = (
        await add_task(
            db,
            flow_id=_step["flow_id"],
            run_number=_step["run_number"],
            step_name=_step["step_name"],
        )
    ).body

    # add artifacts to the task
    _first_artifact = (
        await add_artifact(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            artifact=ARTIFACT_A,
        )
    ).body
    _second_artifact = (
        await add_artifact(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            artifact=ARTIFACT_B,
        )
    ).body

    # expect artifacts' tags to be overridden by tags of their ancestral run
    update_objects_with_run_tags("artifact", [_first_artifact, _second_artifact], _run)

    # try to get all the created artifacts
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts".format(
            **_task
        ),
        data=[_second_artifact, _first_artifact],
        data_is_unordered_list_of_dicts=True,
    )

    # getting artifacts for non-existent flow should return empty list
    await assert_api_get_response(
        cli,
        "/flows/NonExistentFlow/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts".format(
            **_task
        ),
        status=200,
        data=[],
    )

    # getting artifacts for non-existent run should return empty list
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/1234/steps/{step_name}/tasks/{task_id}/artifacts".format(
            **_task
        ),
        status=200,
        data=[],
    )

    # getting artifacts for non-existent step should return empty list
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/nonexistent/tasks/{task_id}/artifacts".format(
            **_task
        ),
        status=200,
        data=[],
    )

    # getting artifacts for non-existent task should return empty list
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/1234/artifacts".format(
            **_task
        ),
        status=200,
        data=[],
    )


async def test_task_artifacts_pagination_get(cli, db):
    # create a flow, run, step and task for the test
    _flow = (
        await add_flow(
            db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"]
        )
    ).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _step = (
        await add_step(
            db,
            flow_id=_run["flow_id"],
            run_number=_run["run_number"],
            step_name="first_step",
        )
    ).body
    _task = (
        await add_task(
            db,
            flow_id=_step["flow_id"],
            run_number=_step["run_number"],
            step_name=_step["step_name"],
        )
    ).body

    # add artifacts to the task
    _first_artifact = (
        await add_artifact(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            artifact=ARTIFACT_A,
        )
    ).body
    _second_artifact = (
        await add_artifact(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            artifact=ARTIFACT_B,
        )
    ).body
    _third_artifact = (
        await add_artifact(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            artifact=ARTIFACT_C,
        )
    ).body

    # expect artifacts' tags to be overridden by tags of their ancestral run
    update_objects_with_run_tags(
        "artifact", [_first_artifact, _second_artifact, _third_artifact], _run
    )

    # first page
    next_cursor = await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts".format(
            **_task
        ),
        data=[_third_artifact, _second_artifact],
        params={"_limit": 2},
    )

    # continue with cursor
    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts".format(
            **_task
        ),
        data=[_first_artifact],
        params={"_limit": 2, "_cursor": next_cursor},
        status=200,
        has_next_cursor=False,
    )

    # invalid cursor
    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts".format(
            **_task
        ),
        params={"_cursor": "garbage1234"},
        status=400,
    )

    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts".format(
            **_task
        ),
        data=[_third_artifact, _second_artifact, _first_artifact],
        params={"_limit": 1000},
        has_next_cursor=False,
    )
    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts".format(
            **_task
        ),
        data=[_third_artifact, _second_artifact, _first_artifact],
        params={"_limit": 3},
        has_next_cursor=False,
    )


async def test_artifact_get(cli, db):
    # create flow, run, step and task for test
    _flow = (
        await add_flow(
            db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"]
        )
    ).body
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body
    _step = (
        await add_step(
            db,
            flow_id=_run["flow_id"],
            run_number=_run["run_number"],
            step_name="first_step",
        )
    ).body
    _task = (
        await add_task(
            db,
            flow_id=_step["flow_id"],
            run_number=_step["run_number"],
            step_name=_step["step_name"],
        )
    ).body

    # add artifact to task for testing
    _artifact = (
        await add_artifact(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            artifact=ARTIFACT_A,
        )
    ).body

    # expect artifact's tags to be overridden by tags of their ancestral run
    update_objects_with_run_tags("artifact", [_artifact], _run)

    # try to get created artifact
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts/{name}".format(
            **_artifact
        ),
        data=_artifact,
    )

    # non-existent flow, run, step, task or name should return 404
    await assert_api_get_response(
        cli,
        "/flows/NonExistentFlow/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts/{name}".format(
            **_artifact
        ),
        status=404,
    )
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/1234/steps/{step_name}/tasks/{task_id}/artifacts/{name}".format(
            **_artifact
        ),
        status=404,
    )
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/nonexistent_step/tasks/{task_id}/artifacts/{name}".format(
            **_artifact
        ),
        status=404,
    )
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/1234/artifacts/{name}".format(
            **_artifact
        ),
        status=404,
    )
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/artifacts/noname".format(
            **_artifact
        ),
        status=404,
    )


async def test_artifacts_get_returns_latest_attempt_only(cli, db):
    "Legacy (non-paginated) listing endpoints only return the latest attempt of an artifact."
    _task, _artifacts = await add_artifacts_for_attempts(db, attempt_ids=(0, 1))

    for path in ARTIFACT_LIST_PATHS:
        await assert_api_get_response(
            cli,
            path.format(**_task),
            data=_artifacts[1],
            data_is_unordered_list_of_dicts=True,
        )


async def test_artifacts_get_by_attempt_id(cli, db):
    "Legacy endpoints scoped to an attempt only return artifacts of that attempt."
    _task, _artifacts = await add_artifacts_for_attempts(db, attempt_ids=(0, 1))

    for attempt_id, _expected in _artifacts.items():
        await assert_api_get_response(
            cli,
            ARTIFACT_ATTEMPT_LIST_PATH.format(attempt_id=attempt_id, **_task),
            data=_expected,
            data_is_unordered_list_of_dicts=True,
        )

        # the single artifact endpoint is scoped to an attempt the same way
        for _artifact in _expected:
            await assert_api_get_response(
                cli,
                "/flows/{flow_id}/runs/{run_number}/steps/{step_name}"
                "/tasks/{task_id}/artifacts/{name}/attempt/{attempt_id}".format(
                    **_artifact
                ),
                data=_artifact,
            )

    # an attempt that was never recorded has no artifacts
    await assert_api_get_response(
        cli,
        ARTIFACT_ATTEMPT_LIST_PATH.format(attempt_id=2, **_task),
        data=[],
    )


async def test_artifacts_pagination_get_returns_latest_attempt_only(cli, db):
    "Paginated listing endpoints only return the latest attempt of an artifact."
    _task, _artifacts = await add_artifacts_for_attempts(db, attempt_ids=(0, 1))
    _first_artifact, _second_artifact, _third_artifact = _artifacts[1]

    for path in ARTIFACT_LIST_PATHS:
        _path = path.format(**_task)

        # a page big enough for every attempt still only contains the latest one
        await assert_paginated_api_get_response(
            cli,
            _path,
            data=[_third_artifact, _second_artifact, _first_artifact],
            params={"_limit": 1000},
            has_next_cursor=False,
        )

        # paging through the results does not leak older attempts either
        next_cursor = await assert_paginated_api_get_response(
            cli,
            _path,
            data=[_third_artifact, _second_artifact],
            params={"_limit": 2},
            has_next_cursor=True,
        )
        await assert_paginated_api_get_response(
            cli,
            _path,
            data=[_first_artifact],
            params={"_limit": 2, "_cursor": next_cursor},
            has_next_cursor=False,
        )


async def test_artifacts_pagination_get_by_attempt_id(cli, db):
    "Pagination parameters do not widen an attempt scoped endpoint beyond its attempt."
    _task, _artifacts = await add_artifacts_for_attempts(db, attempt_ids=(0, 1))

    for attempt_id, _expected in _artifacts.items():
        # NOTE: the attempt scoped endpoint does not implement cursor pagination, so
        # _limit and _cursor are ignored and the whole attempt is returned in one go.
        await assert_api_get_response(
            cli,
            ARTIFACT_ATTEMPT_LIST_PATH.format(attempt_id=attempt_id, **_task),
            params={"_limit": 2},
            data=_expected,
            data_is_unordered_list_of_dicts=True,
        )
