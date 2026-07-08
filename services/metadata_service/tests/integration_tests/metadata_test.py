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
    add_metadata,
    assert_paginated_api_get_response,
)
import pytest

pytestmark = [pytest.mark.integration_tests]


# Shared metadata test data
METADATA_A = {
    "field_name": "test-field-A",
    "value": "test",
    "type": "test-metadata",
    "user_name": "test_user",
    "tags": ["a_tag", "b_tag"],
    "system_tags": ["runtime:test"],
}
METADATA_B = {
    "field_name": "test-field-B",
    "value": "test",
    "type": "test-metadata",
    "user_name": "test_user",
    "tags": ["a_tag", "b_tag"],
    "system_tags": ["runtime:test"],
}
METADATA_C = {
    "field_name": "test-field-C",
    "value": "test",
    "type": "test-metadata",
    "user_name": "test_user",
    "tags": ["a_tag", "b_tag"],
    "system_tags": ["runtime:test"],
}


async def test_metadata_post(cli, db):
    # create flow, run, step and task to add metadata for.
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

    # metadata
    payload = [METADATA_A, METADATA_B]

    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(
            **_task
        ),
        payload=payload,
        status=200,
        expected_body={
            "metadata_created": 2
        },  # api responds with only the number of metadata created.
    )

    # Records should be found in DB
    _data = (
        await db.metadata_table_postgres.get_metadata(
            _task["flow_id"], _task["run_number"], _task["step_name"], _task["task_id"]
        )
    ).body
    _first_found, _second_found = _data

    compare_partial(_first_found, METADATA_A)
    compare_partial(_second_found, METADATA_B)

    # Posting the same metadata twice should succeed, as duplicates are allowed due to
    # task attempts producing items with the same names.
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(
            **_task
        ),
        payload=payload,
        status=200,
        expected_body={"metadata_created": 2},
    )

    # Posting on a non-existent flow_id should result in error
    await assert_api_post_response(
        cli,
        path="/flows/NonExistentFlow/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(
            **_task
        ),
        payload=payload,
        status=400,
        expected_body={"message": "need to register run_id and task_id first"},
    )

    # posting on a non-existent run number should result in an error
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/1234/steps/{step_name}/tasks/{task_id}/metadata".format(
            **_task
        ),
        payload=payload,
        status=400,
        expected_body={"message": "need to register run_id and task_id first"},
    )

    # posting on a non-existent step_name should result in an error
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/nonexistent/tasks/{task_id}/metadata".format(
            **_task
        ),
        payload=payload,
        status=400,
        expected_body={"message": "need to register run_id and task_id first"},
    )

    # posting on a non-existent task_id should result in an error
    await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/1234/metadata".format(
            **_task
        ),
        payload=payload,
        status=400,
        expected_body={"message": "need to register run_id and task_id first"},
    )


async def test_run_metadata_get(cli, db):
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

    # add metadata to the task
    _first_metadata = (
        await add_metadata(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            metadata=METADATA_A,
        )
    ).body
    _second_metadata = (
        await add_metadata(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            metadata=METADATA_B,
        )
    ).body

    # try to get all the created metadata
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/metadata".format(**_task),
        data=[_first_metadata, _second_metadata],
        data_is_unordered_list_of_dicts=True,
    )

    # getting metadata for non-existent flow should return empty list
    await assert_api_get_response(
        cli,
        "/flows/NonExistentFlow/runs/{run_number}/metadata".format(**_task),
        status=200,
        data=[],
    )

    # getting metadata for non-existent run should return empty list
    await assert_api_get_response(
        cli, "/flows/{flow_id}/runs/1234/metadata".format(**_task), status=200, data=[]
    )


async def test_run_metadata_pagination_get(cli, db):
    # create for test
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

    _first_metadata = (
        await add_metadata(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            metadata=METADATA_A,
        )
    ).body
    _second_metadata = (
        await add_metadata(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            metadata=METADATA_B,
        )
    ).body

    _third_metadata = (
        await add_metadata(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            metadata=METADATA_C,
        )
    ).body
     # ========== DEBUG START ==========
    import json as _json
    import base64 as _b64

    print("\n\n########## METADATA VALUES ##########")
    print(f"A: id={_first_metadata['id']}, ts={_first_metadata['ts_epoch']}")
    print(f"B: id={_second_metadata['id']}, ts={_second_metadata['ts_epoch']}")
    print(f"C: id={_third_metadata['id']}, ts={_third_metadata['ts_epoch']}")

    _r1 = await cli.get(
        "/flows/{flow_id}/runs/{run_number}/metadata".format(**_task),
        params={"_limit": 2},
    )
    _b1 = _json.loads(await _r1.text())
    _cur = _r1.headers.get("X-Next-Cursor")
    print("\n########## FIRST PAGE ##########")
    print(f"body: {_b1}")
    print(f"cursor(raw): {_cur}")
    if _cur:
        print(f"cursor(decoded): {_b64.b64decode(_cur).decode()}")

    _r2 = await cli.get(
        "/flows/{flow_id}/runs/{run_number}/metadata".format(**_task),
        params={"_limit": 2, "_cursor": _cur},
    )
    _b2 = _json.loads(await _r2.text())
    print("\n########## SECOND PAGE ##########")
    print(f"body: {_b2}")
    print(f"cursor(raw): {_r2.headers.get('X-Next-Cursor')}")
    print("################################\n")
    # ========== DEBUG END ==========
    # first page
    next_cursor = await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/metadata".format(**_task),
        data=[_third_metadata, _second_metadata],
        params={"_limit": 2},
    )
    # continue with cursor
    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/metadata".format(**_task),
        data=[_first_metadata],
        params={"_limit": 2, "_cursor": next_cursor},
        status=200,
        has_next_cursor=False,
    )
    # invalid cursor
    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/metadata".format(**_task),
        params={"_cursor": "garbage1234"},
        status=400,
    )

    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/metadata".format(**_task),
        data=[_third_metadata, _second_metadata, _first_metadata],
        params={"_limit": 1000},
        has_next_cursor=False,
    )
    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/metadata".format(**_task),
        data=[_third_metadata, _second_metadata, _first_metadata],
        params={"_limit": 3},
        has_next_cursor=False,
    )


async def test_task_metadata_get(cli, db):
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

    # add metadata to the task
    _first_metadata = (
        await add_metadata(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            metadata=METADATA_A,
        )
    ).body
    _second_metadata = (
        await add_metadata(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            metadata=METADATA_B,
        )
    ).body

    # try to get all the created metadata
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(
            **_task
        ),
        data=[_first_metadata, _second_metadata],
        data_is_unordered_list_of_dicts=True,
    )

    # getting metadata for non-existent flow should return empty list
    await assert_api_get_response(
        cli,
        "/flows/NonExistentFlow/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(
            **_task
        ),
        status=200,
        data=[],
    )

    # getting metadata for non-existent run should return empty list
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/1234/steps/{step_name}/tasks/{task_id}/metadata".format(
            **_task
        ),
        status=200,
        data=[],
    )

    # getting metadata for non-existent step should return empty list
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/nonexistent/tasks/{task_id}/metadata".format(
            **_task
        ),
        status=200,
        data=[],
    )

    # getting metadata for non-existent task should return empty list
    await assert_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/1234/metadata".format(
            **_task
        ),
        status=200,
        data=[],
    )


async def test_task_metadata_pagination_get(cli, db):
    # create for test
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

    _first_metadata = (
        await add_metadata(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            metadata=METADATA_A,
        )
    ).body
    _second_metadata = (
        await add_metadata(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            metadata=METADATA_B,
        )
    ).body
    _third_metadata = (
        await add_metadata(
            db,
            flow_id=_task["flow_id"],
            run_number=_task["run_number"],
            step_name=_task["step_name"],
            task_id=_task["task_id"],
            metadata=METADATA_C,
        )
    ).body

    # first page
    next_cursor = await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(
            **_task
        ),
        data=[_third_metadata, _second_metadata],
        params={"_limit": 2},
    )
    # continue with cursor
    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(
            **_task
        ),
        data=[_first_metadata],
        params={"_limit": 2, "_cursor": next_cursor},
        status=200,
        has_next_cursor=False,
    )
    # invalid cursor
    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(
            **_task
        ),
        params={"_cursor": "garbage1234"},
        status=400,
    )

    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(
            **_task
        ),
        data=[_third_metadata, _second_metadata, _first_metadata],
        params={"_limit": 1000},
        has_next_cursor=False,
    )

    await assert_paginated_api_get_response(
        cli,
        "/flows/{flow_id}/runs/{run_number}/steps/{step_name}/tasks/{task_id}/metadata".format(
            **_task
        ),
        data=[_third_metadata, _second_metadata, _first_metadata],
        params={"_limit": 3},
        has_next_cursor=False,
    )
