import asyncio
import json
import uuid
import random

import time

from .utils import (
    cli, db,
    assert_api_get_response, assert_api_post_response, compare_partial,
    add_flow, add_run, assert_api_patch_response
)
import pytest

pytestmark = [pytest.mark.integration_tests]



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


async def test_run_post_has_initial_heartbeat_with_supported_version(cli, db):
    # create flow to add runs for.
    _flow = (await add_flow(db)).body

    # No initial heartbeat without client version info
    _run = await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/run".format(**_flow),
        payload={
            "user_name": "test_user",
            "tags": ["a_tag", "b_tag"],
            "system_tags": ["runtime:test"]
        },
        status=200  # why 200 instead of 201?
    )

    assert _run["last_heartbeat_ts"] is None

    # No initial heartbeat with non-heartbeating client version
    _run = await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/run".format(**_flow),
        payload={
            "user_name": "test_user",
            "tags": ["a_tag", "b_tag"],
            "system_tags": ["runtime:test", "metaflow_version:2.0.5"]
        },
        status=200  # why 200 instead of 201?
    )

    assert _run["last_heartbeat_ts"] is None

    # Should have initial heartbeat with heartbeating client version
    _run = await assert_api_post_response(
        cli,
        path="/flows/{flow_id}/run".format(**_flow),
        payload={
            "user_name": "test_user",
            "tags": ["a_tag", "b_tag"],
            "system_tags": ["runtime:test", "metaflow_version:2.2.12"]
        },
        status=200  # why 200 instead of 201?
    )

    assert _run["last_heartbeat_ts"] is not None



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
    await assert_api_get_response(cli, "/flows/{flow_id}/runs".format(**_first_run),
                                  data=[_second_run, _first_run], data_is_unordered_list_of_dicts=True)

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


async def test_run_mutate_user_tags(cli, db):
    # create flow for test
    _flow = (await add_flow(db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])).body

    # add run to flow for testing
    _run = (await add_run(db, flow_id=_flow["flow_id"])).body

    async def assert_tags_unchanged():
        _run_in_db = (await db.run_table_postgres.get_run(_run["flow_id"], _run["run_number"])).body
        assert sorted(_run_in_db["system_tags"]) == sorted(_run["system_tags"])
        assert sorted(_run_in_db["tags"]) == sorted(_run["tags"])

    async def assert_tags_in_db(tags):
        _run_in_db = (await db.run_table_postgres.get_run(_run["flow_id"], _run["run_number"])).body
        assert all(tag in _run_in_db["tags"] for tag in tags)

    async def assert_tags_not_in_db(tags):
        _run_in_db = (await db.run_table_postgres.get_run(_run["flow_id"], _run["run_number"])).body
        assert all(tag not in _run_in_db["tags"] for tag in tags)

    # try invalid inputs (like tag lists that are not lists, or tag values that are not string)
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run), payload={"tags_to_add": "so_meta"}, status=422)
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run), payload={"tags_to_add": [5]}, status=422)
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run), payload={"tags_to_remove": "so_meta"}, status=422)
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run), payload={"tags_to_remove": [5]}, status=422)
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/__NOT_A_RUN__/tag/mutate'.format(**_run), payload={"tags_to_add": ["user_tag"]}, status=404)

    # try to remove system tags - it should not work
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run), payload={"tags_to_remove": _run["system_tags"]}, status=422)
    await assert_tags_unchanged()

    # try to add system tags - it should be no-op (but no error)
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run), payload={"tags_to_add": _run["system_tags"]}, status=200)
    await assert_tags_unchanged()

    # try to add user tags
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run), payload={"tags_to_add": ["coca-cola", "pepsi"]}, status=200)
    await assert_tags_in_db(["coca-cola", "pepsi"])

    # try to remove user tags
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run), payload={"tags_to_remove": ["coca-cola", "pepsi"]}, status=200)
    await assert_tags_not_in_db(["coca-cola", "pepsi"])

    # try to replace user tags
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run), payload={"tags_to_add": ["coca-cola", "pepsi"]}, status=200)
    await assert_api_patch_response(cli, '/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run), payload={"tags_to_add": ["sprite", "pepsi"], "tags_to_remove": ["coca-cola", "pepsi"]}, status=200)
    await assert_tags_in_db(["sprite", "pepsi"])
    await assert_tags_not_in_db(["coca-cola"])


async def test_run_mutate_user_tags_concurrency(cli, db):
    # create flow for test
    _flow = (await add_flow(db, "TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])).body

    # add run to flow for testing.  Start with 0 user tags
    _run = (await add_run(db, flow_id=_flow["flow_id"], tags=[])).body

    async def _mutation_request_with_retries(path, payload):
        attempts = 0
        delay = 0.3
        r = random.Random(json.dumps(payload))
        for _ in range(10):
            attempts += 1
            response = await cli.patch(path, json=payload)
            if response.status == 200:
                return attempts
            # 500: generic connection acquisition timeout
            elif response.status == 503:
                delay *= r.uniform(1.0, 1.4)
                await asyncio.sleep(delay)
            else:
                raise AssertionError("Unexpected status %d" % response.status)
        raise AssertionError("Retries exhausted")

    # confirm that concurrent requests get serialized correctly, if they retry
    # in other words, no mutations get lost
    expected_tag_set = set()
    awaitables = []
    for i in range(50):
        a_tag = str(i)
        expected_tag_set.add(a_tag)
        awaitables.append(_mutation_request_with_retries('/flows/{flow_id}/runs/{run_number}/tag/mutate'.format(**_run), {"tags_to_add": [a_tag]}))
    retry_counts = await asyncio.gather(*awaitables)
    assert sum(retry_counts) >= 25

    _run_in_db = (await db.run_table_postgres.get_run(_run["flow_id"], _run["run_number"])).body
    assert sorted(_run_in_db["tags"]) == sorted(expected_tag_set)

