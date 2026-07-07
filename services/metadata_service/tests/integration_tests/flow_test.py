from .utils import (
    cli,
    db,
    assert_api_get_response,
    assert_api_post_response,
    compare_partial,
    add_flow,
    assert_paginated_api_get_response,
)
import pytest

pytestmark = [pytest.mark.integration_tests]


async def test_flows_post(cli, db):
    payload = {
        "user_name": "test_user",
        "tags": ["a_tag", "b_tag"],
        "system_tags": ["runtime:test"],
    }
    await assert_api_post_response(
        cli,
        path="/flows/{}".format("TestFlow"),
        payload=payload,
        status=200,  # why 200 instead of 201?
    )

    # Record should be found in DB
    _flow = (await db.flow_table_postgres.get_flow(flow_id="TestFlow")).body

    compare_partial(_flow, payload)

    # Second post should fail as flow already exists.
    await assert_api_post_response(
        cli, path="/flows/{}".format("TestFlow"), payload=payload, status=409
    )


async def test_flows_get(cli, db):
    # create a few flows for test
    _first_flow = (
        await add_flow(
            db,
            flow_id="TestFlow",
            user_name="test_user-1",
            tags=["a_tag", "b_tag"],
            system_tags=["runtime:test"],
        )
    ).body
    _second_flow = (
        await add_flow(db, flow_id="AnotherTestFlow", user_name="test_user-1")
    ).body

    # try to get all the created flows
    await assert_api_get_response(
        cli,
        "/flows",
        data=[_first_flow, _second_flow],
        data_is_unordered_list_of_dicts=True,
    )


async def test_flow_get(cli, db):
    # create flow for test
    _flow = (await add_flow(db, flow_id="TestFlow", user_name="test_user-1")).body

    # try to get created flow
    await assert_api_get_response(cli, "/flows/TestFlow", data=_flow)

    # non-existent flow should return 404
    await assert_api_get_response(cli, "/flows/AnotherFlow", status=404)


async def test_flows_pagination_get(cli, db):
    # create flow for test
    _first_flow = (await add_flow(db, flow_id="TestFlow", user_name="test_user-1")).body
    _second_flow = (
        await add_flow(db, flow_id="TestFlow2", user_name="test_user-1")
    ).body
    _third_flow = (
        await add_flow(db, flow_id="TestFlow3", user_name="test_user-1")
    ).body

    # first page
    next_cursor = await assert_paginated_api_get_response(
        cli,
        "/flows",
        data=[_third_flow, _second_flow],
        params={"_limit": 2},
    )

    # continue with cursor
    await assert_paginated_api_get_response(
        cli,
        "/flows",
        data=[_first_flow],
        params={"_limit": 2, "_cursor": next_cursor},
        status=200,
        has_next_cursor=False,
    )

    # invalid cursor
    await assert_paginated_api_get_response(
        cli,
        "/flows",
        params={"_cursor": "garbage1234"},
        status=400,
    )

    await assert_paginated_api_get_response(
        cli,
        "/flows",
        data=[_third_flow, _second_flow, _first_flow],
        params={"_limit": 1000},
        has_next_cursor=False,
    )
