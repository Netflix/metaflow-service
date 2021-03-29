from .utils import init_app, init_db, clean_db, assert_api_get_response
from services.data.models import FlowRow
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


async def test_flows_post(cli, db):
  payload = {
      "user_name": "test_user",
      "tags": ["a_tag", "b_tag"],
      "system_tags": ["runtime:test"]
  }
  response = await cli.post("/flows/{}".format("TestFlow"), json=payload)

  assert response.status == 200  # why 200 instead of 201?

  # Record should be found in DB
  _flow = await db.flow_table_postgres.get_flow(flow_id="TestFlow")

  assert _flow.body["user_name"] == payload["user_name"]
  assert _flow.body["tags"] == payload["tags"]
  assert _flow.body["system_tags"] == payload["system_tags"]

  # Second post should fail as flow already exists.
  response = await cli.post("/flows/{}".format("TestFlow"), json=payload)

  assert response.status == 409


async def test_flows_get(cli, db):
  # create a few flows for test
  _first_flow = (await db.flow_table_postgres.add_flow(
      FlowRow("TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])
  )).body
  _second_flow = (await db.flow_table_postgres.add_flow(
      FlowRow("AnotherTestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])
  )).body

  # try to get all the created flows
  await assert_api_get_response(cli, "/flows", data=[_first_flow, _second_flow])


async def test_flow_get(cli, db):
  # create flow for test
  _flow = (await db.flow_table_postgres.add_flow(
      FlowRow("TestFlow", "test_user-1", ["a_tag", "b_tag"], ["runtime:test"])
  )).body

  # try to get created flow
  await assert_api_get_response(cli, "/flows/TestFlow", data=_flow)

  # non-existent flow should return 404
  await assert_api_get_response(cli, "/flows/AnotherFlow", status=404)
