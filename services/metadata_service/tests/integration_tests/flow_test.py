from .utils import init_app, init_db, clean_db
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
  # body = await response.json() # TODO: api does not respond with correct json content headers.

  assert response.status == 200  # why 200 instead of 201?

  # Record should be found in DB
  _flow = await db.flow_table_postgres.get_flow(flow_id="TestFlow")

  assert _flow.body["user_name"] == payload["user_name"]
  assert _flow.body["tags"] == payload["tags"]
  assert _flow.body["system_tags"] == payload["system_tags"]

  # Second post should fail as flow already exists.

  response = await cli.post("/flows/{}".format("TestFlow"), json=payload)
  # body = await response.json() # TODO: api fails to respond with correct json content headers

  assert response.status == 409
