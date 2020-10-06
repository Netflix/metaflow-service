import pytest
from .utils import (
    init_app, init_db, clean_db,
    add_flow,
    _test_list_resources, _test_single_resource
)
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


async def test_ping(cli, db):
    resp = await cli.get("/ping")
    body = await resp.text()

    assert resp.status == 200
    assert body == "pong"
