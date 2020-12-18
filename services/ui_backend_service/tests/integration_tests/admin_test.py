import pytest
from .utils import (
    init_app, init_db, clean_db
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


async def test_version(cli, db):
    resp = await cli.get("/version")
    body = await resp.text()

    assert resp.status == 200
    assert "2.0.4" in body


async def test_links(cli, db):
    default_links = [
        {"href": 'https://docs.metaflow.org/', "label": 'Metaflow documentation'},
        {"href": 'https://github.com/Netflix/metaflow', "label": 'Github'}
    ]

    resp = await cli.get("/links")
    body = await resp.json()

    assert resp.status == 200
    assert body == default_links
