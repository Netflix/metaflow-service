import pytest
import json
import os
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


@pytest.fixture
async def custom_quicklinks():
    custom = [
        {"href": 'docs_url', "label": 'Custom Documents'},
        {"href": 'info_url', "label": 'Info'}
    ]

    # Quicklinks are configured through an environment variable.
    custom_string = json.dumps(custom)
    os.environ["CUSTOM_QUICKLINKS"] = custom_string

    yield custom
    del(os.environ["CUSTOM_QUICKLINKS"])  # cleanup afterwards

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
        {"href": 'https://docs.metaflow.org/', "label": 'Documentation'},
        {"href": 'https://gitter.im/metaflow_org/community?source=orgpage', "label": 'Help'}
    ]

    resp = await cli.get("/links")
    body = await resp.json()

    assert resp.status == 200
    assert body == default_links


async def test_custom_links(custom_quicklinks, cli, db):
    resp = await cli.get("/links")
    body = await resp.json()

    assert resp.status == 200
    assert body == custom_quicklinks
