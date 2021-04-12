import pytest
import json
import os
import time
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


@pytest.fixture
async def announcements():
    _announcements = [
        {"message": "Visible: No start/end times defined"},
        {"message": "Visible: No start/end defined and id + type overwritten", "id": "fixed_id", "type": "warning"},
        {"message": "Hidden: Start time not yet reached", "start": time.time() - 1000 * 10},
        {"message": "Visible: Start time is after now()", "start": time.time() + 1000 * 10},
        {"message": "Hidden: End time passed", "end": time.time() + 1000 * 10},
        {"message": "Visible: End time not yet reached", "end": time.time() - 1000 * 10},
        {"message": "Visible: Start time passed and end time not yet reached", "start": time.time() + 1000 * 10, "end": time.time() - 1000 * 10},
    ]

    # Announcements are configured through an environment variable.
    custom_string = json.dumps(_announcements)
    os.environ["ANNOUNCEMENTS"] = custom_string

    yield _announcements
    del(os.environ["ANNOUNCEMENTS"])  # cleanup afterwards


@pytest.fixture
async def broken_env():
    # Set environment variables that can not be parsed as json.
    broken_json = "notvalidjson"
    os.environ["CUSTOM_QUICKLINKS"] = broken_json
    os.environ["ANNOUNCEMENTS"] = broken_json

    yield
    del(os.environ["CUSTOM_QUICKLINKS"])  # cleanup afterwards
    del(os.environ["ANNOUNCEMENTS"])  # cleanup afterwards
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


async def test_announcements(announcements, cli, db):
    resp = await cli.get("/announcements")
    body = await resp.json()

    assert resp.status == 200
    assert len(body) == 5

    for announcement in body:
        assert announcement["message"].startswith("Visible: ")
        assert announcement["type"] is not None
        assert len(announcement["id"]) > 0

    assert body[1]["id"] == "fixed_id"
    assert body[1]["type"] == "warning"

    assert body[4]["start"] is not None
    assert body[4]["end"] is not None


async def test_broken_json_announcements(broken_env, cli, db):
    resp = await cli.get("/announcements")
    body = await resp.json()

    assert resp.status == 200
    assert body == []


async def test_broken_json_links(broken_env, cli, db):
    default_links = [
        {"href": 'https://docs.metaflow.org/', "label": 'Documentation'},
        {"href": 'https://gitter.im/metaflow_org/community?source=orgpage', "label": 'Help'}
    ]
    resp = await cli.get("/links")
    body = await resp.json()

    assert resp.status == 200
    assert body == default_links
