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

NOW = time.time() * 1000  # Epoch time in milliseconds
HOUR = 3600 * 1000  # 1 hour in milliseconds


@pytest.fixture
async def notifications():
    _notifications = [
        {"created": NOW, "message": "Active: No start/end times defined"},
        {"created": NOW, "message": "Active: No start/end defined and id + type overwritten", "id": "fixed_id", "type": "warning"},
        {"created": NOW, "message": "Hidden: Start time not yet reached", "start": NOW + HOUR},
        {"created": NOW, "message": "Active: Start time passed", "start": NOW - HOUR},
        {"created": NOW, "message": "Hidden: End time passed", "end": NOW - HOUR},
        {"created": NOW, "message": "Active: End time not yet reached", "end": NOW + HOUR},
        {"created": NOW, "message": "Active: Start time passed and end time not yet reached", "start": NOW - HOUR, "end": NOW + HOUR},
        {"message": "Ignored due to missing `created` field"},
        {"created": NOW},  # Ignored due to missing `message` field
    ]

    # System notifications are configured through an environment variable.
    custom_string = json.dumps(_notifications)
    os.environ["NOTIFICATIONS"] = custom_string

    yield _notifications
    del(os.environ["NOTIFICATIONS"])  # cleanup afterwards


@pytest.fixture
async def broken_env():
    # Set environment variables that can not be parsed as json.
    broken_json = "notvalidjson"
    os.environ["CUSTOM_QUICKLINKS"] = broken_json
    os.environ["NOTIFICATIONS"] = broken_json

    yield
    del(os.environ["CUSTOM_QUICKLINKS"])  # cleanup afterwards
    del(os.environ["NOTIFICATIONS"])  # cleanup afterwards
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
    assert "2.0.6" in body


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


async def test_notifications_all(notifications, cli, db):
    resp = await cli.get("/notifications")
    body = await resp.json()

    assert resp.status == 200
    assert len(body) == 7  # Excludes two ignored notifications with missing information


async def test_notifications_filter_active(notifications, cli, db):
    resp = await cli.get("/notifications?start:le={now}&end:ge={now}".format(now=NOW))
    body = await resp.json()

    assert resp.status == 200
    assert len(body) == 5

    for notification in body:
        assert notification["message"].startswith("Active: ")
        assert notification["type"] is not None
        assert len(notification["id"]) > 0

    assert body[1]["id"] == "fixed_id"
    assert body[1]["type"] == "warning"

    assert body[4]["start"] is not None
    assert body[4]["end"] is not None


async def test_notifications_filter(notifications, cli, db):
    compare_things = [
        ["/notifications?id={}".format("fixed_id"), 1],
        ["/notifications?id:eq={}".format("fixed_id"), 1],
        ["/notifications?id:ne={}".format("fixed_id"), 6],
        ["/notifications?end:lt={}".format(NOW), 5],
        ["/notifications?end:le={}".format(NOW + HOUR), 7],
        ["/notifications?start:le={}".format(NOW - (HOUR * 12)), 4],
        ["/notifications?start:gt={}".format(NOW), 5],
        ["/notifications?start:ge={}".format(NOW - HOUR), 7],
        ["/notifications?end:ge={}".format(NOW + (HOUR * 12)), 4],
    ]

    for path, expected_len in compare_things:
        resp = await cli.get(path)
        body = await resp.json()

        assert resp.status == 200
        assert len(body) == expected_len


async def test_broken_json_notifications(broken_env, cli, db):
    resp = await cli.get("/notifications")
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
