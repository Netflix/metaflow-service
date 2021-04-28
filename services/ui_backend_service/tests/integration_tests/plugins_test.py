import pytest
from unittest import mock
import json
import os
import contextlib
from .utils import (
    init_app, init_db, clean_db
)
from ...plugins import init_plugins, reset_plugins, Plugin
from aiohttp import web


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
async def setup_plugins():
    _plugins = {
        "mock-plugin": {
            "parameters": {
                "color": "yellow"
            }
        },
        "turbo-eureka": "https://github.com/Netflix/metaflow-ui-plugin-turbo-eureka.git",
        "solid-waffle": {
            "repository": "https://github.com/Netflix/metaflow-ui-plugin-solid-waffle.git",
            "ref": "origin/feature/extra-syrup"
        }
    }
    custom_string = json.dumps(_plugins)
    os.environ["PLUGINS"] = custom_string

    yield _plugins
    del(os.environ["PLUGINS"])  # cleanup afterwards


@pytest.fixture
async def broken_env():
    # Set environment variables that can not be parsed as json.
    os.environ["PLUGINS"] = "notvalidjson"

    yield
    del(os.environ["PLUGINS"])  # cleanup afterwards
# Fixtures end


@contextlib.contextmanager
def mock_plugins():
    with mock.patch("services.ui_backend_service.plugins.Plugin", new=MockPlugin):
        init_plugins()
        try:
            yield
        finally:
            reset_plugins()


async def test_plugins_not_initialized(setup_plugins, cli, db):
    resp = await cli.get("/plugin")
    body = await resp.json()

    assert resp.status == 200
    assert len(body) == 0


async def test_plugins_all(setup_plugins, cli, db):
    with mock_plugins():
        resp = await cli.get("/plugin")
        body = await resp.json()

    assert resp.status == 200
    assert len(body) == 3

    plugin_mock_plugin = body[0]
    plugin_turbo_eureka = body[1]
    plugin_solid_waffle = body[2]

    assert plugin_mock_plugin["name"] == "mock-plugin"
    assert plugin_mock_plugin["repository"] == None
    assert plugin_mock_plugin["ref"] == None
    assert plugin_mock_plugin["parameters"] == {"color": "yellow"}

    assert plugin_turbo_eureka["name"] == "turbo-eureka"
    assert plugin_turbo_eureka["repository"] == "https://github.com/Netflix/metaflow-ui-plugin-turbo-eureka.git"
    assert plugin_turbo_eureka["ref"] == None
    assert plugin_turbo_eureka["parameters"] == {}

    assert plugin_solid_waffle["name"] == "solid-waffle"
    assert plugin_solid_waffle["repository"] == "https://github.com/Netflix/metaflow-ui-plugin-solid-waffle.git"
    assert plugin_solid_waffle["ref"] == "origin/feature/extra-syrup"
    assert plugin_solid_waffle["parameters"] == {}


async def test_plugins_mock_plugin(setup_plugins, cli, db):
    with mock_plugins():
        resp = await cli.get("/plugin/mock-plugin")
        body = await resp.json()

    assert resp.status == 200

    assert body["name"] == "mock-plugin"
    assert body["repository"] == None
    assert body["ref"] == None
    assert body["parameters"] == {"color": "yellow"}
    assert body["files"] == ["manifest.json", "index.html"]
    assert body["config"] == {
        "name": "mock-plugin",
        "version": "1.0.0",
        "entrypoint": "index.html"
    }


async def test_plugins_assets(setup_plugins, cli, db):
    with mock_plugins():
        resp = await cli.get("/plugin/mock-plugin/notfound.json")

        assert resp.status == 404

        resp = await cli.get("/plugin/mock-plugin/manifest.json")
        body = await resp.text()

        assert resp.status == 200
        assert body == json.dumps({
            "name": "mock-plugin",
            "version": "1.0.0",
            "entrypoint": "index.html"
        })

        resp = await cli.get("/plugin/mock-plugin/index.html")
        body = await resp.text()

        assert resp.status == 200
        assert body == "<h1>Hello from mock-plugin</h1>"


async def test_broken_json_plugins(broken_env, cli, db):
    with mock_plugins():
        resp = await cli.get("/plugin")
        body = await resp.json()

    assert resp.status == 200
    assert body == []


class MockPlugin(Plugin):
    def init(self):
        self.files = ["manifest.json", "index.html"]
        self.config = {"name": self.name, "version": "1.0.0", "entrypoint": "index.html"}
        return self

    def checkout(self):
        pass

    def get_file(self, filename):
        _files = {
            "manifest.json": json.dumps(self.config),
            "index.html": "<h1>Hello from {}</h1>".format(self.name)
        }
        return _files.get(filename, None)

    def serve(self, filename):
        _file = self.get_file(filename)
        if _file:
            return web.Response(status=200, body=_file)
        return web.Response(status=404, body="File not found")
