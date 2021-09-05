import pytest
from unittest import mock
import json
import os
import contextlib
from .utils import (
    init_app, init_db, clean_db
)
from ...plugins import init_plugins, list_plugins, _reset_plugins, Plugin
from aiohttp import web
import pygit2


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
        },
        "metaflow-ui-plugins": {
            "repository": "https://github.com/Netflix/metaflow-ui-plugins.git",
            "paths": [
                "plugin-first",
                "plugin-second"
            ]
        }
    }
    custom_string = json.dumps(_plugins)
    os.environ["PLUGINS"] = custom_string

    yield _plugins
    del(os.environ["PLUGINS"])  # cleanup afterwards


@pytest.fixture
async def setup_plugins_auth():
    _plugins = {
        "auth": {
            "public_key": "/root/id_rsa.pub",
            "private_key": "/root/id_rsa",
            "user": "git",
            "pass": "optional-passphrase"
        },
        "plugin-auth-global": "https://github.com/Netflix/metaflow-ui-plugin-turbo-eureka.git",
        "plugin-noauth": {
            "repository": "https://github.com/Netflix/metaflow-ui-plugin-solid-waffle.git",
            "auth": None
        },
        "plugin-auth-user": {
            "repository": "https://github.com/Netflix/metaflow-ui-plugin-solid-waffle.git",
            "auth": {
                "user": "username"
            }
        },
        "plugin-auth-userpass": {
            "repository": "https://github.com/Netflix/metaflow-ui-plugin-solid-waffle.git",
            "auth": {
                "user": "username",
                "pass": "password"
            }
        },
        "plugin-auth-key-user": {
            "repository": "https://github.com/Netflix/metaflow-ui-plugin-solid-waffle.git",
            "auth": {
                "public_key": "ssh-rsa AAAA...",
                "private_key": "-----BEGIN RSA PRIVATE KEY-----...",
                "user": "custom"
            }
        },
        "plugin-auth-key-pass": {
            "repository": "https://github.com/Netflix/metaflow-ui-plugin-solid-waffle.git",
            "auth": {
                "public_key": "/root/id_rsa.pub",
                "private_key": "/root/id_rsa",
                "pass": "optional-passphrase"
            }
        },
        "plugin-auth-agent": {
            "repository": "https://github.com/Netflix/metaflow-ui-plugin-solid-waffle.git",
            "auth": {
                "agent": True
            }
        },
        "plugin-auth-agent-user": {
            "repository": "https://github.com/Netflix/metaflow-ui-plugin-solid-waffle.git",
            "auth": {
                "agent": True,
                "user": "custom"
            }
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
            _reset_plugins()


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
    assert len(body) == 5

    plugin_mock_plugin = body[0]
    plugin_turbo_eureka = body[1]
    plugin_solid_waffle = body[2]
    plugin_first = body[3]
    plugin_secondary = body[4]

    assert plugin_mock_plugin["identifier"] == "mock-plugin"
    assert plugin_mock_plugin["name"] == "mock-plugin"
    assert plugin_mock_plugin["repository"] == None
    assert plugin_mock_plugin["ref"] == None
    assert plugin_mock_plugin["parameters"] == {"color": "yellow"}

    assert plugin_turbo_eureka["identifier"] == "turbo-eureka"
    assert plugin_turbo_eureka["name"] == "turbo-eureka"
    assert plugin_turbo_eureka["repository"] == "https://github.com/Netflix/metaflow-ui-plugin-turbo-eureka.git"
    assert plugin_turbo_eureka["ref"] == None
    assert plugin_turbo_eureka["parameters"] == {}

    assert plugin_solid_waffle["identifier"] == "solid-waffle"
    assert plugin_solid_waffle["name"] == "solid-waffle"
    assert plugin_solid_waffle["repository"] == "https://github.com/Netflix/metaflow-ui-plugin-solid-waffle.git"
    assert plugin_solid_waffle["ref"] == "origin/feature/extra-syrup"
    assert plugin_solid_waffle["parameters"] == {}

    assert plugin_first["identifier"] == "metaflow-ui-plugins"
    assert plugin_first["name"] == "plugin-first"
    assert plugin_first["repository"] == "https://github.com/Netflix/metaflow-ui-plugins.git"
    assert plugin_first["ref"] == None
    assert plugin_first["parameters"] == {}

    assert plugin_secondary["identifier"] == "metaflow-ui-plugins"
    assert plugin_secondary["name"] == "plugin-second"
    assert plugin_secondary["repository"] == "https://github.com/Netflix/metaflow-ui-plugins.git"
    assert plugin_secondary["ref"] == None
    assert plugin_secondary["parameters"] == {}


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


async def test_plugins_multiple_plugins_first(setup_plugins, cli, db):
    with mock_plugins():
        resp = await cli.get("/plugin/plugin-first")
        body = await resp.json()

    assert resp.status == 200

    assert body["identifier"] == "metaflow-ui-plugins"
    assert body["name"] == "plugin-first"
    assert body["repository"] == "https://github.com/Netflix/metaflow-ui-plugins.git"
    assert body["ref"] == None
    assert body["parameters"] == {}
    assert body["files"] == ["manifest.json", "index.html"]
    assert body["config"] == {
        "name": "plugin-first",
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


async def test_plugins_auth(setup_plugins_auth, cli, db):
    with mock_plugins():
        _plugins_list = list_plugins()

    plugin_noauth = next(p for p in _plugins_list if p.identifier == "plugin-noauth")
    plugin_auth_global = next(p for p in _plugins_list if p.identifier == "plugin-auth-global")
    plugin_auth_user = next(p for p in _plugins_list if p.identifier == "plugin-auth-user")
    plugin_auth_userpass = next(p for p in _plugins_list if p.identifier == "plugin-auth-userpass")
    plugin_auth_key_user = next(p for p in _plugins_list if p.identifier == "plugin-auth-key-user")
    plugin_auth_key_pass = next(p for p in _plugins_list if p.identifier == "plugin-auth-key-pass")
    plugin_auth_agent = next(p for p in _plugins_list if p.identifier == "plugin-auth-agent")
    plugin_auth_agent_user = next(p for p in _plugins_list if p.identifier == "plugin-auth-agent-user")

    assert plugin_noauth.credentials == None
    assert isinstance(plugin_auth_global.credentials, pygit2.Keypair)
    assert isinstance(plugin_auth_user.credentials, pygit2.Username)
    assert isinstance(plugin_auth_userpass.credentials, pygit2.UserPass)
    assert isinstance(plugin_auth_key_user.credentials, pygit2.KeypairFromMemory)
    assert isinstance(plugin_auth_key_pass.credentials, pygit2.Keypair)
    assert isinstance(plugin_auth_agent.credentials, pygit2.KeypairFromAgent)
    assert isinstance(plugin_auth_agent_user.credentials, pygit2.KeypairFromAgent)

    assert ("git", "/root/id_rsa.pub", "/root/id_rsa", "optional-passphrase") == plugin_auth_global.credentials.credential_tuple
    assert ("username",) == plugin_auth_user.credentials.credential_tuple
    assert ("username", "password") == plugin_auth_userpass.credentials.credential_tuple
    assert ("custom", "ssh-rsa AAAA...", "-----BEGIN RSA PRIVATE KEY-----...", "") == plugin_auth_key_user.credentials.credential_tuple
    assert ("git", "/root/id_rsa.pub", "/root/id_rsa", "optional-passphrase") == plugin_auth_key_pass.credentials.credential_tuple
    assert ("git", None, None, None) == plugin_auth_agent.credentials.credential_tuple
    assert ("custom", None, None, None) == plugin_auth_agent_user.credentials.credential_tuple


class MockPlugin(Plugin):
    def init(self):
        plugin_name = os.path.basename(os.path.normpath(self.filepath))
        self.files = ["manifest.json", "index.html"]
        self.config = {"name": plugin_name, "version": "1.0.0", "entrypoint": "index.html"}
        return self

    def checkout(self):
        pass

    def get_file(self, filename):
        _files = {
            "manifest.json": json.dumps(self.config),
            "index.html": "<h1>Hello from {}</h1>".format(self.config.get("name", self.identifier))
        }
        return _files.get(filename, None)

    def serve(self, filename):
        _file = self.get_file(filename)
        if _file:
            return web.Response(status=200, body=_file)
        return web.Response(status=404, body="File not found")
