import asyncio
import inspect
import json
import os
import pathlib
import subprocess
import sys
import textwrap

import pytest
from aiohttp import web

from services.data import models as metadata_models
from services.openapi import generator
from services.utils import openapi_components as components
from services.ui_backend_service.data.db import models as ui_models

pytestmark = [pytest.mark.unit_tests]

SERVICES = sorted(generator.SERVICES)

# Keywords that are not valid Swagger 2.0 (OpenAPI 3 or JSON schema only).
FORBIDDEN_KEYWORDS = {
    "anyOf",
    "oneOf",
    "not",
    "nullable",
    "style",
    "explode",
    "allowReserved",
}

# (row class, definition name, keys only serialized in some cases)
SERIALIZER_MODELS = {
    "metadata": [
        (metadata_models.FlowRow, "Flow", set()),
        # status/user are only set when fetched with the status join or the
        # filter select columns.
        (metadata_models.RunRow, "Run", {"status", "user"}),
        (metadata_models.StepRow, "Step", set()),
        (metadata_models.TaskRow, "Task", set()),
        (metadata_models.MetadataRow, "Metadata", set()),
        (metadata_models.ArtifactRow, "Artifact", set()),
    ],
    "ui": [
        (ui_models.FlowRow, "ModelsFlow", set()),
        (ui_models.RunRow, "ModelsRun", set()),
        (ui_models.StepRow, "ModelsStep", set()),
        # foreach_label/content are added by the refiners with postprocess=true.
        (ui_models.TaskRow, "ModelsTask", {"foreach_label"}),
        (ui_models.MetadataRow, "ModelsMetadata", set()),
        (ui_models.ArtifactRow, "ModelsArtifact", {"content"}),
    ],
}


@pytest.fixture(autouse=True)
def no_db_env(monkeypatch):
    """The generator must not need any database configuration."""
    for key in (
        "MF_METADATA_DB_HOST",
        "MF_METADATA_DB_PORT",
        "MF_METADATA_DB_USER",
        "MF_METADATA_DB_PSWD",
        "MF_METADATA_DB_NAME",
        "MF_METADATA_DB_DSN",
    ):
        monkeypatch.delenv(key, raising=False)


def _generate(service):
    spec, undocumented, errors = generator.generate(service)
    assert errors == [], "\n".join(errors)
    return spec, undocumented


def _referenced(spec):
    """Component refs used by paths or by *other* components."""
    refs = set(generator.iter_refs(spec["paths"]))
    for kind in ("definitions", "parameters"):
        for name, component in spec[kind].items():
            own = "#/{}/{}".format(kind, name)
            refs |= {r for r in generator.iter_refs(component) if r != own}
    return refs


def _placeholder_row(cls):
    """Instantiate a row class with placeholders for its required arguments."""
    params = inspect.signature(cls.__init__).parameters.values()
    args = {
        p.name: "x"
        for p in params
        if p.name != "self"
        and p.default is inspect.Parameter.empty
        and p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
    }
    return cls(**args)


@pytest.mark.parametrize("service", SERVICES)
def test_spec_builds_without_db_and_validates(service):
    spec, _ = _generate(service)
    assert spec["swagger"] == "2.0"
    assert spec["basePath"] == "/"
    assert spec["paths"]
    # Round trips as JSON.
    json.loads(json.dumps(spec))


@pytest.mark.parametrize("service", SERVICES)
def test_all_routes_documented(service):
    spec, undocumented = _generate(service)
    assert undocumented == []

    registered = {
        (method.lower(), path)
        for method, path, _ in generator.documentable_routes(
            generator.collect_routes(service)
        )
    }
    documented = {
        (method, path)
        for path, item in spec["paths"].items()
        for method in item
        if method in generator.HTTP_METHODS
    }
    assert documented == registered
    for path, item in spec["paths"].items():
        for operation in item.values():
            assert operation.get("summary") != generator.UNDOCUMENTED_SUMMARY, path


def _aiohttp_routes(service):
    """
    Routes as registered by actually running the service's register_routes()
    on a bare aiohttp application. Needs the service dependencies; the
    generator itself reads the routes statically instead.
    """
    from unittest.mock import MagicMock

    from pyee import AsyncIOEventEmitter

    if service == "metadata":
        from services.metadata_service.server import register_routes

        args = ()
    elif service == "ui":
        from services.ui_backend_service.ui_server import register_routes

        args = (MagicMock(), MagicMock(), AsyncIOEventEmitter())
    else:
        from services.migration_service.migration_server import register_routes

        args = ()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # Enables the optional endpoints, which the static reader always includes.
    previous = os.environ.get("MF_MIGRATION_ENDPOINTS_ENABLED")
    os.environ["MF_MIGRATION_ENDPOINTS_ENABLED"] = "1"
    try:
        app = web.Application()
        register_routes(app, *args)
        return [
            (route.method, route.resource.canonical, route.handler)
            for route in app.router.routes()
        ]
    finally:
        if previous is None:
            os.environ.pop("MF_MIGRATION_ENDPOINTS_ENABLED")
        else:
            os.environ["MF_MIGRATION_ENDPOINTS_ENABLED"] = previous
        pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()
        asyncio.set_event_loop(None)


@pytest.mark.parametrize("service", SERVICES)
def test_static_routes_match_aiohttp(service):
    """The source reader finds exactly what aiohttp registers, in order."""

    def describe(routes):
        return [
            (method, path, handler.name, handler.operation_id, handler.doc)
            for method, path, handler in generator.documentable_routes(routes)
        ]

    static = describe(generator.collect_routes(service))
    live = describe(_aiohttp_routes(service))
    assert static == live


def test_service_version_matches_installed_package():
    from services.utils import METADATA_SERVICE_VERSION

    assert generator.service_version() == str(METADATA_SERVICE_VERSION)


def test_ui_spec_excludes_metadata_ws_and_frontend():
    spec, _ = _generate("ui")
    for path in spec["paths"]:
        assert not path.startswith("/metadata")
        assert path != "/ws"
        assert "{tail" not in path


def test_migration_spec_includes_upgrade(monkeypatch):
    monkeypatch.setenv("MF_MIGRATION_ENDPOINTS_ENABLED", "0")
    spec, _ = _generate("migration")
    assert "patch" in spec["paths"]["/upgrade"]


@pytest.mark.parametrize("service", SERVICES)
def test_no_invalid_swagger2_keywords(service):
    spec, _ = _generate(service)
    # Keys of these maps are names (property names, status codes, ...), not keywords.
    name_maps = {"properties", "definitions", "parameters", "paths", "headers"}

    def check(node, parent_key=None):
        if isinstance(node, dict):
            if parent_key not in name_maps:
                bad = FORBIDDEN_KEYWORDS & set(node)
                assert not bad, (bad, node)
                if "$ref" in node:
                    assert set(node) == {"$ref"}, node
            for key, value in node.items():
                check(value, key)
        elif isinstance(node, list):
            for item in node:
                check(item, None)

    check(spec)


@pytest.mark.parametrize("service", SERVICES)
def test_service_components_are_used(service):
    spec, _ = _generate(service)
    refs = _referenced(spec)
    doc = generator.load_doc_module(service)
    if doc is None:
        return
    for kind in ("definitions", "parameters"):
        for name in getattr(doc, kind):
            assert "#/{}/{}".format(kind, name) in refs, "unused {} {}".format(
                kind, name
            )
    # Pruning: every component in the output is referenced.
    for kind in ("definitions", "parameters"):
        for name in spec[kind]:
            assert "#/{}/{}".format(kind, name) in refs, name


def test_shared_components_are_used():
    refs = set()
    for service in SERVICES:
        spec, _ = _generate(service)
        refs |= _referenced(spec)
    for name in components.SHARED_DEFINITIONS:
        assert "#/definitions/{}".format(name) in refs, name
    for name in components.SHARED_PARAMETERS:
        assert "#/parameters/{}".format(name) in refs, name


@pytest.mark.parametrize(
    "service,row_cls,definition,conditional",
    [
        (service, row_cls, definition, conditional)
        for service, pairs in SERIALIZER_MODELS.items()
        for row_cls, definition, conditional in pairs
    ],
    ids=lambda v: getattr(v, "__name__", None) if isinstance(v, type) else None,
)
def test_models_match_serializers(service, row_cls, definition, conditional):
    spec, _ = _generate(service)
    schema = spec["definitions"][definition]
    properties = set(schema["properties"])
    serialized = set(_placeholder_row(row_cls).serialize().keys())

    assert conditional <= properties, "conditional keys missing from schema"
    assert serialized - properties == set(), "schema lacks serialized keys"
    assert properties - serialized - conditional == set(), "schema has extra keys"


def test_malformed_docstring_names_handler():
    class FakeApi:
        async def broken(self, request):
            """
            ---
            description: [unclosed
            """

    with pytest.raises(generator.DocstringError) as err:
        generator.parse_docstring(FakeApi().broken)
    assert "openapi_test.FakeApi.broken" in str(err.value)


def test_parse_docstring_summary_and_undocumented():
    class FakeApi:
        async def summary_only(self, request):
            """
            Short summary.
            ---
            responses:
                "200":
                    description: ok
            """

        async def no_yaml(self, request):
            "Just text, no separator."

        async def empty_yaml(self, request):
            """
            Text
            ---
            """

    api = FakeApi()
    assert generator.parse_docstring(api.summary_only)["summary"] == "Short summary."
    assert generator.parse_docstring(api.no_yaml) is None
    assert generator.parse_docstring(api.empty_yaml) is None


def test_undocumented_route_gets_stub_and_fails_strict(tmp_path, monkeypatch):
    class FakeApi:
        async def nodoc(self, request):
            pass

    def fake_collect(service):
        return [("GET", "/things/{thing_id}", FakeApi().nodoc)]

    monkeypatch.setattr(generator, "collect_routes", fake_collect)
    spec, undocumented, errors = generator.generate("migration")
    assert errors == []
    assert len(undocumented) == 1 and "FakeApi.nodoc" in undocumented[0]
    stub = spec["paths"]["/things/{thing_id}"]["get"]
    assert stub["summary"] == generator.UNDOCUMENTED_SUMMARY

    out = tmp_path / "docs"
    assert generator.main(["--service", "migration", "--out", str(out)]) == 0
    assert (out / "migration.json").exists()
    assert (
        generator.main(["--service", "migration", "--out", str(out), "--strict"]) == 1
    )


def test_duplicate_operation_id_fails(monkeypatch):
    class FakeApi:
        async def handler(self, request):
            """
            ---
            responses:
                "200":
                    description: ok
            """

    def fake_collect(service):
        api = FakeApi()
        return [("GET", "/a", api.handler), ("GET", "/b", api.handler)]

    monkeypatch.setattr(generator, "collect_routes", fake_collect)
    with pytest.raises(generator.SpecError, match="Duplicate operationId"):
        generator.generate("migration")


def test_cli_writes_all_specs(tmp_path):
    assert generator.main(["--service", "all", "--out", str(tmp_path), "--strict"]) == 0
    for service in SERVICES:
        spec = json.loads((tmp_path / "{}.json".format(service)).read_text())
        assert spec["swagger"] == "2.0"


# --------------------------------------------------------------------------
# Production code must not depend on the dev only generator
# --------------------------------------------------------------------------

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]

# Packages that are not shipped in the service images.
DEV_ONLY_MODULES = ("services.openapi", "yaml", "openapi_spec_validator")

PRODUCTION_MODULES = (
    "services.metadata_service.server",
    "services.ui_backend_service.ui_server",
    "services.migration_service.migration_server",
    "services.metadata_service.doc",
    "services.ui_backend_service.doc",
    "services.utils.openapi_components",
)

_IMPORT_GUARD_SCRIPT = textwrap.dedent("""
    import importlib
    import sys

    BLOCKED = {blocked!r}

    class BlockDevOnly:
        def find_spec(self, name, path=None, target=None):
            for blocked in BLOCKED:
                if name == blocked or name.startswith(blocked + "."):
                    raise ImportError(
                        "dev only module imported by production code: " + name
                    )
            return None

    sys.meta_path.insert(0, BlockDevOnly())
    for module in {modules!r}:
        importlib.import_module(module)
    """)


def test_production_modules_do_not_import_dev_only_packages():
    # A subprocess, so modules already cached by this test session (the
    # generator, yaml, ...) cannot hide a dev only import.
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("MF_METADATA_DB_")
    }
    script = _IMPORT_GUARD_SCRIPT.format(
        blocked=DEV_ONLY_MODULES, modules=PRODUCTION_MODULES
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr


# The docs command must run with only requirements.docs.txt installed.
SERVICE_DEPENDENCIES = (
    "aiohttp",
    "aiopg",
    "psycopg2",
    "multidict",
    "yarl",
    "pyee",
    "metaflow",
    "packaging",
    # Packages whose __init__ (or modules) need the dependencies above.
    "services.data",
    "services.metadata_service.api",
    "services.ui_backend_service.api",
    "services.migration_service.api",
)
# Blocked by exact name only: the generator may load the stdlib only
# services.utils.openapi_components, but never run services/utils/__init__.py.
SERVICE_PACKAGES_EXACT = ("services.utils",)

_CLI_GUARD_SCRIPT = textwrap.dedent("""
    import runpy
    import sys

    PREFIXES = {prefixes!r}
    EXACT = {exact!r}

    class BlockServiceDependencies:
        def find_spec(self, name, path=None, target=None):
            if name in EXACT or any(
                name == p or name.startswith(p + ".") for p in PREFIXES
            ):
                raise ImportError("docs generator imported " + name)
            return None

    sys.meta_path.insert(0, BlockServiceDependencies())
    sys.argv = ["services.openapi", "--service", "all", "--out", {out!r}, "--strict"]
    runpy.run_module("services.openapi", run_name="__main__", alter_sys=True)
    """)


def test_cli_runs_without_service_dependencies(tmp_path):
    script = _CLI_GUARD_SCRIPT.format(
        prefixes=SERVICE_DEPENDENCIES,
        exact=SERVICE_PACKAGES_EXACT,
        out=str(tmp_path),
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    for service in SERVICES:
        written = json.loads((tmp_path / "{}.json".format(service)).read_text())
        spec, _ = _generate(service)
        assert written == spec, service


def test_docs_requirements_match_dev_requirements():
    # requirements.dev.txt cannot -r include the docs file (setup.py reads its
    # lines as extras), so the pins are duplicated and must stay equal.
    def lines(name):
        text = (REPO_ROOT / name).read_text()
        return {line.strip() for line in text.splitlines() if line.strip()}

    docs = lines("requirements.docs.txt")
    assert docs and docs <= lines("requirements.dev.txt")


# --------------------------------------------------------------------------
# Static route reader, on a small fake repository
# --------------------------------------------------------------------------

_FAKE_API = '''
import os


def documented(func):
    return func


def handler_function(request):
    pass


def register_more(app):
    pass


class ThingsApi:
    def __init__(self, app, db=None):
{init}

    @documented
    async def list_things(self, request):
        """
        List things.
        ---
        responses:
            "200":
                description: ok
        """

    async def get_thing(self, request):
        """Get a thing."""

    async def delete_thing(self, request):
        pass

    async def start(self, app):
        pass
'''

_FAKE_SERVER = '''
from .api import ThingsApi


def helper(app):
    pass


class LocalApi:
    def __init__(self, app):
        app.router.add_route("GET", "/local", self.local)

    async def local(self, request):
        """Local."""


def register_routes(app, db):
{body}
'''

_DEFAULT_INIT = """
        app.router.add_route("GET", "/things", self.list_things)
        app.router.add_get("/things/{name:[a-z]+}", self.get_thing)
        if os.environ.get("OPTIONAL_ENDPOINTS"):
            app.router.add_route(
                method="delete", path="/things/{name}", handler=self.delete_thing
            )
        app.on_startup.append(self.start)
"""

_DEFAULT_BODY = """
    print("not a registration")
    ThingsApi(app, db)
    LocalApi(app)
"""


def _fake_repo(tmp_path, monkeypatch, init=_DEFAULT_INIT, body=_DEFAULT_BODY):
    package = tmp_path / "fakesvc"
    (package / "api").mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "api" / "__init__.py").write_text("from .things import ThingsApi\n")
    (package / "api" / "things.py").write_text(
        _FAKE_API.format(init=textwrap.indent(textwrap.dedent(init), " " * 8))
    )
    (package / "server.py").write_text(
        _FAKE_SERVER.format(body=textwrap.indent(textwrap.dedent(body), " " * 4))
    )
    monkeypatch.setattr(generator, "REPO_ROOT", tmp_path)
    monkeypatch.setitem(
        generator.SERVICES,
        "fake",
        {"title": "Fake", "server_module": "fakesvc.server", "doc_module": None},
    )


def test_static_reader_supported_patterns(tmp_path, monkeypatch):
    _fake_repo(tmp_path, monkeypatch)
    routes = generator.collect_routes("fake")
    assert [(m, p, h.name, h.operation_id) for m, p, h in routes] == [
        (
            "GET",
            "/things",
            "fakesvc.api.things.ThingsApi.list_things",
            "ThingsApi.list_things",
        ),
        # Regex patterns are dropped like aiohttp's resource.canonical.
        (
            "GET",
            "/things/{name}",
            "fakesvc.api.things.ThingsApi.get_thing",
            "ThingsApi.get_thing",
        ),
        # Optional endpoints are always included.
        (
            "DELETE",
            "/things/{name}",
            "fakesvc.api.things.ThingsApi.delete_thing",
            "ThingsApi.delete_thing",
        ),
        ("GET", "/local", "fakesvc.server.LocalApi.local", "LocalApi.local"),
    ]
    docs = [h.doc for _, _, h in routes]
    assert docs[0].startswith("List things.\n---\nresponses:")
    assert docs[1:] == ["Get a thing.", None, "Local."]


@pytest.mark.parametrize(
    "init,body,message,where",
    [
        (
            'for path in ("/a", "/b"):\n'
            "    app.router.add_route('GET', path, self.get_thing)",
            _DEFAULT_BODY,
            "loop",
            "fakesvc/api/things.py:",
        ),
        (
            'app.router.add_route("GET", "/" + os.sep, self.get_thing)',
            _DEFAULT_BODY,
            "string literal",
            "fakesvc/api/things.py:",
        ),
        (
            'app.router.add_route("GET", "/a", handler_function)',
            _DEFAULT_BODY,
            "must be a method",
            "fakesvc/api/things.py:",
        ),
        (
            "register_more(app)",
            _DEFAULT_BODY,
            "unsupported registration",
            "fakesvc/api/things.py:",
        ),
        (
            'app.router.add_static("/static", "dir")',
            _DEFAULT_BODY,
            "not supported",
            "fakesvc/api/things.py:",
        ),
        (
            _DEFAULT_INIT,
            'app.router.add_route("GET", "/a", None)',
            "API class",
            "fakesvc/server.py:",
        ),
        (_DEFAULT_INIT, "helper(app)", "is a function", "fakesvc/server.py:"),
        (_DEFAULT_INIT, "MissingApi(app)", "cannot find", "fakesvc/server.py:"),
    ],
    ids=[
        "loop",
        "non-literal-path",
        "foreign-handler",
        "helper-in-init",
        "unsupported-router-method",
        "router-in-register-routes",
        "helper-function",
        "unknown-class",
    ],
)
def test_static_reader_rejects_unsupported_patterns(
    tmp_path, monkeypatch, init, body, message, where
):
    _fake_repo(tmp_path, monkeypatch, init=init, body=body)
    with pytest.raises(generator.RouteParseError, match=message) as err:
        generator.collect_routes("fake")
    assert str(err.value).startswith(where), str(err.value)


# --------------------------------------------------------------------------
# Route duplicates, shadowing and per service isolation
# --------------------------------------------------------------------------


class _RouteApi:
    """Documented handlers with distinct names (and so distinct operationIds)."""

    async def get_thing(self, request):
        """
        ---
        parameters:
          - {name: a, in: path, required: true, type: string}
        responses:
            "200":
                description: ok
        """

    async def get_thing_b(self, request):
        """
        ---
        parameters:
          - {name: b, in: path, required: true, type: string}
        responses:
            "200":
                description: ok
        """

    async def get_thing_again(self, request):
        """
        ---
        parameters:
          - {name: a, in: path, required: true, type: string}
        responses:
            "200":
                description: ok
        """

    async def post_thing(self, request):
        """
        ---
        parameters:
          - {name: a, in: path, required: true, type: string}
        responses:
            "200":
                description: ok
        """

    async def get_latest(self, request):
        """
        ---
        responses:
            "200":
                description: ok
        """

    async def get_other(self, request):
        """
        ---
        responses:
            "200":
                description: ok
        """


def _fake_collect(monkeypatch, routes):
    api = _RouteApi()

    def fake_collect(service):
        return [
            (method, path, getattr(api, handler)) for method, path, handler in routes
        ]

    monkeypatch.setattr(generator, "collect_routes", fake_collect)


def test_shadowed_route_fails(monkeypatch):
    _fake_collect(
        monkeypatch,
        [
            ("GET", "/things/{a}", "get_thing"),
            ("GET", "/other", "get_other"),
            ("GET", "/things/{b}", "get_thing_b"),
        ],
    )
    with pytest.raises(generator.SpecError, match="shadowed") as err:
        generator.generate("migration")
    message = str(err.value)
    assert "GET /things/{b}" in message and "_RouteApi.get_thing_b" in message
    assert "GET /things/{a} (" in message and "_RouteApi.get_thing)" in message


def test_exact_duplicate_route_fails(monkeypatch):
    # Not consecutive, so aiohttp would accept it and keep two resources.
    _fake_collect(
        monkeypatch,
        [
            ("GET", "/things/{a}", "get_thing"),
            ("GET", "/other", "get_other"),
            ("GET", "/things/{a}", "get_thing_again"),
        ],
    )
    with pytest.raises(generator.SpecError, match="Duplicate route") as err:
        generator.generate("migration")
    message = str(err.value)
    assert "_RouteApi.get_thing_again" in message
    assert "_RouteApi.get_thing)" in message


def test_distinct_routes_are_not_duplicates(monkeypatch):
    _fake_collect(
        monkeypatch,
        [
            ("GET", "/things/{a}", "get_thing"),
            ("POST", "/things/{a}", "post_thing"),
            ("GET", "/things/latest", "get_latest"),
        ],
    )
    spec, undocumented, errors = generator.generate("migration")
    assert errors == [] and undocumented == []
    assert set(spec["paths"]["/things/{a}"]) == {"get", "post"}
    assert set(spec["paths"]["/things/latest"]) == {"get"}


def test_specs_are_isolated_per_service():
    def build_all(order):
        specs = {}
        for service in order:
            routes = generator.collect_routes(service)
            spec, _ = generator.build_spec(routes, service)
            expected = {
                (method.lower(), path)
                for method, path, _ in generator.documentable_routes(
                    generator.collect_routes(service)
                )
            }
            actual = {
                (method, path)
                for path, item in spec["paths"].items()
                for method in item
                if method in generator.HTTP_METHODS
            }
            assert actual == expected, service
            specs[service] = spec
        return specs

    order = ["metadata", "ui", "migration"]
    forward = build_all(order)
    backward = build_all(list(reversed(order)))
    for service in order:
        assert forward[service] == backward[service], service
