"""
Generate validated Swagger 2.0 specs from the handler docstrings.

Handlers are documented aiohttp-swagger style: optional free text, a line that
is exactly ``---``, then a Swagger 2.0 operation object in YAML. Routes are
collected by calling each service's ``register_routes`` on a bare
``web.Application``; no database connection or running server is needed.

Usage::

    python -m services.openapi --service {metadata,ui,migration,all} \\
        --out ./api-docs [--strict]
"""

import argparse
import asyncio
import copy
import importlib
import inspect
import json
import os
import re
import sys
import warnings
from contextlib import contextmanager
from unittest.mock import MagicMock

from aiohttp import web

try:
    import yaml
except ImportError:  # pragma: no cover - reported by main()
    yaml = None

from services.utils import openapi_components as components

DEV_DEPS_HINT = (
    "Missing dev dependencies (pyyaml, openapi-spec-validator). "
    "Run: pip install -r requirements.dev.txt"
)

UNDOCUMENTED_SUMMARY = "Undocumented"

SERVICES = {
    "metadata": {
        "title": "Metaflow Metadata Service",
        "doc_module": "services.metadata_service.doc",
    },
    "ui": {
        "title": "Metaflow UI Backend Service",
        "doc_module": "services.ui_backend_service.doc",
    },
    "migration": {
        "title": "Metaflow Migration Service",
        "doc_module": None,
        "description": "Metaflow migration service. Reports and upgrades the "
        "database schema version.",
    },
}

# Routes that are never part of a spec. register_routes() does not add these;
# the exclusions guard against them being moved there by accident.
EXCLUDED_PATHS = {"/ws"}
EXCLUDED_PREFIXES = ("/metadata/", "/static/")
SKIPPED_METHODS = {"HEAD", "OPTIONS"}

HTTP_METHODS = {"get", "put", "post", "delete", "options", "head", "patch"}


class SpecError(Exception):
    """Raised when a spec cannot be built (bad docstring, duplicates, ...)."""


class DocstringError(SpecError):
    pass


# --------------------------------------------------------------------------
# Route collection
# --------------------------------------------------------------------------


@contextmanager
def _env(**values):
    old = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, value in old.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _register(service: str, app: web.Application):
    # Server modules are imported lazily to avoid import cycles with
    # services.utils and to only pay for the service being generated.
    if service == "metadata":
        from services.metadata_service.server import register_routes

        register_routes(app)
    elif service == "ui":
        from pyee import AsyncIOEventEmitter
        from services.ui_backend_service.ui_server import register_routes

        register_routes(app, MagicMock(), MagicMock(), AsyncIOEventEmitter())
    elif service == "migration":
        from services.migration_service.migration_server import register_routes

        register_routes(app)
    else:
        raise SpecError("Unknown service: {}".format(service))


def collect_routes(service: str) -> web.Application:
    """
    Return a bare application with all documented routes of ``service``
    registered. Background tasks scheduled by API constructors are cancelled.
    """
    if service not in SERVICES:
        raise SpecError("Unknown service: {}".format(service))

    previous_loop = None
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            previous_loop = asyncio.get_event_loop_policy().get_event_loop()
    except RuntimeError:
        pass

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        with _env(MF_MIGRATION_ENDPOINTS_ENABLED="1"):
            app = web.Application()
            _register(service, app)
    finally:
        pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()
        asyncio.set_event_loop(previous_loop)
    return app


def _is_excluded(path: str) -> bool:
    if path in EXCLUDED_PATHS or "{tail" in path:
        return True
    return path == "/metadata" or path.startswith(EXCLUDED_PREFIXES)


def documentable_routes(app: web.Application):
    """(method, path, handler) for every route that belongs in the spec."""
    routes = []
    for route in app.router.routes():
        method = route.method.upper()
        if method in SKIPPED_METHODS or method == "*":
            continue
        path = route.resource.canonical if route.resource else None
        if not path or _is_excluded(path):
            continue
        routes.append((method, path, route.handler))
    return routes


# --------------------------------------------------------------------------
# Docstrings
# --------------------------------------------------------------------------


def handler_name(handler) -> str:
    """Fully qualified ``module.ClassName.method`` of a (bound) handler."""
    func = getattr(handler, "__func__", handler)
    module = getattr(func, "__module__", "?")
    owner = getattr(handler, "__self__", None)
    if owner is not None:
        return "{}.{}.{}".format(module, type(owner).__name__, func.__name__)
    return "{}.{}".format(module, getattr(func, "__qualname__", repr(func)))


def default_operation_id(handler) -> str:
    func = getattr(handler, "__func__", handler)
    owner = getattr(handler, "__self__", None)
    if owner is not None:
        return "{}.{}".format(type(owner).__name__, func.__name__)
    return getattr(func, "__qualname__", repr(func))


def parse_docstring(handler):
    """
    Parse the Swagger operation from a handler docstring.

    Returns the operation dict, or None if the handler is undocumented (no
    docstring, no ``---`` line, or an empty YAML block). Raises DocstringError
    naming the handler if the YAML is invalid.
    """
    if yaml is None:
        raise SpecError(DEV_DEPS_HINT)

    doc = getattr(handler, "__doc__", None)
    if not doc:
        return None
    lines = inspect.cleandoc(doc).splitlines()
    try:
        split = next(i for i, line in enumerate(lines) if line == "---")
    except StopIteration:
        return None

    text = "\n".join(lines[:split]).strip()
    block = "\n".join(lines[split + 1 :])
    try:
        operation = yaml.safe_load(block)
    except yaml.YAMLError as ex:
        raise DocstringError(
            "Invalid YAML in docstring of {}: {}".format(handler_name(handler), ex)
        ) from ex

    if operation is None:
        return None
    if not isinstance(operation, dict):
        raise DocstringError(
            "Docstring YAML of {} must be a mapping, got {}".format(
                handler_name(handler), type(operation).__name__
            )
        )
    if text and "description" not in operation and "summary" not in operation:
        operation["summary"] = text
    return operation


def _stub_operation(path: str) -> dict:
    params = [
        components.path_param(segment[1:-1], segment[1:-1])
        for segment in path.split("/")
        if segment.startswith("{") and segment.endswith("}")
    ]
    operation = {
        "summary": UNDOCUMENTED_SUMMARY,
        "responses": {"default": {"description": UNDOCUMENTED_SUMMARY}},
    }
    if params:
        operation["parameters"] = params
    return operation


# --------------------------------------------------------------------------
# Spec assembly
# --------------------------------------------------------------------------


def iter_refs(node):
    """Yield every ``$ref`` value in a nested structure."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "$ref" and isinstance(value, str):
                yield value
            else:
                yield from iter_refs(value)
    elif isinstance(node, list):
        for item in node:
            yield from iter_refs(item)


def _split_ref(ref: str):
    parts = ref.split("/")
    if len(parts) == 3 and parts[0] == "#":
        return parts[1], parts[2]
    return None, None


def _service_components(service: str):
    conf = SERVICES[service]
    if not conf["doc_module"]:
        return conf.get("description", ""), {}, {}
    module = importlib.import_module(conf["doc_module"])
    return module.description, module.definitions, module.parameters


def _merge_components(service, paths, definitions, parameters):
    """
    Merge service specific components with the shared ones. Shared components
    are only included when (transitively) referenced, so specs carry no unused
    shared components. Duplicate keys are an error.
    """
    for kind, own, shared in (
        ("definitions", definitions, components.SHARED_DEFINITIONS),
        ("parameters", parameters, components.SHARED_PARAMETERS),
    ):
        clash = set(own) & set(shared)
        if clash:
            raise SpecError(
                "{} {} of service '{}' clash with shared components".format(
                    kind, sorted(clash), service
                )
            )

    merged = {
        "definitions": copy.deepcopy(definitions),
        "parameters": copy.deepcopy(parameters),
    }
    shared = {
        "definitions": components.SHARED_DEFINITIONS,
        "parameters": components.SHARED_PARAMETERS,
    }
    todo = list(iter_refs([paths, merged["definitions"], merged["parameters"]]))
    while todo:
        kind, name = _split_ref(todo.pop())
        if kind in shared and name in shared[kind] and name not in merged[kind]:
            merged[kind][name] = copy.deepcopy(shared[kind][name])
            todo.extend(iter_refs(merged[kind][name]))
    return merged["definitions"], merged["parameters"]


def build_spec(app: web.Application, service: str, warn=None):
    """
    Build the Swagger 2.0 spec for the routes registered on ``app``.

    Returns ``(spec, undocumented)`` where ``undocumented`` lists
    ``"METHOD path (handler)"`` entries that got a stub operation.
    """
    from services.utils import METADATA_SERVICE_VERSION

    paths = {}
    operation_ids = {}
    # (method, path with placeholders blanked) -> (path, "METHOD path (handler)").
    # aiohttp keeps every resource and dispatches to the first match, so a
    # later /a/{y} next to /a/{x} is dead at runtime and must not be documented.
    route_keys = {}
    undocumented = []

    for method, path, handler in documentable_routes(app):
        name = handler_name(handler)

        key = (method, re.sub(r"\{[^}]+\}", "{}", path))
        if key in route_keys:
            other_path, other = route_keys[key]
            if other_path == path:
                raise SpecError(
                    "Duplicate route {} {}: {} and {}".format(method, path, name, other)
                )
            raise SpecError(
                "Route {} {} ({}) is shadowed by {}".format(method, path, name, other)
            )
        route_keys[key] = (path, "{} {} ({})".format(method, path, name))

        operation = parse_docstring(handler)
        if operation is None:
            undocumented.append("{} {} ({})".format(method, path, name))
            if warn:
                warn(
                    "WARNING: undocumented route {} {} ({})".format(method, path, name)
                )
            operation = _stub_operation(path)

        operation.setdefault("operationId", default_operation_id(handler))
        op_id = operation["operationId"]
        if op_id in operation_ids:
            raise SpecError(
                "Duplicate operationId '{}' for {} {} and {}".format(
                    op_id, method, path, operation_ids[op_id]
                )
            )
        operation_ids[op_id] = "{} {}".format(method, path)

        path_item = paths.setdefault(path, {})
        path_item[method.lower()] = operation

    description, definitions, parameters = _service_components(service)
    definitions, parameters = _merge_components(service, paths, definitions, parameters)

    spec = {
        "swagger": "2.0",
        "info": {
            "title": SERVICES[service]["title"],
            "version": str(METADATA_SERVICE_VERSION),
            "description": description,
        },
        "basePath": "/",
        "consumes": ["application/json"],
        "produces": ["application/json"],
        "paths": paths,
        "definitions": definitions,
        "parameters": parameters,
    }
    return spec, undocumented


# --------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------


def _check_refs(spec) -> list:
    """Local checks: every $ref resolves and no $ref has siblings."""
    errors = []

    def walk(node, where):
        if isinstance(node, dict):
            if "$ref" in node:
                extra = sorted(k for k in node if k != "$ref")
                if extra:
                    errors.append(
                        "{}: $ref has sibling keys {}".format(where or "/", extra)
                    )
                kind, name = _split_ref(node["$ref"])
                if kind not in ("definitions", "parameters") or name not in spec.get(
                    kind, {}
                ):
                    errors.append(
                        "{}: unresolvable $ref '{}'".format(where or "/", node["$ref"])
                    )
            for key, value in node.items():
                if key != "$ref":
                    walk(value, "{}/{}".format(where, key))
        elif isinstance(node, list):
            for i, item in enumerate(node):
                walk(item, "{}/{}".format(where, i))

    walk(spec, "")
    return errors


def validate(spec) -> list:
    """Validate against Swagger 2.0. Returns a list of error messages."""
    try:
        from openapi_spec_validator import OpenAPIV2SpecValidator
    except ImportError as ex:
        raise SpecError(DEV_DEPS_HINT) from ex

    errors = _check_refs(spec)
    if errors:
        # The validator raises on unresolvable refs; report ours instead.
        return errors
    try:
        for error in OpenAPIV2SpecValidator(spec).iter_errors():
            location = "/".join(str(p) for p in getattr(error, "path", []))
            errors.append(
                "{}{}".format("{}: ".format(location) if location else "", error)
            )
    except Exception as ex:  # validator internals raise on some broken specs
        errors.append("{}: {}".format(type(ex).__name__, ex))
    return errors


def generate(service: str, warn=None):
    """Collect, build and validate. Returns ``(spec, undocumented, errors)``."""
    app = collect_routes(service)
    spec, undocumented = build_spec(app, service, warn=warn)
    return spec, undocumented, validate(spec)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _err(msg):
    print(msg, file=sys.stderr)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m services.openapi",
        description="Generate validated Swagger 2.0 specs for the Metaflow services.",
    )
    parser.add_argument("--service", choices=sorted(SERVICES) + ["all"], default="all")
    parser.add_argument("--out", default="./api-docs", help="output directory")
    parser.add_argument(
        "--strict", action="store_true", help="fail on undocumented routes"
    )
    args = parser.parse_args(argv)

    if yaml is None:
        _err(DEV_DEPS_HINT)
        return 1
    try:
        import openapi_spec_validator  # noqa: F401
    except ImportError:
        _err(DEV_DEPS_HINT)
        return 1

    services = sorted(SERVICES) if args.service == "all" else [args.service]
    failed = False
    os.makedirs(args.out, exist_ok=True)
    for service in services:
        try:
            spec, undocumented, errors = generate(service, warn=_err)
        except SpecError as ex:
            _err("ERROR [{}]: {}".format(service, ex))
            failed = True
            continue

        if errors:
            failed = True
            _err("ERROR [{}]: spec failed validation:".format(service))
            for error in errors:
                _err("  - {}".format(error))
            continue
        if undocumented and args.strict:
            failed = True
            _err(
                "ERROR [{}]: {} undocumented route(s) (--strict)".format(
                    service, len(undocumented)
                )
            )
            continue

        out_file = os.path.join(args.out, "{}.json".format(service))
        with open(out_file, "w") as f:
            json.dump(spec, f, indent=2, sort_keys=True)
            f.write("\n")
        print(
            "Wrote {} ({} paths, {} undocumented)".format(
                out_file, len(spec["paths"]), len(undocumented)
            )
        )
    return 1 if failed else 0
