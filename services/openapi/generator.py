"""
Generate validated Swagger 2.0 specs from the handler docstrings.

Handlers are documented aiohttp-swagger style: optional free text, a line that
is exactly ``---``, then a Swagger 2.0 operation object in YAML.

Nothing from the services is imported or executed except plain data modules.
Routes and docstrings are read from the source with ``ast`` (see "Route
collection"), and the component modules (``services/utils/openapi_components.py``
and each service's ``doc.py``) are loaded by file path, so that
``services/utils/__init__.py`` never runs. The command therefore only needs
``pyyaml`` and ``openapi-spec-validator`` (``requirements.docs.txt``), not the
service dependencies (aiohttp, psycopg2, ...). No database or running server
is needed either.

Usage::

    python -m services.openapi --service {metadata,ui,migration,all} \\
        --out ./api-docs [--strict]
"""

import argparse
import ast
import copy
import importlib.metadata
import importlib.util
import inspect
import json
import os
import pathlib
import re
import sys
from typing import NamedTuple, Optional

try:
    import yaml
except ImportError:  # pragma: no cover - reported by main()
    yaml = None

DEV_DEPS_HINT = (
    "Missing dependencies (pyyaml, openapi-spec-validator). "
    "Run: pip install -r requirements.docs.txt"
)

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load_source_module(name: str, rel_path: str):
    """
    Import a plain data module from its file without importing its parent
    packages (whose ``__init__`` may need the service dependencies). Reuses
    the module if it is already imported, e.g. by the service itself.
    """
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / rel_path)
    module = importlib.util.module_from_spec(spec)
    # Registered before executing so that ``from <name> import ...`` in the
    # doc modules resolves to it instead of importing the parent package.
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        del sys.modules[name]
        raise
    return module


components = _load_source_module(
    "services.utils.openapi_components", "services/utils/openapi_components.py"
)

UNDOCUMENTED_SUMMARY = "Undocumented"

SERVICES = {
    "metadata": {
        "title": "Metaflow Metadata Service",
        "server_module": "services.metadata_service.server",
        "doc_module": "services.metadata_service.doc",
    },
    "ui": {
        "title": "Metaflow UI Backend Service",
        "server_module": "services.ui_backend_service.ui_server",
        "doc_module": "services.ui_backend_service.doc",
    },
    "migration": {
        "title": "Metaflow Migration Service",
        "server_module": "services.migration_service.migration_server",
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


class RouteParseError(SpecError):
    """A route registration the static collector cannot follow."""


# --------------------------------------------------------------------------
# Handlers and routes
# --------------------------------------------------------------------------


class Handler(NamedTuple):
    """What the generator needs to know about a route handler."""

    name: str  # module.ClassName.method, used in messages
    operation_id: str  # default operationId, ClassName.method
    doc: Optional[str]  # cleaned docstring

    @classmethod
    def from_callable(cls, handler) -> "Handler":
        """Describe a live (bound) handler, e.g. one registered on aiohttp."""
        func = getattr(handler, "__func__", handler)
        module = getattr(func, "__module__", "?")
        owner = getattr(handler, "__self__", None)
        if owner is not None:
            operation_id = "{}.{}".format(type(owner).__name__, func.__name__)
        else:
            operation_id = getattr(func, "__qualname__", repr(func))
        doc = getattr(handler, "__doc__", None)
        return cls(
            name="{}.{}".format(module, operation_id),
            operation_id=operation_id,
            doc=inspect.cleandoc(doc) if doc else None,
        )


class Route(NamedTuple):
    method: str  # upper case
    path: str  # canonical, i.e. without {name:regex} patterns
    handler: Handler


def _as_handler(handler) -> Handler:
    return handler if isinstance(handler, Handler) else Handler.from_callable(handler)


def handler_name(handler) -> str:
    """Fully qualified ``module.ClassName.method`` of a handler."""
    return _as_handler(handler).name


def default_operation_id(handler) -> str:
    return _as_handler(handler).operation_id


def canonical_path(path: str) -> str:
    """Path as aiohttp's ``resource.canonical``: ``{name:regex}`` -> ``{name}``."""
    return re.sub(r"\{([^}:]+):[^}]*\}", r"{\1}", path)


# --------------------------------------------------------------------------
# Route collection
#
# Each service's register_routes() is read from source instead of executed:
#
#     def register_routes(app, ...):
#         FlowApi(app)                      # class imported with from-import
#
#     class FlowApi:
#         def __init__(self, app):
#             app.router.add_route("GET", "/flows/{flow_id}", self.get_flow)
#
#         async def get_flow(self, request):
#             """... --- <swagger yaml>"""
#
# Supported: API classes defined in or imported (also re-exported through a
# package __init__) into the server module, literal method and path strings,
# and handlers that are methods of the registering class. Registrations behind
# an ``if`` are always included, like enabling every optional endpoint. Any
# other way to pass ``app`` along or to call the router raises
# RouteParseError naming the file and line, so routes are never skipped
# silently. A unit test compares the result with what aiohttp registers.
# --------------------------------------------------------------------------

_ROUTER_METHODS = {
    "add_route": None,
    "add_get": "GET",
    "add_post": "POST",
    "add_put": "PUT",
    "add_patch": "PATCH",
    "add_delete": "DELETE",
}
_LOOPS = (
    ast.For,
    ast.AsyncFor,
    ast.While,
    ast.ListComp,
    ast.SetComp,
    ast.DictComp,
    ast.GeneratorExp,
)
_SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)


class _SourceModule:
    """A parsed module of this repository."""

    def __init__(self, name: str, file: pathlib.Path):
        self.name = name
        self.file = file
        self.rel = file.relative_to(REPO_ROOT).as_posix()
        self.is_package = file.name == "__init__.py"
        self.tree = ast.parse(file.read_text(), filename=str(file))
        self.defs = {
            node.name: node
            for node in self.tree.body
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        }
        # local name -> (absolute module, attribute)
        self.imports = {}
        package = name if self.is_package else name.rpartition(".")[0]
        for node in self.tree.body:
            if not isinstance(node, ast.ImportFrom):
                continue
            if node.level:
                base = package.split(".")
                base = base[: len(base) - (node.level - 1)]
                module = ".".join(base + ([node.module] if node.module else []))
            else:
                module = node.module
            for alias in node.names:
                self.imports[alias.asname or alias.name] = (module, alias.name)

    def where(self, node) -> str:
        return "{}:{}".format(self.rel, node.lineno)


class _Collector:
    def __init__(self):
        self._modules = {}

    def module(self, name: str) -> Optional[_SourceModule]:
        """Parsed module, or None if it is not part of this repository."""
        if name not in self._modules:
            base = REPO_ROOT.joinpath(*name.split("."))
            for file in (base.with_suffix(".py"), base / "__init__.py"):
                if file.is_file():
                    self._modules[name] = _SourceModule(name, file)
                    break
            else:
                self._modules[name] = None
        return self._modules[name]

    def resolve_class(self, module: _SourceModule, name: str, where: str):
        """(module, ClassDef) that ``name`` refers to in ``module``, or None."""
        for _ in range(10):  # follows re-exports, bounded against cycles
            node = module.defs.get(name)
            if isinstance(node, ast.ClassDef):
                return module, node
            if node is not None:
                raise RouteParseError(
                    "{}: {} is a function; only API classes can register routes "
                    "in register_routes()".format(where, name)
                )
            if name not in module.imports:
                return None
            target, name = module.imports[name]
            module = self.module(target)
            if module is None:
                return None
        raise RouteParseError("{}: import cycle resolving {}".format(where, name))

    def service_routes(self, service: str) -> list:
        server = self.module(SERVICES[service]["server_module"])
        func = server.defs.get("register_routes")
        if not isinstance(func, ast.FunctionDef) or not func.args.args:
            raise RouteParseError(
                "{}: no register_routes(app, ...) function".format(server.rel)
            )
        app = func.args.args[0].arg
        routes = []
        for call, in_loop in _calls(func):
            where = server.where(call)
            if _is_router_call(call):
                raise RouteParseError(
                    "{}: register routes in an API class, not directly in "
                    "register_routes()".format(where)
                )
            if not _passes(call, app):
                continue
            if in_loop or not isinstance(call.func, ast.Name):
                raise RouteParseError(
                    "{}: unsupported registration {}; call each API class "
                    "directly".format(where, ast.unparse(call))
                )
            resolved = self.resolve_class(server, call.func.id, where)
            if resolved is None:
                raise RouteParseError(
                    "{}: cannot find the class {} in this repository".format(
                        where, call.func.id
                    )
                )
            routes.extend(self.class_routes(*resolved))
        return routes

    def class_routes(self, module: _SourceModule, cls: ast.ClassDef) -> list:
        methods = {
            node.name: node
            for node in cls.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        init = methods.get("__init__")
        if init is None or len(init.args.args) < 2:
            raise RouteParseError(
                "{}: {} must define __init__(self, app, ...)".format(
                    module.where(cls), cls.name
                )
            )
        app = init.args.args[1].arg
        routes = []
        for call, in_loop in _calls(init):
            where = module.where(call)
            if not _is_router_call(call):
                if _passes(call, app):
                    raise RouteParseError(
                        "{}: unsupported registration {}; call app.router.add_route "
                        "directly in __init__".format(where, ast.unparse(call))
                    )
                continue
            if in_loop:
                raise RouteParseError(
                    "{}: routes registered in a loop are not supported".format(where)
                )
            method, path, handler = _router_call_args(call, where)
            if not (
                isinstance(handler, ast.Attribute)
                and isinstance(handler.value, ast.Name)
                and handler.value.id == init.args.args[0].arg
                and handler.attr in methods
            ):
                raise RouteParseError(
                    "{}: the handler must be a method defined in {} (self.<name>), "
                    "got {}".format(where, cls.name, ast.unparse(handler))
                )
            func = methods[handler.attr]
            operation_id = "{}.{}".format(cls.name, func.name)
            routes.append(
                Route(
                    method,
                    canonical_path(path),
                    Handler(
                        name="{}.{}".format(module.name, operation_id),
                        operation_id=operation_id,
                        doc=ast.get_docstring(func),
                    ),
                )
            )
        return routes


def _calls(func: ast.AST) -> list:
    """
    ``(call, in_loop)`` for every call in ``func`` in source order, excluding
    nested functions and classes (they do not run at registration time).
    """
    found = []

    def visit(node, in_loop):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, _SCOPES):
                continue
            child_in_loop = in_loop or isinstance(child, _LOOPS)
            if isinstance(child, ast.Call):
                found.append((child, child_in_loop))
            visit(child, child_in_loop)

    visit(func, False)
    return sorted(found, key=lambda item: (item[0].lineno, item[0].col_offset))


def _passes(call: ast.Call, name: str) -> bool:
    values = list(call.args) + [kw.value for kw in call.keywords]
    return any(isinstance(v, ast.Name) and v.id == name for v in values)


def _is_router_call(call: ast.Call) -> bool:
    """``<x>.router.<method>(...)``"""
    func = call.func
    return (
        isinstance(func, ast.Attribute)
        and isinstance(func.value, ast.Attribute)
        and func.value.attr == "router"
    )


def _router_call_args(call: ast.Call, where: str):
    """(METHOD, path, handler node) of an ``app.router.add_*`` call."""
    attr = call.func.attr
    if attr not in _ROUTER_METHODS:
        raise RouteParseError(
            "{}: router.{} is not supported, use one of {}".format(
                where, attr, ", ".join(sorted(_ROUTER_METHODS))
            )
        )
    fixed_method = _ROUTER_METHODS[attr]
    names = ("path", "handler") if fixed_method else ("method", "path", "handler")
    keywords = {kw.arg: kw.value for kw in call.keywords}
    values = {
        name: call.args[i] if i < len(call.args) else keywords.get(name)
        for i, name in enumerate(names)
    }

    def literal(name):
        node = values[name]
        if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
            raise RouteParseError(
                "{}: the route {} must be a string literal".format(where, name)
            )
        return node.value

    method = fixed_method or literal("method").upper()
    if values["handler"] is None:
        raise RouteParseError("{}: missing route handler".format(where))
    return method, literal("path"), values["handler"]


def collect_routes(service: str) -> list:
    """
    All routes ``register_routes()`` of ``service`` registers, as ``Route``
    tuples in registration order. Reads the source; imports nothing.
    """
    if service not in SERVICES:
        raise SpecError("Unknown service: {}".format(service))
    return _Collector().service_routes(service)


def _is_excluded(path: str) -> bool:
    if path in EXCLUDED_PATHS or "{tail" in path:
        return True
    return path == "/metadata" or path.startswith(EXCLUDED_PREFIXES)


def documentable_routes(routes) -> list:
    """The routes that belong in the spec, as ``(method, path, handler)``."""
    return [
        Route(method.upper(), path, _as_handler(handler))
        for method, path, handler in routes
        if method.upper() not in SKIPPED_METHODS
        and method != "*"
        and path
        and not _is_excluded(path)
    ]


# --------------------------------------------------------------------------
# Docstrings, components and version
# --------------------------------------------------------------------------


def load_doc_module(service: str):
    """The service's ``doc.py`` module, or None. Loaded by file path."""
    name = SERVICES[service]["doc_module"]
    if not name:
        return None
    return _load_source_module(name, name.replace(".", "/") + ".py")


def service_version() -> str:
    """``version=`` of setup.py, else the installed package version."""
    try:
        tree = ast.parse((REPO_ROOT / "setup.py").read_text())
    except OSError:
        tree = None
    for node in ast.walk(tree) if tree else ():
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "setup":
            for kw in node.keywords:
                if (
                    kw.arg == "version"
                    and isinstance(kw.value, ast.Constant)
                    and isinstance(kw.value.value, str)
                ):
                    return kw.value.value
    try:
        return importlib.metadata.version("metadata_service")
    except importlib.metadata.PackageNotFoundError:
        raise SpecError(
            "Cannot determine the service version: setup.py has no literal "
            "version= and metadata_service is not installed"
        )


def parse_docstring(handler):
    """
    Parse the Swagger operation from a handler docstring.

    ``handler`` is a Handler or a (bound) function. Returns the operation
    dict, or None if the handler is undocumented (no docstring, no ``---``
    line, or an empty YAML block). Raises DocstringError naming the handler if
    the YAML is invalid.
    """
    if yaml is None:
        raise SpecError(DEV_DEPS_HINT)

    handler = _as_handler(handler)
    if not handler.doc:
        return None
    lines = inspect.cleandoc(handler.doc).splitlines()
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
    module = load_doc_module(service)
    if module is None:
        return SERVICES[service].get("description", ""), {}, {}
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


def build_spec(routes, service: str, warn=None):
    """
    Build the Swagger 2.0 spec for ``routes`` (as returned by collect_routes).

    Returns ``(spec, undocumented)`` where ``undocumented`` lists
    ``"METHOD path (handler)"`` entries that got a stub operation.
    """
    paths = {}
    operation_ids = {}
    # (method, path with placeholders blanked) -> (path, "METHOD path (handler)").
    # aiohttp keeps every resource and dispatches to the first match, so a
    # later /a/{y} next to /a/{x} is dead at runtime and must not be documented.
    route_keys = {}
    undocumented = []

    for method, path, handler in documentable_routes(routes):
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
            "version": service_version(),
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
    routes = collect_routes(service)
    spec, undocumented = build_spec(routes, service, warn=warn)
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
