"""
Swagger 2.0 building blocks shared by every service spec.

Handler docstrings reference these as ``#/parameters/<key>`` and
``#/definitions/<key>``. Service specific components live in each service's
``doc.py`` and are built with the helpers below.

Rules for everything defined here (and in the service ``doc.py`` modules):

* strictly Swagger 2.0: no ``anyOf``/``oneOf``, no OpenAPI 3 keywords
  (``style``, ``explode``, ``allowReserved``, ``nullable``),
* a ``$ref`` object never has sibling keys,
* sample values go in ``example``, never ``default`` (``default`` claims a
  server side default),
* model property sets must match the ``serialize()`` output of the matching
  row class. ``services/openapi/tests/openapi_test.py`` enforces this.

This module only builds plain dicts and must not import anything outside the
standard library, so service modules can import it without dev dependencies.
It lives in ``services/utils`` because every service image ships that package;
``services/openapi`` (the generator) is dev only and not part of the images.
"""

import copy

# Operators accepted by the field:operator filter grammar.
FILTER_OPERATORS = "eq,ne,lt,le,gt,ge,co,sw,ew,li,is"


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def ref(name: str) -> dict:
    """JSON reference to a definition."""
    return {"$ref": "#/definitions/{}".format(name)}


def param_ref(name: str) -> dict:
    """JSON reference to a parameter."""
    return {"$ref": "#/parameters/{}".format(name)}


def array_of(name: str) -> dict:
    """Array schema whose items reference a definition."""
    return {"type": "array", "items": ref(name)}


def path_param(name: str, description: str, param_type: str = "string") -> dict:
    return {
        "name": name,
        "in": "path",
        "description": description,
        "required": True,
        "type": param_type,
    }


def query_param(
    name: str,
    description: str,
    param_type: str = "string",
    items_type: str = "string",
    **extra,
) -> dict:
    """Optional query parameter. Array params are always comma separated."""
    param = {
        "name": name,
        "in": "query",
        "description": description,
        "required": False,
        "type": param_type,
    }
    if param_type == "array":
        param["items"] = {"type": items_type}
        param["collectionFormat"] = "csv"
    param.update(extra)
    return param


def prop(name: str, prop_type, description: str, example=None, **extra) -> dict:
    """
    Single model property, returned as ``{name: schema}`` so properties can be
    merged with ``**``. ``prop_type=None`` leaves the type open (any JSON value).
    """
    schema = {"description": description}
    if prop_type is not None:
        schema["type"] = prop_type
    if example is not None:
        schema["example"] = example
    schema.update(extra)
    return {name: schema}


def model(properties: dict, required=None, description: str = None) -> dict:
    schema = {"type": "object", "properties": properties}
    if required:
        schema["required"] = list(required)
    if description:
        schema["description"] = description
    return schema


def extend(base: dict, properties: dict = None, drop=(), description: str = None):
    """
    Copy of an object model with extra (or overridden) properties and some
    properties removed. Used by the UI to build on the shared base models.
    """
    schema = copy.deepcopy(base)
    for key in drop:
        schema["properties"].pop(key, None)
    schema["properties"].update(copy.deepcopy(properties or {}))
    if "required" in schema:
        schema["required"] = [r for r in schema["required"] if r not in drop]
        if not schema["required"]:
            del schema["required"]
    if description:
        schema["description"] = description
    return schema


# Run and task identifiers are exposed either as the integer id or as the user
# supplied string name (see get_exposed_run_id/get_exposed_task_id). Swagger
# 2.0 has no oneOf, so the type is deliberately left out.
ID_PROP = {"description": "integer id or string name"}


def id_prop(name: str, description: str, example=None) -> dict:
    schema = dict(ID_PROP)
    schema["description"] = "{} ({})".format(description, ID_PROP["description"])
    if example is not None:
        schema["example"] = example
    return {name: schema}


TAGS_PROPS = {
    "tags": {
        "type": "array",
        "items": {"type": "string"},
        "description": "User tags",
        "example": ["user:john", "experiment"],
    },
    "system_tags": {
        "type": "array",
        "items": {"type": "string"},
        "description": "System tags",
        "example": ["runtime:dev", "metaflow_version:2.11.4"],
    },
}


# --------------------------------------------------------------------------
# UI response wrappers ({data, status, links, ...})
# --------------------------------------------------------------------------

_STATUS_PROP = {"type": "integer", "description": "Response status", "example": 200}
_QUERY_PROP = {"type": "object", "description": "Object of query parameters"}


def _link(description: str, example: str) -> dict:
    return {"type": "string", "description": description, "example": example}


def _page(description: str, example: int) -> dict:
    return {"type": "integer", "description": description, "example": example}


def ui_response_object(data_schema: dict) -> dict:
    return model(
        {
            "data": data_schema,
            "status": _STATUS_PROP,
            "links": model(
                {
                    "self": _link(
                        "Full URL to current page", "http://localhost:8083/path"
                    )
                }
            ),
            "query": _QUERY_PROP,
        }
    )


def ui_response_list(item_schema: dict) -> dict:
    return model(
        {
            "data": {"type": "array", "items": item_schema},
            "status": _STATUS_PROP,
            "links": model(
                {
                    "self": _link(
                        "Full URL to current page", "http://localhost:8083/path"
                    ),
                    "first": _link(
                        "Full URL to first page", "http://localhost:8083/path?_page=1"
                    ),
                    "prev": _link(
                        "Full URL to previous page",
                        "http://localhost:8083/path?_page=1",
                    ),
                    "next": _link(
                        "Full URL to next page (null on the last page)",
                        "http://localhost:8083/path?_page=2",
                    ),
                    "last": _link(
                        "Full URL to last page (null when the page count is unknown)",
                        "http://localhost:8083/path?_page=5",
                    ),
                }
            ),
            "pages": model(
                {
                    "self": _page("Current page number", 1),
                    "first": _page("First page number", 1),
                    "prev": _page("Previous page number", 1),
                    "next": _page("Next page number (null on the last page)", 2),
                    "last": _page(
                        "Last page number (null when the page count is unknown)", 5
                    ),
                }
            ),
            "query": _QUERY_PROP,
        }
    )


def ui_response_error(status: int) -> dict:
    return model(
        {
            "data": {
                "type": "array",
                "description": "Response data",
                "items": {"type": "object"},
            },
            "status": {
                "type": "integer",
                "description": "Response status",
                "example": status,
            },
        }
    )


def ui_internal_error(error_ids_and_descriptions: dict = None) -> dict:
    """
    Internal error body. Lists the generic error ids plus the given ones in
    the ``id`` enum, with their descriptions.
    """
    errors = {
        "generic-error": "Non-Specific Error",
        "DataException": "Metaflow client DataException",
        "MetaflowS3AccessDenied": "S3 Access Denied",
        "MetaflowS3NotFound": "S3 error 404 not found",
        "MetaflowS3URLException": "S3 URL is malformed",
        "MetaflowS3Exception": "Something went wrong with S3 access",
        **(error_ids_and_descriptions or {}),
    }
    description = "Specific error ID\n" + "\n".join(
        "* {} - {}".format(error_id, desc) for error_id, desc in errors.items()
    )
    return model(
        {
            "id": {"type": "string", "enum": list(errors), "description": description},
            "traceback": {"type": "string", "description": "Stacktrace of the error"},
            "detail": {
                "type": "string",
                "description": "Detailed message of the error",
            },
            "status": {"type": "integer", "example": 500},
            "title": {"type": "string", "example": "Internal Server Error"},
            "type": {"type": "string", "example": "about:blank"},
        },
        required=["id", "traceback", "detail", "status", "title", "type"],
    )


# --------------------------------------------------------------------------
# Shared parameters (#/parameters/<key>)
# --------------------------------------------------------------------------

SHARED_PARAMETERS = {
    "path_flow_id": path_param("flow_id", "Flow id (flow name)"),
    "path_run_number": path_param("run_number", "Run number or run id"),
    "path_step_name": path_param("step_name", "Step name"),
    "path_task_id": path_param("task_id", "Task id or task name"),
    "query_limit": query_param(
        "_limit",
        "Page size (default 50, max 500). Supplying _limit or _cursor turns on "
        "cursor pagination.",
        "integer",
        minimum=1,
        maximum=500,
    ),
    "query_cursor": query_param(
        "_cursor",
        "Opaque pagination cursor, returned in the X-Next-Cursor response header.",
    ),
    "query_tags": query_param(
        "_tags",
        "Filter by tags and system tags, e.g. _tags:any=a,b (OR) or "
        "_tags:all=a,b (AND).",
        "array",
    ),
}


# --------------------------------------------------------------------------
# Shared models (#/definitions/<key>)
#
# Property sets match the non-expanded serialize() output of the row classes in
# services/data/models.py.
# --------------------------------------------------------------------------

_FLOW_ID = prop("flow_id", "string", "Flow id (flow name)", "HelloFlow")
_RUN_NUMBER = id_prop("run_number", "Run number or run id", 5)
_STEP_NAME = prop("step_name", "string", "Step name", "start")
_TASK_ID = id_prop("task_id", "Task id or task name", 32)
_USER_NAME = prop("user_name", "string", "Username", "john")
_TS_EPOCH = prop(
    "ts_epoch", "integer", "Created at, epoch timestamp in milliseconds", 1591788834035
)
_LAST_HEARTBEAT = prop(
    "last_heartbeat_ts",
    "integer",
    "Last heartbeat, epoch timestamp in seconds (null if never sent)",
    1591788834,
)

SHARED_DEFINITIONS = {
    "Flow": model(
        {**_FLOW_ID, **_USER_NAME, **_TS_EPOCH, **TAGS_PROPS},
    ),
    "Run": model(
        {
            **_FLOW_ID,
            **_RUN_NUMBER,
            **_USER_NAME,
            **_TS_EPOCH,
            **TAGS_PROPS,
            **_LAST_HEARTBEAT,
            **prop(
                "status",
                "string",
                "Run status (running/completed/failed). Only present when the "
                "request filters on status.",
                "completed",
            ),
            **prop(
                "user",
                "string",
                "Verified owner, derived from the user:<name> system tag. Only "
                "present on filtered or paginated listings.",
                "john",
            ),
        },
    ),
    "Step": model(
        {
            **_FLOW_ID,
            **_RUN_NUMBER,
            **_STEP_NAME,
            **_USER_NAME,
            **_TS_EPOCH,
            **TAGS_PROPS,
        },
    ),
    "Task": model(
        {
            **_FLOW_ID,
            **_RUN_NUMBER,
            **_STEP_NAME,
            **_TASK_ID,
            **_USER_NAME,
            **_TS_EPOCH,
            **TAGS_PROPS,
            **_LAST_HEARTBEAT,
        },
    ),
    "Metadata": model(
        {
            **prop("id", "integer", "Metadata row id", 1),
            **_FLOW_ID,
            **_RUN_NUMBER,
            **_STEP_NAME,
            **_TASK_ID,
            **prop("field_name", "string", "Field name", "attempt-done"),
            **prop("value", "string", "Value", "0"),
            **prop("type", "string", "Type", "attempt-done"),
            **_USER_NAME,
            **_TS_EPOCH,
            **TAGS_PROPS,
        },
    ),
    "Artifact": model(
        {
            **_FLOW_ID,
            **_RUN_NUMBER,
            **_STEP_NAME,
            **_TASK_ID,
            **prop("name", "string", "Artifact name", "bonus"),
            **prop(
                "location",
                "string",
                "Datastore location of the artifact",
                "/local/path/.metaflow/HelloFlow/data/8d/"
                "8de2c3d91a9384069a3b014fdc3b60b4eb68567c",
            ),
            **prop("ds_type", "string", "Datastore type", "local"),
            **prop(
                "sha", "string", "SHA hash", "8de2c3d91a9384069a3b014fdc3b60b4eb68567c"
            ),
            **prop("type", "string", "Type", "metaflow.artifact"),
            **prop("content_type", "string", "Content-type", "gzip+pickle-v2"),
            **_USER_NAME,
            **prop("attempt_id", "integer", "Attempt id", 0),
            **_TS_EPOCH,
            **TAGS_PROPS,
        },
    ),
}
