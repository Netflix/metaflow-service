"""
Swagger 2.0 components specific to the UI backend service.

Models extend the shared base models from ``services.utils.openapi_components``
with the UI only fields. Property sets match the non-expanded ``serialize()`` output
of ``services/ui_backend_service/data/db/models/*_row.py``; the UI rows always
serialize ``run_number``/``task_id`` as strings.
"""

from services.utils.openapi_components import (
    FILTER_OPERATORS,
    SHARED_DEFINITIONS,
    array_of,
    extend,
    model,
    path_param,
    prop,
    query_param,
    ref,
    ui_internal_error,
    ui_response_error,
    ui_response_list,
    ui_response_object,
)

description = "Metaflow UI backend service"


def custom_param(name: str, param_type: str) -> dict:
    return query_param(
        name,
        "Filter by '{}' column - (Operators: {}), e.g. {}:eq=value. "
        "Comma separated values are OR'ed.".format(name, FILTER_OPERATORS, name),
        param_type,
    )


def _flag_param(name: str, description: str) -> dict:
    return query_param(name, description, "boolean", default=False)


parameters = {
    # Builtin query parameters, shared by all list endpoints.
    "builtin__page": query_param(
        "_page", "Results page", "integer", default=1, minimum=1
    ),
    "builtin__limit": query_param(
        "_limit",
        "Limit results per page",
        "integer",
        default=10,
        minimum=1,
        maximum=1000,
    ),
    "builtin__order": query_param(
        "_order",
        "Order results based on columns, descending by default. Prefix a column "
        "with + for ascending order - (_order=run_number,+user_name)",
        "array",
    ),
    "builtin__tags": query_param(
        "_tags",
        "Filter by resource 'tags' and 'system_tags' - (_tags=runtime:dev "
        "_tags:any=runtime:dev,user:john). Operators: all (default), any, "
        "likeall, likeany.",
        "array",
    ),
    "builtin__group": query_param(
        "_group",
        "Group (partition) results by column(s). Once applied, limits and "
        "filters are affected per group and pagination is no longer supported. "
        "- (_group=flow_id _group=flow_id,user_name)",
        "array",
    ),
    # UI only path parameters. Flow/run/step/task path parameters are shared.
    "path_run_id": path_param("run_id", "Run number or run id"),
    "path_plugin_name": path_param("plugin_name", "Plugin name (identifier)"),
    "path_plugin_filename": path_param(
        "filename", "Relative path to file (e.g. dist/index.html)"
    ),
    "path_hash": path_param("hash", "Card hash"),
    # Column filters (field:operator grammar).
    "custom_id": custom_param("id", "integer"),
    "custom_flow_id": custom_param("flow_id", "string"),
    "custom_run_number": custom_param("run_number", "integer"),
    "custom_run_id": custom_param("run_id", "string"),
    "custom_run": custom_param("run", "string"),
    "custom_user": custom_param("user", "string"),
    "custom_step_name": custom_param("step_name", "string"),
    "custom_task_id": custom_param("task_id", "integer"),
    "custom_name": custom_param("name", "string"),
    "custom_field_name": custom_param("field_name", "string"),
    "custom_value": custom_param("value", "string"),
    "custom_type": custom_param("type", "string"),
    "custom_ds_type": custom_param("ds_type", "string"),
    "custom_attempt_id": custom_param("attempt_id", "integer"),
    "custom_user_name": custom_param("user_name", "string"),
    "custom_ts_epoch": custom_param("ts_epoch", "integer"),
    "custom_finished_at": custom_param("finished_at", "integer"),
    "custom_duration": custom_param("duration", "integer"),
    "custom_status": custom_param("status", "string"),
    "custom_tag": custom_param("tag", "string"),
    "custom_postprocess": _flag_param(
        "postprocess",
        "Control whether any postprocessing is done (if applicable) to the "
        "requested results before returning. This will slow down the request "
        "considerably in case it depends on S3 content.",
    ),
    "custom_invalidate": _flag_param(
        "invalidate",
        "Invalidate cache before fetching new results from S3. Requires "
        "refining/postprocessing to be effective, usually defined with "
        "postprocess=true query parameter.",
    ),
    # Search
    "search_key": query_param(
        "key",
        "Artifact name (ARTIFACT scope) or foreach variable name "
        "(FOREACH_VARIABLE scope) to search for",
        required=True,
    ),
    "search_value": query_param(
        "value",
        "Value to match. Partial match by default, exact match when wrapped in "
        'double quotes ("value"). Omit to list every task with the artifact.',
    ),
    "search_scope": query_param(
        "scope",
        "Comma separated search scopes: ARTIFACT (default), FOREACH_VARIABLE",
        "array",
        default=["ARTIFACT"],
    ),
}


# --------------------------------------------------------------------------
# Models
# --------------------------------------------------------------------------

_RUN_NUMBER = prop(
    "run_number",
    "string",
    "Run number or run id, always serialized as a string",
    "5",
)
_TASK_ID = prop(
    "task_id",
    "string",
    "Task id or task name, always serialized as a string",
    "32",
)
_FINISHED_AT = prop(
    "finished_at",
    "integer",
    "Finished at, epoch timestamp in milliseconds (null if unfinished)",
    1591788834035,
)


def _duration(description: str) -> dict:
    return prop("duration", "integer", description, 456)


_ui_definitions = {
    "ModelsFlow": extend(SHARED_DEFINITIONS["Flow"]),
    "ModelsRun": extend(
        SHARED_DEFINITIONS["Run"],
        {
            **_RUN_NUMBER,
            **prop(
                "status", "string", "Run status (completed/running/failed)", "completed"
            ),
            **_FINISHED_AT,
            **_duration("Duration in milliseconds (null if unfinished)"),
        },
        drop=["user"],
    ),
    "ModelsStep": extend(
        SHARED_DEFINITIONS["Step"],
        {
            **_RUN_NUMBER,
            **_duration(
                "Current duration in milliseconds (null if no tasks exist yet)"
            ),
        },
    ),
    "ModelsTask": extend(
        SHARED_DEFINITIONS["Task"],
        {
            **_RUN_NUMBER,
            **_TASK_ID,
            **prop(
                "status",
                "string",
                "Task status (completed/running/failed/unknown)",
                "completed",
            ),
            **prop(
                "task_ok",
                "string",
                "Location of the _task_ok artifact, used internally to refine the "
                "status (null when not needed)",
            ),
            **prop(
                "started_at",
                "integer",
                "Started at epoch timestamp of the attempt (if this could be "
                "inferred from the database, otherwise null)",
                1591788834035,
            ),
            **prop(
                "finished_at",
                "integer",
                "Finished at epoch timestamp of the attempt",
                1591788834035,
            ),
            **_duration("Duration in milliseconds (null if unfinished)"),
            **prop("attempt_id", "integer", "Attempt id", 0),
            **prop(
                "foreach_label",
                "string",
                "Task foreach label (if applicable). Only present with "
                "postprocess=true.",
                "123[0]",
            ),
        },
    ),
    "ModelsMetadata": extend(
        SHARED_DEFINITIONS["Metadata"],
        {
            **_RUN_NUMBER,
            **_TASK_ID,
            **prop("attempt_id", "integer", "Attempt id (null if not recorded)", 0),
        },
    ),
    "ModelsArtifact": extend(
        SHARED_DEFINITIONS["Artifact"],
        {
            **_RUN_NUMBER,
            **_TASK_ID,
            **prop(
                "content",
                "string",
                "Artifact value. Only present with postprocess=true.",
            ),
        },
    ),
    "ModelsAutocompleteTag": {"type": "string", "example": "user:john"},
    "ModelsAutocompleteFlow": {"type": "string", "example": "HelloFlow"},
    "ModelsAutocompleteRun": {
        "type": "string",
        "description": "Run number or run id",
        "example": "5",
    },
    "ModelsAutocompleteStep": {"type": "string", "example": "start"},
    "ModelsAutocompleteArtifact": {"type": "string", "example": "bonus"},
    "ModelsRunParameters": {
        "type": "object",
        "description": "Run parameters keyed by parameter name",
        "additionalProperties": ref("ModelsRunParameter"),
    },
    "ModelsRunParameter": model(
        prop("value", "string", "Value of the parameter", "A"),
    ),
    "ModelsCardInfo": model(
        {
            **prop("id", "string", "Card id (null if not set)", "my_card"),
            **prop("type", "string", "Card type", "default"),
            **prop("hash", "string", "Card hash", "123456abcdef"),
        }
    ),
    "ModelsSearchResult": model(
        {
            **prop("flow_id", "string", "Flow id (flow name)", "HelloFlow"),
            **prop("run_number", None, "Run number or run id", 5),
            **prop("step_name", "string", "Step name", "start"),
            **prop("task_id", None, "Task id or task name", 32),
            **prop(
                "searchable",
                "boolean",
                "Whether the artifact content could be included in the search",
            ),
            **prop(
                "error",
                "object",
                "Null, or an error object {id, detail} if the artifact could not "
                "be searched",
            ),
        }
    ),
    "ModelsSearchEvent": model(
        {
            "event": model(
                {
                    **prop(
                        "type",
                        "string",
                        "Event type. 'result' carries matches, 'error' an error "
                        "(message, id, traceback, key); progress events from the "
                        "cache may be streamed too.",
                        "result",
                    ),
                    "matches": array_of("ModelsSearchResult"),
                }
            )
        },
        description="Websocket text message (JSON) streamed by the search endpoints",
    ),
    "ModelsLink": model(
        {
            **prop("href", "string", "URL for the link"),
            **prop("label", "string", "Label for the link"),
        },
        required=["href", "label"],
    ),
    "ModelsNotification": model(
        {
            **prop(
                "id",
                "string",
                "Notification identifier (generated SHA1 hash unless configured)",
            ),
            **prop("message", "string", "Message to display (Markdown supported)"),
            **prop(
                "created",
                "integer",
                "Notification created at (Epoch timestamp in milliseconds)",
            ),
            **prop(
                "type",
                "string",
                "Notification type",
                "info",
                enum=["success", "info", "warning", "danger", "default"],
            ),
            **prop(
                "contentType",
                "string",
                "Message content-type",
                "text",
                enum=["text", "markdown"],
            ),
            **prop("url", "string", "Notification url"),
            **prop("urlText", "string", "Human readable url title"),
            **prop(
                "start",
                "integer",
                "Schedule notification to be visible starting at (Epoch timestamp "
                "in milliseconds)",
            ),
            **prop(
                "end",
                "integer",
                "Schedule notification to disappear after (Epoch timestamp in "
                "milliseconds)",
            ),
        },
        required=[
            "id",
            "message",
            "created",
            "type",
            "contentType",
            "url",
            "urlText",
            "start",
            "end",
        ],
    ),
    "ModelsPlugin": model(
        {
            **prop(
                "identifier", "string", "Plugin identifier (repository/folder name)"
            ),
            **prop("name", "string", "Plugin name (manifest.json)"),
            **prop("repository", "string", "Git repository"),
            **prop(
                "ref", "string", "Git refish to use e.g. origin/feature/extra-syrup"
            ),
            **prop("parameters", "object", "Custom parameters to pass to the plugin"),
            "config": ref("ModelsPluginConfig"),
            "files": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of files available for this plugin",
            },
        },
        required=[
            "identifier",
            "name",
            "repository",
            "ref",
            "parameters",
            "config",
            "files",
        ],
    ),
    "ModelsPluginConfig": model(
        {
            **prop("name", "string", "Plugin name", "plugin-name"),
            **prop("version", "string", "Semantic version of this plugin", "1.0.0"),
            **prop(
                "entrypoint",
                "string",
                "Entrypoint for the plugin, relative or absolute. Loaded inside "
                "iframe with parameters",
                "index.html",
            ),
        },
        required=["name", "version", "entrypoint"],
        description="Plugin manifest.json",
    ),
    "ModelsDag": model(
        {
            **prop(
                "file", "string", "Filename of the script used to execute the flow."
            ),
            "steps_info": ref("ModelsDagStepsInfo"),
            "steps_structure": ref("ModelsDagStepsStructure"),
        }
    ),
    # Nested arrays of step names, e.g. ["start", ["a", "b"], "join", "end"].
    # Swagger 2.0 has no anyOf, so the item type is left open.
    "ModelsDagStepsStructure": {
        "type": "array",
        "description": "DAG step structure. Items are step names (string) or "
        "nested arrays of the same structure for parallel branches.",
        "items": {},
    },
    "ModelsDagStepsInfo": {
        "type": "object",
        "description": "DAG step details keyed by step name. Always contains "
        "'start' and 'end'.",
        "additionalProperties": ref("ModelsDagNode"),
    },
    "ModelsDagNode": model(
        {
            **prop("name", "string", "Step name of the node"),
            **prop("type", "string", "DAG Node type"),
            **prop("line", "integer", "Line of code where step definition starts"),
            **prop("doc", "string", "DAG Node Docstring"),
            "decorators": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Decorators of the step",
            },
            "next": {
                "type": "array",
                "items": {"type": "string"},
                "description": "names of next steps that follow from this step",
            },
        }
    ),
    "ModelsLogRow": model(
        {
            **prop("row", "integer", "Row number", 0),
            **prop(
                "timestamp",
                "integer",
                "Epoch timestamp of the log line entry",
                1591788834035,
            ),
            **prop("line", "string", "Log line content", "logged text"),
        },
        required=["row", "line"],
    ),
}

_responses = {
    "ResponsesAutocompleteTagList": ui_response_list(ref("ModelsAutocompleteTag")),
    "ResponsesAutocompleteFlowList": ui_response_list(ref("ModelsAutocompleteFlow")),
    "ResponsesAutocompleteRunList": ui_response_list(ref("ModelsAutocompleteRun")),
    "ResponsesAutocompleteStepList": ui_response_list(ref("ModelsAutocompleteStep")),
    "ResponsesAutocompleteArtifactList": ui_response_list(
        ref("ModelsAutocompleteArtifact")
    ),
    "ResponsesFlow": ui_response_object(ref("ModelsFlow")),
    "ResponsesFlowList": ui_response_list(ref("ModelsFlow")),
    "ResponsesRun": ui_response_object(ref("ModelsRun")),
    "ResponsesRunList": ui_response_list(ref("ModelsRun")),
    "ResponsesRunParameters": ui_response_object(ref("ModelsRunParameters")),
    "ResponsesRunParametersError500": ui_internal_error(),
    "ResponsesStep": ui_response_object(ref("ModelsStep")),
    "ResponsesStepList": ui_response_list(ref("ModelsStep")),
    "ResponsesTask": ui_response_object(ref("ModelsTask")),
    "ResponsesTaskList": ui_response_list(ref("ModelsTask")),
    "ResponsesMetadataList": ui_response_list(ref("ModelsMetadata")),
    "ResponsesArtifactList": ui_response_list(ref("ModelsArtifact")),
    "ResponsesCardList": ui_response_list(ref("ModelsCardInfo")),
    "ResponsesTagList": {
        "type": "array",
        "description": "All distinct tags and system tags of all runs",
        "items": {"type": "string"},
    },
    "ResponsesLinkList": array_of("ModelsLink"),
    "ResponsesNotificationList": array_of("ModelsNotification"),
    "ResponsesLog": ui_response_list(ref("ModelsLogRow")),
    "ResponsesLogError500": ui_internal_error({"log-error": "Parsing the log failed"}),
    "ResponsesDag": ui_response_object(ref("ModelsDag")),
    "ResponsesDagError500": ui_internal_error(
        {
            "DAGParsingFailed": "Processing the DAG Failed",
            "DAGUnsupportedFlowLanguage": "The Flow language is not supported by "
            "the DAG parser. DAG graph can not be generated",
        }
    ),
    "ResponsesPlugin": ref("ModelsPlugin"),
    "ResponsesPluginList": array_of("ModelsPlugin"),
    "ResponsesError405": ui_response_error(405),
    "ResponsesError404": ui_response_error(404),
    "ResponsesError500": ui_internal_error(),
}

definitions = {**_ui_definitions, **_responses}
