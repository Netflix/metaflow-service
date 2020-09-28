def response_object(ref_definition: str):
    return {
        "type": "object",
        "properties": {
            "data": {
                "type": "array",
                "items": {
                    "$ref": ref_definition
                }
            },
            "status": {
                "type": "integer",
                "description": "Response status",
                "default": 200
            },
            "links": {
                "type": "object",
                "properties": {
                    "self": {
                        "type": "string",
                        "description": "Full URL to current page",
                        "default": "http://localhost:8083/path"
                    }
                }
            },
            "query": {
                "type": "object",
                "description": "Object of query parameters"
            }
        }
    }


def response_list(ref_definition: str):
    return {
        "type": "object",
        "properties": {
            "data": {
                "type": "array",
                "items": {
                    "$ref": ref_definition
                }
            },
            "status": {
                "type": "integer",
                "description": "Response status",
                "default": 200
            },
            "links": {
                "type": "object",
                "properties": {
                    "self": {
                        "type": "string",
                        "description": "Full URL to current page",
                        "default": "http://localhost:8083/path"
                    },
                    "first": {
                        "type": "string",
                        "description": "Full URL to first page",
                        "default": "http://localhost:8083/path?_page=1"
                    },
                    "prev": {
                        "type": "string",
                        "description": "Full URL to previous page",
                        "default": "http://localhost:8083/path?_page=1"
                    },
                    "next": {
                        "type": "string",
                        "description": "Full URL to next page",
                        "default": "http://localhost:8083/path?_page=2"
                    },
                    "last": {
                        "type": "string",
                        "description": "Full URL to last page",
                        "default": "http://localhost:8083/path?_page=99"
                    },
                }
            },
            "pages": {
                "type": "object",
                "properties": {
                    "self": {
                        "type": "integer",
                        "description": "Current page number",
                        "default": 1
                    },
                    "first": {
                        "type": "integer",
                        "description": "First page number",
                        "default": 1
                    },
                    "prev": {
                        "type": "integer",
                        "description": "Previous page number",
                        "default": 1
                    },
                    "next": {
                        "type": "integer",
                        "description": "Next page number",
                        "default": 2
                    },
                    "last": {
                        "type": "integer",
                        "description": "Last page number",
                        "default": 99
                    },
                }
            },
            "query": {
                "type": "object",
                "description": "Object of query parameters"
            }
        }
    }


def response_error(status: int):
    return {
        "type": "object",
        "properties": {
            "data": {
                "type": "array",
                "description": "Response data",
                "items": {
                    "type": "object"
                }
            },
            "status": {
                "type": "integer",
                "description": "Response status",
                "default": status
            },
        }
    }


def path_param(name: str, description: str, param_type: str):
    return {
        "name": name,
        "in": "path",
        "description": description,
        "required": True,
        "type": param_type,
    }


def custom_param(name: str, param_type: str):
    return {
        "name": name,
        "in": "query",
        "description": "Filter by '{}' column - (Operators: eq,ne,lt,le,gt,ge,co,sw,ew)".format(name),
        "required": False,
        "type": param_type,
    }


def basemodel(properties={}):
    return {
        "type": "object",
        "properties": {
            **{
                **modelprop("flow_id", "string", "Flow id", "HelloFlow"),
            },
            **properties,
            **{
                **modelprop("user_name", "string", "Username", "john"),
                **modelprop("ts_epoch", "integer", "Created at epoch timestamp", 1591788834035),
                "tags": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                "system_tags": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
            }
        }
    }


def modelprop(name: str, property_type: str, description: str, default_value):
    return {
        name: {
            "type": property_type,
            "description": description,
            "default": default_value
        }
    }


swagger_definitions = {
    "Params": {
        "Builtin": {
            "_page": {
                "name": "_page",
                "in": "query",
                "description": "Results page",
                "required": False,
                "type": "integer",
                "default": 1,
            },
            "_limit": {
                "name": "_limit",
                "in": "query",
                "description": "Limit results per page",
                "required": False,
                "type": "integer",
                "default": 10,
                "minimum": 1,
                "maximum": 50
            },
            "_order": {
                "name": "_order",
                "in": "query",
                "description": "Order results based on columns - (_order=run_number,+user_name)",
                "required": False,
                "type": "array",
                "items": {
                        "type": "string"
                },
                "style": "form",
                "explode": False,
                "allowReserved": True
            },
            "_tags": {
                "name": "_tags",
                "in": "query",
                "description": "Filter by resource 'tags' and 'system_tags' - (_tags=runtime:dev _tags:any=runtime:dev,user:john)",
                "required": False,
                "type": "array",
                "items": {
                        "type": "string"
                },
                "style": "form",
                "explode": False,
                "allowReserved": True
            },
            "_group": {
                "name": "_group",
                "in": "query",
                "description": "Group (partition) results by column(s). Once applied, limits and filters are affected per group and pagination is no longer supported. - (_group=flow_id _group=flow_id,user_name)",
                "required": False,
                "type": "string",
            },
        },
        "Path": {
            "flow_id": path_param("flow_id", "Flow id", "string"),
            "run_number": path_param("run_number", "Run number", "integer"),
            "step_name": path_param("step_name", "Step name", "string"),
            "task_id": path_param("task_id", "Task id", "integer"),
        },
        "Custom": {
            "id": custom_param("id", "integer"),
            "flow_id": custom_param("flow_id", "string"),
            "run_number": custom_param("run_number", "integer"),
            "run_id": custom_param("run_id", "string"),
            "step_name": custom_param("step_name", "string"),
            "task_id": custom_param("task_id", "integer"),
            "name": custom_param("name", "string"),
            "field_name": custom_param("field_name", "string"),
            "value": custom_param("value", "string"),
            "type": custom_param("type", "string"),
            "ds_type": custom_param("ds_type", "string"),
            "attempt_id": custom_param("attempt_id", "integer"),
            "user_name": custom_param("user_name", "string"),
            "ts_epoch": custom_param("ts_epoch", "integer"),
            "finished_at": custom_param("finished_at", "integer"),
            "duration": custom_param("duration", "integer"),
            "status": custom_param("status", "string"),
        }
    },
    "ResponsesFlow": response_object("#/definitions/ModelsFlow"),
    "ResponsesFlowList": response_list("#/definitions/ModelsFlow"),
    "ResponsesRun": response_object("#/definitions/ModelsRun"),
    "ResponsesRunList": response_list("#/definitions/ModelsRun"),
    "ResponsesStep": response_object("#/definitions/ModelsStep"),
    "ResponsesStepList": response_list("#/definitions/ModelsStep"),
    "ResponsesTask": response_object("#/definitions/ModelsTask"),
    "ResponsesTaskList": response_list("#/definitions/ModelsTask"),
    "ResponsesMetadataList": response_list("#/definitions/ModelsMetadata"),
    "ResponsesArtifactList": response_list("#/definitions/ModelsArtifact"),
    "ResponsesError405": response_error(405),
    "ModelsFlow": basemodel({}),
    "ModelsRun": basemodel({
        **modelprop("run_number", "integer", "Run number", 5),
        **modelprop("status", "string", "Run status (completed/running/failed)", "completed"),
        **modelprop("finished_at", "integer", "Finished at epoch timestamp", 1591788834035),
        **modelprop("duration", "integer", "Duration in milliseconds (null if unfinished)", 456),
    }),
    "ModelsStep": basemodel({
        **modelprop("run_number", "integer", "Run number", 5),
        **modelprop("step_name", "string", "Step name", "bonus_movie"),
    }),
    "ModelsTask": basemodel({
        **modelprop("run_number", "integer", "Run number", 5),
        **modelprop("step_name", "string", "Step name", "bonus_movie"),
        **modelprop("task_id", "integer", "Task id", 32),
        **modelprop("status", "string", "Task status (completed/running/failed)", "completed"),
        **modelprop("finished_at", "integer", "Finished at epoch timestamp", 1591788834035),
        **modelprop("duration", "integer", "Duration in milliseconds (null if unfinished)", 456),
    }),
    "ModelsMetadata": basemodel({
        **modelprop("run_number", "integer", "Run number", 5),
        **modelprop("step_name", "string", "Step name", "bonus_movie"),
        **modelprop("task_id", "integer", "Task id", 32),
        **modelprop("field_name", "string", "Field name", "attempt-done"),
        **modelprop("value", "string", "Value", "None"),
        **modelprop("type", "string", "Type", "attempt-done"),
    }),
    "ModelsArtifact": basemodel({
        **modelprop("run_number", "integer", "Run number", 5),
        **modelprop("step_name", "string", "Step name", "bonus_movie"),
        **modelprop("task_id", "integer", "Task id", 32),
        **modelprop("name", "string", "Name", "bonus"),
        **modelprop("location", "string", "Name", "/local/path/.metaflow/HelloFlow/data/8d/8de2c3d91a9384069a3b014fdc3b60b4eb68567c"),
        **modelprop("ds_type", "string", "Datastore type", "local"),
        **modelprop("sha", "string", "SHA hash", "8de2c3d91a9384069a3b014fdc3b60b4eb68567c"),
        **modelprop("type", "string", "Type", "metaflow.artifact"),
        **modelprop("content_type", "string", "Content-type", "gzip+pickle-v2"),
        **modelprop("attempt_id", "integer", "Attempt id", 0),
    }),
}

swagger_description = "Metaflow UI backend service"
