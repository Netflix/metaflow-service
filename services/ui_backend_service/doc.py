def response_object(ref_definition: str):
    return {
        "type": "object",
        "properties": {
            "data": {
                "type": "object",
                "$ref": ref_definition
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
                    }
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
                    }
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


def response_internal_error(error_ids_and_descriptions={}):
    '''Formats a response object for internal errors.
    Includes all passed in keys in the 'id' enum field, along with 'generic-error'.
    Includes all values as descriptions for the error ids.
    '''
    # Include the default error-id.
    _errors = {
        "generic-error": "Non-Specific Error",
        "DataException": "Metaflow client DataException",
        "MetaflowS3AccessDenied": "S3 Access Denied",
        "MetaflowS3NotFound": "S3 error 404 not found",
        "MetaflowS3URLException": "S3 URL is malformed",
        "MetaflowS3Exception": "Something went wrong with S3 access",
        **error_ids_and_descriptions
    }
    error_ids = list(_errors.keys())
    description = "Specific error ID\n" + "\n".join([f"* {id} - {desc}" for id, desc in _errors.items()])
    return {
        "type": "object",
        "properties": {
            "id": {
                "type": "string",
                "enum": error_ids,
                "description": description
            },
            "traceback": {
                "type": "string",
                "description": "Stacktrace of the error"
            },
            "detail": {
                "type": "string",
                "description": "Detailed message of the error"
            },
            "status": {
                "type": "integer",
                "default": 500
            },
            "title": {
                "type": "string",
                "default": 'Internal Server Error'
            },
            "type": {
                "type": "string",
                "default": 'about:blank'
            },
        },
        "required": ["id", "traceback", "detail", "status", "title", "type"]
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
                "description": "Group (partition) results by column(s). Once applied,\
                    limits and filters are affected per group and pagination is no longer supported.\
                     - (_group=flow_id _group=flow_id,user_name)",
                "required": False,
                "type": "string",
            },
        },
        "Path": {
            "flow_id": path_param("flow_id", "Flow id", "string"),
            "run_number": path_param("run_number", "Run number", "integer"),
            "step_name": path_param("step_name", "Step name", "string"),
            "task_id": path_param("task_id", "Task id", "integer"),
            "plugin_name": path_param("plugin_name", "Plugin name (identifier)", "string"),
            "plugin_filename": path_param("filename", "Relative path to file (e.g. dist/index.html)", "string"),
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
            "postprocess": {
                "name": "postprocess",
                "in": "query",
                "description": "Control whether any postprocessing is done (if applicable) to the requested results\
                    before returning. This will slow down the request considerably in case it depends on S3 content.",
                "required": False,
                "default": False,
                "type": "boolean",
            },
            "invalidate": {
                "name": "invalidate",
                "in": "query",
                "description": "Invalidate cache before fetching new results from S3. Requires refining/postprocessing\
                    to be effective, usually defined with postprocess=true query parameter.",
                "required": False,
                "default": False,
                "type": "boolean",
            }
        }
    },
    "ResponsesAutocompleteTagList": response_list("#/definitions/ModelsAutocompleteTag"),
    "ResponsesAutocompleteFlowList": response_list("#/definitions/ModelsAutocompleteFlow"),
    "ResponsesAutocompleteRunList": response_list("#/definitions/ModelsAutocompleteRun"),
    "ResponsesAutocompleteStepList": response_list("#/definitions/ModelsAutocompleteStep"),
    "ResponsesAutocompleteArtifactList": response_list("#/definitions/ModelsAutocompleteArtifact"),
    "ResponsesFlow": response_object("#/definitions/ModelsFlow"),
    "ResponsesFlowList": response_list("#/definitions/ModelsFlow"),
    "ResponsesRun": response_object("#/definitions/ModelsRun"),
    "ResponsesRunList": response_list("#/definitions/ModelsRun"),
    "ResponsesRunParameters": response_object("#/definitions/ModelsRunParameters"),
    "ResponsesRunParametersError500": response_internal_error(),
    "ResponsesStep": response_object("#/definitions/ModelsStep"),
    "ResponsesStepList": response_list("#/definitions/ModelsStep"),
    "ResponsesTask": response_object("#/definitions/ModelsTask"),
    "ResponsesTaskList": response_list("#/definitions/ModelsTask"),
    "ResponsesMetadataList": response_list("#/definitions/ModelsMetadata"),
    "ResponsesArtifactList": response_list("#/definitions/ModelsArtifact"),
    "ResponsesCardList": response_list("#/definitions/ModelsCardInfo"),
    "ResponsesLinkList": {
        "type": "array",
        "items": {
            "type": "object",
            "$ref": "#/definitions/ModelsLink"
        }
    },
    "ResponsesNotificationList": {
        "type": "array",
        "items": {
            "type": "object",
            "$ref": "#/definitions/ModelsNotification"
        }
    },
    "ResponsesLog": response_list("#/definitions/ModelsLogRow"),
    "ResponsesLogError500": response_internal_error(
        {
            "log-error": "Parsing the log failed"
        }
    ),
    "ResponsesDag": response_object("#/definitions/ModelsDag"),
    "ResponsesDagError500": response_internal_error(
        {
            "DAGParsingFailed": "Processing the DAG Failed",
            "DAGUnsupportedFlowLanguage": "The Flow language is not supported by the DAG parser. DAG graph can not be generated"
        }
    ),
    "ResponsesPlugin": {
        "type": "object",
        "$ref": "#/definitions/ModelsPlugin"
    },
    "ResponsesPluginList": {
        "type": "array",
        "items": {
            "type": "object",
            "$ref": "#/definitions/ModelsPlugin"
        }
    },
    "ResponsesError405": response_error(405),
    "ResponsesError404": response_error(404),
    "ResponsesError500": response_internal_error(),
    "ModelsFlow": basemodel({}),
    "ModelsRun": basemodel({
        **modelprop("run_number", "integer", "Run number", 5),
        **modelprop("status", "string", "Run status (completed/running/failed)", "completed"),
        **modelprop("finished_at", "integer", "Finished at epoch timestamp", 1591788834035),
        **modelprop("duration", "integer", "Duration in milliseconds (null if unfinished)", 456),
    }),
    "ModelsAutocompleteTag": {
        "type": "string",
        "default": "tag name"
    },
    "ModelsAutocompleteFlow": {
        "type": "string",
        "default": "Flow id"
    },
    "ModelsAutocompleteRun": {
        "type": "string",
        "default": "run number or run id"
    },
    "ModelsAutocompleteStep": {
        "type": "string",
        "default": "step name"
    },
    "ModelsAutocompleteArtifact": {
        "type": "string",
        "default": "artifact name"
    },
    "ModelsRunParameters": {
        "type": "object",
        "properties": {
            "run_parameter_name": {
                "type": "object",
                "$ref": "#/definitions/ModelsRunParameter"
            }
        }
    },
    "ModelsRunParameter": {
        "type": "object",
        "properties": {
            **modelprop("value", "string", "Value of the parameter", "A"),
        }
    },
    "ModelsStep": basemodel({
        **modelprop("run_number", "integer", "Run number", 5),
        **modelprop("step_name", "string", "Step name", "bonus_movie"),
        **modelprop("duration", "integer", "Current duration in milliseconds (null if no tasks exist yet)", 456),
    }),
    "ModelsTask": basemodel({
        **modelprop("run_number", "integer", "Run number", 5),
        **modelprop("step_name", "string", "Step name", "bonus_movie"),
        **modelprop("task_id", "integer", "Task id", 32),
        **modelprop("foreach_label", "string", "Task foreach label (if applicable)", "123[0]"),
        **modelprop("status", "string", "Task status (completed/running)", "completed"),
        **modelprop("started_at", "integer", "Started at epoch timestamp of the attempt (if this could be inferred from the database, otherwise NULL)", None),
        **modelprop("finished_at", "integer", "Finished at epoch timestamp of the attempt", 1591788834035),
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
        **modelprop("content", "string", "Artifact value", None),
        **modelprop("ds_type", "string", "Datastore type", "local"),
        **modelprop("sha", "string", "SHA hash", "8de2c3d91a9384069a3b014fdc3b60b4eb68567c"),
        **modelprop("type", "string", "Type", "metaflow.artifact"),
        **modelprop("content_type", "string", "Content-type", "gzip+pickle-v2"),
        **modelprop("attempt_id", "integer", "Attempt id", 0),
    }),
    "ModelsCardInfo": {
        "type": "object",
        "properties": {
            **modelprop("type", "string", "Card type", "default"),
            **modelprop("hash", "string", "Card hash", "123456abcdef"),
        }
    },
    "ModelsLink": {
        "type": "object",
        "properties": {
            "href": {
                "type": "string",
                "description": "URL for the link"
            },
            "label": {
                "type": "string",
                "description": "Label for the link"
            }
        },
        "required": ["href", "label"]
    },
    "ModelsNotification": {
        "type": "object",
        "properties": {
            "id": {
                "type": "string",
                "description": "Notification identifier",
                "default": "Generated SHA1 hash"
            },
            "message": {
                "type": "string",
                "description": "Message to display (Markdown supported)"
            },
            "created": {
                "type": "integer",
                "description": "Notification created at (Epoch timestamp in milliseconds)",
                "default": None
            },
            "type": {
                "type": "string",
                "description": "Notification type, allowed values: success|info|warning|danger|default",
                "default": "info"
            },
            "contentType": {
                "type": "string",
                "description": "Message content-type, allowed values: text|markdown",
                "default": "text"
            },
            "url": {
                "type": "string",
                "description": "Notification url",
                "default": None
            },
            "urlText": {
                "type": "string",
                "description": "Human readable url title",
                "default": None
            },
            "start": {
                "type": "integer",
                "description": "Schedule notification to be visible starting at (Epoch timestamp in milliseconds)",
                "default": None
            },
            "end": {
                "type": "integer",
                "description": "Schedule notification to disappear after (Epoch timestamp in milliseconds)",
                "default": None
            }
        },
        "required": ["id", "message", "created", "type", "contentType", "url", "urlText", "start", "end"]
    },
    "ModelsPlugin": {
        "type": "object",
        "properties": {
            "identifier": {
                "type": "string",
                "description": "Plugin identifier (repository/folder name)"
            },
            "name": {
                "type": "string",
                "description": "Plugin name (manifest.json)"
            },
            "repository": {
                "type": "string",
                "description": "Git repository",
                "default": None
            },
            "ref": {
                "type": "string",
                "description": "Git refish to use e.g. origin/feature/extra-syrup",
                "default": None
            },
            "parameters": {
                "type": "object",
                "description": "Custom parameters to pass to the plugin"
            },
            "config": {
                "type": "object",
                "$ref": "#/definitions/ModelsPluginConfig",
                "description": "Plugin manifest.json",
                "default": {
                    "name": "plugin-name",
                    "version": "1.0.0",
                    "entrypoint": "index.html"
                }
            },
            "files": {
                "type": "array",
                "items": {
                    "type": "string"
                },
                "description": "List of files available for this plugin"
            }
        },
        "required": ["identifier", "name", "repository", "ref", "parameters", "config", "files"]
    },
    "ModelsPluginConfig": {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": "Plugin name"
            },
            "version": {
                "type": "string",
                "description": "Semantic version of this plugin"
            },
            "entrypoint": {
                "type": "string",
                "description": "Entrypoint for the plugin, relative or absolute. Loaded inside iframe with parameters"
            }
        },
        "required": ["name", "version", "entrypoint"]
    },
    "ModelsDag": {
        "type": "object",
        "properties": {
            "file": {
                "type": "string",
                "description": "Filename of the script used to execute the flow.",
                "required": False
            },
            "steps_info": {
                "type": "object",
                "$ref": "#/definitions/ModelsDagStepsInfo",
                "description": "DAG Step details"
            },
            "steps_structure": {
                "type": "object",
                "$ref": "#/definitions/ModelsDagStepsStructure",
                "description": "DAG Step structure"
            }
        }

    },
    # TODO: requires addition to type for nested arrays.
    "ModelsDagStepsStructure": {
        "type": "array",
        "items": {
            "anyOf": [
                {"type": "string"},
                {
                    "type": "object",
                    "$ref": "#/definitions/ModelsDagStepsStructure"}

            ]
        }
    },
    "ModelsDagStepsInfo": {
        "type": "object",
        "properties": {
            "start": {
                "type": "object",
                "$ref": "#/definitions/ModelsDagNode",
                "description": "First step of the flow"
            },
            "sample_step": {
                "type": "object",
                "$ref": "#/definitions/ModelsDagNode",
                "description": "One of many steps of the flow, name of the property can be anything, not just sample_step"
            },
            "end": {
                "type": "object",
                "$ref": "#/definitions/ModelsDagNode",
                "description": "Last step in the flow"
            }
        },
        "required": ["start", "end"]
    },
    "ModelsDagNode": {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": "Step name of the node"
            },
            "type": {
                "type": "string",
                "description": "DAG Node type"
            },
            "line": {
                "type": "integer",
                "description": "Line of code where step definition starts"
            },
            "doc": {
                "type": "string",
                "description": "DAG Node Docstring"
            },
            "decorators": {
                "type": "array",
                "items": {
                    "type": "string"
                },
                "required": False
            },
            "next": {
                "type": "array",
                "items": {
                    "type": "string"
                },
                "description": "names of next steps that follow from this step"
            },
        },
    },
    "ModelsLogRow": {
        "type": "object",
        "properties": {
            **modelprop("row", "int", "Row number", 0),
            **modelprop("timestamp", "int", "Epoch timestamp of the log line entry", 1591788834035),
            **modelprop("line", "string", "Log line content", "logged text")
        },
        "required": ["row", "line"]
    }
}

swagger_description = "Metaflow UI backend service"
