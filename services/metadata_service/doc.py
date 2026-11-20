"""
Swagger 2.0 components specific to the metadata service.

Shared parameters and base models come from ``services.utils.openapi_components``.
Responses are the raw serialized rows (no UI style ``{data, status}`` wrapper).
"""

from services.utils.openapi_components import (
    TAGS_PROPS,
    model,
    path_param,
    prop,
)

description = (
    "Metaflow metadata service. Stores and serves flows, runs, steps, tasks, "
    "metadata and artifacts for the Metaflow client."
)

parameters = {
    "path_artifact_name": path_param("artifact_name", "Artifact name"),
    "path_attempt_id": path_param("attempt_id", "Task attempt id", "integer"),
}

_USER_NAME = prop("user_name", "string", "Username", "john")


def _array_body(item_properties: dict, description: str) -> dict:
    return {
        "type": "array",
        "description": description,
        "items": model(item_properties),
    }


# The row models (Flow, Run, Step, Task, Metadata, Artifact) are the shared base
# models from services.utils.openapi_components.SHARED_DEFINITIONS, used as is; the
# generator merges them in, so they are not repeated here.
definitions = {
    # Request bodies, derived from what each handler reads off read_body().
    "CreateFlowBody": model({**_USER_NAME, **TAGS_PROPS}),
    "CreateRunBody": model(
        {
            **_USER_NAME,
            **prop(
                "run_number",
                "string",
                "Optional user supplied run id. Must not be numeric; a run number "
                "is generated either way.",
                "my-run",
            ),
            **TAGS_PROPS,
        }
    ),
    "CreateStepBody": model({**_USER_NAME, **TAGS_PROPS}),
    "CreateTaskBody": model(
        {
            **_USER_NAME,
            **prop(
                "task_id",
                "string",
                "Optional user supplied task name. Must not be numeric; a task id "
                "is generated either way.",
                "my-task",
            ),
            **TAGS_PROPS,
        }
    ),
    "MetadataBody": _array_body(
        {
            **prop("field_name", "string", "Field name", "attempt-done"),
            **prop("value", "string", "Value", "0"),
            **prop("type", "string", "Type", "attempt-done"),
            **_USER_NAME,
            **TAGS_PROPS,
        },
        "Metadata entries to persist for the task",
    ),
    "ArtifactBody": _array_body(
        {
            **prop("name", "string", "Artifact name", "bonus"),
            **prop("location", "string", "Datastore location of the artifact"),
            **prop("ds_type", "string", "Datastore type", "local"),
            **prop("sha", "string", "SHA hash"),
            **prop("type", "string", "Type", "metaflow.artifact"),
            **prop("content_type", "string", "Content-type", "gzip+pickle-v2"),
            **prop("attempt_id", "integer", "Attempt id", 0),
            **_USER_NAME,
            **TAGS_PROPS,
        },
        "Artifacts to persist for the task",
    ),
    "TagMutationBody": model(
        {
            "tags_to_add": {
                "type": "array",
                "items": {"type": "string"},
                "description": "User tags to add. Existing system tags are ignored.",
            },
            "tags_to_remove": {
                "type": "array",
                "items": {"type": "string"},
                "description": "User tags to remove. Removing a system tag is a 422.",
            },
        }
    ),
    # Other response bodies.
    "TagMutationResult": model(
        {
            "tags": {
                "type": "array",
                "items": {"type": "string"},
                "description": "User tags of the run after the mutation",
            }
        },
        required=["tags"],
    ),
    "Heartbeat": {
        "type": "string",
        "description": 'JSON encoded object, e.g. "{\\"wait_time_in_seconds\\": 10}". '
        "The body is double encoded for backwards compatibility.",
    },
    "MetadataCreated": model(
        prop("metadata_created", "integer", "Number of metadata rows created", 3),
        required=["metadata_created"],
    ),
    "ArtifactsCreated": model(
        prop("artifacts_created", "integer", "Number of artifact rows created", 3),
        required=["artifacts_created"],
    ),
    "Version": {
        "type": "string",
        "description": "Service version",
        "example": "2.6.1",
    },
    "HealthStatus": {
        "type": "string",
        "description": 'JSON encoded object, e.g. "{\\"status\\": \\"UP\\"}". '
        "The body is double encoded for backwards compatibility.",
    },
    "AwsCredentials": model(
        {
            **prop("aws_access_key_id", "string", "Access key id"),
            **prop("aws_secret_access_key", "string", "Secret access key"),
            **prop("aws_session_token", "string", "Session token"),
        },
        required=["aws_access_key_id", "aws_secret_access_key", "aws_session_token"],
    ),
}
