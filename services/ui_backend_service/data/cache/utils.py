import os
import pickle
import json
from gzip import GzipFile
from itertools import islice
from contextlib import contextmanager
from typing import Callable, Tuple

from services.utils import get_traceback_str
from metaflow import DataArtifact

# Custom Cache errors


class DAGUnsupportedFlowLanguage(Exception):
    """Unsupported flow language for DAG parsing"""


class DAGParsingFailed(Exception):
    """Something went wrong while parsing the DAG"""

# Generic helpers


def batchiter(it, batch_size):
    it = iter(it)
    while True:
        batch = list(islice(it, batch_size))
        if batch:
            yield batch
        else:
            break


def decode(path):
    "decodes a gzip+pickle compressed object from a file path"
    with GzipFile(path) as f:
        obj = pickle.load(f)
        return obj


# Cache Action helpers


MAX_S3_SIZE = int(os.environ.get("MAX_PROCESSABLE_S3_ARTIFACT_SIZE_KB", 4)) * 1024

# Cache Key helpers


def artifact_cache_id(location):
    "construct a unique cache key for artifact location"
    return 'search:artifactdata:%s' % location


def artifact_location_from_key(x):
    "extract location from the artifact cache key"
    return x[len("search:artifactdata:"):]


def artifact_value(artifact: DataArtifact) -> Tuple[bool, object]:
    """
    Fetch the artifact value, return success along with value.
    Success will be false only in case the artifact size exceeds MAX_S3_SIZE.

    Returns
    -------
    tuple : (bool, object)
        (success, artifact.data)
    """
    if artifact.size < MAX_S3_SIZE:
        return (True, artifact.data)
    else:
        return (False, 'artifact-too-large', "{}: {} bytes".format(artifact.pathspec, artifact.size))


def cacheable_artifact_value(artifact: DataArtifact) -> str:
    """
    Access a DataArtifacts .data property, returning it along a success state as a stringified json.
    A failure will be returned if the artifact size is greater than the allowed MAX_S3_SIZE.

    Returns
    -------
    str
        successful:
        '[true, "some value"]'
        failure:
        '[false, "artifact-too-large", "flow/run/step/task/artifact: 1234 bytes"]'
    """
    return json.dumps(artifact_value(artifact))


def cacheable_exception_value(ex: Exception) -> str:
    """
    Returns a persistable json string representation of an Exception.
    Use this to have a predefined format for persisting exceptions in the cache, for non-recoverable
    exceptions that should not be tried again.

    Returns
    -------
    str
        example:
        '[false, "CustomException", "description of exception", "traceback that lead to the exception"]'
    """
    return json.dumps([False, ex.__class__.__name__, str(ex), get_traceback_str()])

# Cache action stream output helpers


@contextmanager
def streamed_errors(stream_output: Callable[[object], None], re_raise=True):
    """
    Context manager for running cache action processing and streaming possible errors
    to the stream_output

    Parameters
    ----------
    stream_output : Callable
        Cache action stream output callable

    re_raise : bool
        Default true. Whether to re-raise the caught error or not.
    """
    try:
        yield
    except Exception as ex:
        stream_output(
            error_event_msg(
                str(ex),
                ex.__class__.__name__,
                get_traceback_str()
            )
        )
        if re_raise:
            raise ex from None


def progress_event_msg(number):
    "formatter for cache action progress stream messages"
    return {
        "type": "progress",
        "fraction": number
    }


def error_event_msg(msg, id, traceback=None, key=None):
    "formatter for cache action error stream messages"
    return {
        "type": "error",
        "message": msg,
        "id": id,
        "traceback": traceback,
        "key": key
    }


def search_result_event_msg(results):
    "formatter for cache action search result message"
    return {
        "type": "result",
        "matches": results
    }


def unpack_pathspec_with_attempt_id(pathspec: str):
    """
    Extracts Metaflow Client compatible pathspec and attempt id.

    Parameters
    ----------
    pathspec : str
        Task or DataArtifact pathspec that includes attempt id as last component.
            - "FlowId/RunNumber/StepName/TaskId/0"
            - "FlowId/RunNumber/StepName/TaskId/ArtifactName/0"

    Returns
    -------
    Tuple with Metaflow Client compatible pathspec and attempt id.

    Example:
        "FlowId/RunNumber/StepName/TaskId/4" -> ("FlowId/RunNumber/StepName/TaskId", 4)
    """
    pathspec_without_attempt = '/'.join(pathspec.split('/')[:-1])
    attempt_id = int(pathspec.split('/')[-1])
    return (pathspec_without_attempt, attempt_id)
