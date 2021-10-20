import os
import pickle
from gzip import GzipFile
from itertools import islice

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


def read_as_string(path):
    "reads file contents as a string"
    with open(path, "r") as f:
        return f.read()

# Cache Action helpers


MAX_S3_SIZE = int(os.environ.get("MAX_PROCESSABLE_S3_ARTIFACT_SIZE_KB", 4)) * 1024

# Cache Key helpers


def artifact_cache_id(location):
    "construct a unique cache key for artifact location"
    return 'search:artifactdata:%s' % location


def artifact_location_from_key(x):
    "extract location from the artifact cache key"
    return x[len("search:artifactdata:"):]


# Cache action stream output helpers


class StreamedCacheError(Exception):
    "Used for custom raises during cache action stream errors"
    pass


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
