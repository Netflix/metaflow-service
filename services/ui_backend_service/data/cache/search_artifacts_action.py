import hashlib
import json

from .client import CacheAction
from services.utils import get_traceback_str
from .utils import (error_event_msg, progress_event_msg,
                    artifact_cache_id, unpack_pathspec_with_attempt_id,
                    MAX_S3_SIZE)
from ..refiner.refinery import unpack_processed_value
from services.ui_backend_service.api.utils import operators_to_filters


from metaflow import DataArtifact


class SearchArtifacts(CacheAction):
    """
    Fetches artifacts by pathspecs and performs a search against the object contents.
    Caches artifacts based on pathspec, and search results based on a combination of query&artifacts searched

    Parameters
    ----------
    pathspecs : List[str]
        A list of artifact pathspecs (with attempt id as last component)
            to fetch and match the search term against: ["FlowId/RunNumber/StepName/TaskId/ArtifactName/0"]
    searchterm : str
        A searchterm to match against the fetched S3 artifacts contents.

    Returns
    -------
    Dict or None
        example:
        {
            "pathspec": {
                "included": boolean,
                "matches": boolean
            }
        }
        matches: determines whether object content matched search term

        included: denotes if the object content was able to be included in the search (accessible or not)
    """

    @classmethod
    def format_request(cls, pathspecs, searchterm, operator="eq", invalidate_cache=False):
        msg = {
            'pathspecs': list(frozenset(sorted(pathspecs))),
            'searchterm': searchterm,
            'operator': operator
        }

        artifact_keys = []
        for pathspec in pathspecs:
            artifact_keys.append(artifact_cache_id(pathspec))

        request_id = lookup_id(pathspecs, searchterm, operator)
        stream_key = 'search:stream:%s' % request_id
        result_key = 'search:result:%s' % request_id

        return msg,\
            [result_key, *artifact_keys],\
            stream_key,\
            [stream_key, result_key],\
            invalidate_cache

    @classmethod
    def response(cls, keys_objs):
        """
        Action should respond with a dictionary of
        {
            "pathspec": {
                "matches": boolean,
                "included": boolean
            }
        }
        that tells the client whether the search term matches in the given pathspec, or if performing search was impossible
        """
        return [json.loads(val) for key, val in keys_objs.items() if key.startswith('search:result')][0]

    @classmethod
    def stream_response(cls, it):
        for msg in it:
            if msg is None:
                yield msg
            else:
                yield {'event': msg}

    @classmethod
    def execute(cls,
                message=None,
                keys=None,
                existing_keys={},
                stream_output=None,
                invalidate_cache=False,
                **kwargs):
        pathspecs = message['pathspecs']

        if invalidate_cache:
            results = {}
            pathspecs_to_fetch = [loc for loc in pathspecs]
        else:
            # make a copy of already existing results, as the cache action has to produce all keys it promised
            # in the format_request response.
            results = {**existing_keys}
            # Make a list of artifact pathspecs that require fetching (not cached previously)
            pathspecs_to_fetch = [loc for loc in pathspecs if not artifact_cache_id(loc) in existing_keys]

        artifact_keys = [key for key in keys if key.startswith('search:artifactdata')]
        result_key = [key for key in keys if key.startswith('search:result')][0]

        # Helper functions for streaming status updates.
        def stream_progress(num):
            return stream_output(progress_event_msg(num))

        def stream_error(err, id, traceback=None):
            return stream_output(error_event_msg(err, id, traceback))

        # Fetch artifacts that are not cached already
        for idx, pathspec in enumerate(pathspecs_to_fetch):
            stream_progress((idx + 1) / len(pathspecs_to_fetch))

            try:
                pathspec_without_attempt, attempt_id = unpack_pathspec_with_attempt_id(pathspec)
                artifact_key = "search:artifactdata:{}".format(pathspec)
                artifact = DataArtifact(pathspec_without_attempt, attempt=attempt_id)
                if artifact.size < MAX_S3_SIZE:
                    results[artifact_key] = json.dumps([True, artifact.data])
                else:
                    results[artifact_key] = json.dumps(
                        [False, 'artifact-too-large', "{}: {} bytes".format(artifact.pathspec, artifact.size)])
            except Exception as ex:
                stream_error(str(ex), ex.__class__.__name__, get_traceback_str())
                results[artifact_key] = json.dumps([False, ex.__class__.__name__, get_traceback_str()])

        # Perform search on loaded artifacts.
        search_results = {}
        searchterm = message['searchterm']
        operator = message['operator']
        filter_fn = operators_to_filters[operator] if operator in operators_to_filters else operators_to_filters["eq"]

        def format_loc(x):
            "extract pathspec from the artifact cache key"
            return x[len("search:artifactdata:"):]

        for key in artifact_keys:
            if key in results:
                load_success, value, detail = unpack_processed_value(json.loads(results[key]))
            else:
                load_success, value, _ = False, None, None
            # keep the matching case-insensitive
            matches = filter_fn(str(value).lower(), searchterm.lower())

            search_results[format_loc(key)] = {
                "included": load_success,
                "matches": matches,
                "error": None if load_success else {
                    "id": value or "artifact-handle-failed",
                    "detail": detail or "Unknown error during artifact processing"
                }
            }

        results[result_key] = json.dumps(search_results)

        return results


def lookup_id(locations, searchterm, operator):
    "construct a unique id to be used with stream_key and result_key"
    _string = "-".join(list(frozenset(sorted(locations)))) + searchterm + operator
    return hashlib.sha1(_string.encode('utf-8')).hexdigest()
