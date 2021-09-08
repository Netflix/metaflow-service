import hashlib
import json

from .client import CacheAction
from services.utils import get_traceback_str
from ..s3 import (
    S3AccessDenied, S3CredentialsMissing,
    S3NotFound, S3Exception,
    S3URLException, get_s3_size, get_s3_obj, get_s3_client)
from .utils import (decode, error_event_msg, progress_event_msg,
                    artifact_cache_id,
                    MAX_S3_SIZE)
from ..refiner.refinery import unpack_processed_value
from services.ui_backend_service.api.utils import operators_to_filters


class SearchArtifacts(CacheAction):
    """
    Fetches artifacts by locations and performs a search against the object contents.
    Caches artifacts based on location, and search results based on a combination of query&artifacts searched

    Parameters
    ----------
    locations : List[str]
        A list of artifact S3 locations to fetch and match the search term against.
    searchterm : str
        A searchterm to match against the fetched S3 artifacts contents.

    Returns
    -------
    Dict or None
        example:
        {
            "s3_location": {
                "included": boolean,
                "matches": boolean
            }
        }
        matches: determines whether object content matched search term

        included: denotes if the object content was able to be included in the search (accessible or not)
    """

    @classmethod
    def format_request(cls, locations, searchterm, operator="eq", invalidate_cache=False):
        unique_locs = list(frozenset(sorted(locations)))
        msg = {
            'artifact_locations': unique_locs,
            'searchterm': searchterm,
            'operator': operator
        }

        artifact_keys = []
        for location in unique_locs:
            artifact_keys.append(artifact_cache_id(location))

        request_id = lookup_id(unique_locs, searchterm, operator)
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
            location: {
                "matches": boolean,
                "included": boolean
            }
        }
        that tells the client whether the search term matches in the given location, or if performing search was impossible
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
        locations = message['artifact_locations']

        if invalidate_cache:
            results = {}
            locations_to_fetch = [loc for loc in locations]
        else:
            # make a copy of already existing results, as the cache action has to produce all keys it promised
            # in the format_request response.
            results = {**existing_keys}
            # Make a list of artifact locations that require fetching (not cached previously)
            locations_to_fetch = [loc for loc in locations if not artifact_cache_id(loc) in existing_keys]

        artifact_keys = [key for key in keys if key.startswith('search:artifactdata')]
        result_key = [key for key in keys if key.startswith('search:result')][0]

        # Helper functions for streaming status updates.
        def stream_progress(num):
            return stream_output(progress_event_msg(num))

        def stream_error(err, id, traceback=None):
            return stream_output(error_event_msg(err, id, traceback))

        # Fetch the S3 locations data
        s3_locations = [loc for loc in locations_to_fetch if loc.startswith("s3://")]
        s3 = get_s3_client()
        for idx, location in enumerate(s3_locations):
            artifact_key = artifact_cache_id(location)
            stream_progress((idx + 1) / len(s3_locations))
            try:
                obj_size = get_s3_size(s3, location)
                if obj_size < MAX_S3_SIZE:
                    temp_obj = get_s3_obj(s3, location)
                    # TODO: Figure out a way to store the artifact content without decoding?
                    # presumed that cache_data/tmp/ does not persist as long as the cached items themselves,
                    # so we can not rely on the file existing if we only return a filepath as a cached response
                    content = decode(temp_obj.name)
                    results[artifact_key] = json.dumps([True, content])
                else:
                    results[artifact_key] = json.dumps([False, 'artifact-too-large', obj_size])
            except TypeError:
                # In case the artifact was of a type that can not be json serialized,
                # we try casting it to a string first.
                results[artifact_key] = json.dumps([True, str(content)])
            except (S3AccessDenied, S3NotFound, S3URLException, S3CredentialsMissing) as ex:
                stream_error(str(ex), ex.id)
                results[artifact_key] = json.dumps([False, ex.id, location])
            except S3Exception as ex:
                stream_error(str(ex), ex.id, get_traceback_str())
                results[artifact_key] = json.dumps([False, ex.id, get_traceback_str()])
            except Exception as ex:
                # Exceptions might be fixable with configuration changes or other measures,
                # therefore we do not want to write anything to the cache for these artifacts.
                stream_error(str(ex), "artifact-handle-failed", get_traceback_str())
                results[artifact_key] = json.dumps([False, 'artifact-handle-failed', get_traceback_str()])
        # Skip the inaccessible locations
        other_locations = [loc for loc in locations_to_fetch if not loc.startswith("s3://")]
        for loc in other_locations:
            artifact_key = artifact_cache_id(loc)
            stream_error("Artifact is not accessible", "artifact-not-accessible")
            results[artifact_key] = json.dumps([False, 'object is not accessible', loc])

        # Perform search on loaded artifacts.
        search_results = {}
        searchterm = message['searchterm']
        operator = message['operator']
        filter_fn = operators_to_filters[operator] if operator in operators_to_filters else operators_to_filters["eq"]

        def format_loc(x):
            "extract location from the artifact cache key"
            return x[len("search:artifactdata:"):]

        for key in artifact_keys:
            if key in results:
                load_success, value, detail = unpack_processed_value(json.loads(results[key]))
            else:
                load_success, value, _ = False, None, None

            search_results[format_loc(key)] = {
                "included": load_success,
                "matches": filter_fn(value, searchterm),
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
