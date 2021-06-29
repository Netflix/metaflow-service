import hashlib
import json

from .client import CacheAction
from services.utils import get_traceback_str

from .utils import (MetaflowS3AccessDenied, MetaflowS3CredentialsMissing,
                    MetaflowS3Exception, MetaflowS3NotFound,
                    MetaflowS3URLException, NoRetryS3, batchiter, decode,
                    error_event_msg)

MAX_SIZE = 4096
S3_BATCH_SIZE = 512


class GetArtifacts(CacheAction):
    """
    Fetches artifacts by locations returning their contents.
    Caches artifacts based on location, and results based on list of artifacts requested.

    Parameters
    ----------
    locations : List[str]
        A list of S3 locations to fetch artifacts from.

    Returns
    -------
    Dict or None
        example:
        {
            "s3_location": "contents"
        }
    """

    @classmethod
    def format_request(cls, locations):
        unique_locs = list(frozenset(sorted(loc for loc in locations if isinstance(loc, str))))
        msg = {
            'artifact_locations': unique_locs,
        }

        artifact_keys = []
        for location in unique_locs:
            artifact_keys.append(artifact_cache_id(location))

        request_id = lookup_id(unique_locs)
        stream_key = 'parameters:stream:%s' % request_id

        return msg,\
            artifact_keys,\
            stream_key,\
            [stream_key]

    @classmethod
    def response(cls, keys_objs):
        '''Action should respond with a dictionary of
        {
            location: "contents"
        }
        '''

        artifact_keys = [(key, val) for key, val in keys_objs.items() if key.startswith('search:artifactdata')]

        collected = {}
        for key, val in artifact_keys:
            success, value = json.loads(val)

            # Only include artifacts whose content could successfully be read in the cache response.
            if success:
                collected[artifact_location_from_key(key)] = value

        return collected

    @classmethod
    def stream_response(cls, it):
        for msg in it:
            yield msg

    @classmethod
    def execute(cls,
                message=None,
                keys=None,
                existing_keys={},
                stream_output=None,
                **kwargs):
        """
        Execute fetching of artifacts from predefined S3 locations.
        Produces cache results that are either successful or failed.

        Success cached example:
            results[artifact_key] = json.dumps([True, str(content)])
        Failure cached example:
            results[artifact_key] = json.dumps([False, 'artifact-too-large', artifact_data.size])

        Alternatively errors can be streamed to cache client using `stream_error`.
        This way failures won't be cached for individual artifacts, thus making
        it necessary to retry fetching during next attempt. (Will add significant overhead/delay).

        Streaming errors should be avoided to minimize latency for subsequent requests.

        Stream error example:
            stream_error(str(ex), "s3-not-found", get_traceback_str(), artifact_key)
        """
        # make a copy of already existing results, as the cache action has to produce all keys it promised
        # in the format_request response.
        results = {**existing_keys}
        locations = message['artifact_locations']

        # Helper function for streaming status errors.
        #
        # Example usage with catched exception:
        #    stream_error(str(ex), "s3-not-found", get_traceback_str(), artifact_key)
        def stream_error(err, id, traceback=None, key=None):
            return stream_output(error_event_msg(err, id, traceback, key))

        # Make a list of artifact locations that require fetching (not cached previously)
        locations_to_fetch = [loc for loc in locations if not artifact_cache_id(loc) in existing_keys]

        # Fetch the S3 locations data
        s3_locations = [loc for loc in locations_to_fetch if loc.startswith('s3://')]
        with NoRetryS3() as s3:
            for locations in batchiter(s3_locations, S3_BATCH_SIZE):
                for location in locations:
                    artifact_key = artifact_cache_id(location)
                    try:
                        artifact_data = s3.get(location)
                        if artifact_data.size < MAX_SIZE:
                            # TODO: Figure out a way to store the artifact content without decoding?
                            # presumed that cache_data/tmp/ does not persist as long as the cached items themselves,
                            # so we can not rely on the file existing if we only return a filepath as a cached response
                            content = decode(artifact_data.path)
                            results[artifact_key] = json.dumps([True, content])
                        else:
                            # TODO: does this case need to raise an error as well? As is, the fetch simply fails silently.
                            results[artifact_key] = json.dumps([False, 'artifact-too-large', artifact_data.size])

                    except TypeError:
                        # In case the artifact was of a type that can not be json serialized,
                        # we try casting it to a string first.
                        results[artifact_key] = json.dumps([True, str(content)])
                    except MetaflowS3AccessDenied as ex:
                        results[artifact_key] = json.dumps([False, 's3-access-denied', location])
                    except MetaflowS3NotFound as ex:
                        results[artifact_key] = json.dumps([False, 's3-not-found', location])
                    except MetaflowS3URLException as ex:
                        results[artifact_key] = json.dumps([False, 's3-bad-url', location])
                    except MetaflowS3CredentialsMissing as ex:
                        results[artifact_key] = json.dumps([False, 's3-missing-credentials', location])
                    except MetaflowS3Exception as ex:
                        results[artifact_key] = json.dumps([False, 's3-generic-error', get_traceback_str()])
                    except Exception as ex:
                        results[artifact_key] = json.dumps([False, 'artifact-handle-failed', get_traceback_str()])

        # Skip the inaccessible locations
        other_locations = [loc for loc in locations_to_fetch if not loc.startswith('s3://')]
        for loc in other_locations:
            artifact_key = artifact_cache_id(loc)
            results[artifact_key] = json.dumps([False, 'artifact-not-accessible', loc])

        return results


def lookup_id(locations):
    "construct a unique id to be used with stream_key and result_key"
    _string = "-".join(list(frozenset(sorted(locations))))
    return hashlib.sha1(_string.encode('utf-8')).hexdigest()


def artifact_cache_id(location):
    "construct a unique cache key for artifact location"
    return 'search:artifactdata:%s' % location


def artifact_location_from_key(x):
    "extract location from the artifact cache key"
    return x[len("search:artifactdata:"):]
