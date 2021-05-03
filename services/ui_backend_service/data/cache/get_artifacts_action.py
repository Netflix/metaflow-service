import hashlib
import json

from metaflow.client.cache import CacheAction
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

        # make a copy of already existing results, as the cache action has to produce all keys it promised
        # in the format_request response.
        results = {**existing_keys}
        locations = message['artifact_locations']

        # Helper function for streaming status updates.
        def stream_error(err, id, traceback=None):
            return stream_output(error_event_msg(err, id, traceback))

        # Make a list of artifact locations that require fetching (not cached previously)
        locations_to_fetch = [loc for loc in locations if not artifact_cache_id(loc) in existing_keys]

        # Fetch the S3 locations data
        s3_locations = [loc for loc in locations_to_fetch if loc.startswith("s3://")]
        with NoRetryS3() as s3:
            for locations in batchiter(s3_locations, S3_BATCH_SIZE):
                try:
                    for artifact_data in s3.get_many(locations):
                        artifact_key = artifact_cache_id(artifact_data.url)
                        if artifact_data.size < MAX_SIZE:
                            try:
                                # TODO: Figure out a way to store the artifact content without decoding?
                                # presumed that cache_data/tmp/ does not persist as long as the cached items themselves,
                                # so we can not rely on the file existing if we only return a filepath as a cached response
                                content = decode(artifact_data.path)
                                results[artifact_key] = json.dumps([True, content])
                            except TypeError:
                                # In case the artifact was of a type that can not be json serialized,
                                # we try casting it to a string first.
                                results[artifact_key] = json.dumps([True, str(content)])
                            except Exception as ex:
                                # Exceptions might be fixable with configuration changes or other measures,
                                # therefore we do not want to write anything to the cache for these artifacts.
                                stream_error(str(ex), "artifact-handle-failed", get_traceback_str())
                        else:
                            # TODO: does this case need to raise an error as well? As is, the fetch simply fails silently.
                            results[artifact_key] = json.dumps([False, 'object is too large'])
                except MetaflowS3AccessDenied as ex:
                    stream_error(str(ex), "s3-access-denied")
                except MetaflowS3NotFound as ex:
                    stream_error(str(ex), "s3-not-found")
                except MetaflowS3URLException as ex:
                    stream_error(str(ex), "s3-bad-url")
                except MetaflowS3CredentialsMissing as ex:
                    stream_error(str(ex), "s3-missing-credentials")
                except MetaflowS3Exception as ex:
                    stream_error(str(ex), "s3-generic-error", get_traceback_str())
        # Skip the inaccessible locations
        other_locations = [loc for loc in locations_to_fetch if not loc.startswith("s3://")]
        for loc in other_locations:
            stream_error("Artifact is not accessible at URL: {}".format(loc), "artifact-not-accessible")

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
