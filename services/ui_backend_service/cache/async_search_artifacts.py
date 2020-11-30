import hashlib

# from .utils import MetaflowS3CredentialsMissing, MetaflowS3AccessDenied, MetaflowS3Exception, MetaflowS3NotFound, MetaflowS3URLException
from .utils import decode, batchiter, get_artifact
import json
import aiobotocore

MAX_SIZE = 4096
S3_BATCH_SIZE = 512


async def search_artifacts(locations, searchterm):
    # TODO: CACHE output.
    '''
        Fetches artifacts by locations and performs a search against the object contents.
        Caches artifacts based on location, and search results based on a combination of query&artifacts searched

        Returns:
            {
                "s3_location": {
                    "included": boolean,
                    "matches": boolean
                }
            }
        matches: determines whether object content matched search term

        included: denotes if the object content was able to be included in the search (accessible or not)
        '''

    # Helper functions for streaming status updates.
    # def stream_progress(num):
    #     return stream_output({"type": "progress", "fraction": num})

    # def stream_error(err, id):
    #     return stream_output({"type": "error", "message": err, "id": id})

    # Make a list of artifact locations that require fetching (not cached previously)
    # locations_to_fetch = [loc for loc in locations if not artifact_cache_id(loc) in existing_keys]

    # Fetch the S3 locations data
    s3_locations = [loc for loc in locations if loc.startswith("s3://")]
    num_s3_batches = max(1, len(locations) // S3_BATCH_SIZE)
    fetched = {}
    session = aiobotocore.get_session()
    async with session.create_client('s3') as s3_client:
        for locations in batchiter(s3_locations, S3_BATCH_SIZE):
            try:
                for location in locations:
                    # if artifact_data.size < MAX_SIZE:
                    try:
                        artifact_data = get_artifact(s3_client, location)  # this should preferrably hit a cache.
                        # TODO: Figure out a way to store the artifact content without decoding?
                        # presumed that cache_data/tmp/ does not persist as long as the cached items themselves,
                        # so we can not rely on the file existing if we only return a filepath as a cached response
                        content = decode(artifact_data.path)
                        fetched[location] = json.dumps([True, content])
                    except TypeError:
                        # In case the artifact was of a type that can not be json serialized,
                        # we try casting it to a string first.
                        fetched[location] = json.dumps([True, str(content)])
                    except Exception as ex:
                        # Exceptions might be fixable with configuration changes or other measures,
                        # therefore we do not want to write anything to the cache for these artifacts.
                        print("exception happened when parsing artifact content", flush=True)
                        # stream_error(str(ex), "artifact-handle-failed", get_traceback_str())
                    # else:
                    #     results[artifact_key] = json.dumps([False, 'object is too large'])
            except Exception:
                print("Exception unknown...")
            # except MetaflowS3AccessDenied as ex:
            #     stream_error(str(ex), "s3-access-denied")
            # except MetaflowS3NotFound as ex:
            #     stream_error(str(ex), "s3-not-found")
            # except MetaflowS3URLException as ex:
            #     stream_error(str(ex), "s3-bad-url")
            # except MetaflowS3CredentialsMissing as ex:
            #     stream_error(str(ex), "s3-missing-credentials")
            # except MetaflowS3Exception as ex:
            #     stream_error(str(ex), "s3-generic-error", get_traceback_str())
    # Skip the inaccessible locations
    other_locations = [loc for loc in locations if not loc.startswith("s3://")]
    for loc in other_locations:
        # stream_error("Artifact is not accessible", "artifact-not-accessible")
        fetched[loc] = json.dumps([False, 'object is not accessible'])

    # Perform search on loaded artifacts.
    search_results = {}

    for key in locations:
        if key in fetched:
            load_success, value = json.loads(fetched[key])
        else:
            load_success, value = False, None

        search_results[key] = {
            "included": load_success,
            "matches": str(value) == searchterm
        }

    return json.dumps(search_results)
