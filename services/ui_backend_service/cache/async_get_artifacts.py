import asyncio
import hashlib
import json
import os
from .utils import decode, batchiter, get_artifact, S3ObjectTooBig
from services.utils import logging
from botocore.exceptions import ClientError
from . import cached

S3_BATCH_SIZE = 512
TTL = os.environ.get("BULK_ARTIFACT_GET_CACHE_TTL_SECONDS", 60 * 60 * 24)  # Default TTL to one day

logger = logging.getLogger("GetArtifacts")
semaphore = asyncio.BoundedSemaphore(100)


def cache_artifacts_key(function, session, locations):
    "cache key generator for bulk artifact get results. Used to keep the cache keys as short as possible"
    _string = "-".join(locations)
    return "getartifacts:{}".format(hashlib.sha1(_string.encode('utf-8')).hexdigest())


@cached(ttl=TTL, alias="default", key_builder=cache_artifacts_key)
async def get_artifacts(boto_session, locations):
    '''
    Fetches artifacts by locations returning their contents.
    Caches artifacts based on location, and results based on list of artifacts requested.

    Returns:
        {
            "s3_location": "contents"
        }
    '''

    # Fetch the S3 locations data
    s3_locations = [loc for loc in locations if loc.startswith("s3://")]
    fetched = {}
    async with boto_session.create_client('s3') as s3_client:
        for locations in batchiter(s3_locations, S3_BATCH_SIZE):
            try:
                for location in locations:
                    async with semaphore:
                        artifact_data = await get_artifact(s3_client, location)  # this should preferrably hit a cache.
                        try:
                            content = decode(artifact_data)
                            fetched[location] = json.dumps([True, content])
                        except TypeError:
                            # In case the artifact was of a type that can not be json serialized,
                            # we try casting it to a string first.
                            fetched[location] = json.dumps([True, str(content)])
                        except S3ObjectTooBig:
                            fetched[location] = json.dumps([False, 'object is too large'])
                        except Exception:
                            # Exceptions might be fixable with configuration changes or other measures,
                            # therefore we do not want to write anything to the cache for these artifacts.
                            logger.exception("exception during parsing")
            except ClientError:
                logger.exception("S3 access failed.")
            except Exception:
                logger.exception("Unknown Exception")
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

    # Collect the artifact contents into the results.
    collected = {}

    for key in locations:
        if key in fetched:
            success, value = json.loads(fetched[key])
        else:
            success, value = False, None

        if success:
            collected[key] = value

    return collected
