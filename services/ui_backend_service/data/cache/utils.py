import pickle
from gzip import GzipFile
from itertools import islice
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

from botocore.exceptions import ClientError, NoCredentialsError


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

# S3 helpers

def get_s3_size(s3_client, location):
    "Gets the S3 object size for a location, by only fetching the HEAD"
    bucket, key = bucket_and_key(location)
    try:
        resp = s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as ex:
        wrap_boto_client_error(ex)
    except NoCredentialsError:
        raise CacheS3CredentialsMissing
    return resp['ContentLength']


def get_s3_obj(s3_client, location):
    "Gets the s3 file from the given location and returns a temporary file object that will get deleted upon dereferencing."
    bucket, key = bucket_and_key(location)
    tmp = NamedTemporaryFile(prefix='ui_backend.cache.s3.')
    try:
        s3_client.download_file(bucket, key, tmp.name)
    except ClientError as ex:
        wrap_boto_client_error(ex)
    except NoCredentialsError:
        raise CacheS3CredentialsMissing
    return tmp


def bucket_and_key(location):
    "Parse S3 bucket name and the object key from a location"
    loc = urlparse(location)
    return loc.netloc, loc.path.lstrip('/')


def wrap_boto_client_error(err):
    "Wrap relevant botocore ClientError error codes as custom error classes and raise them"
    if err.response['Error']['Code'] == 'AccessDenied':
        raise CacheS3AccessDenied
    elif err.response['Error']['Code'] == '404':
        raise CacheS3NotFound
    elif err.response['Error']['Code'] == 'NoSuchBucket':
        raise CacheS3URLException
    else:
        raise CacheS3Exception

# Custom error classes for S3 access
class CacheS3AccessDenied(Exception):
    "Access to the S3 object is denied"
    pass


class CacheS3NotFound(Exception):
    "Object could not be found in the bucket"
    pass


class CacheS3URLException(Exception):
    "Bucket does not exist"
    pass


class CacheS3CredentialsMissing(Exception):
    "S3 client is missing credentials"
    pass


class CacheS3Exception(Exception):
    "Generic S3 client exception"
    pass
