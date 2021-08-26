import asyncio
import os
from functools import wraps
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from services.ui_backend_service.features import FEATURE_S3_DISABLE

# S3 helpers


def wrap_s3_errors(func):
    """wrap s3 errors into custom error classes, support both sync and async functions"""

    if asyncio.iscoroutinefunction(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                res = await func(*args, **kwargs)
                return res
            except ClientError as ex:
                wrap_boto_client_error(ex)
            except NoCredentialsError:
                raise CacheS3CredentialsMissing
    else:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                res = func(*args, **kwargs)
                return res
            except ClientError as ex:
                wrap_boto_client_error(ex)
            except NoCredentialsError:
                raise CacheS3CredentialsMissing

    return wrapper


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("METAFLOW_S3_ENDPOINT_URL", None),
        verify=os.environ.get("METAFLOW_S3_VERIFY_CERTIFICATE", None))


@wrap_s3_errors
def get_s3_size(s3_client, location):
    "Gets the S3 object size for a location, by only fetching the HEAD"
    if FEATURE_S3_DISABLE:
        raise CacheS3Exception  # S3 is disabled, do not proceed.
    bucket, key = bucket_and_key(location)
    resp = s3_client.head_object(Bucket=bucket, Key=key)
    return resp['ContentLength']


@wrap_s3_errors
def get_s3_obj(s3_client, location):
    "Gets the s3 file from the given location and returns a temporary file object that will get deleted upon dereferencing."
    if FEATURE_S3_DISABLE:
        raise CacheS3Exception  # S3 is disabled, do not proceed.
    bucket, key = bucket_and_key(location)
    tmp = NamedTemporaryFile(prefix='ui_backend.cache.s3.')
    s3_client.download_file(bucket, key, tmp.name)
    return tmp


def bucket_and_key(location):
    "Parse S3 bucket name and the object key from a location"
    loc = urlparse(location)
    return loc.netloc, loc.path.lstrip('/')


def wrap_boto_client_error(err):
    "Wrap relevant botocore ClientError error codes as custom error classes and raise them"
    if err.response['Error']['Code'] in ['AccessDenied', '403']:
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
