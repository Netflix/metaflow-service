import os
from functools import wraps
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from metaflow.exception import MetaflowException, MetaflowNotFound
from services.ui_backend_service.features import FEATURE_S3_DISABLE
from metaflow.datatools.s3 import (
    MetaflowS3Exception, MetaflowS3URLException, MetaflowS3NotFound,
    MetaflowS3AccessDenied, MetaflowS3InvalidObject
)
# S3 helpers


def wrap_s3_errors(func):
    """wrap s3 errors into custom error classes"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            res = func(*args, **kwargs)
            return res
        except ClientError as ex:
            wrap_boto_client_error(ex)
        except NoCredentialsError:
            raise S3CredentialsMissing

    return wrapper


def wrap_metaflow_s3_errors(func):
    """wrap s3 errors into custom error classes"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            res = func(*args, **kwargs)
            return res
        except MetaflowS3AccessDenied:
            raise S3AccessDenied
        except MetaflowS3URLException:
            raise S3URLException
        except MetaflowNotFound:
            raise S3NotFound
        except MetaflowException as ex:
            err = str(ex)
            if "Unable to locate credentials" in err:
                # TODO: metaflow client should raise the credentials missing error separately,
                # instead of hiding it under MetaflowException. Would make this wrapper more robust as well.
                raise S3CredentialsMissing
            else:
                raise S3Exception(msg=err)

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
        raise S3Exception  # S3 is disabled, do not proceed.
    bucket, key = bucket_and_key(location)
    resp = s3_client.head_object(Bucket=bucket, Key=key)
    return resp['ContentLength']


@wrap_s3_errors
def get_s3_obj(s3_client, location):
    "Gets the s3 file from the given location and returns a temporary file object that will get deleted upon dereferencing."
    if FEATURE_S3_DISABLE:
        raise S3Exception  # S3 is disabled, do not proceed.
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
        raise S3AccessDenied
    elif err.response['Error']['Code'] == '404':
        raise S3NotFound
    elif err.response['Error']['Code'] == 'NoSuchBucket':
        raise S3URLException
    else:
        raise S3Exception


# Custom error classes for S3 access

class S3Exception(Exception):
    def __init__(self):
        self.message = "Generic S3 client exception"
        self.id = "s3-generic-exception"

    def __str__(self):
        return self.message


class S3AccessDenied(S3Exception):
    def __init__(self):
        self.message = "Access to the S3 object is denied"
        self.id = "s3-access-denied"


class S3NotFound(S3Exception):

    def __init__(self):
        self.message = "Object could not be found in the bucket"
        self.id = "s3-not-found"


class S3URLException(S3Exception):
    def __init__(self):
        self.message = "Bucket does not exist"
        self.id = "s3-bad-url"


class S3CredentialsMissing(S3Exception):
    def __init__(self):
        self.message = "S3 client is missing credentials"
        self.id = "s3-missing-credentials"
