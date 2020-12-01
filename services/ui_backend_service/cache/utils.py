# from metaflow.datatools.s3 import MetaflowS3AccessDenied, MetaflowS3Exception, MetaflowS3NotFound, MetaflowS3URLException, MetaflowException
from botocore.exceptions import NoCredentialsError, ClientError
import os
import pickle
from urllib.parse import urlparse
import gzip
from itertools import islice
from services.utils import logging
from . import cached

from ..features import FEATURE_S3_DISABLE
MAX_SIZE = 4096

logger = logging.getLogger("Cache.Utils")


def batchiter(it, batch_size):
    it = iter(it)
    while True:
        batch = list(islice(it, batch_size))
        if batch:
            yield batch
        else:
            break


def decode(fileobj):
    "decodes a gzip+pickle compressed object from a bytestring"
    content = gzip.decompress(fileobj)
    obj = pickle.loads(content)
    return obj


@cached()
async def get_artifact(cli, location):
    # TODO: test that the cache key does not depend on passed in s3_client
    url = urlparse(location, allow_fragments=False)
    bucket = url.netloc
    path = url.path.lstrip('/')
    head = await cli.head_object(Bucket=bucket, Key=path)
    if head['ContentLength'] > MAX_SIZE:
        raise S3ObjectTooBig

    art = await cli.get_object(Bucket=bucket, Key=path)
    body = await art['Body'].read()
    return body


@cached()
async def get_codepackage(cli, location):
    # TODO: test that the cache key does not depend on passed in s3_client
    url = urlparse(location, allow_fragments=False)
    bucket = url.netloc
    path = url.path.lstrip('/')
    art = await cli.get_object(Bucket=bucket, Key=path)
    body = await art['Body'].read()
    return body


class S3ObjectTooBig(Exception):
    def __str__(self):
        return "S3 Object being fetched is above the allowed threshold."
