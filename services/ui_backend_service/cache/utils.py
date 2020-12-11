from botocore.exceptions import NoCredentialsError, ClientError
import os
import pickle
from urllib.parse import urlparse
import gzip
from itertools import islice
from services.utils import logging
from . import cached
from ..api.utils import wrap_s3_errors

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


# NOTE: caching requires the key_builder because we do not want to have the s3_client instance
# as part of the cache key.
@cached(alias="default", key_builder=lambda func, cli, loc: "artifact:{}".format(loc))
@wrap_s3_errors
async def get_artifact(cli, location):
    url = urlparse(location, allow_fragments=False)
    bucket = url.netloc
    path = url.path.lstrip('/')
    head = await cli.head_object(Bucket=bucket, Key=path)
    if head['ContentLength'] > MAX_SIZE:
        raise S3ObjectTooBig

    art = await cli.get_object(Bucket=bucket, Key=path)
    async with art['Body'] as stream:
        body = await stream.read()
        return body


# NOTE: caching requires the key_builder because we do not want to have the s3_client instance
# as part of the cache key.
@cached(alias="default", key_builder=lambda func, cli, loc: "codepackage:{}".format(loc))
@wrap_s3_errors
async def get_codepackage(cli, location):
    url = urlparse(location, allow_fragments=False)
    bucket = url.netloc
    path = url.path.lstrip('/')
    art = await cli.get_object(Bucket=bucket, Key=path)
    async with art['Body'] as stream:
        body = await stream.read()
        return body


class S3ObjectTooBig(Exception):
    def __str__(self):
        return "S3 Object being fetched is above the allowed threshold."
