# from metaflow.datatools.s3 import MetaflowS3AccessDenied, MetaflowS3Exception, MetaflowS3NotFound, MetaflowS3URLException, MetaflowException
from botocore.exceptions import NoCredentialsError, ClientError
import os
import pickle
from urllib.parse import urlparse
import gzip
from itertools import islice
from services.utils import logging

from aiocache import cached, Cache, caches
from aiocache.serializers import PickleSerializer

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
    "decodes a gzip+pickle compressed object from a file path"
    content = gzip.decompress(fileobj)
    obj = pickle.loads(content)
    return obj


@cached(cache=Cache.REDIS, serializer=PickleSerializer(), endpoint=os.environ.get("REDIS_HOST"))
async def get_artifact(cli, location):
    # TODO: Cache the output!
    # TODO: Turn this async.
    # TODO: Have this raise an error on too big of an artifact!
    url = urlparse(location, allow_fragments=False)
    bucket = url.netloc
    path = url.path.lstrip('/')
    head = await cli.head_object(Bucket=bucket, Key=path)
    size = await head['ContentLength']
    if size > MAX_SIZE:
        raise S3ObjectTooBig

    art = await cli.get_object(Bucket=bucket, Key=path)
    body = await art['Body'].read()
    return body


@cached(cache=Cache.REDIS, serializer=PickleSerializer(), endpoint=os.environ.get("REDIS_HOST"))
async def get_codepackage(cli, location):
    # TODO: Cache the output!
    # TODO: Turn this async.
    # TODO: Have this raise an error on too big of an artifact!
    url = urlparse(location, allow_fragments=False)
    bucket = url.netloc
    path = url.path.lstrip('/')
    art = await cli.get_object(Bucket=bucket, Key=path)
    body = await art['Body'].read()
    return body


class S3ObjectTooBig(Exception):
    def __str__(self):
        return "S3 Object being fetched is above the allowed threshold."
