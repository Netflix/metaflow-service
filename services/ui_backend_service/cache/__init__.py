import os
from aiocache import cached, Cache, caches
from aiocache.serializers import PickleSerializer
from ..features import FEATURE_CACHE_ENABLE, FEATURE_CACHE_DISABLE


REDIS_ENDPOINT = os.environ.get("REDIS_HOST", None)
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
redis_config = {
    'default': {
        'cache': "aiocache.RedisCache",
        'endpoint': REDIS_ENDPOINT,
        'port': REDIS_PORT,
        'serializer': {
            'class': "aiocache.serializers.PickleSerializer"
        }
    }
}

if REDIS_ENDPOINT and FEATURE_CACHE_ENABLE:
    caches.set_config(redis_config)

if FEATURE_CACHE_DISABLE:
    os.environ['AIOCACHE_DISABLE'] = "1"
