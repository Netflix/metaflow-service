import os
from aiocache import cached, Cache, caches
from aiocache.serializers import PickleSerializer
from ..features import FEATURE_CACHE_ENABLE, FEATURE_CACHE_DISABLE


default_config = {
    'default': {
        'cache': "aiocache.SimpleMemoryCache",
        'serializer': {
            'class': "aiocache.serializers.StringSerializer"
        }
    }
}

REDIS_ENDPOINT = os.environ.get("REDIS_HOST", None)
redis_config = {
    'default': {
        'cache': "aiocache.RedisCache",
        'endpoint': REDIS_ENDPOINT,
        'port': 6379,
        'serializer': {
            'class': "aiocache.serializers.PickleSerializer"
        }
    }
}

if REDIS_ENDPOINT and FEATURE_CACHE_ENABLE:
    caches.set_config(redis_config)

if FEATURE_CACHE_DISABLE:
    os.environ['AIOCACHE_DISABLE'] = "1"
