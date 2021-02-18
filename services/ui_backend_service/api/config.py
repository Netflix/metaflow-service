import os
from services.utils import handle_exceptions, web_response

from ..features import get_features

# These environment values will be available to the frontend
ALLOWED_CONFIG_KEYS = [
    'GA_TRACKING_ID'
]


class ConfigApi(object):
    def __init__(self, app):
        app.router.add_route('GET', '/config', self.get_config)

    @handle_exceptions
    async def get_config(self, request):
        config = {}
        for key in ALLOWED_CONFIG_KEYS:
            val = os.environ.get(key, None)
            if val:
                config[key] = val
        return web_response(200, config)
