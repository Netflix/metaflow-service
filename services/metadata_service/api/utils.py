import json
from functools import wraps

import collections
from aiohttp import web
from multidict import MultiDict
from importlib import metadata

from services.utils import get_traceback_str

version = metadata.version("metadata_service")
METADATA_SERVICE_VERSION = version
METADATA_SERVICE_HEADER = 'METADATA_SERVICE_VERSION'

ServiceResponse = collections.namedtuple("ServiceResponse", "response_code body")


def format_response(func):
    """handle formatting"""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        db_response = await func(*args, **kwargs)
        return web.Response(status=db_response.response_code,
                            body=json.dumps(db_response.body),
                            headers=MultiDict(
                                {METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION}))

    return wrapper


def web_response(status: int, body):
    return web.Response(status=status,
                        body=json.dumps(body),
                        headers=MultiDict(
                            {"Content-Type": "application/json",
                             METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION}))


def http_500(msg, traceback_str=None):
    # NOTE: worth considering if we want to expose tracebacks in the future in the api messages.
    if traceback_str is None:
        traceback_str = get_traceback_str()
    body = {
        'traceback': traceback_str,
        'detail': msg,
        'status': 500,
        'title': 'Internal Server Error',
        'type': 'about:blank'
    }

    return ServiceResponse(500, body)


def handle_exceptions(func):
    """Catch exceptions and return appropriate HTTP error."""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as err:
            return http_500(str(err))

    return wrapper
