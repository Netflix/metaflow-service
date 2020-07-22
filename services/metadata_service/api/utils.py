import json
import sys
import traceback
import collections
from multidict import MultiDict
from aiohttp import web
from functools import wraps
from . import METADATA_SERVICE_VERSION, METADATA_SERVICE_HEADER

Response = collections.namedtuple("Response", "response_code body")


async def read_body(request_content):
    byte_array = bytearray()
    while not request_content.at_eof():
        data = await request_content.read(4)
        byte_array.extend(data)

    return json.loads(byte_array.decode("utf-8"))


def get_traceback_str():
    """Get the traceback as a string."""

    exc_info = sys.exc_info()
    stack = traceback.extract_stack()
    _tb = traceback.extract_tb(exc_info[2])
    full_tb = stack[:-1] + _tb
    exc_line = traceback.format_exception_only(*exc_info[:2])
    return "\n".join(
        [
            "Traceback (most recent call last):",
            "".join(traceback.format_list(full_tb)),
            "".join(exc_line),
        ]
    )

def http_500(msg):
    body = {
        'traceback': get_traceback_str(),
        'detail': msg,
        'status': 500,
        'title': 'Internal Server Error',
        'type': 'about:blank'
    }

    return Response(response_code=500, body=body)


def handle_exceptions(func):
    """Catch exceptions and return appropriate HTTP error."""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as err:
            return http_500(str(err))

    return wrapper


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

