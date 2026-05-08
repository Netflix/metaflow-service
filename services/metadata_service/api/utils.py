import json
from functools import wraps

import collections
from aiohttp import web
from multidict import MultiDict
from importlib import metadata

from services.utils import get_traceback_str

version = metadata.version("metadata_service")
METADATA_SERVICE_VERSION = version
METADATA_SERVICE_HEADER = "METADATA_SERVICE_VERSION"

# Pagination response headers
PAGINATION_LIMIT_HEADER = "X-Pagination-Limit"
TOTAL_COUNT_HEADER = "X-Total-Count"

ServiceResponse = collections.namedtuple("ServiceResponse", "response_code body")


def format_response(func):
    """
    Handle HTTP response formatting.

    Handlers may return either:
    - A plain DBResponse / ServiceResponse object (existing behavior, unchanged).
    - A 2-tuple (DBResponse, dict) where the dict contains extra HTTP headers
      to include in the response (e.g. pagination headers).

    In the tuple case, extra headers are only emitted on successful (2xx) responses.
    Error responses suppress the extra headers so callers don't misinterpret them.
    """

    @wraps(func)
    async def wrapper(*args, **kwargs):
        result = await func(*args, **kwargs)

        extra_headers = {}
        if type(result) is tuple and len(result) == 2 and isinstance(result[1], dict):
            db_response, extra = result
            # Only attach extra headers on success — suppress on error
            if db_response.response_code < 300:
                extra_headers = extra
        else:
            db_response = result

        header_pairs = [(METADATA_SERVICE_HEADER, METADATA_SERVICE_VERSION)]
        header_pairs += [(k, str(v)) for k, v in extra_headers.items()]

        return web.Response(
            status=db_response.response_code,
            body=json.dumps(db_response.body),
            headers=MultiDict(header_pairs),
        )

    return wrapper


def web_response(status: int, body):
    return web.Response(
        status=status,
        body=json.dumps(body),
        headers=MultiDict(
            {
                "Content-Type": "application/json",
                METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION,
            }
        ),
    )


def http_500(msg, traceback_str=None):
    if traceback_str is None:
        traceback_str = get_traceback_str()
    body = {
        "traceback": traceback_str,
        "detail": msg,
        "status": 500,
        "title": "Internal Server Error",
        "type": "about:blank",
    }
    return ServiceResponse(500, body)


def handle_exceptions(func):
    """Catch exceptions and return appropriate HTTP error."""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except web.HTTPClientError as ex:
            return ServiceResponse(ex.status_code, ex.reason)
        except Exception as err:
            return http_500(str(err))

    return wrapper
