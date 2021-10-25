import json
import sys
import os
import traceback
import pkg_resources
from multidict import MultiDict
from urllib.parse import urlencode, quote
from aiohttp import web
from functools import wraps
from typing import Dict
import logging
import psycopg2

version = pkg_resources.require("metadata_service")[0].version

METADATA_SERVICE_VERSION = version
METADATA_SERVICE_HEADER = 'METADATA_SERVICE_VERSION'

# The latest commit hash of the repository, if set as an environment variable.
SERVICE_COMMIT_HASH = os.environ.get("BUILD_COMMIT_HASH", None)
# Build time of service, if set as an environment variable.
SERVICE_BUILD_TIMESTAMP = os.environ.get("BUILD_TIMESTAMP", None)

# Setup log level based on environment variable
log_level = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(level=log_level)


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


def http_500(msg, id, traceback_str=get_traceback_str()):
    # NOTE: worth considering if we want to expose tracebacks in the future in the api messages.
    body = {
        'id': id,
        'traceback': traceback_str,
        'detail': msg,
        'status': 500,
        'title': 'Internal Server Error',
        'type': 'about:blank'
    }

    return web_response(500, body)


def handle_exceptions(func):
    """Catch exceptions and return appropriate HTTP error."""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as err:
            # pass along an id for the error
            err_id = getattr(err, 'id', None)
            # either use provided traceback from subprocess, or generate trace from current process
            err_trace = getattr(err, 'traceback_str', None) or get_traceback_str()
            if not err_id:
                # Log error only in case it is not a known case.
                err_id = 'generic-error'
                logging.error(err_trace)
            return http_500(str(err), err_id, err_trace)

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


def web_response(status: int, body):
    return web.Response(status=status,
                        body=json.dumps(body),
                        headers=MultiDict(
                            {"Content-Type": "application/json",
                             "Access-Control-Allow-Origin": "*",
                             METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION}))


def format_qs(query: Dict[str, str], overwrite=None):
    q = dict(query)
    if overwrite:
        for key in overwrite:
            q[key] = overwrite[key]
    qs = urlencode(q, safe=':,')
    return ("?" if len(qs) > 0 else "") + qs


def format_baseurl(request: web.BaseRequest):
    scheme = request.headers.get("X-Forwarded-Proto") or request.scheme
    host = request.headers.get("X-Forwarded-Host") or request.host
    baseurl = os.environ.get(
        "MF_BASEURL", "{scheme}://{host}".format(scheme=scheme, host=host))
    return "{baseurl}{path}".format(baseurl=baseurl, path=request.path)


# Database configuration helper
# Prioritizes DSN string over individual connection arguments (host,user,...)
#
# Supports prefix for environment variables:
#   prefix=MF_METADATA_DB_ -> MF_METADATA_DB_USER=username
#
# Prioritizes configuration in following order:
#
#   1. Env DSN string (MF_METADATA_DB_DSN="...")
#   2. DSN string as argument (DBConfiguration(dsn="..."))
#   3. Env connection arguments (MF_METADATA_DB_HOST="..." MF_METADATA_DB...)
#   4. Default connection arguments (DBConfiguration(host="..."))
#


class DBConfiguration(object):
    host: str = None
    port: int = None
    user: str = None
    password: str = None
    database_name: str = None

    # aiopg default pool sizes
    # https://aiopg.readthedocs.io/en/stable/_modules/aiopg/pool.html#create_pool
    pool_min: int = None  # aiopg default: 1
    pool_max: int = None  # aiopg default: 10

    timeout: int = None  # aiopg default: 60 (seconds)

    _dsn: str = None

    def __init__(self,
                 dsn: str = None,
                 host: str = "localhost",
                 port: int = 5432,
                 user: str = "postgres",
                 password: str = "postgres",
                 database_name: str = "postgres",
                 prefix="MF_METADATA_DB_",
                 pool_min: int = 1,
                 pool_max: int = 10,
                 timeout: int = 60):

        self._dsn = os.environ.get(prefix + "DSN", dsn)
        # Check if it is a BAD DSN String.
        # if bad dsn string set self._dsn as None.
        if self._dsn is not None:
            if not self._is_valid_dsn(self._dsn):
                self._dsn = None
        self._host = os.environ.get(prefix + "HOST", host)
        self._port = int(os.environ.get(prefix + "PORT", port))
        self._user = os.environ.get(prefix + "USER", user)
        self._password = os.environ.get(prefix + "PSWD", password)
        self._database_name = os.environ.get(prefix + "NAME", database_name)
        conn_str_required_values = [
            self._host,
            self._port,
            self._user,
            self._password,
            self._database_name
        ]
        some_conn_str_values_missing = any(v is None for v in conn_str_required_values)
        if self._dsn is None and some_conn_str_values_missing:
            env_values = ', '.join([
                prefix + "HOST",
                prefix + "PORT",
                prefix + "USER",
                prefix + "PSWD",
                prefix + "NAME",
            ])
            dsn_var = prefix + "DSN"
            raise Exception(
                f"Some of the environment variables '{env_values}' are not set. "
                f"Please either set '{env_values}' or {dsn_var}.  "
            )

        self.pool_min = int(os.environ.get(prefix + "POOL_MIN", pool_min))
        self.pool_max = int(os.environ.get(prefix + "POOL_MAX", pool_max))

        self.timeout = int(os.environ.get(prefix + "TIMEOUT", timeout))

    @staticmethod
    def _is_valid_dsn(dsn):
        try:
            psycopg2.extensions.parse_dsn(dsn)
            return True
        except psycopg2.ProgrammingError:
            # This means that the DSN is unparsable.
            return None

    @property
    def connection_string_url(self):
        # postgresql://[user[:password]@][host][:port][/dbname][?param1=value1&...]
        return f'postgresql://{quote(self._user)}:{quote(self._password)}@{self._host}:{self._port}/{self._database_name}?sslmode=disable'

    @property
    def dsn(self):
        if self._dsn is None:
            return psycopg2.extensions.make_dsn(
                dbname=self._database_name,
                user=self._user,
                host=self._host,
                port=self._port,
                password=self._password
            )
        else:
            return self._dsn

    @property
    def port(self):
        return self._port

    @property
    def password(self):
        return self._password

    @property
    def user(self):
        return self._user

    @property
    def database_name(self):
        return self._database_name

    @property
    def host(self):
        return self._host
