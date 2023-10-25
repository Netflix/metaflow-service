import json
import sys
import os
import traceback
from multidict import MultiDict
from urllib.parse import urlencode, quote
from aiohttp import web
from enum import Enum
from functools import wraps
from typing import Dict
import logging
import psycopg2
from packaging.version import Version, parse
from importlib import metadata


USE_SEPARATE_READER_POOL = os.environ.get("USE_SEPARATE_READER_POOL", "0") in ["True", "true", "1"]

version = metadata.version("metadata_service")

METADATA_SERVICE_VERSION = version
METADATA_SERVICE_HEADER = 'METADATA_SERVICE_VERSION'

# The latest commit hash of the repository, if set as an environment variable.
SERVICE_COMMIT_HASH = os.environ.get("BUILD_COMMIT_HASH", None)
# Build time of service, if set as an environment variable.
SERVICE_BUILD_TIMESTAMP = os.environ.get("BUILD_TIMESTAMP", None)

# Setup log level based on environment variable
log_level = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(level=log_level)

# E.g. set to http://localhost:3000 to allow CORS coming from a UI served from there
# Setting to '*' to be maximally loose.
ORIGIN_TO_ALLOW_CORS_FROM = os.environ.get('ORIGIN_TO_ALLOW_CORS_FROM', None)


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
    headers = MultiDict(
        {"Content-Type": "application/json",
         METADATA_SERVICE_HEADER: METADATA_SERVICE_VERSION})
    if not ORIGIN_TO_ALLOW_CORS_FROM:
        # The aiohttp-cors library actively asserts that this response header
        # is actively NOT set, before setting it to the original request's origin.
        #
        # Therefore, we only want to add this blanket response header iff ORIGIN_TO_ALLOW_CORS_FROM
        # is not configured (default).
        headers["Access-Control-Allow-Origin"] = "*"
    return web.Response(status=status,
                        body=json.dumps(body),
                        headers=headers)


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
    # Only get the first Forwarded-Host/Proto in case there are more than one
    scheme = scheme.split(",")[0].strip()
    host = host.split(",")[0].strip()
    baseurl = os.environ.get(
        "MF_BASEURL", "{scheme}://{host}".format(scheme=scheme, host=host))
    return "{baseurl}{path}".format(baseurl=baseurl, path=request.path)


def has_heartbeat_capable_version_tag(system_tags):
    """Check client version tag whether it is known to support heartbeats or not"""
    try:
        version_tags = [tag for tag in system_tags if tag.startswith('metaflow_version:')]
        version = parse(version_tags[0][17:])

        if version >= Version("1") and version < Version("2"):
            return version >= Version("1.14.0")

        return version >= Version("2.2.12")
    except Exception:
        return False

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


class DBType(Enum):
    # The DB host is a read replica
    READER = 1
    # The DB host is a writer instance
    WRITER = 2


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
                 ssl_mode: str = "disabled",
                 ssl_cert_path: str = None,
                 ssl_key_path: str = None,
                 ssl_root_cert_path: str = None,
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
        self._read_replica_host = \
            os.environ.get(prefix + "READ_REPLICA_HOST") if USE_SEPARATE_READER_POOL else self._host
        self._port = int(os.environ.get(prefix + "PORT", port))
        self._user = os.environ.get(prefix + "USER", user)
        self._password = os.environ.get(prefix + "PSWD", password)
        self._database_name = os.environ.get(prefix + "NAME", database_name)
        self._ssl_mode = os.environ.get(prefix + "SSL_MODE", ssl_mode)
        self._ssl_cert_path = os.environ.get(prefix + "SSL_CERT_PATH", ssl_cert_path)
        self._ssl_key_path = os.environ.get(prefix + "SSL_KEY_PATH", ssl_key_path),
        self._ssl_root_cert_path = os.environ.get(prefix + "SSL_ROOT_CERT_PATH", ssl_root_cert_path)
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

    def connection_string_url(self, type=None):
        # postgresql://[user[:password]@][host][:port][/dbname][?param1=value1&...]
        if type is None or type == DBType.WRITER:
            base_url = f'postgresql://{quote(self._user)}:{quote(self._password)}@{self._host}:{self._port}/{self._database_name}'
        elif type == DBType.READER:
            base_url = f'postgresql://{quote(self._user)}:{quote(self._password)}@{self._read_replica_host}:{self._port}/{self._database_name}'

        return f'{base_url}?{ssl_query}'

    def get_dsn(self, type=None):
        if self._dsn is None:
            ssl_mode = self._ssl_mode
            sslcert = self._ssl_cert_path
            sslkey = self._ssl_key_path
            sslrootcert = self._ssl_root_cert_path
            if (ssl_mode not in ['allow', 'prefer', 'require', 'verify-ca', 'verify-full']):
                ssl_mode = None
                sslcert = None
                sslkey = None
                sslrootcert = None
            kwargs = {
                'dbname': self._database_name,
                'user': self._user,
                'host': self._host,
                'port': self._port,
                'password': self._password,
                'sslmode': ssl_mode,
                'sslcert': sslcert,
                'sslkey': sslkey,
                'sslrootcert': sslrootcert
            }

            if type == DBType.READER:
                # We assume that everything except the hostname remains the same for a reader.
                # At the moment this is a fair assumption for Postgres read replicas.
                kwargs.update({"host":self._read_replica_host})


            return psycopg2.extensions.make_dsn(**{k: v for k, v in kwargs.items() if v is not None})
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

    @property
    def read_replica_host(self):
        return self._read_replica_host
