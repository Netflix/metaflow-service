import json
import sys
import os
import traceback
from urllib.parse import urlencode, quote
from aiohttp import web
from typing import Dict
import logging
import psycopg2
from distutils.version import LooseVersion

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

def has_heartbeat_capable_version_tag(system_tags):
    """Check client version tag whether it is known to support heartbeats or not"""
    try:
        version_tags = [tag for tag in system_tags if tag.startswith('metaflow_version:')]
        version = LooseVersion(version_tags[0][17:])

        if version >= LooseVersion("1") and version < LooseVersion("2"):
            return version >= LooseVersion("1.14.0")

        return version >= LooseVersion("2.2.12")
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
