import pytest
import os
import contextlib
import json
from aiohttp.test_utils import make_mocked_request
from services.utils import format_qs, format_baseurl, DBConfiguration, handle_exceptions

pytestmark = [pytest.mark.unit_tests]


def test_format_qs():
    qs = format_qs({
        "foo": "bar",
        "_tags": "runtime:dev",
        "status": "completed,running"
    })
    assert qs == "?foo=bar&_tags=runtime:dev&status=completed,running"


def test_format_baseurl():
    request = make_mocked_request(
        "GET", "/foo/bar?foo=bar", headers={"Host": "test"})
    baseurl = format_baseurl(request)
    assert baseurl == "http://test/foo/bar"


def test_format_baseurl_x_forwarded():
    request = make_mocked_request(
        "GET", "/foo/bar", headers={
            "Host": "test",
            "X-Forwarded-Proto": "proto",
            "X-Forwarded-Host": "proxy"})
    baseurl = format_baseurl(request)
    assert baseurl == "proto://proxy/foo/bar"


@contextlib.contextmanager
def set_env(environ={}):
    old_environ = dict(os.environ)
    os.environ.clear()
    os.environ.update(environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


def test_db_conf():
    with set_env():
        db_conf = DBConfiguration()
        assert db_conf.dsn == 'dbname=postgres user=postgres host=localhost port=5432 password=postgres'
        assert db_conf.pool_min == 1
        assert db_conf.pool_max == 10
        assert db_conf.timeout == 60


def test_db_conf_dsn():
    with set_env():
        assert DBConfiguration(dsn='user=foo').dsn == 'user=foo'


def test_db_conf_arguments():
    with set_env():
        db_conf = DBConfiguration(host='foo', port=1234, user='user', password='password', database_name='bar')
        assert db_conf.dsn == 'dbname=bar user=user host=foo port=1234 password=password'
        assert db_conf.host == 'foo'
        assert db_conf.port == 1234
        assert db_conf.user == 'user'
        assert db_conf.password == 'password'
        assert db_conf.database_name == 'bar'


def test_db_conf_env_default_prefix():
    with set_env({
        'MF_METADATA_DB_HOST': 'foo',
        'MF_METADATA_DB_PORT': '1234',
        'MF_METADATA_DB_USER': 'user',
        'MF_METADATA_DB_PSWD': 'password',
        'MF_METADATA_DB_NAME': 'bar',
        'MF_METADATA_DB_POOL_MIN': '2',
        'MF_METADATA_DB_POOL_MAX': '4',
        'MF_METADATA_DB_TIMEOUT': '5'
    }):
        db_conf = DBConfiguration()
        assert db_conf.dsn == 'dbname=bar user=user host=foo port=1234 password=password'
        assert db_conf.host == 'foo'
        assert db_conf.port == 1234
        assert db_conf.user == 'user'
        assert db_conf.password == 'password'
        assert db_conf.database_name == 'bar'
        assert db_conf.pool_min == 2
        assert db_conf.pool_max == 4
        assert db_conf.timeout == 5


def test_db_conf_env_custom_prefix():
    with set_env({
        'FOO_HOST': 'foo',
        'FOO_PORT': '1234',
        'FOO_USER': 'user',
        'FOO_PSWD': 'password',
        'FOO_NAME': 'bar',
        'FOO_POOL_MIN': '2',
        'FOO_POOL_MAX': '4',
        'FOO_TIMEOUT': '5'
    }):
        db_conf = DBConfiguration(prefix='FOO_')
        assert db_conf.dsn == 'dbname=bar user=user host=foo port=1234 password=password'
        assert db_conf.host == 'foo'
        assert db_conf.port == 1234
        assert db_conf.user == 'user'
        assert db_conf.password == 'password'
        assert db_conf.database_name == 'bar'
        assert db_conf.pool_min == 2
        assert db_conf.pool_max == 4
        assert db_conf.timeout == 5


def test_db_conf_env_dsn():
    with set_env({'MF_METADATA_DB_DSN': 'foo'}):
        # Should use default dsn with invalid dsn in environment
        assert DBConfiguration().dsn == 'dbname=postgres user=postgres host=localhost port=5432 password=postgres'

    with set_env({'MF_METADATA_DB_DSN': 'dbname=testgres user=test_user host=test_host port=1234 password=test_pwd'}):
        # valid DSN in env should set correctly.
        assert DBConfiguration().dsn == 'dbname=testgres user=test_user host=test_host port=1234 password=test_pwd'


def test_db_conf_pool_size():
    with set_env():
        db_conf = DBConfiguration(pool_min=2, pool_max=4)
        assert db_conf.pool_min == 2
        assert db_conf.pool_max == 4


def test_db_conf_timeout():
    with set_env():
        db_conf = DBConfiguration(timeout=5)
        assert db_conf.timeout == 5


async def test_handle_exceptions():
    class FakeException(Exception):
        def __init__(self, id, trace):
            self.id = id
            self.traceback_str = trace

    @handle_exceptions
    async def do_not_raise():
        return True

    @handle_exceptions
    async def raise_with_id():
        raise FakeException("test-id", "test-trace")

    @handle_exceptions
    async def raise_without_id():
        raise Exception()

    # wrapper should not touch successful calls.
    assert (await do_not_raise())

    # NOTE: aiohttp Response StringPayload only has the internal property _value for accessing the payload value.
    response_with_id = await raise_with_id()
    assert response_with_id.status == 500
    _body = json.loads(response_with_id.body._value)
    assert _body['id'] == 'test-id'
    assert _body['traceback'] == 'test-trace'

    response_without_id = await raise_without_id()
    assert response_without_id.status == 500
    _body = json.loads(response_without_id.body._value)
    assert _body['id'] == 'generic-error'
    assert _body['traceback'] is not None
