import pytest
from aiohttp.test_utils import make_mocked_request
from services.utils import format_qs, format_baseurl

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
