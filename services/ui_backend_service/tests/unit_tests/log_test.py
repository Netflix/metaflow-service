import pytest
import collections

from services.ui_backend_service.api.log import paginate_log_lines

pytestmark = [pytest.mark.unit_tests]

TEST_LOG = list("log line {}".format(i) for i in range(1, 1001))


@pytest.fixture
def req():
    "A Dummy request for testing with query parameters"
    req = collections.namedtuple("Request", ["scheme", "host", "path", "headers", "query"])
    req.headers = {}
    req.query = {}
    return req


async def test_log_lines_pagination(req):
    _, body = paginate_log_lines(req, TEST_LOG)

    # 1000 lines should fit in default pagination
    assert len(body['data']) == 1000
    # order should be reverse
    assert body['data'][0] == "log line 1000"


async def test_log_lines_pagination_oob_page(req):
    req.query["_page"] = 2
    _, body = paginate_log_lines(req, TEST_LOG)

    assert len(body['data']) == 0


async def test_log_lines_pagination_with_limit(req):
    req.query = {
        "_limit": 5,
        "_page": 2
    }

    _, body = paginate_log_lines(req, TEST_LOG)

    assert len(body['data']) == 5
    assert body['data'] == list("log line {}".format(i) for i in range(991, 996))[::-1]
