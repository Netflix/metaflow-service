import pytest
from services.data.db_utils import DBResponse, DBPagination
import json

from aiohttp.test_utils import make_mocked_request

from services.ui_backend_service.api.utils import (
    format_response, format_response_list,
    pagination_query,
    builtin_conditions_query,
    custom_conditions_query,
    resource_conditions
)

pytestmark = [pytest.mark.unit_tests]


def test_format_response():
    request = make_mocked_request(
        'GET', '/runs?_limit=10', headers={'Host': 'test'})

    db_response = DBResponse(
        response_code=200, body={"foo": "bar"})

    expected_response = {
        "data": {"foo": "bar"},
        "status": 200,
        "links": {
            "self": "http://test/runs?_limit=10"
        },
        "query": {"_limit": "10"},
    }

    status, response = format_response(request, db_response)
    assert json.dumps(response) == json.dumps(expected_response)
    assert status == 200


def test_format_response_list():
    request = make_mocked_request(
        'GET', '/runs?_limit=10&_page=2', headers={'Host': 'test'})

    db_response = DBResponse(
        response_code=200, body=[{"foo": "bar"}])

    expected_response = {
        "data": [{"foo": "bar"}],
        "status": 200,
        "links": {
            "self": "http://test/runs?_limit=10&_page=2",
            "first": "http://test/runs?_limit=10&_page=1",
            "prev": "http://test/runs?_limit=10&_page=1",
            "next": "http://test/runs?_limit=10&_page=3"
        },
        "pages": {
            "self": 2,
            "first": 1,
            "prev": 1,
            "next": 3
        },
        "query": {
            "_limit": "10",
            "_page": "2"
        },
    }

    status, response = format_response_list(request, db_response, 2)
    assert json.dumps(response) == json.dumps(expected_response)
    assert status == 200


def test_pagination_query_defaults():
    request = make_mocked_request('GET', '/runs')

    page, limit, offset, order, groups, group_limit = pagination_query(request=request)

    assert page == 1
    assert limit == 10
    assert offset == 0
    assert order == None
    assert groups == None
    assert group_limit == 10


def test_pagination_query_custom():
    request = make_mocked_request(
        'GET', '/runs?_limit=5&_page=3&_order=foo&_group=bar')

    page, limit, offset, order, groups, group_limit = pagination_query(
        request=request, allowed_order=["foo"],
        allowed_group=["bar"])

    assert page == 3
    assert limit == 5
    assert offset == 10
    assert order == ["foo DESC"]
    assert groups == ["bar"]
    assert group_limit == 10


def test_pagination_query_custom_order_asc():
    request = make_mocked_request('GET', '/runs?_order=%2Bfoo')

    _, _, _, order, _, _ = pagination_query(
        request=request, allowed_order=["foo"])

    assert order == ["foo ASC"]


def test_pagination_query_not_allowed():
    request = make_mocked_request(
        'GET', '/runs?_limit=5&_page=3&_order=none&_group=none')

    _, _, _, order, groups, _ = pagination_query(request=request)

    assert order == None
    assert groups == None


def test_builtin_conditions_query_tags_all():
    request = make_mocked_request(
        'GET', '/runs?_tags=foo,bar')

    conditions, values = builtin_conditions_query(request)

    assert len(conditions) == 1
    assert conditions[0] == "tags||system_tags ?& array[%s,%s]"

    assert len(values) == 2
    assert values[0] == "foo"
    assert values[1] == "bar"


def test_builtin_conditions_query_tags_all_explicit():
    request = make_mocked_request(
        'GET', '/runs?_tags:all=foo,bar')

    conditions, values = builtin_conditions_query(request)

    assert len(conditions) == 1
    assert conditions[0] == "tags||system_tags ?& array[%s,%s]"

    assert len(values) == 2
    assert values[0] == "foo"
    assert values[1] == "bar"


def test_builtin_conditions_query_tags_any():
    request = make_mocked_request(
        'GET', '/runs?_tags:any=foo,bar')

    conditions, values = builtin_conditions_query(request)

    assert len(conditions) == 1
    assert conditions[0] == "tags||system_tags ?| array[%s,%s]"

    assert len(values) == 2
    assert values[0] == "foo"
    assert values[1] == "bar"


def test_builtin_conditions_query_tags_likeall():
    request = make_mocked_request(
        'GET', '/runs?_tags:likeall=foo,bar')

    conditions, values = builtin_conditions_query(request)

    assert len(conditions) == 1
    assert conditions[0] == "tags||system_tags::text LIKE ALL(array[%s,%s])"

    assert len(values) == 2
    assert values[0] == "%foo%"
    assert values[1] == "%bar%"


def test_builtin_conditions_query_tags_likeany():
    request = make_mocked_request(
        'GET', '/runs?_tags:likeany=foo,bar')

    conditions, values = builtin_conditions_query(request)

    assert len(conditions) == 1
    assert conditions[0] == "tags||system_tags::text LIKE ANY(array[%s,%s])"

    assert len(values) == 2
    assert values[0] == "%foo%"
    assert values[1] == "%bar%"


def test_custom_conditions_query():
    operators = {
        "flow_id": ["flow_id = %s", "{}"],
        "flow_id:eq": ["flow_id = %s", "{}"],
        "flow_id:ne": ["flow_id != %s", "{}"],
        "flow_id:lt": ["flow_id < %s", "{}"],
        "flow_id:le": ["flow_id <= %s", "{}"],
        "flow_id:gt": ["flow_id > %s", "{}"],
        "flow_id:ge": ["flow_id >= %s", "{}"],
        "flow_id:co": ["flow_id ILIKE %s", "%{}%"],
        "flow_id:sw": ["flow_id ILIKE %s", "{}%"],
        "flow_id:ew": ["flow_id ILIKE %s", "%{}"]
    }

    for op, query in operators.items():
        where = query[0]
        val = query[1]

        request = make_mocked_request(
            "GET", "/runs?{0}=HelloFlow,AnotherFlow&_tags=foo&user_name=dipper&{0}=ThirdFlow".format(op))

        conditions, values = custom_conditions_query(
            request, allowed_keys=["flow_id"])

        assert len(conditions) == 2
        assert conditions[0] == "({0} OR {0})".format(where)
        assert conditions[1] == "({0})".format(where)

        assert len(values) == 3
        assert values[0] == val.format("HelloFlow")
        assert values[1] == val.format("AnotherFlow")
        assert values[2] == val.format("ThirdFlow")


def test_custom_conditions_query_allow_any_key():
    request = make_mocked_request(
        "GET", "/runs?flow_id=HelloFlow&status=completed")

    conditions, values = custom_conditions_query(
        request, allowed_keys=None)

    assert len(conditions) == 2
    assert conditions[0] == "(flow_id = %s)"
    assert conditions[1] == "(status = %s)"

    assert len(values) == 2
    assert values[0] == "HelloFlow"
    assert values[1] == "completed"


def test_resource_conditions():
    path, query, conditions, values = resource_conditions(
        "/runs?flow_id=HelloFlow&status=running")

    assert path == "/runs"

    assert query.get("flow_id") == "HelloFlow"
    assert query.get("status") == "running"

    assert len(conditions) == 2
    assert conditions[0] == "(flow_id = %s)"
    assert conditions[1] == "(status = %s)"

    assert len(values) == 2
    assert values[0] == "HelloFlow"
    assert values[1] == "running"
