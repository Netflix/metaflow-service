import pytest

from services.ui_backend_service.data.cache.get_log_file_action import paginated_result, log_cache_id, lookup_id

pytestmark = [pytest.mark.unit_tests]

TEST_LOG = "\n".join(list("log line {}".format(i) for i in range(1, 1001)))


async def test_paginated_result():
    body = paginated_result(content=TEST_LOG)

    # 1000 lines should fit in default pagination
    assert len(body['content']) == 1000
    assert body["pages"] == 1
    # order should be oldest to newest
    assert body['content'][0] == {"row": 0, "line": "log line 1"}


async def test_paginated_result_oob_page():
    body = paginated_result(
        content=TEST_LOG, page=2,
        limit=2000, reverse_order=False,
        output_raw=False
    )

    assert body["pages"] == 1
    assert len(body['content']) == 0

    # with zero limit, if requesting pages beyond the first, should receive nothing.
    body = paginated_result(
        content=TEST_LOG, page=2,
        limit=0, reverse_order=False,
        output_raw=False
    )

    assert body["pages"] == 1
    assert len(body['content']) == 0


async def test_paginated_result_with_limit():
    body = paginated_result(
        content=TEST_LOG, page=2,
        limit=5, reverse_order=False,
        output_raw=False
    )

    assert len(body['content']) == 5
    assert body["pages"] == 200
    assert [obj["line"] for obj in body['content']] == list("log line {}".format(i) for i in range(6, 11))


async def test_paginated_result_ordering():
    body = paginated_result(
        content=TEST_LOG, page=1,
        limit=0, reverse_order=False,
        output_raw=False
    )
    assert [obj["line"] for obj in body["content"]] == TEST_LOG.split("\n")

    body = paginated_result(
        content=TEST_LOG, page=1,
        limit=0, reverse_order=True,
        output_raw=False
    )
    assert [obj["line"] for obj in body["content"]] == TEST_LOG.split("\n")[::-1]


async def test_paginated_result_raw_output():
    body = paginated_result(
        content=TEST_LOG, page=1,
        limit=5, reverse_order=False,
        output_raw=True
    )
    assert body["pages"] == 1
    # should return full log despite pagination limit when requesting raw.
    assert body["content"] == TEST_LOG


async def test_log_cache_id_uniqueness():
    first_task = {
        "flow_id": "TestFlow",
        "run_number": "1234",
        "step_name": "test_step",
        "task_id": "1234",
        "attempt_id": "0"
    }

    first_task_second_attempt = {
        "flow_id": "TestFlow",
        "run_number": "1234",
        "step_name": "test_step",
        "task_id": "1234",
        "attempt_id": "1"
    }

    second_task = {
        "flow_id": "TestFlow",
        "run_number": "1234",
        "step_name": "test_step",
        "task_id": "1235",
        "attempt_id": "0"
    }

    assert log_cache_id(first_task, "stdout") == log_cache_id(first_task, "stdout")
    assert log_cache_id(first_task, "stdout") != log_cache_id(first_task, "stderr")
    assert log_cache_id(first_task, "stdout") != log_cache_id(first_task_second_attempt, "stdout")
    assert log_cache_id(first_task, "stdout") != log_cache_id(second_task, "stdout")


async def test_lookup_id_uniqueness():
    first_task = {
        "flow_id": "TestFlow",
        "run_number": "1234",
        "step_name": "test_step",
        "task_id": "1234",
        "attempt_id": "0"
    }

    first_task_second_attempt = {
        "flow_id": "TestFlow",
        "run_number": "1234",
        "step_name": "test_step",
        "task_id": "1234",
        "attempt_id": "1"
    }

    second_task = {
        "flow_id": "TestFlow",
        "run_number": "1234",
        "step_name": "test_step",
        "task_id": "1235",
        "attempt_id": "0"
    }

    assert lookup_id(first_task, "stdout", 0, 1, False, False) == \
        lookup_id(first_task, "stdout", 0, 1, False, False)

    assert lookup_id(first_task, "stdout", 0, 1, False, False) != \
        lookup_id(first_task_second_attempt, "stdout", 0, 1, False, False)

    assert lookup_id(first_task, "stdout", 0, 1, False, False) != \
        lookup_id(second_task, "stdout", 0, 1, False, False)

    assert lookup_id(first_task, "stdout", 0, 1, False, False) != \
        lookup_id(first_task, "stdout", 1, 1, False, False)

    assert lookup_id(first_task, "stdout", 1, 1, False, False) != \
        lookup_id(first_task, "stdout", 1, 0, False, False)

    assert lookup_id(first_task, "stdout", 1, 1, False, False) != \
        lookup_id(first_task, "stdout", 1, 1, True, False)

    assert lookup_id(first_task, "stdout", 1, 1, False, False) != \
        lookup_id(first_task, "stdout", 1, 1, False, True)
