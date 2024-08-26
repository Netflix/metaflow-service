import pytest
import datetime
import os
from tempfile import TemporaryDirectory
from services.ui_backend_service.data.cache.get_log_file_action import paginated_result, log_cache_id, lookup_id, \
    _datetime_to_epoch, FullLogProvider, STDOUT, TailLogProvider, BlurbOnlyLogProvider, stream_sorted_logs

from unittest.mock import MagicMock, patch

pytestmark = [pytest.mark.unit_tests]

TEST_LOG = list((None, "log line {}".format(i)) for i in range(1, 1001))
TEST_MFLOG = list((i, "log line {}".format(i)) for i in range(1, 1001))


@pytest.mark.parametrize(
    "test_log, first_expected_item",
    [
        (TEST_LOG, {"row": 0, "line": "log line 1", "timestamp": None}),
        (TEST_MFLOG, {"row": 0, "line": "log line 1", "timestamp": 1})
    ]
)
async def test_paginated_result(test_log, first_expected_item):
    body = paginated_result(content_iterator=_list_iter(test_log))

    # 1000 lines should fit in default pagination
    assert len(body['content']) == 1000
    assert body["pages"] == 1
    # order should be oldest to newest
    assert body['content'][0] == first_expected_item


@pytest.mark.parametrize("test_log", [TEST_LOG, TEST_MFLOG])
async def test_paginated_result_oob_page(test_log):
    body = paginated_result(
        content_iterator=_list_iter(test_log), page=2,
        limit=2000, reverse_order=False,
        output_raw=False
    )

    assert body["pages"] == 1
    assert len(body['content']) == 0

    # with zero limit, if requesting pages beyond the first, should receive nothing.
    body = paginated_result(
        content_iterator=_list_iter(test_log), page=2,
        limit=0, reverse_order=False,
        output_raw=False
    )

    assert body["pages"] == 1
    assert len(body['content']) == 0


@pytest.mark.parametrize("test_log", [TEST_LOG, TEST_MFLOG])
async def test_paginated_result_with_limit(test_log):
    body = paginated_result(
        content_iterator=_list_iter(test_log), page=2,
        limit=5, line_total=len(test_log), reverse_order=False,
        output_raw=False
    )

    assert len(body['content']) == 5
    assert body["pages"] == 200
    assert [obj["line"] for obj in body['content']] == list("log line {}".format(i) for i in range(6, 11))


@pytest.mark.parametrize("test_log", [TEST_LOG, TEST_MFLOG])
async def test_paginated_result_ordering(test_log):
    body = paginated_result(
        content_iterator=_list_iter(test_log), page=1,
        limit=0, line_total=len(test_log), reverse_order=False,
        output_raw=False
    )
    assert [obj["line"] for obj in body["content"]] == [line for _, line in test_log]

    body = paginated_result(
        content_iterator=_list_iter(test_log), page=1,
        limit=0, line_total=len(test_log), reverse_order=True,
        output_raw=False
    )
    assert [obj["line"] for obj in body["content"]] == [line for _, line in test_log[::-1]]


@pytest.mark.parametrize("test_log", [TEST_LOG, TEST_MFLOG])
async def test_paginated_result_raw_output(test_log):
    body = paginated_result(
        content_iterator=_list_iter(test_log), page=1,
        limit=5, line_total=len(test_log), reverse_order=False,
        output_raw=True
    )
    assert body["pages"] == 200
    # should skip timestamps in raw content for all log types
    # should have trailing newline if not last page
    assert body["content"] == "\n".join(line for _, line in test_log[:5])+"\n"

    body = paginated_result(
        content_iterator=_list_iter(test_log), page=200,
        limit=5, line_total=len(test_log), reverse_order=False,
        output_raw=True
    )
    # Last page should not have trailing newline.
    assert body["content"] == "\n".join(line for _, line in test_log[-5::])


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


datetime_expectations = [
    (None, None),
    ("123", None),
    (datetime.datetime(2021, 10, 27, 0, 0, tzinfo=datetime.timezone.utc), 1635292800000)
]


@pytest.mark.parametrize("datetime, output", datetime_expectations)
async def test_datetime_to_epoch(datetime, output):
    assert _datetime_to_epoch(datetime) == output


@patch("services.ui_backend_service.data.cache.get_log_file_action.get_log_content")
@patch("services.ui_backend_service.data.cache.get_log_file_action.get_log_size")
def test_full_log_provider(m_get_log_size, m_get_log_content):
    mock_task = MagicMock()
    mock_log_content = [(None, 'log line 1'),
                        (None, 'log line 2'),
                        (None, 'log line 3')]
    # sum line lengths and account for newline characters
    mock_log_size = sum(len(line) + 1 for _, line in mock_log_content)
    m_get_log_size.return_value = mock_log_size
    m_get_log_content.return_value = mock_log_content

    full_log_provider = FullLogProvider()
    assert full_log_provider.get_log_content(mock_task, STDOUT) == mock_log_content
    assert full_log_provider.get_log_hash(mock_task, STDOUT) == mock_log_size


@patch("services.ui_backend_service.data.cache.get_log_file_action.get_log_content")
@patch("services.ui_backend_service.data.cache.get_log_file_action.get_log_size")
def test_tail_log_provider(m_get_log_size, m_get_log_content):
    mock_task = MagicMock()
    mock_log_content = []
    for i in range(1000):
        # 3 chars per line
        mock_log_content.append((None, '%03d' % i))

    # sum line lengths and account for newline characters
    mock_log_size = sum(len(line) + 1 for _, line in mock_log_content)
    m_get_log_size.return_value = mock_log_size
    m_get_log_content.return_value = mock_log_content

    for case in [
        {
            "max_tail_chars": 10000,
            "max_total_size": 200*1024,
            "expected_tail_lines": 1000,
            "expect_to_truncate": False,
        },
        {
            "max_tail_chars": 1000,
            "max_total_size": 200*1024,
            "expected_tail_lines": 333,
            "expect_to_truncate": True,
        },
        {
            "max_tail_chars": 100,
            "max_total_size": 200*1024,
            "expected_tail_lines": 33,
            "expect_to_truncate": True,
        },
        {
            "max_tail_chars": 0,
            "max_total_size": 200*1024,
            "expected_tail_lines": 0,
            "expect_to_truncate": True,
        },
    ]:
        provider = TailLogProvider(case["max_tail_chars"], max_log_size_in_kb=case["max_total_size"])
        # Log size should still report full log size (even if only partial content returned)
        assert provider.get_log_hash(mock_task, STDOUT) == mock_log_size
        tail_log_content = provider.get_log_content(mock_task, STDOUT)
        if case["expect_to_truncate"]:
            # Truncation IS expected, check for "truncated messaging", then the tailed content
            assert len(tail_log_content) == case["expected_tail_lines"] + 1
            assert 'truncated' in tail_log_content[0][1]
            assert tail_log_content[1:] == mock_log_content[len(mock_log_content) - case["expected_tail_lines"]:]
        else:
            # If no truncation expected.... just check for full content... no "truncated" messaging
            assert tail_log_content == mock_log_content


@patch("services.ui_backend_service.data.cache.get_log_file_action.get_log_content")
def test_blurb_only_log_provider(m_get_log_content):
    mock_task = MagicMock()
    mock_log_content = []

    m_get_log_content.return_value = mock_log_content
    provider = BlurbOnlyLogProvider()
    eternal_log_hash = provider.get_log_hash(mock_task, STDOUT)
    eternal_log_content = provider.get_log_content(mock_task, STDOUT)
    for i in range(1000):
        # 3 chars per line
        mock_log_content.append((None, '%03d' % i))
        # Log size should NOT report true log size. In fact it should stay constant even as raw log content changes.
        assert provider.get_log_hash(mock_task, STDOUT) == eternal_log_hash
        # Log content should always be the same blurb
        assert provider.get_log_content(mock_task, STDOUT) == eternal_log_content


def _list_iter(list):
    def _gen():
        yield from list
    return _gen

# template for mflog format lines
def _logline_bytes(ts: datetime.datetime, source, msg):
    MFLOG_LINE_TEMPLATE = "[MFLOG|0|{ts}|{source}|123-abc]{msg}"
    ISOFORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

    ts = ts.strftime(ISOFORMAT)
    return MFLOG_LINE_TEMPLATE.format(ts=ts, source=source, msg=msg).encode("utf-8")


def test_stream_sorted_logs_consecutively():
    start_a_ts = datetime.datetime(2021, 10, 27, 0, 0, tzinfo=datetime.timezone.utc)
    raw_stream_a = [(start_a_ts + datetime.timedelta(seconds=i), f"A line {i}") for i in range(0,10)]
    
    start_b_ts = datetime.datetime(2021, 10, 27, 0, 10, tzinfo=datetime.timezone.utc)
    raw_stream_b = [(start_b_ts + datetime.timedelta(seconds=i), f"B line {i}") for i in range(0,10)]
    with TemporaryDirectory() as d:
        a_path = os.path.join(d, "log_a")
        b_path = os.path.join(d, "log_b")
        with open(a_path, "wb+") as f:
            f.write(b"\n".join(_logline_bytes(ts, "A", line) for ts, line in raw_stream_a))
        with open(b_path, "wb+") as f:
            f.write(b"\n".join(_logline_bytes(ts, "B", line) for ts, line in raw_stream_b))
        
        results = stream_sorted_logs([a_path, b_path])

        assert [line for _ts, line in results] == [line for _ts, line in (raw_stream_a + raw_stream_b)]


def test_stream_sorted_logs_sandwiched():
    start_b_start_ts = datetime.datetime(2021, 10, 27, 0, 0, tzinfo=datetime.timezone.utc)
    raw_stream_b_start = [(start_b_start_ts + datetime.timedelta(seconds=i), f"B line {i}") for i in range(0,10)]
    
    start_a_ts = datetime.datetime(2021, 10, 27, 0, 0, 10, tzinfo=datetime.timezone.utc)
    raw_stream_a = [(start_a_ts + datetime.timedelta(seconds=i), f"A line {i}") for i in range(0,10)]
    
    start_b_end_ts = datetime.datetime(2021, 10, 27, 0, 0, 20, tzinfo=datetime.timezone.utc)
    raw_stream_b_end = [(start_b_end_ts + datetime.timedelta(seconds=i), f"B line {i}") for i in range(11,20)]

    raw_stream_b = (raw_stream_b_start + raw_stream_b_end)
    with TemporaryDirectory() as d:
        a_path = os.path.join(d, "log_a")
        b_path = os.path.join(d, "log_b")
        with open(a_path, "wb+") as f:
            f.write(b"\n".join(_logline_bytes(ts, "A", line) for ts, line in raw_stream_a))
        with open(b_path, "wb+") as f:
            f.write(b"\n".join(_logline_bytes(ts, "B", line) for ts, line in raw_stream_b))
        
        results = stream_sorted_logs([a_path, b_path])

        assert [line for _ts, line in results] == [line for _ts, line in (raw_stream_b_start + raw_stream_a + raw_stream_b_end)]