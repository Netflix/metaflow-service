import logging
import pytest
from unittest.mock import patch

from services.data.query_tracing import (
    start_trace,
    record_query,
    finish_trace,
    _request_trace,
)


class TestQueryTracingEnabled:

    @patch("services.data.query_tracing.QUERY_TRACING_ENABLED", True)
    def test_single_query_trace(self, caplog):
        start_trace()
        record_query("runs_v3", "SELECT * FROM runs_v3 WHERE flow_id = %s", 50, 12.5)

        with caplog.at_level(logging.INFO, logger="QueryTracing"):
            finish_trace("GET", "/flows/TestFlow/runs")

        assert "[RequestTrace]" in caplog.text
        assert "GET /flows/TestFlow/runs" in caplog.text
        assert "queries=1" in caplog.text
        assert "total_rows=50" in caplog.text

    @patch("services.data.query_tracing.QUERY_TRACING_ENABLED", True)
    def test_multiple_queries_aggregated(self, caplog):
        start_trace()
        record_query("tasks_v3", "SELECT * FROM tasks_v3", 30, 5.2)
        record_query("runs_v3", "SELECT * FROM runs_v3", 1, 3.1)

        with caplog.at_level(logging.INFO, logger="QueryTracing"):
            finish_trace("GET", "/flows/TestFlow/runs/1/steps/train/tasks")

        assert "queries=2" in caplog.text
        assert "total_rows=31" in caplog.text

    @patch("services.data.query_tracing.QUERY_TRACING_ENABLED", True)
    def test_sql_truncated_at_200_chars(self):
        """Long SQL strings are truncated to 200 characters."""
        start_trace()
        long_sql = "SELECT " + "x" * 300
        record_query("runs_v3", long_sql, 10, 5.0)

        trace = _request_trace.get()
        assert len(trace["queries"][0]["sql"]) == 200


        _request_trace.set(None)

    @patch("services.data.query_tracing.QUERY_TRACING_ENABLED", True)
    def test_trace_cleaned_up_after_finish(self):
        start_trace()
        record_query("runs_v3", "SELECT *", 10, 1.0)
        finish_trace("GET", "/test")

        assert _request_trace.get(None) is None

    @patch("services.data.query_tracing.QUERY_TRACING_ENABLED", True)
    def test_zero_queries_logged(self, caplog):
        start_trace()

        with caplog.at_level(logging.INFO, logger="QueryTracing"):
            finish_trace("GET", "/healthcheck")

        assert "queries=0" in caplog.text
        assert "total_rows=0" in caplog.text

    @patch("services.data.query_tracing.QUERY_TRACING_ENABLED", True)
    def test_debug_level_shows_individual_queries(self, caplog):
        start_trace()
        record_query("runs_v3", "SELECT * FROM runs_v3", 50, 12.5)
        record_query("steps_v3", "SELECT * FROM steps_v3", 200, 8.3)

        with caplog.at_level(logging.DEBUG, logger="QueryTracing"):
            finish_trace("GET", "/test")

        assert "[Query 1/2]" in caplog.text
        assert "[Query 2/2]" in caplog.text
        assert "table=runs_v3" in caplog.text
        assert "table=steps_v3" in caplog.text


class TestQueryTracingDisabled:

    @patch("services.data.query_tracing.QUERY_TRACING_ENABLED", False)
    def test_start_trace_noop_when_disabled(self):
        start_trace()
        assert _request_trace.get(None) is None

    @patch("services.data.query_tracing.QUERY_TRACING_ENABLED", False)
    def test_record_query_noop_when_disabled(self):
        record_query("runs_v3", "SELECT *", 50, 12.5)

    @patch("services.data.query_tracing.QUERY_TRACING_ENABLED", False)
    def test_finish_trace_noop_when_disabled(self, caplog):
        with caplog.at_level(logging.INFO, logger="QueryTracing"):
            finish_trace("GET", "/test")

        assert "[RequestTrace]" not in caplog.text
