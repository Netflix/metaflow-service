import os
import time
import logging
from contextvars import ContextVar
from aiohttp import web

logger = logging.getLogger("QueryTracing")

QUERY_TRACING_ENABLED = os.environ.get("QUERY_TRACING_ENABLED", "0") == "1"

_request_trace: ContextVar[dict] = ContextVar("request_trace", default=None)


def start_trace():
    if not QUERY_TRACING_ENABLED:
        return
    _request_trace.set({
        "start_time": time.monotonic(),
        "queries": [],
    })


def record_query(table_name: str, sql: str, row_count: int, elapsed_ms: float):
    trace = _request_trace.get(None)
    if trace is None:
        return
    trace["queries"].append({
        "table": table_name,
        "sql": sql[:200],
        "rows": row_count,
        "time_ms": round(elapsed_ms, 2),
    })


def finish_trace(method: str, path: str):
    trace = _request_trace.get(None)
    if trace is None:
        return

    total_time = (time.monotonic() - trace["start_time"]) * 1000
    total_queries = len(trace["queries"])
    total_rows = sum(q["rows"] for q in trace["queries"])
    total_db_time = sum(q["time_ms"] for q in trace["queries"])

    logger.info(
        "[RequestTrace] %s %s | queries=%d total_rows=%d "
        "db_time=%.2fms request_time=%.2fms",
        method, path, total_queries, total_rows,
        total_db_time, total_time
    )

    for i, q in enumerate(trace["queries"], 1):
        logger.debug(
            "  [Query %d/%d] table=%s rows=%d time=%.2fms sql=%s",
            i, total_queries, q["table"], q["rows"], q["time_ms"], q["sql"]
        )

    _request_trace.set(None)


@web.middleware
async def query_tracing_middleware(request, handler):
    start_trace()
    try:
        response = await handler(request)
        return response
    finally:
        finish_trace(request.method, request.path)
