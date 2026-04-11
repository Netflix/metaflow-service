"""
Unit tests for cache lifecycle fixes (QA report defects M1-M7).

All tests use AsyncMock / MagicMock to avoid Docker-level dependencies
(aiopg, real subprocesses, etc.).

conftest.py in this directory pre-loads the relevant modules before collection
using the cumulative partial-import trick, so no sys.modules manipulation is
needed here.
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call

from services.ui_backend_service.data.cache.client.cache_async_client import CacheAsyncClient
from services.ui_backend_service.data.cache.client.cache_client import CacheServerUnreachable

pytestmark = [pytest.mark.unit_tests]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client(**attrs):
    """Return a CacheAsyncClient instance with __init__ bypassed, attributes set."""
    obj = CacheAsyncClient.__new__(CacheAsyncClient)
    obj._is_alive = True
    obj._restart_requested = False
    obj.pending_requests = set()
    obj.logger = MagicMock()
    obj._drain_lock = asyncio.Lock()
    for k, v in attrs.items():
        setattr(obj, k, v)
    return obj


def _make_mock_proc(returncode=None):
    proc = MagicMock()
    proc.returncode = returncode
    proc.pid = 12345
    proc.terminate = MagicMock()
    proc.stdin = MagicMock()
    proc.stdin.write = MagicMock()
    proc.stdin.drain = AsyncMock()
    proc.wait = AsyncMock()
    return proc




# ---------------------------------------------------------------------------
# M1: BrokenPipeError is caught by except ConnectionError
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_m1_broken_pipe_triggers_restart():
    """BrokenPipeError on stdin.write must set _is_alive=False and raise CacheServerUnreachable."""
    proc = _make_mock_proc()
    proc.stdin.write.side_effect = BrokenPipeError("broken pipe")
    client = _make_client(_proc=proc)

    with pytest.raises(CacheServerUnreachable):
        await client.send_request(b'{"op": "test"}\n')

    assert client._is_alive is False
    assert client._restart_requested is True


@pytest.mark.asyncio
async def test_m1_connection_reset_still_caught():
    """ConnectionResetError must still be caught (regression guard)."""
    proc = _make_mock_proc()
    proc.stdin.write.side_effect = ConnectionResetError("reset")
    client = _make_client(_proc=proc)

    with pytest.raises(CacheServerUnreachable):
        await client.send_request(b'{"op": "test"}\n')

    assert client._is_alive is False
    assert client._restart_requested is True


# ---------------------------------------------------------------------------
# M2: stop_server reaps subprocess even when _is_alive was already False
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_m2_stop_server_reaps_proc_when_already_dead():
    """Zombie fix: _proc.wait() must be called even if _is_alive was already False."""
    proc = _make_mock_proc(returncode=1)  # proc already exited
    client = _make_client(_proc=proc, _is_alive=False)

    await client.stop_server()

    proc.wait.assert_awaited_once()
    # returncode is not None so terminate should NOT be called
    proc.terminate.assert_not_called()


@pytest.mark.asyncio
async def test_m2_stop_server_terminates_running_proc():
    """stop_server must terminate a still-running proc (returncode is None)."""
    proc = _make_mock_proc(returncode=None)  # still running
    client = _make_client(_proc=proc)

    await client.stop_server()

    proc.terminate.assert_called_once()
    proc.wait.assert_awaited_once()
    assert client._is_alive is False


# ---------------------------------------------------------------------------
# M3: stop_server cancels stored task handles
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_m3_stop_server_cancels_background_tasks():
    """stop_server must cancel _heartbeat_task and _read_task."""
    proc = _make_mock_proc(returncode=0)
    heartbeat_task = asyncio.ensure_future(asyncio.sleep(1000))
    read_task = asyncio.ensure_future(asyncio.sleep(1000))
    client = _make_client(
        _proc=proc,
        _heartbeat_task=heartbeat_task,
        _read_task=read_task,
    )

    await client.stop_server()

    assert heartbeat_task.cancelled()
    assert read_task.cancelled()


@pytest.mark.asyncio
async def test_m3_stop_server_no_crash_without_tasks():
    """stop_server must not crash when called before start_server (no task attrs)."""
    proc = _make_mock_proc(returncode=0)
    client = _make_client(_proc=proc)
    # _heartbeat_task / _read_task deliberately NOT set

    await client.stop_server()  # must not raise AttributeError


@pytest.mark.asyncio
async def test_m3_start_server_stores_task_handles():
    """start_server must store _heartbeat_task and _read_task handles."""
    client = _make_client()
    mock_proc = _make_mock_proc()

    client._root = "test_root"
    with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=mock_proc)):
        await client.start_server(["fake_cmd"], {})

    assert hasattr(client, "_heartbeat_task")
    assert hasattr(client, "_read_task")
    # Clean up background tasks
    client._heartbeat_task.cancel()
    client._read_task.cancel()
    await asyncio.gather(client._heartbeat_task, client._read_task, return_exceptions=True)


# ---------------------------------------------------------------------------
# m1: TimeoutError in send_request sets _is_alive = False
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_m1_minor_timeout_sets_is_alive_false():
    """asyncio.wait_for timing out on drain must mark _is_alive=False.

    The timeout comes from wait_for expiring, NOT from drain raising directly.
    We make drain hang indefinitely and patch WAIT_FREQUENCY to a tiny value
    so wait_for genuinely fires (DEF-A05-D1 fix).
    """
    async def _drain_hang(*_args, **_kwargs):
        await asyncio.sleep(9999)

    proc = _make_mock_proc()
    # drain blocks forever — wait_for must be the one to time it out
    proc.stdin.drain = AsyncMock(side_effect=_drain_hang)
    client = _make_client(_proc=proc)

    import services.ui_backend_service.data.cache.client.cache_async_client as _cac
    with patch.object(_cac, "WAIT_FREQUENCY", 0.001):
        await client.send_request(b'{"op": "test"}\n')

    assert client._is_alive is False
    assert client._restart_requested is True


# ---------------------------------------------------------------------------
# M4: CacheStore._monitor_task cancelled in stop_caches
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_m4_stop_caches_cancels_monitor_task():
    """stop_caches must cancel _monitor_task before stopping individual caches."""
    from services.ui_backend_service.data.cache.store import CacheStore

    store = CacheStore.__new__(CacheStore)

    # Replace sub-caches with mocks whose stop_cache is a no-op
    for attr in ("artifact_cache", "dag_cache", "log_cache", "card_cache"):
        mock_cache = MagicMock()
        mock_cache.stop_cache = AsyncMock()
        setattr(store, attr, mock_cache)

    monitor_task = asyncio.ensure_future(asyncio.sleep(1000))
    store._monitor_task = monitor_task

    await store.stop_caches(app=None)

    assert monitor_task.cancelled()
    # All sub-caches still stopped
    store.artifact_cache.stop_cache.assert_awaited_once()
    store.dag_cache.stop_cache.assert_awaited_once()
    store.log_cache.stop_cache.assert_awaited_once()
    store.card_cache.stop_cache.assert_awaited_once()


@pytest.mark.asyncio
async def test_m4_stop_caches_safe_without_monitor_task():
    """stop_caches must not raise if _monitor_task was never set."""
    from services.ui_backend_service.data.cache.store import CacheStore

    store = CacheStore.__new__(CacheStore)
    for attr in ("artifact_cache", "dag_cache", "log_cache", "card_cache"):
        mock_cache = MagicMock()
        mock_cache.stop_cache = AsyncMock()
        setattr(store, attr, mock_cache)

    await store.stop_caches(app=None)  # no _monitor_task attribute — must not raise


# ---------------------------------------------------------------------------
# M5: CardCacheStore.stop_cache safe before start_cache is called
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_m5_card_cache_stop_before_start_no_error():
    """stop_cache must not raise AttributeError if start_cache was never called."""
    from services.ui_backend_service.data.cache.store import CardCacheStore

    store = CardCacheStore.__new__(CardCacheStore)
    store._cleanup_coroutine = None
    store._disk_cleanup_coroutine = None

    await store.stop_cache()  # must not raise


# ---------------------------------------------------------------------------
# M6: self.cache only assigned after start() completes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_m6_artifact_cache_null_during_start():
    """self.cache must remain None until CacheAsyncClient.start() has completed."""
    from services.ui_backend_service.data.cache.store import ArtifactCacheStore

    store = ArtifactCacheStore.__new__(ArtifactCacheStore)
    store.cache = None

    observed_cache_during_start = []

    async def fake_start(self_inner):
        # At this point, store.cache must still be None
        observed_cache_during_start.append(store.cache)

    with patch(
        "services.ui_backend_service.data.cache.store.CacheAsyncClient"
    ) as MockClient, patch(
        "services.ui_backend_service.data.cache.store.FEATURE_CACHE_ENABLE", True
    ):
        mock_instance = MagicMock()
        mock_instance.start = AsyncMock(side_effect=lambda: observed_cache_during_start.append(store.cache))
        MockClient.return_value = mock_instance

        await store.start_cache()

    # cache was still None when start() was called
    assert observed_cache_during_start[0] is None
    # but is set after start() completes
    assert store.cache is mock_instance


@pytest.mark.asyncio
async def test_m6_dag_cache_null_during_start():
    """Same race-fix check for DAGCacheStore."""
    from services.ui_backend_service.data.cache.store import DAGCacheStore

    store = DAGCacheStore.__new__(DAGCacheStore)
    store.cache = None

    observed = []

    with patch(
        "services.ui_backend_service.data.cache.store.CacheAsyncClient"
    ) as MockClient, patch(
        "services.ui_backend_service.data.cache.store.FEATURE_CACHE_ENABLE", True
    ):
        mock_instance = MagicMock()
        mock_instance.start = AsyncMock(side_effect=lambda: observed.append(store.cache))
        MockClient.return_value = mock_instance

        await store.start_cache()

    assert observed[0] is None
    assert store.cache is mock_instance


# ---------------------------------------------------------------------------
# M7: successful retry result overrides stale errors from failed attempt
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_m7_successful_retry_overrides_stale_stream_errors():
    """
    Scenario: attempt 1 streams an error for target A then raises CacheServerUnreachable.
    Attempt 2 succeeds and returns data for A.
    _process must NOT set postprocess_error on the record for A.
    """
    from services.ui_backend_service.data.refiner.refinery import Refinery
    from services.data.db_utils import DBResponse

    refinery = Refinery.__new__(Refinery)
    refinery.logger = MagicMock()
    refinery.cache_store = MagicMock()

    # The record maps to target "flow/1/step/task"
    record = {"flow_id": "flow", "run_number": 1, "step_name": "step", "task_id": "task"}

    # fetch_data returns successful data — simulates the post-retry state
    # (errors dict may still contain stale entries from the failed first attempt)
    successful_data = {"flow/1/step/task": [True, {"my": "value"}, None, None]}

    async def fake_fetch_data(targets, event_stream=None, invalidate_cache=False):
        # Simulate stale error event from a failed prior attempt
        if event_stream:
            event_stream({
                "type": "error",
                "key": "some:prefix:flow/1/step/task",
                "id": "stale-error",
                "message": "from failed attempt",
                "traceback": None,
            })
        return successful_data

    refinery.fetch_data = fake_fetch_data

    async def fake_refine_record(record, values):
        record["refined"] = True
        return record

    refinery.refine_record = fake_refine_record

    response = DBResponse(response_code=200, body=record)

    with patch("services.ui_backend_service.data.refiner.refinery.FEATURE_REFINE_DISABLE", False):
        result = await refinery.postprocess(response)

    assert result.response_code == 200
    assert "postprocess_error" not in result.body, (
        "stale error from failed attempt must not appear when retry succeeded"
    )
    assert result.body.get("refined") is True


@pytest.mark.asyncio
async def test_m7_error_used_when_no_data_returned():
    """When no data entry exists for a target, the stream error IS reported."""
    from services.ui_backend_service.data.refiner.refinery import Refinery
    from services.data.db_utils import DBResponse

    refinery = Refinery.__new__(Refinery)
    refinery.logger = MagicMock()
    refinery.cache_store = MagicMock()

    record = {"flow_id": "flow", "run_number": 1, "step_name": "step", "task_id": "task"}

    async def fake_fetch_data(targets, event_stream=None, invalidate_cache=False):
        if event_stream:
            event_stream({
                "type": "error",
                "key": "some:prefix:flow/1/step/task",
                "id": "real-error",
                "message": "actual failure",
                "traceback": None,
            })
        return {}  # no data

    refinery.fetch_data = fake_fetch_data
    refinery.refine_record = AsyncMock(side_effect=lambda r, v: r)

    response = DBResponse(response_code=200, body=record)

    with patch("services.ui_backend_service.data.refiner.refinery.FEATURE_REFINE_DISABLE", False):
        result = await refinery.postprocess(response)

    assert "postprocess_error" in result.body
    assert result.body["postprocess_error"]["id"] == "real-error"
