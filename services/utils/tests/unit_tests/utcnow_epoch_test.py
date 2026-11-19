"""Anti-Potemkin reproduction suite for Netflix/metaflow-service `main` `utcnow()` bugs.

Proves TWO distinct failure classes across the 4 `utcnow()` sites in `Netflix/metaflow-service`:
1. **Runtime Timezone Epoch Drift Bug**:
   `new_heartbeat_ts()`, `heartbeat_time_now()`, and `get_heartbeat_ts()` call
   `int(datetime.datetime.utcnow().timestamp())`. Because `utcnow()` returns a *naive*
   `datetime`, Python's `.timestamp()` interprets the UTC wall-clock components in the
   host's *local* timezone! Under `TZ="America/Los_Angeles"` (`UTC-8`) or `TZ="Asia/Tokyo"`
   (`UTC+9`), heartbeat timestamps drift by hours (`+28,800s` / `-32,400s`) relative to
   `int(time.time())` and PostgreSQL `extract(epoch from now())`.
2. **Python 3.12+ `DeprecationWarning` Gate (`DTZ003`)**:
   All 4 sites (`db_utils.new_heartbeat_ts`, `heartbeat_monitor.heartbeat_time_now`,
   `cache_server.echo`, and `integration_tests.utils.get_heartbeat_ts`) emit
   `DeprecationWarning` on Python 3.12+ and will raise `AttributeError` upon removal.
"""

from contextlib import contextmanager
import os
import re
import time
import warnings

import pytest

from services.data.db_utils import new_heartbeat_ts
from services.ui_backend_service.api.heartbeat_monitor import heartbeat_time_now
from services.ui_backend_service.data.cache.client.cache_server import (
    echo as cache_echo,
)
from services.ui_backend_service.tests.integration_tests.utils import get_heartbeat_ts


@contextmanager
def _temporary_tz(tz_name: str):
    prev_tz = os.environ.get("TZ")
    os.environ["TZ"] = tz_name
    time.tzset()
    try:
        yield
    finally:
        if prev_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = prev_tz
        time.tzset()


def test_new_heartbeat_ts_no_deprecation_warning():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        ts = new_heartbeat_ts()
    assert isinstance(ts, int)


def test_heartbeat_time_now_no_deprecation_warning():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        ts = heartbeat_time_now()
    assert isinstance(ts, int)


def test_cache_server_echo_no_deprecation_warning_and_preserves_iso_format(capsys):
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        cache_echo("worker-ready")
    captured = capsys.readouterr()
    assert re.match(
        r"^CACHE \[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\] worker-ready\n$",
        captured.out,
    )


def test_integration_get_heartbeat_ts_no_deprecation_warning():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        ts = get_heartbeat_ts(offset=5)
    assert isinstance(ts, int)


@pytest.mark.parametrize("tz_name", ["America/Los_Angeles", "Asia/Tokyo"])
def test_new_heartbeat_ts_matches_true_utc_epoch_in_non_utc_tz(tz_name):
    with _temporary_tz(tz_name):
        expected = int(time.time())
        actual = new_heartbeat_ts()
        assert abs(actual - expected) <= 2, (
            f"Under TZ={tz_name}, new_heartbeat_ts()={actual} drifted by "
            f"{actual - expected}s from true UTC epoch {expected}!"
        )


@pytest.mark.parametrize("tz_name", ["America/Los_Angeles", "Asia/Tokyo"])
def test_heartbeat_time_now_matches_true_utc_epoch_in_non_utc_tz(tz_name):
    with _temporary_tz(tz_name):
        expected = int(time.time())
        actual = heartbeat_time_now()
        assert abs(actual - expected) <= 2, (
            f"Under TZ={tz_name}, heartbeat_time_now()={actual} drifted by "
            f"{actual - expected}s from true UTC epoch {expected}!"
        )


@pytest.mark.parametrize("tz_name", ["America/Los_Angeles", "Asia/Tokyo"])
def test_get_heartbeat_ts_matches_true_utc_epoch_offset_in_non_utc_tz(tz_name):
    with _temporary_tz(tz_name):
        expected = int(time.time()) + 15
        actual = get_heartbeat_ts(offset=15)
        assert abs(actual - expected) <= 2, (
            f"Under TZ={tz_name}, get_heartbeat_ts(offset=15)={actual} drifted by "
            f"{actual - expected}s from expected {expected}!"
        )
