import pytest

from services.ui_backend_service.data.cache.utils import (
    error_event_msg, progress_event_msg, search_result_event_msg
)

pytestmark = [pytest.mark.unit_tests]


def test_error_event_msg():
  assert error_event_msg("test message", "test-id") == \
      {"type": "error", "message": "test message", "id": "test-id", "traceback": None}

  assert error_event_msg("test message", "test-id", "test-traceback") == \
      {"type": "error", "message": "test message", "id": "test-id", "traceback": "test-traceback"}


def test_progress_event_msg():
  assert progress_event_msg(0.5) == {"type": "progress", "fraction": 0.5}


def test_search_result_event_msg():
  assert search_result_event_msg([1, 2, 3]) == {"type": "result", "matches": [1, 2, 3]}
