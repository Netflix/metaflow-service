import pytest

from services.ui_backend_service.data.cache.utils import (
    error_event_msg, progress_event_msg, search_result_event_msg,
    artifact_location_from_key, artifact_cache_id, unpack_pathspec_with_attempt_id
)

pytestmark = [pytest.mark.unit_tests]


def test_error_event_msg():
    assert error_event_msg("test message", "test-id") == \
        {"type": "error", "message": "test message", "id": "test-id", "traceback": None, "key": None}

    assert error_event_msg("test message", "test-id", "test-traceback") == \
        {"type": "error", "message": "test message", "id": "test-id", "traceback": "test-traceback", "key": None}

    assert error_event_msg("test message", "test-id", "test-traceback", "search:artifact:s3://etc") == \
        {"type": "error", "message": "test message", "id": "test-id", "traceback": "test-traceback", "key": "search:artifact:s3://etc"}


def test_progress_event_msg():
    assert progress_event_msg(0.5) == {"type": "progress", "fraction": 0.5}


def test_search_result_event_msg():
    assert search_result_event_msg([1, 2, 3]) == {"type": "result", "matches": [1, 2, 3]}


def test_artifact_cache_key_and_location_from_key():
    # first generate an artifact cache key with any location
    _loc = "s3://test-s3-locations/artifact_location/for/cache/1"

    key = artifact_cache_id(_loc)

    assert _loc in key

    # We need to be able to extract the location from a cache key, to form correctly keyed responses
    _extracted_loc = artifact_location_from_key(key)

    assert _extracted_loc == _loc


def test_unpack_pathspec_with_attempt_id():
    pathspec = "FlowName/RunNumber/StepName/TaskId/4"
    pathspec_without_attempt_id, attempt_id = unpack_pathspec_with_attempt_id(pathspec)
    assert pathspec_without_attempt_id == "FlowName/RunNumber/StepName/TaskId"
    assert attempt_id == 4
