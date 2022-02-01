import pytest

from services.ui_backend_service.data.cache.utils import (
    error_event_msg, progress_event_msg, search_result_event_msg,
    artifact_location_from_key, artifact_cache_id, unpack_pathspec_with_attempt_id,
    streamed_errors, cacheable_artifact_value, artifact_value
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


def test_streamed_errors_no_op():
    # if nothing raised, callable should not be called
    def _called():
        # should not have been called
        assert False
    try:
        with streamed_errors(_called):
            pass
    except Exception as ex:
        assert False #  Should not have raised any exception



def test_streamed_errors_exception_output():
    # raised errors should be written to output callable.
    def _raised(output):
        assert output['type'] == 'error'
        assert output['id'] == 'Exception'
        assert output['message'] == 'Custom exception'
        assert output['traceback'] is not None

    try:
        with streamed_errors(_raised):
            raise Exception("Custom exception")
        assert False #  Should never get here due to re-raising of the exception
    except Exception as ex:
        assert str(ex) == "Custom exception"


def test_streamed_errors_exception_output():
    # should not raise any exception with re_raise set to false.
    def _re_raise(output):
        pass
    
    try:
        with streamed_errors(_re_raise, re_raise=False):
            raise Exception("Should not be reraised")
    except Exception as ex:
        assert False #  Should not have re-raised exception
    

def test_cacheable_artifact_value():
    artifact = MockArtifact("pathspec/to", 1, "test")
    big_artifact = MockArtifact("pathspec/to", 123456789, "test")

    assert cacheable_artifact_value(artifact) == '[true, "test"]'
    assert cacheable_artifact_value(big_artifact) == '[false, "artifact-too-large", "pathspec/to: 123456789 bytes"]'


def test_artifact_value():
    artifact = MockArtifact("pathspec/to", 1, "test")
    big_artifact = MockArtifact("pathspec/to", 123456789, "test")

    assert artifact_value(artifact) == (True, "test")
    assert artifact_value(big_artifact) == (False, "artifact-too-large", "pathspec/to: 123456789 bytes")


class MockArtifact():
    def __init__(self, pathspec, size, data):
        self.pathspec = pathspec
        self.size = size
        self.data = data
