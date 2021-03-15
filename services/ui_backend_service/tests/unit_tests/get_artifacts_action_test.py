import pytest

from services.ui_backend_service.data.cache.get_artifacts_action import lookup_id, artifact_cache_id, artifact_location_from_key

pytestmark = [pytest.mark.unit_tests]


async def test_cache_key_independent_of_location_order():
    locs = ["a", "b", "c"]
    a = lookup_id(locs)
    b = lookup_id(reversed(locs))

    assert a == b


async def test_artifact_cache_key_and_location_from_key():
    # first generate an artifact cache key with any location
    _loc = "s3://test-s3-locations/artifact_location/for/cache/1"

    key = artifact_cache_id(_loc)

    assert _loc in key

    # We need to be able to extract the location from a cache key, to form correctly keyed responses
    _extracted_loc = artifact_location_from_key(key)

    assert _extracted_loc == _loc
