import pytest

from services.ui_backend_service.cache.async_get_artifacts import cache_artifacts_key

pytestmark = [pytest.mark.unit_tests]


async def test_cache_key_independent_of_location_order():
    locs = ["a", "b", "c"]
    a = cache_artifacts_key(None, None, locs)
    b = cache_artifacts_key(None, None, reversed(locs))

    assert a == b


async def test_cache_key_independent_of_function_or_session():
    locs = ["a", "b", "c"]
    a = cache_artifacts_key(lambda x: 1, None, locs)
    b = cache_artifacts_key(lambda x: 2, None, locs)
    c = cache_artifacts_key(lambda x: 1, 1, locs)
    d = cache_artifacts_key(lambda x: 2, 2, locs)

    assert a == b
    assert c == d
    assert a == d
