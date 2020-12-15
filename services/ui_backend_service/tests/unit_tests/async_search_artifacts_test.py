import pytest

from services.ui_backend_service.cache.async_search_artifacts import cache_search_key

pytestmark = [pytest.mark.unit_tests]


async def test_cache_key_independent_of_location_order():
    locs = ["a", "b", "c"]
    a = cache_search_key(None, None, locs, "test", None)
    b = cache_search_key(None, None, reversed(locs), "test", None)

    assert a == b


async def test_cache_key_depends_on_searchterm():
    locs = ["a", "b", "c"]
    a = cache_search_key(None, None, locs, "test", None)
    b = cache_search_key(None, None, locs, "anothertest", None)

    assert not a == b


async def test_cache_key_independent_of_function_or_session():
    locs = ["a", "b", "c"]
    a = cache_search_key(lambda x: 1, None, locs, "test", None)
    b = cache_search_key(lambda x: 2, None, locs, "test", None)
    c = cache_search_key(lambda x: 1, 1, locs, "test", None)
    d = cache_search_key(lambda x: 2, 2, locs, "test", None)

    assert a == b
    assert c == d
    assert a == d
