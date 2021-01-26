import pytest

from services.ui_backend_service.data.cache.search_artifacts_action import lookup_id

pytestmark = [pytest.mark.unit_tests]


async def test_cache_key_independent_of_location_order():
    locs = ["a", "b", "c"]
    a = lookup_id(locs, "test")
    b = lookup_id(reversed(locs), "test")

    assert a == b


async def test_cache_key_dependent_on_searchterm():
    locs = ["a", "b", "c"]
    a = lookup_id(locs, "test")
    b = lookup_id(locs, "another test")

    assert not a == b
