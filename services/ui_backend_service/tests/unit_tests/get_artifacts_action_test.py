import pytest

from services.ui_backend_service.data.cache.get_data_action import lookup_id

pytestmark = [pytest.mark.unit_tests]


async def test_cache_key_independent_of_location_order():
    locs = ["a", "b", "c"]
    a = lookup_id(locs)
    b = lookup_id(reversed(locs))

    assert a == b
