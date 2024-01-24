import pytest

from services.ui_backend_service.api.search import _parse_search_term

pytestmark = [pytest.mark.unit_tests]


async def test_search_term_parsing():

    op, term = _parse_search_term("\"test term\"")

    assert op == "eq"
    assert term == "test term"

    op, term = _parse_search_term("test term")

    assert op == "co"
    assert term == "test term"

    op, term = _parse_search_term("test \"term\"")

    assert op == "co"
    assert term == "test \"term\""
