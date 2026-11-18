import pytest
from multidict import MultiDict

from services.data import filter_grammar
from services.ui_backend_service.api import utils as ui_utils

pytestmark = [pytest.mark.unit_tests]


# Both services must parse filters identically.
@pytest.fixture(params=[filter_grammar, ui_utils], ids=["filter_grammar", "ui_backend"])
def custom_conditions_query_dict(request):
    return request.param.custom_conditions_query_dict


@pytest.mark.parametrize(
    "query, condition, values",
    [
        ("user_name=null", '("user_name" IS %s)', [None]),
        ("user_name:eq=null", '("user_name" IS %s)', [None]),
        ("user_name:is=null", '("user_name" IS %s)', [None]),
        ("user_name:ne=null", '("user_name" IS NOT %s)', [None]),
        (
            "user_name:ne=null,alice",
            '("user_name" IS NOT %s OR "user_name" != %s)',
            [None, "alice"],
        ),
    ],
)
def test_custom_conditions_null_values(
    custom_conditions_query_dict, query, condition, values
):
    key, val = query.split("=", 1)
    conditions, _values = custom_conditions_query_dict(
        MultiDict({key: val}), allowed_keys=["user_name"]
    )

    assert conditions == [condition]
    assert _values == values
