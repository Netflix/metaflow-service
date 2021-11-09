import pytest

from services.ui_backend_service.data import unpack_processed_value

pytestmark = [pytest.mark.unit_tests]


@pytest.mark.parametrize("value, expected", [
    ([True, "test_value"], [True, 'test_value', None, None]),
    ([False, "CustomException"], [False, 'CustomException', None, None]),
    ([False, "CustomException", "error details"], [False, 'CustomException', "error details", None]),
    ([False, "CustomException", "error details", "stacktrace"], [False, 'CustomException', "error details", "stacktrace"]),
])
def test_unpack_processed_value_padding(value, expected):
    # test that the helper pads the output list with enough None items by default.
    assert unpack_processed_value(value) == expected
