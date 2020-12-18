import pytest

# we need to register the utils helper for assert rewriting in order to get descriptive assertion errors.
pytest.register_assert_rewrite("services.ui_backend_service.tests.integration_tests.utils")
