import pytest

# we need to register the utils helper for assert rewriting in order to get descriptive assertion errors.
pytest.register_assert_rewrite("services.metadata_service.tests.integration_tests.utils")
