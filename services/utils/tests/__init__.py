from services.utils import DBConfiguration
import pytest


def get_test_dbconf():
    """
    Returns a DBConfiguration suitable for the test environment, or exits pytest completely upon failure
    """
    db_conf = DBConfiguration(timeout=1)

    if db_conf.dsn != "dbname=test user=test host=db_test port=5432 password=test":
        pytest.exit("The test suite should only be run in a test environment. \n \
            Configured database host is not suited for running tests. \n \
            expected DSN to be: dbname=test user=test host=db_test port=5432 password=test")

    return db_conf
