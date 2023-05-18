from services.utils import DBConfiguration
import pytest
import os


def get_test_dbconf():
    """
    Returns a DBConfiguration suitable for the test environment, or exits pytest completely upon failure
    """
    db_conf = DBConfiguration(timeout=1)
    prefix = "MF_METADATA_DB_"
    ssl_mode = os.environ.get(prefix + "SSL_MODE")
    ssl_cert_path = os.environ.get(prefix + "SSL_CERT_PATH")
    ssl_key_path = os.environ.get(prefix + "SSL_KEY_PATH"),
    ssl_root_cert_path = os.environ.get(prefix + "SSL_ROOT_CERT_PATH")

    expected_dsn = f"dbname=test user=test host=db_test port=5432 password=test"
    if (ssl_mode in ['allow', 'prefer', 'require', 'verify-ca', 'verify-full']):
        ssl_query = f'sslmode={ssl_mode}'
        if ssl_cert_path is not None:
            ssl_query = f'{ssl_query} sslcert={ssl_cert_path}'
        if ssl_key_path is not None:
            ssl_query = f'{ssl_query} sslkey={ssl_key_path}'
        if ssl_root_cert_path is not None:
            ssl_query = f'{ssl_query} sslrootcert={ssl_root_cert_path}'
    else:
        ssl_query = f'sslmode=disable'

    if db_conf.dsn != f"{expected_dsn} {ssl_query}":
        pytest.exit("The test suite should only be run in a test environment. \n \
            Configured database host is not suited for running tests. \n \
            expected DSN to be: dbname=test user=test host=db_test port=5432 password=test")

    return db_conf
