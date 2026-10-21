import subprocess

import psycopg2
import pytest

from services.migration_service.api import make_goose_migration_template
from services.migration_service.api.utils import ApiUtils
from services.utils.tests import get_test_dbconf

pytestmark = [pytest.mark.integration_tests]

# Migrations are applied in a dedicated schema so that rolling them back
# does not interfere with the tables used by the other integration tests.
TEST_SCHEMA = "migration_test"


@pytest.fixture
def goose():
    db_conf = get_test_dbconf()
    conn = psycopg2.connect(db_conf.get_dsn())
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
        cur.execute(f"CREATE SCHEMA {TEST_SCHEMA}")

    conn_str = f"{db_conf.connection_string_url()}&search_path={TEST_SCHEMA}"

    def run(command):
        result = subprocess.run(
            make_goose_migration_template(conn_str, command),
            shell=True,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"goose {command} failed:\n{result.stderr}"
        return result.stderr

    yield run

    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
    conn.close()


def test_migrations_apply_and_roll_back(goose):
    latest_version = ApiUtils.list_migrations()[-1]

    goose("up")
    assert f"goose: version {latest_version}" in goose("version")

    # Roll back every migration, one by one
    goose("reset")
    assert "goose: version 0" in goose("version")

    # Migrations can be applied again after a full rollback
    goose("up")
    assert f"goose: version {latest_version}" in goose("version")
