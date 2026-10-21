import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

pytestmark = [pytest.mark.unit_tests]

REPO_ROOT = Path(__file__).resolve().parents[4]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
sys.path.insert(0, str(REPO_ROOT))
import run_goose  # noqa: E402

SSL_ENV_KEYS = (
    "MF_METADATA_DB_SSL_MODE",
    "MF_METADATA_DB_SSL_CERT_PATH",
    "MF_METADATA_DB_SSL_KEY_PATH",
    "MF_METADATA_DB_SSL_ROOT_CERT",
    "MF_METADATA_DB_SSL_ROOT_CERT_PATH",
)

DB_ENV = {
    "MF_METADATA_DB_USER": "postgres",
    "MF_METADATA_DB_PSWD": "postgres",
    "MF_METADATA_DB_HOST": "db",
    "MF_METADATA_DB_PORT": "5432",
    "MF_METADATA_DB_NAME": "postgres",
}


def _dsn_from_main(monkeypatch, extra_env=None):
    env = dict(DB_ENV)
    if extra_env:
        env.update(extra_env)
    monkeypatch.setattr(os, "environ", env)
    monkeypatch.setattr(run_goose, "wait_for_postgres", lambda *args, **kwargs: None)
    captured = {}

    def fake_popen(args):
        captured["dsn"] = args[4]
        proc = MagicMock()
        proc.wait.return_value = 0
        return proc

    monkeypatch.setattr(run_goose, "Popen", fake_popen)
    monkeypatch.setattr("sys.argv", ["run_goose.py", "--wait", "0"])
    run_goose.main()
    return captured["dsn"]


def test_docker_compose_passes_through_ssl_env_without_defaults():
    text = COMPOSE_FILE.read_text()
    for key in SSL_ENV_KEYS:
        assert f"{key}=${{{key}:-}}" in text
    assert "MF_METADATA_DB_HOST=db" in text
    assert "MF_METADATA_DB_USER=postgres" in text
    assert "global-bundle.pem" not in text
    assert "rds-ca" not in text.lower()


def test_run_goose_unset_ssl_disables_ssl(monkeypatch):
    dsn = _dsn_from_main(monkeypatch)
    assert dsn.endswith("?sslmode=disable")
    assert "sslcert=" not in dsn


def test_run_goose_empty_ssl_env_does_not_add_cert_args(monkeypatch):
    dsn = _dsn_from_main(
        monkeypatch,
        {
            "MF_METADATA_DB_SSL_MODE": "",
            "MF_METADATA_DB_SSL_CERT_PATH": "",
            "MF_METADATA_DB_SSL_KEY_PATH": "",
            "MF_METADATA_DB_SSL_ROOT_CERT": "",
            "MF_METADATA_DB_SSL_ROOT_CERT_PATH": "",
        },
    )
    assert dsn.endswith("?sslmode=disable")
    assert "sslcert=" not in dsn
    assert "sslkey=" not in dsn
    assert "sslrootcert=" not in dsn


def test_run_goose_ssl_mode_prefer(monkeypatch):
    dsn = _dsn_from_main(monkeypatch, {"MF_METADATA_DB_SSL_MODE": "prefer"})
    assert "sslmode=prefer" in dsn
    assert "sslcert=" not in dsn


def test_run_goose_ssl_cert_paths(monkeypatch):
    dsn = _dsn_from_main(
        monkeypatch,
        {
            "MF_METADATA_DB_SSL_MODE": "verify-ca",
            "MF_METADATA_DB_SSL_CERT_PATH": "/certs/client.crt",
            "MF_METADATA_DB_SSL_KEY_PATH": "/certs/client.key",
            "MF_METADATA_DB_SSL_ROOT_CERT": "/certs/root.crt",
        },
    )
    assert "sslmode=verify-ca" in dsn
    assert "sslcert=/certs/client.crt" in dsn
    assert "sslkey=/certs/client.key" in dsn
    assert "sslrootcert=/certs/root.crt" in dsn


def test_run_goose_ssl_root_cert_only(monkeypatch):
    dsn = _dsn_from_main(
        monkeypatch,
        {
            "MF_METADATA_DB_SSL_MODE": "require",
            "MF_METADATA_DB_SSL_ROOT_CERT": "/certs/goose-root.crt",
        },
    )
    assert "sslrootcert=/certs/goose-root.crt" in dsn


def test_run_goose_ssl_root_cert_path_alias(monkeypatch):
    dsn = _dsn_from_main(
        monkeypatch,
        {
            "MF_METADATA_DB_SSL_MODE": "require",
            "MF_METADATA_DB_SSL_ROOT_CERT_PATH": "/certs/root.crt",
        },
    )
    assert "sslmode=require" in dsn
    assert "sslrootcert=/certs/root.crt" in dsn


def test_run_goose_ssl_root_cert_prefers_historical_var_when_both_set(monkeypatch):
    dsn = _dsn_from_main(
        monkeypatch,
        {
            "MF_METADATA_DB_SSL_MODE": "require",
            "MF_METADATA_DB_SSL_ROOT_CERT": "/certs/goose-root.crt",
            "MF_METADATA_DB_SSL_ROOT_CERT_PATH": "/certs/service-root.crt",
        },
    )
    assert "sslrootcert=/certs/goose-root.crt" in dsn
    assert "sslrootcert=/certs/service-root.crt" not in dsn


def test_run_goose_ssl_root_cert_empty_falls_back_to_path(monkeypatch):
    dsn = _dsn_from_main(
        monkeypatch,
        {
            "MF_METADATA_DB_SSL_MODE": "require",
            "MF_METADATA_DB_SSL_ROOT_CERT": "",
            "MF_METADATA_DB_SSL_ROOT_CERT_PATH": "/certs/service-root.crt",
        },
    )
    assert "sslrootcert=/certs/service-root.crt" in dsn
