import pytest
from sqlalchemy.engine import URL

from app.config import ConfigurationError, Settings


def test_settings_parse_escaped_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg://scheduler:p%40ss%3Aword@localhost:5432/jobs",
    )

    settings = Settings.from_env()

    assert settings.database_url == URL.create(
        "postgresql+psycopg",
        username="scheduler",
        password="p@ss:word",
        host="localhost",
        port=5432,
        database="jobs",
    )
    assert settings.db_pool_size == 5
    assert settings.db_max_overflow == 10
    assert settings.worker_concurrency == 4
    assert settings.claim_batch_size == 10
    assert settings.lease_seconds == 30
    assert settings.poll_interval_seconds == 0.5
    assert settings.consumer_id
    assert settings.retry_base_seconds == 1


def test_settings_accept_configured_consumer_parameters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg://scheduler:secret@localhost:5432/jobs",
    )
    monkeypatch.setenv("CONSUMER_ID", "test-worker-1")
    monkeypatch.setenv("WORKER_CONCURRENCY", "3")
    monkeypatch.setenv("CLAIM_BATCH_SIZE", "7")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "0.2")
    monkeypatch.setenv("LEASE_SECONDS", "42")
    monkeypatch.setenv("SHUTDOWN_GRACE_SECONDS", "5")
    monkeypatch.setenv("DB_RECONNECT_BASE_SECONDS", "0.25")
    monkeypatch.setenv("DB_RECONNECT_MAX_SECONDS", "4")
    monkeypatch.setenv("DB_POOL_SIZE", "8")
    monkeypatch.setenv("DB_MAX_OVERFLOW", "12")
    monkeypatch.setenv("RETRY_BASE_SECONDS", "0.4")
    monkeypatch.setenv("RETRY_MAX_SECONDS", "8")
    monkeypatch.setenv("RETRY_JITTER_RATIO", "0.5")
    monkeypatch.setenv("LEASE_REAP_INTERVAL_SECONDS", "2")

    settings = Settings.from_env()

    assert settings.consumer_id == "test-worker-1"
    assert settings.worker_concurrency == 3
    assert settings.claim_batch_size == 7
    assert settings.poll_interval_seconds == 0.2
    assert settings.lease_seconds == 42
    assert settings.shutdown_grace_seconds == 5
    assert settings.reconnect_base_seconds == 0.25
    assert settings.reconnect_max_seconds == 4
    assert settings.db_pool_size == 8
    assert settings.db_max_overflow == 12
    assert settings.retry_base_seconds == 0.4
    assert settings.retry_max_seconds == 8
    assert settings.retry_jitter_ratio == 0.5
    assert settings.lease_reap_interval_seconds == 2


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("WORKER_CONCURRENCY", "0"),
        ("WORKER_CONCURRENCY", "257"),
        ("CLAIM_BATCH_SIZE", "101"),
        ("POLL_INTERVAL_SECONDS", "nan"),
        ("LEASE_SECONDS", "0"),
        ("DB_RECONNECT_BASE_SECONDS", "16"),
        ("DB_RECONNECT_MAX_SECONDS", "0"),
        ("DB_POOL_SIZE", "0"),
        ("DB_MAX_OVERFLOW", "-1"),
        ("RETRY_BASE_SECONDS", "nan"),
        ("RETRY_MAX_SECONDS", "0"),
        ("RETRY_JITTER_RATIO", "1.1"),
        ("LEASE_REAP_INTERVAL_SECONDS", "0"),
    ],
)
def test_settings_reject_invalid_consumer_parameters(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
) -> None:
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg://scheduler:secret@localhost:5432/jobs",
    )
    monkeypatch.setenv(name, value)

    with pytest.raises(ConfigurationError):
        Settings.from_env()


@pytest.mark.parametrize(
    "database_url",
    [
        None,
        "not-a-url",
        "sqlite:///jobs.db",
        "postgresql+psycopg://scheduler@localhost/jobs",
    ],
)
def test_settings_reject_invalid_database_urls(
    monkeypatch: pytest.MonkeyPatch,
    database_url: str | None,
) -> None:
    if database_url is None:
        monkeypatch.delenv("DATABASE_URL", raising=False)
    else:
        monkeypatch.setenv("DATABASE_URL", database_url)

    with pytest.raises(ConfigurationError):
        Settings.from_env()
