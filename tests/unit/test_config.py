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
