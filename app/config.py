import os
from dataclasses import dataclass

from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError


class ConfigurationError(ValueError):
    """Raised when application configuration is invalid."""


@dataclass(frozen=True)
class Settings:
    database_url: URL

    @classmethod
    def from_env(cls) -> "Settings":
        raw_url = os.environ.get("DATABASE_URL")
        if not raw_url:
            raise ConfigurationError("DATABASE_URL must be set")

        try:
            database_url = make_url(raw_url)
        except ArgumentError:
            raise ConfigurationError(
                "DATABASE_URL must be a valid SQLAlchemy URL"
            ) from None

        if (
            database_url.drivername != "postgresql+psycopg"
            or not database_url.host
            or not database_url.database
            or not database_url.username
            or not database_url.password
        ):
            raise ConfigurationError(
                "DATABASE_URL must specify PostgreSQL via psycopg, "
                "including host, database, username, and password"
            )

        return cls(database_url=database_url)
