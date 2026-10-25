import os
import socket
from dataclasses import dataclass
from math import isfinite
from uuid import uuid4

from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError


class ConfigurationError(ValueError):
    """Raised when application configuration is invalid."""


def _float_setting(
    name: str,
    default: float,
    *,
    minimum: float = 0.0,
) -> float:
    raw_value = os.environ.get(name, str(default))
    try:
        value = float(raw_value)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be a number") from error
    if value < minimum or not isfinite(value):
        raise ConfigurationError(f"{name} must be at least {minimum}")
    return value


def _int_setting(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw_value = os.environ.get(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be an integer") from error
    if not minimum <= value <= maximum:
        raise ConfigurationError(f"{name} must be between {minimum} and {maximum}")
    return value


@dataclass(frozen=True)
class Settings:
    database_url: URL
    db_pool_size: int
    db_max_overflow: int
    consumer_id: str
    worker_concurrency: int
    poll_interval_seconds: float
    claim_batch_size: int
    lease_seconds: float
    shutdown_grace_seconds: float
    reconnect_base_seconds: float
    reconnect_max_seconds: float
    retry_base_seconds: float
    retry_max_seconds: float
    retry_jitter_ratio: float
    lease_reap_interval_seconds: float

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

        consumer_id = os.environ.get("CONSUMER_ID", "").strip()
        if not consumer_id:
            consumer_id = f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex[:8]}"
        if len(consumer_id) > 255 or "\x00" in consumer_id:
            raise ConfigurationError("CONSUMER_ID must be at most 255 characters")

        reconnect_base = _float_setting(
            "DB_RECONNECT_BASE_SECONDS",
            0.5,
            minimum=0.01,
        )
        reconnect_max = _float_setting(
            "DB_RECONNECT_MAX_SECONDS",
            15.0,
            minimum=0.01,
        )
        if reconnect_max < reconnect_base:
            raise ConfigurationError(
                "DB_RECONNECT_MAX_SECONDS must be at least DB_RECONNECT_BASE_SECONDS"
            )

        retry_base = _float_setting("RETRY_BASE_SECONDS", 1.0, minimum=0.01)
        retry_max = _float_setting("RETRY_MAX_SECONDS", 60.0, minimum=0.01)
        if retry_max < retry_base:
            raise ConfigurationError(
                "RETRY_MAX_SECONDS must be at least RETRY_BASE_SECONDS"
            )
        retry_jitter_ratio = _float_setting(
            "RETRY_JITTER_RATIO",
            0.25,
            minimum=0.0,
        )
        if retry_jitter_ratio > 1:
            raise ConfigurationError("RETRY_JITTER_RATIO must not exceed 1")

        return cls(
            database_url=database_url,
            db_pool_size=_int_setting(
                "DB_POOL_SIZE",
                5,
                minimum=1,
                maximum=100,
            ),
            db_max_overflow=_int_setting(
                "DB_MAX_OVERFLOW",
                10,
                minimum=0,
                maximum=100,
            ),
            consumer_id=consumer_id,
            worker_concurrency=_int_setting(
                "WORKER_CONCURRENCY",
                4,
                minimum=1,
                maximum=256,
            ),
            poll_interval_seconds=_float_setting(
                "POLL_INTERVAL_SECONDS",
                0.5,
                minimum=0.01,
            ),
            claim_batch_size=_int_setting(
                "CLAIM_BATCH_SIZE",
                10,
                minimum=1,
                maximum=100,
            ),
            lease_seconds=_float_setting(
                "LEASE_SECONDS",
                30.0,
                minimum=0.01,
            ),
            shutdown_grace_seconds=_float_setting(
                "SHUTDOWN_GRACE_SECONDS",
                20.0,
            ),
            reconnect_base_seconds=reconnect_base,
            reconnect_max_seconds=reconnect_max,
            retry_base_seconds=retry_base,
            retry_max_seconds=retry_max,
            retry_jitter_ratio=retry_jitter_ratio,
            lease_reap_interval_seconds=_float_setting(
                "LEASE_REAP_INTERVAL_SECONDS",
                5.0,
                minimum=0.01,
            ),
        )
