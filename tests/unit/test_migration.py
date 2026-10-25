from io import StringIO
from pathlib import Path

from alembic import command
from alembic.config import Config
import pytest


def test_initial_migration_renders_postgresql_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = Path(__file__).resolve().parents[2]
    output = StringIO()
    config = Config(
        str(project_root / "alembic.ini"),
        output_buffer=output,
    )
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg://scheduler:test-only@localhost:5432/scheduler",
    )

    command.upgrade(config, "head", sql=True)

    migration_sql = output.getvalue()
    assert "CREATE TABLE organizations" in migration_sql
    assert "CREATE TABLE jobs" in migration_sql
    assert "payload JSONB NOT NULL" in migration_sql
    assert "ck_jobs_claim_metadata_matches_status" in migration_sql
    assert "ix_jobs_expired_lease" in migration_sql
