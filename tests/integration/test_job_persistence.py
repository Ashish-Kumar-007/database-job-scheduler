import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine as sqlalchemy_create_engine, delete
from sqlalchemy.engine import URL, make_url
from sqlalchemy.orm import sessionmaker

from app.models import Job, JobStatus, Organization
from app.repositories.jobs import JobRepository
from app.repositories.organizations import OrganizationRepository
from app.services.errors import OrganizationNotFound
from app.services.jobs import JobService
from app.services.organizations import OrganizationService


@pytest.fixture
def migrated_test_database(monkeypatch: pytest.MonkeyPatch) -> URL:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("Set TEST_DATABASE_URL to a dedicated PostgreSQL test database")

    project_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", database_url)
    alembic_config = Config(str(project_root / "alembic.ini"))
    alembic_config.set_main_option(
        "script_location",
        str(project_root / "migrations"),
    )
    command.upgrade(alembic_config, "head")
    return make_url(database_url)


@pytest.mark.integration
def test_organization_and_job_persist_across_engine_restart(
    migrated_test_database,
) -> None:
    organization_id = None
    job_id = None
    initial_engine = sqlalchemy_create_engine(migrated_test_database, pool_pre_ping=True)
    initial_sessions = sessionmaker(bind=initial_engine, expire_on_commit=False)
    try:
        organizations = OrganizationService(OrganizationRepository(initial_sessions))
        organization = organizations.create(
            name=f"persistence-test-{uuid4()}",
            max_concurrent_jobs=2,
        )
        organization_id = organization.id
        assert organization_id is not None
        jobs = JobService(JobRepository(initial_sessions))
        submitted_job = jobs.submit(
            organization_id=organization_id,
            job_type="test.persist",
            payload={"value": 42},
            scheduled_at=datetime.now(UTC),
            max_attempts=2,
        )
        job_id = submitted_job.id
        assert job_id is not None
        persisted_organization = organizations.get_by_id(organization_id)
        assert persisted_organization is not None
        assert persisted_organization.id == organization_id
        with pytest.raises(OrganizationNotFound):
            jobs.submit(
                organization_id=uuid4(),
                job_type="test.orphan",
                payload={},
                scheduled_at=datetime.now(UTC),
            )
    finally:
        initial_engine.dispose()

    restarted_engine = sqlalchemy_create_engine(
        migrated_test_database,
        pool_pre_ping=True,
    )
    restarted_sessions = sessionmaker(bind=restarted_engine, expire_on_commit=False)
    try:
        child_environment = os.environ.copy()
        child_environment["DATABASE_URL"] = migrated_test_database.render_as_string(
            hide_password=False
        )
        child_environment["PERSISTENCE_TEST_JOB_ID"] = str(job_id)
        child_environment["PERSISTENCE_TEST_ORGANIZATION_ID"] = str(organization_id)
        child_script = """
import os
from uuid import UUID

from app.config import Settings
from app.db.engine import create_engine
from app.repositories.jobs import JobRepository
from app.repositories.organizations import OrganizationRepository
from app.services.jobs import JobService
from app.services.organizations import OrganizationService
from sqlalchemy.orm import sessionmaker

engine = create_engine(Settings.from_env())
try:
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    job = JobService(JobRepository(sessions)).get_by_id(
        UUID(os.environ["PERSISTENCE_TEST_JOB_ID"])
    )
    organization = OrganizationService(
        OrganizationRepository(sessions)
    ).get_by_id(UUID(os.environ["PERSISTENCE_TEST_ORGANIZATION_ID"]))
    assert job is not None and job.payload == {"value": 42}
    assert organization is not None
    print("Persisted organization and job reloaded in a new process.")
finally:
    engine.dispose()
"""
        subprocess_result = subprocess.run(
            [sys.executable, "-c", child_script],
            cwd=Path(__file__).resolve().parents[2],
            env=child_environment,
            check=True,
            capture_output=True,
            text=True,
        )
        assert "reloaded in a new process" in subprocess_result.stdout

        jobs = JobService(JobRepository(restarted_sessions))
        persisted_job = jobs.get_by_id(job_id)

        assert persisted_job is not None
        assert persisted_job.organization_id == organization_id
        assert persisted_job.payload == {"value": 42}
        assert persisted_job.status == JobStatus.SCHEDULED
        listed_jobs = jobs.list_for_organization(organization_id)
        assert [job.id for job in listed_jobs] == [persisted_job.id]
        assert jobs.list_for_organization(
            organization_id,
            status=JobStatus.COMPLETED,
        ) == []
    finally:
        with restarted_engine.begin() as connection:
            if job_id is not None:
                connection.execute(delete(Job).where(Job.id == job_id))
            if organization_id is not None:
                connection.execute(
                    delete(Organization).where(Organization.id == organization_id)
                )
        restarted_engine.dispose()
