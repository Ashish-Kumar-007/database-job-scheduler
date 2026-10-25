import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
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


def _create_due_job(organization_id, sessions, *, scheduled_at=None):
    return JobService(JobRepository(sessions)).submit(
        organization_id=organization_id,
        job_type="test.claim",
        payload={"marker": str(uuid4())},
        scheduled_at=scheduled_at or datetime.now(UTC),
        max_attempts=3,
    )


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


@pytest.mark.integration
def test_sequential_claim_is_unique_and_records_ownership(
    migrated_test_database: URL,
) -> None:
    engine = sqlalchemy_create_engine(migrated_test_database, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    organization = OrganizationService(
        OrganizationRepository(sessions)
    ).create(f"claim-test-{uuid4()}", max_concurrent_jobs=1)
    job = _create_due_job(organization.id, sessions)
    try:
        repository = JobRepository(sessions)
        claims = repository.claim_due_jobs("consumer-a", lease_seconds=45)

        assert len(claims) == 1
        claim = next(claim for claim in claims if claim.job_id == job.id)
        assert claim.owner == "consumer-a"
        assert claim.attempts == 1
        assert claim.max_attempts == 3
        assert claim.claim_token is not None
        assert claim.lease_expires_at > datetime.now(UTC)
        assert repository.claim_due_jobs("consumer-b") == []

        persisted_job = repository.get_by_id(job.id)
        assert persisted_job is not None
        assert persisted_job.status == JobStatus.RUNNING
        assert persisted_job.locked_by == "consumer-a"
        assert persisted_job.claim_token == claim.claim_token
        assert persisted_job.attempts == 1
        assert persisted_job.started_at is None
    finally:
        with engine.begin() as connection:
            connection.execute(delete(Job).where(Job.id == job.id))
            connection.execute(
                delete(Organization).where(Organization.id == organization.id)
            )
        engine.dispose()


@pytest.mark.integration
def test_future_job_is_not_claimed_early(
    migrated_test_database: URL,
) -> None:
    engine = sqlalchemy_create_engine(migrated_test_database, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    organization = OrganizationService(
        OrganizationRepository(sessions)
    ).create(f"future-claim-test-{uuid4()}", max_concurrent_jobs=1)
    job = _create_due_job(
        organization.id,
        sessions,
        scheduled_at=datetime.now(UTC) + timedelta(days=1),
    )
    try:
        repository = JobRepository(sessions)
        claims = repository.claim_due_jobs("consumer-future")

        assert all(claim.job_id != job.id for claim in claims)
        assert repository.get_by_id(job.id).status == JobStatus.SCHEDULED
    finally:
        with engine.begin() as connection:
            connection.execute(delete(Job).where(Job.id == job.id))
            connection.execute(
                delete(Organization).where(Organization.id == organization.id)
            )
        engine.dispose()


@pytest.mark.integration
def test_concurrent_claimers_cannot_both_claim_the_same_job(
    migrated_test_database: URL,
) -> None:
    engine = sqlalchemy_create_engine(migrated_test_database, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    organization = OrganizationService(
        OrganizationRepository(sessions)
    ).create(f"concurrent-claim-test-{uuid4()}", max_concurrent_jobs=1)
    job = _create_due_job(organization.id, sessions)
    barrier = Barrier(2)

    def claim_as(owner: str):
        repository = JobRepository(sessions)
        barrier.wait(timeout=10)
        return repository.claim_due_jobs(owner, limit=1)

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(claim_as, "consumer-one"),
                executor.submit(claim_as, "consumer-two"),
            ]
            claims_by_consumer = [future.result(timeout=20) for future in futures]

        all_claims = [
            claim for consumer_claims in claims_by_consumer for claim in consumer_claims
        ]
        claims_for_job = [claim for claim in all_claims if claim.job_id == job.id]
        assert len(claims_for_job) == 1
        assert claims_for_job[0].attempts == 1
        assert len({claim.claim_token for claim in all_claims}) == len(all_claims)
    finally:
        with engine.begin() as connection:
            connection.execute(delete(Job).where(Job.id == job.id))
            connection.execute(
                delete(Organization).where(Organization.id == organization.id)
            )
        engine.dispose()
