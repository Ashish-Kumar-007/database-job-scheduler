import os
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier, Event
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import (
    create_engine as _sqlalchemy_create_engine,
    delete,
    event,
    func,
    select,
    update,
)
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from app.config import Settings
from app.consumers import Consumer
from app.handlers import HandlerRegistry
from app.models import Job, JobStatus, Organization, SampleOperation
from app.repositories.jobs import JobRepository
from app.repositories.organizations import OrganizationRepository
from app.services.errors import OrganizationNotFound
from app.services.jobs import JobService
from app.services.organizations import OrganizationService
from app.workers import WorkerPool


def sqlalchemy_create_engine(*args, **kwargs):
    connect_args = kwargs.pop("connect_args", {})
    return _sqlalchemy_create_engine(
        *args,
        connect_args={**connect_args, "prepare_threshold": None},
        **kwargs,
    )


def _create_due_job(
    organization_id,
    sessions,
    *,
    scheduled_at=None,
    job_type="test.claim",
):
    return JobService(JobRepository(sessions)).submit(
        organization_id=organization_id,
        job_type=job_type,
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
            stdin=subprocess.DEVNULL,
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
def test_consumer_drains_backlog_and_reports_delay_statistics(
    migrated_test_database: URL,
) -> None:
    engine = sqlalchemy_create_engine(migrated_test_database, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    organizations = []
    jobs = []
    try:
        organization_service = OrganizationService(OrganizationRepository(sessions))
        organizations = [
            organization_service.create(
                f"backlog-test-{index}-{uuid4()}",
                max_concurrent_jobs=4,
            )
            for index in range(3)
        ]
        scheduled_at = datetime.now(UTC) - timedelta(seconds=2)
        job_service = JobService(JobRepository(sessions))
        jobs = [
            job_service.submit(
                organization_id=organization.id,
                job_type="sample.backlog",
                payload={"index": index},
                scheduled_at=scheduled_at,
                max_attempts=2,
            )
            for index, organization in enumerate(organizations * 10)
        ]

        repository = JobRepository(sessions)
        handlers = HandlerRegistry()
        handlers.register("sample.backlog", lambda *args: "ok")
        settings = Settings(
            database_url=migrated_test_database,
            db_pool_size=5,
            db_max_overflow=10,
            consumer_id=f"backlog-consumer-{uuid4()}",
            worker_concurrency=4,
            poll_interval_seconds=0.01,
            claim_batch_size=10,
            lease_seconds=30,
            shutdown_grace_seconds=2,
            reconnect_base_seconds=0.01,
            reconnect_max_seconds=1,
            retry_base_seconds=0.01,
            retry_max_seconds=1,
            retry_jitter_ratio=0,
            lease_reap_interval_seconds=1,
        )
        result = Consumer(
            settings,
            repository,
            WorkerPool(repository, handlers, max_workers=4),
        ).run(Event(), max_completed=len(jobs))

        assert result.claimed == len(jobs)
        assert result.completed == len(jobs)
        assert result.failed == 0
        assert result.stale == 0
        assert result.completed_per_second > 0
        assert result.scheduling_delay_average_seconds is not None
        assert result.scheduling_delay_max_seconds >= (
            result.scheduling_delay_average_seconds
        )
        assert result.scheduling_delay_p95_seconds is not None
        persisted = [repository.get_by_id(job.id) for job in jobs]
        assert all(
            job is not None and job.status == JobStatus.COMPLETED
            for job in persisted
        )
        assert all(
            job is not None
            and job.started_at is not None
            and job.completed_at is not None
            and job.started_at >= job.scheduled_at
            and job.completed_at >= job.started_at
            for job in persisted
        )
    finally:
        with engine.begin() as connection:
            if jobs:
                connection.execute(delete(Job).where(Job.id.in_([job.id for job in jobs])))
            if organizations:
                connection.execute(
                    delete(Organization).where(
                        Organization.id.in_(
                            [organization.id for organization in organizations]
                        )
                    )
                )
        engine.dispose()


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
def test_database_time_boundary_claims_past_and_equal_but_not_future_jobs(
    migrated_test_database: URL,
) -> None:
    engine = sqlalchemy_create_engine(migrated_test_database, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    organizations = [
        OrganizationService(OrganizationRepository(sessions)).create(
            f"schedule-boundary-{index}-{uuid4()}",
            max_concurrent_jobs=1,
        )
        for index in range(3)
    ]
    repository = JobRepository(sessions)
    jobs = []
    try:
        with engine.connect() as connection:
            database_now = connection.scalar(select(func.clock_timestamp()))
        assert database_now is not None

        job_service = JobService(repository)
        jobs = [
            job_service.submit(
                organization_id=organizations[0].id,
                job_type="test.schedule-boundary",
                payload={"boundary": "before"},
                scheduled_at=database_now - timedelta(seconds=1),
                max_attempts=1,
            ),
            job_service.submit(
                organization_id=organizations[1].id,
                job_type="test.schedule-boundary",
                payload={"boundary": "equal"},
                scheduled_at=database_now,
                max_attempts=1,
            ),
            job_service.submit(
                organization_id=organizations[2].id,
                job_type="test.schedule-boundary",
                payload={"boundary": "after"},
                scheduled_at=database_now + timedelta(seconds=5),
                max_attempts=1,
            ),
        ]

        claims = repository.claim_due_jobs("schedule-boundary-consumer", limit=3)

        assert {claim.job_id for claim in claims} == {
            jobs[0].id,
            jobs[1].id,
        }
        assert repository.get_by_id(jobs[2].id).status == JobStatus.SCHEDULED
    finally:
        with engine.begin() as connection:
            if jobs:
                connection.execute(delete(Job).where(Job.id.in_([job.id for job in jobs])))
            connection.execute(
                delete(Organization).where(
                    Organization.id.in_(
                        [organization.id for organization in organizations]
                    )
                )
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


@pytest.mark.integration
def test_round_robin_claims_give_small_tenants_turns_and_bound_prefetch(
    migrated_test_database: URL,
) -> None:
    engine = sqlalchemy_create_engine(migrated_test_database, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    organizations = [
        OrganizationService(OrganizationRepository(sessions)).create(
            f"fairness-test-{index}-{uuid4()}",
            max_concurrent_jobs=10,
        )
        for index in range(3)
    ]
    jobs = [
        _create_due_job(organizations[0].id, sessions)
        for _ in range(8)
    ]
    jobs.extend(
        _create_due_job(organization.id, sessions)
        for organization in organizations[1:]
        for _ in range(3)
    )
    repository = JobRepository(sessions)
    try:
        first_turns = repository.claim_due_jobs("round-robin-test", limit=10)

        assert len(first_turns) == 3
        assert {claim.organization_id for claim in first_turns} == {
            organization.id for organization in organizations
        }
        assert repository.claim_due_jobs("prefetch-test", limit=10) == []

        for claim in first_turns:
            assert repository.start_claim(claim) is not None

        next_turns = repository.claim_due_jobs("next-round-test", limit=10)
        assert [claim.organization_id for claim in next_turns] == [
            claim.organization_id for claim in first_turns
        ]
    finally:
        with engine.begin() as connection:
            connection.execute(delete(Job).where(Job.id.in_([job.id for job in jobs])))
            connection.execute(
                delete(Organization).where(
                    Organization.id.in_([organization.id for organization in organizations])
                )
            )
        engine.dispose()


@pytest.mark.integration
def test_concurrent_processes_respect_organization_concurrency_limit(
    migrated_test_database: URL,
) -> None:
    project_root = Path(__file__).resolve().parents[2]
    engine = sqlalchemy_create_engine(migrated_test_database, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    organization = OrganizationService(
        OrganizationRepository(sessions)
    ).create(f"tenant-cap-process-test-{uuid4()}", max_concurrent_jobs=1)
    jobs = [
        _create_due_job(organization.id, sessions)
        for _ in range(2)
    ]
    child_script = """
import os
import time
from pathlib import Path
from app.config import Settings
from app.db.engine import create_engine
from app.db.session import create_session_factory
from app.repositories.jobs import JobRepository

barrier = Path(os.environ["CLAIM_BARRIER_DIR"])
ready = barrier / f"ready-{os.environ['CLAIM_PROCESS_ID']}"
ready.touch()
deadline = time.monotonic() + 10
while len(list(barrier.glob("ready-*"))) < 2:
    if time.monotonic() >= deadline:
        raise TimeoutError("second claimant did not reach barrier")
    time.sleep(0.005)

engine = create_engine(Settings.from_env())
try:
    repository = JobRepository(create_session_factory(engine))
    claims = repository.claim_due_jobs(
        os.environ["CLAIM_PROCESS_ID"],
        limit=1,
    )
    print("claimed=" + ",".join(str(claim.job_id) for claim in claims))
finally:
    engine.dispose()
"""
    with tempfile.TemporaryDirectory(prefix="scheduler-tenant-claim-") as directory:
        processes = []
        try:
            for process_id in ("process-a", "process-b"):
                child_environment = os.environ.copy()
                child_environment["CLAIM_BARRIER_DIR"] = directory
                child_environment["CLAIM_PROCESS_ID"] = process_id
                processes.append(
                    subprocess.Popen(
                        [sys.executable, "-c", child_script],
                        cwd=project_root,
                        env=child_environment,
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                    )
                )
            outputs = [
                process.communicate(timeout=20)[0]
                for process in processes
            ]
            assert all(process.returncode == 0 for process in processes), outputs
            claimed_job_ids = [
                job_id
                for output in outputs
                for job_id in output.strip().split("=", 1)[1].split(",")
                if job_id
            ]
            assert sum(str(job.id) in claimed_job_ids for job in jobs) == 1, outputs
            running_jobs = [
                job
                for job in (JobRepository(sessions).get_by_id(item.id) for item in jobs)
                if job is not None and job.status == JobStatus.RUNNING
            ]
            assert len(running_jobs) == 1
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=5)
            with engine.begin() as connection:
                connection.execute(delete(Job).where(Job.id.in_([job.id for job in jobs])))
                connection.execute(
                    delete(Organization).where(Organization.id == organization.id)
                )
            engine.dispose()


@pytest.mark.integration
def test_tenant_limits_hold_with_mixed_job_durations(
    migrated_test_database: URL,
) -> None:
    engine = sqlalchemy_create_engine(migrated_test_database, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    organizations = [
        OrganizationService(OrganizationRepository(sessions)).create(
            f"mixed-duration-test-{index}-{uuid4()}",
            max_concurrent_jobs=1,
        )
        for index in range(3)
    ]
    jobs = [
        _create_due_job(
            organization.id,
            sessions,
            job_type=(
                "sample.slow"
                if organization is organizations[0] and index == 0
                else "sample.success"
            ),
        )
        for organization in organizations
        for index in range(2)
    ]
    repository = JobRepository(sessions)
    try:
        first_batch = repository.claim_due_jobs("mixed-duration-test", limit=10)
        assert len(first_batch) == len(organizations)
        assert len({claim.organization_id for claim in first_batch}) == len(organizations)

        long_running_claim = next(
            claim
            for claim in first_batch
            if claim.organization_id == organizations[0].id
        )
        short_running_claims = [
            claim
            for claim in first_batch
            if claim.organization_id in {item.id for item in organizations[1:]}
        ]
        assert long_running_claim.job_type == "sample.slow"
        assert repository.start_claim(long_running_claim) is not None
        for claim in short_running_claims:
            assert claim.job_type == "sample.success"
            assert repository.start_claim(claim) is not None
            assert repository.complete_claim(claim)

        short_tenant_turns = repository.claim_due_jobs(
            "short-tenant-turns",
            limit=10,
        )
        assert {
            claim.organization_id for claim in short_tenant_turns
        } == {organization.id for organization in organizations[1:]}
        assert all(
            claim.organization_id != organizations[0].id
            for claim in short_tenant_turns
        )
        for claim in short_tenant_turns:
            assert repository.start_claim(claim) is not None
            assert repository.complete_claim(claim)
        assert repository.complete_claim(long_running_claim)
    finally:
        with engine.begin() as connection:
            connection.execute(delete(Job).where(Job.id.in_([job.id for job in jobs])))
            connection.execute(
                delete(Organization).where(
                    Organization.id.in_([organization.id for organization in organizations])
                )
            )
        engine.dispose()


@pytest.mark.integration
def test_claim_completion_and_stale_token_rejection(
    migrated_test_database: URL,
) -> None:
    engine = sqlalchemy_create_engine(migrated_test_database, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    organization = OrganizationService(
        OrganizationRepository(sessions)
    ).create(f"completion-test-{uuid4()}", max_concurrent_jobs=1)
    job = _create_due_job(organization.id, sessions)
    repository = JobRepository(sessions)
    try:
        claim = next(
            claim
            for claim in repository.claim_due_jobs("worker-completion")
            if claim.job_id == job.id
        )

        assert repository.start_claim(claim)
        persisted_started = repository.get_by_id(job.id)
        assert persisted_started is not None
        assert persisted_started.started_at is not None
        assert repository.complete_claim(claim)
        assert not repository.complete_claim(claim)

        completed_job = repository.get_by_id(job.id)
        assert completed_job is not None
        assert completed_job.status == JobStatus.COMPLETED
        assert completed_job.started_at is not None
        assert completed_job.completed_at is not None
        assert completed_job.completed_at >= completed_job.started_at
        assert completed_job.locked_by is None
        assert completed_job.claim_token is None
    finally:
        with engine.begin() as connection:
            connection.execute(delete(Job).where(Job.id == job.id))
            connection.execute(
                delete(Organization).where(Organization.id == organization.id)
            )
        engine.dispose()


@pytest.mark.integration
def test_failure_is_retryable_then_terminal_after_attempt_limit(
    migrated_test_database: URL,
) -> None:
    engine = sqlalchemy_create_engine(migrated_test_database, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    organization = OrganizationService(
        OrganizationRepository(sessions)
    ).create(f"failure-test-{uuid4()}", max_concurrent_jobs=1)
    job = JobService(JobRepository(sessions)).submit(
        organization_id=organization.id,
        job_type="test.failure",
        payload={},
        scheduled_at=datetime.now(UTC),
        max_attempts=2,
    )
    repository = JobRepository(sessions)
    try:
        first_claim = next(
            claim
            for claim in repository.claim_due_jobs("worker-failure")
            if claim.job_id == job.id
        )
        assert repository.start_claim(first_claim)
        assert repository.fail_claim(first_claim, "first attempt failed") == (
            JobStatus.RETRY_WAIT
        )
        assert repository.fail_claim(first_claim, "stale failure") is None

        second_claim = next(
            claim
            for claim in repository.claim_due_jobs("worker-retry")
            if claim.job_id == job.id
        )
        assert second_claim.claim_token != first_claim.claim_token
        assert second_claim.attempts == 2
        assert repository.start_claim(second_claim)
        assert repository.fail_claim(second_claim, "second attempt failed") == (
            JobStatus.FAILED
        )

        failed_job = repository.get_by_id(job.id)
        assert failed_job is not None
        assert failed_job.status == JobStatus.FAILED
        assert failed_job.last_error == "second attempt failed"
        assert failed_job.claim_token is None
        assert failed_job.next_attempt_at is None
    finally:
        with engine.begin() as connection:
            connection.execute(delete(Job).where(Job.id == job.id))
            connection.execute(
                delete(Organization).where(Organization.id == organization.id)
            )
        engine.dispose()


@pytest.mark.integration
def test_completed_and_failed_jobs_are_never_claimed_again(
    migrated_test_database: URL,
) -> None:
    engine = sqlalchemy_create_engine(migrated_test_database, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    organizations = [
        OrganizationService(OrganizationRepository(sessions)).create(
            f"terminal-state-{index}-{uuid4()}",
            max_concurrent_jobs=1,
        )
        for index in range(2)
    ]
    jobs = [
        JobService(JobRepository(sessions)).submit(
            organization_id=organizations[index].id,
            job_type="test.terminal",
            payload={},
            scheduled_at=datetime.now(UTC),
            max_attempts=1,
        )
        for index in range(2)
    ]
    repository = JobRepository(sessions)
    try:
        claims = repository.claim_due_jobs("terminal-state-consumer", limit=2)
        claims_by_job = {claim.job_id: claim for claim in claims}

        completed_claim = claims_by_job[jobs[0].id]
        assert repository.start_claim(completed_claim) is not None
        assert repository.complete_claim(completed_claim)

        failed_claim = claims_by_job[jobs[1].id]
        assert repository.start_claim(failed_claim) is not None
        assert repository.fail_claim(
            failed_claim,
            "permanent test failure",
            retryable=False,
        ) == JobStatus.FAILED

        assert repository.claim_due_jobs("terminal-state-restart", limit=2) == []
        assert repository.get_by_id(jobs[0].id).status == JobStatus.COMPLETED
        assert repository.get_by_id(jobs[1].id).status == JobStatus.FAILED
    finally:
        with engine.begin() as connection:
            connection.execute(delete(Job).where(Job.id.in_([job.id for job in jobs])))
            connection.execute(
                delete(Organization).where(
                    Organization.id.in_(
                        [organization.id for organization in organizations]
                    )
                )
            )
        engine.dispose()


@pytest.mark.integration
def test_expired_claim_recovery_fences_old_owner_and_exhausts_attempts(
    migrated_test_database: URL,
) -> None:
    engine = sqlalchemy_create_engine(migrated_test_database, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    organization = OrganizationService(
        OrganizationRepository(sessions)
    ).create(f"lease-recovery-test-{uuid4()}", max_concurrent_jobs=1)
    job = JobService(JobRepository(sessions)).submit(
        organization_id=organization.id,
        job_type="test.recovery",
        payload={},
        scheduled_at=datetime.now(UTC),
        max_attempts=2,
    )
    repository = JobRepository(sessions)
    try:
        first_claim = next(
            claim
            for claim in repository.claim_due_jobs(
                "worker-before-crash",
                lease_seconds=60,
            )
            if claim.job_id == job.id
        )
        assert repository.start_claim(first_claim)
        assert repository.renew_lease(first_claim, lease_seconds=120)

        with engine.begin() as connection:
            connection.execute(
                update(Job)
                .where(Job.id == job.id)
                .values(
                    lease_expires_at=func.clock_timestamp()
                    - func.make_interval(0, 0, 0, 0, 0, 0, 1)
                )
            )

        recovered = repository.recover_expired_claims(retry_delay_seconds=0)
        assert [(item.job_id, item.status) for item in recovered] == [
            (job.id, JobStatus.RETRY_WAIT)
        ]
        assert not repository.complete_claim(first_claim)
        assert not repository.renew_lease(first_claim, lease_seconds=30)

        second_claim = next(
            claim
            for claim in repository.claim_due_jobs("worker-after-crash")
            if claim.job_id == job.id
        )
        assert second_claim.attempts == 2
        assert not repository.complete_claim(first_claim)
        with engine.begin() as connection:
            connection.execute(
                update(Job)
                .where(Job.id == job.id)
                .values(
                    lease_expires_at=func.clock_timestamp()
                    - func.make_interval(0, 0, 0, 0, 0, 0, 1)
                )
            )

        exhausted = repository.recover_expired_claims()
        assert [(item.job_id, item.status) for item in exhausted] == [
            (job.id, JobStatus.FAILED)
        ]
        failed = repository.get_by_id(job.id)
        assert failed is not None
        assert failed.status == JobStatus.FAILED
        assert failed.last_error is not None
        assert "attempt limit exhausted" in failed.last_error
    finally:
        with engine.begin() as connection:
            connection.execute(delete(Job).where(Job.id == job.id))
            connection.execute(
                delete(Organization).where(Organization.id == organization.id)
            )
        engine.dispose()


@pytest.mark.integration
def test_killed_consumer_recovers_idempotent_operation_after_restart(
    migrated_test_database: URL,
) -> None:
    project_root = Path(__file__).resolve().parents[2]
    engine = sqlalchemy_create_engine(migrated_test_database, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    organization = OrganizationService(
        OrganizationRepository(sessions)
    ).create(f"crash-recovery-test-{uuid4()}", max_concurrent_jobs=1)
    job = JobService(JobRepository(sessions)).submit(
        organization_id=organization.id,
        job_type="sample.idempotent",
        payload={"operation": "durable"},
        scheduled_at=datetime.now(UTC),
        max_attempts=2,
    )
    environment = os.environ.copy()
    environment["DATABASE_URL"] = migrated_test_database.render_as_string(
        hide_password=False
    )
    environment["CRASH_SIGNAL_FILE"] = ""
    child_script = """
import os
import time
from app.config import Settings
from app.db.engine import create_engine
from app.db.session import create_session_factory
from app.repositories.jobs import JobRepository
from uuid import UUID

engine = create_engine(Settings.from_env())
try:
    repository = JobRepository(create_session_factory(engine))
    job_id = UUID(os.environ["CRASH_TEST_JOB_ID"])
    claim = next(
        claim for claim in repository.claim_due_jobs("crash-test-consumer")
        if claim.job_id == job_id
    )
    assert repository.start_claim(claim)
    repository.record_sample_operation(
        job_id,
        str(job_id),
        {
            "job_id": str(job_id),
            "organization_id": str(claim.organization_id),
            "payload": claim.payload,
        },
    )
    with open(os.environ["CRASH_SIGNAL_FILE"], "w", encoding="utf-8") as signal:
        signal.write("side effect committed")
    time.sleep(120)
finally:
    engine.dispose()
"""
    with tempfile.TemporaryDirectory(prefix="scheduler-crash-test-") as directory:
        signal_file = Path(directory) / "claimed.txt"
        environment["CRASH_SIGNAL_FILE"] = str(signal_file)
        environment["CRASH_TEST_JOB_ID"] = str(job.id)
        process = subprocess.Popen(
            [sys.executable, "-c", child_script],
            cwd=project_root,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            deadline = time.monotonic() + 15
            while not signal_file.exists() and process.poll() is None:
                if time.monotonic() >= deadline:
                    raise TimeoutError("child consumer did not reach crash barrier")
                time.sleep(0.01)
            assert signal_file.exists()
            process.terminate()
            process.wait(timeout=5)
            assert process.returncode is not None

            repository = JobRepository(sessions)
            with engine.begin() as connection:
                connection.execute(
                    update(Job)
                    .where(Job.id == job.id)
                    .values(
                        lease_expires_at=func.clock_timestamp()
                        - func.make_interval(0, 0, 0, 0, 0, 0, 1)
                    )
                )
            restart_environment = environment.copy()
            restart_environment.update(
                {
                    "CLAIM_BATCH_SIZE": "1",
                    "CONSUMER_ID": "crash-recovery-replacement",
                    "LEASE_REAP_INTERVAL_SECONDS": "0.05",
                    "LEASE_SECONDS": "30",
                    "POLL_INTERVAL_SECONDS": "0.01",
                    "RETRY_BASE_SECONDS": "0.01",
                    "RETRY_JITTER_RATIO": "0",
                    "WORKER_CONCURRENCY": "1",
                }
            )
            restart_result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "app.consumers",
                    "--max-completed",
                    "1",
                ],
                cwd=project_root,
                env=restart_environment,
                stdin=subprocess.DEVNULL,
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
            restart_output = restart_result.stdout + restart_result.stderr
            assert "claimed=1 completed=1" in restart_output
            assert "recovered=1" in restart_output
            with sessions() as session:
                operations = list(
                    session.scalars(
                        select(SampleOperation).where(
                            SampleOperation.job_id == job.id
                        )
                    )
                )
            assert len(operations) == 1
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)
            with engine.begin() as connection:
                connection.execute(
                    delete(SampleOperation).where(SampleOperation.job_id == job.id)
                )
                connection.execute(delete(Job).where(Job.id == job.id))
                connection.execute(
                    delete(Organization).where(Organization.id == organization.id)
                )
            engine.dispose()


@pytest.mark.integration
def test_two_consumer_processes_execute_distinct_jobs(
    migrated_test_database: URL,
) -> None:
    project_root = Path(__file__).resolve().parents[2]
    engine = sqlalchemy_create_engine(migrated_test_database, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    organization = OrganizationService(
        OrganizationRepository(sessions)
    ).create(f"process-consumer-test-{uuid4()}", max_concurrent_jobs=2)
    jobs = [
        _create_due_job(organization.id, sessions, job_type="sample.slow")
        for _ in range(2)
    ]
    environment = os.environ.copy()
    environment.update(
        {
            "DATABASE_URL": migrated_test_database.render_as_string(
                hide_password=False
            ),
            "WORKER_CONCURRENCY": "1",
            "CLAIM_BATCH_SIZE": "1",
            "POLL_INTERVAL_SECONDS": "0.01",
            "LEASE_SECONDS": "30",
            "CONSUMER_ID": "",
        }
    )
    processes = []
    try:
        for _ in range(2):
            processes.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "app.consumers",
                        "--max-completed",
                        "1",
                    ],
                    cwd=project_root,
                    env=environment,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
            )

        outputs = []
        consumer_ids = set()
        for process in processes:
            output, _ = process.communicate(timeout=15)
            outputs.append(output)
            assert process.returncode == 0, output
            stopped_line = next(
                line
                for line in output.splitlines()
                if "Consumer " in line and " stopped " in line
            )
            consumer_id = stopped_line.split("Consumer ", 1)[1].split(" stopped ", 1)[0]
            consumer_ids.add(consumer_id)
            assert "completed=1" in stopped_line

        assert all("Job execution finished" in output for output in outputs)
        assert len(consumer_ids) == 2
        persisted = [JobRepository(sessions).get_by_id(job.id) for job in jobs]
        assert all(
            job is not None and job.status == JobStatus.COMPLETED
            for job in persisted
        )
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)
        with engine.begin() as connection:
            connection.execute(delete(Job).where(Job.id.in_([job.id for job in jobs])))
            connection.execute(
                delete(Organization).where(Organization.id == organization.id)
            )
        engine.dispose()


@pytest.mark.integration
def test_consumer_worker_capacity_bounds_claimed_batch(
    migrated_test_database: URL,
) -> None:
    engine = sqlalchemy_create_engine(migrated_test_database, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    organization = OrganizationService(
        OrganizationRepository(sessions)
    ).create(f"worker-capacity-test-{uuid4()}", max_concurrent_jobs=3)
    jobs = [
        _create_due_job(organization.id, sessions, job_type="sample.slow")
        for _ in range(3)
    ]
    environment = os.environ.copy()
    environment.update(
        {
            "DATABASE_URL": migrated_test_database.render_as_string(
                hide_password=False
            ),
            "WORKER_CONCURRENCY": "2",
            "CLAIM_BATCH_SIZE": "10",
            "POLL_INTERVAL_SECONDS": "0.01",
            "LEASE_SECONDS": "30",
            "CONSUMER_ID": f"capacity-test-{uuid4()}",
        }
    )
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "app.consumers",
                "--max-completed",
                "3",
            ],
            cwd=Path(__file__).resolve().parents[2],
            env=environment,
            stdin=subprocess.DEVNULL,
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )

        assert "claimed=3 completed=3" in result.stdout + result.stderr, (
            result.stdout + result.stderr
        )
        persisted = [JobRepository(sessions).get_by_id(job.id) for job in jobs]
        assert all(
            job is not None and job.status == JobStatus.COMPLETED
            for job in persisted
        )
    finally:
        with engine.begin() as connection:
            connection.execute(delete(Job).where(Job.id.in_([job.id for job in jobs])))
            connection.execute(
                delete(Organization).where(Organization.id == organization.id)
            )
        engine.dispose()


@pytest.mark.integration
def test_consumer_resumes_after_transient_database_connectivity_failure(
    migrated_test_database: URL,
) -> None:
    engine = sqlalchemy_create_engine(migrated_test_database, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    organization = OrganizationService(
        OrganizationRepository(sessions)
    ).create(f"transient-database-failure-{uuid4()}", max_concurrent_jobs=1)
    job = _create_due_job(organization.id, sessions, job_type="test.outage")
    failure_injected = False

    def interrupt_one_statement(
        connection,
        cursor,
        statement,
        parameters,
        context,
        executemany,
    ) -> None:
        nonlocal failure_injected
        if not failure_injected:
            failure_injected = True
            raise OperationalError(
                statement,
                parameters,
                ConnectionError("simulated temporary database outage"),
            )

    settings = Settings(
        database_url=migrated_test_database,
        db_pool_size=5,
        db_max_overflow=10,
        consumer_id=f"transient-failure-consumer-{uuid4()}",
        worker_concurrency=1,
        poll_interval_seconds=0.01,
        claim_batch_size=1,
        lease_seconds=30,
        shutdown_grace_seconds=2,
        reconnect_base_seconds=0.01,
        reconnect_max_seconds=0.02,
        retry_base_seconds=0.01,
        retry_max_seconds=1,
        retry_jitter_ratio=0,
        lease_reap_interval_seconds=1,
    )
    handlers = HandlerRegistry()
    handlers.register("test.outage", lambda *args: "recovered")
    repository = JobRepository(sessions)
    event.listen(engine, "before_cursor_execute", interrupt_one_statement)
    try:
        with WorkerPool(repository, handlers, max_workers=1) as worker_pool:
            result = Consumer(settings, repository, worker_pool).run(
                Event(),
                max_completed=1,
            )

        persisted = repository.get_by_id(job.id)
        assert failure_injected
        assert result.claimed == 1
        assert result.completed == 1
        assert persisted is not None
        assert persisted.status == JobStatus.COMPLETED
        assert persisted.attempts == 1
    finally:
        event.remove(engine, "before_cursor_execute", interrupt_one_statement)
        with engine.begin() as connection:
            connection.execute(delete(Job).where(Job.id == job.id))
            connection.execute(
                delete(Organization).where(Organization.id == organization.id)
            )
        engine.dispose()
