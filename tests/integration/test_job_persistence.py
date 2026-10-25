import os
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import (
    create_engine as sqlalchemy_create_engine,
    delete,
    func,
    select,
    update,
)
from sqlalchemy.engine import URL, make_url
from sqlalchemy.orm import sessionmaker

from app.handlers import HandlerRegistry
from app.handlers.sample import IdempotentSampleHandler
from app.models import Job, JobStatus, Organization, SampleOperation
from app.repositories.jobs import JobRepository
from app.repositories.organizations import OrganizationRepository
from app.services.errors import OrganizationNotFound
from app.services.jobs import JobService
from app.services.organizations import OrganizationService
from app.workers import ExecutionOutcome, WorkerPool


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
        assert completed_job.completed_at is not None
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
            assert repository.recover_expired_claims(retry_delay_seconds=0)

            retry_claim = next(
                claim
                for claim in repository.claim_due_jobs("replacement-consumer")
                if claim.job_id == job.id
            )
            handlers = HandlerRegistry()
            handlers.register("sample.idempotent", IdempotentSampleHandler())
            with WorkerPool(
                repository,
                handlers,
                max_workers=1,
                lease_seconds=30,
            ) as worker_pool:
                result = worker_pool.dispatch(retry_claim).result(timeout=10)
            assert result.outcome == ExecutionOutcome.COMPLETED
            assert result.result == {
                "job_id": str(job.id),
                "organization_id": str(organization.id),
                "payload": {"operation": "durable"},
            }
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
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )

        assert "claimed=3 completed=3" in result.stdout + result.stderr
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
