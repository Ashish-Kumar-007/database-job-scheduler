import argparse
from collections import Counter
import math
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

from sqlalchemy import delete, func, select
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session, sessionmaker

from app.config import Settings
from app.db.engine import create_engine
from app.db.session import create_session_factory
from app.models import Job, JobStatus, Organization, SampleOperation
from app.repositories.jobs import JobRepository
from app.repositories.organizations import OrganizationRepository
from app.services.jobs import JobService
from app.services.organizations import OrganizationService

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ORGANIZATION_COUNT = 3
JOB_COUNT = 60
JOB_TYPES = (
    "sample.success",
    "sample.slow",
    "sample.failure",
    "sample.idempotent",
)
CONSUMER_COUNT = 3
LEASE_SECONDS = 2.0


@dataclass(frozen=True)
class DemoSeed:
    run_id: UUID
    job_ids: tuple[UUID, ...]


def _create_organizations(
    sessions: sessionmaker[Session],
    run_id: UUID,
    *,
    include_crash_tenant: bool,
) -> list[Organization]:
    organization_service = OrganizationService(OrganizationRepository(sessions))
    count = ORGANIZATION_COUNT + int(include_crash_tenant)
    return [
        organization_service.create(
            name=f"demo-{run_id.hex}-tenant-{index + 1}",
            max_concurrent_jobs=4,
        )
        for index in range(count)
    ]


def _submit_workload(
    sessions: sessionmaker[Session],
    organizations: list[Organization],
) -> tuple[UUID, ...]:
    service = JobService(JobRepository(sessions))
    job_ids: list[UUID] = []
    for index in range(JOB_COUNT):
        is_future = index >= JOB_COUNT - 3
        scheduled_at = (
            datetime.now(UTC) + timedelta(seconds=8)
            if is_future
            else datetime.now(UTC) - timedelta(seconds=1)
        )
        job = service.submit(
            organization_id=organizations[index % ORGANIZATION_COUNT].id,
            job_type=JOB_TYPES[index % len(JOB_TYPES)],
            payload={
                "demo_index": index,
                "scheduled_kind": "future" if is_future else "due",
            },
            scheduled_at=scheduled_at,
            max_attempts=(
                2
                if JOB_TYPES[index % len(JOB_TYPES)] == "sample.failure"
                else 3
            ),
        )
        job_ids.append(job.id)
    return tuple(job_ids)


def _create_crash_job(
    sessions: sessionmaker[Session],
    organization: Organization,
) -> UUID:
    job = JobService(JobRepository(sessions)).submit(
        organization_id=organization.id,
        job_type="sample.idempotent",
        payload={"scenario": "consumer-crash-recovery"},
        scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
        max_attempts=3,
    )
    return job.id


def _simulate_crash(
    database_url: URL,
    job_id: UUID,
    *,
    lease_seconds: float,
) -> None:
    child_environment = os.environ.copy()
    child_environment.update(
        {
            "DATABASE_URL": database_url.render_as_string(hide_password=False),
            "CONSUMER_ID": f"demo-crash-{uuid4().hex[:8]}",
            "LEASE_SECONDS": str(lease_seconds),
        }
    )
    with tempfile.TemporaryDirectory(prefix="scheduler-demo-crash-") as directory:
        ready_file = Path(directory) / "side-effect-committed"
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "scripts.crash_worker",
                "--job-id",
                str(job_id),
                "--ready-file",
                str(ready_file),
            ],
            cwd=PROJECT_ROOT,
            env=child_environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            deadline = time.monotonic() + 20
            while not ready_file.exists() and process.poll() is None:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        "Crash worker did not reach its side-effect barrier"
                    )
                time.sleep(0.02)
            if not ready_file.exists():
                _, error = process.communicate(timeout=5)
                raise RuntimeError(f"Crash worker exited before claim: {error}")
            process.terminate()
            process.wait(timeout=5)
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)

    print(f"Crash simulated after durable side effect for job {job_id}")
    time.sleep(lease_seconds + 0.25)


def _start_consumers(
    database_url: URL,
    log_files: list[object],
) -> list[subprocess.Popen]:
    processes: list[subprocess.Popen] = []
    for index in range(CONSUMER_COUNT):
        environment = os.environ.copy()
        environment.update(
            {
                "DATABASE_URL": database_url.render_as_string(hide_password=False),
                "CONSUMER_ID": f"demo-consumer-{index + 1}-{uuid4().hex[:6]}",
                "WORKER_CONCURRENCY": "4",
                "CLAIM_BATCH_SIZE": "8",
                "POLL_INTERVAL_SECONDS": "0.05",
                "LEASE_SECONDS": str(LEASE_SECONDS),
                "LEASE_REAP_INTERVAL_SECONDS": "0.05",
                "RETRY_BASE_SECONDS": "0.1",
                "RETRY_MAX_SECONDS": "0.2",
                "RETRY_JITTER_RATIO": "0",
            }
        )
        processes.append(
            subprocess.Popen(
                [sys.executable, "-m", "app.consumers"],
                cwd=PROJECT_ROOT,
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=log_files[index],
                stderr=subprocess.STDOUT,
            )
        )
    return processes


def _wait_for_terminal_jobs(
    sessions: sessionmaker[Session],
    job_ids: tuple[UUID, ...],
    processes: list[subprocess.Popen],
    *,
    timeout_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    terminal_statuses = (JobStatus.COMPLETED, JobStatus.FAILED)
    while time.monotonic() < deadline:
        with sessions() as session:
            remaining = session.scalar(
                select(func.count(Job.id)).where(
                    Job.id.in_(job_ids),
                    Job.status.not_in(terminal_statuses),
                )
            )
        if remaining == 0:
            return
        exited = [process for process in processes if process.poll() is not None]
        if exited:
            raise RuntimeError(
                "A consumer exited before the demo workload reached terminal states"
            )
        time.sleep(0.2)
    raise TimeoutError("Demo jobs did not all reach terminal states within 120 seconds")


def _stop_consumers(processes: list[subprocess.Popen]) -> None:
    for process in processes:
        if process.poll() is None:
            process.terminate()
    for process in processes:
        if process.poll() is None:
            process.wait(timeout=10)


def _load_run_jobs(
    sessions: sessionmaker[Session],
    run_id: UUID,
) -> tuple[list[Organization], list[Job]]:
    prefix = f"demo-{run_id.hex}-"
    with sessions() as session:
        organizations = list(
            session.scalars(
                select(Organization)
                .where(Organization.name.startswith(prefix))
                .order_by(Organization.name)
            )
        )
        if not organizations:
            raise ValueError(f"No demo run found for run ID {run_id}")
        jobs = list(
            session.scalars(
                select(Job)
                .where(
                    Job.organization_id.in_(
                        [organization.id for organization in organizations]
                    )
                )
                .order_by(Job.created_at, Job.id)
            )
        )
    return organizations, jobs


def _report_run(
    sessions: sessionmaker[Session],
    run_id: UUID,
    *,
    elapsed_seconds: float | None = None,
) -> bool:
    organizations, jobs = _load_run_jobs(sessions, run_id)
    statuses = {status: 0 for status in JobStatus}
    delays: list[float] = []
    retried_jobs = 0
    extra_attempts = 0
    recovered_jobs = 0
    future_jobs = 0
    future_jobs_started_early = 0
    for job in jobs:
        statuses[job.status] += 1
        if job.attempts > 1:
            retried_jobs += 1
            extra_attempts += job.attempts - 1
        if job.last_error and job.last_error.startswith("Lease expired"):
            recovered_jobs += 1
        if (
            isinstance(job.payload, dict)
            and job.payload.get("scheduled_kind") == "future"
        ):
            future_jobs += 1
            if job.started_at is not None and job.started_at < job.scheduled_at:
                future_jobs_started_early += 1
        if job.started_at is not None:
            delays.append(max(0.0, (job.started_at - job.scheduled_at).total_seconds()))

    delays.sort()
    submitted = len(jobs)
    accounted = sum(statuses.values())
    reconciled = submitted == accounted
    print(f"Demo run: {run_id}")
    print(f"Organizations: {len(organizations)}")
    type_counts = Counter(job.job_type for job in jobs)
    organization_counts = Counter(job.organization_id for job in jobs)
    print(
        "Job types: "
        + ", ".join(
            f"{job_type}={count}"
            for job_type, count in sorted(type_counts.items())
        )
    )
    print(
        "Jobs per organization: "
        + ", ".join(
            f"{organization.name}={organization_counts[organization.id]}"
            for organization in organizations
        )
    )
    print(
        "Jobs: submitted={submitted} completed={completed} failed={failed} "
        "retried_jobs={retried} retry_attempts={extra} recovered={recovered}".format(
            submitted=submitted,
            completed=statuses[JobStatus.COMPLETED],
            failed=statuses[JobStatus.FAILED],
            retried=retried_jobs,
            extra=extra_attempts,
            recovered=recovered_jobs,
        )
    )
    print(
        (
            "Outstanding: scheduled={scheduled} running={running} "
            "retry_wait={retry_wait}"
        ).format(
            scheduled=statuses[JobStatus.SCHEDULED],
            running=statuses[JobStatus.RUNNING],
            retry_wait=statuses[JobStatus.RETRY_WAIT],
        )
    )
    print(
        "Future scheduling: jobs={} started_early={}".format(
            future_jobs,
            future_jobs_started_early,
        )
    )
    print(
        "Scheduling delay (latest start per job): average={average} max={maximum} "
        "p95={p95} seconds".format(
            average=(sum(delays) / len(delays)) if delays else "n/a",
            maximum=max(delays) if delays else "n/a",
            p95=delays[math.ceil(len(delays) * 0.95) - 1] if delays else "n/a",
        )
    )
    if elapsed_seconds is not None and elapsed_seconds > 0:
        print(
            "Throughput: {:.2f} terminal jobs/second".format(
                (statuses[JobStatus.COMPLETED] + statuses[JobStatus.FAILED])
                / elapsed_seconds
            )
        )
    print(f"Reconciliation: {accounted}/{submitted} accounted")
    return reconciled


def _require_empty_database(engine) -> None:
    with engine.connect() as connection:
        job_count = connection.scalar(select(func.count()).select_from(Job))
    if job_count:
        raise RuntimeError(
            "The automated demo requires an empty jobs table so consumers cannot "
            "execute unrelated jobs. Use a dedicated demo database."
        )


def _seed_run(sessions: sessionmaker[Session]) -> DemoSeed:
    run_id = uuid4()
    organizations = _create_organizations(
        sessions,
        run_id,
        include_crash_tenant=False,
    )
    job_ids = _submit_workload(sessions, organizations)
    print(
        f"Seeded run {run_id}: {len(job_ids)} jobs across "
        f"{len(organizations)} organizations"
    )
    return DemoSeed(run_id, job_ids)


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed and run the scheduler demo")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "run",
        help="simulate a crash, process the workload, and report",
    )
    subparsers.add_parser("seed", help="create 60 jobs across three organizations")
    report_parser = subparsers.add_parser(
        "report",
        help="report persisted state for a run",
    )
    report_parser.add_argument("--run-id", required=True, type=UUID)
    cleanup_parser = subparsers.add_parser(
        "cleanup",
        help="delete only one demo run's jobs, sample operations, and organizations",
    )
    cleanup_parser.add_argument("--run-id", required=True, type=UUID)
    cleanup_parser.add_argument("--confirm", action="store_true")
    arguments = parser.parse_args()

    settings = Settings.from_env()
    engine = create_engine(settings)
    sessions = create_session_factory(engine)
    try:
        if arguments.command == "seed":
            seed = _seed_run(sessions)
            print(f"Run consumers with: python -m app.consumers")
            print(
                "Inspect the run with: "
                f"python -m scripts.demo report --run-id {seed.run_id}"
            )
            return
        if arguments.command == "report":
            if not _report_run(sessions, arguments.run_id):
                raise RuntimeError("Demo run contains unaccounted jobs")
            return
        if arguments.command == "cleanup":
            if not arguments.confirm:
                raise ValueError("Pass --confirm to delete the selected demo run")
            organizations, jobs = _load_run_jobs(sessions, arguments.run_id)
            organization_ids = [organization.id for organization in organizations]
            job_ids = [job.id for job in jobs]
            with engine.begin() as connection:
                connection.execute(
                    delete(SampleOperation).where(SampleOperation.job_id.in_(job_ids))
                )
                connection.execute(delete(Job).where(Job.id.in_(job_ids)))
                connection.execute(
                    delete(Organization).where(Organization.id.in_(organization_ids))
                )
            print(f"Removed demo run {arguments.run_id}: {len(job_ids)} jobs")
            return

        _require_empty_database(engine)
        run_id = uuid4()
        organizations = _create_organizations(
            sessions,
            run_id,
            include_crash_tenant=True,
        )
        crash_job_id = _create_crash_job(sessions, organizations[-1])
        _simulate_crash(
            settings.database_url,
            crash_job_id,
            lease_seconds=LEASE_SECONDS,
        )
        job_ids = _submit_workload(sessions, organizations) + (crash_job_id,)
        print(
            f"Seeded run {run_id}: {len(job_ids)} jobs across "
            f"{len(organizations)} organizations"
        )
        log_handles = []
        processes = []
        with tempfile.TemporaryDirectory(
            prefix="scheduler-demo-consumers-"
        ) as directory:
            try:
                for index in range(CONSUMER_COUNT):
                    log_handles.append(
                        open(
                            Path(directory) / f"consumer-{index + 1}.log",
                            "w",
                            encoding="utf-8",
                        )
                    )
                started_at = time.monotonic()
                processes = _start_consumers(settings.database_url, log_handles)
                _wait_for_terminal_jobs(
                    sessions,
                    job_ids,
                    processes,
                    timeout_seconds=120,
                )
            finally:
                _stop_consumers(processes)
                for handle in log_handles:
                    handle.close()
            for index in range(CONSUMER_COUNT):
                log_path = Path(directory) / f"consumer-{index + 1}.log"
                lines = log_path.read_text(encoding="utf-8").splitlines()
                summaries = [line for line in lines if " stopped " in line]
                for summary in summaries:
                    print(summary)
        elapsed_seconds = time.monotonic() - started_at
        if not _report_run(sessions, run_id, elapsed_seconds=elapsed_seconds):
            raise RuntimeError("Demo run contains unaccounted jobs")
        _, demo_jobs = _load_run_jobs(sessions, run_id)
        future_jobs = [
            job
            for job in demo_jobs
            if isinstance(job.payload, dict)
            and job.payload.get("scheduled_kind") == "future"
        ]
        if (
            not future_jobs
            or any(
                job.started_at is None or job.started_at < job.scheduled_at
                for job in future_jobs
            )
            or not any(job.status == JobStatus.FAILED for job in demo_jobs)
            or not any(job.attempts > 1 for job in demo_jobs)
            or not any(
                job.last_error and job.last_error.startswith("Lease expired")
                for job in demo_jobs
            )
        ):
            raise RuntimeError("The demo did not verify every expected scenario")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
