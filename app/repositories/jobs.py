import math
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from app.models import Job, JobStatus, Organization, SampleOperation
from app.services.errors import InputValidationError, OrganizationNotFound
from app.services.inputs import JobSubmission

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobClaim:
    job_id: UUID
    organization_id: UUID
    job_type: str
    payload: object
    scheduled_at: datetime
    attempts: int
    max_attempts: int
    owner: str
    claim_token: UUID
    lease_expires_at: datetime
    retry_eligible_at: datetime | None = None


@dataclass(frozen=True)
class RecoveredClaim:
    job_id: UUID
    organization_id: UUID
    attempts: int
    status: JobStatus


class JobRepository:
    DEFAULT_PAGE_SIZE = 100
    MAX_PAGE_SIZE = 500
    MAX_CLAIM_BATCH_SIZE = 100

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def create(self, submission: JobSubmission) -> Job:
        with self._session_factory.begin() as session:
            if session.get(Organization, submission.organization_id) is None:
                raise OrganizationNotFound(
                    f"Organization {submission.organization_id} does not exist"
                )
            job = Job(
                organization_id=submission.organization_id,
                job_type=submission.job_type,
                payload=submission.payload,
                scheduled_at=submission.scheduled_at,
                max_attempts=submission.max_attempts,
            )
            session.add(job)
            session.flush()
            logger.info(
                "Job created job_id=%s organization_id=%s",
                job.id,
                job.organization_id,
                extra={
                    "job_id": str(job.id),
                    "organization_id": str(job.organization_id),
                    "consumer_id": None,
                    "attempt": 0,
                    "scheduled_at": job.scheduled_at.isoformat(),
                    "event_type": "job_created",
                },
            )
        return job

    def claim_due_jobs(
        self,
        owner: str,
        *,
        limit: int = 10,
        lease_seconds: float = 30.0,
    ) -> list[JobClaim]:
        self._validate_claim_request(owner, limit, lease_seconds)
        owner = owner.strip()
        claims: list[JobClaim] = []
        with self._session_factory.begin() as session:
            database_now = session.scalar(select(func.clock_timestamp()))
            if database_now is None:
                raise RuntimeError("Database did not return its current timestamp")

            eligible = or_(
                (
                    (Job.status == JobStatus.SCHEDULED)
                    & (Job.scheduled_at <= database_now)
                ),
                (
                    (Job.status == JobStatus.RETRY_WAIT)
                    & (Job.next_attempt_at <= database_now)
                ),
            )
            has_due_jobs = (
                select(Job.id)
                .where(Job.organization_id == Organization.id, eligible)
                .exists()
            )
            active_claim_count = (
                select(func.count(Job.id))
                .where(
                    Job.organization_id == Organization.id,
                    Job.status == JobStatus.RUNNING,
                    Job.lease_expires_at > func.clock_timestamp(),
                )
                .correlate(Organization)
                .scalar_subquery()
            )
            unstarted_claim_count = (
                select(func.count(Job.id))
                .where(
                    Job.organization_id == Organization.id,
                    Job.status == JobStatus.RUNNING,
                    Job.started_at.is_(None),
                    Job.lease_expires_at > func.clock_timestamp(),
                )
                .correlate(Organization)
                .scalar_subquery()
            )
            skipped_organizations: set[UUID] = set()

            while len(claims) < limit:
                organization_query = (
                    select(Organization)
                    .where(
                        has_due_jobs,
                        active_claim_count < Organization.max_concurrent_jobs,
                        unstarted_claim_count == 0,
                    )
                    .order_by(
                        Organization.last_claimed_at.asc().nulls_first(),
                        Organization.id,
                    )
                    .limit(1)
                    .with_for_update(skip_locked=True, of=Organization)
                )
                if skipped_organizations:
                    organization_query = organization_query.where(
                        Organization.id.not_in(skipped_organizations)
                    )
                organization = session.scalar(organization_query)
                if organization is None:
                    break

                job = session.scalar(
                    select(Job)
                    .where(
                        Job.organization_id == organization.id,
                        eligible,
                    )
                    .order_by(Job.scheduled_at, Job.id)
                    .limit(1)
                    .with_for_update(skip_locked=True, of=Job)
                )
                if job is None:
                    skipped_organizations.add(organization.id)
                    continue

                token = uuid4()
                lease_expires_at = database_now + timedelta(seconds=lease_seconds)
                retry_eligible_at = (
                    job.next_attempt_at
                    if job.status == JobStatus.RETRY_WAIT
                    else None
                )
                job.status = JobStatus.RUNNING
                job.locked_by = owner
                job.claim_token = token
                job.lease_expires_at = lease_expires_at
                job.next_attempt_at = None
                job.started_at = None
                job.attempts += 1
                organization.last_claimed_at = func.clock_timestamp()
                claims.append(
                    JobClaim(
                        job_id=job.id,
                        organization_id=job.organization_id,
                        job_type=job.job_type,
                        payload=deepcopy(job.payload),
                        scheduled_at=job.scheduled_at,
                        attempts=job.attempts,
                        max_attempts=job.max_attempts,
                        owner=owner,
                        claim_token=token,
                        lease_expires_at=lease_expires_at,
                        retry_eligible_at=retry_eligible_at,
                    )
                )
            session.flush()

        return claims

    def start_claim(self, claim: JobClaim) -> datetime | None:
        with self._session_factory.begin() as session:
            result = session.execute(
                update(Job)
                .where(
                    Job.id == claim.job_id,
                    Job.status == JobStatus.RUNNING,
                    Job.locked_by == claim.owner,
                    Job.claim_token == claim.claim_token,
                    Job.lease_expires_at > func.clock_timestamp(),
                )
                .values(
                    started_at=func.clock_timestamp(),
                    updated_at=func.clock_timestamp(),
                )
                .returning(Job.started_at)
            )
            return result.scalar_one_or_none()

    def complete_claim(self, claim: JobClaim) -> bool:
        with self._session_factory.begin() as session:
            result = session.execute(
                update(Job)
                .where(
                    Job.id == claim.job_id,
                    Job.status == JobStatus.RUNNING,
                    Job.locked_by == claim.owner,
                    Job.claim_token == claim.claim_token,
                    Job.lease_expires_at > func.clock_timestamp(),
                )
                .values(
                    status=JobStatus.COMPLETED,
                    locked_by=None,
                    claim_token=None,
                    lease_expires_at=None,
                    next_attempt_at=None,
                    completed_at=func.clock_timestamp(),
                    updated_at=func.clock_timestamp(),
                )
            )
        return result.rowcount == 1

    def renew_lease(self, claim: JobClaim, *, lease_seconds: float) -> bool:
        self._validate_lease_seconds(lease_seconds)
        with self._session_factory.begin() as session:
            result = session.execute(
                update(Job)
                .where(
                    Job.id == claim.job_id,
                    Job.status == JobStatus.RUNNING,
                    Job.locked_by == claim.owner,
                    Job.claim_token == claim.claim_token,
                    Job.lease_expires_at > func.clock_timestamp(),
                )
                .values(
                    lease_expires_at=func.clock_timestamp()
                    + func.make_interval(0, 0, 0, 0, 0, 0, lease_seconds),
                    updated_at=func.clock_timestamp(),
                )
            )
        return result.rowcount == 1

    def fail_claim(
        self,
        claim: JobClaim,
        error_message: str,
        *,
        retryable: bool = True,
        retry_delay_seconds: float = 0,
    ) -> JobStatus | None:
        if not isinstance(error_message, str) or not error_message.strip():
            raise InputValidationError("error_message must be a non-empty string")
        self._validate_retry_delay(retry_delay_seconds)
        with self._session_factory.begin() as session:
            job = session.scalar(
                select(Job)
                .where(
                    Job.id == claim.job_id,
                    Job.status == JobStatus.RUNNING,
                    Job.locked_by == claim.owner,
                    Job.claim_token == claim.claim_token,
                    Job.lease_expires_at > func.clock_timestamp(),
                )
                .with_for_update()
            )
            if job is None:
                return None
            should_retry = retryable and job.attempts < job.max_attempts
            job.status = JobStatus.RETRY_WAIT if should_retry else JobStatus.FAILED
            job.locked_by = None
            job.claim_token = None
            job.lease_expires_at = None
            job.next_attempt_at = (
                func.clock_timestamp()
                + func.make_interval(0, 0, 0, 0, 0, 0, retry_delay_seconds)
                if should_retry
                else None
            )
            job.last_error = error_message[:4000]
            job.updated_at = func.clock_timestamp()
            session.flush()
            status = job.status
        return status

    def recover_expired_claims(
        self,
        *,
        limit: int = 100,
        retry_delay_seconds: float = 0,
    ) -> list[RecoveredClaim]:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= self.MAX_CLAIM_BATCH_SIZE
        ):
            raise InputValidationError(
                f"limit must be an integer between 1 and {self.MAX_CLAIM_BATCH_SIZE}"
            )
        self._validate_retry_delay(retry_delay_seconds)
        recovered: list[RecoveredClaim] = []
        with self._session_factory.begin() as session:
            expired_jobs = list(
                session.scalars(
                    select(Job)
                    .where(
                        Job.status == JobStatus.RUNNING,
                        Job.lease_expires_at <= func.clock_timestamp(),
                    )
                    .order_by(Job.lease_expires_at, Job.id)
                    .limit(limit)
                    .with_for_update(skip_locked=True, of=Job)
                )
            )
            for job in expired_jobs:
                can_retry = job.attempts < job.max_attempts
                job.status = (
                    JobStatus.RETRY_WAIT if can_retry else JobStatus.FAILED
                )
                job.locked_by = None
                job.claim_token = None
                job.lease_expires_at = None
                job.next_attempt_at = (
                    func.clock_timestamp()
                    + func.make_interval(0, 0, 0, 0, 0, 0, retry_delay_seconds)
                    if can_retry
                    else None
                )
                job.last_error = (
                    f"Lease expired for attempt {job.attempts}; "
                    + (
                        "retry scheduled"
                        if can_retry
                        else "attempt limit exhausted"
                    )
                )
                job.updated_at = func.clock_timestamp()
                recovered.append(
                    RecoveredClaim(
                        job_id=job.id,
                        organization_id=job.organization_id,
                        attempts=job.attempts,
                        status=job.status,
                    )
                )
            session.flush()
        return recovered

    def record_sample_operation(
        self,
        job_id: UUID,
        idempotency_key: str,
        result: object,
    ) -> object:
        if not isinstance(job_id, UUID):
            raise InputValidationError("job_id must be a UUID")
        if (
            not isinstance(idempotency_key, str)
            or not idempotency_key.strip()
            or len(idempotency_key) > 255
        ):
            raise InputValidationError(
                "idempotency_key must contain 1 to 255 characters"
            )
        with self._session_factory.begin() as session:
            inserted = session.scalar(
                insert(SampleOperation)
                .values(
                    job_id=job_id,
                    idempotency_key=idempotency_key,
                    result=result,
                )
                .on_conflict_do_nothing()
                .returning(SampleOperation.result)
            )
            if inserted is not None:
                return inserted
            existing = session.scalar(
                select(SampleOperation).where(SampleOperation.job_id == job_id)
            )
            if existing is None or existing.idempotency_key != idempotency_key:
                raise RuntimeError(
                    "Idempotency key is already associated with another operation"
                )
            return existing.result

    def get_by_id(self, job_id: UUID) -> Job | None:
        if not isinstance(job_id, UUID):
            raise InputValidationError("job_id must be a UUID")
        with self._session_factory() as session:
            return session.get(Job, job_id)

    def list_for_organization(
        self,
        organization_id: UUID,
        *,
        status: JobStatus | None = None,
        limit: int = DEFAULT_PAGE_SIZE,
        offset: int = 0,
    ) -> list[Job]:
        self._validate_pagination(limit, offset)
        if not isinstance(organization_id, UUID):
            raise InputValidationError("organization_id must be a UUID")
        if status is not None and not isinstance(status, JobStatus):
            raise InputValidationError("status must be a JobStatus")

        statement = select(Job).where(Job.organization_id == organization_id)
        if status is not None:
            statement = statement.where(Job.status == status)
        statement = (
            statement.order_by(Job.created_at.desc(), Job.id.desc())
            .limit(limit)
            .offset(offset)
        )
        with self._session_factory() as session:
            return list(session.scalars(statement))

    @classmethod
    def _validate_pagination(cls, limit: int, offset: int) -> None:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= cls.MAX_PAGE_SIZE
        ):
            raise InputValidationError(
                f"limit must be an integer between 1 and {cls.MAX_PAGE_SIZE}"
            )
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise InputValidationError("offset must be a non-negative integer")

    @classmethod
    def _validate_claim_request(
        cls,
        owner: str,
        limit: int,
        lease_seconds: float,
    ) -> None:
        if not isinstance(owner, str) or not owner.strip():
            raise InputValidationError("owner must be a non-empty string")
        if len(owner) > 255 or "\x00" in owner:
            raise InputValidationError("owner must be at most 255 characters")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= cls.MAX_CLAIM_BATCH_SIZE
        ):
            raise InputValidationError(
                f"limit must be an integer between 1 and {cls.MAX_CLAIM_BATCH_SIZE}"
            )
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(lease_seconds)
            or lease_seconds <= 0
        ):
            raise InputValidationError("lease_seconds must be a positive finite number")

    @staticmethod
    def _validate_lease_seconds(lease_seconds: float) -> None:
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(lease_seconds)
            or lease_seconds <= 0
        ):
            raise InputValidationError("lease_seconds must be a positive finite number")

    @staticmethod
    def _validate_retry_delay(retry_delay_seconds: float) -> None:
        if (
            isinstance(retry_delay_seconds, bool)
            or not isinstance(retry_delay_seconds, (int, float))
            or not math.isfinite(retry_delay_seconds)
            or retry_delay_seconds < 0
        ):
            raise InputValidationError(
                "retry_delay_seconds must be a non-negative finite number"
            )
