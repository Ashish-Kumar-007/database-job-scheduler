import math
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, sessionmaker

from app.models import Job, JobStatus, Organization
from app.services.errors import InputValidationError, OrganizationNotFound
from app.services.inputs import JobSubmission


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
            jobs = list(
                session.scalars(
                    select(Job)
                    .where(eligible)
                    .order_by(Job.scheduled_at, Job.id)
                    .limit(limit)
                    .with_for_update(skip_locked=True, of=Job)
                )
            )

            for job in jobs:
                token = uuid4()
                lease_expires_at = database_now + timedelta(seconds=lease_seconds)
                job.status = JobStatus.RUNNING
                job.locked_by = owner
                job.claim_token = token
                job.lease_expires_at = lease_expires_at
                job.next_attempt_at = None
                job.attempts += 1
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
                    )
                )
            session.flush()

        return claims

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
