from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.models import Job, JobStatus, Organization
from app.services.errors import InputValidationError, OrganizationNotFound
from app.services.inputs import JobSubmission


class JobRepository:
    DEFAULT_PAGE_SIZE = 100
    MAX_PAGE_SIZE = 500

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
