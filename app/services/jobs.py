from datetime import datetime
from uuid import UUID

from app.models import Job, JobStatus
from app.repositories.jobs import JobRepository
from app.services.inputs import JobSubmission


class JobService:
    def __init__(self, repository: JobRepository) -> None:
        self._repository = repository

    def submit(
        self,
        organization_id: UUID,
        job_type: str,
        payload: object,
        scheduled_at: datetime,
        max_attempts: int = 3,
    ) -> Job:
        return self._repository.create(
            JobSubmission(
                organization_id=organization_id,
                job_type=job_type,
                payload=payload,
                scheduled_at=scheduled_at,
                max_attempts=max_attempts,
            )
        )

    def get_by_id(self, job_id: UUID) -> Job | None:
        return self._repository.get_by_id(job_id)

    def list_for_organization(
        self,
        organization_id: UUID,
        *,
        status: JobStatus | None = None,
        limit: int = JobRepository.DEFAULT_PAGE_SIZE,
        offset: int = 0,
    ) -> list[Job]:
        return self._repository.list_for_organization(
            organization_id,
            status=status,
            limit=limit,
            offset=offset,
        )
