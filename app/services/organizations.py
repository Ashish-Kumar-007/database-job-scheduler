from uuid import UUID

from app.models import Organization
from app.repositories.organizations import OrganizationRepository
from app.services.inputs import OrganizationInput


class OrganizationService:
    def __init__(self, repository: OrganizationRepository) -> None:
        self._repository = repository

    def create(
        self,
        name: str,
        max_concurrent_jobs: int,
        weight: int = 1,
    ) -> Organization:
        return self._repository.create(
            OrganizationInput(
                name=name,
                max_concurrent_jobs=max_concurrent_jobs,
                weight=weight,
            )
        )

    def get_by_id(self, organization_id: UUID) -> Organization | None:
        return self._repository.get_by_id(organization_id)
