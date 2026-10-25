from uuid import UUID

from sqlalchemy.orm import Session, sessionmaker

from app.models import Organization
from app.services.errors import InputValidationError
from app.services.inputs import OrganizationInput


class OrganizationRepository:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def create(self, organization_input: OrganizationInput) -> Organization:
        with self._session_factory.begin() as session:
            organization = Organization(
                name=organization_input.name,
                max_concurrent_jobs=organization_input.max_concurrent_jobs,
                weight=organization_input.weight,
            )
            session.add(organization)
            session.flush()
        return organization

    def get_by_id(self, organization_id: UUID) -> Organization | None:
        if not isinstance(organization_id, UUID):
            raise InputValidationError("organization_id must be a UUID")
        with self._session_factory() as session:
            return session.get(Organization, organization_id)
