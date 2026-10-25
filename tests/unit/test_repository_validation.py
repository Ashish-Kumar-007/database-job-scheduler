from uuid import uuid4

import pytest
from sqlalchemy.orm import sessionmaker

from app.repositories.jobs import JobRepository
from app.services.errors import InputValidationError


@pytest.fixture
def repository() -> JobRepository:
    return JobRepository(session_factory=sessionmaker())


@pytest.mark.parametrize("limit", [0, -1, 501, True, "10"])
def test_job_listing_rejects_invalid_limits(
    repository: JobRepository,
    limit,
) -> None:
    with pytest.raises(InputValidationError):
        repository.list_for_organization(uuid4(), limit=limit)


@pytest.mark.parametrize("offset", [-1, True, "0"])
def test_job_listing_rejects_invalid_offsets(
    repository: JobRepository,
    offset,
) -> None:
    with pytest.raises(InputValidationError):
        repository.list_for_organization(uuid4(), offset=offset)


def test_job_listing_requires_tenant_uuid_and_known_status(
    repository: JobRepository,
) -> None:
    with pytest.raises(InputValidationError):
        repository.list_for_organization("not-a-uuid")
    with pytest.raises(InputValidationError):
        repository.list_for_organization(uuid4(), status="running")
