from dataclasses import dataclass
from typing import Protocol
from uuid import UUID


@dataclass(frozen=True)
class OrganizationContext:
    organization_id: UUID


@dataclass(frozen=True)
class IdempotencyContext:
    key: str
    attempt: int


class Handler(Protocol):
    def __call__(
        self,
        job_id: UUID,
        payload: object,
        organization: OrganizationContext,
        idempotency: IdempotencyContext,
    ) -> object:
        """Perform one attempt for a currently owned job."""
