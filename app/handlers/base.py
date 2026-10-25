from dataclasses import dataclass
from typing import Callable, Protocol
from uuid import UUID


@dataclass(frozen=True)
class OrganizationContext:
    organization_id: UUID


@dataclass(frozen=True)
class IdempotencyContext:
    key: str
    attempt: int
    _record_once: Callable[[str, object], object] | None = None

    def record_once(self, result: object) -> object:
        if self._record_once is None:
            raise RuntimeError("No durable idempotency store is configured")
        return self._record_once(self.key, result)


class Handler(Protocol):
    def __call__(
        self,
        job_id: UUID,
        payload: object,
        organization: OrganizationContext,
        idempotency: IdempotencyContext,
    ) -> object:
        """Perform one attempt for a currently owned job."""
