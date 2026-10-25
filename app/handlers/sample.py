import math
import time
from uuid import UUID

from app.handlers.base import IdempotencyContext, OrganizationContext
from app.services.errors import RetryableJobError


class SuccessfulSampleHandler:
    def __call__(
        self,
        job_id: UUID,
        payload: object,
        organization: OrganizationContext,
        idempotency: IdempotencyContext,
    ) -> dict[str, object]:
        time.sleep(0.01)
        return {
            "job_id": str(job_id),
            "organization_id": str(organization.organization_id),
            "idempotency_key": idempotency.key,
            "attempt": idempotency.attempt,
            "payload": payload,
        }


class SlowSampleHandler:
    def __init__(self, duration_seconds: float) -> None:
        if (
            isinstance(duration_seconds, bool)
            or not isinstance(duration_seconds, (int, float))
            or not math.isfinite(duration_seconds)
            or duration_seconds < 0
        ):
            raise ValueError("duration_seconds must be a non-negative finite number")
        self._duration_seconds = duration_seconds

    def __call__(
        self,
        job_id: UUID,
        payload: object,
        organization: OrganizationContext,
        idempotency: IdempotencyContext,
    ) -> object:
        time.sleep(self._duration_seconds)
        return SuccessfulSampleHandler()(
            job_id,
            payload,
            organization,
            idempotency,
        )


class ControlledFailureHandler:
    def __init__(self, message: str = "controlled sample failure") -> None:
        self._message = message

    def __call__(
        self,
        job_id: UUID,
        payload: object,
        organization: OrganizationContext,
        idempotency: IdempotencyContext,
    ) -> None:
        raise RetryableJobError(self._message)


class IdempotentSampleHandler:
    def __call__(
        self,
        job_id: UUID,
        payload: object,
        organization: OrganizationContext,
        idempotency: IdempotencyContext,
    ) -> object:
        return idempotency.record_once(
            {
                "job_id": str(job_id),
                "organization_id": str(organization.organization_id),
                "payload": payload,
            }
        )
