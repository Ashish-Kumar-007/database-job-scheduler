from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import logging
import math
from enum import StrEnum
from threading import BoundedSemaphore, Event, Lock, Thread
from types import TracebackType
from uuid import UUID

from app.models import JobStatus
from app.handlers import (
    HandlerRegistry,
    IdempotencyContext,
    OrganizationContext,
    UnknownJobType,
)
from app.repositories.jobs import JobClaim, JobRepository
from app.services.errors import InputValidationError, PermanentJobError
from app.services.retry import RetryPolicy

logger = logging.getLogger(__name__)


class WorkerCapacityError(RuntimeError):
    """Raised when all configured worker slots are occupied."""


class ExecutionOutcome(StrEnum):
    COMPLETED = "completed"
    RETRY_SCHEDULED = "retry_scheduled"
    FAILED = "failed"
    STALE_CLAIM = "stale_claim"


@dataclass(frozen=True)
class ExecutionResult:
    job_id: UUID
    outcome: ExecutionOutcome
    result: object | None = None
    error: str | None = None


class WorkerPool:
    def __init__(
        self,
        repository: JobRepository,
        handlers: HandlerRegistry,
        *,
        max_workers: int,
        lease_seconds: float = 30.0,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        if (
            isinstance(max_workers, bool)
            or not isinstance(max_workers, int)
            or max_workers < 1
        ):
            raise InputValidationError("max_workers must be a positive integer")
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(lease_seconds)
            or lease_seconds <= 0
        ):
            raise InputValidationError("lease_seconds must be positive")
        self._repository = repository
        self._handlers = handlers
        self._lease_seconds = float(lease_seconds)
        self._retry_policy = retry_policy or RetryPolicy(
            base_seconds=1.0,
            max_seconds=60.0,
            jitter_ratio=0.25,
        )
        self._capacity = BoundedSemaphore(max_workers)
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="job-worker",
        )
        self._closed = False
        self._state_lock = Lock()

    def dispatch(self, claim: JobClaim) -> Future[ExecutionResult]:
        if not isinstance(claim, JobClaim):
            raise InputValidationError("only a JobClaim can be dispatched")
        with self._state_lock:
            if self._closed:
                raise RuntimeError("worker pool is shut down")
            if not self._capacity.acquire(blocking=False):
                raise WorkerCapacityError("all worker slots are occupied")
            try:
                future = self._executor.submit(self._execute, claim)
            except BaseException:
                self._capacity.release()
                raise
        future.add_done_callback(lambda _: self._capacity.release())
        return future

    def shutdown(self, *, wait: bool = True, cancel_futures: bool = False) -> None:
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
        self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)

    def __enter__(self) -> "WorkerPool":
        with self._state_lock:
            if self._closed:
                raise RuntimeError("worker pool is shut down")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.shutdown()

    def _execute(self, claim: JobClaim) -> ExecutionResult:
        started_at = self._repository.start_claim(claim)
        if not started_at:
            logger.warning(
                "Stale job claim rejected before handler start",
                extra={
                    "job_id": str(claim.job_id),
                    "organization_id": str(claim.organization_id),
                    "consumer_id": claim.owner,
                    "attempt": claim.attempts,
                },
            )
            return ExecutionResult(claim.job_id, ExecutionOutcome.STALE_CLAIM)

        lease_renewer = _LeaseRenewer(
            self._repository,
            claim,
            lease_seconds=self._lease_seconds,
        )
        lease_renewer.start()
        idempotency = IdempotencyContext(
            key=str(claim.job_id),
            attempt=claim.attempts,
            _record_once=lambda key, value: self._repository.record_sample_operation(
                claim.job_id,
                key,
                value,
            ),
        )
        organization = OrganizationContext(
            organization_id=claim.organization_id,
        )
        scheduling_delay_seconds = (
            max(0.0, (started_at - claim.scheduled_at).total_seconds())
            if claim.retry_eligible_at is None
            else None
        )
        retry_wait_seconds = (
            max(0.0, (started_at - claim.retry_eligible_at).total_seconds())
            if claim.retry_eligible_at is not None
            else None
        )
        logger.info(
            "Job handler started scheduling_delay_seconds=%s retry_wait_seconds=%s",
            scheduling_delay_seconds,
            retry_wait_seconds,
            extra={
                "job_id": str(claim.job_id),
                "organization_id": str(claim.organization_id),
                "consumer_id": claim.owner,
                "attempt": claim.attempts,
                "scheduled_at": claim.scheduled_at.isoformat(),
                "started_at": started_at.isoformat(),
                "scheduling_delay_seconds": scheduling_delay_seconds,
                "retry_eligible_at": (
                    claim.retry_eligible_at.isoformat()
                    if claim.retry_eligible_at is not None
                    else None
                ),
                "retry_wait_seconds": retry_wait_seconds,
            },
        )
        try:
            try:
                handler = self._handlers.get(claim.job_type)
                result = handler(
                    claim.job_id,
                    claim.payload,
                    organization,
                    idempotency,
                )
            except Exception as error:
                message = f"{type(error).__name__}: {error}"
                retryable = not isinstance(
                    error,
                    (PermanentJobError, UnknownJobType),
                )
                persisted_status = self._repository.fail_claim(
                    claim,
                    message,
                    retryable=retryable,
                    retry_delay_seconds=(
                        self._retry_policy.delay_seconds(claim.attempts)
                        if retryable
                        else 0
                    ),
                )
                if persisted_status is None:
                    logger.warning(
                        "Stale job claim rejected during failure update",
                        extra={
                            "job_id": str(claim.job_id),
                            "organization_id": str(claim.organization_id),
                            "consumer_id": claim.owner,
                            "attempt": claim.attempts,
                        },
                    )
                    return ExecutionResult(
                        claim.job_id,
                        ExecutionOutcome.STALE_CLAIM,
                    )
                outcome = (
                    ExecutionOutcome.RETRY_SCHEDULED
                    if persisted_status == JobStatus.RETRY_WAIT
                    else ExecutionOutcome.FAILED
                )
                return ExecutionResult(claim.job_id, outcome, error=message)

            completed = self._repository.complete_claim(claim)
            if not completed:
                logger.warning(
                    "Stale job claim rejected during completion update",
                    extra={
                        "job_id": str(claim.job_id),
                        "organization_id": str(claim.organization_id),
                        "consumer_id": claim.owner,
                        "attempt": claim.attempts,
                    },
                )
                return ExecutionResult(claim.job_id, ExecutionOutcome.STALE_CLAIM)
            return ExecutionResult(
                claim.job_id,
                ExecutionOutcome.COMPLETED,
                result=result,
            )
        finally:
            lease_renewer.stop()


class _LeaseRenewer:
    def __init__(
        self,
        repository: JobRepository,
        claim: JobClaim,
        *,
        lease_seconds: float,
    ) -> None:
        self._repository = repository
        self._claim = claim
        self._interval_seconds = lease_seconds / 3
        self._stop_event = Event()
        self._thread = Thread(
            target=self._renew,
            name=f"lease-renewal-{claim.job_id}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join()

    def _renew(self) -> None:
        while not self._stop_event.wait(self._interval_seconds):
            try:
                renewed = self._repository.renew_lease(
                    self._claim,
                    lease_seconds=self._interval_seconds * 3,
                )
            except Exception:
                logger.exception(
                    "Job lease renewal failed",
                    extra={
                        "job_id": str(self._claim.job_id),
                        "organization_id": str(self._claim.organization_id),
                        "consumer_id": self._claim.owner,
                        "attempt": self._claim.attempts,
                    },
                )
                continue
            if not renewed:
                logger.warning(
                    "Job lease ownership lost",
                    extra={
                        "job_id": str(self._claim.job_id),
                        "organization_id": str(self._claim.organization_id),
                        "consumer_id": self._claim.owner,
                        "attempt": self._claim.attempts,
                    },
                )
                return
            logger.debug(
                "Job lease renewed",
                extra={
                    "job_id": str(self._claim.job_id),
                    "organization_id": str(self._claim.organization_id),
                    "consumer_id": self._claim.owner,
                    "attempt": self._claim.attempts,
                },
            )
