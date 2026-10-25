from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from enum import StrEnum
from threading import BoundedSemaphore, Lock
from types import TracebackType
from uuid import UUID

from app.models import JobStatus
from app.handlers import (
    HandlerRegistry,
    IdempotencyContext,
    OrganizationContext,
)
from app.repositories.jobs import JobClaim, JobRepository
from app.services.errors import InputValidationError


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
    ) -> None:
        if (
            isinstance(max_workers, bool)
            or not isinstance(max_workers, int)
            or max_workers < 1
        ):
            raise InputValidationError("max_workers must be a positive integer")
        self._repository = repository
        self._handlers = handlers
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
        if not self._repository.start_claim(claim):
            return ExecutionResult(claim.job_id, ExecutionOutcome.STALE_CLAIM)

        idempotency = IdempotencyContext(
            key=str(claim.job_id),
            attempt=claim.attempts,
        )
        organization = OrganizationContext(
            organization_id=claim.organization_id,
        )
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
            persisted_status = self._repository.fail_claim(claim, message)
            if persisted_status is None:
                return ExecutionResult(claim.job_id, ExecutionOutcome.STALE_CLAIM)
            outcome = (
                ExecutionOutcome.RETRY_SCHEDULED
                if persisted_status == JobStatus.RETRY_WAIT
                else ExecutionOutcome.FAILED
            )
            return ExecutionResult(claim.job_id, outcome, error=message)

        if not self._repository.complete_claim(claim):
            return ExecutionResult(claim.job_id, ExecutionOutcome.STALE_CLAIM)
        return ExecutionResult(
            claim.job_id,
            ExecutionOutcome.COMPLETED,
            result=result,
        )
