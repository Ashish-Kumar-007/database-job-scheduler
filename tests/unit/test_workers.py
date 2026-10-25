from concurrent.futures import Future
from datetime import UTC, datetime, timedelta
from threading import Event, Lock
from time import sleep
from unittest.mock import patch
from uuid import uuid4

import pytest

from app.handlers import (
    HandlerRegistry,
    IdempotencyContext,
    OrganizationContext,
    UnknownJobType,
)
from app.handlers.sample import (
    ControlledFailureHandler,
    SlowSampleHandler,
    SuccessfulSampleHandler,
)
from app.models import JobStatus
from app.repositories.jobs import JobClaim
from app.services.errors import PermanentJobError, RetryableJobError
from app.workers import (
    ExecutionOutcome,
    ExecutionResult,
    WorkerCapacityError,
    WorkerPool,
)


class FakeJobRepository:
    def __init__(self) -> None:
        self.can_start = True
        self.can_complete = True
        self.failure_status = JobStatus.RETRY_WAIT
        self.start_error: Exception | None = None
        self.started: list[JobClaim] = []
        self.completed: list[JobClaim] = []
        self.failures: list[tuple[JobClaim, str]] = []

    def start_claim(self, claim: JobClaim) -> datetime | None:
        if self.start_error is not None:
            error = self.start_error
            self.start_error = None
            raise error
        if self.can_start:
            self.started.append(claim)
        return datetime.now(UTC) if self.can_start else None

    def complete_claim(self, claim: JobClaim) -> bool:
        self.completed.append(claim)
        return self.can_complete

    def fail_claim(
        self,
        claim: JobClaim,
        error_message: str,
        *,
        retryable: bool = True,
        retry_delay_seconds: float = 0,
    ) -> JobStatus | None:
        self.failures.append((claim, error_message))
        return self.failure_status if retryable else JobStatus.FAILED

    def renew_lease(self, claim: JobClaim, *, lease_seconds: float) -> bool:
        return True

    def record_sample_operation(
        self,
        job_id,
        idempotency_key: str,
        result: object,
    ) -> object:
        return result

    def recover_expired_claims(
        self,
        *,
        limit: int,
        retry_delay_seconds: float,
    ) -> list:
        return []


def make_claim(
    job_type: str = "sample.success",
    *,
    attempts: int = 1,
    max_attempts: int = 3,
    retry_eligible_at: datetime | None = None,
) -> JobClaim:
    now = datetime.now(UTC)
    return JobClaim(
        job_id=uuid4(),
        organization_id=uuid4(),
        job_type=job_type,
        payload={"message": "hello"},
        scheduled_at=now - timedelta(seconds=1),
        attempts=attempts,
        max_attempts=max_attempts,
        owner="consumer-test",
        claim_token=uuid4(),
        lease_expires_at=now + timedelta(seconds=60),
        retry_eligible_at=retry_eligible_at,
    )


def test_handler_registry_rejects_unknown_and_duplicate_types() -> None:
    registry = HandlerRegistry()
    handler = SuccessfulSampleHandler()
    registry.register("sample.success", handler)

    assert registry.get("sample.success") is handler
    with pytest.raises(UnknownJobType):
        registry.get("unknown")
    with pytest.raises(ValueError, match="already registered"):
        registry.register("sample.success", handler)
    with pytest.raises(ValueError):
        registry.register("  ", handler)
    with pytest.raises(TypeError):
        registry.register("not.callable", object())


@pytest.mark.parametrize("max_workers", [0, -1, True, 1.5])
def test_worker_pool_requires_positive_integer_capacity(max_workers) -> None:
    with pytest.raises(ValueError):
        WorkerPool(FakeJobRepository(), HandlerRegistry(), max_workers=max_workers)


def test_successful_sample_handler_uses_idempotency_and_tenant_context() -> None:
    handler = SuccessfulSampleHandler()
    job_id = uuid4()
    organization = OrganizationContext(uuid4())
    idempotency = IdempotencyContext(str(job_id), 2)

    result = handler(job_id, {"x": 1}, organization, idempotency)

    assert result == {
        "job_id": str(job_id),
        "organization_id": str(organization.organization_id),
        "idempotency_key": str(job_id),
        "attempt": 2,
        "payload": {"x": 1},
    }


@pytest.mark.parametrize("duration", [-1, float("inf"), float("nan"), True])
def test_slow_handler_rejects_invalid_durations(duration) -> None:
    with pytest.raises(ValueError):
        SlowSampleHandler(duration)


def test_slow_handler_returns_normal_handler_result() -> None:
    claim = make_claim()
    result = SlowSampleHandler(0)(
        claim.job_id,
        claim.payload,
        OrganizationContext(claim.organization_id),
        IdempotencyContext(str(claim.job_id), claim.attempts),
    )

    assert result["job_id"] == str(claim.job_id)
    assert result["organization_id"] == str(claim.organization_id)


def test_controlled_failure_is_explicitly_retryable() -> None:
    handler = ControlledFailureHandler()

    with pytest.raises(RetryableJobError, match="controlled sample failure"):
        handler(
            uuid4(),
            {},
            OrganizationContext(uuid4()),
            IdempotencyContext("stable-key", 1),
        )


def test_worker_dispatch_executes_only_a_valid_claim() -> None:
    repository = FakeJobRepository()
    registry = HandlerRegistry()
    registry.register("sample.success", SuccessfulSampleHandler())
    claim = make_claim()

    with WorkerPool(repository, registry, max_workers=1) as pool:
        future = pool.dispatch(claim)

    result = future.result(timeout=1)

    assert result.outcome == ExecutionOutcome.COMPLETED
    assert result.result["idempotency_key"] == str(claim.job_id)
    assert repository.started == [claim]
    assert repository.completed == [claim]
    assert repository.failures == []


def test_worker_does_not_invoke_handler_after_claim_is_stale() -> None:
    repository = FakeJobRepository()
    repository.can_start = False
    registry = HandlerRegistry()
    invoked = Event()
    registry.register("sample.success", lambda *args: invoked.set())

    with WorkerPool(repository, registry, max_workers=1) as pool:
        result = pool.dispatch(make_claim()).result(timeout=1)

    assert result.outcome == ExecutionOutcome.STALE_CLAIM
    assert not invoked.is_set()
    assert repository.completed == []


def test_worker_rejects_completion_when_claim_becomes_stale() -> None:
    repository = FakeJobRepository()
    repository.can_complete = False
    registry = HandlerRegistry()
    registry.register("sample.success", SuccessfulSampleHandler())

    with WorkerPool(repository, registry, max_workers=1) as pool:
        result = pool.dispatch(make_claim()).result(timeout=1)

    assert result.outcome == ExecutionOutcome.STALE_CLAIM


def test_handler_failure_is_persisted_and_left_retryable() -> None:
    repository = FakeJobRepository()
    registry = HandlerRegistry()
    registry.register("sample.failure", ControlledFailureHandler())
    claim = make_claim("sample.failure")

    with WorkerPool(repository, registry, max_workers=1) as pool:
        result = pool.dispatch(claim).result(timeout=1)

    assert result.outcome == ExecutionOutcome.RETRY_SCHEDULED
    assert result.error is not None
    assert "controlled sample failure" in result.error
    assert repository.failures[0][0] == claim
    assert repository.failures[0][1] == result.error


def test_exhausted_failure_is_reported_terminal() -> None:
    repository = FakeJobRepository()
    repository.failure_status = JobStatus.FAILED
    registry = HandlerRegistry()
    registry.register("sample.failure", ControlledFailureHandler())

    with WorkerPool(repository, registry, max_workers=1) as pool:
        result = pool.dispatch(make_claim("sample.failure", attempts=3, max_attempts=3))
        outcome = result.result(timeout=1)

    assert outcome.outcome == ExecutionOutcome.FAILED


def test_unknown_handler_fails_predictably_and_persists_failure() -> None:
    repository = FakeJobRepository()
    registry = HandlerRegistry()

    with WorkerPool(repository, registry, max_workers=1) as pool:
        result = pool.dispatch(make_claim("not.registered")).result(timeout=1)

    assert result.outcome == ExecutionOutcome.FAILED
    assert "UnknownJobType" in repository.failures[0][1]


def test_permanent_handler_failure_does_not_schedule_retry() -> None:
    repository = FakeJobRepository()
    registry = HandlerRegistry()

    def reject(*args):
        raise PermanentJobError("invalid payload")

    registry.register("sample.permanent", reject)
    claim = make_claim("sample.permanent")

    with WorkerPool(repository, registry, max_workers=1) as pool:
        result = pool.dispatch(claim).result(timeout=1)

    assert result.outcome == ExecutionOutcome.FAILED
    assert repository.failures == [(claim, "PermanentJobError: invalid payload")]


def test_worker_slot_is_released_after_a_handler_failure() -> None:
    repository = FakeJobRepository()
    registry = HandlerRegistry()
    registry.register("sample.failure", ControlledFailureHandler())
    registry.register("sample.success", SuccessfulSampleHandler())

    with WorkerPool(repository, registry, max_workers=1) as pool:
        failed = pool.dispatch(make_claim("sample.failure")).result(timeout=1)
        succeeded = pool.dispatch(make_claim()).result(timeout=1)

    assert failed.outcome == ExecutionOutcome.RETRY_SCHEDULED
    assert succeeded.outcome == ExecutionOutcome.COMPLETED


def test_worker_renews_lease_while_handler_is_running() -> None:
    repository = FakeJobRepository()
    registry = HandlerRegistry()
    renewals = []

    def renew_lease(claim: JobClaim, *, lease_seconds: float) -> bool:
        renewals.append(claim)
        return True

    repository.renew_lease = renew_lease
    registry.register("sample.slow", SlowSampleHandler(0.08))
    claim = make_claim("sample.slow")

    with WorkerPool(
        repository,
        registry,
        max_workers=1,
        lease_seconds=0.03,
    ) as pool:
        result = pool.dispatch(claim).result(timeout=1)

    assert result.outcome == ExecutionOutcome.COMPLETED
    assert renewals


def test_worker_logs_retry_wait_separately_from_initial_schedule_delay() -> None:
    repository = FakeJobRepository()
    handlers = HandlerRegistry()
    handlers.register("sample.success", SuccessfulSampleHandler())
    claim = make_claim(
        retry_eligible_at=datetime.now(UTC) - timedelta(seconds=1),
    )

    with patch("app.workers.pool.logger.info") as log_info:
        with WorkerPool(repository, handlers, max_workers=1) as pool:
            result = pool.dispatch(claim).result(timeout=1)
    start_log = next(
        call
        for call in log_info.call_args_list
        if call.args[0].startswith("Job handler started")
    )
    extra = start_log.kwargs["extra"]
    assert result.outcome == ExecutionOutcome.COMPLETED
    assert extra["scheduling_delay_seconds"] >= 1
    assert extra["retry_wait_seconds"] >= 1


def test_worker_slot_is_released_after_database_exception() -> None:
    repository = FakeJobRepository()
    repository.start_error = RuntimeError("database unavailable")
    registry = HandlerRegistry()
    registry.register("sample.success", SuccessfulSampleHandler())

    with WorkerPool(repository, registry, max_workers=1) as pool:
        with pytest.raises(RuntimeError, match="database unavailable"):
            pool.dispatch(make_claim()).result(timeout=1)
        result = pool.dispatch(make_claim()).result(timeout=1)

    assert result.outcome == ExecutionOutcome.COMPLETED


def test_worker_pool_bounds_active_and_queued_jobs() -> None:
    repository = FakeJobRepository()
    registry = HandlerRegistry()
    handler_started = Event()
    release_handler = Event()

    def blocking_handler(*args):
        handler_started.set()
        assert release_handler.wait(timeout=2)

    registry.register("sample.block", blocking_handler)
    pool = WorkerPool(repository, registry, max_workers=1)
    try:
        first = pool.dispatch(make_claim("sample.block"))
        assert handler_started.wait(timeout=1)
        with pytest.raises(WorkerCapacityError):
            pool.dispatch(make_claim("sample.block"))
    finally:
        release_handler.set()
        pool.shutdown()

    assert first.result(timeout=1).outcome == ExecutionOutcome.COMPLETED


def test_mixed_duration_jobs_run_concurrently_within_worker_limit() -> None:
    repository = FakeJobRepository()
    registry = HandlerRegistry()
    state_lock = Lock()
    active = 0
    maximum_active = 0

    def timed_handler(duration: float):
        def handle(*args):
            nonlocal active, maximum_active
            with state_lock:
                active += 1
                maximum_active = max(maximum_active, active)
            try:
                sleep(duration)
            finally:
                with state_lock:
                    active -= 1
        return handle

    registry.register("short", timed_handler(0.03))
    registry.register("long", timed_handler(0.08))
    claims = [
        make_claim("short"),
        make_claim("long"),
        make_claim("short"),
        make_claim("long"),
    ]

    with WorkerPool(repository, registry, max_workers=2) as pool:
        futures: list[Future[ExecutionResult]] = [
            pool.dispatch(claim) for claim in claims[:2]
        ]
        with pytest.raises(WorkerCapacityError):
            pool.dispatch(claims[2])
        for future in futures:
            assert future.result(timeout=1).outcome == ExecutionOutcome.COMPLETED
        remaining = [pool.dispatch(claim) for claim in claims[2:]]
        outcomes = [future.result(timeout=1).outcome for future in remaining]

    assert outcomes == [
        ExecutionOutcome.COMPLETED,
        ExecutionOutcome.COMPLETED,
    ]
    assert 1 < maximum_active <= 2
