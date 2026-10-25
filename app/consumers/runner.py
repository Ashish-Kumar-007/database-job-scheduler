import logging
import math
import random
from concurrent.futures import FIRST_COMPLETED, Future, wait
from dataclasses import dataclass, field
from threading import Event
from time import monotonic

from sqlalchemy.exc import DBAPIError, OperationalError, TimeoutError as PoolTimeoutError

from app.config import Settings
from app.repositories.jobs import JobClaim, JobRepository
from app.workers import ExecutionOutcome, ExecutionResult, WorkerCapacityError, WorkerPool

logger = logging.getLogger(__name__)


@dataclass
class _SchedulingDelayMetrics:
    count: int = 0
    total_seconds: float = 0
    maximum_seconds: float | None = None
    reservoir: list[float] = field(default_factory=list)

    MAX_RESERVOIR_SIZE = 10_000

    def record(self, delay_seconds: float) -> None:
        self.count += 1
        self.total_seconds += delay_seconds
        if self.maximum_seconds is None or delay_seconds > self.maximum_seconds:
            self.maximum_seconds = delay_seconds

        if len(self.reservoir) < self.MAX_RESERVOIR_SIZE:
            self.reservoir.append(delay_seconds)
            return

        replacement_index = random.randrange(self.count)
        if replacement_index < self.MAX_RESERVOIR_SIZE:
            self.reservoir[replacement_index] = delay_seconds


@dataclass(frozen=True)
class ConsumerResult:
    claimed: int
    completed: int
    retry_scheduled: int
    failed: int
    stale: int
    recovered: int
    elapsed_seconds: float
    scheduling_delay_average_seconds: float | None
    scheduling_delay_max_seconds: float | None
    scheduling_delay_p95_seconds: float | None
    completed_per_second: float


class Consumer:
    def __init__(
        self,
        settings: Settings,
        repository: JobRepository,
        worker_pool: WorkerPool,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._worker_pool = worker_pool

    def run(
        self,
        stop_event: Event,
        *,
        max_completed: int | None = None,
    ) -> ConsumerResult:
        if max_completed is not None and (
            isinstance(max_completed, bool)
            or not isinstance(max_completed, int)
            or max_completed < 1
        ):
            raise ValueError("max_completed must be a positive integer")

        pending: dict[Future[ExecutionResult], JobClaim] = {}
        totals = {
            "claimed": 0,
            "completed": 0,
            "retry_scheduled": 0,
            "failed": 0,
            "stale": 0,
            "recovered": 0,
        }
        consecutive_database_errors = 0
        next_recovery_at = 0.0
        run_started_at = monotonic()
        scheduling_delays = _SchedulingDelayMetrics()
        try:
            while not stop_event.is_set():
                self._collect_finished(pending, totals, scheduling_delays)
                finished = sum(
                    totals[key]
                    for key in ("completed", "retry_scheduled", "failed", "stale")
                )
                if max_completed is not None and finished >= max_completed:
                    break

                available_slots = self._settings.worker_concurrency - len(pending)
                if available_slots > 0:
                    claim_limit = min(
                        self._settings.claim_batch_size,
                        available_slots,
                    )
                    if max_completed is not None:
                        claim_limit = min(claim_limit, max_completed - finished)
                    try:
                        if monotonic() >= next_recovery_at:
                            recovered = self._repository.recover_expired_claims(
                                limit=min(
                                    self._settings.claim_batch_size,
                                    JobRepository.MAX_CLAIM_BATCH_SIZE,
                                ),
                                retry_delay_seconds=self._settings.retry_base_seconds,
                            )
                            totals["recovered"] += len(recovered)
                            for item in recovered:
                                logger.warning(
                                    "Expired job claim recovered",
                                    extra={
                                        "job_id": str(item.job_id),
                                        "organization_id": str(item.organization_id),
                                        "consumer_id": self._settings.consumer_id,
                                        "attempt": item.attempts,
                                        "status": item.status.value,
                                        "event_type": "job_recovered",
                                    },
                                )
                            next_recovery_at = (
                                monotonic()
                                + self._settings.lease_reap_interval_seconds
                            )
                        claims = self._repository.claim_due_jobs(
                            self._settings.consumer_id,
                            limit=claim_limit,
                            lease_seconds=self._settings.lease_seconds,
                        )
                    except (OperationalError, PoolTimeoutError) as error:
                        consecutive_database_errors += 1
                        delay = self._reconnect_delay(consecutive_database_errors)
                        logger.warning(
                            "Consumer database polling failed; retrying",
                            extra={
                                "consumer_id": self._settings.consumer_id,
                                "retry_in_seconds": delay,
                                "error_type": type(error).__name__,
                                "event_type": "database_poll_failed",
                            },
                        )
                        stop_event.wait(delay)
                        continue
                    except DBAPIError as error:
                        if not error.connection_invalidated:
                            raise
                        consecutive_database_errors += 1
                        delay = self._reconnect_delay(consecutive_database_errors)
                        logger.warning(
                            "Consumer database connection was invalidated; retrying",
                            extra={
                                "consumer_id": self._settings.consumer_id,
                                "retry_in_seconds": delay,
                                "error_type": type(error).__name__,
                                "event_type": "database_connection_invalidated",
                            },
                        )
                        stop_event.wait(delay)
                        continue

                    consecutive_database_errors = 0
                    for claim in claims:
                        try:
                            pending[self._worker_pool.dispatch(claim)] = claim
                            totals["claimed"] += 1
                            logger.info(
                                "Job dispatched job_id=%s organization_id=%s attempt=%s",
                                claim.job_id,
                                claim.organization_id,
                                claim.attempts,
                                extra={
                                    "consumer_id": self._settings.consumer_id,
                                    "job_id": str(claim.job_id),
                                    "organization_id": str(claim.organization_id),
                                    "attempt": claim.attempts,
                                    "event_type": "job_claimed",
                                },
                            )
                        except WorkerCapacityError:
                            logger.error(
                                "Claimed job could not be dispatched due to capacity",
                                extra={
                                    "consumer_id": self._settings.consumer_id,
                                    "job_id": str(claim.job_id),
                                    "organization_id": str(claim.organization_id),
                                    "event_type": "claim_dispatch_failed",
                                },
                            )
                            raise

                if pending:
                    wait(
                        tuple(pending),
                        timeout=self._settings.poll_interval_seconds,
                        return_when=FIRST_COMPLETED,
                    )
                else:
                    stop_event.wait(self._settings.poll_interval_seconds)
        finally:
            self._drain(pending, totals, stop_event, scheduling_delays)

        elapsed_seconds = max(monotonic() - run_started_at, 1e-9)
        delays = sorted(scheduling_delays.reservoir)
        return ConsumerResult(
            **totals,
            elapsed_seconds=elapsed_seconds,
            scheduling_delay_average_seconds=(
                scheduling_delays.total_seconds / scheduling_delays.count
                if scheduling_delays.count
                else None
            ),
            scheduling_delay_max_seconds=scheduling_delays.maximum_seconds,
            scheduling_delay_p95_seconds=(
                delays[max(0, math.ceil(len(delays) * 0.95) - 1)]
                if delays
                else None
            ),
            completed_per_second=totals["completed"] / elapsed_seconds,
        )

    def _collect_finished(
        self,
        pending: dict[Future[ExecutionResult], JobClaim],
        totals: dict[str, int],
        scheduling_delays: _SchedulingDelayMetrics,
    ) -> None:
        for future, claim in list(pending.items()):
            if not future.done():
                continue
            del pending[future]
            try:
                result = future.result()
            except Exception:
                logger.exception(
                    "Worker execution raised outside handler processing",
                    extra={
                        "consumer_id": self._settings.consumer_id,
                        "job_id": str(claim.job_id),
                        "organization_id": str(claim.organization_id),
                        "attempt": claim.attempts,
                        "event_type": "worker_execution_error",
                    },
                )
                continue
            outcome_counter = {
                ExecutionOutcome.COMPLETED: "completed",
                ExecutionOutcome.RETRY_SCHEDULED: "retry_scheduled",
                ExecutionOutcome.FAILED: "failed",
                ExecutionOutcome.STALE_CLAIM: "stale",
            }[result.outcome]
            totals[outcome_counter] += 1
            if result.scheduling_delay_seconds is not None:
                scheduling_delays.record(result.scheduling_delay_seconds)
            logger.info(
                "Job execution finished job_id=%s organization_id=%s "
                "attempt=%s outcome=%s",
                result.job_id,
                claim.organization_id,
                claim.attempts,
                result.outcome.value,
                extra={
                    "consumer_id": self._settings.consumer_id,
                    "job_id": str(result.job_id),
                    "organization_id": str(claim.organization_id),
                    "attempt": claim.attempts,
                    "outcome": result.outcome.value,
                    "event_type": f"job_{result.outcome.value}",
                    "scheduling_delay_seconds": result.scheduling_delay_seconds,
                    "retry_wait_seconds": result.retry_wait_seconds,
                },
            )

    def _drain(
        self,
        pending: dict[Future[ExecutionResult], JobClaim],
        totals: dict[str, int],
        stop_event: Event,
        scheduling_delays: _SchedulingDelayMetrics,
    ) -> None:
        if pending:
            _, unfinished = wait(
                tuple(pending),
                timeout=self._settings.shutdown_grace_seconds,
            )
            if unfinished:
                logger.warning(
                    "Worker shutdown grace period elapsed; waiting for running handlers "
                    "before disposing database resources",
                    extra={
                        "consumer_id": self._settings.consumer_id,
                        "in_flight": len(unfinished),
                    },
                )
        self._worker_pool.shutdown(wait=True)
        self._collect_finished(pending, totals, scheduling_delays)

    def _reconnect_delay(self, failure_count: int) -> float:
        exponential_delay = min(
            self._settings.reconnect_max_seconds,
            self._settings.reconnect_base_seconds * (2 ** min(failure_count - 1, 20)),
        )
        return random.uniform(exponential_delay * 0.8, exponential_delay)
