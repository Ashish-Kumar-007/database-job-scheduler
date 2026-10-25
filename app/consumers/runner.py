import logging
import random
from concurrent.futures import FIRST_COMPLETED, Future, wait
from dataclasses import dataclass
from threading import Event

from sqlalchemy.exc import DBAPIError, OperationalError, TimeoutError as PoolTimeoutError

from app.config import Settings
from app.repositories.jobs import JobClaim, JobRepository
from app.workers import ExecutionOutcome, ExecutionResult, WorkerCapacityError, WorkerPool

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConsumerResult:
    claimed: int
    completed: int
    retry_scheduled: int
    failed: int
    stale: int


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
        }
        consecutive_database_errors = 0
        try:
            while not stop_event.is_set():
                self._collect_finished(pending, totals)
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
                    try:
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
                                },
                            )
                        except WorkerCapacityError:
                            logger.error(
                                "Claimed job could not be dispatched due to capacity",
                                extra={
                                    "consumer_id": self._settings.consumer_id,
                                    "job_id": str(claim.job_id),
                                    "organization_id": str(claim.organization_id),
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
            self._drain(pending, totals, stop_event)

        return ConsumerResult(**totals)

    def _collect_finished(
        self,
        pending: dict[Future[ExecutionResult], JobClaim],
        totals: dict[str, int],
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
                },
            )

    def _drain(
        self,
        pending: dict[Future[ExecutionResult], JobClaim],
        totals: dict[str, int],
        stop_event: Event,
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
        self._collect_finished(pending, totals)

    def _reconnect_delay(self, failure_count: int) -> float:
        exponential_delay = min(
            self._settings.reconnect_max_seconds,
            self._settings.reconnect_base_seconds * (2 ** min(failure_count - 1, 20)),
        )
        return random.uniform(exponential_delay * 0.8, exponential_delay)
