from collections import deque
from threading import Event, Thread

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine import URL

from app.config import Settings
from app.consumers import Consumer
from app.handlers import HandlerRegistry
from app.repositories.jobs import JobClaim
from app.workers import WorkerPool
from tests.unit.test_workers import FakeJobRepository, make_claim


def make_settings(
    *,
    worker_concurrency: int = 2,
    claim_batch_size: int = 10,
    poll_interval_seconds: float = 0.01,
    shutdown_grace_seconds: float = 1,
) -> Settings:
    return Settings(
        database_url=URL.create(
            "postgresql+psycopg",
            username="test",
            password="test",
            host="localhost",
            database="test",
        ),
        db_pool_size=5,
        db_max_overflow=10,
        consumer_id="test-consumer",
        worker_concurrency=worker_concurrency,
        poll_interval_seconds=poll_interval_seconds,
        claim_batch_size=claim_batch_size,
        lease_seconds=30,
        shutdown_grace_seconds=shutdown_grace_seconds,
        reconnect_base_seconds=0.01,
        reconnect_max_seconds=0.02,
        retry_base_seconds=0.01,
        retry_max_seconds=1,
        retry_jitter_ratio=0,
        lease_reap_interval_seconds=5,
    )


class PollingRepository(FakeJobRepository):
    def __init__(self, claims: list[JobClaim] | None = None) -> None:
        super().__init__()
        self.claims = deque(claims or [])
        self.poll_limits: list[int] = []
        self.poll_count = 0
        self.poll_error: Exception | None = None

    def claim_due_jobs(
        self,
        owner: str,
        *,
        limit: int,
        lease_seconds: float,
    ) -> list[JobClaim]:
        self.poll_count += 1
        self.poll_limits.append(limit)
        if self.poll_error is not None:
            error = self.poll_error
            self.poll_error = None
            raise error
        claims = []
        for _ in range(min(limit, len(self.claims))):
            claims.append(self.claims.popleft())
        return claims


def test_consumer_polls_and_dispatches_without_exceeding_worker_capacity() -> None:
    repository = PollingRepository(
        [make_claim() for _ in range(3)]
    )
    handlers = HandlerRegistry()
    handlers.register("sample.success", lambda *args: "ok")
    stop_event = Event()
    pool = WorkerPool(repository, handlers, max_workers=2)

    result = Consumer(
        make_settings(worker_concurrency=2, claim_batch_size=10),
        repository,
        pool,
    ).run(stop_event, max_completed=3)

    assert result.claimed == 3
    assert result.completed == 3
    assert max(repository.poll_limits) <= 2
    assert sum(repository.poll_limits) >= 3


def test_consumer_does_not_claim_past_bounded_run_outcome_limit() -> None:
    repository = PollingRepository([make_claim() for _ in range(3)])
    handlers = HandlerRegistry()
    handlers.register("sample.success", lambda *args: "ok")
    pool = WorkerPool(repository, handlers, max_workers=2)

    result = Consumer(
        make_settings(worker_concurrency=2, claim_batch_size=10),
        repository,
        pool,
    ).run(Event(), max_completed=1)

    assert result.claimed == 1
    assert result.completed == 1
    assert len(repository.claims) == 2


def test_consumer_keeps_polling_when_no_jobs_are_due() -> None:
    repository = PollingRepository()
    stop_event = Event()
    original_claim = repository.claim_due_jobs

    def claim_and_stop_after_two_polls(owner, *, limit, lease_seconds):
        claims = original_claim(
            owner,
            limit=limit,
            lease_seconds=lease_seconds,
        )
        if repository.poll_count >= 2:
            stop_event.set()
        return claims

    repository.claim_due_jobs = claim_and_stop_after_two_polls
    pool = WorkerPool(repository, HandlerRegistry(), max_workers=1)

    result = Consumer(
        make_settings(worker_concurrency=1),
        repository,
        pool,
    ).run(stop_event)

    assert result.claimed == 0
    assert repository.poll_count >= 2


def test_consumer_retries_transient_database_outage_with_backoff() -> None:
    repository = PollingRepository([make_claim()])
    repository.poll_error = OperationalError("SELECT", {}, ConnectionError("offline"))
    handlers = HandlerRegistry()
    handlers.register("sample.success", lambda *args: "ok")
    pool = WorkerPool(repository, handlers, max_workers=1)

    result = Consumer(
        make_settings(worker_concurrency=1),
        repository,
        pool,
    ).run(Event(), max_completed=1)

    assert repository.poll_count >= 2
    assert result.completed == 1


def test_consumer_drain_waits_for_inflight_work_before_returning() -> None:
    repository = PollingRepository([make_claim()])
    handler_started = Event()
    release_handler = Event()
    handlers = HandlerRegistry()

    def blocking_handler(*args):
        handler_started.set()
        assert release_handler.wait(timeout=2)

    handlers.register("sample.success", blocking_handler)
    stop_event = Event()
    consumer = Consumer(
        make_settings(worker_concurrency=1, shutdown_grace_seconds=1),
        repository,
        WorkerPool(repository, handlers, max_workers=1),
    )
    run_results = []
    thread = Thread(target=lambda: run_results.append(consumer.run(stop_event)))
    thread.start()
    assert handler_started.wait(timeout=1)
    stop_event.set()
    assert thread.is_alive()
    release_handler.set()
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert run_results[0].completed == 1


@pytest.mark.parametrize("max_completed", [0, -1, True, 1.5])
def test_consumer_rejects_invalid_completion_limit(max_completed) -> None:
    repository = PollingRepository()
    consumer = Consumer(
        make_settings(),
        repository,
        WorkerPool(repository, HandlerRegistry(), max_workers=1),
    )
    with pytest.raises(ValueError):
        consumer.run(Event(), max_completed=max_completed)
    consumer._worker_pool.shutdown()
