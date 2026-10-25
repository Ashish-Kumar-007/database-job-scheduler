import argparse
import logging
import signal
from threading import Event

from app.config import Settings
from app.consumers.runner import Consumer
from app.db.engine import create_engine
from app.db.session import create_session_factory
from app.handlers import HandlerRegistry
from app.handlers.sample import (
    ControlledFailureHandler,
    SlowSampleHandler,
    SuccessfulSampleHandler,
)
from app.repositories.jobs import JobRepository
from app.workers import WorkerPool


def _build_handlers() -> HandlerRegistry:
    handlers = HandlerRegistry()
    handlers.register("sample.success", SuccessfulSampleHandler())
    handlers.register("sample.slow", SlowSampleHandler(0.2))
    handlers.register("sample.failure", ControlledFailureHandler())
    return handlers


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an independent job consumer")
    parser.add_argument(
        "--max-completed",
        type=int,
        help="stop after this many execution outcomes (useful for bounded runs)",
    )
    arguments = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    settings = Settings.from_env()
    engine = create_engine(settings)
    stop_event = Event()

    def request_shutdown(signum: int, frame: object) -> None:
        logging.getLogger(__name__).info(
            "Consumer shutdown requested",
            extra={"consumer_id": settings.consumer_id, "signal": signum},
        )
        stop_event.set()

    previous_sigint = signal.signal(signal.SIGINT, request_shutdown)
    previous_sigterm = signal.signal(signal.SIGTERM, request_shutdown)
    try:
        sessions = create_session_factory(engine)
        repository = JobRepository(sessions)
        worker_pool = WorkerPool(
            repository,
            _build_handlers(),
            max_workers=settings.worker_concurrency,
        )
        result = Consumer(settings, repository, worker_pool).run(
            stop_event,
            max_completed=arguments.max_completed,
        )
        logging.getLogger(__name__).info(
            "Consumer %s stopped claimed=%s completed=%s retry_scheduled=%s "
            "failed=%s stale=%s",
            settings.consumer_id,
            result.claimed,
            result.completed,
            result.retry_scheduled,
            result.failed,
            result.stale,
            extra={
                "consumer_id": settings.consumer_id,
                "claimed": result.claimed,
                "completed": result.completed,
                "retry_scheduled": result.retry_scheduled,
                "failed": result.failed,
                "stale": result.stale,
            },
        )
    finally:
        engine.dispose()
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)
