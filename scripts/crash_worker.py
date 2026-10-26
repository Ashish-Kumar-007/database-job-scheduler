import argparse
import os
import time
from pathlib import Path
from uuid import UUID

from app.config import Settings
from app.db.engine import create_engine
from app.db.session import create_session_factory
from app.repositories.jobs import JobRepository


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True, type=UUID)
    parser.add_argument("--ready-file", required=True, type=Path)
    arguments = parser.parse_args()

    settings = Settings.from_env()
    engine = create_engine(settings)
    try:
        repository = JobRepository(create_session_factory(engine))
        claim = next(
            (
                claim
                for claim in repository.claim_due_jobs(
                    settings.consumer_id,
                    limit=1,
                    lease_seconds=settings.lease_seconds,
                )
                if claim.job_id == arguments.job_id
            ),
            None,
        )
        if claim is None:
            raise RuntimeError(f"Demo crash job {arguments.job_id} was not claimable")
        if repository.start_claim(claim) is None:
            raise RuntimeError(f"Demo crash job {arguments.job_id} claim became stale")

        repository.record_sample_operation(
            claim.job_id,
            str(claim.job_id),
            {
                "job_id": str(claim.job_id),
                "organization_id": str(claim.organization_id),
                "payload": claim.payload,
            },
        )
        arguments.ready_file.write_text("side effect committed", encoding="utf-8")
        time.sleep(120)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
