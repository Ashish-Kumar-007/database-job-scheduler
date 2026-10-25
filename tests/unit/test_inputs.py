from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest

from app.services.errors import InputValidationError
from app.services.inputs import JobSubmission, OrganizationInput


@pytest.mark.parametrize(
    ("name", "concurrency", "weight"),
    [
        ("  ", 1, 1),
        ("valid", 0, 1),
        ("valid", -1, 1),
        ("valid", True, 1),
        ("valid", 1, 0),
        ("valid", 1, False),
        ("x" * 256, 1, 1),
        ("invalid\x00name", 1, 1),
    ],
)
def test_organization_input_rejects_invalid_values(
    name: str,
    concurrency: int,
    weight: int,
) -> None:
    with pytest.raises(InputValidationError):
        OrganizationInput(name, concurrency, weight)


def test_organization_input_trims_name() -> None:
    assert OrganizationInput("  Acme  ", 2).name == "Acme"


def test_job_submission_normalizes_payload_and_timestamp_to_utc() -> None:
    local_timestamp = datetime(
        2026,
        10,
        25,
        12,
        tzinfo=timezone(timedelta(hours=5, minutes=30)),
    )
    submission = JobSubmission(
        organization_id=uuid4(),
        job_type="  email.send ",
        payload={"recipients": ["a@example.test"], "retry": 2},
        scheduled_at=local_timestamp,
    )

    assert submission.job_type == "email.send"
    assert submission.scheduled_at == local_timestamp.astimezone(UTC)
    assert submission.scheduled_at.tzinfo is UTC
    assert submission.payload == {
        "recipients": ["a@example.test"],
        "retry": 2,
    }


@pytest.mark.parametrize(
    ("organization_id", "job_type", "payload", "scheduled_at", "max_attempts"),
    [
        ("not-a-uuid", "valid", {}, datetime.now(UTC), 1),
        (uuid4(), "  ", {}, datetime.now(UTC), 1),
        (uuid4(), "valid", {"bad": float("nan")}, datetime.now(UTC), 1),
        (uuid4(), "valid", {"bad": {1, 2}}, datetime.now(UTC), 1),
        (uuid4(), "valid", {1: "bad key"}, datetime.now(UTC), 1),
        (uuid4(), "valid", {"bad": "\x00"}, datetime.now(UTC), 1),
        (uuid4(), "bad\x00type", {}, datetime.now(UTC), 1),
        (uuid4(), "valid", {}, datetime(2026, 1, 1), 1),
        (uuid4(), "valid", {}, datetime.now(UTC), 0),
        (uuid4(), "valid", {}, datetime.now(UTC), True),
    ],
)
def test_job_submission_rejects_invalid_values(
    organization_id,
    job_type,
    payload,
    scheduled_at,
    max_attempts,
) -> None:
    with pytest.raises(InputValidationError):
        JobSubmission(
            organization_id=organization_id,
            job_type=job_type,
            payload=payload,
            scheduled_at=scheduled_at,
            max_attempts=max_attempts,
        )
