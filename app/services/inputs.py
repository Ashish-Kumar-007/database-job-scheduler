import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from app.services.errors import InputValidationError


def _positive_integer(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InputValidationError(f"{field_name} must be a positive integer")


def _validate_json_string(value: str, path: str) -> None:
    contains_surrogate = any(
        0xD800 <= ord(character) <= 0xDFFF for character in value
    )
    if "\x00" in value or contains_surrogate:
        raise InputValidationError(
            f"{path} contains text that PostgreSQL JSONB cannot store"
        )


def _validate_json_value(value: object, path: str = "payload") -> None:
    if isinstance(value, str):
        _validate_json_string(value, path)
        return
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, int):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise InputValidationError(f"{path} must not contain non-finite numbers")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise InputValidationError(f"{path} object keys must be strings")
            _validate_json_string(key, f"{path} object key")
            _validate_json_value(item, f"{path}.{key}")
        return
    raise InputValidationError(f"{path} contains a value that is not JSON-compatible")


@dataclass(frozen=True)
class OrganizationInput:
    name: str
    max_concurrent_jobs: int
    weight: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.name, str):
            raise InputValidationError("name must be a string")
        normalized_name = self.name.strip()
        if not normalized_name:
            raise InputValidationError("name must not be empty")
        if "\x00" in normalized_name:
            raise InputValidationError("name must not contain a null character")
        if len(normalized_name) > 255:
            raise InputValidationError("name must be at most 255 characters")
        _positive_integer(self.max_concurrent_jobs, "max_concurrent_jobs")
        _positive_integer(self.weight, "weight")
        object.__setattr__(self, "name", normalized_name)


@dataclass(frozen=True)
class JobSubmission:
    organization_id: UUID
    job_type: str
    payload: object
    scheduled_at: datetime
    max_attempts: int = 3

    def __post_init__(self) -> None:
        if not isinstance(self.organization_id, UUID):
            raise InputValidationError("organization_id must be a UUID")
        if not isinstance(self.job_type, str):
            raise InputValidationError("job_type must be a string")
        normalized_job_type = self.job_type.strip()
        if not normalized_job_type:
            raise InputValidationError("job_type must not be empty")
        if "\x00" in normalized_job_type:
            raise InputValidationError("job_type must not contain a null character")
        if len(normalized_job_type) > 100:
            raise InputValidationError("job_type must be at most 100 characters")
        if not isinstance(self.scheduled_at, datetime):
            raise InputValidationError("scheduled_at must be a datetime")
        if self.scheduled_at.tzinfo is None or self.scheduled_at.utcoffset() is None:
            raise InputValidationError("scheduled_at must be timezone-aware")
        _positive_integer(self.max_attempts, "max_attempts")
        try:
            _validate_json_value(self.payload)
        except RecursionError as error:
            raise InputValidationError("payload nesting is too deep") from error
        try:
            normalized_payload = json.loads(
                json.dumps(self.payload, allow_nan=False, separators=(",", ":"))
            )
        except (TypeError, ValueError, RecursionError) as error:
            raise InputValidationError("payload must be valid JSON") from error

        object.__setattr__(self, "job_type", normalized_job_type)
        object.__setattr__(self, "payload", normalized_payload)
        object.__setattr__(self, "scheduled_at", self.scheduled_at.astimezone(UTC))
