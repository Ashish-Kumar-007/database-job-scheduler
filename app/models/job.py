from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class JobStatus(StrEnum):
    SCHEDULED = "scheduled"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    COMPLETED = "completed"
    FAILED = "failed"


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint(
            "length(btrim(job_type)) > 0",
            name="ck_jobs_job_type_nonempty",
        ),
        CheckConstraint(
            "attempts >= 0 AND attempts <= max_attempts",
            name="ck_jobs_attempts_within_limit",
        ),
        CheckConstraint(
            "max_attempts > 0",
            name="ck_jobs_max_attempts_positive",
        ),
        CheckConstraint(
            "status NOT IN ('scheduled', 'retry_wait') OR attempts < max_attempts",
            name="ck_jobs_waiting_has_attempts_remaining",
        ),
        CheckConstraint(
            "(status = 'running' AND locked_by IS NOT NULL "
            "AND length(btrim(locked_by)) > 0 AND claim_token IS NOT NULL "
            "AND lease_expires_at IS NOT NULL) OR "
            "(status <> 'running' AND locked_by IS NULL "
            "AND claim_token IS NULL AND lease_expires_at IS NULL)",
            name="ck_jobs_claim_metadata_matches_status",
        ),
        CheckConstraint(
            "(status = 'retry_wait' AND next_attempt_at IS NOT NULL) OR "
            "(status <> 'retry_wait' AND next_attempt_at IS NULL)",
            name="ck_jobs_retry_schedule_matches_status",
        ),
        CheckConstraint(
            "(status = 'completed' AND completed_at IS NOT NULL) OR "
            "(status <> 'completed' AND completed_at IS NULL)",
            name="ck_jobs_completion_matches_status",
        ),
        Index(
            "ix_jobs_organization_id",
            "organization_id",
        ),
        Index(
            "ix_jobs_scheduled_due",
            "scheduled_at",
            "organization_id",
            postgresql_where=text("status = 'scheduled'"),
        ),
        Index(
            "ix_jobs_retry_due",
            "next_attempt_at",
            "organization_id",
            postgresql_where=text("status = 'retry_wait'"),
        ),
        Index(
            "ix_jobs_expired_lease",
            "lease_expires_at",
            postgresql_where=text("status = 'running'"),
        ),
    )

    id: Mapped[UUID] = mapped_column(
        primary_key=True,
        default=uuid4,
        server_default=text("gen_random_uuid()"),
    )
    organization_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("organizations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    job_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[Any] = mapped_column(JSONB, nullable=False)
    scheduled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    status: Mapped[JobStatus] = mapped_column(
        Enum(
            JobStatus,
            name="job_status",
            native_enum=False,
            create_constraint=True,
            validate_strings=True,
            values_callable=lambda statuses: [status.value for status in statuses],
            length=16,
        ),
        nullable=False,
        default=JobStatus.SCHEDULED,
        server_default=text("'scheduled'"),
    )
    attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=3,
        server_default="3",
    )
    locked_by: Mapped[str | None] = mapped_column(String(255))
    claim_token: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)

    organization: Mapped["Organization"] = relationship(back_populates="jobs")
