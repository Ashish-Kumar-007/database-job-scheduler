"""Create organization and job tables.

Revision ID: 1a2b3c4d5e6f
Revises:
Create Date: 2026-10-25 19:15:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "1a2b3c4d5e6f"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "organizations",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("max_concurrent_jobs", sa.Integer(), nullable=False),
        sa.Column(
            "weight",
            sa.Integer(),
            server_default="1",
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "length(btrim(name)) > 0",
            name="ck_organizations_name_nonempty",
        ),
        sa.CheckConstraint(
            "max_concurrent_jobs > 0",
            name="ck_organizations_max_concurrent_jobs_positive",
        ),
        sa.CheckConstraint("weight > 0", name="ck_organizations_weight_positive"),
        sa.PrimaryKeyConstraint("id", name="pk_organizations"),
    )
    op.create_table(
        "jobs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("job_type", sa.String(length=100), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "scheduled",
                "running",
                "retry_wait",
                "completed",
                "failed",
                name="job_status",
                native_enum=False,
                create_constraint=True,
                length=16,
            ),
            server_default=sa.text("'scheduled'"),
            nullable=False,
        ),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("max_attempts", sa.Integer(), server_default="3", nullable=False),
        sa.Column("locked_by", sa.String(length=255), nullable=True),
        sa.Column("claim_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "attempts >= 0 AND attempts <= max_attempts",
            name="ck_jobs_attempts_within_limit",
        ),
        sa.CheckConstraint(
            "max_attempts > 0",
            name="ck_jobs_max_attempts_positive",
        ),
        sa.CheckConstraint(
            "status NOT IN ('scheduled', 'retry_wait') OR attempts < max_attempts",
            name="ck_jobs_waiting_has_attempts_remaining",
        ),
        sa.CheckConstraint(
            "(status = 'running' AND locked_by IS NOT NULL "
            "AND length(btrim(locked_by)) > 0 AND claim_token IS NOT NULL "
            "AND lease_expires_at IS NOT NULL) OR "
            "(status <> 'running' AND locked_by IS NULL "
            "AND claim_token IS NULL AND lease_expires_at IS NULL)",
            name="ck_jobs_claim_metadata_matches_status",
        ),
        sa.CheckConstraint(
            "(status = 'completed' AND completed_at IS NOT NULL) OR "
            "(status <> 'completed' AND completed_at IS NULL)",
            name="ck_jobs_completion_matches_status",
        ),
        sa.CheckConstraint(
            "length(btrim(job_type)) > 0",
            name="ck_jobs_job_type_nonempty",
        ),
        sa.CheckConstraint(
            "(status = 'retry_wait' AND next_attempt_at IS NOT NULL) OR "
            "(status <> 'retry_wait' AND next_attempt_at IS NULL)",
            name="ck_jobs_retry_schedule_matches_status",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_jobs_organization_id_organizations",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_jobs"),
    )
    op.create_index(
        "ix_jobs_expired_lease",
        "jobs",
        ["lease_expires_at"],
        unique=False,
        postgresql_where=sa.text("status = 'running'"),
    )
    op.create_index(
        "ix_jobs_organization_id",
        "jobs",
        ["organization_id"],
        unique=False,
    )
    op.create_index(
        "ix_jobs_retry_due",
        "jobs",
        ["next_attempt_at", "organization_id"],
        unique=False,
        postgresql_where=sa.text("status = 'retry_wait'"),
    )
    op.create_index(
        "ix_jobs_scheduled_due",
        "jobs",
        ["scheduled_at", "organization_id"],
        unique=False,
        postgresql_where=sa.text("status = 'scheduled'"),
    )


def downgrade() -> None:
    op.drop_index("ix_jobs_scheduled_due", table_name="jobs")
    op.drop_index("ix_jobs_retry_due", table_name="jobs")
    op.drop_index("ix_jobs_expired_lease", table_name="jobs")
    op.drop_index("ix_jobs_organization_id", table_name="jobs")
    op.drop_table("jobs")
    op.drop_table("organizations")
