"""Create durable idempotent sample-operation ledger.

Revision ID: 7f8e9d0c1b2a
Revises: 1a2b3c4d5e6f
Create Date: 2026-10-25 21:05:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "7f8e9d0c1b2a"
down_revision: Union[str, None] = "1a2b3c4d5e6f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "sample_operations",
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["jobs.id"],
            name="fk_sample_operations_job_id_jobs",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("job_id", name="pk_sample_operations"),
        sa.UniqueConstraint(
            "idempotency_key",
            name="uq_sample_operations_idempotency_key",
        ),
    )


def downgrade() -> None:
    op.drop_table("sample_operations")
