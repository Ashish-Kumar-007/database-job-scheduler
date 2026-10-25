"""Add persistent organization round-robin position.

Revision ID: 2b3c4d5e6f7a
Revises: 7f8e9d0c1b2a
Create Date: 2026-10-25 22:25:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "2b3c4d5e6f7a"
down_revision: Union[str, None] = "7f8e9d0c1b2a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "organizations",
        sa.Column("last_claimed_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("organizations", "last_claimed_at")
