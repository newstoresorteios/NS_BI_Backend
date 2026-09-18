"""Add generic landing table for extended Tray resources.

Revision ID: 20260917_03
Revises: 20260831_02
Create Date: 2026-09-17
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260917_03"
down_revision: str | None = "20260831_02"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if inspector.has_table("tray_entities"):
        return
    op.create_table(
        "tray_entities",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("resource", sa.String(length=80), nullable=False),
        sa.Column("source_id", sa.String(length=120), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("synced_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "resource",
            "source_id",
            name="uq_tray_entities_resource_source",
        ),
    )
    op.create_index(
        "ix_tray_entities_resource",
        "tray_entities",
        ["resource"],
        unique=False,
    )


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("tray_entities"):
        return
    op.drop_index("ix_tray_entities_resource", table_name="tray_entities")
    op.drop_table("tray_entities")
