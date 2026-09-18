"""Store complete Tray customer contact data.

Revision ID: 20260918_05
Revises: 20260917_04
Create Date: 2026-09-18
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260918_05"
down_revision: str | None = "20260917_04"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


COLUMNS = (
    ("emails", "[]"),
    ("phones", "[]"),
    ("addresses", "[]"),
    ("contact_profile", "{}"),
)


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("customers"):
        return
    existing = {column["name"] for column in inspector.get_columns("customers")}
    for name, default in COLUMNS:
        if name not in existing:
            op.add_column(
                "customers",
                sa.Column(
                    name,
                    sa.JSON(),
                    nullable=False,
                    server_default=sa.text(f"'{default}'"),
                ),
            )


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("customers"):
        return
    existing = {column["name"] for column in inspector.get_columns("customers")}
    for name, _default in reversed(COLUMNS):
        if name in existing:
            op.drop_column("customers", name)
