"""Add Tray logistics fields to orders.

Revision ID: 20260917_04
Revises: 20260917_03
Create Date: 2026-09-17
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "20260917_04"
down_revision: str | None = "20260917_03"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


COLUMNS = (
    ("shipping_method_id", sa.String(length=80)),
    ("shipping_method", sa.String(length=200)),
    ("shipping_cost", sa.Numeric(precision=18, scale=2)),
    ("shipment_status", sa.String(length=80)),
    ("tracking_code", sa.String(length=200)),
    ("tracking_url", sa.Text()),
    ("shipped_at", sa.DateTime(timezone=True)),
    ("delivered_at", sa.DateTime(timezone=True)),
    ("estimated_delivery", sa.String(length=120)),
    ("shipment_integrator", sa.String(length=160)),
    ("distribution_center_id", sa.String(length=80)),
    ("shipping_city", sa.String(length=120)),
    ("shipping_state", sa.String(length=10)),
)

INDEXED = (
    "shipping_method_id",
    "shipping_method",
    "shipment_status",
    "tracking_code",
    "shipped_at",
    "delivered_at",
    "shipment_integrator",
    "distribution_center_id",
    "shipping_city",
    "shipping_state",
)


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("orders"):
        return
    existing = {column["name"] for column in inspector.get_columns("orders")}
    for name, column_type in COLUMNS:
        if name not in existing:
            op.add_column("orders", sa.Column(name, column_type, nullable=True))

    indexes = {index["name"] for index in inspector.get_indexes("orders")}
    for name in INDEXED:
        index_name = f"ix_orders_{name}"
        if index_name not in indexes:
            op.create_index(index_name, "orders", [name], unique=False)


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("orders"):
        return
    indexes = {index["name"] for index in inspector.get_indexes("orders")}
    for name in reversed(INDEXED):
        index_name = f"ix_orders_{name}"
        if index_name in indexes:
            op.drop_index(index_name, table_name="orders")
    existing = {column["name"] for column in inspector.get_columns("orders")}
    for name, _column_type in reversed(COLUMNS):
        if name in existing:
            op.drop_column("orders", name)
