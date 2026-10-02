"""Durable demand windows, independent price revisions, and pricing outbox/audits."""
from alembic import op
import sqlalchemy as sa

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade():
    for name, kind, default in [
        ("price_version", sa.Integer(), "0"), ("reference_stock", sa.Integer(), "0"),
        ("demand_units", sa.Integer(), "0"), ("demand_events", sa.Integer(), "0"),
        ("demand_state", sa.String(20), "NORMAL"), ("pricing_reason", sa.String(250), "No price changes yet"),
        ("price_direction", sa.String(20), "UNCHANGED"),
    ]:
        op.add_column("products", sa.Column(name, kind, nullable=False, server_default=default))
    for name, kind in [("demand_window_started", sa.DateTime(timezone=True)), ("last_price_change", sa.DateTime(timezone=True)), ("pending_price_decision", sa.String(36))]:
        op.add_column("products", sa.Column(name, kind))
    op.execute("UPDATE products SET reference_stock = GREATEST(stock, 1)")
    op.create_table("pricing_decisions",
        sa.Column("decision_id", sa.String(36), primary_key=True),
        sa.Column("source_event_id", sa.String(36), nullable=False, unique=True),
        sa.Column("product_id", sa.String(36), nullable=False),
        sa.Column("inventory_version", sa.Integer(), nullable=False),
        sa.Column("expected_price_version", sa.Integer(), nullable=False),
        sa.Column("previous_price", sa.Numeric(10, 2), nullable=False),
        sa.Column("recommended_price", sa.Numeric(10, 2), nullable=False),
        sa.Column("adjustment_percentage", sa.Numeric(9, 4), nullable=False),
        sa.Column("signals", sa.JSON(), nullable=False),
        sa.Column("reason", sa.String(250), nullable=False),
        sa.Column("decision_source", sa.String(20), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("published", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("applied_at", sa.DateTime(timezone=True)),
        sa.Column("applied_price", sa.Numeric(10, 2)),
        sa.Column("application_reason", sa.String(250)))
    op.create_index("ix_pricing_decisions_product_id", "pricing_decisions", ["product_id"])
    op.create_index("ix_pricing_decisions_created_at", "pricing_decisions", ["created_at"])


def downgrade():
    op.drop_table("pricing_decisions")
    for name in ["price_version", "reference_stock", "demand_units", "demand_events", "demand_state", "pricing_reason", "price_direction", "demand_window_started", "last_price_change", "pending_price_decision"]:
        op.drop_column("products", name)
