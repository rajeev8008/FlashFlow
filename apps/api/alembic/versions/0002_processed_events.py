"""Durable event deduplication ledger."""
from alembic import op
import sqlalchemy as sa

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table("processed_events",
        sa.Column("event_id", sa.String(36), primary_key=True),
        sa.Column("product_id", sa.String(36), nullable=False),
        sa.Column("outcome", sa.String(20), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False))


def downgrade() -> None:
    op.drop_table("processed_events")
