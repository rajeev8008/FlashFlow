"""Advisory retail history, immutable forecasts and audited human actions."""

from alembic import op
import sqlalchemy as sa

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "retail_buckets",
        sa.Column("product_id", sa.String(36), primary_key=True),
        sa.Column("bucket", sa.Integer(), primary_key=True),
        sa.Column("sales", sa.Integer(), nullable=False),
        sa.Column("stock", sa.Integer(), nullable=False),
        sa.Column("reserved", sa.Integer(), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
    )
    op.create_table(
        "feature_receipts", sa.Column("event_id", sa.String(36), primary_key=True)
    )
    op.create_table(
        "forecasts",
        sa.Column("forecast_id", sa.String(36), primary_key=True),
        sa.Column("product_id", sa.String(36), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("target_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("data", sa.JSON(), nullable=False),
        sa.Column("actual_sales", sa.Integer()),
    )
    for key in ("product_id", "generated_at", "target_end"):
        op.create_index(f"ix_forecasts_{key}", "forecasts", [key])
    op.create_table(
        "recommendations",
        sa.Column("recommendation_id", sa.String(36), primary_key=True),
        sa.Column("product_id", sa.String(36), nullable=False),
        sa.Column("forecast_id", sa.String(36), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("reason", sa.String(500), nullable=False),
        sa.Column("risk", sa.String(20), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_id", sa.String(36)),
        sa.Column("published", sa.Boolean(), nullable=False),
        sa.Column("operator", sa.String(80)),
        sa.Column("decision_note", sa.String(500)),
    )
    for key in ("product_id", "status"):
        op.create_index(f"ix_recommendations_{key}", "recommendations", [key])
    op.create_table(
        "retail_scenarios",
        sa.Column("scenario_id", sa.String(36), primary_key=True),
        sa.Column("kind", sa.String(20), nullable=False),
        sa.Column("seed", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("data", sa.JSON(), nullable=False),
    )


def downgrade():
    for table in (
        "retail_scenarios",
        "recommendations",
        "forecasts",
        "feature_receipts",
        "retail_buckets",
    ):
        op.drop_table(table)
