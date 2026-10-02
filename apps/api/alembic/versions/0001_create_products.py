"""create products"""
from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "products",
        sa.Column("product_id", sa.String(36), primary_key=True),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("category", sa.String(80), nullable=False),
        sa.Column("base_price", sa.Numeric(10, 2), nullable=False),
        sa.Column("current_price", sa.Numeric(10, 2), nullable=False),
        sa.Column("stock", sa.Integer(), nullable=False),
        sa.Column("reserved_stock", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("sales_velocity", sa.Float(), nullable=False, server_default="0"),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("last_updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.CheckConstraint("stock >= 0", name="ck_products_stock_nonnegative"),
        sa.CheckConstraint("reserved_stock >= 0 AND reserved_stock <= stock", name="ck_products_reserved_valid"),
        sa.CheckConstraint("base_price > 0 AND current_price > 0", name="ck_products_prices_positive"),
    )


def downgrade() -> None:
    op.drop_table("products")
