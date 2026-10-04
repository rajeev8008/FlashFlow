"""Preserve each human decision/publication/execution timestamp separately."""

from alembic import op
import sqlalchemy as sa

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("recommendations", sa.Column("decision", sa.String(20)))
    for name in ("decided_at", "published_at", "executed_at"):
        op.add_column("recommendations", sa.Column(name, sa.DateTime(timezone=True)))


def downgrade():
    for name in ("executed_at", "published_at", "decided_at", "decision"):
        op.drop_column("recommendations", name)
