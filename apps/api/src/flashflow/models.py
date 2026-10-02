from datetime import datetime
from decimal import Decimal

from sqlalchemy import CheckConstraint, DateTime, Float, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class ProductRow(Base):
    __tablename__ = "products"
    __table_args__ = (
        CheckConstraint("stock >= 0", name="ck_products_stock_nonnegative"),
        CheckConstraint("reserved_stock >= 0 AND reserved_stock <= stock", name="ck_products_reserved_valid"),
        CheckConstraint("base_price > 0 AND current_price > 0", name="ck_products_prices_positive"),
    )

    product_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(120))
    category: Mapped[str] = mapped_column(String(80))
    base_price: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    current_price: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    stock: Mapped[int] = mapped_column(Integer)
    reserved_stock: Mapped[int] = mapped_column(Integer, default=0)
    sales_velocity: Mapped[float] = mapped_column(Float, default=0)
    status: Mapped[str] = mapped_column(String(20))
    last_updated: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    version: Mapped[int] = mapped_column(Integer, default=1)


class ProcessedEvent(Base):
    __tablename__ = "processed_events"
    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    product_id: Mapped[str] = mapped_column(String(36))
    outcome: Mapped[str] = mapped_column(String(20))
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
