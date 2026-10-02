from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, CheckConstraint, DateTime, Float, Integer, JSON, Numeric, String
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
    price_version: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    reference_stock: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    demand_window_started: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    demand_units: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    demand_events: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    demand_state: Mapped[str] = mapped_column(String(20), default="NORMAL", server_default="NORMAL")
    pricing_reason: Mapped[str] = mapped_column(String(250), default="No price changes yet", server_default="No price changes yet")
    price_direction: Mapped[str] = mapped_column(String(20), default="UNCHANGED", server_default="UNCHANGED")
    last_price_change: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    pending_price_decision: Mapped[str | None] = mapped_column(String(36))


class PricingDecision(Base):
    __tablename__ = "pricing_decisions"
    decision_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    source_event_id: Mapped[str] = mapped_column(String(36), unique=True)
    product_id: Mapped[str] = mapped_column(String(36), index=True)
    inventory_version: Mapped[int] = mapped_column(Integer)
    expected_price_version: Mapped[int] = mapped_column(Integer)
    previous_price: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    recommended_price: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    adjustment_percentage: Mapped[Decimal] = mapped_column(Numeric(9, 4))
    signals: Mapped[dict] = mapped_column(JSON)
    reason: Mapped[str] = mapped_column(String(250))
    decision_source: Mapped[str] = mapped_column(String(20), default="RULES")
    status: Mapped[str] = mapped_column(String(20))
    published: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    applied_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    applied_price: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    application_reason: Mapped[str | None] = mapped_column(String(250))


class ProcessedEvent(Base):
    __tablename__ = "processed_events"
    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    product_id: Mapped[str] = mapped_column(String(36))
    outcome: Mapped[str] = mapped_column(String(20))
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
