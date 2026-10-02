from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ProductStatus(StrEnum):
    ACTIVE = "ACTIVE"
    LOW_STOCK = "LOW_STOCK"
    SOLD_OUT = "SOLD_OUT"


class EventType(StrEnum):
    STOCK_RESERVED = "STOCK_RESERVED"
    STOCK_RELEASED = "STOCK_RELEASED"
    PURCHASE_COMPLETED = "PURCHASE_COMPLETED"
    INVENTORY_RESTOCKED = "INVENTORY_RESTOCKED"
    PRICE_UPDATED = "PRICE_UPDATED"
    PRODUCT_SOLD_OUT = "PRODUCT_SOLD_OUT"


class Product(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    product_id: UUID
    name: str = Field(min_length=1, max_length=120)
    category: str = Field(min_length=1, max_length=80)
    base_price: Decimal = Field(gt=0, max_digits=10, decimal_places=2)
    current_price: Decimal = Field(gt=0, max_digits=10, decimal_places=2)
    stock: int = Field(ge=0)
    reserved_stock: int = Field(ge=0)
    sales_velocity: float = Field(ge=0)
    status: ProductStatus
    last_updated: datetime
    version: int = Field(ge=1)
    price_version: int = Field(0, ge=0)
    demand_state: str = "NORMAL"
    pricing_reason: str = "No price changes yet"
    price_direction: str = "UNCHANGED"
    pricing_source: str = "RULES"

    @model_validator(mode="after")
    def reserved_cannot_exceed_stock(self) -> "Product":
        if self.reserved_stock > self.stock:
            raise ValueError("reserved_stock cannot exceed stock")
        return self


class EventEnvelope(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: EventType
    product_id: UUID
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    schema_version: str = "1.0"
    source: str = Field(min_length=1)
    payload: dict[str, Any]


class KafkaRecord(BaseModel):
    key: bytes
    value: bytes


def kafka_record(event: EventEnvelope) -> KafkaRecord:
    return KafkaRecord(
        key=str(event.product_id).encode(),
        value=event.model_dump_json().encode(),
    )
