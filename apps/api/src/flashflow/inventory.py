from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from .database import Session
from .models import ProcessedEvent, ProductRow
from .schemas import EventEnvelope, EventType, Product
from .pricing import record_decision


class InventoryPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: int = Field(ge=2, strict=True)
    quantity: int | None = Field(None, gt=0, strict=True)
    stock: int | None = Field(None, ge=0, strict=True)
    reserved_stock: int | None = Field(None, ge=0, strict=True)
    current_price: Decimal | None = Field(None, gt=0, max_digits=10, decimal_places=2)


def transition(product: Product, event: EventEnvelope) -> Product:
    payload = InventoryPayload.model_validate(event.payload)
    if payload.version <= product.version:
        return product
    if payload.version != product.version + 1:
        raise ValueError("product version gap")
    data = product.model_dump()
    quantity = payload.quantity
    if event.event_type in (EventType.STOCK_RESERVED, EventType.STOCK_RELEASED, EventType.PURCHASE_COMPLETED, EventType.INVENTORY_RESTOCKED):
        if quantity is None:
            raise ValueError("inventory event requires quantity")
        if event.event_type == EventType.STOCK_RESERVED:
            data["reserved_stock"] += quantity
        elif event.event_type == EventType.STOCK_RELEASED:
            data["reserved_stock"] -= quantity
        elif event.event_type == EventType.PURCHASE_COMPLETED:
            data["reserved_stock"] -= quantity
            data["stock"] -= quantity
        else:
            data["stock"] += quantity
    elif event.event_type == EventType.PRICE_UPDATED:
        raise ValueError("Price changes require an audited pricing event")
    elif data["stock"] != 0:
        raise ValueError("sold-out event requires zero stock")
    for field in ("stock", "reserved_stock"):
        if getattr(payload, field) is not None and getattr(payload, field) != data[field]:
            raise ValueError(f"{field} snapshot does not match transition")
    data.update(version=payload.version, last_updated=max(product.last_updated, event.timestamp),
                status="SOLD_OUT" if data["stock"] == 0 else "LOW_STOCK" if data["stock"] <= 10 else "ACTIVE")
    return Product.model_validate(data)


async def process_event(event: EventEnvelope) -> tuple[Product, Literal["processed", "duplicate", "stale"]]:
    async with Session() as session, session.begin():
        row = await session.scalar(select(ProductRow).where(ProductRow.product_id == str(event.product_id)).with_for_update())
        if row is None:
            raise ValueError("unknown product")
        product = Product.model_validate(row)
        if await session.get(ProcessedEvent, str(event.event_id)):
            return product, "duplicate"
        updated = transition(product, event)
        outcome = "stale" if updated.version == product.version else "processed"
        if outcome == "processed":
            for field, value in updated.model_dump(mode="python").items():
                if field not in ("product_id", "pricing_source"):
                    setattr(row, field, value)
            record_decision(session, row, event)
            updated = Product.model_validate(row)
        session.add(ProcessedEvent(event_id=str(event.event_id), product_id=str(event.product_id), outcome=outcome, processed_at=datetime.now(timezone.utc)))
    return updated, outcome
