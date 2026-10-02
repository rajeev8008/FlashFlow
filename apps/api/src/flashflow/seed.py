import asyncio
import random
from datetime import datetime, timezone
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy.dialects.postgresql import insert

from .database import Session
from .models import ProductRow

CATEGORIES = ("Electronics", "Home", "Fitness", "Gaming", "Fashion")


def build_products(count: int = 500, seed: int = 42) -> list[ProductRow]:
    rng = random.Random(seed)
    products = []
    for index in range(1, count + 1):
        category = CATEGORIES[(index - 1) % len(CATEGORIES)]
        price = Decimal(rng.randrange(999, 149999)) / 100
        stock = rng.randint(50, 500)
        products.append(ProductRow(
            product_id=str(uuid5(NAMESPACE_URL, f"flashflow-product-{index}")),
            name=f"{category} Deal {index:03d}",
            category=category,
            base_price=price,
            current_price=price,
            stock=stock,
            reserved_stock=0,
            sales_velocity=0,
            status="ACTIVE",
            last_updated=datetime.now(timezone.utc),
            version=1,
            price_version=0, reference_stock=stock, demand_units=0, demand_events=0,
            demand_state="NORMAL", pricing_reason="No price changes yet", price_direction="UNCHANGED",
        ))
    return products


async def seed_products(count: int = 500) -> None:
    rows = [{column.name: getattr(product, column.name) for column in ProductRow.__table__.columns} for product in build_products(count)]
    async with Session() as session:
        await session.execute(insert(ProductRow).values(rows).on_conflict_do_nothing(index_elements=["product_id"]))
        await session.commit()


if __name__ == "__main__":
    asyncio.run(seed_products())
