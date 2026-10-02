"""Run in the API container: python tests/integration_pricing.py."""
import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4, uuid5

from aiokafka import AIOKafkaProducer
from sqlalchemy import select
from websockets.asyncio.client import connect

from flashflow.config import settings
from flashflow.database import Session
from flashflow.models import ProductRow, PricingDecision
from flashflow.pricing import apply_decision
from flashflow.inventory import process_event
from flashflow.schemas import EventEnvelope, EventType


async def main():
    identity = str(uuid4())
    async with Session() as session, session.begin():
        session.add(ProductRow(product_id=identity, name='Pricing probe', category='Test', base_price=100,
            current_price=100, stock=100, reserved_stock=0, reference_stock=100, sales_velocity=0,
            status='ACTIVE', version=1, last_updated=datetime.now(timezone.utc)))
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    try:
        async with connect('ws://localhost:8000/ws') as socket:
            event = EventEnvelope(product_id=identity, event_type=EventType.STOCK_RESERVED, source='pricing-test', payload={'quantity': 60, 'version': 2})
            await producer.send_and_wait(settings.kafka_inventory_topic, key=identity.encode(), value=event.model_dump_json().encode())
            async def wait_price():
                while True:
                    message = json.loads(await socket.recv())
                    if message['type'] == 'heartbeat':
                        await socket.send('pong')
                    elif message.get('product', {}).get('product_id') == identity and message['product']['price_version'] == 1:
                        assert message['product']['current_price'] == '105.00'
                        assert message['product']['version'] == 2 and message['product']['reserved_stock'] == 60
                        return
            await asyncio.wait_for(wait_price(), 30)
        price_event = EventEnvelope(event_id=uuid5(event.event_id, 'pricing'), product_id=identity,
            event_type=EventType.PRICE_UPDATED, source='pricing-rules', payload={'decision_id': str(uuid5(event.event_id, 'pricing'))})
        product, outcome = await apply_decision(price_event)
        assert outcome == 'duplicate' and product.price_version == 1
        # Retry the source event through Kafka; the durable audit remains singular.
        await producer.send_and_wait(settings.kafka_inventory_topic, key=identity.encode(), value=event.model_dump_json().encode())
        async with Session() as session:
            decisions = (await session.scalars(select(PricingDecision).where(PricingDecision.source_event_id == str(event.event_id)))).all()
            assert len(decisions) == 1 and decisions[0].status == 'APPLIED'
            assert decisions[0].recommended_price == Decimal('105.00')
        followup = EventEnvelope(product_id=identity, event_type=EventType.STOCK_RELEASED, source='pricing-test', payload={'quantity': 1, 'version': 3})
        await process_event(followup)
        async with Session() as session:
            hold = await session.scalar(select(PricingDecision).where(PricingDecision.source_event_id == str(followup.event_id)))
            assert hold.status == 'HOLD' and hold.reason == 'Cooldown active'
        try:
            await apply_decision(price_event.model_copy(update={'source': 'untrusted'}))
            raise AssertionError('Unaudited source accepted')
        except ValueError:
            pass
        print('PASS: inventory -> audited outbox -> pricing Kafka -> database -> WebSocket; independent revision and duplicate safety')
    finally:
        await producer.stop()


if __name__ == '__main__':
    asyncio.run(main())
