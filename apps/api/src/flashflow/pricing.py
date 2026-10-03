"""Rule decisions are a durable outbox; Kafka applies each audited change once."""
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from uuid import uuid5

from sqlalchemy import select

from .config import settings
from .database import Session
from .models import PricingDecision, ProductRow
from .schemas import EventEnvelope, EventType, Product


def recommend(row, now):
    base, old = row.base_price, row.current_price
    lower = max(settings.pricing_min_price, base * (1 - settings.pricing_max_discount), old * (1 - settings.pricing_max_step)).quantize(Decimal('.01'), rounding=ROUND_CEILING)
    upper = min(settings.pricing_max_price, base * (1 + settings.pricing_max_increase), old * (1 + settings.pricing_max_step)).quantize(Decimal('.01'), rounding=ROUND_FLOOR)
    target = base * {'HIGH': Decimal('1.10'), 'LOW': Decimal('.95'), 'NORMAL': Decimal('1')}[row.demand_state]
    if not settings.pricing_enabled or not row.stock or lower > upper:
        return old, 'Pricing disabled, sold out, or bounds incompatible'
    if row.pending_price_decision:
        return old, 'Earlier decision awaiting delivery'
    elapsed = (now - row.last_price_change).total_seconds() if row.last_price_change else float('inf')
    if elapsed < settings.pricing_cooldown_seconds:
        return old, 'Cooldown active'
    new = min(upper, max(lower, target.quantize(Decimal('.01'), rounding=ROUND_HALF_UP)))
    direction = 'UP' if new > old else 'DOWN' if new < old else 'UNCHANGED'
    if direction != 'UNCHANGED' and row.price_direction not in ('UNCHANGED', direction) and elapsed < settings.pricing_reversal_seconds:
        return old, 'Reversal protection active'
    return new, f'{row.demand_state.title()} demand target' if new != old else 'Price already at demand target'


def record_decision(session, row, event):
    # ponytail: event-driven windows do not decay while idle; add scheduled expiry if required.
    now = datetime.now(timezone.utc)
    window = settings.pricing_window_seconds
    if row.demand_window_started is None or (event.timestamp - row.demand_window_started).total_seconds() >= window:
        row.demand_window_started, row.demand_units, row.demand_events = event.timestamp, 0, 0
    row.demand_events += 1
    if event.event_type == EventType.PURCHASE_COMPLETED:
        row.demand_units += event.payload['quantity']
    row.sales_velocity = row.demand_units / window
    observed = max(0, (event.timestamp - row.demand_window_started).total_seconds())
    available = (row.stock - row.reserved_stock) / max(row.reference_stock, 1)
    pressure = row.reserved_stock / max(row.stock, 1)
    row.demand_state = 'HIGH' if pressure >= .5 or row.sales_velocity >= .05 else 'LOW' if available >= .8 and row.sales_velocity <= .01 and observed >= window / 2 else 'NORMAL'
    new, reason = recommend(row, now)
    if not -5 <= (now - event.timestamp).total_seconds() <= window * 2 or observed < window / 2 and row.demand_state != 'HIGH':
        new, reason = row.current_price, 'Window warming or event outside live demand window'
    decision_id = str(uuid5(event.event_id, 'pricing'))
    session.add(PricingDecision(decision_id=decision_id, source_event_id=str(event.event_id), product_id=row.product_id,
        inventory_version=row.version, expected_price_version=row.price_version, previous_price=row.current_price,
        recommended_price=new, adjustment_percentage=(new / row.current_price - 1) * 100,
        signals={'base_price': str(row.base_price), 'stock': row.stock, 'available_ratio': available, 'reservation_ratio': pressure, 'sales_velocity': row.sales_velocity,
                 'event_velocity': row.demand_events / window, 'window_seconds': window, 'observed_seconds': observed, 'demand_state': row.demand_state},
        reason=reason, decision_source='RULES', status='PENDING' if new != row.current_price else 'HOLD', created_at=now, published=False))
    if new != row.current_price:
        row.pending_price_decision = decision_id


async def publish_decision(event, producer):
    await publish_decisions([event], producer)


async def publish_decisions(events, producer):
    async with Session() as session:
        decisions = (await session.scalars(select(PricingDecision).where(
            PricingDecision.source_event_id.in_([str(event.event_id) for event in events]),
            PricingDecision.status == 'PENDING', PricingDecision.published.is_(False)))).all()
        for decision in decisions:
            envelope = EventEnvelope(event_id=decision.decision_id, product_id=decision.product_id, event_type=EventType.PRICE_UPDATED,
                source='pricing-rules', payload={'decision_id': decision.decision_id})
            await producer.send_and_wait(settings.kafka_pricing_topic, key=decision.product_id.encode(), value=envelope.model_dump_json().encode())
            decision.published = True
        if decisions:
            await session.commit()


async def apply_decision(event):
    if event.event_type != EventType.PRICE_UPDATED or event.source != 'pricing-rules' or event.payload != {'decision_id': str(event.event_id)}:
        raise ValueError('Invalid pricing event')
    async with Session() as session, session.begin():
        row = await session.scalar(select(ProductRow).where(ProductRow.product_id == str(event.product_id)).with_for_update())
        decision = await session.get(PricingDecision, str(event.event_id))
        if row is None or decision is None or decision.product_id != row.product_id:
            raise ValueError('Unknown pricing decision')
        if decision.status != 'PENDING':
            return Product.model_validate(row), 'duplicate'
        now = datetime.now(timezone.utc)
        # Recheck time/revision guards, but retain the recorded demand target.
        pending = row.pending_price_decision
        row.pending_price_decision = None
        expected, _ = recommend(row, now)
        valid = pending == decision.decision_id and row.price_version == decision.expected_price_version and row.current_price == decision.previous_price and expected == decision.recommended_price and (now - decision.created_at).total_seconds() <= settings.pricing_window_seconds * 2
        if valid:
            row.price_direction = 'UP' if decision.recommended_price > row.current_price else 'DOWN'
            row.current_price = decision.recommended_price
            row.price_version += 1
            row.last_price_change = now
            row.last_updated = max(row.last_updated, now)
            row.pricing_reason = decision.reason
            decision.status, decision.applied_at, decision.applied_price = 'APPLIED', now, row.current_price
        else:
            decision.status, decision.application_reason = 'REJECTED', 'Superseded, expired, or guardrails changed'
        if pending != decision.decision_id:
            row.pending_price_decision = pending
        return Product.model_validate(row), 'processed' if valid else 'stale'
