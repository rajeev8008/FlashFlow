"""Suppress unchanged advice without mutating or deleting its audit record."""


def materially_changed(old, old_forecast, output, min_units, fraction):
    quantity_delta = abs(output["recommended_quantity"] - old.quantity)
    expected = (old_forecast or {}).get("expected_sales")
    return (
        old.risk != output["risk_level"]
        or quantity_delta >= max(min_units, old.quantity * fraction)
        or expected is not None
        and abs(output["expected_sales"] - expected)
        >= max(min_units, expected * fraction)
    )


def should_recommend(old, old_forecast, output, at, cooldown, min_units, fraction):
    if not output["recommended_quantity"]:
        return False
    if old is None:
        return True
    if old.status == "ACCEPTED":
        return False  # An approved restock is already in flight.
    changed = materially_changed(old, old_forecast, output, min_units, fraction)
    since = (
        at
        - (old.updated_at if old.status in ("EXECUTED", "REJECTED") else old.created_at)
    ).total_seconds()
    return changed or since >= cooldown
