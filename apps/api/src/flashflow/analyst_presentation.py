"""Deterministic prose over retrieved facts; absent fields stay unavailable."""

LABELS = {
    "get_attention_products": "product inventory and forecasts",
    "get_product_details": "product details and stored pricing decisions",
    "get_scenario_history": "scenario history (core-confirmed inventory events)",
    "get_recommendations": "recommendation audit history",
    "get_system_health": "core metrics and model telemetry",
}


def value(data, key):
    v = data.get(key)
    return "unavailable" if v is None else str(v)


def forecast_summary(f, stale=False):
    if not f:
        return "A forecast is not available yet; no estimate has been inferred."
    lines = [
        f"Expected sales: {value(f, 'expected_sales')} units over the next simulated hour.",
        f"Prediction range: {value(f, 'lower_bound')}–{value(f, 'upper_bound')} units. This advisory band is not a guaranteed outcome.",
        f"Risk at forecast time: {value(f, 'risk_level')}.",
        f"Available when generated: {value(f, 'available_stock')} units.",
        f"Forecast generated: {value(f, 'generated_at')}.",
    ]
    if f.get("risk_reason"):
        lines.append(f"Why: {f['risk_reason']}")
    if stale:
        lines.append(
            "This forecast is stale. Do not approve an action using old predictions."
        )
    return "\n".join(lines)


def recommendation_summary(r):
    name = r.get("product_name") or r.get("product_id") or "Product unavailable"
    return f"{name}: recommended restock of {value(r, 'quantity')} units. Status: {value(r, 'status')}. Risk: {value(r, 'risk')}. Recorded: {value(r, 'created_at')}."


def evidence_answer(evidence, question=""):
    q = question.lower()
    sections, sources = [], []
    for result in evidence:
        sources.append(
            f"- {LABELS.get(result.tool, result.tool)} · {result.retrieved_at.isoformat()}"
        )
        if result.error or result.data is None:
            sections.append(
                "Data unavailable\n"
                + (result.error or "This source returned no data; no values inferred.")
            )
            continue
        d = result.data
        if result.tool == "get_attention_products":
            lines = ["Products needing attention"]
            for item in d.get("items", []):
                p = item.get("product", {})
                available = (
                    p.get("stock") - p.get("reserved_stock")
                    if p.get("stock") is not None
                    and p.get("reserved_stock") is not None
                    else "unavailable"
                )
                lines.append(
                    f"{p.get('name', 'Product unavailable')}\nCurrent available stock: {available} units.\n"
                    + forecast_summary(item.get("forecast"), item.get("stale", False))
                )
                if item.get("recommendation"):
                    lines.append(recommendation_summary(item["recommendation"]))
            if not d.get("items"):
                lines.append(
                    "No products were returned as needing attention under the current policy."
                )
        elif result.tool == "get_product_details":
            p = d.get("product", {})
            lines = [
                p.get("name", "Product details"),
                f"Current stock: {value(p, 'stock')} units; reserved: {value(p, 'reserved_stock')} units. Inventory updated: {value(p, 'last_updated')}.",
            ]
            price_question = "price" in q or "pricing" in q
            if not price_question:
                rows = d.get("forecasts", [])
                f = (
                    {**rows[0]["data"], "generated_at": rows[0].get("generated_at")}
                    if rows
                    else None
                )
                lines.append(forecast_summary(f, d.get("forecast_stale", False)))
            if price_question:
                prices = d.get("pricing_decisions", [])
                if prices:
                    price = prices[0]
                    lines += [
                        "Why the price changed",
                        f"The stored pricing policy recorded: {value(price, 'reason')}.",
                        f"Price changed from {value(price, 'previous_price')} to {value(price, 'applied_price')} at {value(price, 'applied_at')}. This is a recorded rule decision, not an inferred cause.",
                    ]
                else:
                    lines.append(
                        "No applied price decision is available. A reason cannot be inferred."
                    )
            for r in d.get("recommendations", [])[:3]:
                lines.append(
                    recommendation_summary({**r, "product_name": p.get("name")})
                )
        elif result.tool == "get_scenario_history":
            flash = "flash" in q
            rows = (
                sorted(d, key=lambda s: (s.get("kind") != "FLASH_SALE",), reverse=False)
                if flash
                else d
            )
            lines = ["Flash sale summary" if flash else "Retail scenario summary"]
            if flash and not any(s.get("kind") == "FLASH_SALE" for s in rows):
                lines.append(
                    "No flash sale appears in the retrieved scenario history. Other recorded scenarios are listed below."
                )
            for s in rows:
                name = str(s.get("kind", "Scenario")).replace("_", " ").capitalize()
                data = s.get("data", {})
                status = s.get("status")
                lines += [
                    f"{name}: {'completed successfully' if status == 'COMPLETED' else 'status ' + str(status or 'unavailable')}.",
                    f"Started: {value(s, 'started_at')}. Ended: {value(s, 'ended_at')}.",
                ]
                if s.get("kind") == "RESTOCK":
                    lines.append(
                        f"{value(data, 'restocked')} units were restocked through inventory events."
                    )
                else:
                    lines += [
                        f"{value(data, 'attempted')} purchase attempts were recorded.",
                        f"{value(data, 'fulfilled')} purchases were fulfilled.",
                        f"{value(data, 'censored')} attempts could not be fulfilled because inventory was unavailable.",
                    ]
            if not rows:
                lines.append(
                    "No retail scenarios are recorded in this retrieved history."
                )
        elif result.tool == "get_recommendations":
            rows = (
                [r for r in d if r.get("status") == "PENDING"] if "pending" in q else d
            )
            lines = [
                "Pending recommendations"
                if "pending" in q
                else "Replenishment decisions"
            ]
            lines.extend(recommendation_summary(r) for r in rows)
            if not rows:
                lines.append(
                    "No matching recommendations were returned. This is a bounded history, not an assertion about all archived records."
                )
        else:
            core, model = d.get("core", {}), d.get("forecast", {})
            lines = [
                "FlashFlow health summary",
                f"Core consumer lag: {value(core, 'consumer_lag')} records.",
                f"Failed processing attempts: {value(core, 'failed')}; dead-letter records: {value(core, 'dlq')}.",
                f"Core metrics sampled: {value(core, 'server_now')}.",
                f"Forecast worker: {value(model, 'status')}; model: {value(model, 'model_version')}.",
                f"Last forecast: {value(model, 'last_successful_forecast')}. Feature age: {value(model, 'feature_age_seconds')} seconds.",
                f"Recent fulfilled-sales MAE: {value(model, 'recent_mae')}; RMSE: {value(model, 'recent_rmse')}.",
                "Recent live errors differ from offline synthetic evaluation. These metrics alone do not establish that the whole system is healthy.",
            ]
        sections.append("\n".join(lines))
    return (
        "\n\n".join(sections)
        + "\n\nVerified sources and retrieval times\n"
        + "\n".join(sources)
    )
