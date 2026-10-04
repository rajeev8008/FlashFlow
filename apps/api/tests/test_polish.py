from datetime import timedelta
from types import SimpleNamespace

from flashflow.analyst_presentation import evidence_answer
from flashflow.retail_contracts import ToolResult
from flashflow.retail import now
from flashflow.recommendation_policy import should_recommend
from flashflow.retail_simulation import scenario_plan
import pytest
from flashflow import analyst


@pytest.mark.asyncio
async def test_named_product_question_without_context_requests_verified_selection():
    assert analyst.planned_tools("What is the forecast for Coffee?") == [
        "get_product_details"
    ]
    result = await analyst.tool("get_product_details", {})
    assert result.data is None and "Select a product context" in result.error


def test_friendly_flash_summary_prioritizes_verified_flash_history():
    evidence = [
        ToolResult(
            tool="get_scenario_history",
            retrieved_at=now(),
            data=[
                {
                    "kind": "DEMAND_SPIKE",
                    "status": "COMPLETED",
                    "data": {"attempted": 10, "fulfilled": 10, "censored": 0},
                },
                {
                    "kind": "FLASH_SALE",
                    "status": "COMPLETED",
                    "started_at": "2026-10-04T00:00:00Z",
                    "data": {"attempted": 83, "fulfilled": 60, "censored": 23},
                },
            ],
        )
    ]
    text = evidence_answer(evidence, "What happened during the flash sale?")
    assert text.startswith("Flash sale summary")
    assert text.index("Flash sale: completed") < text.index("Demand spike")
    assert "83 purchase attempts" in text and "23 attempts could not" in text
    assert "2026-10-04T00:00:00Z" in text and "Verified sources" in text
    assert (
        evidence[0].data[0]["kind"] == "DEMAND_SPIKE"
    )  # Presentation never edits evidence.


def test_missing_analyst_facts_are_unavailable_not_zero_or_healthy():
    r = ToolResult(
        tool="get_system_health", retrieved_at=now(), data={"core": {}, "forecast": {}}
    )
    text = evidence_answer([r])
    assert "Core consumer lag: unavailable" in text
    assert "MAE: unavailable" in text and "healthy" not in text.splitlines()[1].lower()
    r = ToolResult(
        tool="get_product_details",
        retrieved_at=now(),
        data={"product": {"name": "Coffee"}},
    )
    text = evidence_answer([r], "Why did the price change?")
    assert "reason cannot be inferred" in text and "Current stock: unavailable" in text
    r = ToolResult(
        tool="get_scenario_history",
        retrieved_at=now(),
        data=[{"kind": "NORMAL", "status": "RUNNING", "data": {}}],
    )
    assert "No flash sale" in evidence_answer([r], "flash sale")
    assert "unavailable purchase attempts" in evidence_answer([r])


def test_recommendation_cooldown_material_changes_and_inflight_suppression():
    at = now()
    old = SimpleNamespace(
        quantity=20,
        risk="HIGH",
        status="EXPIRED",
        created_at=at - timedelta(seconds=35),
        updated_at=at,
    )
    output = {"recommended_quantity": 21, "risk_level": "HIGH", "expected_sales": 30}

    def allowed():
        return should_recommend(old, {"expected_sales": 30}, output, at, 120, 5, 0.25)

    assert not allowed()
    output["recommended_quantity"] = 25
    assert allowed()
    output["recommended_quantity"] = 21
    output["risk_level"] = "CRITICAL"
    assert allowed()
    old.status = "ACCEPTED"
    assert not allowed()
    old.status = "REJECTED"
    old.updated_at = at - timedelta(seconds=130)
    output["risk_level"] = "HIGH"
    assert allowed()
    old.status = "EXECUTED"
    old.updated_at = at
    assert not allowed()
    output["expected_sales"] = 40
    assert allowed()
    output["recommended_quantity"] = 0
    assert not allowed()


def test_demo_has_real_normal_warmup_and_deterministic_increased_attempts():
    plan = scenario_plan(42, "FLASH_SALE", 36, 6)
    assert plan == scenario_plan(42, "FLASH_SALE", 36, 6)
    normal = scenario_plan(42, "NORMAL", 36, 6)
    assert plan[:12] == normal[:12]
    assert sum(plan[12:]) > sum(normal[12:])


def test_friendly_product_forecast_preserves_snapshot_and_pricing_evidence():
    r = ToolResult(
        tool="get_product_details",
        retrieved_at=now(),
        data={
            "product": {"name": "Headphones", "stock": 50, "reserved_stock": 2},
            "forecasts": [
                {
                    "generated_at": "2026-10-04T00:00:00Z",
                    "data": {
                        "expected_sales": 60,
                        "lower_bound": 40,
                        "upper_bound": 80,
                        "risk_level": "HIGH",
                        "available_stock": 45,
                        "risk_reason": "45 available; expected 60.",
                    },
                }
            ],
            "forecast_stale": True,
            "pricing_decisions": [
                {
                    "reason": "High demand target",
                    "previous_price": "100",
                    "applied_price": "105",
                    "applied_at": "2026-10-04T00:00:00Z",
                }
            ],
        },
    )
    text = evidence_answer([r], "Why does this product need attention?")
    assert (
        "Current stock: 50" in text
        and "Available when generated: 45" in text
        and "stale" in text
    )
    text = evidence_answer([r], "Why did the price change?")
    assert "100 to 105" in text and "recorded rule decision" in text
