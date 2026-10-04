import json
from types import SimpleNamespace
from uuid import uuid4
import pytest
from flashflow.forecasting import (
    FEATURES,
    ForecastModel,
    features,
    baseline,
    predict,
    assess,
)
from flashflow.train_forecast import dataset, split
from flashflow.retail_simulation import scenario_plan
from flashflow.inventory import transition
from flashflow.schemas import EventEnvelope, EventType
from flashflow.seed import build_products
from flashflow.schemas import Product
from flashflow.retail_contracts import (
    ToolResult,
    AnalystRequest,
    ToolArgs,
    ScenarioRequest,
)
from flashflow import analyst
from flashflow.retail import now, controls
from flashflow.config import settings
from flashflow.retail_contracts import ForecastOutput
from fastapi import HTTPException


def test_typed_forecast_rejects_invalid_intervals_and_nonfinite_values():
    payload = dict(
        expected_sales=10,
        lower_bound=5,
        upper_bound=15,
        model_version="test",
        fallback=False,
        interval_method="fixture",
    )
    assert ForecastOutput.model_validate(payload).expected_sales == 10
    with pytest.raises(ValueError):
        ForecastOutput.model_validate({**payload, "lower_bound": 11})
    with pytest.raises(ValueError):
        ForecastOutput.model_validate({**payload, "expected_sales": float("inf")})


def test_causal_features_and_purged_chronological_split():
    rows = dataset(42, 2, 100)
    assert rows == dataset(42, 2, 100)
    train, val, test = split(rows, 100)
    assert max(r["target_end"] for r in train) < min(r["bin"] for r in val)
    assert max(r["target_end"] for r in val) < min(r["bin"] for r in test)
    history = list(range(30))
    x = features(history[:12], 40, 1, 60)
    history[12:] = [999] * 18
    assert x == features(history[:12], 40, 1, 60)
    assert x[:3] == [11, 10, 9]
    with pytest.raises(ValueError):
        features([1] * 11, 1, 1, 0)
    with pytest.raises(ValueError):
        features([float("nan")] * 12, 1, 1, 0)


def test_baseline_fallback_interval_risk_and_quantity():
    x = features([2] * 12, 10, 1, 60)
    assert baseline(x) == 24
    f = predict(None, x)
    assert f["fallback"] and f["lower_bound"] <= f["expected_sales"] <= f["upper_bound"]
    assert "heuristic" in f["interval_method"]
    r = assess(10, f, 10, 500)
    assert r["risk_level"] == "HIGH" and r["recommended_quantity"] == 24
    assert assess(0, f)["risk_level"] == "CRITICAL"
    assert assess(1000, f)["risk_level"] == "HEALTHY"
    assert (
        assess(1, {**f, "upper_bound": 99999}, max_restock=20)["recommended_quantity"]
        == 20
    )
    assert (
        assess(10, {**f, "expected_sales": 0, "upper_bound": 0})[
            "estimated_stockout_minutes"
        ]
        is None
    )


def test_portable_model_rejects_invalid_schema_and_cycles(tmp_path):
    artifact = {
        "version": "test",
        "features": FEATURES,
        "horizon_buckets": 12,
        "bucket_simulated_minutes": 5,
        "base": 2,
        "residual_radius": 3,
        "trees": [
            [
                {
                    "value": 4,
                    "threshold": 0,
                    "feature": 0,
                    "left": 0,
                    "right": 0,
                    "leaf": True,
                }
            ]
        ],
    }
    path = tmp_path / "model.json"
    path.write_text(json.dumps(artifact))
    model = ForecastModel(path)
    assert model.predict([0] * 12) == 6
    f = predict(model, [0] * 12)
    assert (f["lower_bound"], f["expected_sales"], f["upper_bound"]) == (3, 6, 9)
    artifact["features"] = []
    path.write_text(json.dumps(artifact))
    with pytest.raises(ValueError):
        ForecastModel(path)
    artifact["features"] = FEATURES
    artifact["trees"][0][0]["leaf"] = False
    path.write_text(json.dumps(artifact))
    with pytest.raises(ValueError):
        ForecastModel(path)
    path.write_text("not json")
    with pytest.raises(ValueError):
        ForecastModel(path)

    class Broken:
        def predict(self, x):
            raise ValueError("broken")

    assert predict(Broken(), [0] * 12)["fallback"]


def test_scenarios_are_repeatable_and_distinct():
    assert scenario_plan(42, "FLASH_SALE", 24) == scenario_plan(42, "FLASH_SALE", 24)
    assert sum(scenario_plan(42, "FLASH_SALE", 24)) > sum(
        scenario_plan(42, "NORMAL", 24)
    )
    assert scenario_plan(42, "DEMAND_SPIKE", 24) != scenario_plan(
        43, "DEMAND_SPIKE", 24
    )
    with pytest.raises(ValueError):
        scenario_plan(1, "BAD", 10)
    with pytest.raises(ValueError):
        ScenarioRequest(kind="RESTOCK", product_ids=[uuid4()], restock_quantity=501)


def test_versionless_operator_commands_preserve_stock_guards():
    p = Product.model_validate(build_products(1, 7)[0])
    event = EventEnvelope(
        product_id=p.product_id,
        event_type=EventType.INVENTORY_RESTOCKED,
        source="retail-operator",
        payload={"quantity": 3},
    )
    out = transition(p, event)
    assert out.stock == p.stock + 3 and out.version == p.version + 1
    with pytest.raises(ValueError):
        transition(p, event.model_copy(update={"source": "external"}))
    with pytest.raises(ValueError):
        transition(
            p,
            event.model_copy(
                update={
                    "event_type": EventType.STOCK_RELEASED,
                    "payload": {"quantity": p.stock + 100},
                }
            ),
        )


def test_retail_controls_disabled_and_invalid_token(monkeypatch):
    monkeypatch.setattr(settings, "app_env", "production")
    with pytest.raises(HTTPException) as e:
        controls("Bearer secret")
    assert e.value.status_code == 404
    monkeypatch.setattr(settings, "app_env", "development")
    monkeypatch.setattr(settings, "enable_retail_controls", True)
    monkeypatch.setattr(settings, "chaos_token", "secret")
    with pytest.raises(HTTPException) as e:
        controls("Bearer wrong")
    assert e.value.status_code == 403
    controls("Bearer secret")


@pytest.mark.asyncio
async def test_analyst_mock_tool_call_grounding_and_provider_failure(monkeypatch):
    async def fake_tool(name, args):
        return ToolResult(
            tool=name,
            retrieved_at=now(),
            data={"core": {"consumer_lag": 7}, "forecast": {"status": "running"}},
        )

    monkeypatch.setattr(analyst, "tool", fake_tool)

    class Provider:
        async def complete(self, messages, tools=None):
            if tools:
                return {
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "type": "function",
                            "function": {
                                "name": "get_system_health",
                                "arguments": "{}",
                            },
                        }
                    ]
                }
            return {"content": "Consumer lag is 7 according to get_system_health."}

    request = AnalystRequest(question="How healthy is the system?")
    answer = await analyst.analyze(request, Provider())
    assert (
        answer["mode"].startswith("LLM")
        and "7" in answer["answer"]
        and answer["evidence"]
    )

    class Hallucinated(Provider):
        async def complete(self, messages, tools=None):
            return (
                await super().complete(messages, tools)
                if tools
                else {"content": "Consumer lag is 999999999."}
            )

    assert (await analyst.analyze(request, Hallucinated()))[
        "mode"
    ] == "Evidence-only fallback"

    class Down:
        async def complete(self, *args):
            raise TimeoutError()

    assert (await analyst.analyze(request, Down()))["error"]


@pytest.mark.asyncio
async def test_unknown_or_invalid_tools_fail_closed():
    assert (await analyst.tool("execute_restock", {})).error
    assert (await analyst.tool("get_product_details", {"product_id": "bad"})).error
    with pytest.raises(ValueError):
        ToolArgs(limit=10000)
