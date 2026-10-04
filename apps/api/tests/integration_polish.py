"""Real, durable demo validation. Leaves simulated events and audit history intact."""

import json
import os
import sys
import time
import urllib.request


def request(path, body=None):
    req = urllib.request.Request(
        "http://localhost:8000/retail/" + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Authorization": "Bearer " + os.environ["CHAOS_TOKEN"],
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=20) as response:
        return json.load(response)


def main():
    products = request("overview")["items"]
    demo = min(
        (i for i in products if i["product"]["category"] == "Retail Demo"),
        key=lambda i: i["product"]["stock"],
    )
    pid = demo["product"]["product_id"]
    scenario = request(
        "scenarios",
        {
            "kind": "FLASH_SALE",
            "product_ids": [pid],
            "seed": 42,
            "bins": 36,
            "strength": 6,
            "demo_start_stock": 80,
        },
    )
    samples = []
    previous = None
    start = time.monotonic()
    while time.monotonic() - start < 300:
        d = request(f"products/{pid}")
        f = d["forecasts"][0]["data"] if d["forecasts"] else {}
        s = next(
            s
            for s in request("scenarios")
            if s["scenario_id"] == scenario["scenario_id"]
        )
        sample = {
            "tick": s["data"]["tick"],
            "stock": d["product"]["stock"],
            "risk": f.get("risk_level"),
            "expected_sales": f.get("expected_sales"),
            "forecast_id": d["forecasts"][0]["forecast_id"] if d["forecasts"] else None,
        }
        if sample != previous:
            samples.append(sample)
            previous = sample
        if len(samples) % 10 == 1:
            print(
                f"Demo tick {sample['tick']}, stock {sample['stock']}, risk {sample['risk']}",
                file=sys.stderr,
                flush=True,
            )
        if s["status"] == "COMPLETED":
            break
        time.sleep(2)
    assert s["status"] == "COMPLETED", "Scenario did not complete"
    assert any(x["risk"] == "HEALTHY" for x in samples[3:]), (
        "No observed healthy warm-up"
    )
    assert any(x["risk"] in ("HIGH", "CRITICAL") for x in samples), "Risk did not rise"
    assert s["data"]["fulfilled"] > 0 and min(x["stock"] for x in samples) < 80
    assert any(
        x["expected_sales"] is not None
        and x["expected_sales"]
        > min(y["expected_sales"] for y in samples if y["expected_sales"] is not None)
        for x in samples
    )
    explanation = request(
        "analyst",
        {"question": "Why does this product need attention?", "product_id": pid},
    )
    assert (
        explanation["mode"].startswith("Evidence-only")
        and "Expected sales" in explanation["answer"]
        and explanation["evidence"]
    )
    before = request(f"products/{pid}")
    before_stock = before["product"]["stock"]
    before_risk = before["forecasts"][0]["data"]["risk_level"]
    approved = None
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        d = request(f"products/{pid}")
        pending = next(
            (r for r in d["recommendations"] if r["status"] == "PENDING"), None
        )
        if pending:
            approved = request(
                f"recommendations/{pending['recommendation_id']}/decision",
                {"decision": "APPROVE", "note": "Final polish real demo verification"},
            )
            if approved["status"] == "ACCEPTED":
                break
        time.sleep(2)
    assert approved and approved["status"] == "ACCEPTED"
    executed = None
    after = None
    deadline = time.monotonic() + 100
    while time.monotonic() < deadline:
        after = request(f"products/{pid}")
        executed = next(
            (
                r
                for r in after["recommendations"]
                if r["recommendation_id"] == approved["recommendation_id"]
                and r["status"] == "EXECUTED"
            ),
            None,
        )
        rank = {"HEALTHY": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
        after_risk = after["forecasts"][0]["data"]["risk_level"]
        if (
            executed
            and after["product"]["stock"] == before_stock + approved["quantity"]
            and rank[after_risk] < rank[before_risk]
        ):
            break
        time.sleep(2)
    assert (
        executed
        and executed["event_id"]
        and executed["published_at"]
        and executed["executed_at"]
    )
    assert after["product"]["stock"] == before_stock + approved["quantity"]
    assert rank[after_risk] < rank[before_risk], "Restock risk did not decrease"
    summary = request("analyst", {"question": "What happened during the flash sale?"})
    assert "Flash sale summary" in summary["answer"] and summary["evidence"]
    print(
        json.dumps(
            {
                "status": "passed",
                "scenario": s,
                "samples": samples,
                "product_id": pid,
                "stock_before_approval": before_stock,
                "stock_after_execution": after["product"]["stock"],
                "risk_before": before_risk,
                "risk_after": after_risk,
                "executed_recommendation": executed,
                "attention_explanation": explanation,
                "scenario_explanation": summary,
                "model_health": request("model-health"),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
