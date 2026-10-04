"""Bounded real scenario -> Kafka -> SQL -> features -> forecast -> approved restock.

Run with forecast worker and protected development retail controls enabled.
Creates lasting, labelled demo audit data; no real catalog mutation or deletion.
"""

import json
import os
import time
import urllib.request
import urllib.error

BASE = "http://localhost:8000"
TOKEN = os.environ["CHAOS_TOKEN"]


def request(path, body=None, token=TOKEN):
    req = urllib.request.Request(
        BASE + "/retail/" + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)


def until(fn, timeout=100):
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        value = fn()
        if value:
            return value
        time.sleep(1)
    raise AssertionError("Retail condition timed out")


def main():
    overview = request("overview")
    assert overview["controls_enabled"]
    demo = min(
        (
            i
            for i in overview["items"]
            if i["product"]["category"] == "Retail Demo" and i["product"]["stock"] >= 30
        ),
        key=lambda i: i["product"]["stock"],
    )
    pid = demo["product"]["product_id"]
    try:
        request("scenarios", {"kind": "NORMAL", "product_ids": [pid]}, token="wrong")
        raise AssertionError("Bad token accepted")
    except urllib.error.HTTPError as e:
        assert e.code == 403
    scenario = request(
        "scenarios",
        {
            "kind": "FLASH_SALE",
            "product_ids": [pid],
            "seed": 42,
            "bins": 12,
            "strength": 6,
        },
    )
    sid = scenario["scenario_id"]
    until(
        lambda: next(
            (
                s
                for s in request("scenarios")
                if s["scenario_id"] == sid and s["data"]["fulfilled"] > 0
            ),
            None,
        )
    )

    def get_recommendation():
        latest = request(f"products/{pid}")
        return next(
            (r for r in latest["recommendations"] if r["status"] == "PENDING"), None
        )

    rec = until(get_recommendation)
    detail = request(f"products/{pid}")
    assert detail["history"] and detail["forecasts"]
    f = detail["forecasts"][0]["data"]
    assert f["lower_bound"] <= f["expected_sales"] <= f["upper_bound"]
    from datetime import datetime

    assert datetime.fromisoformat(f["target_start"]) > datetime.fromisoformat(
        f["issued_at"]
    )
    result = request(
        f"recommendations/{rec['recommendation_id']}/decision",
        {"decision": "REJECT", "note": "Integration rejection audit"},
    )
    assert result["status"] == "REJECTED"
    rec = until(get_recommendation)
    result = request(
        f"recommendations/{rec['recommendation_id']}/decision",
        {"decision": "APPROVE", "note": "Integration approved Kafka restock"},
    )
    assert result["status"] == "ACCEPTED"

    def executed():
        return next(
            (
                r
                for r in request("recommendations")
                if r["recommendation_id"] == rec["recommendation_id"]
                and r["status"] == "EXECUTED"
            ),
            None,
        )

    done = until(executed)
    assert done["event_id"] and done["published"]
    assert (
        done["decision"] == "APPROVE"
        and done["decided_at"]
        and done["published_at"]
        and done["executed_at"]
    )
    try:
        request(
            f"recommendations/{rec['recommendation_id']}/decision",
            {"decision": "APPROVE"},
        )
        raise AssertionError("Double approval accepted")
    except urllib.error.HTTPError as e:
        assert e.code == 409
    analyst = request(
        "analyst",
        {"question": "Why does this product need attention?", "product_id": pid},
    )
    assert analyst["evidence"] and analyst["mode"].startswith("Evidence-only")
    completed = until(
        lambda: next(
            (
                s
                for s in request("scenarios")
                if s["scenario_id"] == sid and s["status"] == "COMPLETED"
            ),
            None,
        ),
        160,
    )
    assert completed["data"]["attempted"] >= completed["data"]["fulfilled"]
    restock = request(
        "scenarios",
        {
            "kind": "RESTOCK",
            "product_ids": [pid],
            "seed": 42,
            "bins": 1,
            "restock_quantity": 50,
        },
    )
    until(
        lambda: next(
            (
                s
                for s in request("scenarios")
                if s["scenario_id"] == restock["scenario_id"]
                and s["status"] == "COMPLETED"
            ),
            None,
        )
    )
    extra = []
    for kind, bins in [("NORMAL", 1), ("DEMAND_SPIKE", 3)]:
        s = request(
            "scenarios",
            {
                "kind": kind,
                "product_ids": [pid],
                "seed": 77,
                "bins": bins,
                "strength": 6,
            },
        )
        extra.append(
            until(
                lambda: next(
                    (
                        r
                        for r in request("scenarios")
                        if r["scenario_id"] == s["scenario_id"]
                        and r["status"] == "COMPLETED"
                    ),
                    None,
                )
            )
        )
    print(
        json.dumps(
            {
                "status": "passed",
                "scenario_id": sid,
                "product_id": pid,
                "completed_scenario": completed,
                "executed_recommendation": done,
                "analyst_mode": analyst["mode"],
                "latest_forecast": f,
                "additional_scenarios": extra,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
