"""Shared causal features, portable numeric trees, and deterministic advice.

Runtime inference uses JSON, never pickle. Training lives in train_forecast.py.
One bucket = five simulated minutes; twelve future buckets = one hour.
"""

import json
import math
import statistics
from pathlib import Path

FEATURES = [
    "lag1",
    "lag2",
    "lag3",
    "mean3",
    "mean12",
    "std12",
    "acceleration",
    "available",
    "price_ratio",
    "hour_sin",
    "hour_cos",
    "promotion",
]
HORIZON = 12
WINDOW = 12


def features(sales, available, price_ratio, minute, promotion=0):
    if len(sales) < WINDOW:
        raise ValueError("Twelve completed buckets required")
    history = sales[-WINDOW:]
    if not all(math.isfinite(v) and v >= 0 for v in history):
        raise ValueError("Invalid sales history")
    values = [
        history[-1],
        history[-2],
        history[-3],
        statistics.mean(history[-3:]),
        statistics.mean(history),
        statistics.pstdev(history),
        history[-1] - history[-3],
        available,
        price_ratio,
        math.sin(minute / 1440 * 2 * math.pi),
        math.cos(minute / 1440 * 2 * math.pi),
        int(promotion),
    ]
    if not all(math.isfinite(v) and abs(v) < 1e9 for v in values):
        raise ValueError("Invalid feature values")
    return values


def baseline(x):
    return max(0, x[3] * HORIZON)


class ForecastModel:
    def __init__(self, path):
        if Path(path).stat().st_size > 5_000_000:
            raise ValueError("Artifact too large")
        self.artifact = json.loads(Path(path).read_text(encoding="utf-8"))
        a = self.artifact
        if (
            a["features"] != FEATURES
            or a["horizon_buckets"] != HORIZON
            or a["bucket_simulated_minutes"] != 5
        ):
            raise ValueError("Model schema mismatch")
        if not isinstance(a["version"], str) or not 1 <= len(a["trees"]) <= 500:
            raise ValueError("Invalid model metadata")
        if (
            not math.isfinite(a["base"])
            or not math.isfinite(a["residual_radius"])
            or a["residual_radius"] < 0
        ):
            raise ValueError("Invalid calibration")
        for tree in a["trees"]:
            if not 1 <= len(tree) <= 1023:
                raise ValueError("Invalid tree size")
            for i, node in enumerate(tree):
                if not all(math.isfinite(node[k]) for k in ("value", "threshold")):
                    raise ValueError("Nonfinite node")
                if not node["leaf"] and not (
                    0 <= node["feature"] < len(FEATURES)
                    and i < node["left"] < len(tree)
                    and i < node["right"] < len(tree)
                ):
                    raise ValueError("Invalid tree structure")

    def predict(self, x):
        if len(x) != len(FEATURES) or not all(math.isfinite(v) for v in x):
            raise ValueError("Invalid inference features")
        value = self.artifact["base"]
        for tree in self.artifact["trees"]:
            index = 0
            while not tree[index]["leaf"]:
                n = tree[index]
                index = n["left"] if x[n["feature"]] <= n["threshold"] else n["right"]
            value += tree[index]["value"]
        return max(0, value)


def predict(model, x):
    fallback = False
    try:
        if model is None:
            raise ValueError("Model unavailable")
        expected = model.predict(x)
        radius, version = model.artifact["residual_radius"], model.artifact["version"]
    except Exception:
        expected, version, fallback = baseline(x), "moving-average-v1", True
        # Variability band is a heuristic, explicitly not a calibrated interval.
        radius = x[5] * math.sqrt(HORIZON) * 1.645
    return {
        "expected_sales": round(expected, 2),
        "lower_bound": round(max(0, expected - radius), 2),
        "upper_bound": round(expected + radius, 2),
        "model_version": version,
        "fallback": fallback,
        "interval_method": "heuristic variability band"
        if fallback
        else "90% validation absolute-residual band",
        "horizon_minutes": 60,
        "target": "observed completed sales; stock-constrained, not latent demand",
    }


def assess(available, forecast, safety_stock=10, max_restock=500):
    expected, upper = forecast["expected_sales"], forecast["upper_bound"]
    eta = 60 * available / expected if expected > 0 else None
    risk = (
        "CRITICAL"
        if available <= 0 or eta is not None and eta <= 15
        else "HIGH"
        if expected >= available
        else "MEDIUM"
        if upper >= available
        else "HEALTHY"
    )
    quantity = (
        min(max_restock, max(0, math.ceil(upper + safety_stock - available)))
        if risk != "HEALTHY"
        else 0
    )
    return {
        "risk_level": risk,
        "estimated_stockout_minutes": round(eta, 1) if eta is not None else None,
        "recommended_quantity": quantity,
        "safety_stock": safety_stock,
        "max_restock": max_restock,
        "risk_reason": f"{available} available; expected {expected:g}, upper band {upper:g} over 60 simulated minutes.",
        "recommendation_reason": f"ceil(upper {upper:g} + safety stock {safety_stock} - available {available}), clamped to 0–{max_restock}.",
        "stockout_assumption": "Constant average forecast rate; no inbound stock; advisory, not a probability.",
    }
