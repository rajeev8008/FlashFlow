"""Seeded simulated dataset, purged chronological evaluation, numeric JSON trees."""

import argparse
import csv
import hashlib
import json
import math
import random
from datetime import datetime, timezone
from pathlib import Path
from .forecasting import FEATURES, WINDOW, HORIZON, features, baseline, ForecastModel
from .retail_simulation import demand_bin


def dataset(seed=42, products=24, bins=1152):
    examples = []
    for pid in range(products):
        rng = random.Random(seed + pid)
        popularity = rng.uniform(0.5, 3)
        stock = rng.randint(30, 120)
        history, states, ratios, kinds, attempts = [], [], [], [], []
        for t in range(bins):
            phase = (t // 48) % 4
            kind = ["NORMAL", "FLASH_SALE", "NORMAL", "DEMAND_SPIKE"][phase]
            ratio = 1.0 if phase in (0, 2) else 0.9
            attempted = demand_bin(
                rng, t * 5, popularity, ratio, kind, strength=4, progress=t % 48 / 47
            )
            sold = min(stock, attempted)
            stock -= sold
            if t % 36 == 0:
                stock += rng.randint(30, 100)
            history.append(sold)
            states.append(stock)
            ratios.append(ratio)
            kinds.append(kind)
            attempts.append(attempted)
        for t in range(WINDOW, bins - HORIZON):
            x = features(
                history[t - WINDOW : t],
                states[t - 1],
                ratios[t - 1],
                t * 5,
                kinds[t - 1] != "NORMAL",
            )
            examples.append(
                {
                    "product": pid,
                    "bin": t,
                    "target_end": t + HORIZON,
                    "scenario": kinds[t - 1],
                    "x": x,
                    "y": sum(history[t : t + HORIZON]),
                    "attempted_target": sum(attempts[t : t + HORIZON]),
                }
            )
    return sorted(examples, key=lambda r: (r["bin"], r["product"]))


def split(rows, bins):
    a, b = int(bins * 0.6), int(bins * 0.8)
    return (
        [r for r in rows if r["target_end"] < a],
        [r for r in rows if a <= r["bin"] and r["target_end"] < b],
        [r for r in rows if b <= r["bin"]],
    )


def metrics(actual, predicted):
    errors = [a - p for a, p in zip(actual, predicted)]
    return {
        "count": len(errors),
        "mae": sum(abs(e) for e in errors) / len(errors),
        "rmse": math.sqrt(sum(e * e for e in errors) / len(errors)),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="artifacts")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--mode", choices=["dataset", "train", "evaluate"], default="train"
    )
    parser.add_argument("--model", default="artifacts/demand-model.json")
    args = parser.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    if args.mode == "evaluate":
        model = ForecastModel(args.model)
        rows = dataset(args.seed)
        _, _, test = split(rows, 1152)
        report = {
            "kind": "portable-artifact-evaluation",
            "simulated": True,
            "model_version": model.artifact["version"],
            "seed": args.seed,
            "baseline": metrics(
                [r["y"] for r in test], [baseline(r["x"]) for r in test]
            ),
            "model": metrics(
                [r["y"] for r in test], [model.predict(r["x"]) for r in test]
            ),
        }
        with (out / "portable-evaluation.json").open("x", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(json.dumps(report, indent=2))
        return
    if args.mode == "dataset":
        rows = dataset(args.seed)
        with (out / "simulated-retail.csv").open(
            "x", newline="", encoding="utf-8"
        ) as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "product",
                    "bin",
                    "target_end",
                    "scenario",
                    *FEATURES,
                    "observed_sales_target",
                    "latent_attempts_target",
                ]
            )
            for r in rows:
                writer.writerow(
                    [
                        r["product"],
                        r["bin"],
                        r["target_end"],
                        r["scenario"],
                        *r["x"],
                        r["y"],
                        r["attempted_target"],
                    ]
                )
        print(
            json.dumps(
                {
                    "rows": len(rows),
                    "seed": args.seed,
                    "simulated": True,
                    "sha256": hashlib.sha256(
                        (out / "simulated-retail.csv").read_bytes()
                    ).hexdigest(),
                }
            )
        )
        return
    import numpy as np
    import sklearn
    from sklearn.ensemble import HistGradientBoostingRegressor

    if (out / "demand-model.json").exists():
        parser.error("Refusing to overwrite model; select a new output directory")
    rows = dataset(args.seed)
    train, validation, test = split(rows, 1152)
    model = HistGradientBoostingRegressor(
        max_iter=100,
        max_leaf_nodes=15,
        max_depth=5,
        learning_rate=0.08,
        l2_regularization=2,
        early_stopping=False,
        random_state=args.seed,
    )
    model.fit([r["x"] for r in train], [r["y"] for r in train])
    val = np.maximum(0, model.predict([r["x"] for r in validation]))
    residuals = sorted(abs(r["y"] - p) for r, p in zip(validation, val))
    radius = residuals[
        min(len(residuals) - 1, math.ceil((len(residuals) + 1) * 0.9) - 1)
    ]
    predicted = np.maximum(0, model.predict([r["x"] for r in test]))
    report = {
        "dataset_kind": "simulated; no real retail accuracy claim",
        "seed": args.seed,
        "products": 24,
        "bins": 1152,
        "simulated_days": 4,
        "bucket_simulated_minutes": 5,
        "horizon_minutes": 60,
        "input_minutes": 60,
        "target": "completed stock-constrained sales over next 12 bins; latent attempts retained separately",
        "split": "60/20/20 chronological, 12-bin purges; overlapping horizons are correlated",
        "split_counts": {
            "train": len(train),
            "validation": len(validation),
            "test": len(test),
        },
        "train_last_target_end": max(r["target_end"] for r in train),
        "validation_first_bin": min(r["bin"] for r in validation),
        "validation_last_target_end": max(r["target_end"] for r in validation),
        "test_first_bin": min(r["bin"] for r in test),
        "baseline": metrics([r["y"] for r in test], [baseline(r["x"]) for r in test]),
        "model": metrics([r["y"] for r in test], predicted),
        "by_scenario": {},
        "interval": {
            "method": "90% validation absolute-residual band, clipped at zero",
            "radius": float(radius),
            "test_coverage": float(
                np.mean(
                    [
                        max(0, p - radius) <= r["y"] <= p + radius
                        for r, p in zip(test, predicted)
                    ]
                )
            ),
            "warning": "Temporal correlation/regime shift prevents exchangeability coverage guarantees.",
        },
        "sklearn_version": sklearn.__version__,
        "features": FEATURES,
    }
    for scenario in sorted({r["scenario"] for r in test}):
        indices = [i for i, r in enumerate(test) if r["scenario"] == scenario]
        report["by_scenario"][scenario] = {
            "baseline": metrics(
                [test[i]["y"] for i in indices],
                [baseline(test[i]["x"]) for i in indices],
            ),
            "model": metrics(
                [test[i]["y"] for i in indices], [float(predicted[i]) for i in indices]
            ),
        }
    heldout = dataset(args.seed + 10000, products=6, bins=288)
    hp = np.maximum(0, model.predict([r["x"] for r in heldout]))
    report["unseen_seed_products"] = {
        "baseline": metrics(
            [r["y"] for r in heldout], [baseline(r["x"]) for r in heldout]
        ),
        "model": metrics([r["y"] for r in heldout], hp),
    }
    csvpath = out / "simulated-retail.csv"
    with csvpath.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "product",
                "bin",
                "target_end",
                "scenario",
                *FEATURES,
                "observed_sales_target",
                "latent_attempts_target",
            ]
        )
        for r in rows:
            writer.writerow(
                [
                    r["product"],
                    r["bin"],
                    r["target_end"],
                    r["scenario"],
                    *r["x"],
                    r["y"],
                    r["attempted_target"],
                ]
            )
    digest = hashlib.sha256(csvpath.read_bytes()).hexdigest()
    artifact = {
        "version": f"hgbr-{digest[:12]}",
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "dataset_sha256": digest,
        "features": FEATURES,
        "horizon_buckets": HORIZON,
        "bucket_simulated_minutes": 5,
        "base": float(model._baseline_prediction[0, 0]),
        "residual_radius": float(radius),
        "metrics": report,
        "config": model.get_params(),
        "trees": [],
    }
    for group in model._predictors:
        artifact["trees"].append(
            [
                {
                    "value": float(n["value"]),
                    "feature": int(n["feature_idx"]),
                    "threshold": float(n["num_threshold"]),
                    "left": int(n["left"]),
                    "right": int(n["right"]),
                    "leaf": bool(n["is_leaf"]),
                }
                for n in group[0].nodes
            ]
        )
    modelpath = out / "demand-model.json"
    modelpath.write_text(
        json.dumps(artifact, indent=2, allow_nan=False), encoding="utf-8"
    )
    portable = ForecastModel(modelpath)
    delta = max(abs(portable.predict(r["x"]) - p) for r, p in zip(test, predicted))
    assert delta < 1e-8, delta
    report["portable_prediction_max_difference"] = delta
    report["dataset_sha256"] = digest
    (out / "evaluation.json").write_text(
        json.dumps(report, indent=2, allow_nan=False), encoding="utf-8"
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
