# Demand forecasting experiment

All data is simulated. These results do not describe real customers, Falabella data, or production forecast accuracy. See `apps/api/artifacts/evaluation.json` for raw evaluation and `simulated-retail.csv` for the generated examples.

## Problem and data

Predict completed, stock-constrained sales over twelve future five-minute bins: one simulated hour. Inputs contain the preceding twelve completed bins: one simulated hour. Latent purchase attempts are retained as a separate target column for investigating censoring, but are not predictor inputs or the model's target. Zero sales during a stockout do not establish zero demand.

The seeded generator uses 24 simulated products across four simulated days (1,152 bins per product). Product popularity, a daily sinusoidal pattern, price sensitivity, Poisson variation, normal/flash-sale/spike regimes, finite stock, and periodic restocks produce **27,072 examples**. Restocks are simulated, not optimized supply decisions. Regimes follow a repeating schedule; the held-out test does not establish performance on arbitrary novel economic regimes.

CSV SHA-256: `76457dadb4af0454d3e1ed0fc525c8a0a957beec0c8b1e2f5c5822aa99f9c4f4`.

Features: last 1/2/3 sales bins, three-bin mean, twelve-bin mean/standard deviation, last-minus-third sales acceleration, available stock, current/base price ratio, sine/cosine of simulated time-of-day, and a current promotion indicator. Future scenario labels, future attempted demand, future stock and future promotion changes are excluded. The category/product ID is not a predictive feature.

## Split and measured results

All products share chronological cutoffs. Training uses origins 12–678; its latest target end is bin 690. Validation uses origins 691–908; latest target end 920. Test uses origins 921–1139. Twelve-origin purge gaps prevent target windows crossing a subsequent split. Early windows without twelve observations and late origins without a complete target are omitted. Horizons within a split overlap, so errors are temporally correlated.

| Population | Examples | Baseline MAE | ML MAE | Baseline RMSE | ML RMSE |
| --- | ---: | ---: | ---: | ---: | ---: |
| Chronological test overall | 5,256 | 17.20 | 6.13 | 25.93 | 9.08 |
| Normal | 2,304 | 10.73 | 5.28 | 16.13 | 7.44 |
| Flash sale | 1,152 | 26.03 | 6.64 | 36.34 | 10.06 |
| Demand spike | 1,800 | 19.82 | 6.89 | 28.02 | 10.24 |
| Additional unseen seed/products | 1,584 | 14.06 | 6.85 | 21.58 | 9.53 |

Training: 16,008 examples; validation: 5,232; test: 5,256; 576 otherwise-valid examples are purged. Baseline is the last-three-bin mean × twelve. ML is scikit-learn 1.7.2 `HistGradientBoostingRegressor`, 100 iterations, learning rate 0.08, at most fifteen leaves/depth five, L2 regularization two, seed 42, and early stopping disabled. No random train/test shuffle or automated tuning on the test set occurred.

## Uncertainty and artifact

Absolute validation residuals yield a finite-sample 90th-percentile radius of **13.9874 units**. The band is `[max(0, prediction − radius), prediction + radius]`; point predictions are clipped nonnegative. Measured chronological test coverage was **89.65%**. Temporal dependence and domain shifts invalidate an exchangeability-based coverage guarantee. This is a validation-calibrated prediction band, not a confidence interval on a mean or a stockout probability.

The safe JSON artifact contains numerical tree nodes, base prediction, feature schema, horizon, dataset hash, model version, training time, configuration, calibration and metrics. Runtime checks bound artifact/tree size, reject nonfinite numbers, incompatible schemas and invalid child links. No pickle is loaded. Portable inference matched the training model on all test examples with measured maximum difference **0** in the training export check; a separate portable evaluation reproduced the reported metrics to floating-point precision.

If loading/inference fails, the baseline runs with an explicitly labelled **heuristic** variability band: standard deviation of the twelve observed bins × sqrt(12) × 1.645. It is not the trained model's calibrated band. If features/worker fail, predictions become stale/unavailable and the core inventory path continues.

## Runtime clock and monitoring

One real second corresponds to one simulated minute. Five real seconds form a five-simulated-minute bucket. Forecasting runs approximately every five real seconds, once per bucket ID. Sales inputs use completed event-time buckets; current stock/price are read before prediction. The live target starts at the next whole bucket after issuance, avoiding use of current stock as if it existed before the prediction. Its twelve complete bins span sixty real seconds. There is up to five real seconds of alignment delay before that target begins; it is labelled simulated time throughout the UI. Relative to the offline boundary-origin dataset, live features can have an additional partial-bin acquisition gap. Online errors are reported separately rather than assuming offline scores transfer unchanged.

The observer starts at latest Kafka offsets on first installation, then resumes its independent group offsets. Durable feature receipts prevent duplicate sales aggregation. Feature lag and the observer watermark expose delayed aggregation; stock/price history is an observed durable snapshot, not exact event-by-event market state. Forecast rows preserve predictions; only their separately stored matured actual-sales value is completed later, after the observer catches up. Recent MAE/RMSE use up to 200 matured forecasts, mixing runtime regimes/model versions; they are not interchangeable with the held-out ML evaluation.

The live demo uses seeded plans with configurable strength and duration. Plans reproduce demand attempts, not identical inventory outcomes after changing starting stock or human actions. Physical scheduler/processing delays can stretch a live tick. The high-throughput random engineering simulator is a different workload from structured training data. No accuracy guarantee is made for that workload; distribution/retention management and richer real data remain future work.

## Reproduce

From the repository root, using Python 3.12+:

```powershell
python -m pip install -e 'apps/api[ml,test]'
python -m flashflow.train_forecast --mode dataset --seed 42 --output .tmp/dataset-replay
python -m flashflow.train_forecast --mode train --seed 42 --output .tmp/model-replay
python -m flashflow.train_forecast --mode evaluate --seed 42 --model apps/api/artifacts/demand-model.json --output .tmp/model-evaluation
```

Outputs refuse model/dataset/evaluation overwrite. Choose a new output directory for another run. Normal CI tests causal features and a small safe fixture; the separate manual `ml.yml` workflow trains/evaluates the model. Training dependencies are optional and absent from the API/runtime image.
