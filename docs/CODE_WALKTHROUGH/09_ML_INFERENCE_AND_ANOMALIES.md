# Inference and hourly anomaly state

`inference.py` locates the two deployed joblib paths relative to the checkout. `_models_loaded` prevents loading on every event. `load_models` imports joblib lazily, loads existing artifacts and logs missing/incompatible-load failures without inventing a trained estimator. Deserialization is trusted-code execution: load only this project's controlled artifacts. After a process has attempted loading, adding a file does not reload it; restart is the current policy.

`predict_disaster_type(text)` tries pipeline predict and predict_proba on `[text]`, a batch of shape `[1]`. Maximum class score becomes the uncalibrated text confidence. Model exceptions log and fall through to deterministic substring keyword rules in a fixed order (flood before earthquake before cyclone before fire before infrastructure). Order can change ambiguous narratives' labels, and substring matching can produce false positives. Fallback returns score zero; no keyword match gives unknown.

`predict_severity` checks all missing inputs first and returns unknown/zero. Otherwise the feature row is `[rainfall, water, wind, temperature]`, shape `[1,4]`, with missing defaults `[0,0,0,25]`. A trained forest predicts label/maximum probability; failure falls through to documented threshold rules. The model has weather-only features, so earthquake narratives with no telemetry cannot yield a physically validated damage estimate. Zero-confidence fallback is marked heuristic or unavailable per output by the consumer. `model_status` reports artifact availability and limitations; it is not an individual prediction calibration certificate.

`anomaly.train_report_rate_detector` checks at least 20 historical windows and finite nonnegative counts, reshapes to `[n,1]`, fits IsolationForest with seed 42 and configured contamination, and stores training maximum. `anomaly_scores` obtains library labels/decision_function for `[m,1]`, then marks an anomaly if either the learned label is -1 or count exceeds that maximum. The separate outside_training_range flag names the guard explicitly. A score is not a probability and the guard is not evidence of learned extrapolation. These helpers are CPU-only and inexpensive at a maximum few hundred bins; tree fitting/scoring complexity is derived in the concepts chapter.

`detect_anomalies.main` initializes schema, opens a connection and uses a SQL CTE to find the first actual historical hour and last completed hour, cap history to 168 bins and generate zero-count hours. A left join counts matching event timestamps. Known synthetic/test/manual-dispatch sources are excluded by source convention; arbitrary forged source labels or unrecognized synthetic sources can still contaminate this analysis, so authenticated provenance would be necessary before operational use. The latest completed bin is held out from fitting and used for scoring. Fewer than 21 total bins or constant training history produces an explanatory message instead of fake output.

The result upserts one anomaly_windows row keyed by hour, with count, score, flag and training size. The DB connection context commits or rolls back; finally closes the pool. CLI interval below 60 seconds is rejected; repeated mode logs errors and sleeps, while one-shot errors escape. The guard reason itself is not yet stored in a dedicated database column, so the stored final flag conflates rule/forest triggers; the documentation makes this limit explicit. Dashboard `/api/anomalies` exposes completed stored rows, and the operations panel displays the latest actual scored hour/count/decision score or an explicit unavailable state.

To change inference, test missing and zero separately, preserve the feature order and labels, and compare artifact-version compatibility. Unit tests intentionally force heuristic mode rather than depending on a user's ignored binary artifacts. Actual trained inference is exercised by smoke/application persistence. There is no independently labelled anomaly accuracy benchmark.

<!-- SOURCE_REFERENCE: generated; edit prose above, refresh with scripts/sync_source_reference.py -->

## Exact source reference

These numbered snapshots are synchronized to the source hashes below. They are reference text, not executable replacements. Open the source for editing; run the refresh command after changes. Imports are listed so dependency boundaries remain visible.

### `src/ml/inference.py`

[Open source](../../src/ml/inference.py) · SHA-256 `2ea8ac8b875bf1c1097df2de460510333b73847a7d062de2e20544677c89bc0a`

Dependencies: `import os`; `import logging`; `from typing import Tuple, Optional`

- `load_models`: lines 18–36. Read its control flow with the module guide above.

- `predict_disaster_type`: lines 39–80. Predicts disaster category from text with fallback heuristic.

- `predict_severity`: lines 83–118. Predicts severity rating from physical telemetry with fallback heuristic.

- `model_status`: lines 121–129. Expose provenance; zero confidence means uncalibrated heuristic output.


```text
0001 | import os
0002 | import logging
0003 | from typing import Tuple, Optional
0004 | 
0005 | logger = logging.getLogger("cataclysm.ml")
0006 | 
0007 | MODELS_DIR = os.path.join(
0008 |     os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "models"
0009 | )
0010 | TEXT_MODEL_PATH = os.path.join(MODELS_DIR, "disaster_classifier.joblib")
0011 | SEV_MODEL_PATH = os.path.join(MODELS_DIR, "severity_predictor.joblib")
0012 | 
0013 | _text_model = None
0014 | _sev_model = None
0015 | _models_loaded = False
0016 | 
0017 | 
0018 | def load_models():
0019 |     global _text_model, _sev_model, _models_loaded
0020 |     if _models_loaded:
0021 |         return
0022 |     try:
0023 |         import joblib
0024 | 
0025 |         if os.path.exists(TEXT_MODEL_PATH):
0026 |             _text_model = joblib.load(TEXT_MODEL_PATH)
0027 |             logger.info("Loaded text disaster classifier from disk.")
0028 |         if os.path.exists(SEV_MODEL_PATH):
0029 |             _sev_model = joblib.load(SEV_MODEL_PATH)
0030 |             logger.info("Loaded severity predictor from disk.")
0031 |         _models_loaded = True
0032 |     except Exception as e:
0033 |         logger.warning(
0034 |             f"Could not load ML models from disk: {e}. Using resilient heuristic fallback."
0035 |         )
0036 |         _models_loaded = True
0037 | 
0038 | 
0039 | def predict_disaster_type(text: str) -> Tuple[str, float]:
0040 |     """Predicts disaster category from text with fallback heuristic."""
0041 |     load_models()
0042 |     if _text_model is not None:
0043 |         try:
0044 |             pred = _text_model.predict([text])[0]
0045 |             probs = _text_model.predict_proba([text])[0]
0046 |             confidence = float(max(probs))
0047 |             return pred, confidence
0048 |         except Exception as e:
0049 |             logger.warning(f"Model prediction failed: {e}")
0050 | 
0051 |     # Resilient Heuristic Fallback
0052 |     t = text.lower()
0053 |     if any(w in t for w in ["flood", "water", "river", "rain", "submerged", "inundat"]):
0054 |         return "flood", 0.0
0055 |     elif any(
0056 |         w in t for w in ["quake", "tremor", "shake", "richter", "seismic", "crack"]
0057 |     ):
0058 |         return "earthquake", 0.0
0059 |     elif any(
0060 |         w in t for w in ["cyclone", "storm", "gale", "hurricane", "tornado", "surge"]
0061 |     ):
0062 |         return "cyclone", 0.0
0063 |     elif any(
0064 |         w in t for w in ["fire", "blaze", "wildfire", "smoke", "flame", "inferno"]
0065 |     ):
0066 |         return "fire", 0.0
0067 |     elif any(
0068 |         w in t
0069 |         for w in [
0070 |             "bridge",
0071 |             "collapse",
0072 |             "pipeline",
0073 |             "blackout",
0074 |             "track",
0075 |             "tunnel",
0076 |             "structural",
0077 |         ]
0078 |     ):
0079 |         return "infrastructure", 0.0
0080 |     return "unknown", 0.0
0081 | 
0082 | 
0083 | def predict_severity(
0084 |     rainfall_mm: Optional[float] = None,
0085 |     water_level_m: Optional[float] = None,
0086 |     wind_speed_kmh: Optional[float] = None,
0087 |     temperature_c: Optional[float] = None,
0088 | ) -> Tuple[str, float]:
0089 |     """Predicts severity rating from physical telemetry with fallback heuristic."""
0090 |     load_models()
0091 |     if all(
0092 |         value is None
0093 |         for value in (rainfall_mm, water_level_m, wind_speed_kmh, temperature_c)
0094 |     ):
0095 |         return "unknown", 0.0
0096 |     r = rainfall_mm if rainfall_mm is not None else 0.0
0097 |     w = water_level_m if water_level_m is not None else 0.0
0098 |     wind = wind_speed_kmh if wind_speed_kmh is not None else 0.0
0099 |     temp = temperature_c if temperature_c is not None else 25.0
0100 | 
0101 |     if _sev_model is not None:
0102 |         try:
0103 |             features = [[r, w, wind, temp]]
0104 |             pred = _sev_model.predict(features)[0]
0105 |             probs = _sev_model.predict_proba(features)[0]
0106 |             confidence = float(max(probs))
0107 |             return pred, confidence
0108 |         except Exception as e:
0109 |             logger.warning(f"Severity model prediction failed: {e}")
0110 | 
0111 |     # Resilient Heuristic Fallback
0112 |     if r > 150 or w > 4.5 or wind > 110 or temp > 45:
0113 |         return "critical", 0.0
0114 |     elif r > 70 or w > 2.0 or wind > 65:
0115 |         return "severe", 0.0
0116 |     elif r > 25 or w > 0.8 or wind > 30:
0117 |         return "moderate", 0.0
0118 |     return "minor", 0.0
0119 | 
0120 | 
0121 | def model_status() -> dict:
0122 |     """Expose provenance; zero confidence means uncalibrated heuristic output."""
0123 |     load_models()
0124 |     return {
0125 |         "classifier": "trained" if _text_model is not None else "heuristic",
0126 |         "severity": "trained" if _sev_model is not None else "heuristic",
0127 |         "confidence_note": "Model probabilities are uncalibrated; heuristic confidence is zero.",
0128 |         "training_note": "Bundled training uses a small curated text corpus and synthetic telemetry.",
0129 |     }
```

### `src/ml/anomaly.py`

[Open source](../../src/ml/anomaly.py) · SHA-256 `f44df8f91378f2a2ee4c733e475469d0f43731f35310e347c71268c51db49fdf`

Dependencies: `from __future__ import annotations`; `import numpy as np`; `from sklearn.ensemble import IsolationForest`

- `train_report_rate_detector`: lines 9–22. Train on historical fixed-window report counts, shape [n_windows, 1].

- `anomaly_scores`: lines 25–42. Read its control flow with the module guide above.


```text
0001 | """Fit and apply a transparent Isolation Forest report-rate anomaly detector."""
0002 | 
0003 | from __future__ import annotations
0004 | 
0005 | import numpy as np
0006 | from sklearn.ensemble import IsolationForest
0007 | 
0008 | 
0009 | def train_report_rate_detector(
0010 |     report_counts: list[float], contamination: float = 0.05
0011 | ) -> IsolationForest:
0012 |     """Train on historical fixed-window report counts, shape [n_windows, 1]."""
0013 |     if len(report_counts) < 20:
0014 |         raise ValueError("at least 20 historical windows are required")
0015 |     values = np.asarray(report_counts, dtype=float)
0016 |     if not np.isfinite(values).all() or (values < 0).any():
0017 |         raise ValueError("report counts must be finite and nonnegative")
0018 |     model = IsolationForest(contamination=contamination, random_state=42).fit(
0019 |         values.reshape(-1, 1)
0020 |     )
0021 |     model.training_count_max_ = float(values.max())
0022 |     return model
0023 | 
0024 | 
0025 | def anomaly_scores(
0026 |     model: IsolationForest, report_counts: list[float]
0027 | ) -> list[dict[str, float | bool]]:
0028 |     values = np.asarray(report_counts).reshape(-1, 1)
0029 |     labels = model.predict(values)
0030 |     scores = model.decision_function(values)
0031 |     # Isolation trees cannot extrapolate beyond their largest training split:
0032 |     # arbitrarily large spikes may tie at a boundary leaf. Report a separate,
0033 |     # explicit training-range guard instead of misrepresenting that as ML skill.
0034 |     return [
0035 |         {
0036 |             "report_count": float(value[0]),
0037 |             "score": float(score),
0038 |             "outside_training_range": bool(value[0] > model.training_count_max_),
0039 |             "is_anomaly": bool(label == -1 or value[0] > model.training_count_max_),
0040 |         }
0041 |         for value, score, label in zip(values, scores, labels)
0042 |     ]
```

### `src/ml/detect_anomalies.py`

[Open source](../../src/ml/detect_anomalies.py) · SHA-256 `4898ea053b8648342f6518d983c449e01e92bafe0feb727548b2edf8e571ab44`

Dependencies: `from src.common.db import init_db, get_pool, close_pool`; `from src.ml.anomaly import train_report_rate_detector, anomaly_scores`

- `main`: lines 7–54. Read its control flow with the module guide above.


```text
0001 | """Fit on historical hourly counts and score the latest completed hour only."""
0002 | 
0003 | from src.common.db import init_db, get_pool, close_pool
0004 | from src.ml.anomaly import train_report_rate_detector, anomaly_scores
0005 | 
0006 | 
0007 | def main() -> None:
0008 |     init_db()
0009 |     try:
0010 |         with get_pool().connection() as conn:
0011 |             rows = conn.execute("""
0012 |                 WITH bounds AS (
0013 |                   SELECT date_trunc('hour', MIN(timestamp)) AS first_hour,
0014 |                          date_trunc('hour', CURRENT_TIMESTAMP) - INTERVAL '1 hour' AS last_hour
0015 |                   FROM crisis_events WHERE timestamp <= CURRENT_TIMESTAMP
0016 |                     AND source NOT LIKE 'synthetic%%' AND source NOT IN ('integration','test','operator_manual_dispatch')
0017 |                 ), hours AS (
0018 |                   SELECT generate_series(GREATEST(first_hour, last_hour - INTERVAL '167 hours'),
0019 |                                          last_hour, INTERVAL '1 hour') AS hour FROM bounds
0020 |                 )
0021 |                 SELECT hour, COUNT(e.event_id)::integer AS count FROM hours
0022 |                 LEFT JOIN crisis_events e ON e.timestamp >= hour AND e.timestamp < hour + INTERVAL '1 hour'
0023 |                   AND e.source NOT LIKE 'synthetic%%' AND e.source NOT IN ('integration','test','operator_manual_dispatch')
0024 |                 GROUP BY hour ORDER BY hour
0025 |             """).fetchall()
0026 |             if len(rows) < 21:
0027 |                 print(
0028 |                     "Insufficient history: need 20 completed training hours plus one scoring hour"
0029 |                 )
0030 |                 return
0031 |             history = [row["count"] for row in rows[:-1]]
0032 |             if len(set(history)) < 2:
0033 |                 print("Insufficient variability in historical report counts")
0034 |                 return
0035 |             model = train_report_rate_detector(history)
0036 |             result = anomaly_scores(model, [rows[-1]["count"]])[0]
0037 |             conn.execute(
0038 |                 """
0039 |                 INSERT INTO anomaly_windows(window_start,report_count,score,is_anomaly,training_windows)
0040 |                 VALUES (%s,%s,%s,%s,%s) ON CONFLICT(window_start) DO UPDATE SET
0041 |                 report_count=EXCLUDED.report_count,score=EXCLUDED.score,is_anomaly=EXCLUDED.is_anomaly,
0042 |                 training_windows=EXCLUDED.training_windows
0043 |             """,
0044 |                 (
0045 |                     rows[-1]["hour"],
0046 |                     rows[-1]["count"],
0047 |                     result["score"],
0048 |                     result["is_anomaly"],
0049 |                     len(history),
0050 |                 ),
0051 |             )
0052 |             print(result)
0053 |     finally:
0054 |         close_pool()
0055 | 
0056 | 
0057 | if __name__ == "__main__":
0058 |     import argparse
0059 |     import time
0060 |     import logging
0061 | 
0062 |     parser = argparse.ArgumentParser()
0063 |     parser.add_argument("--interval", type=int, default=0)
0064 |     args = parser.parse_args()
0065 |     if args.interval and args.interval < 60:
0066 |         parser.error("interval must be at least 60 seconds")
0067 |     while True:
0068 |         try:
0069 |             main()
0070 |         except Exception:
0071 |             if not args.interval:
0072 |                 raise
0073 |             logging.exception("Anomaly update failed")
0074 |         if not args.interval:
0075 |             break
0076 |         time.sleep(args.interval)
```
