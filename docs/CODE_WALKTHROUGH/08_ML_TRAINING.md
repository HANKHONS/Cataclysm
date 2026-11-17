# Training: distinguish a connected model from a validated target

`train.py` imports NumPy for synthetic feature arrays, scikit-learn for splitting/feature extraction/classifiers/metrics, joblib for model persistence and pathlib/JSON/hash/copy utilities for versioned evidence. `TEXT_SAMPLES` is a literal 50-example curated English corpus across five classes, not an acquired large independent crisis dataset. Inspect the actual samples below: they establish what the labels mean and how narrow the language is.

`generate_telemetry_dataset(num_samples)` sets a fixed NumPy seed, samples severity with specified class probabilities, then generates rainfall/water/wind/temperature conditioned on that label. Occasional feature replacement simulates missing values with the same simple defaults used by inference. Returned X has shape `[num_samples,4]` in the exact documented feature order; y has shape `[num_samples]`. This construction creates easy learnable label-feature structure by design. Holding out rows from it does not validate natural hazard severity.

`train_and_evaluate` creates a timestamp version directory and copies previous deployed artifacts/evaluation before writing replacements. The text split uses random seed 42, stratification and 20% held-out rows: 40 train and 10 test. The Pipeline learns unigram/bigram TF-IDF with English stop words on train only, followed by class-balanced Logistic Regression. Fitting preprocessing inside the pipeline prevents train/test IDF leakage. It then predicts the test set and records macro F1/per-class report. The deployed preprocessing and classifier are serialized together.

The severity split uses 1,500 generated rows with a 300-row stratified holdout. RandomForestClassifier has 100 trees and maximum depth eight. It predicts the holdout, computes reports and saves the estimator. Its test F1 is a generator-recovery result, not an independent physical target score. A separate validation split/hyperparameter search is deliberately absent: ten text test examples cannot support a credible extensive tuning procedure. Do not repeatedly tune against this fixed test set and still call it untouched.

Report generation records actual class-label ordering, confusion matrices, macro/weighted metrics, accuracy/support, a most-frequent DummyClassifier baseline on the same split, seed, library version, timestamp version and artifact hashes. The version directory retains both current and previous files; compatible legacy filenames keep inference loading unchanged. The evaluation JSON is small tracked evidence; binary joblib files remain ignored. Reproduction from a clean checkout requires training because merely tracking a report does not ship an estimator.

Text scoring costs sparse O(K nnz(x)) per observation; weights need O(KV) storage. Forest inference walks up to T depth-d trees, O(Td), with node-count storage. Neither needs GPU for this dataset. Random train/test splitting controls only row allocation, not independence between semantically related examples. A useful next dataset must define independently collected targets, preserve licensing/provenance and split by event/time/template group.

Inspect the full matrix rather than only one F1: current fire recall is zero and text accuracy is 0.50. Synthetic severity accuracy near one is unsurprising given label-conditioned features. To modify safely, preserve the held-out policy, compare identical-split baselines, version artifacts, restart consumer/API to refresh caches and rerun actual inference plus persistence evidence. Never overwrite the interpretation of historical metrics with a score from a different vectorizer.

<!-- SOURCE_REFERENCE: generated; edit prose above, refresh with scripts/sync_source_reference.py -->

## Exact source reference

These numbered snapshots are synchronized to the source hashes below. They are reference text, not executable replacements. Open the source for editing; run the refresh command after changes. Imports are listed so dependency boundaries remain visible.

### `src/ml/train.py`

[Open source](../../src/ml/train.py) · SHA-256 `24092057a4a48788967b85fe35d0bbe2d54bd6fd28bd133ce25bd8e069b8a375`

Dependencies: `import os`; `import json`; `import hashlib`; `import shutil`; `from datetime import datetime, timezone`; `from pathlib import Path`; `import joblib`; `import numpy as np`; `from sklearn.feature_extraction.text import TfidfVectorizer`; `from sklearn.linear_model import LogisticRegression`; `from sklearn.ensemble import RandomForestClassifier`; `from sklearn.pipeline import Pipeline`; `from sklearn.model_selection import train_test_split`; `from sklearn.metrics import classification_report, f1_score, confusion_matrix`; `from sklearn.dummy import DummyClassifier`

- `generate_telemetry_dataset`: lines 199–241. Generates synthetic physics telemetry samples for severity prediction.

- `train_and_evaluate`: lines 244–367. Read its control flow with the module guide above.


```text
0001 | import os
0002 | import json
0003 | import hashlib
0004 | import shutil
0005 | from datetime import datetime, timezone
0006 | from pathlib import Path
0007 | import joblib
0008 | import numpy as np
0009 | from sklearn.feature_extraction.text import TfidfVectorizer
0010 | from sklearn.linear_model import LogisticRegression
0011 | from sklearn.ensemble import RandomForestClassifier
0012 | from sklearn.pipeline import Pipeline
0013 | from sklearn.model_selection import train_test_split
0014 | from sklearn.metrics import classification_report, f1_score, confusion_matrix
0015 | from sklearn.dummy import DummyClassifier
0016 | 
0017 | MODELS_DIR = os.path.join(
0018 |     os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "models"
0019 | )
0020 | os.makedirs(MODELS_DIR, exist_ok=True)
0021 | 
0022 | # 1. Training data for Disaster Type Classification
0023 | TEXT_SAMPLES = [
0024 |     # Floods
0025 |     (
0026 |         "Massive flooding after river banks breached, submerging residential areas.",
0027 |         "flood",
0028 |     ),
0029 |     ("Water entering ground floor apartments, flash flood warning issued.", "flood"),
0030 |     (
0031 |         "Severe waterlogging on arterial roads, rescue boats deployed in low lying areas.",
0032 |         "flood",
0033 |     ),
0034 |     (
0035 |         "Continuous rainfall of 250mm has flooded the city center and subway stations.",
0036 |         "flood",
0037 |     ),
0038 |     (
0039 |         "Dam overflow alert, emergency evacuation ordered for villages downstream.",
0040 |         "flood",
0041 |     ),
0042 |     (
0043 |         "Rising water levels in river reach danger mark, residents fleeing homes.",
0044 |         "flood",
0045 |     ),
0046 |     (
0047 |         "Torrential monsoon rains trigger flash floods across multiple districts.",
0048 |         "flood",
0049 |     ),
0050 |     ("Houses submerged under 5 feet of muddy water after lake breached.", "flood"),
0051 |     (
0052 |         "Drainage failure leads to catastrophic neighborhood flooding and vehicle damage.",
0053 |         "flood",
0054 |     ),
0055 |     (
0056 |         "Search and rescue teams saving marooned villagers from rooftop using boats.",
0057 |         "flood",
0058 |     ),
0059 |     # Earthquakes
0060 |     (
0061 |         "Violent tremors felt across the region, multiple building collapses reported.",
0062 |         "earthquake",
0063 |     ),
0064 |     (
0065 |         "Magnitude 6.8 earthquake strikes with severe shaking, cracks in bridges.",
0066 |         "earthquake",
0067 |     ),
0068 |     (
0069 |         "Aftershocks continue to rock the city, high-rise buildings evacuated.",
0070 |         "earthquake",
0071 |     ),
0072 |     (
0073 |         "Ground shaking violently, walls cracked, chimneys and roofs caved in.",
0074 |         "earthquake",
0075 |     ),
0076 |     (
0077 |         "Seismic shockwave triggered landslides blocking major mountain highways.",
0078 |         "earthquake",
0079 |     ),
0080 |     (
0081 |         "Residents rushed out into open streets in panic as multi-story buildings swayed.",
0082 |         "earthquake",
0083 |     ),
0084 |     (
0085 |         "Significant structural damage reported at hospital and schools following quake.",
0086 |         "earthquake",
0087 |     ),
0088 |     (
0089 |         "Underground fault slip produces severe ground fissures and pipe ruptures.",
0090 |         "earthquake",
0091 |     ),
0092 |     (
0093 |         "Tremors measuring 5.9 on Richter scale cause masonry to collapse on cars.",
0094 |         "earthquake",
0095 |     ),
0096 |     ("Epicenter located 20km north, tremors felt across a 300km radius.", "earthquake"),
0097 |     # Cyclones
0098 |     (
0099 |         "Severe cyclonic storm making landfall with wind gusts up to 180 km/h.",
0100 |         "cyclone",
0101 |     ),
0102 |     (
0103 |         "Heavy storm surge inundates coastal villages, uprooting trees and towers.",
0104 |         "cyclone",
0105 |     ),
0106 |     ("Super cyclone approaching coast, category 4 storm alerts activated.", "cyclone"),
0107 |     (
0108 |         "Fierce gales and relentless rain destroying tin roofs and power transmission.",
0109 |         "cyclone",
0110 |     ),
0111 |     (
0112 |         "Coastal seawalls damaged by massive 5-meter waves driven by tropical storm.",
0113 |         "cyclone",
0114 |     ),
0115 |     (
0116 |         "High velocity winds knocking down electrical poles and blocking traffic.",
0117 |         "cyclone",
0118 |     ),
0119 |     (
0120 |         "Depression intensified into deep cyclonic storm heading directly toward port.",
0121 |         "cyclone",
0122 |     ),
0123 |     ("Harbor battered by gale force winds, fishing boats wrecked at jetty.", "cyclone"),
0124 |     (
0125 |         "Tornado-like vortex ripped through settlement during severe cyclonic weather.",
0126 |         "cyclone",
0127 |     ),
0128 |     ("Hurricane force winds tear off signboards and shatter glass facades.", "cyclone"),
0129 |     # Wildfires
0130 |     (
0131 |         "Wildfire raging through pine forest, rapidly spreading toward residential zones.",
0132 |         "fire",
0133 |     ),
0134 |     ("Massive blaze engulfs chemical storage facility with thick toxic smoke.", "fire"),
0135 |     ("Dry brush fire out of control due to 45C heatwave and shifting winds.", "fire"),
0136 |     ("Firefighters battling raging inferno threatening hillside communities.", "fire"),
0137 |     ("Flames leaping over highways, zero visibility due to dense smoke plume.", "fire"),
0138 |     (
0139 |         "Emergency services deploying water-bomber planes to contain forest fire.",
0140 |         "fire",
0141 |     ),
0142 |     (
0143 |         "Industrial plant fire triggers series of loud explosions and evacuation orders.",
0144 |         "fire",
0145 |     ),
0146 |     (
0147 |         "Scorched earth and charred trees as brushfire burns over 10,000 hectares.",
0148 |         "fire",
0149 |     ),
0150 |     ("Commercial market caught fire, dozens of shops completely gutted.", "fire"),
0151 |     (
0152 |         "Smoldering embers carried by wind spark secondary fires in suburban park.",
0153 |         "fire",
0154 |     ),
0155 |     # Infrastructure Failures
0156 |     (
0157 |         "Major suspension bridge collapsed into river carrying vehicles during peak hour.",
0158 |         "infrastructure",
0159 |     ),
0160 |     (
0161 |         "Main power substation explosion causes city-wide blackout across metro grid.",
0162 |         "infrastructure",
0163 |     ),
0164 |     (
0165 |         "Underground high-pressure gas pipeline rupture causes massive crater on road.",
0166 |         "infrastructure",
0167 |     ),
0168 |     (
0169 |         "Overpass flyover structure developed massive structural crack, closed indefinitely.",
0170 |         "infrastructure",
0171 |     ),
0172 |     (
0173 |         "Municipal water main burst flooding train tracks and cutting water to 100k homes.",
0174 |         "infrastructure",
0175 |     ),
0176 |     (
0177 |         "Railway track buckling under extreme heat causes derailment of cargo train.",
0178 |         "infrastructure",
0179 |     ),
0180 |     (
0181 |         "Dam spillway gate failure threatens catastrophic structural collapse.",
0182 |         "infrastructure",
0183 |     ),
0184 |     (
0185 |         "Telecommunication tower collapsed on highway during utility repair work.",
0186 |         "infrastructure",
0187 |     ),
0188 |     (
0189 |         "Sewage treatment plant equipment failure spilling untreated effluent into bay.",
0190 |         "infrastructure",
0191 |     ),
0192 |     (
0193 |         "Highway tunnel cave-in traps maintenance vehicles, emergency rescue underway.",
0194 |         "infrastructure",
0195 |     ),
0196 | ]
0197 | 
0198 | 
0199 | def generate_telemetry_dataset(num_samples: int = 1000):
0200 |     """Generates synthetic physics telemetry samples for severity prediction."""
0201 |     np.random.seed(42)
0202 |     X = []
0203 |     y = []
0204 | 
0205 |     for _ in range(num_samples):
0206 |         sev_choice = np.random.choice(
0207 |             ["minor", "moderate", "severe", "critical"], p=[0.3, 0.3, 0.25, 0.15]
0208 |         )
0209 | 
0210 |         if sev_choice == "minor":
0211 |             rainfall = np.random.uniform(0, 30)
0212 |             water_level = np.random.uniform(0, 1.0)
0213 |             wind_speed = np.random.uniform(5, 35)
0214 |             temp = np.random.uniform(20, 35)
0215 |         elif sev_choice == "moderate":
0216 |             rainfall = np.random.uniform(25, 80)
0217 |             water_level = np.random.uniform(0.8, 2.5)
0218 |             wind_speed = np.random.uniform(30, 70)
0219 |             temp = np.random.uniform(22, 38)
0220 |         elif sev_choice == "severe":
0221 |             rainfall = np.random.uniform(70, 180)
0222 |             water_level = np.random.uniform(2.0, 5.0)
0223 |             wind_speed = np.random.uniform(65, 120)
0224 |             temp = np.random.uniform(15, 42)
0225 |         else:  # critical
0226 |             rainfall = np.random.uniform(150, 400)
0227 |             water_level = np.random.uniform(4.5, 10.0)
0228 |             wind_speed = np.random.uniform(110, 220)
0229 |             temp = np.random.uniform(38, 50)
0230 | 
0231 |         # Impute missing values occasionally (simulating real sensor drops)
0232 |         features = [
0233 |             rainfall if np.random.rand() > 0.05 else 0.0,
0234 |             water_level if np.random.rand() > 0.05 else 0.0,
0235 |             wind_speed if np.random.rand() > 0.05 else 0.0,
0236 |             temp if np.random.rand() > 0.05 else 25.0,
0237 |         ]
0238 |         X.append(features)
0239 |         y.append(sev_choice)
0240 | 
0241 |     return np.array(X), np.array(y)
0242 | 
0243 | 
0244 | def train_and_evaluate():
0245 |     version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
0246 |     version_dir = Path(MODELS_DIR, "versions", version)
0247 |     version_dir.mkdir(parents=True)
0248 |     for filename in (
0249 |         "disaster_classifier.joblib",
0250 |         "severity_predictor.joblib",
0251 |         "evaluation.json",
0252 |     ):
0253 |         previous = Path(MODELS_DIR, filename)
0254 |         if previous.exists():
0255 |             shutil.copy2(previous, version_dir / ("previous-" + filename))
0256 |     print("==================================================")
0257 |     print(" Cataclysm ML Pipeline Training & Evaluation")
0258 |     print("==================================================")
0259 | 
0260 |     # 1. Train Text Classifier
0261 |     texts, labels = zip(*TEXT_SAMPLES)
0262 |     X_train_t, X_test_t, y_train_t, y_test_t = train_test_split(
0263 |         texts, labels, test_size=0.2, random_state=42, stratify=labels
0264 |     )
0265 | 
0266 |     text_clf = Pipeline(
0267 |         [
0268 |             (
0269 |                 "tfidf",
0270 |                 TfidfVectorizer(ngram_range=(1, 2), stop_words="english", min_df=1),
0271 |             ),
0272 |             (
0273 |                 "clf",
0274 |                 LogisticRegression(
0275 |                     class_weight="balanced", max_iter=200, random_state=42
0276 |                 ),
0277 |             ),
0278 |         ]
0279 |     )
0280 | 
0281 |     text_clf.fit(X_train_t, y_train_t)
0282 |     y_pred_t = text_clf.predict(X_test_t)
0283 |     f1_text = f1_score(y_test_t, y_pred_t, average="macro")
0284 | 
0285 |     print(f"\n[Text Classifier] Macro F1 Score: {f1_text:.4f}")
0286 |     print(classification_report(y_test_t, y_pred_t))
0287 | 
0288 |     text_model_path = os.path.join(MODELS_DIR, "disaster_classifier.joblib")
0289 |     joblib.dump(text_clf, text_model_path)
0290 |     print(f"Saved Text Classifier to: {text_model_path}")
0291 | 
0292 |     # 2. Train Severity Predictor
0293 |     X_telem, y_telem = generate_telemetry_dataset(num_samples=1500)
0294 |     X_train_s, X_test_s, y_train_s, y_test_s = train_test_split(
0295 |         X_telem, y_telem, test_size=0.2, random_state=42, stratify=y_telem
0296 |     )
0297 | 
0298 |     sev_clf = RandomForestClassifier(
0299 |         n_estimators=100, max_depth=8, random_state=42, class_weight="balanced"
0300 |     )
0301 |     sev_clf.fit(X_train_s, y_train_s)
0302 |     y_pred_s = sev_clf.predict(X_test_s)
0303 |     f1_sev = f1_score(y_test_s, y_pred_s, average="macro")
0304 | 
0305 |     print(f"\n[Severity Predictor] Macro F1 Score: {f1_sev:.4f}")
0306 |     print(classification_report(y_test_s, y_pred_s))
0307 | 
0308 |     sev_model_path = os.path.join(MODELS_DIR, "severity_predictor.joblib")
0309 |     joblib.dump(sev_clf, sev_model_path)
0310 |     print(f"Saved Severity Predictor to: {sev_model_path}")
0311 | 
0312 |     print("\nTraining complete. Models serialized successfully.")
0313 |     report = {
0314 |         "version": version,
0315 |         "trained_at": datetime.now(timezone.utc).isoformat(),
0316 |         "seed": 42,
0317 |         "text_corpus_size": len(TEXT_SAMPLES),
0318 |         "text_test_size": len(y_test_t),
0319 |         "text_macro_f1": f1_text,
0320 |         "text_labels": text_clf.classes_.tolist(),
0321 |         "text_confusion_matrix": confusion_matrix(
0322 |             y_test_t, y_pred_t, labels=text_clf.classes_
0323 |         ).tolist(),
0324 |         "text_majority_baseline_macro_f1": f1_score(
0325 |             y_test_t,
0326 |             DummyClassifier(strategy="most_frequent")
0327 |             .fit(X_train_t, y_train_t)
0328 |             .predict(X_test_t),
0329 |             average="macro",
0330 |             zero_division=0,
0331 |         ),
0332 |         "text_classification_report": classification_report(
0333 |             y_test_t, y_pred_t, output_dict=True, zero_division=0
0334 |         ),
0335 |         "severity_macro_f1": f1_sev,
0336 |         "severity_labels": sev_clf.classes_.tolist(),
0337 |         "severity_confusion_matrix": confusion_matrix(
0338 |             y_test_s, y_pred_s, labels=sev_clf.classes_
0339 |         ).tolist(),
0340 |         "severity_majority_baseline_macro_f1": f1_score(
0341 |             y_test_s,
0342 |             DummyClassifier(strategy="most_frequent")
0343 |             .fit(X_train_s, y_train_s)
0344 |             .predict(X_test_s),
0345 |             average="macro",
0346 |             zero_division=0,
0347 |         ),
0348 |         "severity_classification_report": classification_report(
0349 |             y_test_s, y_pred_s, output_dict=True, zero_division=0
0350 |         ),
0351 |         "limitations": "Small curated text corpus; synthetic severity labels. Scores do not establish real-world accuracy. Probabilities are uncalibrated.",
0352 |     }
0353 |     import sklearn
0354 | 
0355 |     report["sklearn_version"] = sklearn.__version__
0356 |     report["artifact_sha256"] = {
0357 |         Path(path).name: hashlib.sha256(Path(path).read_bytes()).hexdigest()
0358 |         for path in (text_model_path, sev_model_path)
0359 |     }
0360 |     for path in (text_model_path, sev_model_path):
0361 |         shutil.copy2(path, version_dir / Path(path).name)
0362 |     (version_dir / "evaluation.json").write_text(
0363 |         json.dumps(report, indent=2), encoding="utf-8"
0364 |     )
0365 |     Path(MODELS_DIR, "evaluation.json").write_text(
0366 |         json.dumps(report, indent=2), encoding="utf-8"
0367 |     )
0368 | 
0369 | 
0370 | if __name__ == "__main__":
0371 |     train_and_evaluate()
```
