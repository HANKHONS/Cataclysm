import os
import json
import hashlib
import shutil
from datetime import datetime, timezone
from pathlib import Path
import joblib
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, f1_score, confusion_matrix
from sklearn.dummy import DummyClassifier

MODELS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "models"
)
os.makedirs(MODELS_DIR, exist_ok=True)

# 1. Training data for Disaster Type Classification
TEXT_SAMPLES = [
    # Floods
    (
        "Massive flooding after river banks breached, submerging residential areas.",
        "flood",
    ),
    ("Water entering ground floor apartments, flash flood warning issued.", "flood"),
    (
        "Severe waterlogging on arterial roads, rescue boats deployed in low lying areas.",
        "flood",
    ),
    (
        "Continuous rainfall of 250mm has flooded the city center and subway stations.",
        "flood",
    ),
    (
        "Dam overflow alert, emergency evacuation ordered for villages downstream.",
        "flood",
    ),
    (
        "Rising water levels in river reach danger mark, residents fleeing homes.",
        "flood",
    ),
    (
        "Torrential monsoon rains trigger flash floods across multiple districts.",
        "flood",
    ),
    ("Houses submerged under 5 feet of muddy water after lake breached.", "flood"),
    (
        "Drainage failure leads to catastrophic neighborhood flooding and vehicle damage.",
        "flood",
    ),
    (
        "Search and rescue teams saving marooned villagers from rooftop using boats.",
        "flood",
    ),
    # Earthquakes
    (
        "Violent tremors felt across the region, multiple building collapses reported.",
        "earthquake",
    ),
    (
        "Magnitude 6.8 earthquake strikes with severe shaking, cracks in bridges.",
        "earthquake",
    ),
    (
        "Aftershocks continue to rock the city, high-rise buildings evacuated.",
        "earthquake",
    ),
    (
        "Ground shaking violently, walls cracked, chimneys and roofs caved in.",
        "earthquake",
    ),
    (
        "Seismic shockwave triggered landslides blocking major mountain highways.",
        "earthquake",
    ),
    (
        "Residents rushed out into open streets in panic as multi-story buildings swayed.",
        "earthquake",
    ),
    (
        "Significant structural damage reported at hospital and schools following quake.",
        "earthquake",
    ),
    (
        "Underground fault slip produces severe ground fissures and pipe ruptures.",
        "earthquake",
    ),
    (
        "Tremors measuring 5.9 on Richter scale cause masonry to collapse on cars.",
        "earthquake",
    ),
    ("Epicenter located 20km north, tremors felt across a 300km radius.", "earthquake"),
    # Cyclones
    (
        "Severe cyclonic storm making landfall with wind gusts up to 180 km/h.",
        "cyclone",
    ),
    (
        "Heavy storm surge inundates coastal villages, uprooting trees and towers.",
        "cyclone",
    ),
    ("Super cyclone approaching coast, category 4 storm alerts activated.", "cyclone"),
    (
        "Fierce gales and relentless rain destroying tin roofs and power transmission.",
        "cyclone",
    ),
    (
        "Coastal seawalls damaged by massive 5-meter waves driven by tropical storm.",
        "cyclone",
    ),
    (
        "High velocity winds knocking down electrical poles and blocking traffic.",
        "cyclone",
    ),
    (
        "Depression intensified into deep cyclonic storm heading directly toward port.",
        "cyclone",
    ),
    ("Harbor battered by gale force winds, fishing boats wrecked at jetty.", "cyclone"),
    (
        "Tornado-like vortex ripped through settlement during severe cyclonic weather.",
        "cyclone",
    ),
    ("Hurricane force winds tear off signboards and shatter glass facades.", "cyclone"),
    # Wildfires
    (
        "Wildfire raging through pine forest, rapidly spreading toward residential zones.",
        "fire",
    ),
    ("Massive blaze engulfs chemical storage facility with thick toxic smoke.", "fire"),
    ("Dry brush fire out of control due to 45C heatwave and shifting winds.", "fire"),
    ("Firefighters battling raging inferno threatening hillside communities.", "fire"),
    ("Flames leaping over highways, zero visibility due to dense smoke plume.", "fire"),
    (
        "Emergency services deploying water-bomber planes to contain forest fire.",
        "fire",
    ),
    (
        "Industrial plant fire triggers series of loud explosions and evacuation orders.",
        "fire",
    ),
    (
        "Scorched earth and charred trees as brushfire burns over 10,000 hectares.",
        "fire",
    ),
    ("Commercial market caught fire, dozens of shops completely gutted.", "fire"),
    (
        "Smoldering embers carried by wind spark secondary fires in suburban park.",
        "fire",
    ),
    # Infrastructure Failures
    (
        "Major suspension bridge collapsed into river carrying vehicles during peak hour.",
        "infrastructure",
    ),
    (
        "Main power substation explosion causes city-wide blackout across metro grid.",
        "infrastructure",
    ),
    (
        "Underground high-pressure gas pipeline rupture causes massive crater on road.",
        "infrastructure",
    ),
    (
        "Overpass flyover structure developed massive structural crack, closed indefinitely.",
        "infrastructure",
    ),
    (
        "Municipal water main burst flooding train tracks and cutting water to 100k homes.",
        "infrastructure",
    ),
    (
        "Railway track buckling under extreme heat causes derailment of cargo train.",
        "infrastructure",
    ),
    (
        "Dam spillway gate failure threatens catastrophic structural collapse.",
        "infrastructure",
    ),
    (
        "Telecommunication tower collapsed on highway during utility repair work.",
        "infrastructure",
    ),
    (
        "Sewage treatment plant equipment failure spilling untreated effluent into bay.",
        "infrastructure",
    ),
    (
        "Highway tunnel cave-in traps maintenance vehicles, emergency rescue underway.",
        "infrastructure",
    ),
]


def generate_telemetry_dataset(num_samples: int = 1000):
    """Generates synthetic physics telemetry samples for severity prediction."""
    np.random.seed(42)
    X = []
    y = []

    for _ in range(num_samples):
        sev_choice = np.random.choice(
            ["minor", "moderate", "severe", "critical"], p=[0.3, 0.3, 0.25, 0.15]
        )

        if sev_choice == "minor":
            rainfall = np.random.uniform(0, 30)
            water_level = np.random.uniform(0, 1.0)
            wind_speed = np.random.uniform(5, 35)
            temp = np.random.uniform(20, 35)
        elif sev_choice == "moderate":
            rainfall = np.random.uniform(25, 80)
            water_level = np.random.uniform(0.8, 2.5)
            wind_speed = np.random.uniform(30, 70)
            temp = np.random.uniform(22, 38)
        elif sev_choice == "severe":
            rainfall = np.random.uniform(70, 180)
            water_level = np.random.uniform(2.0, 5.0)
            wind_speed = np.random.uniform(65, 120)
            temp = np.random.uniform(15, 42)
        else:  # critical
            rainfall = np.random.uniform(150, 400)
            water_level = np.random.uniform(4.5, 10.0)
            wind_speed = np.random.uniform(110, 220)
            temp = np.random.uniform(38, 50)

        # Impute missing values occasionally (simulating real sensor drops)
        features = [
            rainfall if np.random.rand() > 0.05 else 0.0,
            water_level if np.random.rand() > 0.05 else 0.0,
            wind_speed if np.random.rand() > 0.05 else 0.0,
            temp if np.random.rand() > 0.05 else 25.0,
        ]
        X.append(features)
        y.append(sev_choice)

    return np.array(X), np.array(y)


def train_and_evaluate():
    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    version_dir = Path(MODELS_DIR, "versions", version)
    version_dir.mkdir(parents=True)
    for filename in (
        "disaster_classifier.joblib",
        "severity_predictor.joblib",
        "evaluation.json",
    ):
        previous = Path(MODELS_DIR, filename)
        if previous.exists():
            shutil.copy2(previous, version_dir / ("previous-" + filename))
    print("==================================================")
    print(" Cataclysm ML Pipeline Training & Evaluation")
    print("==================================================")

    # 1. Train Text Classifier
    texts, labels = zip(*TEXT_SAMPLES)
    X_train_t, X_test_t, y_train_t, y_test_t = train_test_split(
        texts, labels, test_size=0.2, random_state=42, stratify=labels
    )

    text_clf = Pipeline(
        [
            (
                "tfidf",
                TfidfVectorizer(ngram_range=(1, 2), stop_words="english", min_df=1),
            ),
            (
                "clf",
                LogisticRegression(
                    class_weight="balanced", max_iter=200, random_state=42
                ),
            ),
        ]
    )

    text_clf.fit(X_train_t, y_train_t)
    y_pred_t = text_clf.predict(X_test_t)
    f1_text = f1_score(y_test_t, y_pred_t, average="macro")

    print(f"\n[Text Classifier] Macro F1 Score: {f1_text:.4f}")
    print(classification_report(y_test_t, y_pred_t))

    text_model_path = os.path.join(MODELS_DIR, "disaster_classifier.joblib")
    joblib.dump(text_clf, text_model_path)
    print(f"Saved Text Classifier to: {text_model_path}")

    # 2. Train Severity Predictor
    X_telem, y_telem = generate_telemetry_dataset(num_samples=1500)
    X_train_s, X_test_s, y_train_s, y_test_s = train_test_split(
        X_telem, y_telem, test_size=0.2, random_state=42, stratify=y_telem
    )

    sev_clf = RandomForestClassifier(
        n_estimators=100, max_depth=8, random_state=42, class_weight="balanced"
    )
    sev_clf.fit(X_train_s, y_train_s)
    y_pred_s = sev_clf.predict(X_test_s)
    f1_sev = f1_score(y_test_s, y_pred_s, average="macro")

    print(f"\n[Severity Predictor] Macro F1 Score: {f1_sev:.4f}")
    print(classification_report(y_test_s, y_pred_s))

    sev_model_path = os.path.join(MODELS_DIR, "severity_predictor.joblib")
    joblib.dump(sev_clf, sev_model_path)
    print(f"Saved Severity Predictor to: {sev_model_path}")

    print("\nTraining complete. Models serialized successfully.")
    report = {
        "version": version,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "seed": 42,
        "text_corpus_size": len(TEXT_SAMPLES),
        "text_test_size": len(y_test_t),
        "text_macro_f1": f1_text,
        "text_labels": text_clf.classes_.tolist(),
        "text_confusion_matrix": confusion_matrix(
            y_test_t, y_pred_t, labels=text_clf.classes_
        ).tolist(),
        "text_majority_baseline_macro_f1": f1_score(
            y_test_t,
            DummyClassifier(strategy="most_frequent")
            .fit(X_train_t, y_train_t)
            .predict(X_test_t),
            average="macro",
            zero_division=0,
        ),
        "text_classification_report": classification_report(
            y_test_t, y_pred_t, output_dict=True, zero_division=0
        ),
        "severity_macro_f1": f1_sev,
        "severity_labels": sev_clf.classes_.tolist(),
        "severity_confusion_matrix": confusion_matrix(
            y_test_s, y_pred_s, labels=sev_clf.classes_
        ).tolist(),
        "severity_majority_baseline_macro_f1": f1_score(
            y_test_s,
            DummyClassifier(strategy="most_frequent")
            .fit(X_train_s, y_train_s)
            .predict(X_test_s),
            average="macro",
            zero_division=0,
        ),
        "severity_classification_report": classification_report(
            y_test_s, y_pred_s, output_dict=True, zero_division=0
        ),
        "limitations": "Small curated text corpus; synthetic severity labels. Scores do not establish real-world accuracy. Probabilities are uncalibrated.",
    }
    import sklearn

    report["sklearn_version"] = sklearn.__version__
    report["artifact_sha256"] = {
        Path(path).name: hashlib.sha256(Path(path).read_bytes()).hexdigest()
        for path in (text_model_path, sev_model_path)
    }
    for path in (text_model_path, sev_model_path):
        shutil.copy2(path, version_dir / Path(path).name)
    (version_dir / "evaluation.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    Path(MODELS_DIR, "evaluation.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )


if __name__ == "__main__":
    train_and_evaluate()
