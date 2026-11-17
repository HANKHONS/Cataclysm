"""Compare text classifiers on exactly the same small held-out corpus."""

import json
import time
import argparse
from pathlib import Path
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from src.ml.train import TEXT_SAMPLES
from src.ml.train import generate_telemetry_dataset


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    texts, labels = zip(*TEXT_SAMPLES)
    train, test, y_train, y_test = train_test_split(
        texts, labels, stratify=labels, test_size=0.2, random_state=42
    )
    results = {}
    estimators = [
        ("logistic_regression", LogisticRegression(max_iter=300, random_state=42)),
        ("random_forest", RandomForestClassifier(n_estimators=100, random_state=42)),
    ]
    skipped = []
    try:
        from xgboost import XGBClassifier

        estimators.append(
            (
                "xgboost",
                XGBClassifier(n_estimators=100, max_depth=4, n_jobs=2, random_state=42),
            )
        )
    except ImportError:
        skipped.append("xgboost: install requirements-experiments.txt")
    for name, estimator in estimators:
        model = Pipeline(
            [("tfidf", TfidfVectorizer(ngram_range=(1, 2))), ("model", estimator)]
        )
        start = time.perf_counter()
        encoder = LabelEncoder().fit(y_train)
        model.fit(train, encoder.transform(y_train) if name == "xgboost" else y_train)
        train_seconds = time.perf_counter() - start
        start = time.perf_counter()
        predictions = model.predict(test)
        if name == "xgboost":
            predictions = encoder.inverse_transform(predictions.astype(int))
        results[name] = {
            "training_seconds": train_seconds,
            "inference_seconds": time.perf_counter() - start,
            "report": classification_report(
                y_test, predictions, output_dict=True, zero_division=0
            ),
        }
    features, severity = generate_telemetry_dataset(1500)
    x_train, x_test, s_train, s_test = train_test_split(
        features, severity, stratify=severity, test_size=0.2, random_state=42
    )
    severity_estimators = [
        (
            "random_forest",
            RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42),
        )
    ]
    if not skipped:
        severity_estimators.append(
            (
                "xgboost",
                XGBClassifier(n_estimators=100, max_depth=4, n_jobs=2, random_state=42),
            )
        )
    severity_results = {}
    encoder = LabelEncoder().fit(s_train)
    for name, estimator in severity_estimators:
        start = time.perf_counter()
        estimator.fit(
            x_train, encoder.transform(s_train) if name == "xgboost" else s_train
        )
        train_seconds = time.perf_counter() - start
        start = time.perf_counter()
        predicted = estimator.predict(x_test)
        elapsed = time.perf_counter() - start
        if name == "xgboost":
            predicted = encoder.inverse_transform(predicted.astype(int))
        severity_results[name] = {
            "training_seconds": train_seconds,
            "inference_seconds": elapsed,
            "report": classification_report(
                s_test, predicted, output_dict=True, zero_division=0
            ),
        }
    report = json.dumps(
        {
            "results": results,
            "severity_results": severity_results,
            "test_examples": len(test),
            "skipped": skipped,
            "limitations": "Small curated corpus; no claim of deployment accuracy.",
        },
        indent=2,
    )
    if args.output:
        args.output.write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
