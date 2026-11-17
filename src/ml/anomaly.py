"""Fit and apply a transparent Isolation Forest report-rate anomaly detector."""

from __future__ import annotations

import numpy as np
from sklearn.ensemble import IsolationForest


def train_report_rate_detector(
    report_counts: list[float], contamination: float = 0.05
) -> IsolationForest:
    """Train on historical fixed-window report counts, shape [n_windows, 1]."""
    if len(report_counts) < 20:
        raise ValueError("at least 20 historical windows are required")
    values = np.asarray(report_counts, dtype=float)
    if not np.isfinite(values).all() or (values < 0).any():
        raise ValueError("report counts must be finite and nonnegative")
    model = IsolationForest(contamination=contamination, random_state=42).fit(
        values.reshape(-1, 1)
    )
    model.training_count_max_ = float(values.max())
    return model


def anomaly_scores(
    model: IsolationForest, report_counts: list[float]
) -> list[dict[str, float | bool]]:
    values = np.asarray(report_counts).reshape(-1, 1)
    labels = model.predict(values)
    scores = model.decision_function(values)
    # Isolation trees cannot extrapolate beyond their largest training split:
    # arbitrarily large spikes may tie at a boundary leaf. Report a separate,
    # explicit training-range guard instead of misrepresenting that as ML skill.
    return [
        {
            "report_count": float(value[0]),
            "score": float(score),
            "outside_training_range": bool(value[0] > model.training_count_max_),
            "is_anomaly": bool(label == -1 or value[0] > model.training_count_max_),
        }
        for value, score, label in zip(values, scores, labels)
    ]
