import pytest
from src.ml.anomaly import train_report_rate_detector, anomaly_scores


def test_anomaly_requires_history():
    with pytest.raises(ValueError):
        train_report_rate_detector([1, 2])


def test_report_spike_is_anomalous():
    model = train_report_rate_detector([8, 9, 10, 11, 12] * 20)
    result = anomaly_scores(model, [10, 1000])
    assert not result[0]["is_anomaly"]
    assert result[1]["is_anomaly"]
