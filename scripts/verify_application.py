"""Verify the running API, persistence identity, SSE and monitoring contracts."""

from __future__ import annotations
import json
import time
import uuid
from pathlib import Path
from datetime import datetime, timezone
import requests
from confluent_kafka import Consumer, TopicPartition
from src.common.config import config
from src.common.db import get_pool


def main():
    base = "http://localhost:8000"

    def get(path, **kwargs):
        r = requests.get(base + path, timeout=15, **kwargs)
        r.raise_for_status()
        return r.json()

    deadline = time.monotonic() + 45
    while True:
        try:
            if get("/live")["status"] == "alive":
                break
        except requests.RequestException:
            if time.monotonic() >= deadline:
                raise
        time.sleep(1)
    assert get("/ready")["status"] == "healthy"
    assert (
        requests.post(
            base + "/events", json={"event_id": "invalid"}, timeout=5
        ).status_code
        == 422
    )
    assert (
        requests.get(base + "/api/events?bbox=10,0,-10,1", timeout=5).status_code == 422
    )
    assert requests.get(base + "/api/events?limit=501", timeout=5).status_code == 422
    assert get("/api/events?q=THIS-WILL-NOT-MATCH-" + uuid.uuid4().hex) == []
    summary = get("/api/overview")
    assert summary["total"] == sum(x["count"] for x in summary["severity"])
    assert summary["total"] == sum(x["count"] for x in summary["sources"])
    assert summary["trends"] and all(row["count"] > 0 for row in summary["trends"])
    first = get("/api/events?limit=3")
    second = get("/api/events?limit=3&offset=3")
    assert not ({x["event_id"] for x in first} & {x["event_id"] for x in second})
    usgs = get("/api/events?source=usgs_feed&limit=5")
    assert usgs and all(
        x["source"] == "usgs_feed" and x["data_mode"] == "live" for x in usgs
    )
    identifier = "IDENTITY-" + uuid.uuid4().hex[:16]
    event = dict(
        event_id=identifier,
        timestamp=datetime.now(timezone.utc).isoformat(),
        location="Identity test",
        latitude=0,
        longitude=0,
        text="Original synthetic flood",
        disaster_type="flood",
        severity="minor",
        source="synthetic_verification",
        metadata={
            "is_synthetic": True,
            "location_accuracy": "Controlled zero-coordinate fixture",
        },
    )
    r = requests.post(base + "/events", json=event, timeout=10)
    r.raise_for_status()
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        r = requests.get(base + f"/api/events/{identifier}/details", timeout=5)
        if r.ok:
            break
        time.sleep(0.5)
    r.raise_for_status()
    conflict = {**event, "text": "Conflicting fire narrative", "severity": "critical"}
    r = requests.post(base + "/events", json=conflict, timeout=10)
    r.raise_for_status()
    ack = r.json()
    observer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "crisis-dashboard-group",
            "enable.auto.commit": False,
        }
    )
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        committed = observer.committed(
            [TopicPartition(config.KAFKA_TOPIC, ack["partition"])], timeout=3
        )[0].offset
        if committed > ack["offset"]:
            break
        time.sleep(0.5)
    observer.close()
    assert committed > ack["offset"]
    detail = get(f"/api/events/{identifier}/details")
    assert detail["text"] == event["text"] and detail["severity"] == "minor"
    assert detail["data_mode"] == "synthetic" and len(detail["predictions"]) == 2
    import io
    import pyarrow.parquet as pq
    from src.storage.lake_writer import ParquetLakeWriter

    writer = ParquetLakeWriter()
    _, _, prefix = writer._partition(event)
    body = writer._client().get_object(
        Bucket=config.MINIO_BUCKET_RAW, Key=f"{prefix}/event_id={identifier}.parquet"
    )["Body"]
    try:
        archived = pq.ParquetFile(io.BytesIO(body.read())).read().to_pylist()[0]
    finally:
        body.close()
    assert json.loads(archived["raw_payload"])["text"] == event["text"]
    assert archived["severity"] == "minor"
    # Read a real SSE event and close the HTTP connection deterministically.
    with requests.get(
        base + "/api/events/stream", stream=True, timeout=(5, 15)
    ) as response:
        response.raise_for_status()
        saw_event = False
        for line in response.iter_lines(chunk_size=1, decode_unicode=True):
            if line.startswith("event: crisis_event"):
                saw_event = True
            if saw_event and line.startswith("data: "):
                streamed = json.loads(line[6:])
                assert "event_id" in streamed
                break
        assert saw_event
    targets = requests.get("http://prometheus:9090/api/v1/targets", timeout=10).json()[
        "data"
    ]["activeTargets"]
    assert targets and all(x["health"] == "up" for x in targets)
    grafana = requests.get("http://grafana:3000/api/health", timeout=10).json()
    assert grafana["database"] == "ok"
    pipeline = get("/api/pipeline")
    with get_pool().connection() as conn:
        plan = conn.execute(
            "EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM crisis_events ORDER BY timestamp DESC LIMIT 100"
        ).fetchone()
    report = dict(
        observed_at=datetime.now(timezone.utc).isoformat(),
        invalid_payload_rejected=True,
        invalid_bbox_rejected=True,
        pagination_disjoint=True,
        overview_consistent=True,
        live_usgs_rows=len(usgs),
        identity_event=identifier,
        first_observation_wins=True,
        raw_first_observation_wins=True,
        sse_database_event_received=True,
        prometheus_targets=[
            {"job": x["labels"].get("job"), "health": x["health"]} for x in targets
        ],
        grafana_database=grafana["database"],
        pipeline=pipeline,
        query_plan=plan,
    )
    Path("docs/evidence/application.json").write_text(
        json.dumps(report, indent=2, default=str)
    )
    print(json.dumps({k: v for k, v in report.items() if k != "query_plan"}, indent=2))


if __name__ == "__main__":
    main()
