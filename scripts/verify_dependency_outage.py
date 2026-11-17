"""Brief reversible DB/broker outage rehearsal. Run on host, not in a container."""

import json
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
import requests

BASE = "http://127.0.0.1:8000"


def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True)


def wait_ready():
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            if requests.get(BASE + "/ready", timeout=10).status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError("Service did not recover")


def main():
    report = {"observed_at": datetime.now(timezone.utc).isoformat()}
    try:
        compose("stop", "db")
        report["database_outage_events_status"] = requests.get(
            BASE + "/api/events", timeout=15
        ).status_code
        report["database_outage_ready_status"] = requests.get(
            BASE + "/ready", timeout=15
        ).status_code
        assert (
            report["database_outage_events_status"] == 503
            and report["database_outage_ready_status"] == 503
        )
    finally:
        compose("start", "db")
        wait_ready()
    event = dict(
        event_id="OUTAGE-" + uuid.uuid4().hex[:16],
        timestamp=datetime.now(timezone.utc).isoformat(),
        text="Synthetic broker outage fixture",
        location="Outage test",
        latitude=0,
        longitude=0,
        disaster_type="flood",
        severity="minor",
        source="synthetic_outage",
        metadata={"is_synthetic": True},
    )
    try:
        compose("stop", "broker")
        report["broker_outage_ingest_status"] = requests.post(
            BASE + "/events", json=event, timeout=15
        ).status_code
        assert report["broker_outage_ingest_status"] == 503
    finally:
        compose("start", "broker")
        wait_ready()
    response = requests.post(BASE + "/events", json=event, timeout=15)
    response.raise_for_status()
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        r = requests.get(BASE + f"/api/events/{event['event_id']}/details", timeout=10)
        if r.ok:
            break
        time.sleep(1)
    r.raise_for_status()
    report.update(
        event_id=event["event_id"],
        same_id_retry_persisted=True,
        recovered_readiness=True,
        note="503 acknowledgment timeout is uncertain acceptance; retry uses original identity. Services restored in finally blocks.",
    )
    Path("docs/evidence/dependency-outages.json").write_text(
        json.dumps(report, indent=2)
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
