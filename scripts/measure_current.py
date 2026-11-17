"""Repeat bounded broker-acknowledgment measurements and record runtime context."""

import importlib.metadata
import json
import platform
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

reports = []
for index in range(3):
    output = subprocess.check_output(
        [
            sys.executable,
            "benchmarks/kafka_load.py",
            "--count",
            "100",
            "--concurrency",
            "4",
            "--endpoint",
            "http://127.0.0.1:8000/events",
        ],
        text=True,
    )
    reports.append(json.loads(output))
rates = [r["accepted_per_second"] for r in reports]
report = {
    "observed_at": datetime.now(timezone.utc).isoformat(),
    "runtime": platform.platform(),
    "python": platform.python_version(),
    "versions": {
        p: importlib.metadata.version(p)
        for p in [
            "fastapi",
            "confluent-kafka",
            "psycopg",
            "pyarrow",
            "scikit-learn",
            "pandas",
        ]
    },
    "policy": "One preceding 100-event warm-up; three sequential measured 100-event runs; concurrency 4; no pauses; one location key. Existing browser and streaming workloads remain active.",
    "runs": reports,
    "ack_rate_mean": statistics.mean(rates),
    "ack_rate_sample_stddev": statistics.stdev(rates),
    "scope": "Container loopback HTTP-to-Kafka acknowledgment only; no sustained-capacity or end-to-end latency claim.",
}
Path("docs/evidence/benchmark-current.json").write_text(json.dumps(report, indent=2))
print(json.dumps(report, indent=2))
