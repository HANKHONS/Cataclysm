"""Bounded local load experiment; records accepted/rejected latency separately."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
import time
import uuid
import requests


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--endpoint", default="http://127.0.0.1:8000/events")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 1 <= args.count <= 10000 or not 1 <= args.concurrency <= 16:
        parser.error("count must be 1..10000 and concurrency 1..16")

    def send(index):
        event = {
            "event_id": "LOAD-" + uuid.uuid4().hex,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "text": "Synthetic load test flood report",
            "location": "Benchmark",
            "disaster_type": "flood",
            "latitude": 12,
            "longitude": 77,
            "severity": "minor",
            "source": "synthetic_benchmark",
            "metadata": {"is_synthetic": True},
        }
        start = time.perf_counter()
        try:
            response = requests.post(args.endpoint, json=event, timeout=10)
            return response.status_code == 200, time.perf_counter() - start
        except requests.RequestException:
            return False, time.perf_counter() - start

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        results = list(executor.map(send, range(args.count)))
    duration = time.perf_counter() - start
    accepted = sorted(elapsed for ok, elapsed in results if ok)
    report = {
        "requested": args.count,
        "accepted": len(accepted),
        "rejected": args.count - len(accepted),
        "concurrency": args.concurrency,
        "wall_seconds": duration,
        "accepted_per_second": len(accepted) / duration,
        "accepted_p50_seconds": statistics.median(accepted) if accepted else None,
        "accepted_p95_seconds": accepted[
            min(len(accepted) - 1, int(len(accepted) * 0.95))
        ]
        if accepted
        else None,
        "scope": "HTTP-to-Kafka acknowledgement; downstream lag must be inspected separately",
    }
    output = json.dumps(report, indent=2)
    if args.output:
        args.output.write_text(output, encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
