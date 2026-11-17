"""Restart local durable services and verify existing operational IDs survive."""

import json
import subprocess
import time
import requests


def main() -> None:
    base = "http://127.0.0.1:8000"
    response = requests.get(base + "/api/events?limit=500", timeout=10)
    response.raise_for_status()
    identifiers = {event["event_id"] for event in response.json()}
    if not identifiers:
        raise RuntimeError("Run demo_smoke before the recovery check")
    subprocess.run(
        ["docker", "compose", "restart", "db", "broker", "minio", "spark-streaming"],
        check=True,
    )
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            response = requests.get(base + "/api/events?limit=500", timeout=5)
            response.raise_for_status()
            actual = {event["event_id"] for event in response.json()}
            if identifiers <= actual:
                print(
                    json.dumps(
                        {
                            "preserved_event_ids": len(identifiers),
                            "database_restart_recovery": True,
                            "note": "Run demo_smoke --spark next to verify writes after restart",
                        },
                        indent=2,
                    )
                )
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError("Stored IDs did not become available after restart")


if __name__ == "__main__":
    main()
