"""Bounded local streaming endurance check; fails on exits or restarts."""

import argparse
import json
from pathlib import Path
import subprocess
import time


def inspect():
    result = subprocess.run(
        ["docker", "inspect", "spark-streaming", "--format", "{{json .}}"],
        capture_output=True,
        text=True,
        check=True,
    )
    state = json.loads(result.stdout)
    return state["State"]["Running"], state["RestartCount"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=600)
    args = parser.parse_args()
    running, baseline = inspect()
    if not running:
        raise RuntimeError("Streaming is not running")
    started = time.monotonic()
    samples = []
    while time.monotonic() - started < args.seconds:
        running, restarts = inspect()
        if not running or restarts != baseline:
            raise RuntimeError("Streaming exited or restarted during stability check")
        usage = subprocess.run(
            [
                "docker",
                "stats",
                "spark-streaming",
                "--no-stream",
                "--format",
                "{{json .}}",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        sample = {
            "elapsed_seconds": round(time.monotonic() - started, 1),
            **json.loads(usage.stdout),
        }
        samples.append(sample)
        print(json.dumps(sample), flush=True)
        time.sleep(30)
    output = {
        "duration_seconds": time.monotonic() - started,
        "restarts": 0,
        "samples": samples,
    }
    Path("work/stability.json").write_text(
        json.dumps(output, indent=2), encoding="utf-8"
    )
    print("Stability check passed", flush=True)


if __name__ == "__main__":
    main()
