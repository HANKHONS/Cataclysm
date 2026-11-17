"""Measure Pandas and Spark separately; emits facts, never prefilled claims."""

from __future__ import annotations
import argparse
import json
import time
from pathlib import Path


def process_peak_bytes():
    """Cumulative process RSS peak; Spark executor memory is excluded."""
    try:
        import resource

        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    except ImportError:
        try:
            import psutil

            return psutil.Process().memory_info().peak_wset
        except ImportError:
            return None


def pandas_case(rows: int) -> dict[str, float | int | str]:
    import numpy as np
    import pandas as pd

    indices = np.arange(rows)
    frame = pd.DataFrame(
        {"type": (indices % 3).astype(str), "value": (indices % 100).astype(float)}
    )
    cpu = time.process_time()
    started = time.perf_counter()
    frame.groupby("type", as_index=False)["value"].mean()
    elapsed = time.perf_counter() - started
    return {
        "engine": "pandas",
        "rows": rows,
        "seconds": elapsed,
        "cpu_seconds": time.process_time() - cpu,
        "process_peak_rss_bytes": process_peak_bytes(),
        "dataframe_bytes": int(frame.memory_usage(deep=True).sum()),
        "measurement_scope": "Warm in-memory aggregation; cumulative Python-process RSS peak",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rows", type=int, nargs="+", default=[100_000, 500_000, 1_000_000]
    )
    parser.add_argument("--engine", choices=["pandas", "spark"], default="pandas")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if any(rows <= 0 for rows in args.rows) or args.repetitions < 1:
        parser.error("rows and repetitions must be positive")
    results = []
    spark = None
    if args.engine == "spark":
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master(args.master)
            .appName("cataclysm-benchmark")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
    try:
        for rows in args.rows:
            for repetition in range(args.repetitions):
                if spark is None:
                    result = pandas_case(rows)
                else:
                    frame = (
                        spark.range(rows)
                        .selectExpr(
                            "CAST(id % 3 AS STRING) AS type",
                            "CAST(id % 100 AS DOUBLE) AS value",
                        )
                        .cache()
                    )
                    frame.count()
                    started = time.perf_counter()
                    cpu = time.process_time()
                    frame.groupBy("type").avg("value").collect()
                    result = {
                        "engine": "spark",
                        "rows": rows,
                        "seconds": time.perf_counter() - started,
                        "master": args.master,
                        "driver_cpu_seconds": time.process_time() - cpu,
                        "python_driver_peak_rss_bytes": process_peak_bytes(),
                        "measurement_scope": "Warm cached aggregation; Python-driver memory/CPU excludes JVM and executors",
                    }
                    frame.unpersist()
                result["repetition"] = repetition
                results.append(result)
    finally:
        if spark is not None:
            spark.stop()
    serialized = json.dumps(results, indent=2)
    if args.output:
        args.output.write_text(serialized, encoding="utf-8")
    print(serialized)


if __name__ == "__main__":
    main()
