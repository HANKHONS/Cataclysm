"""Refresh exact source snapshots inside the canonical module walkthroughs."""

from __future__ import annotations
import argparse
import ast
import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GROUPS = {
    "01_CONFIGURATION_AND_DOCKER": [
        "src/common/config.py",
        "compose.yaml",
        "Dockerfile",
        "infra/minio/Dockerfile",
        "requirements.txt",
        "requirements.lock.txt",
        "scripts/prepare_env.py",
    ],
    "02_SOURCES_AND_GENERATION": [
        "src/data/generator.py",
        "src/ingestion/clients.py",
        "src/ingestion/poll_sources.py",
    ],
    "03_API_AND_SCHEMAS": [
        "src/common/models.py",
        "src/api/main.py",
        "src/api/queries.py",
    ],
    "04_KAFKA_CONSUMER": ["src/kafka/consumer.py"],
    "05_POSTGRESQL": ["src/common/db.py"],
    "06_SPARK_STREAMING": ["src/spark/streaming_job.py"],
    "07_MINIO_PARQUET_REPLAY": [
        "src/storage/lake_writer.py",
        "src/storage/init_buckets.py",
        "src/data/replay.py",
    ],
    "08_ML_TRAINING": ["src/ml/train.py"],
    "09_ML_INFERENCE_AND_ANOMALIES": [
        "src/ml/inference.py",
        "src/ml/anomaly.py",
        "src/ml/detect_anomalies.py",
    ],
    "10_DASHBOARD": [
        "src/api/static/index.html",
        "src/api/static/console.js",
        "src/api/static/console.css",
        "docs/handbook/handbook.js",
        "docs/handbook/handbook.css",
        "scripts/build_handbook.py",
    ],
    "11_MONITORING": [
        "src/common/exporter.py",
        "monitoring/prometheus.yml",
        "monitoring/alerts.yml",
        "monitoring/grafana/provisioning/dashboards/api.json",
        "monitoring/grafana/provisioning/dashboards/provider.yml",
        "monitoring/grafana/provisioning/datasources/prometheus.yml",
    ],
    "12_TESTS_AND_RECOVERY": sorted(
        str(p.as_posix()) for p in Path("tests").glob("test_*.py")
    )
    + [
        "scripts/demo_smoke.py",
        "scripts/check_dlq.py",
        "scripts/audit_lake.py",
        "scripts/recovery_check.py",
        "scripts/check_stability.py",
        "scripts/correct_prediction_provenance.py",
        "scripts/verify_application.py",
        "scripts/verify_crash_replay.py",
        "scripts/verify_spark_semantics.py",
        "scripts/verify_dependency_outage.py",
    ],
    "13_DEPLOYMENT_AND_BENCHMARKS": [
        "infra/azure/main.bicep",
        "infra/azure/bootstrap.sh",
        "benchmarks/kafka_load.py",
        "benchmarks/dataframe_benchmark.py",
        "benchmarks/compare_models.py",
        "scripts/measure_current.py",
        "scripts/benchmark_scaling.ps1",
        "compose.experiments.yaml",
        "scripts/sync_source_reference.py",
        ".github/workflows/tests.yml",
    ],
}
MARKER = "<!-- SOURCE_REFERENCE: generated; edit prose above, refresh with scripts/sync_source_reference.py -->"


def reference(paths: list[str]) -> str:
    parts = [
        MARKER,
        "\n## Exact source reference\n",
        "These numbered snapshots are synchronized to the source hashes below. They are reference text, not executable replacements. Open the source for editing; run the refresh command after changes. Imports are listed so dependency boundaries remain visible.\n",
    ]
    for name in paths:
        path = ROOT / name
        if not path.exists():
            continue
        raw = path.read_bytes()
        text = raw.decode("utf-8-sig")
        parts.extend(
            [
                f"### `{name}`\n",
                f"[Open source](../../{name}) · SHA-256 `{hashlib.sha256(raw).hexdigest()}`\n",
            ]
        )
        if path.suffix == ".py":
            tree = ast.parse(text)
            imports = [
                ast.get_source_segment(text, node)
                for node in tree.body
                if isinstance(node, (ast.Import, ast.ImportFrom))
            ]
            if imports:
                parts.append(
                    "Dependencies: "
                    + "; ".join("`" + x.replace("\n", " ") + "`" for x in imports)
                    + "\n"
                )
            definitions = []
            for node in ast.walk(tree):
                if isinstance(
                    node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
                ):
                    definitions.append(
                        (
                            node.lineno,
                            f"- `{node.name}`: lines {node.lineno}–{node.end_lineno}. "
                            + (
                                ast.get_docstring(node)
                                or "Read its control flow with the module guide above."
                            ).split("\n")[0],
                        )
                    )
            parts.extend([line + "\n" for _, line in sorted(definitions)])
        numbered = "\n".join(
            f"{i:04d} | {line}" for i, line in enumerate(text.splitlines(), 1)
        )
        parts.extend(["\n```text\n" + numbered + "\n```\n"])
    return "\n".join(parts)


def main(check: bool = False):
    mismatch = []
    for stem, paths in GROUPS.items():
        path = ROOT / "docs/CODE_WALKTHROUGH" / f"{stem}.md"
        old = path.read_text(encoding="utf-8")
        prose = old.split(MARKER)[0].rstrip() + "\n\n"
        new = prose + reference(paths)
        if old != new:
            mismatch.append(str(path.relative_to(ROOT)))
            if not check:
                path.write_text(new, encoding="utf-8")
    if check and mismatch:
        raise SystemExit("Outdated references: " + ", ".join(mismatch))
    print(
        "Source references "
        + ("verified" if check else "refreshed")
        + f" for {len(GROUPS)} chapters"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    main(args.check)
