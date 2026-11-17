# Deployment and benchmarks: configuration is not deployment evidence

The Bicep file defines a VM name/size, region, username, secure SSH public key parameter, restricted SSH source CIDR and optional data disk. It creates NSG, virtual network/subnet, public IP, NIC and Ubuntu VM with system-assigned identity and SSH-only OS configuration. The only allowed inbound rule is source-restricted SSH. A public IP does not imply an exposed authenticated crisis API; use the existing SSH-tunnel guide for a private demonstration. No cloud resource was created in this execution, and no new local Bicep compile is claimed when the tooling is absent.

`bootstrap.sh` is intended to run as root on an already provisioned Ubuntu VM. `set -euo pipefail` stops errors/unset variables/pipeline failures; it checks UID before package work. It installs CA/curl, configures Docker's signed package repository from the OS codename, installs Docker/Compose and starts the daemon. It does not deploy an Azure resource, mount a new disk or copy secrets. Executing it on a different OS is not a portable setup path. `.env` must be configured privately; mounting/persisting data disks needs a deliberate plan. VM architecture remains a single failure domain, and local success does not verify cloud networking, quotas or spend.

## Measured workload code

`kafka_load.main` bounds count and concurrency, builds unique synthetic dictionaries, and sends independent timed POSTs via a ThreadPoolExecutor. Its nested send catches request failures and returns `(accepted, elapsed)`; global wall duration includes task scheduling/client overhead. It sorts accepted latencies and reports count/rate/median/p95. Failure count remains visible; scope ends at HTTP-to-Kafka acknowledgment. One location key deliberately concentrates broker partition work. Random new IDs make runs additive. `measure_current` invokes three measured subprocess runs after an explicitly preceding warm-up and records runtime/library versions plus sample mean/stddev; it does not create independent-host experiments.

`dataframe_benchmark.pandas_case` creates deterministic type/value columns before timing groupby mean, records CPU/process RSS and dataframe bytes. Spark's branch builds equivalent values, caches/materializes first, then times grouped mean collection and unpersists afterward. Each engine runs configured row counts/repetitions; JSON preserves every repetition. `process_peak_bytes` uses platform-specific process cumulative high-water RSS; Spark's Python-driver RSS excludes JVM/executors. Comparing those values as complete memory footprints is invalid. Timing initialization/JIT/cross-platform effects remain explained in the experiment document.

`compare_models` uses an optional experiment dependency set for controlled same-holdout LR/RF/XGBoost comparisons. It is separate from deployed training and uses a different text vectorizer; do not substitute its score for the deployed evaluation. XGBoost is an experiment dependency, not a new production requirement. Small synthetic/curated data still limit interpretation regardless of classifier sophistication.

`benchmark_scaling.ps1` changes worker count through the experiment Compose override after pausing production streaming, invokes the benchmark and restores local service layout. The override removes fixed container name/hostname/port bindings so multiple workers can start. Workers still share one PC; measured speedup includes contention and scheduling, not independent-machine availability. Preserve checkpoint mounts and do not run production streaming/benchmark drivers against the same query state. Use a controlled rehearsal, not a live presentation session.

The manual GitHub Actions workflow installs the locked runtime plus test/lint tools on Linux Python 3.11 and runs undefined/unused-name checks and offline pytest. It does not spin up the full service stack, build Azure or visually test the dashboard. A workflow file existing is not proof CI ran; no remote CI success is claimed here. Reproducible local commands and actual evidence are in the runbook and verification ledger.

<!-- SOURCE_REFERENCE: generated; edit prose above, refresh with scripts/sync_source_reference.py -->

## Exact source reference

These numbered snapshots are synchronized to the source hashes below. They are reference text, not executable replacements. Open the source for editing; run the refresh command after changes. Imports are listed so dependency boundaries remain visible.

### `infra/azure/main.bicep`

[Open source](../../infra/azure/main.bicep) · SHA-256 `29ed05bbf061cdc2e9fdce8c345a33af75f05a6b975cdcbecee68a91e203dbb7`


```text
0001 | @description('Region for the IaaS virtual machine.')
0002 | param location string = resourceGroup().location
0003 | @description('Administrator username. Supply a non-default value at deployment.')
0004 | param adminUsername string
0005 | @secure()
0006 | param adminPublicKey string
0007 | param sshSourceCidr string
0008 | param vmSize string = 'Standard_D4s_v5'
0009 | param vmName string = 'cataclysm'
0010 | @description('Optional extra data disk; zero avoids provisioning an unused disk for a short demo.')
0011 | @minValue(0)
0012 | param dataDiskSizeGB int = 0
0013 | 
0014 | resource nsg 'Microsoft.Network/networkSecurityGroups@2023-11-01' = {
0015 |   name: 'cataclysm-nsg'
0016 |   location: location
0017 |   properties: { securityRules: [
0018 |     {
0019 |       name: 'ssh'
0020 |       properties: {
0021 |         priority: 100
0022 |         protocol: 'Tcp'
0023 |         access: 'Allow'
0024 |         direction: 'Inbound'
0025 |         sourceAddressPrefix: sshSourceCidr
0026 |         sourcePortRange: '*'
0027 |         destinationAddressPrefix: '*'
0028 |         destinationPortRange: '22'
0029 |       }
0030 |     }
0031 |   ] }
0032 | }
0033 | 
0034 | resource network 'Microsoft.Network/virtualNetworks@2023-11-01' = {
0035 |   name: '${vmName}-network'
0036 |   location: location
0037 |   properties: {
0038 |     addressSpace: { addressPrefixes: ['10.10.0.0/16'] }
0039 |     subnets: [{
0040 |       name: 'application'
0041 |       properties: {
0042 |         addressPrefix: '10.10.1.0/24'
0043 |         networkSecurityGroup: { id: nsg.id }
0044 |       }
0045 |     }]
0046 |   }
0047 | }
0048 | 
0049 | resource publicIp 'Microsoft.Network/publicIPAddresses@2023-11-01' = {
0050 |   name: '${vmName}-ip'
0051 |   location: location
0052 |   sku: { name: 'Standard' }
0053 |   properties: { publicIPAllocationMethod: 'Static' }
0054 | }
0055 | 
0056 | resource nic 'Microsoft.Network/networkInterfaces@2023-11-01' = {
0057 |   name: '${vmName}-nic'
0058 |   location: location
0059 |   properties: {
0060 |     ipConfigurations: [{
0061 |       name: 'primary'
0062 |       properties: {
0063 |         privateIPAllocationMethod: 'Dynamic'
0064 |         subnet: { id: '${network.id}/subnets/application' }
0065 |         publicIPAddress: { id: publicIp.id }
0066 |       }
0067 |     }]
0068 |   }
0069 | }
0070 | 
0071 | resource vm 'Microsoft.Compute/virtualMachines@2024-03-01' = {
0072 |   name: vmName
0073 |   location: location
0074 |   identity: { type: 'SystemAssigned' }
0075 |   properties: {
0076 |     hardwareProfile: { vmSize: vmSize }
0077 |     storageProfile: {
0078 |       imageReference: {
0079 |         publisher: 'Canonical'
0080 |         offer: '0001-com-ubuntu-server-jammy'
0081 |         sku: '22_04-lts-gen2'
0082 |         version: 'latest'
0083 |       }
0084 |       osDisk: {
0085 |         createOption: 'FromImage'
0086 |         diskSizeGB: 64
0087 |         managedDisk: { storageAccountType: 'StandardSSD_LRS' }
0088 |       }
0089 |       dataDisks: dataDiskSizeGB > 0 ? [{
0090 |         lun: 0
0091 |         createOption: 'Empty'
0092 |         diskSizeGB: dataDiskSizeGB
0093 |         managedDisk: { storageAccountType: 'StandardSSD_LRS' }
0094 |       }] : []
0095 |     }
0096 |     osProfile: {
0097 |       computerName: vmName
0098 |       adminUsername: adminUsername
0099 |       linuxConfiguration: {
0100 |         disablePasswordAuthentication: true
0101 |         ssh: { publicKeys: [{ path: '/home/${adminUsername}/.ssh/authorized_keys', keyData: adminPublicKey }] }
0102 |       }
0103 |     }
0104 |     networkProfile: { networkInterfaces: [{ id: nic.id }] }
0105 |   }
0106 | }
0107 | 
0108 | output publicAddress string = publicIp.properties.ipAddress
0109 | output vmResourceId string = vm.id
```

### `infra/azure/bootstrap.sh`

[Open source](../../infra/azure/bootstrap.sh) · SHA-256 `a06f8568a1f83b9d03ec46732493f3dc05350420da0ca2a34a9ebd313603c22a`


```text
0001 | #!/usr/bin/env bash
0002 | # Run on the provisioned Ubuntu VM; no Azure resource creation occurs here.
0003 | set -euo pipefail
0004 | if [ "$(id -u)" -ne 0 ]; then
0005 |   echo 'Run with sudo after copying the repository to the VM.' >&2
0006 |   exit 1
0007 | fi
0008 | apt-get update
0009 | apt-get install -y ca-certificates curl
0010 | install -m 0755 -d /etc/apt/keyrings
0011 | curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
0012 | chmod a+r /etc/apt/keyrings/docker.asc
0013 | . /etc/os-release
0014 | printf 'deb [arch=%s signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu %s stable\n' "$(dpkg --print-architecture)" "$VERSION_CODENAME" > /etc/apt/sources.list.d/docker.list
0015 | apt-get update
0016 | apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
0017 | systemctl enable --now docker
0018 | docker compose version
0019 | echo 'Docker installed. Mount the data disk, configure ignored .env, then run docker compose up -d --build from the repository.'
```

### `benchmarks/kafka_load.py`

[Open source](../../benchmarks/kafka_load.py) · SHA-256 `e5081e55b4e5236a561b47816242de3bbd97eb90bb98f4afc13dc66d5a8c2989`

Dependencies: `import argparse`; `from concurrent.futures import ThreadPoolExecutor`; `from datetime import datetime, timezone`; `import json`; `from pathlib import Path`; `import statistics`; `import time`; `import uuid`; `import requests`

- `main`: lines 14–67. Read its control flow with the module guide above.

- `send`: lines 24–42. Read its control flow with the module guide above.


```text
0001 | """Bounded local load experiment; records accepted/rejected latency separately."""
0002 | 
0003 | import argparse
0004 | from concurrent.futures import ThreadPoolExecutor
0005 | from datetime import datetime, timezone
0006 | import json
0007 | from pathlib import Path
0008 | import statistics
0009 | import time
0010 | import uuid
0011 | import requests
0012 | 
0013 | 
0014 | def main() -> None:
0015 |     parser = argparse.ArgumentParser()
0016 |     parser.add_argument("--count", type=int, default=100)
0017 |     parser.add_argument("--concurrency", type=int, default=4)
0018 |     parser.add_argument("--endpoint", default="http://127.0.0.1:8000/events")
0019 |     parser.add_argument("--output", type=Path)
0020 |     args = parser.parse_args()
0021 |     if not 1 <= args.count <= 10000 or not 1 <= args.concurrency <= 16:
0022 |         parser.error("count must be 1..10000 and concurrency 1..16")
0023 | 
0024 |     def send(index):
0025 |         event = {
0026 |             "event_id": "LOAD-" + uuid.uuid4().hex,
0027 |             "timestamp": datetime.now(timezone.utc).isoformat(),
0028 |             "text": "Synthetic load test flood report",
0029 |             "location": "Benchmark",
0030 |             "disaster_type": "flood",
0031 |             "latitude": 12,
0032 |             "longitude": 77,
0033 |             "severity": "minor",
0034 |             "source": "synthetic_benchmark",
0035 |             "metadata": {"is_synthetic": True},
0036 |         }
0037 |         start = time.perf_counter()
0038 |         try:
0039 |             response = requests.post(args.endpoint, json=event, timeout=10)
0040 |             return response.status_code == 200, time.perf_counter() - start
0041 |         except requests.RequestException:
0042 |             return False, time.perf_counter() - start
0043 | 
0044 |     start = time.perf_counter()
0045 |     with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
0046 |         results = list(executor.map(send, range(args.count)))
0047 |     duration = time.perf_counter() - start
0048 |     accepted = sorted(elapsed for ok, elapsed in results if ok)
0049 |     report = {
0050 |         "requested": args.count,
0051 |         "accepted": len(accepted),
0052 |         "rejected": args.count - len(accepted),
0053 |         "concurrency": args.concurrency,
0054 |         "wall_seconds": duration,
0055 |         "accepted_per_second": len(accepted) / duration,
0056 |         "accepted_p50_seconds": statistics.median(accepted) if accepted else None,
0057 |         "accepted_p95_seconds": accepted[
0058 |             min(len(accepted) - 1, int(len(accepted) * 0.95))
0059 |         ]
0060 |         if accepted
0061 |         else None,
0062 |         "scope": "HTTP-to-Kafka acknowledgement; downstream lag must be inspected separately",
0063 |     }
0064 |     output = json.dumps(report, indent=2)
0065 |     if args.output:
0066 |         args.output.write_text(output, encoding="utf-8")
0067 |     print(output)
0068 | 
0069 | 
0070 | if __name__ == "__main__":
0071 |     main()
```

### `benchmarks/dataframe_benchmark.py`

[Open source](../../benchmarks/dataframe_benchmark.py) · SHA-256 `b354a6ff908b36889825fb54ac69dc92b1f45b669a10ac814470655a5e3a8dc9`

Dependencies: `from __future__ import annotations`; `import argparse`; `import json`; `import time`; `from pathlib import Path`

- `process_peak_bytes`: lines 10–22. Cumulative process RSS peak; Spark executor memory is excluded.

- `pandas_case`: lines 25–45. Read its control flow with the module guide above.

- `main`: lines 48–107. Read its control flow with the module guide above.


```text
0001 | """Measure Pandas and Spark separately; emits facts, never prefilled claims."""
0002 | 
0003 | from __future__ import annotations
0004 | import argparse
0005 | import json
0006 | import time
0007 | from pathlib import Path
0008 | 
0009 | 
0010 | def process_peak_bytes():
0011 |     """Cumulative process RSS peak; Spark executor memory is excluded."""
0012 |     try:
0013 |         import resource
0014 | 
0015 |         return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
0016 |     except ImportError:
0017 |         try:
0018 |             import psutil
0019 | 
0020 |             return psutil.Process().memory_info().peak_wset
0021 |         except ImportError:
0022 |             return None
0023 | 
0024 | 
0025 | def pandas_case(rows: int) -> dict[str, float | int | str]:
0026 |     import numpy as np
0027 |     import pandas as pd
0028 | 
0029 |     indices = np.arange(rows)
0030 |     frame = pd.DataFrame(
0031 |         {"type": (indices % 3).astype(str), "value": (indices % 100).astype(float)}
0032 |     )
0033 |     cpu = time.process_time()
0034 |     started = time.perf_counter()
0035 |     frame.groupby("type", as_index=False)["value"].mean()
0036 |     elapsed = time.perf_counter() - started
0037 |     return {
0038 |         "engine": "pandas",
0039 |         "rows": rows,
0040 |         "seconds": elapsed,
0041 |         "cpu_seconds": time.process_time() - cpu,
0042 |         "process_peak_rss_bytes": process_peak_bytes(),
0043 |         "dataframe_bytes": int(frame.memory_usage(deep=True).sum()),
0044 |         "measurement_scope": "Warm in-memory aggregation; cumulative Python-process RSS peak",
0045 |     }
0046 | 
0047 | 
0048 | def main() -> None:
0049 |     parser = argparse.ArgumentParser()
0050 |     parser.add_argument(
0051 |         "--rows", type=int, nargs="+", default=[100_000, 500_000, 1_000_000]
0052 |     )
0053 |     parser.add_argument("--engine", choices=["pandas", "spark"], default="pandas")
0054 |     parser.add_argument("--master", default="local[2]")
0055 |     parser.add_argument("--repetitions", type=int, default=3)
0056 |     parser.add_argument("--output", type=Path)
0057 |     args = parser.parse_args()
0058 |     if any(rows <= 0 for rows in args.rows) or args.repetitions < 1:
0059 |         parser.error("rows and repetitions must be positive")
0060 |     results = []
0061 |     spark = None
0062 |     if args.engine == "spark":
0063 |         from pyspark.sql import SparkSession
0064 | 
0065 |         spark = (
0066 |             SparkSession.builder.master(args.master)
0067 |             .appName("cataclysm-benchmark")
0068 |             .getOrCreate()
0069 |         )
0070 |         spark.sparkContext.setLogLevel("WARN")
0071 |     try:
0072 |         for rows in args.rows:
0073 |             for repetition in range(args.repetitions):
0074 |                 if spark is None:
0075 |                     result = pandas_case(rows)
0076 |                 else:
0077 |                     frame = (
0078 |                         spark.range(rows)
0079 |                         .selectExpr(
0080 |                             "CAST(id % 3 AS STRING) AS type",
0081 |                             "CAST(id % 100 AS DOUBLE) AS value",
0082 |                         )
0083 |                         .cache()
0084 |                     )
0085 |                     frame.count()
0086 |                     started = time.perf_counter()
0087 |                     cpu = time.process_time()
0088 |                     frame.groupBy("type").avg("value").collect()
0089 |                     result = {
0090 |                         "engine": "spark",
0091 |                         "rows": rows,
0092 |                         "seconds": time.perf_counter() - started,
0093 |                         "master": args.master,
0094 |                         "driver_cpu_seconds": time.process_time() - cpu,
0095 |                         "python_driver_peak_rss_bytes": process_peak_bytes(),
0096 |                         "measurement_scope": "Warm cached aggregation; Python-driver memory/CPU excludes JVM and executors",
0097 |                     }
0098 |                     frame.unpersist()
0099 |                 result["repetition"] = repetition
0100 |                 results.append(result)
0101 |     finally:
0102 |         if spark is not None:
0103 |             spark.stop()
0104 |     serialized = json.dumps(results, indent=2)
0105 |     if args.output:
0106 |         args.output.write_text(serialized, encoding="utf-8")
0107 |     print(serialized)
0108 | 
0109 | 
0110 | if __name__ == "__main__":
0111 |     main()
```

### `benchmarks/compare_models.py`

[Open source](../../benchmarks/compare_models.py) · SHA-256 `74d26cb57bed42dba9cadd1ed0892f0822d9455f186a4e5358db40e73700c843`

Dependencies: `import json`; `import time`; `import argparse`; `from pathlib import Path`; `from sklearn.preprocessing import LabelEncoder`; `from sklearn.ensemble import RandomForestClassifier`; `from sklearn.feature_extraction.text import TfidfVectorizer`; `from sklearn.linear_model import LogisticRegression`; `from sklearn.metrics import classification_report`; `from sklearn.model_selection import train_test_split`; `from sklearn.pipeline import Pipeline`; `from src.ml.train import TEXT_SAMPLES`; `from src.ml.train import generate_telemetry_dataset`

- `main`: lines 18–111. Read its control flow with the module guide above.


```text
0001 | """Compare text classifiers on exactly the same small held-out corpus."""
0002 | 
0003 | import json
0004 | import time
0005 | import argparse
0006 | from pathlib import Path
0007 | from sklearn.preprocessing import LabelEncoder
0008 | from sklearn.ensemble import RandomForestClassifier
0009 | from sklearn.feature_extraction.text import TfidfVectorizer
0010 | from sklearn.linear_model import LogisticRegression
0011 | from sklearn.metrics import classification_report
0012 | from sklearn.model_selection import train_test_split
0013 | from sklearn.pipeline import Pipeline
0014 | from src.ml.train import TEXT_SAMPLES
0015 | from src.ml.train import generate_telemetry_dataset
0016 | 
0017 | 
0018 | def main() -> None:
0019 |     parser = argparse.ArgumentParser()
0020 |     parser.add_argument("--output", type=Path)
0021 |     args = parser.parse_args()
0022 |     texts, labels = zip(*TEXT_SAMPLES)
0023 |     train, test, y_train, y_test = train_test_split(
0024 |         texts, labels, stratify=labels, test_size=0.2, random_state=42
0025 |     )
0026 |     results = {}
0027 |     estimators = [
0028 |         ("logistic_regression", LogisticRegression(max_iter=300, random_state=42)),
0029 |         ("random_forest", RandomForestClassifier(n_estimators=100, random_state=42)),
0030 |     ]
0031 |     skipped = []
0032 |     try:
0033 |         from xgboost import XGBClassifier
0034 | 
0035 |         estimators.append(
0036 |             (
0037 |                 "xgboost",
0038 |                 XGBClassifier(n_estimators=100, max_depth=4, n_jobs=2, random_state=42),
0039 |             )
0040 |         )
0041 |     except ImportError:
0042 |         skipped.append("xgboost: install requirements-experiments.txt")
0043 |     for name, estimator in estimators:
0044 |         model = Pipeline(
0045 |             [("tfidf", TfidfVectorizer(ngram_range=(1, 2))), ("model", estimator)]
0046 |         )
0047 |         start = time.perf_counter()
0048 |         encoder = LabelEncoder().fit(y_train)
0049 |         model.fit(train, encoder.transform(y_train) if name == "xgboost" else y_train)
0050 |         train_seconds = time.perf_counter() - start
0051 |         start = time.perf_counter()
0052 |         predictions = model.predict(test)
0053 |         if name == "xgboost":
0054 |             predictions = encoder.inverse_transform(predictions.astype(int))
0055 |         results[name] = {
0056 |             "training_seconds": train_seconds,
0057 |             "inference_seconds": time.perf_counter() - start,
0058 |             "report": classification_report(
0059 |                 y_test, predictions, output_dict=True, zero_division=0
0060 |             ),
0061 |         }
0062 |     features, severity = generate_telemetry_dataset(1500)
0063 |     x_train, x_test, s_train, s_test = train_test_split(
0064 |         features, severity, stratify=severity, test_size=0.2, random_state=42
0065 |     )
0066 |     severity_estimators = [
0067 |         (
0068 |             "random_forest",
0069 |             RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42),
0070 |         )
0071 |     ]
0072 |     if not skipped:
0073 |         severity_estimators.append(
0074 |             (
0075 |                 "xgboost",
0076 |                 XGBClassifier(n_estimators=100, max_depth=4, n_jobs=2, random_state=42),
0077 |             )
0078 |         )
0079 |     severity_results = {}
0080 |     encoder = LabelEncoder().fit(s_train)
0081 |     for name, estimator in severity_estimators:
0082 |         start = time.perf_counter()
0083 |         estimator.fit(
0084 |             x_train, encoder.transform(s_train) if name == "xgboost" else s_train
0085 |         )
0086 |         train_seconds = time.perf_counter() - start
0087 |         start = time.perf_counter()
0088 |         predicted = estimator.predict(x_test)
0089 |         elapsed = time.perf_counter() - start
0090 |         if name == "xgboost":
0091 |             predicted = encoder.inverse_transform(predicted.astype(int))
0092 |         severity_results[name] = {
0093 |             "training_seconds": train_seconds,
0094 |             "inference_seconds": elapsed,
0095 |             "report": classification_report(
0096 |                 s_test, predicted, output_dict=True, zero_division=0
0097 |             ),
0098 |         }
0099 |     report = json.dumps(
0100 |         {
0101 |             "results": results,
0102 |             "severity_results": severity_results,
0103 |             "test_examples": len(test),
0104 |             "skipped": skipped,
0105 |             "limitations": "Small curated corpus; no claim of deployment accuracy.",
0106 |         },
0107 |         indent=2,
0108 |     )
0109 |     if args.output:
0110 |         args.output.write_text(report, encoding="utf-8")
0111 |     print(report)
0112 | 
0113 | 
0114 | if __name__ == "__main__":
0115 |     main()
```

### `scripts/measure_current.py`

[Open source](../../scripts/measure_current.py) · SHA-256 `7c82b1359e32a124534d5540ab5160b2f69977b6fc1af2d93b39456daa210529`

Dependencies: `import importlib.metadata`; `import json`; `import platform`; `import statistics`; `import subprocess`; `import sys`; `from datetime import datetime, timezone`; `from pathlib import Path`


```text
0001 | """Repeat bounded broker-acknowledgment measurements and record runtime context."""
0002 | 
0003 | import importlib.metadata
0004 | import json
0005 | import platform
0006 | import statistics
0007 | import subprocess
0008 | import sys
0009 | from datetime import datetime, timezone
0010 | from pathlib import Path
0011 | 
0012 | reports = []
0013 | for index in range(3):
0014 |     output = subprocess.check_output(
0015 |         [
0016 |             sys.executable,
0017 |             "benchmarks/kafka_load.py",
0018 |             "--count",
0019 |             "100",
0020 |             "--concurrency",
0021 |             "4",
0022 |             "--endpoint",
0023 |             "http://127.0.0.1:8000/events",
0024 |         ],
0025 |         text=True,
0026 |     )
0027 |     reports.append(json.loads(output))
0028 | rates = [r["accepted_per_second"] for r in reports]
0029 | report = {
0030 |     "observed_at": datetime.now(timezone.utc).isoformat(),
0031 |     "runtime": platform.platform(),
0032 |     "python": platform.python_version(),
0033 |     "versions": {
0034 |         p: importlib.metadata.version(p)
0035 |         for p in [
0036 |             "fastapi",
0037 |             "confluent-kafka",
0038 |             "psycopg",
0039 |             "pyarrow",
0040 |             "scikit-learn",
0041 |             "pandas",
0042 |         ]
0043 |     },
0044 |     "policy": "One preceding 100-event warm-up; three sequential measured 100-event runs; concurrency 4; no pauses; one location key. Existing browser and streaming workloads remain active.",
0045 |     "runs": reports,
0046 |     "ack_rate_mean": statistics.mean(rates),
0047 |     "ack_rate_sample_stddev": statistics.stdev(rates),
0048 |     "scope": "Container loopback HTTP-to-Kafka acknowledgment only; no sustained-capacity or end-to-end latency claim.",
0049 | }
0050 | Path("docs/evidence/benchmark-current.json").write_text(json.dumps(report, indent=2))
0051 | print(json.dumps(report, indent=2))
```

### `scripts/benchmark_scaling.ps1`

[Open source](../../scripts/benchmark_scaling.ps1) · SHA-256 `cac2c476ab995b892318c05d0f7909a3ed961b20678fbe7e17322f9cabd41492`


```text
0001 | param([int[]]$Workers = @(1, 2, 4))
0002 | $ErrorActionPreference = 'Stop'
0003 | foreach ($Count in $Workers) {
0004 |     if ($Count -lt 1 -or $Count -gt 4) { throw 'Use 1..4 workers on the 16 GB machine' }
0005 | }
0006 | docker compose stop spark-streaming
0007 | try {
0008 |     foreach ($Count in $Workers) {
0009 |         docker compose -f compose.yaml -f compose.experiments.yaml up -d --scale "spark-worker=$Count" spark-worker
0010 |         if ($LASTEXITCODE -ne 0) { throw 'Worker scaling failed' }
0011 |         docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores $Count --executor-memory 512m /opt/spark/app/benchmarks/dataframe_benchmark.py --engine spark --master spark://spark-master:7077 --rows 100000 500000 1000000 --repetitions 3 --output "/opt/spark/app/work/scaling-$Count.json"
0012 |         if ($LASTEXITCODE -ne 0) { throw 'Spark benchmark failed' }
0013 |     }
0014 | } finally {
0015 |     docker compose -f compose.yaml -f compose.experiments.yaml up -d --scale spark-worker=1 spark-worker
0016 |     docker compose up -d spark-streaming
0017 | }
```

### `compose.experiments.yaml`

[Open source](../../compose.experiments.yaml) · SHA-256 `df8d644411e6f20244b95193b81ab432ef1783435d596f92f7e857ff5c345c0b`


```text
0001 | # Use only during controlled scaling experiments, after stopping spark-streaming.
0002 | services:
0003 |   spark-worker:
0004 |     container_name: !reset null
0005 |     hostname: !reset null
0006 |     ports: !reset []
```

### `scripts/sync_source_reference.py`

[Open source](../../scripts/sync_source_reference.py) · SHA-256 `8ca4f18837a5855bfb5259c716c3b064394d5b85d69e085252fbf9e25741f0a6`

Dependencies: `from __future__ import annotations`; `import argparse`; `import ast`; `import hashlib`; `from pathlib import Path`

- `reference`: lines 91–142. Read its control flow with the module guide above.

- `main`: lines 145–162. Read its control flow with the module guide above.


```text
0001 | """Refresh exact source snapshots inside the canonical module walkthroughs."""
0002 | 
0003 | from __future__ import annotations
0004 | import argparse
0005 | import ast
0006 | import hashlib
0007 | from pathlib import Path
0008 | 
0009 | ROOT = Path(__file__).resolve().parents[1]
0010 | GROUPS = {
0011 |     "01_CONFIGURATION_AND_DOCKER": [
0012 |         "src/common/config.py",
0013 |         "compose.yaml",
0014 |         "Dockerfile",
0015 |         "infra/minio/Dockerfile",
0016 |         "requirements.txt",
0017 |         "requirements.lock.txt",
0018 |         "scripts/prepare_env.py",
0019 |     ],
0020 |     "02_SOURCES_AND_GENERATION": [
0021 |         "src/data/generator.py",
0022 |         "src/ingestion/clients.py",
0023 |         "src/ingestion/poll_sources.py",
0024 |     ],
0025 |     "03_API_AND_SCHEMAS": [
0026 |         "src/common/models.py",
0027 |         "src/api/main.py",
0028 |         "src/api/queries.py",
0029 |     ],
0030 |     "04_KAFKA_CONSUMER": ["src/kafka/consumer.py"],
0031 |     "05_POSTGRESQL": ["src/common/db.py"],
0032 |     "06_SPARK_STREAMING": ["src/spark/streaming_job.py"],
0033 |     "07_MINIO_PARQUET_REPLAY": [
0034 |         "src/storage/lake_writer.py",
0035 |         "src/storage/init_buckets.py",
0036 |         "src/data/replay.py",
0037 |     ],
0038 |     "08_ML_TRAINING": ["src/ml/train.py"],
0039 |     "09_ML_INFERENCE_AND_ANOMALIES": [
0040 |         "src/ml/inference.py",
0041 |         "src/ml/anomaly.py",
0042 |         "src/ml/detect_anomalies.py",
0043 |     ],
0044 |     "10_DASHBOARD": [
0045 |         "src/api/static/index.html",
0046 |         "src/api/static/console.js",
0047 |         "src/api/static/console.css",
0048 |         "docs/handbook/handbook.js",
0049 |         "docs/handbook/handbook.css",
0050 |         "scripts/build_handbook.py",
0051 |     ],
0052 |     "11_MONITORING": [
0053 |         "src/common/exporter.py",
0054 |         "monitoring/prometheus.yml",
0055 |         "monitoring/alerts.yml",
0056 |         "monitoring/grafana/provisioning/dashboards/api.json",
0057 |         "monitoring/grafana/provisioning/dashboards/provider.yml",
0058 |         "monitoring/grafana/provisioning/datasources/prometheus.yml",
0059 |     ],
0060 |     "12_TESTS_AND_RECOVERY": sorted(
0061 |         str(p.as_posix()) for p in Path("tests").glob("test_*.py")
0062 |     )
0063 |     + [
0064 |         "scripts/demo_smoke.py",
0065 |         "scripts/check_dlq.py",
0066 |         "scripts/audit_lake.py",
0067 |         "scripts/recovery_check.py",
0068 |         "scripts/check_stability.py",
0069 |         "scripts/correct_prediction_provenance.py",
0070 |         "scripts/verify_application.py",
0071 |         "scripts/verify_crash_replay.py",
0072 |         "scripts/verify_spark_semantics.py",
0073 |         "scripts/verify_dependency_outage.py",
0074 |     ],
0075 |     "13_DEPLOYMENT_AND_BENCHMARKS": [
0076 |         "infra/azure/main.bicep",
0077 |         "infra/azure/bootstrap.sh",
0078 |         "benchmarks/kafka_load.py",
0079 |         "benchmarks/dataframe_benchmark.py",
0080 |         "benchmarks/compare_models.py",
0081 |         "scripts/measure_current.py",
0082 |         "scripts/benchmark_scaling.ps1",
0083 |         "compose.experiments.yaml",
0084 |         "scripts/sync_source_reference.py",
0085 |         ".github/workflows/tests.yml",
0086 |     ],
0087 | }
0088 | MARKER = "<!-- SOURCE_REFERENCE: generated; edit prose above, refresh with scripts/sync_source_reference.py -->"
0089 | 
0090 | 
0091 | def reference(paths: list[str]) -> str:
0092 |     parts = [
0093 |         MARKER,
0094 |         "\n## Exact source reference\n",
0095 |         "These numbered snapshots are synchronized to the source hashes below. They are reference text, not executable replacements. Open the source for editing; run the refresh command after changes. Imports are listed so dependency boundaries remain visible.\n",
0096 |     ]
0097 |     for name in paths:
0098 |         path = ROOT / name
0099 |         if not path.exists():
0100 |             continue
0101 |         raw = path.read_bytes()
0102 |         text = raw.decode("utf-8-sig")
0103 |         parts.extend(
0104 |             [
0105 |                 f"### `{name}`\n",
0106 |                 f"[Open source](../../{name}) · SHA-256 `{hashlib.sha256(raw).hexdigest()}`\n",
0107 |             ]
0108 |         )
0109 |         if path.suffix == ".py":
0110 |             tree = ast.parse(text)
0111 |             imports = [
0112 |                 ast.get_source_segment(text, node)
0113 |                 for node in tree.body
0114 |                 if isinstance(node, (ast.Import, ast.ImportFrom))
0115 |             ]
0116 |             if imports:
0117 |                 parts.append(
0118 |                     "Dependencies: "
0119 |                     + "; ".join("`" + x.replace("\n", " ") + "`" for x in imports)
0120 |                     + "\n"
0121 |                 )
0122 |             definitions = []
0123 |             for node in ast.walk(tree):
0124 |                 if isinstance(
0125 |                     node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
0126 |                 ):
0127 |                     definitions.append(
0128 |                         (
0129 |                             node.lineno,
0130 |                             f"- `{node.name}`: lines {node.lineno}–{node.end_lineno}. "
0131 |                             + (
0132 |                                 ast.get_docstring(node)
0133 |                                 or "Read its control flow with the module guide above."
0134 |                             ).split("\n")[0],
0135 |                         )
0136 |                     )
0137 |             parts.extend([line + "\n" for _, line in sorted(definitions)])
0138 |         numbered = "\n".join(
0139 |             f"{i:04d} | {line}" for i, line in enumerate(text.splitlines(), 1)
0140 |         )
0141 |         parts.extend(["\n```text\n" + numbered + "\n```\n"])
0142 |     return "\n".join(parts)
0143 | 
0144 | 
0145 | def main(check: bool = False):
0146 |     mismatch = []
0147 |     for stem, paths in GROUPS.items():
0148 |         path = ROOT / "docs/CODE_WALKTHROUGH" / f"{stem}.md"
0149 |         old = path.read_text(encoding="utf-8")
0150 |         prose = old.split(MARKER)[0].rstrip() + "\n\n"
0151 |         new = prose + reference(paths)
0152 |         if old != new:
0153 |             mismatch.append(str(path.relative_to(ROOT)))
0154 |             if not check:
0155 |                 path.write_text(new, encoding="utf-8")
0156 |     if check and mismatch:
0157 |         raise SystemExit("Outdated references: " + ", ".join(mismatch))
0158 |     print(
0159 |         "Source references "
0160 |         + ("verified" if check else "refreshed")
0161 |         + f" for {len(GROUPS)} chapters"
0162 |     )
0163 | 
0164 | 
0165 | if __name__ == "__main__":
0166 |     parser = argparse.ArgumentParser(description=__doc__)
0167 |     parser.add_argument("--check", action="store_true")
0168 |     args = parser.parse_args()
0169 |     main(args.check)
```

### `.github/workflows/tests.yml`

[Open source](../../.github/workflows/tests.yml) · SHA-256 `334aa316966892454b55d7bda48adea76d4e0ab15b4778c87dd08c174d51a568`


```text
0001 | name: Offline tests
0002 | on: workflow_dispatch
0003 | jobs:
0004 |   test:
0005 |     runs-on: ubuntu-latest
0006 |     steps:
0007 |       - uses: actions/checkout@v4
0008 |       - uses: actions/setup-python@v5
0009 |         with:
0010 |           python-version: '3.11'
0011 |           cache: pip
0012 |       - run: python -m pip install -r requirements.lock.txt pytest httpx ruff
0013 |       - run: python -m ruff check src scripts benchmarks tests --select F
0014 |       - run: python -m pytest tests -q
```
