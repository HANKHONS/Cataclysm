param([int[]]$Workers = @(1, 2, 4))
$ErrorActionPreference = 'Stop'
foreach ($Count in $Workers) {
    if ($Count -lt 1 -or $Count -gt 4) { throw 'Use 1..4 workers on the 16 GB machine' }
}
docker compose stop spark-streaming
try {
    foreach ($Count in $Workers) {
        docker compose -f compose.yaml -f compose.experiments.yaml up -d --scale "spark-worker=$Count" spark-worker
        if ($LASTEXITCODE -ne 0) { throw 'Worker scaling failed' }
        docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --total-executor-cores $Count --executor-memory 512m /opt/spark/app/benchmarks/dataframe_benchmark.py --engine spark --master spark://spark-master:7077 --rows 100000 500000 1000000 --repetitions 3 --output "/opt/spark/app/work/scaling-$Count.json"
        if ($LASTEXITCODE -ne 0) { throw 'Spark benchmark failed' }
    }
} finally {
    docker compose -f compose.yaml -f compose.experiments.yaml up -d --scale spark-worker=1 spark-worker
    docker compose up -d spark-streaming
}
