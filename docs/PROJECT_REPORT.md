# CATACLYSM project report

## Abstract

CATACLYSM is a single-host crisis-observation processing prototype integrating FastAPI, Kafka KRaft, a Python operational consumer, PostgreSQL, MinIO, Spark Structured Streaming, classical machine learning and a geospatial browser console. The project emphasizes explicit delivery boundaries and reproducible evidence rather than unverified emergency-response claims. Public USGS observations and labelled synthetic tests traverse the gateway. Operational writes are transactional and keyed by event identity; raw archival precedes manual Kafka commit. An independent Spark path performs bounded event-time deduplication and sliding-window lake aggregation. Current evaluation includes actual process-crash replay, controlled late-event/checkpoint/window tests, database/broker outage recovery, backend-driven UI interaction and bounded load measurements. Text classification remains weak on a tiny curated holdout, synthetic severity scores do not validate physical hazard prediction, and cloud deployment is deferred.

## Problem statement

Crisis observations arrive with inconsistent schemas, units, timestamps and provenance. A single spreadsheet or direct browser feed does not demonstrate durable ingestion, replay, independent analytics or recovery after partial failure. An engineering prototype should normalize observations, preserve their identity/source, separate input facts from model output and expose the difference between accepted, persisted and analytically finalized data. It must also remain reproducible on a student's resource-limited workstation.

The project does not attempt to solve report authentication, disaster entity resolution, physically validated damage estimation or authoritative emergency dispatch. Claiming those outcomes without independent data/domain validation would exceed both implementation and evidence.

## Objectives and acceptance method

The objectives are a functioning source-to-dashboard system, preserved working state, real provider integration where credentials permit, explicit synthetic/replay labels, durable operational identity, explainable streaming semantics, measured monitoring, restrained usable visual design and source-synchronized educational material. Acceptance uses actual known-event readback and negative-path behavior, not roadmap checkboxes or file existence.

The supplied master execution request governs the current work. `CATACLYSM_IMPLEMENTATION_PLAN.md` was not found in the checkout, and pre-existing Git changes/deleted GSD files were preserved. The implementation audit distinguishes historical evidence from fresh 10 November 2026 results.

## Technology and literature context

Event logs decouple producers from independent consumers and retain positions for recovery. Their delivery guarantees depend on the external side-effect boundary; Kafka cannot independently make PostgreSQL and object storage one atomic transaction. Stream processing introduces event time and bounded state because arrivals can be delayed/out of order. Spark watermarks permit controlled state cleanup and finalized windows but do not replace permanent event identity.

Relational storage serves transactional keyed queries, whereas Parquet/object storage favors typed analytical files and replay. Sparse text features with a linear classifier are a transparent baseline for small text corpora. Tree ensembles can learn nonlinear telemetry relationships, but target validity and independent evaluation are more important than estimator complexity. Unsupervised isolation of unusual report counts is distinct from supervised disaster detection.

These choices follow primary technology documentation cited below. The project's actual configuration and measured experiments determine the narrower claims in this report; documentation of a platform capability does not prove this checkout exercises all of it.

## Architecture and methodology

Sources normalize to CrisisEvent and POST through FastAPI. The gateway validates, publishes by location key and waits for broker acknowledgment. The operational consumer validates again, performs CPU inference, commits event/report/prediction rows in PostgreSQL, archives the supported enriched payload as raw Parquet and only then commits its Kafka offset. Independent Spark queries transform/deduplicate the Kafka stream into processed Parquet and aggregate that committed dataset into ten-minute windows sliding every five minutes. API queries and two-second database-polled SSE support the Leaflet console. Prometheus/Grafana use actual instrumented/probed values.

Methodology followed inspect, preserve, test, correct and verify in dependency order. Baseline offline tests passed 18. Confirmed gaps included lossy local replay, missing API metadata/provenance, feed-only dashboard totals, nonfinite metadata acceptance and ambiguous timestamp interpretation. Corrections added faithful acknowledged replay, supported provenance fields, UTC normalization, finite metadata constraints, parameterized filters/pagination, period-wide aggregates and explicit identity policy. A later live browser inspection found SSE serialization needed created_at conversion; the resulting regression test protects that path.

## Implementation details

USGS normalization retains provider identity, event time, coordinates, magnitude, depth and URL, isolating malformed features so valid neighbors survive. Magnitude-based severity is identified as a heuristic rather than provider damage assessment. Weather setup is credentialed and offline-tested; missing observed timestamp now fails rather than substituting arrival time. Synthetic generation and dashboard submission identify simulations explicitly. Supported metadata, including replay flags, survives raw JSON archival.

The database uses primary/foreign/unique keys, transaction contexts and pooled connections with bounded acquisition. A conflicting event identity preserves first-observation facts and predictions. Duplicate archival uses the stored original payload. There is no claimed distributed transaction: a database row can become visible before raw archival/offset commit. Kafka has three source partitions and one broker, with no host-failure redundancy. The broker remains KRaft; no downgrade to ZooKeeper was made.

Spark keeps existing v2 output prefixes/checkpoints, two stateful queries, ten-minute event-time watermarks and bounded deduplication. Driver/worker checkpoint mounts remain shared. Query triggers/history retention and heap settings reflect the resource-conscious existing implementation. Only one driver owns production query state. Isolated tests use separate fixture directories/checkpoints and do not alter production watermark merely to force output.

The dashboard retains the buildless frontend/Leaflet stack but now uses a warm neutral operations design. Server-derived period totals are separate from loaded rows. Filters cover period, hazard, severity, source, search and optional viewport bounds; map/register share persisted data. Detail views separate input severity, its origin and experimental model outputs. The test form waits for broker acknowledgment then actual stored details. Charts use API values, monitoring failures remain unavailable, and external-map failure has an explanatory list-based fallback. Full offline vendoring and exhaustive assistive-technology certification are not claimed.

## Results

Current offline tests cover schema, delivery callbacks, no-commit failure behavior, Parquet fidelity, source credentials/units, anomaly guards, safe filters, timestamp/provenance and replay regression paths. Current test counts and exact final commands are in `VERIFICATION.md` because they may change after this report is generated.

Actual smoke events reached the broker, one operational row, two predictions, exact raw object and exact identity inside processed Parquet. Duplicate consumption advanced the operational offset without a second event row. A real consumer process exited 91 after its DB transaction and before raw archival/commit, then resumed on its dedicated group: offset -1001 became 1 while row count stayed one and prediction count stayed two. An isolated Spark 3.5.1 test retained dedup across restart, dropped one controlled late event and read back two finalized count-one windows. The live application verifier checked filters/pagination/aggregate sums, conflicting identity, actual SSE data, both Prometheus jobs and Grafana health. Actual DB/broker outages returned 503 and recovered with a same-ID retry.

The current public USGS fetch submitted four observations through the normal gateway; the application check read actual provider-labelled rows. OpenWeather live verification is blocked by the absent key. No Azure resource was deployed or billed.

After one warm-up, three 100-event, four-client, one-key container-loopback runs accepted 300/300 requests. Mean broker-acknowledgment rate was 109.31/s with sample standard deviation 2.94/s; per-run p95 accepted latency ranged about 33.54–49.56 ms. This is a short bounded broker boundary, not sustained end-to-end capacity. The Docker VM exposed about 8.17 GB and 16 logical CPUs; point resource samples are preserved, with no peak/endurance claim. Historical Pandas/Spark and worker-count results remain separately dated and did not show linear scaling.

The deployed text split remains 40 train/10 test examples, accuracy 0.50 and macro F1 0.4467, with fire recall zero. Severity macro F1 0.9971 comes from the same label-conditioned synthetic generator as training and has no established real-world validity. Current training now records confusion matrices, majority baselines, label order, versions and hashes while preserving previous artifacts. No independently labelled anomaly accuracy is measured.

## Challenges and limitations

The central reliability challenge is partial success across independent stores. The solution is ordered commit and stable identity, with a clearly bounded claim rather than invented exactly once. Streaming historical replay introduces watermark/version compatibility; preserving old datasets/checkpoints is safer than deleting state. Tiny-file archival and sequential operational processing remain plausible bottlenecks, but current burst timing cannot rank full-system bottlenecks definitively.

All services share one physical failure domain. Retention, disk loss, authentication, secret rotation, backup restoration, multi-host scaling and production incident governance require further work. Source labels can be spoofed through the unauthenticated local API. Supported typed metadata is preserved, but unknown extra fields are not a universal schema-free archive. SSE polls a bounded latest-500 view with no durable resume cursor. Dashboard aggregate statements are not a single repeatable-read snapshot. Prediction rows do not yet carry full artifact hashes. Independent source-validated labels, richer telemetry and calibration are absent.

Some failure branches are source/mocked evidence only: exact kill inside DB transaction, post-offset-commit kill, MinIO network outage, forced CDN/tile fallback and physical-host loss were not separately executed in this session. The failure table states these limits. Existing deprecated startup hooks still emit warnings. No remote CI execution or new Azure template compile is claimed.

## Future work, in dependency order

First strengthen sustained end-to-end measurement with per-stage latency/lag/resource evidence and retention/backpressure limits. Next define authenticated source/provenance, revision semantics, artifact hashes per prediction and backup restoration. Batch raw writes/compact files only after preserving partition-aware commit/idempotency tests. Add keyset queries or specialized spatial/text indexes when representative query plans justify them. Acquire independently labelled reports and physical severity targets before model tuning or richer frameworks. Vendor map assets for offline demos, broaden accessibility/reconnection tests, then evaluate independent-host infrastructure if the requirements justify cost. Cloud resource creation remains an explicit separate authorization.

## Conclusion

CATACLYSM demonstrates an actual local event-processing system with distinct durability and event-time boundaries, connected experimental inference, real backend-driven visualization and reproducible selected failure evidence. Its strongest contribution is technical transparency: successful broker ingestion, durable operational processing and finalized streaming analytics are distinguished, while data/model/deployment limitations remain visible. It is a defensible academic prototype, not an emergency-response service.

## References and evidence

1. [Apache Kafka design](https://kafka.apache.org/40/design/design/), especially message delivery semantics.
2. [Spark 3.5.1 Dataset API](https://archive.apache.org/dist/spark/docs/3.5.1/api/java/org/apache/spark/sql/Dataset.html), bounded duplicate handling; related [Structured Streaming guide](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html). Deployment here is 3.5.1.
3. [PostgreSQL 15 transaction isolation](https://www.postgresql.org/docs/15/transaction-iso.html).
4. [scikit-learn feature extraction](https://scikit-learn.org/stable/modules/feature_extraction.html) and [evaluation metrics](https://scikit-learn.org/stable/modules/model_evaluation.html).
5. Repository implementation: `src/`, `compose.yaml`, `monitoring/`, `infra/`; exact source snapshots in `CODE_WALKTHROUGH/`.
6. Current structured observations: `evidence/`; current ledger `VERIFICATION.md`; methods `PERFORMANCE_AND_EXPERIMENTS.md`; model report `../models/evaluation.json`.

The literature references explain mechanisms; the checked-in source and dated evidence determine implemented/demonstrated status.
