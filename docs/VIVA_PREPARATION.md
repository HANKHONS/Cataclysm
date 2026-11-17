# Viva preparation

Questions progress from fundamentals to design defense. Answers describe the actual checkout and current evidence, not a hypothetical cloud architecture.

## Foundations and architecture

### 1. What problem does CATACLYSM actually solve?

It normalizes heterogeneous crisis observations, retains a replayable log, materializes operational queries and computes event-time lake aggregates. It demonstrates a credible local processing path; it does not establish that an observation is true or direct emergency response.

### 2. What is the difference between a report and a disaster?

A report is one identified observation. Several reports can describe one disaster, and one report can be wrong. There is no entity-resolution algorithm proving unique disaster identity; UI counts and curated windows therefore count reports.

### 3. What is the actual data flow?

Adapters or synthetic clients POST to FastAPI, which publishes acknowledged Kafka records. The operational consumer runs ML, writes PostgreSQL and raw MinIO, then commits. Independently, Spark writes processed Parquet and sliding counts. API/SSE read PostgreSQL for the dashboard.

### 4. Why not write directly from FastAPI to PostgreSQL?

That would be simpler and probably sufficient for this scale. Kafka is retained to demonstrate asynchronous decoupling, replay, manual recovery semantics and independent Spark consumption. No measurement proves Kafka is required for the current workload.

### 5. Why both a Python consumer and Spark?

The consumer owns per-event operational persistence and classical inference; Spark owns bounded event-time state and file-based aggregates. Neither independently writes the same database schema. The extra complexity is justified educationally by different processing requirements, not by automatic performance gains.

### 6. Is this truly distributed?

The processes communicate over network protocols and separate task/driver/worker roles, so distributed-system behaviors are present. All containers run on one physical host, sharing storage, CPU, RAM and failure domain. It is not multi-host availability or independently scalable infrastructure.

### 7. What proves the dashboard uses real backend state?

The form first receives a Kafka acknowledgment, then waits for the persisted details endpoint. The register and map use database queries; the verifier parses a real database-derived SSE event. Synthetic flags and provider labels are visible. A rendered UI alone would not prove this path.

### 8. Why is ML not the final authority?

Training data are tiny or synthetic and scores are uncalibrated. Reported/source severity and model-predicted severity are separate fields. The model has no validated ability to determine real physical damage, verify reports or prioritize emergency response.

## Kafka and delivery guarantees

### 9. What does a partition offset mean?

It identifies a record position within one topic partition. It is not a topic-wide sequence number. The committed consumer offset is the next position to read; after processing offset 81, committing that message records 82.

### 10. What does the message key control?

The API uses the location string as the key, grouping the same string into a partition under stable partitioner/partition-count assumptions. This preserves partition-local sequence for that location but can create hot spots and does not resolve different spellings of a city.

### 11. What does a consumer group provide?

Each partition is assigned to at most one active consumer in that group. Another group keeps independent offsets and can read the same log. With three partitions, adding more than three consumers to the operational group cannot create more than three active partition owners.

### 12. What does a successful produce call prove?

Only that the client accepted the message into its local producer queue. The API polls delivery callbacks and returns 200 only after broker acknowledgment. Queue acceptance and broker persistence are separate events.

### 13. Does acks=all make the local broker highly available?

No. The local topic has replication factor one. All in-sync replicas then means one broker copy; losing that storage can lose the record. Acknowledgment is not independent-host redundancy.

### 14. What happens if the API returns a timeout?

The queued message may still be accepted later. HTTP 503 is uncertain acceptance, not proof of absence. Retry with the original event ID so an eventual duplicate does not create another operational row.

### 15. Why manual commits?

The consumer commits only after its database transaction and raw archive succeed. Automatic or premature commit could skip a record whose side effect failed. Manual ordering implements at-least-once replay under the documented retention/storage assumptions.

### 16. What happens after a consumer crashes?

Before DB commit, the transaction rolls back and the uncommitted source replays. After DB commit but before offset commit, it replays an already visible row. Keys and first-observation ownership make the operational effect duplicate-safe. The actual crash fixture verified the latter boundary.

### 17. Does Kafka guarantee end-to-end exactly once here?

No. Kafka does not atomically commit the independent PostgreSQL transaction, MinIO PUT and offset. The implementation provides at-least-once execution with idempotent operational IDs; Spark uses its own checkpoint/sink mechanisms and bounded deduplication.

### 18. Why acknowledge DLQ publication before committing source?

Committing first could permanently skip poison input whose diagnostic record was never stored. If DLQ delivery fails, consumption terminates with the source uncommitted. A crash after DLQ acknowledgment but before source commit can still duplicate the DLQ entry.

### 19. How is lag measured?

For each partition, the exporter compares log-end next offset with committed next offset, clamped to retention. It is a committed backlog measure, including in-flight processing; it does not measure Spark lag or tell how many seconds remain to drain.

### 20. What is the largest likely operational bottleneck?

The serial consumer does inference, several SQL operations and a one-event Parquet object PUT before commit. Tiny-file remote writes are a plausible bottleneck. Current HTTP-to-broker tests do not identify the largest measured end-to-end bottleneck; sustained lag and stage timings would be required.

## Spark and event time

### 21. What are driver, master, worker and executor?

The driver owns the application plan/query progress. The standalone master allocates resources. Workers launch executors; executors run tasks and hold application state/cache. They are distinct roles even when hosted on the same PC.

### 22. What is lazy evaluation?

DataFrame transformations construct a logical plan rather than immediately iterating Python records. An action or streaming microbatch triggers optimized execution. This permits projection/filter optimization but means errors can surface at query start or execution rather than transformation construction.

### 23. Why define a Spark schema explicitly?

Kafka values are bytes, not typed rows. from_json applies declared field types, and later filters reject missing identity or unparsable time. This schema is not identical to full Pydantic validation; operational DLQ and Spark filtering have different boundaries.

### 24. What does a microbatch contain?

A bounded source-offset interval scheduled as tasks, with transformations/state/output progress committed for recovery. The production trigger requests ten-second intervals; a long batch does not magically finish at ten seconds or execute overlapping copies of itself.

### 25. How do event time and processing time differ?

Event time comes from the source observation; processing time comes from when the system executes. A historical report can arrive now with yesterday's event time. Window membership uses event time, while service latency and host scheduling use processing-time clocks.

### 26. What does the ten-minute watermark actually do?

It expresses tolerated event-time delay and allows state eviction/final output as maximum observed event time progresses. It is not wall-clock now minus ten minutes and is not a guarantee that every event older than ten minutes is always dropped at one exact boundary.

### 27. Why can a quiet stream have no finalized recent windows?

Its maximum event time may stop advancing, so the watermark does not pass those window ends. Append-mode output emits finalized windows, not provisional counters. Inspect progress and watermark before diagnosing a missing output as failure.

### 28. Is Spark deduplication permanent?

No. dropDuplicatesWithinWatermark bounds its event-ID state by watermark semantics. PostgreSQL identity persists indefinitely, but old replay can be discarded or later duplicates outside the protected horizon can differ from permanent-key behavior.

### 29. Why does one event count in two windows?

The configured windows are ten minutes wide and start every five minutes. A 12:07 report belongs to [12:00,12:10) and [12:05,12:15). Summing all sliding-window counts therefore does not equal unique report count.

### 30. Why are there two stateful queries?

The first validates/transforms/deduplicates to a committed processed Parquet sink. The second reads that dataset and maintains window aggregation state. This separates stateful stages, checkpoint ownership and restart behavior rather than chaining incompatible stateful output semantics.

### 31. What is in a checkpoint?

Source offsets/progress, state-store data and query/sink commit metadata needed to resume the same logical computation. It is not the analytical dataset itself. Driver and worker mounts preserve it; incompatible schema/query changes need deliberate versioning.

### 32. Can two drivers use one checkpoint?

No. They would contend over query progress/state ownership. Only the configured streaming driver owns each v2 production checkpoint; test scripts create unique isolated directories.

### 33. How was late behavior actually tested?

The isolated Spark verifier submitted controlled file microbatches to the actual clean_events function, restarted from its checkpoint, advanced event time, then sent an old event. Readback excluded the old ID and the state metric counted one watermark drop. Production Kafka integration is tested separately.

### 34. Why might more Spark workers make this slower?

Scheduling, shuffle, serialization and shared-host contention can exceed useful parallel work for small datasets. Historical one/two/four-worker results did not show linear speedup. Resource count is not a substitute for measured workload size and partition behavior.

## Database and lake storage

### 35. Why both PostgreSQL and Parquet?

PostgreSQL provides transactional keyed records and indexed operational queries. Parquet in object storage supports compressed typed analytics and historical replay. They optimize different access patterns and do not share one transaction.

### 36. What makes duplicate inserts safe?

Primary keys on event ID and event/model pairs enforce identity in the database, including concurrent sessions. ON CONFLICT DO NOTHING preserves the first observation. A Python set alone would not survive restart or coordinate processes.

### 37. How are conflicting observations with the same ID handled?

First accepted facts and predictions remain. The consumer reads the stored report when a duplicate exists and uses it for the raw archive, preventing a conflicting retry from overwriting canonical content. A correction requires an explicit new/revision identity policy.

### 38. What does a transaction protect?

Event, location/report relation and prediction writes either commit together or roll back together within PostgreSQL. It does not include the later MinIO PUT or Kafka commit. Database row visibility can precede complete pipeline processing.

### 39. Why does an index not always speed a query?

An index has lookup and random-page cost and helps only suitable predicates/selectivity/order. A tiny table or broad aggregate can favor a sequential scan. Leading-wildcard search and large offsets need separate investigation, not blanket claims from one timestamp query plan.

### 40. Why Parquet instead of CSV?

Parquet stores typed column chunks with compression and supports column projection. CSV is interoperable but lacks equivalent typing/columnar layout. The tiny raw object strategy still incurs high per-file overhead, so format choice alone does not make storage efficient.

### 41. Why preserve raw_payload in Parquet?

Flattened telemetry columns omit magnitude, provider URL and provenance unless stored separately. The complete supported enriched JSON permits faithful replay and distinguishes missing from measured zero. Legacy flattened-only rows have less information.

### 42. Does the lake archive have immutable WORM semantics?

No. Event-derived object keys are PUT again on retries. The current canonical-content policy makes ordinary retry overwrite consistent, but object storage/versioning or retention lock would be needed for physical immutability.

### 43. Why can raw, processed and curated counts disagree?

Raw/DB retain late observations; Spark can drop them. Processed data is per accepted report while curated data aggregates overlapping windows and emits only finalized windows. Old v1 prefixes also remain preserved. Compare known inputs and defined boundaries rather than requiring all counts equal.

### 44. What is safe historical replay?

Restore original ID/time/metadata, mark replay, publish through Kafka and confirm callbacks. Replaying to a progressed Spark checkpoint may be too late; a separate versioned historical rebuild is needed for complete analytics. Do not erase state to make a demo look fresh.

## Machine learning and evaluation

### 45. What is the text prediction target?

One of five hand-labelled hazard categories in a curated 50-example English corpus. The TF-IDF/LR pipeline predicts a textual category, not report credibility, disaster existence or physical severity.

### 46. Why fit TF-IDF inside a pipeline?

The vectorizer must learn vocabulary/IDF from training data only and apply identical preprocessing at inference. Serializing the pipeline keeps those transformations with classifier coefficients and avoids fitting feature statistics on held-out text.

### 47. What does TF-IDF mathematically do?

Term count is multiplied by smoothed inverse document frequency log((1+n)/(1+df))+1, then the document vector is L2-normalized. Common terms get less discriminating weight; sparse representation avoids storing zero vocabulary coordinates. The concepts chapter derives an example.

### 48. What is logistic regression learning?

Class-specific linear logits in sparse feature space, converted through a softmax and fitted with regularized weighted cross-entropy. A linear decision boundary in TF-IDF space can capture lexical cues; it does not imply mechanistic understanding of disasters.

### 49. What is the current honest text result?

The fixed ten-example holdout has accuracy 0.50 and macro F1 about 0.4467; fire has no correct predictions. Two examples per class provide very weak generalization evidence. The model is connected, but useful operational performance is unproved.

### 50. Why is severity F1 near one not persuasive?

The generator samples severity first, then draws features from severity-specific distributions. Train/test share those constructed rules. High held-out accuracy recovers generator structure and says little about physical hazards outside that construction.

### 51. How is class imbalance handled and reported?

Class-balanced estimator weighting is used, and stored evaluation includes class supports, per-class precision/recall/F1, macro and weighted averages, accuracy and majority baseline. Weighting cannot repair poor labels or too few independent examples.

### 52. How would you improve evaluation without leakage?

Collect independent source-labelled reports with a defined target, split by event/source/time or template group, fit preprocessing only on train, tune on validation and reserve test for final assessment. Compare a simple baseline on the same split and report uncertainty/failure cases.

### 53. Is prediction_confidence a probability of correctness?

No. It is the uncalibrated maximum text-class score; unrelated severity probabilities are not averaged into it. Heuristic fallback scores are zero; all-missing telemetry yields unknown. Calibration would require independent held-out evidence and explicit evaluation.

### 54. What does Isolation Forest detect?

Unusual completed hourly report counts relative to recent historical bins, not independently confirmed crises. A separate maximum-training-count guard handles extrapolation limits. No labelled anomaly accuracy is available and 5% contamination is a threshold choice, not measured prevalence.

### 55. What is model versioning here?

Training saves timestamp-versioned artifacts, preserves prior files, records SHA-256 hashes, scikit-learn version, splits/metrics and retains the deployed filenames for compatibility. Services load once per process and require restart after training. The prediction table does not yet store a full artifact hash per prediction.

### 56. Why use CPU rather than GPU?

Sparse linear scoring and 100 shallow trees are small CPU workloads. GPU setup, transfers and new dependencies add cost without demonstrated benefit. The meaningful bottleneck may be serial storage I/O rather than arithmetic; measure before optimizing hardware.

## Infrastructure, frontend and monitoring

### 57. What survives a container restart?

Named volumes retain DB/broker/MinIO/checkpoints, and bind-mounted checkout files retain source/model/evidence. Writable-layer-only files may disappear on recreation. None survives arbitrary physical disk loss without independent backups.

### 58. What is the difference between liveness and readiness?

Liveness indicates the API process answers; readiness also requires DB/broker. The descriptive health route retains its healthy/degraded fields. None of these individually proves Spark output freshness or complete sink durability.

### 59. How does SSE behave?

The server polls the latest 500 DB rows every two seconds and emits changed JSON fingerprints plus heartbeats. The browser debounces filtered refreshes and EventSource reconnects. There is no durable Last-Event-ID cursor, so this is bounded notification rather than an exhaustive event log.

### 60. What UI statistics are authoritative?

Period totals/distributions come from server aggregates across all matching persisted rows; loaded feed count is separate. Recent persistence counts new IDs by created_at in the last minute under selected filters, not Kafka arrival throughput or unique disaster count.

### 61. How do you prevent XSS in observation text?

The browser constructs escaped text through textContent-based encoding before inserting templates. Provider links are restricted to HTTP/HTTPS and opened with noopener/noreferrer. This does not replace a full security review or authenticated input boundaries.

### 62. What happens when maps cannot load?

The primary tile layer attempts a secondary fallback; final error text preserves the register. A missing Leaflet library explains that mapping requires external access. Full offline use still requires vendoring assets; that was not claimed as verified.

### 63. What do Prometheus metrics really establish?

API counters/histograms measure HTTP behavior; the exporter probes DB/Kafka/MinIO and reports lag, objects and Spark progress. Scrape up means collection succeeded, not complete correctness. Old gauge samples and idle progress require timestamp/dependency context.

### 64. Has Azure been deployed?

No. Infrastructure files describe a single Ubuntu VM with restricted SSH and optional disk. Historical local template compilation is documented; this execution did not validate a live deployment, spend credits or create resources.

## Advanced defense and limitations

### 65. What would need to change before emergency-response use?

Independent operational requirements and hazard validation, authenticated sources and operators, privacy/retention controls, calibrated and monitored models, independent-host redundancy, tested backup restoration, incident procedures, security review and domain-expert governance. A polished academic UI is insufficient.

### 66. What experiment would you run next?

A sustained controlled ingest rate with stage timestamps, complete sink acknowledgments, lag/drain time and resource sampling across repeated runs. Increase offered load until backlog grows, preserve all failures, and isolate source/key distribution. Current short broker-ack bursts cannot locate full-system capacity.

### 67. Can the architecture guarantee no data loss forever?

No. Retention can expire, disks can fail, credentials can revoke access, schema changes can break replay and all services share one host. State the tested failure model and retention assumptions. At-least-once describes processing under those assumptions, not universal permanence.

### 68. Why not hide the weak ML results in the viva?

That would invalidate the engineering claim. A defensible project separates connected inference from validated predictive utility and shows the actual holdout/confusion matrix. Identifying the failure mechanism and proposing an independent evaluation is stronger than quoting synthetic accuracy as reality.


Total: 68 questions. Study the architecture, trace and concepts before memorizing answers; reproduce the named experiments and explain their limits.
