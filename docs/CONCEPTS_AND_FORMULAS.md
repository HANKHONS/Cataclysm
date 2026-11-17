# Concepts and formulas tied to CATACLYSM

Read the architecture and event trace first. Each concept below connects intuition, a mathematical model, computational cost and the actual implementation. Units and boundary conditions matter more than impressive terminology.

## 1. Events, ordering and clocks

An event is an observation, not an independently confirmed disaster. Its ID controls identity; its timestamp is source event time. `created_at` measures first operational persistence; Spark `processed_at` is processing time. Event latency can include provider delay, network delay and processing delay. Subtracting timestamps from unsynchronized clocks can give a negative result; the UI explicitly calls out future timestamps.

For event time \(t_e\) and processing time \(t_p\), delay is \(d=t_p-t_e\), in seconds. A provider observation at 10:00 received at 10:07 has seven minutes delay even if API processing takes 40 ms. This delay does not measure API latency. `CrisisEvent.normalize_timestamp` normalizes offsets to UTC; legacy naive input is interpreted as UTC. Equality of timestamps does not imply identical observations.

## 2. Kafka log, partitions and keys

Think of a partition as an append-only indexed notebook. A broker stores notebooks; a topic groups them. A record has a key, payload and partition-local offset. The API keys by location, so equal location strings route together under the producer partitioner while partition count remains stable. Kafka does not promise global ordering between notebooks; changing partition count can change key placement.

If there are \(P\) partitions and \(C\) consumers in one group, useful simultaneous partition owners are bounded by \(\min(P,C)\). With three partitions and five consumers, at most three have assigned partitions. Separate groups keep independent offsets and each receive the log; this is why Spark can process the same events as the operational consumer. Assignment/rebalancing temporarily changes ownership after failures. Broker replication factor one is unrelated to consumer count.

Producer serialization is \(O(B)\) for \(B\) payload bytes. Log append is sequential storage I/O, but its actual latency includes batching, network and durability configuration. Consumer model/database/object-store work dominates many small local events. A key concentrating all reports in one city can create a hot partition even if other partitions are idle. Do not assume three partitions imply a threefold throughput improvement.

## 3. Acknowledgments, retries and delivery semantics

`produce()` returning means local enqueue, not durable broker delivery. `ingest_event` polls the callback and checks its result. `acks=all` waits for the in-sync replicas configured for that partition; with one replica this still means one copy. The API can time out while Kafka later accepts the queued record. Therefore HTTP 503 can mean uncertain acceptance, and retries must retain identity.

Let \(W\) mean all required sink writes have completed and \(K\) mean the source offset is committed. The intended ordering is \(W\prec K\). A crash between them causes replay; reversing them permits loss. The consumer implements DB transaction, raw PUT, then synchronous commit. Retry sleep is \(b_a=\min(0.5\cdot2^{a-1},5)\) seconds for attempt \(a\); the configured five delays are 0.5, 1, 2, 4 and 5 seconds. These sleeps do not include pool/network timeout, so their sum is not a total recovery bound.

At-most-once would commit before durable processing. At-least-once can execute writes more than once. Exactly-once effects require an atomic boundary or idempotent identities spanning the desired scope. Kafka transactions alone would not atomically commit this PostgreSQL transaction and MinIO PUT. The system explicitly claims at-least-once with idempotent operational IDs, not global exactly once. DLQ routing has the same write-before-commit requirement and can itself replay.

## 4. Idempotency and transactions: a short proof

A function \(f\) is idempotent when \(f(f(x))=f(x)\). For an event ID \(i\), define the operational state as a map \(D:i\mapsto r\). First-observation insertion is

\[
F_i(D)=\begin{cases}D\cup\{i\mapsto r\},&i\notin\operatorname{dom}(D),\\D,&i\in\operatorname{dom}(D).\end{cases}
\]

After one application, \(i\) is in the domain, so a second application leaves the map unchanged. Thus \(F_i(F_i(D))=F_i(D)\). PostgreSQL's primary key plus `ON CONFLICT DO NOTHING` enforces this across concurrent inserts, not merely inside one Python process. The `predictions` composite key applies the same rule per model. This proof assumes a stable identity: assigning a new UUID on every retry defeats it. It proves cardinality/first-write behavior, not provider truth, cross-sink atomicity or protection against disk loss.

A database transaction groups event/report/prediction writes so failure rolls them all back. Atomicity, consistency, isolation and durability are ACID properties within PostgreSQL. Default read-committed isolation prevents dirty reads; separate dashboard aggregate queries may see different committed snapshots during ingestion. `conn.transaction()` is not a distributed transaction. `locations` uniqueness prevents exact duplicate triples; floating-point near-equal locations are not geographic entity resolution.

B-tree equality/range lookup is approximately \(O(\log N+k)\) for \(N\) entries and \(k\) returned rows; actual page I/O and selectivity matter. Index writes add maintenance cost. A leading-wildcard `ILIKE` search is generally \(O(NL)\) without a suitable specialized index, for average text length \(L\). Offset pagination still skips rows, roughly \(O(o+k)\) after index positioning for offset \(o\); high offsets warrant keyset pagination. Current query-plan evidence is one small warmed query, not a universal latency guarantee.

## 5. Lag, backpressure and stability

For partition \(p\), let \(H_p\) be the log-end *next* offset and \(C_p\) the consumer's committed *next* offset. Approximate committed lag is

\[
L_p=\max\bigl(0,H_p-\max(C_p,\ell_p)\bigr),
\]

where \(\ell_p\) is the earliest retained offset. The exporter clamps missing/expired offsets against retention. If \(H_p=120\), \(C_p=115\), lag is five records, not six. Committed lag includes work currently in flight and says nothing about another group's progress.

With mean arrival rate \(\lambda\) and processing capacity \(\mu\), backlog evolves approximately as \(\Delta L/\Delta t\approx\lambda-\mu\) while work is available. Sustained \(\lambda>\mu\) makes queues grow. A bounded burst can be accepted quickly into Kafka while the consumer drains slowly. Backpressure here includes producer buffer rejection, Spark `maxOffsetsPerTrigger=10000`, and visible lag; it is not an end-to-end admission controller.

Little's law \(Q=\lambda W\) relates mean in-system items \(Q\), stable throughput \(\lambda\) (items/s), and mean residence time \(W\) (s). For 20 reports/s and 0.5 s mean residence, expect 10 reports in flight. This requires a stable measured boundary and long-run averages. Applying it to a growing queue or combining broker throughput with unrelated event-time delay is invalid.

## 6. Spark execution and data structures

The driver constructs logical plans, coordinates queries and tracks progress; the standalone master allocates worker resources; executors run tasks. A worker process is not itself the application driver. A DataFrame is a distributed typed relation and plan, not a Python list of all rows. Transformations such as select/filter/withColumn construct plans lazily; actions and streaming triggers execute them.

A microbatch obtains source offsets, schedules work, applies state, writes output and commits progress. Parsing/filtering \(N\) records with average payload length \(B\) costs roughly \(O(NB)\). Grouping/deduplication shuffles keys across partitions; distributed cost includes serialization, network and coordination, not just arithmetic Big-O. Raising shuffle partitions on this tiny single-worker cluster can increase task overhead. Driver heap, executor heap, Python processes, off-heap buffers, caches and the OS all share the Docker allocation.

`clean_events` parses schema, drops null identity/time and unparsable event timestamps, then runs stateful deduplication. Spark's schema is narrower than Pydantic and does not reproduce every gateway check. Direct malformed Kafka input can be routed to the operational DLQ while being dropped or partially parsed independently by Spark. This is a documented boundary, not a shared exactly-once validation system.

## 7. Watermarks, state and sliding windows

A useful conceptual watermark is \(w\approx t_{\max}-\delta\), where \(t_{\max}\) is maximum observed event time and \(\delta=10\) minutes is allowed lateness. Actual watermark propagation across batches/stateful operators is delayed; it is not simply wall-clock now minus ten minutes. An idle input may never finalize a current window. Events inside the lateness threshold have a one-sided no-drop guarantee; events beyond it may be dropped, and exact equality/batch timing is not a portable acceptance contract.

Bounded dedup retains event IDs long enough to recognize duplicates within its configured event-time horizon. Expected state size is on the order of \(O(\lambda\delta)\) under a regular stream of unique IDs; disorder and implementation eviction details can increase it. At 50 unique IDs/s and a 600-second horizon, a first estimate is 30,000 retained IDs, before state-store overhead. PostgreSQL's primary key is permanent; bounded Spark state is not.

For width \(W=10\) minutes, slide \(S=5\) minutes and aligned window index \(j\), a record at time \(t\) belongs when

\[
jS\le t<jS+W.
\]

At 12:07 it belongs to [12:00,12:10) and [12:05,12:15). Half-open intervals assign boundary times consistently. Since \(W/S=2\), a typical event contributes to two windows. Summing all window counts double-counts reports; calling that sum unique incidents is wrong. Append-mode output waits until the watermark passes the window end. The isolated test reads back two count-one windows for its first event.

Checkpointing persists source progress, state and sink commit information. Restart uses the same logical query and checkpoint to continue. Reusing a checkpoint after incompatible state/schema/output changes can fail or corrupt meaning. Two drivers cannot share one query checkpoint. The production v2 checkpoint is on a shared named volume; test checkpoints are unique separate directories. An immutable Parquet file sink and its commit log are part of Spark recovery semantics, distinct from the raw consumer's overwrite-by-key strategy.

## 8. Lake layout, columnar storage and replay

A row store favors transactional updates and indexed narrow queries. A lake stores files; Parquet groups values by column, enabling projection and compression. Reading only event IDs in the smoke script avoids decoding unrelated columns, although remote object transfer still has its own cost. Partition directories encode low/moderate-cardinality dimensions for pruning. Partitioning raw storage by ID would create many directories; instead identity is the filename.

For uncompressed bytes \(U\) and compressed bytes \(C\), compression ratio \(R=U/C\) is unitless. If a 10 MB payload becomes 2 MB, \(R=5\); this example is illustrative, not a measured project ratio. Tiny one-event objects pay metadata, request and footer overhead and can compress poorly. Batching would improve file sizes but requires explicit flush/retry/idempotency semantics.

Raw Arrow timestamp columns use millisecond precision and no timezone annotation; the complete raw JSON preserves the original normalized ISO timestamp. Legacy reconstructed replay is less expressive; prefer `raw_payload`. `pq.ParquetFile` reads one file without Hive partition inference, avoiding conflicts between an encoded string column and a directory-derived dictionary column. Replay preserves IDs/event time, adds `is_replay`, verifies callbacks and limits by rows. It cannot force an old event through a progressed Spark watermark.

## 9. TF-IDF and sparse linear classification

Intuition: a frequent term within a report can be useful, but a term appearing in almost every training report carries less discriminating information. The deployed vectorizer uses unigrams/bigrams, English stopword removal and training-only vocabulary/IDF fitting. For \(n\) training documents, document frequency \(\mathrm{df}(t)\), and term count \(\mathrm{tf}(t,d)\), the smoothed weight is

\[
\mathrm{idf}(t)=\log\frac{1+n}{1+\mathrm{df}(t)}+1,\qquad
z_{dt}=\mathrm{tf}(t,d)\mathrm{idf}(t),\qquad
x_d=\frac{z_d}{\lVert z_d\rVert_2}.
\]

All quantities are unitless. If \(n=9\) and a term occurs in one document, IDF is \(\log(10/2)+1\approx2.609\). A term in all nine gets IDF one. A zero vector remains zero under the library's safe normalization. Unknown terms do not invent new vocabulary at inference. Source: [scikit-learn feature extraction](https://scikit-learn.org/stable/modules/feature_extraction.html).

Let \(K\) be class count, \(V\) vocabulary size, \(W\in\mathbb{R}^{K\times V}\), \(b\in\mathbb{R}^K\). Multinomial logistic regression computes

\[
p(y=k\mid x)=\frac{\exp(w_k^\top x+b_k)}{\sum_{j=1}^K\exp(w_j^\top x+b_j)}.
\]

Regularized weighted cross-entropy trades training fit against coefficient size. One compatible mathematical form is

\[
\mathcal L= -\frac{1}{\sum_i a_i}\sum_i a_i\log p(y_i\mid x_i)
+\frac{\alpha}{2}\lVert W\rVert_F^2.
\]

Here \(a_i\) are class-derived sample weights, \(\alpha\) denotes the effective regularization strength; scikit-learn parameterizes it through inverse strength `C` with solver-dependent objective scaling. Do not substitute `alpha=1/C` without accounting for that normalization. Differentiating an unweighted sample yields \(\partial\ell_i/\partial w_k=(p_{ik}-\mathbf1[y_i=k])x_i\): a predicted probability above the target indicator pushes its weight in the negative gradient direction. The linear model's objective is convex in its coefficients; that does not make its predictions causally true or calibrated.

Sparse scoring costs \(O(K\,\mathrm{nnz}(x))\) and coefficient storage \(O(KV)\). Dense vocabulary-length vectors waste memory when most terms are absent. Training is approximately repeated sparse matrix/gradient operations, depending on solver iterations. These small models use CPU; launching a GPU would add unmeasured overhead. A high maximum class probability is a relative model score, not the chance a real disaster exists.

## 10. Random Forest and synthetic severity

A decision tree asks feature-threshold questions. A random forest averages many trees trained with randomness, reducing variance when their errors are not perfectly correlated. For tree class distributions \(p_{tk}(x)\), the forest probability is \(\hat p_k(x)=T^{-1}\sum_{t=1}^{T}p_{tk}(x)\); predicted class maximizes it. With two illustrative trees giving class probabilities 0.8 and 0.6, their mean is 0.7, which remains uncalibrated.

The deployed forest uses 100 trees, maximum depth eight and four weather features. Inference costs \(O(Td)\) path comparisons for depth \(d\), plus class aggregation; storage scales with total nodes. A rough fitting bound for balanced trees is \(O(Tmn\log n)\) under particular split-search assumptions, with \(m\) candidate features, not a universal implementation guarantee. Missing features are currently hand-imputed, not inferred by a calibrated missingness model.

The severity training generator first samples a severity label and then samples rainfall/water/wind/temperature from label-specific ranges. Evaluation on the same generator mostly measures recovery of those construction rules. Its approximately 0.997 macro F1 is genuine computation but weak external evidence. Independence of train/test rows does not establish independence of the data-generating assumptions. Weather features cannot infer earthquake damage from magnitude, building quality or exposure, because those variables are absent.

## 11. Evaluation, leakage and baselines

For one class viewed against all others, true positives \(TP\), false positives \(FP\) and false negatives \(FN\) give

\[
P=\frac{TP}{TP+FP},\quad R=\frac{TP}{TP+FN},\quad
F_1=\frac{2PR}{P+R}=\frac{2TP}{2TP+FP+FN}.
\]

The final equality follows by substituting the first two fractions and canceling denominators. If \(TP=8,FP=2,FN=4\), precision is 0.8, recall 2/3 and F1 approximately 0.727. These are unitless. Zero-denominator cases require an explicit convention; stored reports use `zero_division=0`. Accuracy is correct classifications divided by all test examples; on 95% noncrisis data, predicting noncrisis always scores 95% while missing all crises.

For class F1 values \(f_k\), supports \(n_k\), \(N=\sum n_k\):

\[
F_{\mathrm{macro}}=\frac1K\sum_k f_k,\qquad
F_{\mathrm{weighted}}=\sum_k\frac{n_k}{N}f_k.
\]

For F1 values 0.9 and 0.3, macro is 0.6; with supports 90 and 10, weighted is 0.84. Macro exposes weak rare classes. A confusion matrix \(M_{ij}\) counts true class \(i\) predicted as \(j\); diagonal entries are correct. `models/evaluation.json` records label order, matrices, full reports and a majority-class baseline on the same holdout. The text split has only ten examples, two per class; one error changes accuracy by ten percentage points. A second validation set is not meaningful at this size, and the fixed test split must not be used for tuning. Collect independent labelled data and use grouped/time-separated splits before model selection.

The pipeline fits TF-IDF on training data only. Fitting IDF on the entire corpus leaks test distribution. Splitting paraphrases of a shared template across train/test leaks linguistic structure. Neither severity synthetic accuracy nor anomaly flag rate measures real-world predictive utility. Compare baselines on the same evidence and report failure classes, not only an average. See [classification metrics](https://scikit-learn.org/stable/modules/model_evaluation.html).

## 12. Isolation Forest and guards

An Isolation Forest isolates unusual feature values with short random partition paths. The report-rate implementation fits on historical completed hourly counts of shape \([n_{hours},1]\), requires at least 20 training bins and scores the next completed bin. It avoids scoring a partial current hour. Empty historical hours are generated as zero-count observations rather than silently removed.

For subsample size \(\psi>1\), expected unsuccessful search path normalization is

\[
c(\psi)=2H_{\psi-1}-\frac{2(\psi-1)}{\psi},\qquad
s(x)=2^{-\mathbb E[h(x)]/c(\psi)}.
\]

\(H_n=\sum_{j=1}^{n}1/j\) and path length \(h\) counts tree edges; both are unitless. A shorter average path gives a larger anomaly score. For an illustrative \(c=4\), paths two and four produce scores approximately 0.707 and 0.5. The library's `decision_function` applies its own offset/sign convention; project code tests a negative result through `predict` plus a separate boundary guard. Do not equate the conceptual \(s\) directly with the stored decision value.

Training costs roughly \(O(T\psi\log\psi)\); scoring one point costs \(O(T\log\psi)\) on typical trees. The 5% contamination parameter chooses a threshold from training scores; it is not a measured anomaly prevalence or accuracy. Very large unseen counts can share a terminal boundary leaf. `outside_training_range` explicitly flags values above the historical maximum; this is a heuristic guard, not learned extrapolation. New source coverage, time-zone patterns and polling outages can all change report rate without changing actual hazards. There are no independently labelled anomaly accuracy results.

## 13. Performance formulas and measurement boundaries

For \(N\) accepted records over measured wall time \(T\) seconds, throughput \(r=N/T\) has records/s units. Latency for request \(i\) is \(l_i=t_{end,i}-t_{start,i}\) seconds. A percentile describes a sample distribution, not a guarantee for every future request. The load script's p95 is the sorted item at zero-based index \(\lfloor0.95n\rfloor\), clamped to the last element; with 100 samples that is the 96th order statistic. Different percentile conventions differ on small samples.

Error rate is \(N_{failed}/N_{attempted}\), measured over a stated interval and endpoint. Successful-request latency excludes failed requests; report both populations. With 100 accepted in 0.926 s, broker-ack throughput is about 108/s. The current three-run mean is about 109.3/s, sample standard deviation 2.94/s; it is not a rate at which all sinks were verified continuously. No current end-to-end p95 is measured.

For repetitions \(r_j\), sample mean \(\bar r=m^{-1}\sum r_j\) and sample standard deviation \(s=\sqrt{\sum(r_j-\bar r)^2/(m-1)}\) summarize observed variation, assuming \(m>1\). Three runs are not a robust tail/variance study. The benchmark keeps a clear warm-up/repeat policy; process/host contention remains a confound.

Parallel speedup \(S_p=T_1/T_p\), efficiency \(E_p=S_p/p\). If one worker takes 0.3065 s and two take 0.2828 s (historical experiment), speedup is 1.084 and efficiency 0.542. Both are unitless. Amdahl's idealized bound \(S_p\le1/[(1-f)+f/p]\) assumes parallel fraction \(f\), fixed workload and no extra overhead: if half the work is parallel, four processors cannot exceed 1.6x. Actual scheduling/shuffle costs can make speedup below one. Do not infer multi-host scaling from several containers on one PC.

## 14. Containers, persistence and observability

An image is a layered filesystem/program definition; a container is a running process environment. Container DNS names refer to the Compose network, whereas `localhost` inside a container means that container. Host bindings expose chosen ports only to loopback. A named volume survives container recreation; a bind mount exposes checkout files. Neither substitutes for an independent backup.

Liveness means a process can respond; readiness means its dependencies support work. HTTP `/ready` checks DB/broker; it does not certify Spark/lake completion. Metrics are numeric time series with label dimensions. Counters accumulate; rates derive changes per time. Gauges represent current values. A histogram supports quantile estimates over bucketed observations; a p95 estimate is not raw request tracing. Scrape `up=1` means the scrape succeeded, not that all application logic is correct. Spark progress age can increase during idle streams and is not alone proof a job died. Gauge values can remain stale after dependency failure, so inspect probe status and timestamps together.

Cloud IaaS would move the same host failure domain to a VM. The Bicep file prepares a VM, SSH-restricted networking and optional disk; it does not create an authenticated public crisis service, multi-node Kafka or replicated storage. This session created no Azure resources.

## Primary references and scope

- [Kafka design / delivery semantics](https://kafka.apache.org/40/design/design/) explains broker/consumer boundaries; repository configuration remains the authority for actual settings.
- [Spark 3.5.1 Dataset API](https://archive.apache.org/dist/spark/docs/3.5.1/api/java/org/apache/spark/sql/Dataset.html) specifies bounded deduplication. [Spark Structured Streaming guide](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html) explains event-time/window guarantees; deployed version is 3.5.1 and its behavior is tested locally.
- [PostgreSQL 15 isolation](https://www.postgresql.org/docs/15/transaction-iso.html) describes transaction boundaries.
- Scikit-learn references above support terminology/equations; `src/ml/train.py` and evidence determine what this project measured.
