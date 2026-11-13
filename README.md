# Spark Real-time Mode (RTM) vs Apache Flink: reproducible benchmark

Kafka → transform → Kafka pipelines run on **Apache Spark Real-time Mode** and **Apache Flink**
with identical data, resources and placement, comparing **latency, CPU, memory, cost, fault
tolerance and scaling**, locally in Docker and on EKS.

This repo has two rounds:

1. **[Spark 4.3 vs Flink 2.3](#latest-spark-430-rtm-vs-flink-230-stateless-stateful-joins-scaling)**
   (latest): stateless, three stateful workloads, two joins, recovery and scaling, all on EKS.
   Write-up: [`post/piece1-4.3.md`](post/piece1-4.3.md).
2. **[Spark 4.1 vs Flink 2.2](#original-benchmark-spark-412-rtm-vs-flink-220-stateless)**
   (original): the stateless pipeline, locally and on EKS. Write-up: [`REPORT.md`](REPORT.md).

---

# Latest: Spark 4.3.0 RTM vs Flink 2.3.0 (stateless, stateful, joins, scaling)

Open-source Apache Spark 4.3.0 (`apache/spark:4.3.0`, not Databricks Runtime) against Apache
Flink 2.3.0 on EKS: 6 × m5.2xlarge, Strimzi Kafka (3 brokers, RF 3, 6 partitions), durable S3
checkpoints every 60 s, two dedicated engine nodes with identical pods for both engines
(1 coordinator of 1 core / 2 GB, 2 workers of 3 cores / 6 GB). Each run measures 30 minutes
after a 60 s warm-up; stateless at 100k evt/s, stateful at 50k evt/s. Latency is pipeline
latency, `out_ts − ts` per record.

![Spark 4.3 fixed the stateless RTM tail. Stateful RTM still commits synchronously.](post/charts/00_hero.png)

**Headline results** (full write-up in [`post/piece1-4.3.md`](post/piece1-4.3.md), per-run
numbers in [`results/final30/RESULTS.md`](results/final30/RESULTS.md)):

- **Stateless: the tail gap is gone.** RTM's p99 dropped from 642 ms (4.1.2) to ~30 ms, because
  4.3 moves offset and commit-log writes to an async thread
  ([SPARK-58131](https://issues.apache.org/jira/browse/SPARK-58131)). p99 is now roughly a tie
  (Flink 27.4 ms vs Spark RTM 30.3 ms), at a ~6 ms median for both. Flink still uses less memory.
- **Stateful: tuned Flink wins every workload both engines can run.** OSS stateful RTM commits
  state synchronously at each batch boundary (async state checkpointing is DBR-only).

  | workload (50k evt/s, both tuned) | Flink p50 / p99 | Spark RTM p50 / p99 |
  |---|---|---|
  | S1 10 s tumbling count/sum per user | 6.9 ms / 1.6 s | 13.4 ms / 4.5 s |
  | S2 dedup on `event_id` | 7.0 ms / 1.7 s | 13.1 ms / 4.3 s |
  | S3 per-user session state machine with timers | 8.3 ms / 4.3 s, keeps up | falls behind: 29.7k evt/s out (0.76x Flink), lag grows +17 s/min |

- **Recovery** (worker killed mid-stream): Flink is back in about a minute; Spark had not
  recovered within the measurement window.
- **Joins:** S4 stream-static enrichment ties at the median (~6.5 ms), but RTM's p99 is ~13.5x
  Flink's (275 vs 20 ms). S5 stream-stream interval join is not supported in RTM; against Spark
  micro-batch, Flink's interval join wins (p99 0.65-0.69 s vs 5.5-5.7 s).
- **Scaling** (S1, 50k → 100k → 50k): both kept state exact through every rescale. Only static
  Flink sized for the peak kept p99 under 2 s; the Flink operator autoscaler halved node-hours at
  ~2.75 s p99. OSS stateful RTM has no autoscaling path (it rejects dynamic allocation).
- **Cost:** same hardware for both, so the same $/hour; the difference is how much load each
  sustains on it.

## Workloads

| id | workload | Spark | Flink |
|---|---|---|---|
| stateless | parse, filter, enrich (the original pipeline) | RTM: Java, Scala, PySpark | DataStream, SQL, PyFlink |
| S1 | 10 s tumbling count/sum per user | RTM | DataStream, SQL |
| S2 | dedup on `event_id` | RTM | DataStream, SQL |
| S3 | per-user session state machine with timers | RTM (`transformWithState`) | DataStream (`KeyedProcessFunction`) |
| S4 | stream-static enrichment join | RTM (broadcast join) | DataStream |
| S5 | stream-stream interval join | micro-batch (unsupported in RTM) | DataStream interval join |

Each stateful workload runs as a 2×2 (both engines at defaults and tuned: Spark shuffle flush
50 → 1 ms, Flink buffer timeout 100 → 0 ms) plus a Spark **vendor-tuned** arm using the OSS-
applicable settings from
[databricks-solutions/latency-benchmarks](https://github.com/databricks-solutions/latency-benchmarks).
Every setting, and the semantic differences between engines, is in
[`STATEFUL_CONFIG.md`](STATEFUL_CONFIG.md).

## Reproduce

> ⚠ **This creates billable AWS resources.** The 30-minute matrix ran on 4 clusters in
> parallel, ~29 cluster-hours in total. With `SELF_TEARDOWN=1` each runner Job uploads its
> results to S3 and scales its nodegroups to 0 when it finishes; `eks/stateful/wrapup_cluster.sh`
> then checks the S3 results (`verify_s3_results.sh`), deletes the cluster and verifies nothing
> billable is left.

```bash
export AWS_PROFILE=your-profile
# cluster + platform (same as the original round, see Part 2 below)
eks/01_create_cluster.sh && eks/02_install.sh && eks/02b_irsa_s3.sh && eks/03_build_push_images.sh

# stateless p6 matrix (Spark 4.3.0 / Flink 2.3.0)
eks/04b_run_p6.sh <engine>

# stateful + joins matrix as an in-cluster Kubernetes Job, one cluster per plan set F30A..F30D
eks/stateful/check.sh                       # offline check of the runner
eks/stateful/preflight_runner.sh
PLAN_SET=F30A BUDGET_S=34200 RUN_EST_S=1260 SELF_TEARDOWN=1 eks/stateful/deploy_runner.sh

# tables and charts
python3 bench/analyze_final_run.py results/final30 --runs 2   # -> results/final30/cells.md, cells.json
python3 bench/make_post_charts.py                             # -> post/charts/*.png + numbers.json
```

Local correctness checks for the stateful workloads (every arm matches the ground truth):

```bash
bench/stateful_input.sh && PHASE=replay bench/run_stateful.sh flink && PHASE=replay bench/run_stateful.sh spark
bench/stateful_correctness.sh                       # diff every engine against the ground truth
```

Results in [`results/local-stateful/RESULTS.md`](results/local-stateful/RESULTS.md). Full
reproduction steps are in Appendix B of the [post](post/piece1-4.3.md).

---

# Original benchmark: Spark 4.1.2 RTM vs Flink 2.2.0 (stateless)

Validates the claims in the Medium post *"Spark RTM Mode: low-latency streaming
pipelines"* (neuw84) and runs the **identical** stateless Kafka → transform → Kafka
pipeline on **Apache Flink 2.2**, comparing **latency, CPU, memory, cost, and fault
tolerance** across six engine variants — locally in Docker and on a production EKS
cluster at ~100k events/sec.

**Headline results** (full write-up in [`REPORT.md`](REPORT.md)):

- RTM is real: it cuts median end-to-end latency from the micro-batch floor (~50 ms) to
  **~8 ms**, a 6.3× win.
- At the median RTM ties Flink (~6 ms). On the **tail**, Flink wins — and the gap only opens
  under **durable S3 checkpointing at scale**: on EKS at ~100k/s, RTM's synchronous offset
  commit spikes p99 to ~600–700 ms while Flink's async checkpoint stays ~31–33 ms — a **~20×
  gap** (~30× at 12 partitions). A Flink **SQL** variant with Spark's *exact* schema-based JSON
  path + same keying still shows the gap, so it isn't the parser. Locally (local-disk
  checkpoints, 10k/s) RTM's tail collapses to ~9 ms — the penalty is durable-storage-at-scale.
- Flink does the same work on **~70% of Spark RTM's memory, at comparable CPU**.
- Java ≡ Scala for Spark (same JVM plan). On worker loss, both recover in a comparable
  **~18–24 s**.

![RTM vs micro-batch](docs/charts/01_rtm_vs_microbatch.png)

## What's compared (6 engine variants)

| variant | engine | language | role |
|---|---|---|---|
| `spark-rtm` | Spark 4.1.2, `Trigger.RealTime("5s")`, update mode | Scala 2.13 | the claim under test |
| `spark-microbatch` | Spark 4.1.2, default trigger | Scala 2.13 | the latency floor RTM beats |
| `pyspark-rtm` | Spark 4.1.2 RTM via py4j bridge | Python | Python-vs-JVM on Spark |
| `flink` | Flink 2.2.0 DataStream, `bufferTimeout(0)`, keyed by `user_id` | Java 17 | the comparison engine |
| `flink-sql` | Flink 2.2.0 Table API/SQL, schema-based `json` format, keyed by `user_id` | Java 17 | JSON-path-matched to Spark's Catalyst |
| `pyflink` | Flink 2.2.0 Table API, tuned bundle | Python | Python-vs-JVM on Flink |

On EKS a **Java** Spark RTM job (`spark-rtm-java`) is added too, so the JVM comparison is
Java-on-both. The `flink-sql` variant uses Flink's declarative schema-based JSON (the
structural analogue of Spark's `from_json`/`to_json`) so the JSON-handling path matches Spark,
isolating the engine from the parser implementation. All run the same transform on the same
event (see
[`common/event_schema.md`](common/event_schema.md)): keep `purchase`/`add_to_cart` with
`amount>0`, add `amount_with_tax = amount*1.21`, uppercase `country`, stamp `out_ts`,
carry `ts` through for latency measurement.

---

# Part 1 — Local benchmark (Docker)

Runs on a laptop. Stacks run **sequentially** to fit in ~8 GB; Kafka is shared and stays
up between engines.

## 1.1 Prerequisites

| tool | version used | check |
|---|---|---|
| Docker (≥ 8 GB RAM allocated) | 29.x | `docker info \| grep Memory` |
| `sbt` (Scala build) | 1.10 | `sbt --version` |
| Maven (Java/Flink build) | 3.9 | `mvn -version` |
| Python 3 | 3.9+ | `python3 --version` |

```bash
# from the repo root
python3 -m venv .venv
.venv/bin/pip install -r common/requirements.txt   # confluent-kafka
```

> Apple Silicon or x86 both work (images are multi-arch). We ran native arm64 — relevant
> because the original post's amd64-under-emulation run is what produced its fat tail.

## 1.2 Run an engine

Each `bench/run_*.sh` does the whole cycle: brings up the stack → builds the jar →
submits the job → runs the producer (`TARGET_RATE` evt/s for `RUN_SECONDS`, both in
`.env`) while a latency consumer and a `docker stats` sampler record results → tears the
engine down. Kafka stays up.

```bash
bench/run_spark.sh rtm          # Spark Real-time Mode      -> results/spark-rtm_*
bench/run_spark.sh microbatch   # the floor RTM beats       -> results/spark-microbatch_*
bench/run_pyspark.sh            # PySpark RTM (py4j bridge)  -> results/pyspark-rtm_*
bench/run_flink.sh              # Java DataStream (keyed)    -> results/flink_*
bench/run_flink_sql.sh          # Java SQL / Table API       -> results/flink-sql_*

docker build -t pyflink-bench:2.2.0 flink/pyflink/   # one-time image build (~5 min)
bench/run_pyflink.sh            # PyFlink Table API          -> results/pyflink_*
```

**What you'll see** (per run, ~3–4 min): compose pulling/starting containers, a build log,
`[RtmPipeline] mode=rtm ... trigger=RealTime(5 seconds)` (or the Flink JobID), then a
1-second-cadence producer/consumer progress, ending in a `LATENCY SUMMARY` JSON block like:

```json
{
  "engine": "spark-rtm", "throughput_eps": 3326.6,
  "pipeline_ms": { "p50": 5.9, "p95": 8.6, "p99": 39.2, "max": 118.3 },
  "e2e_ms":      { "p50": 7.9, "p95": 11.1, "p99": 42.1, "max": 120.8 }
}
```

Artifacts land in `results/<engine>_{latency.csv,summary.json,stats.csv}`.

## 1.3 Build the comparison tables

```bash
.venv/bin/python bench/analyze.py        # local 5-engine latency + CPU/mem + cost tables
```

## 1.4 Validate the PySpark py4j-bridge claim

Shows the native `realTime=` kwarg is absent on stable 4.1.2 and the py4j bridge works.
Spark stack must be up:

```bash
docker compose -f docker-compose.kafka.yml -f docker-compose.spark.yml up -d
docker exec -e HOME=/opt/spark -e PYSPARK_PYTHON=python3 spark-jupyter \
  /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  /workspace/spark/pyspark-bridge/rtm_pyspark_measured.py
```

Expected: `native realTime= kwarg present? False` then
`py4j Trigger.RealTime('5 seconds') -> RealTimeTrigger(5000)` and a live query id.

## 1.5 Fault-tolerance test (local)

```bash
bench/chaos_flink.sh java     # kill a Flink taskmanager mid-stream, measure recovery
bench/chaos_spark.sh          # kill the Spark worker holding the partition tasks
```

These print recovery downtime + duplicate counts. (See the EKS chaos test for the
production-correct version with durable checkpoints + operator restart.)

## 1.6 Local troubleshooting

| symptom | cause / fix |
|---|---|
| `mkdir of file:/tmp/spark-checkpoints ... failed` | checkpoint dir not writable — the run scripts `chmod 777` it on each node; re-run |
| `--packages` download fails / Ivy error | Spark image runs with `HOME=/nonexistent`; scripts set `HOME=/opt/spark` + `spark.jars.ivy` |
| `maxOffsetsPerTrigger is not compatible with real time mode` | RTM rejects it; only micro-batch sets it (already handled) |
| negative pipeline latency | `current_timestamp()` is frozen under RTM — the jobs stamp `out_ts` per-row instead (see REPORT §7) |
| PyFlink p50 ~456 ms | Python UDF bundle floor — `python.fn-execution.bundle.time=1` fixes it (already set) |
| producer < target rate | bump Docker CPU, or lower `TARGET_RATE` in `.env` |

---

# Part 2 — Scale-out on AWS EKS (~100k evt/s)

The same jobs on a production cluster: 6× m5.2xlarge, Strimzi Kafka (3 brokers, KRaft),
the Spark-on-K8s and Flink Kubernetes operators, durable S3 checkpoints, 8 producer pods.
Full detail and the security baseline in [`eks/README.md`](eks/README.md).

> ⚠️ **This creates billable AWS resources** (EKS + nodes + S3). `eks/99_teardown.sh`
> removes them. Edit the `<YOUR_*>` placeholders in `eks/cluster.yaml` first.

```bash
export AWS_PROFILE=your-profile

eks/01_create_cluster.sh        # EKS cluster (~18 min). Writes a dedicated eks/kubeconfig.
eks/02_install.sh               # metrics-server + Strimzi(3-broker Kafka) + operators
eks/02b_irsa_s3.sh              # IRSA so Spark/Flink can write checkpoints to S3
eks/03_build_push_images.sh     # build + push engine images to ECR

# Steady-state matrix (each: deploy → 100k/s load → latency + CPU/mem + backlog → clean)
eks/04_run_benchmark.sh flink
eks/04_run_benchmark.sh spark-rtm-java
eks/04_run_benchmark.sh spark-rtm-scala
eks/04_run_benchmark.sh pyspark-rtm
eks/04_run_benchmark.sh pyflink

# Equal-resource matrix (identical cores/mem per engine, durable 60s checkpoints).
# 04c = 12 partitions/parallelism 12; 04b = 6 partitions/parallelism 6.
# Engines: spark-rtm-java | spark-rtm-scala | pyspark-rtm | flink | flink-sql | pyflink
eks/04c_run_p12eq.sh flink-sql      # JSON-path-matched Flink, 12-part
eks/04b_run_p6.sh    flink-sql      # JSON-path-matched Flink, 6-part

# Fault tolerance (kill a worker pod mid-stream, operator restarts from checkpoint)
eks/05_chaos.sh flink
eks/05_chaos.sh spark-rtm-java

.venv/bin/python bench/analyze_eks.py    # EKS comparison tables

eks/99_teardown.sh              # delete cluster, ECR, S3, KMS — leaves shared VPC intact
```

**Isolation note:** all EKS scripts pin `KUBECONFIG=eks/kubeconfig` so they never touch
your shared `~/.kube/config` (important if another cluster/session is active).

---

## Key charts

| | |
|---|---|
| EKS 5-engine matrix | `docs/charts/02_eks_matrix.png` |
| **Durable-checkpoint tail blowup** | `docs/charts/03_checkpointing_tail.png` |
| Data-rate sweep (flat latency) | `docs/charts/04_datarate_sweep.png` |
| CPU/memory efficiency | `docs/charts/05_efficiency.png` |
| Recovery downtime | `docs/charts/06_recovery.png` |

Regenerate with `.venv/bin/python bench/make_charts.py`.

## Layout

```
docker-compose.{kafka,spark,flink,pyflink}.yml   local stacks
.env                            versions, topics, load profile
common/                         producer.py, latency_consumer.py, event_schema.md
spark/scala-rtm/                Scala RTM/micro-batch pipeline (sbt)
spark/java-rtm/                 Java RTM pipeline (Maven)
spark/pyspark-bridge/           measured PySpark RTM job (py4j bridge)
flink/java-datastream/          Java DataStream pipeline (Maven shade)
flink/pyflink/                  PyFlink Table API job + image
bench/                          run_*.sh, chaos_*.sh, analyze*.py, make_charts.py
eks/                            cluster + operators + jobs + chaos + teardown (IaC)
results/                        summaries, stats, consumer logs, findings
docs/charts/                    generated PNG charts
REPORT.md                       original 4.1/2.2 write-up (local §1–8, EKS §9–10)
post/                           4.3/2.3 write-up (piece1-4.3.md) + its charts
STATEFUL_CONFIG.md              stateful/join settings for both engines
eks/stateful/                   in-cluster matrix runner for the 4.3/2.3 round
```

## Results

Latest round: **[`post/piece1-4.3.md`](post/piece1-4.3.md)** and
**[`results/final30/RESULTS.md`](results/final30/RESULTS.md)**. Original round: see
**[`REPORT.md`](REPORT.md)** for the complete write-up and **`results/`** for raw data
(`results/eks/` for the cluster run, including `EQUAL_RESOURCES_findings.md` — the fair,
equal-resource 6- and 12-partition matrices).
