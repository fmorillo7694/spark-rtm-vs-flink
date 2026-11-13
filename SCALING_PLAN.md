# Scaling comparison plan: how each engine handles changing load (not launched)

Scope: Flink 2.3.0 with Kubernetes operator 1.16.1, vs Spark 4.3.0 OSS (Real-time Mode and
micro-batch). This is local research only: nothing below has run on EKS.

## 1. Facts

### Flink: operator job autoscaler (operator 1.16)

Source: [operator 1.16 Autoscaler docs](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.16/docs/managing/autoscaler/).

- **Enabling it:** `job.autoscaler.enabled: "true"` on the FlinkDeployment. It works on
  streaming jobs only, in application-mode FlinkDeployments and FlinkSessionJobs. Sources must
  use the FLIP-27 API (KafkaSource does).
- **What drives it:** per-vertex **busy time** and observed rates from the Flink REST
  metrics, not container CPU/memory. Kafka also reports `pendingRecords` (backlog), which the
  catch-up capacity term uses.
  - One sample per reconcile pass, every minute by default.
  - `job.autoscaler.stabilization.interval`, default 5 min: no decisions after a start or
    rescale.
  - `job.autoscaler.metrics.window`, default 15 min: samples are averaged over this window.
  - Busy time is aggregated with `MAX` across subtasks by default.
- **Decision:**
  - Target utilization `job.autoscaler.utilization.target` 0.7, with a no-action band of
    ±0.3 (`utilization.max` / `.min`).
  - Catch-up: `job.autoscaler.catch-up.duration` 30 min, plus `job.autoscaler.restart.time`
    5 min as the backlog allowance for the rescale itself (tracking is optional).
  - Bounds: `vertex.min-parallelism` 1, `vertex.max-parallelism` 200,
    `scale-down.max-factor` 0.6.
  - **Scale-downs** are deferred and merged over `job.autoscaler.scale-down.interval`
    (**default 1 h**). The deepest dip in the window does not win: the highest recommendation
    in the window is applied. **A short test must lower this.**
- **Applying a decision:** the new parallelism is written as
  `pipeline.jobvertex-parallelism-overrides`.
  - With `jobmanager.scheduler: adaptive`, it is applied **in place** through Externalized
    Declarative Resource Management: no cluster redeploy.
  - Without the adaptive scheduler, every decision is a **full job upgrade** (savepoint or
    last-state, then redeploy).
  - In-place still restarts the tasks from the latest checkpoint. Flink 2.3's elastic-scaling
    doc says adaptive scheduler "scaling events trigger job and task restarts"
    ([Flink 2.3 Elastic Scaling](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/elastic_scaling/)).
    So "in place" means no pod or JobManager churn, not zero stall.
- **Keyed state redistribution:**
  - `pipeline.max-parallelism` (or a per-operator max parallelism) fixes the number of
    **key groups**. That is the hard ceiling for any rescale, and changing it is incompatible
    with restoring existing state.
  - On a rescale, whole key groups move between subtasks, restored from the checkpoint.
  - Alignment (`scaling.parallelism-alignment.mode`, default BALANCED) snaps the
    recommendation to a divisor of the key groups or source partitions.
  - **Source parallelism is capped at the Kafka partition count.** Our topics have 6
    partitions, so a 6-partition topic cannot scale its source past 6.

### Flink: Reactive Mode

Source: [Flink 2.3 Elastic Scaling](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/elastic_scaling/).

- It is an Adaptive Scheduler mode: the job always uses every slot in the cluster.
  **Adding or removing a TaskManager rescales the job.**
- **Restart semantics:** a rescale restarts the job **from the latest completed checkpoint**,
  with no savepoint.
  - The data reprocessed depends on the checkpoint interval, and the restore time on state
    size.
  - It needs periodic checkpoints and a restart strategy; without a restart strategy the
    job fails instead of scaling.
  - `jobmanager.adaptive-scheduler.scaling-interval.min` (default 30 s) limits how often it
    rescales.
  - A TaskManager killed with SIGKILL stalls the job until `heartbeat.timeout` expires (50 s).
- **Deployment:** standalone application mode only (active resource managers like native
  K8s are not supported), single-job.
- **On the operator** ([operator 1.16 Deployment overview, Standalone Mode](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.16/docs/deployment/overview/)):
  - Use `mode: standalone` plus `scheduler-mode: reactive`. The operator only changes the
    TaskManager Deployment's replica count.
  - The FlinkDeployment CRD exposes the **scale subresource** (`specReplicasPath:
    .spec.taskManager.replicas`, verified in the release-1.16 CRD). So a
    HorizontalPodAutoscaler can drive it.
  - The HPA's metric is ours to choose: CPU, or Kafka lag through an adapter. Unlike the
    autoscaler, it does not use Flink's busy-time model.

### Spark 4.3.0 OSS

| question | answer | evidence |
|---|---|---|
| Dynamic allocation with **stateful** RTM | **Rejected at runtime.** The query fails: `Dynamic allocation is not supported for a job that uses a pipelined shuffle dependency: a pipelined stage group must be co-scheduled against a known cluster size. Disable spark.dynamicAllocation.enabled for such jobs.` | String in `org.apache.spark.scheduler.DAGScheduler` (spark-core_2.13-4.3.0.jar). **Reproduced locally**: `results/local-scaling-smoke/spark-da-rtm.txt` (StreamingQueryException, 0 commits) |
| Slot check | `spark.scheduler.pipelinedGroup.slotCheck.enabled`: before co-scheduling a pipelined group, the driver checks the group's total task demand fits the free slots, else fails with `CONCURRENT_SCHEDULER_INSUFFICIENT_SLOT` ("Provision more executors, or reduce the parallelism of the query") | `internal/config/package$` and `error-conditions.json` in the 4.3.0 jars |
| Stateless RTM + dynamic allocation | Not blocked by that check (no pipelined shuffle), but pointless: RTM maps Kafka partitions 1:1 to long-running tasks (`minPartitions` unsupported), so executors never go idle and extra ones get no work | [RTM docs, Best Practices](https://spark.apache.org/docs/latest/streaming/real-time-mode.html). Not run locally |
| Dynamic allocation in **micro-batch** | **Works.** A stateful s1 micro-batch query ran 347 batches with DA on (`shuffleTracking`, no shuffle service) | `results/local-scaling-smoke/spark-da-microbatch.txt`. The local standalone cluster has one worker, so actual executor add/remove was **not** exercised. On K8s, DA needs `spark.dynamicAllocation.shuffleTracking.enabled` or decommissioning ([Job Scheduling, 4.3.0](https://spark.apache.org/docs/4.3.0/job-scheduling.html)) |
| Stateful partitions | The number of partitions "when shuffling data for stateful operations" (an internal conf that picks up `spark.sql.shuffle.partitions`) "cannot be changed between query restarts from the same checkpoint location" | `SQLConf$` doc string, spark-catalyst_2.13-4.3.0.jar |
| Manual scaling | Stop the query, restart from the same checkpoint with more executors | RTM docs sizing rule |

Manual scaling detail:

- **Stateless RTM:** read parallelism = Kafka partitions, so more executors only help when
  there are more partitions than cores.
- **Stateful RTM:** both stages hold slots (source partitions + state partitions), and the
  state partitions are frozen by the checkpoint. To have room to grow, the query must start
  with `spark.sql.shuffle.partitions` and topic partitions sized for peak.
- **Rescale cost:** one batch is lost (output resumes from the last committed batch), plus
  executor start-up and state reload. That matches the kill-run outage we measured: 71–77 s
  stop, about 150 s to catch up.

**Bottom line:**

- Spark RTM has **no automatic scaling**. Dynamic allocation is refused for stateful RTM, and
  it is useless for stateless RTM.
- Spark micro-batch has generic dynamic allocation, but it reacts to idle executors and
  pending tasks, not to lag. It is bounded by the fixed state partitions.
- Flink has two real paths: the operator autoscaler (busy-time driven, in place with the
  adaptive scheduler) and Reactive + HPA. Both restart tasks from the latest checkpoint on
  each rescale.

## 2. Test design

**Load profile:** 50k → 100k → 50k evt/s, measured for 10 min + 20 min + 25 min. A second
profile, 100k → 25k, tests scale-in.

**Workload:** S1 (keyed window state; the existing truth / `recovery_check.py` applies).
Topics need **24 partitions** (vs 6 in the main matrix) and `pipeline.max-parallelism` 120,
so sources and keyed operators can grow past 6.

**Prerequisites:**

- The autoscaler needs `stabilization.interval` 2 min, `metrics.window` 5 min and
  `scale-down.interval` 10 min, or nothing happens inside an hour.
- `restart.time-tracking.enabled: true`.

**Per arm, recorded:**

- time from the load step to steady state (p99 < 2 s for 60 s);
- latency spike and output stall (`rate.json`, per-second records and e2e);
- number of rescales;
- correctness: the S1 truth check (lost / overcounted windows) plus re-emitted records;
- node-hours and engine-pod core-hours vs a static cluster sized for 100k.

| arm | mechanism | runs |
|---|---|---:|
| F-auto | Flink operator autoscaler, native + adaptive scheduler (in place) | 2 |
| F-reactive | Flink standalone + `scheduler-mode: reactive` + HPA on the FlinkDeployment scale subresource (CPU or Kafka lag) | 2 |
| F-static-peak | Flink, parallelism sized for 100k (baseline for cost and latency) | 1 |
| S-rtm-static-peak | Spark RTM sized for 100k, never rescaled (the only automatic option) | 1 |
| S-rtm-manual | Spark RTM restarted at each load step with more / fewer executors, same checkpoint, partitions pre-sized for peak | 2 |
| S-mb-DA | Spark micro-batch + dynamic allocation (shuffle tracking), "the Spark option that does scale", labelled not-RTM | 2 |
| scale-in | F-auto and F-reactive on 100k → 25k | 2 |
| | **core: 10 runs; with scale-in: 12** | |

**Infrastructure:**

- Node scale-out needs **Cluster Autoscaler** on the engine nodegroup (min 1, max 4
  m5.2xlarge, IRSA, a nodegroup tag). Cluster Autoscaler is the smaller change for an eksctl
  managed nodegroup. Karpenter would mean NodePools and an extra controller for little gain
  here.
- Node boot plus image pull (~2–4 min) adds to the time-to-steady-state for every arm that
  needs a new node. Record it separately, so engine rescale time and EC2 time are not mixed.
- The NAT gateway, EIP and S3 endpoint stay untouched. The existing self-teardown and
  EventBridge deadline apply unchanged; with Cluster Autoscaler, the self-teardown must also
  set the nodegroup max to 0.

## 3. Local feasibility smoke

- **Done (Spark, local Docker, about 4 min):** stateful RTM with dynamic allocation fails at
  start, as the jar says; micro-batch with dynamic allocation runs.
- **Not done:** a Flink operator autoscaler smoke on kind. It needs the operator, cert-free
  install, Kafka and a load step, about 1 h of laptop time under memory pressure, and it
  would only confirm documented behaviour. Recommended as the first 30 min of the EKS session
  instead: `scaling.enabled: false`, metrics-only mode.

## 4. Cost and recommendation

| scope | runs | runner h | cluster h (+1.5 setup / teardown) | ~$ (at $4–5/h, nodes vary) |
|---|---:|---:|---:|---:|
| core | 10 | ~10 | ~11.5 | ~$50–55 |
| + scale-in | 12 | ~12 | ~13.5 | ~$60–65 |

These assume ~60 min per run (55 min of load + setup); scaling arms spend part of the time on
up to 4 engine nodes.

**Recommendation: the Part 2 tuning/operations post, not piece 1.**

- Piece 1 is a like-for-like latency/correctness comparison at fixed resources. Scaling is
  not symmetric: Spark RTM cannot autoscale at all.
- A long, partly one-sided matrix would dilute piece 1. The fact belongs in piece 1 as one
  sourced paragraph (the `DAGScheduler` message plus the local reproduction); that needs no
  cluster run.
- Part 2 can then measure the cost question properly: node-hours of Flink autoscaler and
  Reactive vs a peak-sized static cluster, with Spark micro-batch DA and RTM manual restarts
  as the Spark side.
