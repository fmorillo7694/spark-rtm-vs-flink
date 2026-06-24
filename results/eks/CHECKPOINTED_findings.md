# EKS with durable checkpointing (S3) + operator-based recovery — findings

All engines configured for **fault tolerance**: durable S3 checkpoints (Spark RTM
checkpointLocation on s3a://; Flink execution.checkpointing to s3://, 10s interval),
operator restart (Spark `restartPolicy: OnFailure`; Flink `restart-strategy: fixed-delay`).
~100k evt/s in, ~31k/s out, ~4M records, 6× m5.2xlarge, 3-broker Strimzi Kafka.

## Steady-state latency WITH durable checkpointing (ms)

| engine | pipe p50 | pipe p95 | pipe p99 | e2e p50 | e2e p99 |
|---|--:|--:|--:|--:|--:|
| **Flink** (Java)   | 6.9  | 36.2 |  **50.5** | 15.1 | **78** |
| **PyFlink**        | 10.9 | 47.5 |  **58.2** | 33.1 | **99** |
| Spark RTM (Java)   | 8.4  | 981  | **1325**  | 71.1 | 1513 |
| Spark RTM (Scala)  | 8.2  | 839  | **1095**  | 67.5 | 1215 |
| PySpark RTM        | 8.2  | 974  | **1417**  | 66.4 | 1737 |

**THE headline finding (only visible once checkpointing is on):**
- All three Spark RTM variants suffer a **~1.1–1.4s p99 tail** when checkpointing offsets to
  durable S3 every 5s — RTM's checkpoint commit is synchronous and stalls the data path.
- Both Flink variants stay at **~50–60ms p99** — Flink's checkpointing is asynchronous and
  barely touches the data path.
- So durable fault tolerance costs Spark RTM ~20–25× worse tail latency than Flink.
- Medians stay close (~7–11ms) across all engines; the divergence is entirely in the tail.
- Java ≡ Scala for Spark RTM under checkpointing too (p99 1325 vs 1095; same mechanism).

## Fault tolerance: kill a worker pod mid-stream (operator auto-restart)

| engine | recovery downtime (max output stall) | query recovered? |
|---|--:|---|
| **Spark RTM** (Java) | **~21.5 s** | yes — operator kept SparkApplication RUNNING, resumed from S3 checkpoint |
| **Flink** (Java)     | **~18.0 s** | yes — Flink restart-strategy restored from checkpoint |

- **Both recover automatically in the same ballpark (~18–21s)** when properly configured
  with durable checkpoints + operator restart. Flink slightly faster.
- This CORRECTS an earlier flawed local test that reported RTM recovering in 129ms — that
  was an artifact of killing a partly-idle worker without durable checkpointing. With a
  real executor loss + S3 checkpoint, RTM stalls ~21s to detect, reschedule, and restore.
- Recovery downtime is dominated by failure detection + executor/TM re-acquisition +
  checkpoint restore — neither engine is instant; neither is dramatically better.
- Both sinks are at-least-once (no exactly-once sink configured), so a duplicate burst on
  replay is expected for both.

## Practical takeaway
- For LOW-LATENCY stateless streaming where you can tolerate at-most one checkpoint of
  replay and don't need durable checkpoints, RTM (no/local checkpoint) is competitive.
- The moment you require DURABLE fault tolerance (production), RTM's synchronous
  checkpoint tax (~1.3s p99) makes Flink materially better for tail-sensitive workloads.
- Recovery time on worker loss is comparable (~20s) for both.
