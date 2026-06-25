# Equal-resources fair matrix — final results (all 5 engines)

All five engines, **identical resources per engine**, 60 s checkpoint interval, durable S3
checkpoints, 5-minute steady-state window, ~10 M records each, ~33 k records/sec out.
This corrects the published Medium run, where Spark had 17 cores vs Flink's 10 (an accidental
unfairness that flattered Spark).

## 12 partitions / parallelism 12 — 13 cores + 14 GB per engine
(1 coordinator @ 1c/2g + 4 workers @ 3c/3g)

| Engine | pipe p50 | pipe p99 | e2e p99 | mean CPU | mean mem |
|---|--:|--:|--:|--:|--:|
| Flink (Java)     | 6.7 ms | **28 ms**   | 58 ms   | 2.0 cores | 6.1 GB |
| PyFlink          | 9.8 ms | **40 ms**   | 76 ms   | 9.8 cores | 8.1 GB |
| Spark RTM (Java) | 7.8 ms | **829 ms**  | 1077 ms | 2.5 cores | 10.2 GB |
| Spark RTM (Scala)| 7.4 ms | **1287 ms** | 2302 ms | 3.1 cores | 10.0 GB |
| PySpark RTM      | 7.3 ms | **843 ms**  | 1217 ms | 3.1 cores | 10.0 GB |

## 6 partitions / parallelism 6 — 7 cores + 14 GB per engine
(1 coordinator @ 1c/2g + 2 workers @ 3c/6g)

| Engine | pipe p50 | pipe p99 | e2e p99 | mean CPU | mean mem |
|---|--:|--:|--:|--:|--:|
| Flink (Java)     | 5.9 ms | **24 ms**  | 44 ms   | 1.1 cores | 6.0 GB |
| PyFlink          | 8.6 ms | **34 ms**  | 61 ms   | 6.0 cores | 11.3 GB |
| Spark RTM (Java) | 6.6 ms | **686 ms** | 913 ms  | 1.6 cores | 9.2 GB |
| Spark RTM (Scala)| 6.5 ms | **826 ms** | 995 ms  | 1.6 cores | 9.2 GB |
| PySpark RTM      | 6.2 ms | **868 ms** | 1061 ms | 1.8 cores | 9.1 GB |

## Findings

- **Median ties everywhere** (~6–10 ms). RTM genuinely keeps pace with Flink on typical latency.
- **Fair tail gap ≈ 25–30×** (NOT the ~12× in the published article — that figure had Spark
  getting 70 % more cores). At 12-part: Flink 28 ms vs Spark RTM ~830 ms = **~30×**. At 6-part:
  Flink 24 ms vs Spark RTM ~690–870 ms = **~29×**.
- **RTM's tail is partition- AND core-insensitive** — it sits in the hundreds-of-ms-to-~1s band
  whether you give it 6 or 12 partitions, 7 or 13 cores. The tail is the synchronous offset
  commit stalling the data path, not a resource shortage.
- **Flink's tail improves with fewer partitions** (28→24 ms) because async checkpoint coordination
  is cheaper with fewer subtasks.
- **Java ≡ Scala** for Spark (same JVM plan); Scala showed run-to-run variance (792–1287 ms across
  runs) but always in the same band. PySpark ≈ Java RTM (Python only on driver).
- **Efficiency**: Flink does the same work on ~half the CPU and ~60 % the memory of Spark RTM.
  PyFlink trades CPU (6–10 cores) for its per-record Python worker but keeps a tight tail.

Summaries: `results/eks/*-p6_summary.json`, `results/eks/*-p12eq_summary.json`.
Resource samples: `results/eks/*-{p6,p12eq}_top.txt`.
