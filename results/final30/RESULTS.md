# Final run, option A: 30-minute fixed-duration re-run (results)

**Method.** 30 min measured after a 60 s warm-up; stateless at 100k evt/s, stateful at 50k evt/s
(6,250 per producer pod); 2 runs per configuration (r1 and r2), by design. Spark 4.3.0 RTM vs
Flink 2.3.0, same harness, resources and placement as the final run (`results/final-run/RESULTS.md`):
4 clusters `rtm-flink-bench7a..d` (6 x m5.2xlarge each, nodegroups across two AZs), one runner each
(`PLAN_SET=F30A..D`, `eks/stateful/run_matrix_incluster.sh` `plan_f30`). Stateless cells combine
r1 from one cluster and r2 from another (A+C: Java, Flink DataStream, PySpark; B+D: Scala, Flink
SQL, PyFlink). 44/44 runs, 0 failed attempts, 0 OOM. Tables: `python3 bench/analyze_final_run.py
results/final30 --runs 2 --title "Final run A (30 min, 4 clusters)"` -> `cells.md`, `cells.json`.

Raw per-run data: `s3://rtm-flink-bench-ckpt-<acct>/final30/{a,b,c,d}/runs/` (pulled to
`results/final30/`; the raw `cadvisor.jsonl` files stay out of git, as in the final run).

## Per cell

Pipeline latency ms: mean over runs (min-max across runs). CPU cores / memory GiB: engine pods only (cAdvisor, 2 s, measured window), mean / peak. Heap GiB: JVM heap used, summed over engine JVMs, peak. RocksDB MB (Flink): block cache + memtables + table readers, peak; Spark: state-store memoryUsedBytes peak. Checkpoint: MB written under the run's S3 prefix and object count (= PUT lower bound), mean over runs. Backlog s/min: growth of pipeline latency over the window (least-squares slope of the per-second mean, rate.json); ~0 = keeps up, else 60 x (1 - out/in), independent of the window length.

| cell | cl | n | p50 | p95 | p99 | evt/s out | backlog s/min | CPU mean / peak | WS mean / peak | heap peak | RocksDB / state MB | throttle % | ckpt MB | S3 PUTs |
|---|---|--:|---|---|---|--:|--:|---|---|--:|--:|--:|--:|--:|
| `flink-ds-bt100-s1-update-at50k-1800s` | a | 2 | 55.3 (55.2-55.5) | 185 (163-207) | 1,323 (1,245-1,402) | 49,600 | -0.0 | 1.9 / 2.6 | 6.9 / 7.8 | 3.2 | 6,893 | 2.9 | 178 | 55 |
| `flink-ds-s1-update-at50k-1800s` | a | 2 | 6.9 (6.9-6.9) | 204 (193-216) | 1,576 (1,561-1,590) | 49,600 | -0.0 | 3.5 / 4.2 | 7.1 / 8.3 | 3.2 | 8,189 | 3.5 | 208 | 55 |
| `flink-p6` | a | 2 | 6.1 (6.1-6.2) | 7 (7-7) | 28 (23-33) | 33,334 | -0.0 | 1.1 / 1.2 | 5.5 / 5.6 | 3.2 | - | 0.0 | 0 | 1 |
| `pyspark-rtm-p6` | a | 2 | 6.3 (6.3-6.3) | 8 (8-8) | 30 (27-34) | 33,329 | -0.0 | 1.7 / 2.5 | 8.1 / 8.2 | 6.2 | - | - | 0 | 68 |
| `spark-rtm-java-p6` | a | 2 | 6.3 (6.3-6.3) | 8 (8-8) | 30 (30-30) | 33,333 | -0.0 | 1.7 / 2.7 | 8.1 / 8.1 | 6.3 | - | - | 0 | 67 |
| `spark-rtm-s1-at50k-1800s` | a | 2 | 40.4 (39.7-41.2) | 5,935 (5,737-6,133) | 7,484 (7,264-7,704) | 49,581 | 0.0 | 3.9 / 5.9 | 9.5 / 10.0 | 6.2 | 459 | 16.7 | 3,160 | 567 |
| `spark-rtm-vt-s1-at50k-1800s` | a | 2 | 13.4 (13.3-13.5) | 2,990 (2,771-3,209) | 4,457 (4,301-4,612) | 49,614 | -0.0 | 4.0 / 5.3 | 9.5 / 9.9 | 6.5 | 437 | 12.1 | 3,331 | 600 |
| `flink-ds-bt100-s2-et-at50k-1800s` | b | 2 | 54.6 (54.4-54.7) | 110 (109-111) | 1,670 (1,638-1,703) | 49,600 | -0.0 | 2.0 / 2.8 | 6.2 / 6.7 | 3.2 | 2,971 | 1.4 | 228 | 55 |
| `flink-ds-s2-et-at50k-1800s` | b | 2 | 7.0 (6.9-7.0) | 49 (49-49) | 1,706 (1,697-1,715) | 49,600 | 0.0 | 3.7 / 4.4 | 6.2 / 6.7 | 3.3 | 2,978 | 1.7 | 229 | 55 |
| `flink-sql-p6` | b | 2 | 6.2 (6.2-6.3) | 8 (8-8) | 26 (25-27) | 33,332 | -0.0 | 1.3 / 1.5 | 5.6 / 5.7 | 3.0 | - | 0.0 | 0 | 1 |
| `pyflink-p6` | b | 2 | 8.6 (8.4-8.7) | 11 (11-11) | 31 (30-31) | 33,337 | -0.0 | 5.2 / 5.9 | 6.5 / 6.6 | 3.2 | - | 0.5 | 0 | 1 |
| `spark-rtm-s2-at50k-1800s` | b | 2 | 39.1 (39.0-39.2) | 6,144 (6,083-6,205) | 7,693 (7,675-7,712) | 49,600 | 0.0 | 3.4 / 5.6 | 9.3 / 9.7 | 6.3 | 416 | 12.2 | 2,656 | 567 |
| `spark-rtm-scala-p6` | b | 2 | 6.3 (6.3-6.4) | 8 (8-8) | 28 (27-28) | 33,332 | -0.0 | 1.6 / 2.2 | 8.0 / 8.1 | 6.3 | - | - | 0 | 68 |
| `spark-rtm-vt-s2-at50k-1800s` | b | 2 | 13.1 (13.1-13.1) | 2,725 (2,717-2,733) | 4,334 (4,312-4,356) | 49,600 | 0.0 | 3.6 / 4.6 | 9.2 / 9.6 | 6.4 | 470 | 9.0 | 2,748 | 591 |
| `flink-ds-bt100-s3-at50k-1800s` | c | 2 | 68.7 (67.7-69.7) | 2,908 (2,889-2,927) | 4,187 (4,170-4,204) | 39,121 | -0.0 | 3.4 / 4.3 | 7.4 / 8.7 | 3.2 | 18,870 | 3.9 | 277 | 133 |
| `flink-ds-s3-at50k-1800s` | c | 2 | 8.3 (8.3-8.3) | 3,063 (3,054-3,072) | 4,281 (4,270-4,292) | 39,049 | -0.0 | 4.9 / 5.6 | 7.4 / 8.6 | 3.2 | 18,437 | 5.6 | 283 | 133 |
| `spark-rtm-s3-at50k-1800s` | c | 2 | 443,295.5 (430,207.5-456,383.5) | 786,121 (784,482-787,760) | 813,628 (813,448-813,807) | 23,721 | 26.2 | 5.9 / 6.9 | 10.2 / 11.1 | 6.5 | 415 | 96.4 | 6,291 | 183 |
| `spark-rtm-vt-s3-at50k-1800s` | c | 2 | 299,907.6 (291,608.9-308,206.3) | 527,469 (511,620-543,319) | 538,754 (523,407-554,101) | 29,657 | 16.7 | 5.9 / 6.8 | 10.5 / 11.5 | 6.8 | 396 | 94.4 | 7,521 | 264 |
| `flink-ds-bt100-s4-at50k-1800s` | d | 2 | 6.4 (6.4-6.4) | 7 (7-7) | 18 (17-19) | 48,608 | 0.0 | 1.0 / 1.2 | 5.7 / 5.8 | 3.2 | - | 0.0 | 0 | 1 |
| `flink-ds-s4-at50k-1800s` | d | 2 | 6.4 (6.4-6.5) | 8 (7-8) | 20 (20-21) | 48,608 | -0.0 | 1.0 / 1.2 | 5.7 / 5.8 | 3.2 | - | 0.0 | 0 | 1 |
| `spark-rtm-s4-at50k-1800s` | d | 2 | 6.6 (6.6-6.6) | 8 (8-8) | 276 (252-299) | 48,608 | -0.0 | 1.7 / 2.0 | 11.3 / 12.1 | 9.0 | - | 0.2 | 0 | 76 |
| `spark-rtm-vt-s4-at50k-1800s` | d | 2 | 6.7 (6.7-6.7) | 8 (8-8) | 287 (278-296) | 48,607 | -0.0 | 1.7 / 2.0 | 11.3 / 12.2 | 8.6 | - | 0.2 | 0 | 76 |

**S3, where Spark does not hold the rate.** p50/p99 there are backlog and grow with the window
(p50 443 s at 30 min vs 120 s at 10 min), so quote the rate instead:

| cell | evt/s out | vs Flink tuned (39,049) | backlog growth, s of lag per min |
|---|--:|--:|--:|
| `spark-rtm-s3` | 23,721 | 0.61x | +26.2 |
| `spark-rtm-vt-s3` | 29,657 | 0.76x | +16.7 |
| `flink-ds-s3` / `flink-ds-bt100-s3` | 39,049 / 39,121 | 1.00x | -0.0 |

Backlog growth = least-squares slope of the per-second mean latency over the measured window
(`rate.json`); ~0 = keeps up, else 60 x (1 - out/in). Final run (600 s windows): +28.5 (default),
+22.6 (vendor-tuned).

## Against the post (`post/piece1-4.3.md` @ `abd4a0f`, final-run 600 s windows)

Same cell, post number vs this run. Post numbers are geomeans and this run's are means over 2 runs
(n=2: the two coincide to well under 1%).

| metric | post (600 s) | final30 (1800 s) | change |
|---|--:|--:|--:|
| stateless p99, Flink DataStream | 38.7 | 27.9 | -28% **>20%** |
| stateless p99, Flink SQL | 27.3 | 26.1 | -4% |
| stateless p99, PyFlink | 44.0 | 30.7 | -30% **>20%** |
| stateless p99, Spark RTM Java | 41.2 | 30.3 | -27% **>20%** |
| stateless p99, Spark RTM Scala | 49.9 | 27.7 | -45% **>20%** |
| stateless p99, PySpark RTM | 44.1 | 30.3 | -31% **>20%** |
| stateless WS GiB, Flink DataStream | 5.4 | 5.5 | +2% |
| stateless WS GiB, Spark RTM Java | 8.0 | 8.1 | +1% |
| stateless CPU, Flink DataStream | 1.1 | 1.1 | -7% |
| stateless CPU, Spark RTM Java | 1.8 | 1.7 | -0% |
| S1 p50, Spark default | 40.9 | 40.4 | -1% |
| S1 p99, Spark default | 7,022 | 7,484 | +7% |
| S1 p50, Spark vendor-tuned | 13.7 | 13.4 | -3% |
| S1 p99, Spark vendor-tuned | 4,563 | 4,457 | -2% |
| S1 p50, Flink default | 56.1 | 55.3 | -1% |
| S1 p99, Flink default | 1,573 | 1,323 | -16% |
| S1 p50, Flink tuned | 7.0 | 6.9 | -2% |
| S1 p99, Flink tuned | 1,522 | 1,576 | +4% |
| S2 p50, Spark default | 38.8 | 39.1 | +1% |
| S2 p99, Spark default | 8,487 | 7,693 | -9% |
| S2 p50, Spark vendor-tuned | 13.1 | 13.1 | -0% |
| S2 p99, Spark vendor-tuned | 4,965 | 4,334 | -13% |
| S2 p50, Flink default | 54.5 | 54.6 | +0% |
| S2 p99, Flink default | 1,724 | 1,670 | -3% |
| S2 p50, Flink tuned | 7.0 | 7.0 | -1% |
| S2 p99, Flink tuned | 1,798 | 1,706 | -5% |
| S3 p50, Flink default | 71.5 | 68.7 | -4% |
| S3 p99, Flink default | 4,612 | 4,187 | -9% |
| S3 p50, Flink tuned | 9.0 | 8.3 | -7% |
| S3 p99, Flink tuned | 4,639 | 4,281 | -8% |
| S3 out evt/s, Spark default | 21,996 | 23,721 | +8% |
| S3 out evt/s, Spark vendor-tuned | 26,165 | 29,657 | +13% |
| S3 out evt/s, Flink tuned | 39,069 | 39,049 | -0% |
| S3 backlog s/min, Spark default (final-run cells.md) | 28.5 | 26.2 | -8% |
| S3 backlog s/min, Spark vendor-tuned (final-run cells.md) | 22.6 | 16.7 | -26% **>20%** |
| S4 p50, Spark RTM | 6.7 | 6.6 | -1% |
| S4 p99, Spark RTM | 390 | 276 | -29% **>20%** |
| S4 p99, Flink default | 21.5 | 18.1 | -16% |
| S4 p99, Flink 0 ms | 30.5 | 20.3 | -34% **>20%** |
| S4 CPU, Spark RTM | 1.8 | 1.7 | -5% |
| S4 WS GiB, Spark RTM | 9.8 | 11.3 | +15% |
| S4 heap peak GiB, Spark RTM | 7.5 | 9.0 | +20% **>20%** |
| S4 WS GiB, Flink 0 ms | 5.6 | 5.7 | +2% |
| S1 CPU, Flink tuned | 3.5 | 3.5 | -0% |
| S1 CPU, Flink default | 2.0 | 1.9 | -2% |
| S1 CPU, Spark default | 3.9 | 3.9 | -2% |
| S1 throttle %, Spark default | 17.9 | 16.7 | -7% |
| S2 throttle %, Spark default | 16.0 | 12.2 | -24% **>20%** |
| S1 WS GiB, Flink tuned | 6.1 | 7.1 | +17% |
| S1 WS GiB, Spark default | 9.0 | 9.5 | +6% |
| S1 RocksDB peak MB, Flink tuned | 2,789 | 8,189 | +194% **>20%** |
| S2 RocksDB peak MB, Flink tuned | 2,091 | 2,978 | +42% **>20%** |
| S3 RocksDB peak MB, Flink tuned | 9,000 | 18,437 | +105% **>20%** |
| S1 checkpoint MB, Spark default | 680 | 3,160 | +365% **>20%** |
| S1 checkpoint MB, Flink tuned | 124 | 208 | +68% **>20%** |
| S1 S3 PUTs, Spark default | 186 | 567 | +205% **>20%** |
| S1 S3 PUTs, Flink tuned | 43.0 | 55.0 | +28% **>20%** |

### Headline by headline

| # | post headline | verdict | now |
|---|---|---|---|
| 1 | Stateless tail is now about a tie (Flink DataStream 38.7 vs Spark RTM Java 41.2 ms) | **holds**; absolute p99s **move >20%** | 27.9 (23-33) vs 30.3 (30-30) ms. All stateless p99s fell 27-45% except Flink SQL (-4%); see caveat 1 |
| 2 | Flink leaner on stateless memory and CPU (5.4 vs 8.0 GiB, 1.1 vs 1.7 cores) | **holds** | 5.5 vs 8.1 GiB, 1.1 vs 1.7 cores |
| 3 | Tuned Flink wins every stateful workload: ~2x lower p50, 2.8-3x lower p99 than vendor-tuned Spark | **holds** (each number within 13%) | p50 1.9x (S1, S2), p99 2.8x (S1) / 2.5x (S2): "2.5-2.8x" |
| 4 | At defaults the median is a coin toss; Spark default p99 7.0-8.5 s vs Flink 1.6-1.7 s | **holds** | p50 Spark 40 / 39 vs Flink 55 / 55 ms; p99 Spark 7.5-7.7 s vs Flink 1.3-1.7 s |
| 5 | Spark can't sustain S3 at 50k (~22k vs ~39k out; vendor ~26k) | **holds** | 23.7k (0.61x) and 29.7k (0.76x, +13%) vs 39.0k; backlog +26 / +17 s/min. Drop the post's p50 120 s / 91 s: duration-dependent |
| 6 | S4: median tie, ~13x tail, ~1.7-1.8x CPU and memory | median and tail ratio **hold**; absolute p99s **move >20%** | p50 6.6 vs 6.4 ms; p99 276 vs 20 ms (13.6x vs 0 ms, 15x vs 100 ms; was 390 vs 30.5); CPU 1.6x, working set 2.0x (11.3 vs 5.7 GiB), Spark heap peak 9.0 GiB (+20%) |
| 7 | Flink default ~41-44% less CPU than tuned, and still beats every Spark arm's p99 | **holds** | 45% less on S1 and S2; p99 1.3 / 1.7 s vs Spark's best 4.3-4.5 s |
| 8 | Flink runs on ~2/3 of Spark's working set | **holds** | 0.68 stateless, 0.75 S1, 0.67 S2 |
| 9 | Spark executors throttled 16-18% at default (S1/S2) | **moves >20%** on S2 | S1 16.7%, S2 12.2% (was 16.0): "12-17%" |
| 10 | Spark writes more checkpoint data: 0.6-0.7 GB vs 0.12-0.15 GB (4-5x), ~190 vs 43 PUTs | direction **holds**, numbers **move >20%** (duration-dependent) | end-of-run prefix 2.7-3.2 GB vs 0.21-0.23 GB (12-15x), 567 vs 55 objects. Quote per hour or per run length |
| 11 | Flink state memory 2.1-3.0 GB on S1/S2, 9 GB on S3, inside the working set | **does not hold: the metric over-counts** | S3 RocksDB "peak" 18.4 GB vs an 8.6 GiB working-set peak (was 9.0 GB vs 7.0 GiB in the final run: already impossible); see caveat 2 |

No headline flips. Not re-measured here, so the post's numbers stand as they are: Spark 1 ms
arm, async-progress-off control, checkpoint-interval sweep, recovery, dedup trap, hot keys /
final-result windows / exactly-once, S5, scaling.

### Caveats

1. **Stateless consumer changed.** The final run's stateless consumer was the published v43 image;
   this run's is v43d with `--no-raw` (1800 s needs the 6/10 Gi consumer without raw output). The
   stateless p99s fell by a similar 27-31% on both engines (Flink DataStream, PyFlink, Spark Java,
   PySpark), which points at the measurement more than at the engines; Flink SQL (-4%) and Spark
   Scala (-45%) do not fit that pattern. The tie and the ordering hold either way. Not isolated: no
   v43 vs v43d A/B was run.
2. **RocksDB memory column.** `rocks_stats` sums `block-cache-usage + size-all-mem-tables +
   estimate-table-readers-mem` over every vertex in `rocksdb.jsonl`. Flink's managed-memory block
   cache is shared, so the sum counts it more than once: on S3 it exceeds the pods' whole working
   set (2.1x here, 1.25x in the final run). Use working set and heap for Flink memory until the sum
   de-duplicates the shared cache; the post's state-memory sentence needs the same fix.
3. **Duration-dependent columns.** Checkpoint MB and S3 objects are measured at the end of the run,
   so they grow with run length on Spark (2.7-3.2 GB here vs 0.6-0.7 GB at 600 s); S3 backlog
   p50/p99 grow with the window. Throughput, backlog slope, CPU, working set and the non-saturated
   latencies do not.
4. CPU resolution ~14 s (kubelet refresh), engine pods only, as in the final run.

## Cost

| cluster | content | created (UTC) | verified gone | cluster-h | list $ at $2.6/h |
|---|---|---|---|--:|--:|
| `rtm-flink-bench7a` | S1 2x2 + 3 stateless r1, plus the smoke | 10:10:14 | 17:45:56 | 7.59 | 19.73 |
| `rtm-flink-bench7b` | S2 2x2 + 3 stateless r1 | 11:07:16 | 18:15:40 | 7.14 | 18.56 |
| `rtm-flink-bench7c` | S3 2x2 + 3 stateless r2 | 11:07:29 | 18:34:54 | 7.46 | 19.40 |
| `rtm-flink-bench7d` | S4 2x2 + 3 stateless r2 | 11:07:41 | 18:14:32 | 7.11 | 18.49 |
| **total** | | | | **29.30** | **76.18** |

Plan 30.9 cluster-h (~$80), cap 38.6 (~$100). Cluster-h counts from deadline arm (just before
create) to the verified delete. Cross-AZ transfer at list price was estimated at ~$70-80 in the
plan; actual account billing for transfer has been far below list (11-11: $0.34 for 4,025 GB).

**Billed: not in yet.** Cost Explorer (queried 2026-11-12 ~18:45Z) shows 11-12 at $6.17 estimated,
of which EC2 compute $0.75: the previous run's early-UTC tail, not this run's ~29 cluster-h.
Re-query after ~24 h.

## Teardown

Each cluster self-tore down (Kafka + PVCs deleted, nodegroups to 0), then `eks/stateful/wrapup_cluster.sh`:
S3 gate (`verify_s3_results.sh`: 11 runs, 0 gaps on each), `eksctl delete cluster`, deadline disarm,
verification (`*/logs/teardown_verify.txt`, global: `teardown_verify.txt`): no clusters, stacks,
instances, schedules; unattached volumes = the 3 bench4 leftovers; NAT available.
