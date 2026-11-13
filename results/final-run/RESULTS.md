# Final lean run (option B): results

Two clusters in parallel, 2026-11-11 16:36 UTC -> 2026-11-12 ~01:50 UTC: `rtm-flink-bench6a` (`PLAN_SET=FA`,
stateless + S2 + S4 + O7) and `rtm-flink-bench6b` (`PLAN_SET=FB`, S1 + S3 + S5 + recovery), 6 x m5.2xlarge
each (2 tainted engine nodes + 4 `bench-ng`). Spark 4.3.0 / Flink 2.3.0, images `v43d` (stateful) and `v43`
(stateless, the published images). 5-minute measured window after a 60 s warm-up, fresh topics and
checkpoint path per run, one engine at a time. Plan: `FINAL_RUN_PLAN.md`. Runner prefixes:
`stateful/final-a-20261111T172702Z/`, `stateful/final-b-20261111T172717Z/`.

**Runs: 92 of 100.** A 52/52 (O7 included). B 40/48: its 8-hour runner budget stopped it before the
last 8 runs of round 3 (Spark S3 runs took 17-20.5 min, not the planned 12.2). **0 failed attempts on
either cluster.** Cells with **n=2** (accepted, no top-up): `spark-rtm-vt-s3`, `flink-ds-s3`,
`spark-mb-s5-at8k`, `flink-ds-bt100-s5-at8k`, `spark-mb1s-s5-at8k`, `flink-ds-s5-at8k`,
`spark-rtm-s1-kill`, `flink-ds-s1-update-kill`. O7 (`-at16k`) is 1 run per arm by design.

**Placement change vs the original post:** stateless engines are now pinned to the 2 dedicated engine
nodes like the stateful arms (load pods unpinned on `bench-ng`). The published stateless runs and session
B ran on one shared 6-node pool. Stateless numbers here are therefore not the same setup as the post's
Part 1 table, and the stateless p99 moved (diff below).

## Per cell

All 92 runs, both clusters (`cells.md` / `cells.json`, `bench/analyze_final_run.py`). Latency = pipeline
latency (output `out_ts` - event `ts`) in ms: mean over runs, (min-max across runs). Out evt/s = consumer
throughput. Resource columns are the engine pods only, from `eks/stateful/sampler.py`:

- **CPU** cores, mean / peak, and **WS** (working set, GiB) from kubelet cAdvisor. The sampler scrapes every
  2 s, but the kubelet refreshes container stats only every ~14 s (measured), so CPU is a rate between
  counter changes: **effective resolution ~14 s**, not 2 s. Peaks are 14 s averages.
- **throttle %**: CFS throttled / total periods. Spark executors run 6 or 9 task slots on a 3-core limit,
  so a high value means the executors were CPU-bound (Spark stateless executors have no CPU limit: "-").
- **heap peak** GiB: JVM heap used summed over the engine JVMs (Spark `jcmd GC.heap_info`, Flink REST).
- **RocksDB / state MB**: Flink = block cache + memtables + table readers (native, outside the heap), peak;
  Spark = state-store `memoryUsedBytes`, peak. Not the same quantity; each shows that engine's state memory.
- **ckpt MB** / **S3 PUTs**: bytes and objects under the run's checkpoint prefix after the run (objects =
  PUT lower bound; GET / LIST not measured). Stateless and S4 Flink checkpoints hold only offsets (~0 MB).

| cell | cl | n | p50 | p95 | p99 | evt/s out | CPU mean / peak | WS mean / peak | heap peak | RocksDB / state MB | throttle % | ckpt MB | S3 PUTs |
|---|---|--:|---|---|---|--:|---|---|--:|--:|--:|--:|--:|
| `flink-ds-bt100-s2-et` | a | 3 | 54.5 (54.3-54.7) | 116 (110-122) | 1,724 (1,685-1,769) | 49,602 | 2.1 / 2.8 | 5.9 / 6.2 | 2.9 | 2,076 | 1.6 | 154 | 43 |
| `flink-ds-bt100-s4` | a | 3 | 6.5 (6.5-6.5) | 8 (7-8) | 22 (17-27) | 48,609 | 1.1 / 1.2 | 5.6 / 5.7 | 3.1 | - | 0.1 | 0 | 1 |
| `flink-ds-bt100-s5-at16k` | a | 1 | 88.3 (88.3-88.3) | 2,306 (2,306-2,306) | 3,872 (3,872-3,872) | 12,793 | 5.6 / 6.4 | 6.1 / 6.4 | 2.8 | 3,315 | 52.0 | 157 | 61 |
| `flink-ds-s2-et` | a | 3 | 7.0 (6.9-7.0) | 113 (72-160) | 1,798 (1,786-1,806) | 49,602 | 3.6 / 4.0 | 5.9 / 6.2 | 3.0 | 2,091 | 2.0 | 154 | 43 |
| `flink-ds-s2` | a | 3 | 6.8 (6.7-6.9) | 9 (8-9) | 316 (300-330) | 49,604 | 3.3 / 3.8 | 5.7 / 5.8 | 3.1 | 653 | 1.4 | 80 | 25 |
| `flink-ds-s4` | a | 3 | 6.5 (6.4-6.5) | 8 (7-8) | 33 (18-44) | 48,610 | 1.0 / 1.2 | 5.6 / 5.7 | 3.0 | - | 0.1 | 0 | 1 |
| `flink-ds-s5-at16k` | a | 1 | 32.9 (32.9-32.9) | 2,080 (2,080-2,080) | 3,528 (3,528-3,528) | 12,796 | 5.9 / 6.4 | 6.1 / 6.4 | 2.8 | 3,080 | 62.2 | 158 | 61 |
| `flink-p6` | a | 3 | 6.2 (6.1-6.3) | 8 (7-8) | 40 (31-52) | 33,321 | 1.1 / 1.3 | 5.4 / 5.5 | 2.9 | - | 0.1 | 0 | 1 |
| `flink-sql-s2-ttl60` | a | 3 | 6.9 (6.7-7.0) | 13 (10-15) | 744 (735-758) | 49,601 | 3.7 / 4.2 | 6.2 / 6.5 | 3.1 | 2,209 | 2.1 | 122 | 25 |
| `pyflink-p6` | a | 3 | 8.7 (8.6-8.9) | 12 (11-12) | 45 (37-56) | 33,337 | 5.2 / 5.7 | 6.5 / 6.5 | 3.2 | - | 1.1 | 0 | 1 |
| `pyspark-rtm-p6` | a | 3 | 6.4 (6.3-6.4) | 8 (8-8) | 46 (33-66) | 33,335 | 1.7 / 2.4 | 8.0 / 8.1 | 5.7 | - | - | 0 | 18 |
| `spark-mb-s5-at16k` | a | 1 | 4,828.7 (4,828.7-4,828.7) | 7,377 (7,377-7,377) | 8,367 (8,367-8,367) | 12,747 | 3.4 / 4.4 | 9.1 / 9.4 | 5.1 | 706 | 29.5 | 945 | 3,895 |
| `spark-mb1s-s5-at16k` | a | 1 | 5,345.6 (5,345.6-5,345.6) | 8,422 (8,422-8,422) | 10,069 (10,069-10,069) | 12,695 | 3.4 / 4.3 | 9.1 / 9.5 | 5.9 | 746 | 34.3 | 973 | 3,577 |
| `spark-rtm-java-p6` | a | 3 | 6.3 (6.3-6.4) | 8 (8-8) | 41 (40-43) | 33,340 | 1.7 / 2.5 | 8.0 / 8.0 | 5.8 | - | - | 0 | 18 |
| `spark-rtm-s2` | a | 3 | 38.8 (38.0-39.7) | 6,208 (5,762-7,052) | 8,507 (8,002-9,325) | 49,600 | 3.5 / 5.2 | 8.9 / 9.3 | 6.1 | 170 | 16.0 | 695 | 194 |
| `spark-rtm-s4` | a | 3 | 6.7 (6.7-6.8) | 9 (8-10) | 393 (327-449) | 48,560 | 1.8 / 2.2 | 9.8 / 10.1 | 7.5 | - | 0.5 | 0 | 28 |
| `spark-rtm-scala-p6` | a | 3 | 6.3 (6.3-6.4) | 8 (8-8) | 52 (40-73) | 33,332 | 1.7 / 2.5 | 7.9 / 8.0 | 5.7 | - | - | 0 | 18 |
| `spark-rtm-sw1-s2` | a | 3 | 33.4 (30.9-38.2) | 8,034 (6,928-9,734) | 10,694 (8,668-13,659) | 50,645 | 5.9 / 6.5 | 9.1 / 9.4 | 6.1 | 174 | 93.4 | 584 | 188 |
| `spark-rtm-syncckpt-p6` | a | 3 | 6.3 (6.3-6.4) | 10 (8-14) | 573 (563-591) | 33,333 | 1.7 / 2.4 | 7.9 / 8.0 | 5.6 | - | - | 0 | 18 |
| `spark-rtm-vt-s2` | a | 3 | 13.1 (13.0-13.1) | 2,831 (2,746-2,953) | 4,967 (4,835-5,134) | 49,602 | 3.7 / 4.4 | 8.9 / 9.2 | 5.6 | 327 | 11.2 | 601 | 191 |
| `flink-ds-bt100-s1-update` | b | 3 | 56.1 (55.8-56.4) | 236 (203-263) | 1,576 (1,450-1,689) | 49,598 | 2.0 / 2.5 | 6.1 / 6.5 | 2.8 | 2,966 | 3.4 | 136 | 43 |
| `flink-ds-bt100-s3` | b | 3 | 71.5 (70.3-72.7) | 3,327 (3,307-3,355) | 4,612 (4,552-4,644) | 39,134 | 3.8 / 4.8 | 6.5 / 7.0 | 2.8 | 9,021 | 5.1 | 189 | 97 |
| `flink-ds-bt100-s5-at8k` **n=2** | b | 2 | 61.1 (60.6-61.6) | 107 (106-107) | 653 (624-682) | 6,405 | 2.8 / 3.7 | 5.9 / 6.0 | 3.2 | 1,580 | 1.3 | 80 | 61 |
| `flink-ds-s1-update-kill` **n=2** | b | 2 | 7.4 (7.4-7.4) | 52,654 (51,476-53,833) | 59,064 (58,032-60,096) | 57,183 | 3.6 / 5.7 | 6.0 / 6.8 | 3.0 | 3,462 | 9.8 | 110 | 43 |
| `flink-ds-s1-update` | b | 3 | 7.0 (6.9-7.1) | 190 (181-197) | 1,523 (1,479-1,586) | 49,603 | 3.6 / 3.9 | 6.1 / 6.4 | 2.9 | 2,789 | 3.7 | 124 | 43 |
| `flink-ds-s3` **n=2** | b | 2 | 9.0 (8.8-9.2) | 3,374 (3,358-3,391) | 4,640 (4,594-4,685) | 39,069 | 4.9 / 5.4 | 6.5 / 7.0 | 3.1 | 9,000 | 7.4 | 193 | 97 |
| `flink-ds-s5-at8k` **n=2** | b | 2 | 7.5 (7.5-7.5) | 9 (9-9) | 692 (690-695) | 6,404 | 3.9 / 4.8 | 5.9 / 6.0 | 2.9 | 1,527 | 1.8 | 80 | 61 |
| `spark-mb-s5-at8k` **n=2** | b | 2 | 3,255.7 (3,248.2-3,263.1) | 5,007 (4,956-5,058) | 5,461 (5,358-5,564) | 6,441 | 2.4 / 3.6 | 8.8 / 9.0 | 6.0 | 380 | 17.9 | 501 | 5,382 |
| `spark-mb1s-s5-at8k` **n=2** | b | 2 | 3,423.1 (3,333.9-3,512.3) | 5,251 (5,069-5,433) | 5,735 (5,470-5,999) | 6,371 | 2.4 / 3.6 | 8.8 / 9.0 | 6.0 | 387 | 20.3 | 502 | 5,128 |
| `spark-rtm-s1-kill` **n=2** | b | 2 | 101,933.4 (98,900.6-104,966.3) | 153,772 (152,937-154,607) | 158,407 (157,654-159,160) | 44,324 | 4.3 / 6.4 | 7.5 / 9.4 | 6.1 | 331 | 54.9 | 237 | 127 |
| `spark-rtm-s1` | b | 3 | 40.9 (40.7-41.4) | 5,384 (5,124-5,819) | 7,025 (6,886-7,289) | 49,825 | 3.9 / 5.2 | 9.0 / 9.3 | 6.0 | 414 | 17.9 | 680 | 186 |
| `spark-rtm-s3` | b | 3 | 119,722.2 (117,928.1-122,142.1) | 168,979 (165,450-172,601) | 177,738 (176,429-180,176) | 21,996 | 5.8 / 6.6 | 9.1 / 9.3 | 6.0 | 314 | 93.2 | 1,097 | 126 |
| `spark-rtm-sw1-s1` | b | 3 | 1,914.0 (1,247.4-2,730.8) | 14,616 (13,402-17,028) | 16,797 (15,185-18,385) | 50,531 | 5.9 / 6.4 | 9.2 / 9.5 | 5.6 | 443 | 94.0 | 731 | 191 |
| `spark-rtm-sw1-s3` | b | 3 | 144,455.8 (143,908.9-145,169.3) | 189,874 (188,119-191,963) | 197,642 (193,067-201,314) | 21,695 | 6.0 / 6.7 | 9.2 / 9.6 | 5.7 | 151 | 95.2 | 654 | 118 |
| `spark-rtm-vt-s1` | b | 3 | 13.7 (13.7-13.7) | 2,829 (2,579-2,978) | 4,566 (4,417-4,792) | 50,017 | 4.3 / 5.2 | 9.0 / 9.4 | 6.0 | 418 | 17.9 | 687 | 191 |
| `spark-rtm-vt-s3` **n=2** | b | 2 | 90,713.3 (89,262.4-92,164.2) | 144,737 (142,487-146,987) | 148,675 (147,179-150,171) | 26,165 | 5.8 / 6.6 | 9.3 / 9.8 | 5.6 | 362 | 94.0 | 949 | 127 |

Recovery (worker pod killed 180 s into the load):

| cell | n | outage s | recover s | lost / over / settled windows |
|---|--:|---|---|---|
| `flink-ds-s1-update-kill` | 2 | 10, 9 | 62, 70 | 0/0/121153; 0/0/125104 |
| `spark-rtm-s1-kill` | 2 | 93, 90 | -, - | 0/0/85928; 0/0/85863 |

Recovery details: Spark RTM's output stopped for 90-93 s after the executor kill and did **not** get back
to >= 90 % of the input rate with e2e < 1 s before the run ended (~180 s after the kill; "-" above), and
it re-emitted 3.2 M records per run (output minus input). Flink stopped for 9-10 s, recovered
in 62-70 s, and re-emitted 2.3 M per run (draft: ~2.0-2.35 M). Both: 0 lost, 0 overcounted windows.

## Diff against the provisional draft (`post/piece1-4.3.md` @ `20d1d9e`)

Draft numbers are geomeans of the runs of sessions B / C (and the 4.3 stateless round), final numbers
the geomean of this run's runs. Flagged: any headline that flips, and any p50 / p99 moving by more
than 20 %.

| cell | n | draft p50 / p99 | final p50 / p99 (geomean) | p99 change | flag |
|---|--:|---|---|--:|---|
| `flink-p6` | 3 | 6.1 / 25.8 | 6.2 / 38.7 | +50% | **p99 +50%** |
| `pyflink-p6` | 3 | 9.0 / 38.5 | 8.7 / 44.0 | +14% | |
| `spark-rtm-java-p6` | 3 | 6.2 / 38.4 | 6.3 / 41.2 | +7% | |
| `spark-rtm-scala-p6` | 3 | 6.2 / 34.5 | 6.3 / 49.9 | +44% | **p99 +44%** |
| `pyspark-rtm-p6` | 3 | 6.2 / 36.3 | 6.4 / 44.1 | +22% | **p99 +22%** |
| `spark-rtm-syncckpt-p6` | 3 | 6.2 / 594 | 6.3 / 573 | -4% | |
| `spark-rtm-s1` | 3 | 39.3 / 6,839 | 40.9 / 7,022 | +3% | |
| `spark-rtm-sw1-s1` | 3 | 244 / 21,364 | 1,818 / 16,746 | -22% | **p50 +645%, p99 -22%** |
| `spark-rtm-vt-s1` | 3 | 13.0 / 3,972 | 13.7 / 4,563 | +15% | |
| `flink-ds-bt100-s1-update` | 3 | 55.5 / 1,473 | 56.1 / 1,573 | +7% | |
| `flink-ds-s1-update` | 3 | 6.9 / 1,421 | 7.0 / 1,522 | +7% | |
| `spark-rtm-s2` | 3 | 38.3 / 7,911 | 38.8 / 8,487 | +7% | |
| `spark-rtm-sw1-s2` | 3 | 329 / 23,208 | 33.2 / 10,492 | -55% | **p50 -90%, p99 -55%** |
| `spark-rtm-vt-s2` | 3 | 12.7 / 4,211 | 13.1 / 4,965 | +18% | |
| `flink-ds-bt100-s2-et` | 3 | 54.5 / 1,722 | 54.5 / 1,724 | +0% | |
| `flink-ds-s2-et` | 3 | 6.8 / 1,773 | 7.0 / 1,798 | +1% | |
| `flink-ds-s2` (leaky 5 s TTL) | 3 | - / 320 | 6.8 / 316 | -1% | |
| `flink-sql-s2-ttl60` | 3 | - / 760 | 6.9 / 744 | -2% | |
| `spark-rtm-s3` | 3 | 109,292 / 181,564 | 119,709 / 177,729 | -2% | |
| `spark-rtm-sw1-s3` | 3 | 130,082 / 183,945 | 144,455 / 197,612 | +7% | |
| `flink-ds-bt100-s3` | 3 | 69.2 / 4,403 | 71.5 / 4,611 | +5% | |
| `flink-ds-s3` | **2** | 8.5 / 4,441 | 9.0 / 4,639 | +4% | |
| `spark-rtm-s4` | 3 | 6.6 / 492 | 6.7 / 390 | -21% | **p99 -21%** |
| `flink-ds-bt100-s4` | 3 | 6.4 / 20.7 | 6.5 / 21.5 | +4% | |
| `flink-ds-s4` | 3 | 6.3 / 20.7 | 6.5 / 30.5 | +48% | **p99 +48%** |

Headline by headline:

| draft headline | final | verdict |
|---|---|---|
| Stateless: RTM p99 642 -> 38 ms from async progress tracking; async-off control back at 594 ms | 41 ms; control 573 ms | **holds** |
| Stateless: "Flink's tail is still ~1.5x lower (26 vs 38 ms)" | 38.7 vs 41.2 ms (1.06x); Scala and PySpark RTM 44-50 ms | **gone: Flink and Spark RTM stateless tails are now about equal.** Likely the pinning change (2 engine nodes instead of a 6-node pool); not tested separately |
| Stateless resources: Flink 1.2 cores / 5.3 GiB vs Spark 1.6 / 8.0 | Flink 1.1 / 5.4 WS vs Spark 1.7 / 8.0 WS (heap peak 2.9 vs 5.8 GiB) | holds |
| Tuned Flink wins every stateful workload; ~2x lower p50 and ~2.4-2.8x lower p99 than vendor-tuned Spark | S1 7.0 vs 13.7 ms, 1.5 vs 4.6 s (3.0x); S2 7.0 vs 13.1 ms, 1.8 vs 5.0 s (2.8x) | **holds** (gap slightly wider) |
| At defaults the median is a coin toss (Spark 50 ms flush beats Flink 100 ms at p50); tails 6.8-7.9 s vs 1.4-1.8 s | p50 41 vs 56 ms (S1), 39 vs 55 ms (S2); tails 7.0-8.5 s vs 1.6-1.7 s | holds |
| "The obvious Spark tuning (1 ms) backfires": p50 244 ms / p99 21 s on S1, worse on S2 | S1: p50 1.8 s, p99 16.7 s (worse at the median, still far behind on the tail); **S2: p50 33 ms (better than the 50 ms default), p99 10.5 s (24 % worse than the default)**; 5.9 cores and 93-94 % CFS throttling on both | **partly**: still backfires on S1 and on every tail, but not at the S2 median. The CPU cost (throttled executors) is the robust part |
| Spark can't sustain S3 at 50k (p50 ~109 s backlog, ~21k sessions/s out vs Flink ~39k) | p50 120 s (vt: 91 s), out 22k/s vs Flink 39k/s; 93 % throttled | **holds**; vendor-tuned S3 now measured (n=2): still backlog |
| S4: Spark RTM broadcast p99 492 ms vs Flink 21 ms | 390 ms vs 22-31 ms | holds (Spark -21 %, Flink 0 ms +48 %, still 13x apart) |
| S2 dedup trap: leaky 5 s TTL p99 0.32 s vs correct 1.8 s; SQL TTL60 0.76 s | 0.32 / 1.8 / 0.74 s | holds |
| Recovery (session C, n=1): Flink out 9 s, back in 70 s; Spark out 77 s, back in 155 s, 0 re-emitted; both 0 lost / 0 overcount | Flink 9-10 s / 62-70 s; **Spark 90-93 s out and not back within the ~180 s left in the run, 3.2 M re-emitted**; both 0 / 0 | **Spark recovery got worse** (+20 % outage, recovery not reached, re-emits now nonzero); correctness holds |
| S5 (session C, saturated at 3400 orders/s per pod): Spark micro-batch p99 ~17 s beat Flink intervalJoin p99 73-85 s | at 1000/pod (new rate, ~15k records/s): Flink p50 7.5-61 ms, p99 0.65-0.69 s; Spark mb p50 3.3-3.4 s, p99 5.5-5.7 s. At 2000/pod (O7, n=1): Flink p99 3.5-3.9 s (52-62 % throttled), Spark 8.4-10.1 s | **flips**: off saturation, Flink's interval join has the lower median and tail; the draft's S5 tail comparison only held under saturation. Not a like-for-like diff (different rate) |

## Operations and cost

| | A (`rtm-flink-bench6a`) | B (`rtm-flink-bench6b`) |
|---|---|---|
| created | 16:36 UTC | 16:36 UTC |
| runner | 17:27 -> 01:26 (52/52, `MATRIX DONE`) | 17:28 -> 01:32 (40/48, `MATRIX STOPPED (budget)`) |
| self-teardown | Kafka + PVCs deleted, nodegroups 0 by 01:31 | same, by 01:36 |
| `eksctl delete cluster` | done 01:45 | done 01:50 |
| cluster-h | ~9.15 | ~9.25 |

Total **~18.4 cluster-h, ~$48** at the measured ~$2.6/h (an estimate, not billing), under the approved cap
(~$56, estimate + 25 %). Deadlines (per-cluster role policy after the shared-policy bug was fixed in
`dc714b5`) were never needed and are disarmed. Verified 01:51 UTC (`teardown_verify.txt`): no clusters, no
bench6 stacks, instances, tagged volumes or schedules; NAT `<NAT_ID>` available; bench4
leftovers (3 volumes, 9 stacks) untouched.

Sampler smoke (cluster A, before the matrix): `a/smoke/`. The raw `cadvisor.jsonl` (44 MB), `truth.jsonl`
and `maxcnt.json` are not committed; they stay under the runner prefixes in S3. Everything derived from them
is in `cells.json`.

Not checked: whether the stateless p99 shift comes from the pinning or from run-to-run variance (no
shared-pool control in this run); GET / LIST S3 request counts.
