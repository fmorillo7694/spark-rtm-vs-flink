# Smoke on rtm-flink-bench7a (2026-11-12) — PASS

Deadline armed before create: 21:10Z nodegroups / 21:55Z cluster (own policy `eks-teardown-only-rtm-flink-bench7a`).

| check | result |
|---|---|
| 2 x 300 s runs (`SELF_TEARDOWN=0`, `PLAN_SET=F30A`) | `spark-rtm-s1-at50k-r1` ok (p50 38.8 ms, p99 6.17 s: same tail as the final-run smoke at 100k), `flink-p6-r1` ok (p50 6.1 ms, p99 32 ms); MATRIX DONE 1292 s |
| sampler files | in S3 `final30/a-smoke/runs/*`: cadvisor 487/507 KB, jvm 8/26 KB, ckpt_s3.json both, rocksdb 1.3 KB (Flink; Spark has none, as in the final run); runner: errors 0 |
| consumer memory | peak 173 Mi (stateful), 869 Mi (stateless) vs 2 Gi request (300 s runs; the 6/10 Gi path for >= 1200 s is verified only in the rendered dry run) |
| `/configz` | `containerLogMaxSize=200Mi`, MaxFiles 5 on all 6 nodes (2 engine, 4 bench; AZs 1d/1f) |
| TERM without marker | Job with `SELF_TEARDOWN=1 BUDGET_S=34200` deleted 2 min in, no `stateful-runner-stop`: pod took ~6 min to exit (trap waits for the foreground step); afterwards engine-ng 2/2, bench-ng 4/4 ACTIVE, Kafka `bench` Ready, 3 PVCs, 6 nodes; run resources cleaned. The TERM log line itself was not captured (pod gone between 15 s log polls) |

Preflight before the matrix Job: `../logs/teardown_preflight.txt` (IAM simulate allowed x4, RBAC can-i yes x6, no marker).
