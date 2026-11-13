# Final run, option A: 30-minute fixed-duration re-run (PREPARED, not launched)

22 configurations x 2 runs = **44 runs**, each **60 s warm-up + 1800 s measured**, on **4 clusters in
parallel** (`rtm-flink-bench7a..d`). **Stateful at 50k evt/s** (6,250 per producer pod x 8, the post's
stateful comparison rate; at 100k stateful Spark saturates and the 2x2 would become a capacity
test), **stateless at 100k evt/s** (12,500 per pod, as published). Same images, job configs, pod
resources and placement as the final lean run (`FINAL_RUN_PLAN.md`, `results/final-run/RESULTS.md`):
6 x m5.2xlarge per cluster across 2 AZs (2 tainted engine nodes + 4 `bench-ng`), stateless engines
pinned to the engine nodes, the `eks/stateful/sampler.py` measurement stack on every run.
**The smoke and the launch both wait for an explicit GO.**

## 1. Cells and cluster split

Arm names as the post uses them: Flink default = `flink-ds-bt100` (100 ms buffer timeout), Flink
tuned = `flink-ds` (0 ms), Spark RTM default = `spark-rtm` (50 ms flush), Spark RTM vendor-tuned =
`spark-rtm-vt`. Workloads: S1 = Flink `s1-update` / Spark `s1`; **S2 = the post's headline dedup,
Flink `s2-et` (event-time) / Spark `s2`**; S3 `s3`; S4 `s4`. Labels: stateful
`<arm>-<workload>-at50k-1800s-rN`, stateless `<engine>-p6-rN`.

Each cluster gets one stateful workload's whole 2x2 (both runs), so every head-to-head runs on the
same nodes, plus 3 stateless runs; each Spark-vs-Flink stateless pair shares a cluster (r1 on A/B,
r2 on C/D). Every cluster has exactly 2 Spark stateful cells; the two slowest (Spark S3) land on C.

| cluster | `PLAN_SET` | stateful cells (x2 runs) | stateless runs |
|---|---|---|---|
| A `rtm-flink-bench7a` | `F30A` | S1: `spark-rtm-s1`, `flink-ds-bt100-s1-update`, `spark-rtm-vt-s1`, `flink-ds-s1-update` | `spark-rtm-java-p6` r1, `flink-p6` r1, `pyspark-rtm-p6` r1 |
| B `rtm-flink-bench7b` | `F30B` | S2: `spark-rtm-s2`, `flink-ds-bt100-s2-et`, `spark-rtm-vt-s2`, `flink-ds-s2-et` | `spark-rtm-scala-p6` r1, `flink-sql-p6` r1, `pyflink-p6` r1 |
| C `rtm-flink-bench7c` | `F30C` | S3: `spark-rtm-s3`, `flink-ds-bt100-s3`, `spark-rtm-vt-s3`, `flink-ds-s3` | `spark-rtm-java-p6` r2, `flink-p6` r2, `pyspark-rtm-p6` r2 |
| D `rtm-flink-bench7d` | `F30D` | S4: `spark-rtm-s4`, `flink-ds-bt100-s4`, `spark-rtm-vt-s4`, `flink-ds-s4` | `spark-rtm-scala-p6` r2, `flink-sql-p6` r2, `pyflink-p6` r2 |

Order inside every Job (`plan_f30` in the runner; 11 runs, Spark and Flink alternate, a stateless
run after every two stateful ones, round 1 before round 2), e.g. cluster A:

```
spark-rtm:s1:1  flink-ds-bt100:s1-update:1  sl-spark-rtm-java:p6:1  spark-rtm-vt:s1:1  flink-ds:s1-update:1
sl-flink:p6:1  spark-rtm:s1:2  flink-ds-bt100:s1-update:2  sl-pyspark-rtm:p6:1  spark-rtm-vt:s1:2  flink-ds:s1-update:2
```

(every item `:secs=1800`). Dry runs: section 8.

Note: `spark-rtm-vt-s4` has never run. S4 has no shuffle and no state store, so the vendor settings
(10 ms shuffle flush, `trackTotalNumberOfRows=false`) should change nothing; it is in because the
brief asks for 4 arms per workload.

## 2. Time and cost

Per run = measured overhead + 31 min (60 s warm-up + 30 min): stateless 35, Flink stateful 35.5,
Spark stateful 40, Spark S3 45 min. (Final run at 50k: Spark S3 runs took 17-20.5 min for a 5-min
window, i.e. 12-15 min overhead; the 14 min used here is inside that range.)

| cluster | runner | create + install | teardown + delete | **cluster-h** |
|---|--:|--:|--:|--:|
| A | 142 + 160 + 105 = **407 min** (6.8 h) | 0.5 h | 0.35 h | **7.63** |
| B | 142 + 160 + 105 = **407 min** (6.8 h) | 0.5 h | 0.35 h | **7.63** |
| C | 142 + 180 + 105 = **427 min** (7.1 h) | 0.5 h | 0.35 h | **7.97** |
| D | 142 + 160 + 105 = **407 min** (6.8 h) | 0.5 h | 0.35 h | **7.63** |
| total | 27.5 h runner | | | **30.9** |

(Flink stateful 2 cells x 2 runs x 35.5 = 142; Spark 2 x 2 x 40 = 160, S3 2 x 2 x 45 = 180;
stateless 3 x 35 = 105.)

- **Wall-clock ~8.0 h** (cluster C).
- **Nodes and control plane: ~$80** at $2.6/cluster-h (30.9 cluster-h) list price.
- **Cap = estimate x 1.25 = 38.6 cluster-h (~$100 at $2.6/h).**
- **Cross-AZ transfer at 50k, list price, on top:** Cost Explorer for 2026-11-11 (the final run at
  the same mix, stateful 50k + stateless 100k, plus D2) shows **4,025 GB** of
  `DataTransfer-Regional-Bytes`, ~5.3 GB per load-minute. Option A is 44 x 31 = 1,364
  load-minutes with a slightly larger stateless share (27 % of runs vs 20 %), so **~7-8 TB,
  ~$70-80 at the $0.01/GB list price**, i.e. ~$150-160 all-in at list. The account's actual billing
  is far below list (that day: $0.34 for 4,025 GB, m5.2xlarge ~$0.023/h), so the $ here are list
  estimates. Multi-AZ placement is kept, as in the final run.

## 3. Runner budgets (projected runner time + 40 %)

The budget is the last moment a run may **start**, fixed at launch. With `RUN_EST_S=1260` a run
only starts if 1,260 + 1,500 = 2,760 s (46 min) remain, so any started run (Spark S3 ~45 min)
finishes inside the budget.

| cluster | projected runner | `BUDGET_S` (x 1.4) | Job `activeDeadlineSeconds` (budget + 1800) |
|---|--:|--:|--:|
| A, B, D | 407 min | **34,200 s (9.5 h)** | 36,000 s |
| C | 427 min | **35,900 s (9.97 h)** | 37,700 s |

## 4. EventBridge deadlines

Armed per cluster at create time T0 (`arm_deadline.sh`, one inline role policy per cluster since
`dc714b5`), **after** the budget: nodegroups at T0 + 0.5 h (setup) + budget + 1.0 h (Job deadline +
teardown slack); the cluster 45 min later.

| cluster | nodegroups deleted | cluster deleted |
|---|--:|--:|
| A, B, D | T0 + **11.0 h** | T0 + 11.75 h |
| C | T0 + **11.5 h** | T0 + 12.25 h |

Backstop only: all four clusters running to their deadlines would be ~47.5 cluster-h (~$124 at
list, nodes only), above the 38.6 cluster-h cap. The cap is enforced by the poll: every turn
reports cluster-h, and if the projected total passes 38.6 the runners get the explicit stop
(section 7).

## 5. Capacity (read-only checks, 2026-11-12)

| check | finding | quota increase? |
|---|---|---|
| EC2 on-demand standard vCPU (`L-1216C47A`), us-east-1 | quota **1,152**; running **0** instances; needs 4 x 6 x 8 = **192** | no |
| Free IPs, VPC of `eksctl-eks-vs-msf-bench-cluster` (`<VPC_ID>`, CREATE_COMPLETE) | private subnets (us-east-1d / 1f, /19 each): **8,187 + 8,187** free (public 8,187 + 8,186); VPC CNI warm pools ~30-45 IPs per node, ~1,100 for 4 clusters | no |
| EKS clusters per region | quota **100**; existing **0** | no |
| NAT | available, the only ENI in the VPC; kept and untouched | - |

## 6. 1800 s: memory, logs, disk, analyzer

| item | at 1800 s | handled by |
|---|---|---|
| stateful consumer | S1/S2 at 50k emit ~1 record per input: ~90 M latencies; float32 arrays (pipeline, e2e, window) ~1.1 GB, plus float64 copy + percentile copy ~1.4 GB: **~3 GB peak**, under the old 4 Gi limit only narrowly | runner: 6 Gi request / 10 Gi limit for `secs >= 1200` |
| stateless consumer | ~33k out/s x 1800 s = ~60 M records. The published `v43` consumer would keep Python lists and write a ~5 GB raw CSV | runner, `secs >= 1200`: consumer only on the `v43d` load image (float32 arrays) with `--no-raw` and 6/10 Gi; producers and engines stay `v43`. Peak ~1.5 GB |
| kubelet log rotation (10 Mi) | a 30-min Spark driver log may pass it, and `kubectl logs` returns only the newest file, losing early `progress.jsonl` batches | cluster configs: kubelet drop-in `containerLogMaxSize: 200Mi` (pre-bootstrap; eksctl has no `kubeletExtraConfig` for managed nodegroups). **Verified on the smoke** via the node's `/configz`; fallback if it does not apply: periodic progress capture |
| Kafka disk per broker (100 GiB) | stateful 50k: ~7 GB input (lz4) + ~19 GB output (uncompressed sink); stateless 100k: ~13 + ~12 GB; topics deleted and recreated per run; retention 2 h (stateful) / 1 h (p6) > 31 min | nothing to change |
| sampler | ~5,400 cAdvisor lines (~3 MB) per run; 11 runs in the runner's 1 GiB emptyDir ~35 MB | nothing to change |
| truth check | no kill cells, so off (on at 1800 s it would be ~1.8 M sampled windows, a ~50 MB MAXJSON line) | nothing to change |
| `bench/analyze_final_run.py` | windows from the consumer summary (`started_at` + warm-up .. `ended_at`; the v43d stateless consumer now writes them too, the old fallback stays) | nothing to change; point it at `results/final30/{a..d}` |
| S3 layout | per-run `runs/<label>/` as before; checkpoint prefixes hold ~30 checkpoints instead of ~6 (final run at 300 s: Spark S1 ~0.7 GB, Flink ~0.13 GB) | nothing to change |

## 7. Rules and how the code enforces them

- One runner Job per cluster; `SELF_TEARDOWN=1`, preflight-checked before launch (IAM simulation
  of the scale-down calls, RBAC `can-i` for Kafka / node pool / PVC delete, PV list, `nodes/proxy`,
  the stop-marker ConfigMap), never neutralized.
- **Teardown only at the budget end or on an explicit stop** (`on_term` in the runner): on SIGTERM
  the runner tears down only if `elapsed >= BUDGET_S` (the Job's `activeDeadlineSeconds`) or the
  ConfigMap `kafka/stateful-runner-stop` exists. Any other TERM (Job deleted to relaunch, node
  drain, eviction) exits without teardown; the Job restarts the pod and runs already in S3 are
  skipped. `MATRIX DONE` / `MATRIX STOPPED (budget)` tear down as before.
  **Explicit stop** = `kubectl -n kafka create configmap stateful-runner-stop`, then delete the Job.
  `terminationGracePeriodSeconds` is 900 s so a TERM-triggered teardown (Kafka delete, up to 5 min
  of PV wait, nodegroup scale-down) finishes before SIGKILL; it was 120 s.
- NAT, EIP, vpce and VPC kept; bench4 leftovers untouched; multi-AZ placement as in the final run.

## 8. Prepared and verified locally (no AWS)

| blocker | change | verified |
|---|---|---|
| 1 trap | `on_term` + `stop_requested` in `run_matrix_incluster.sh`; RBAC `get` on `stateful-runner-stop`; grace 900 s | `check.sh` runs the runner's own `on_term` on 3 cases (before budget: no teardown; budget end: teardown; marker: teardown): pass; with the rule broken it fails (`111`) |
| 2 consumer memory + v43d stateless consumer | `secs >= 1200`: stateful 6/10 Gi; stateless consumer on `TAG` (v43d), `--no-raw`, 6/10 Gi | rendered load Jobs: consumer `load:v43d` + `--no-raw` + 6/10 Gi, producers `load:v43` |
| 3 `RUN_EST_S` | passed through `runner-job.yaml` / `deploy_runner.sh` (default 780; launch with 1260) | rendered Job carries it; dry run with 1260 cuts nothing |
| 4 plans | `plan_f30a..d` (`PLAN_SET=F30A..D`), `-at50k` label at the base rate for `F30*` | `check.sh`: all 11 plan sets incl. F30A-D dry-run ok, placement ok (11 engines pinned, 6 stateless loads unpinned per cluster), self-teardown reached; with the launch budgets: A/B/D 11 runs in 9.5 h, C in 10.0 h, 0 cut |
| 5 log size | `eks/cluster.yaml` (template) and the gitignored `eks/cluster.rtm-flink-bench7{a..d}.local.yaml`: kubelet drop-in `containerLogMaxSize: 200Mi` on both nodegroups | YAML parses; the pre-bootstrap script runs and writes valid JSON (tested against a scratch dir). Effect on a real node: the smoke |

Smoke (after GO, on cluster A only, `SELF_TEARDOWN=0 NO_DEADLINE=1`, ~15 min): one
`spark-rtm:s1:1:secs=300` and one `sl-flink:p6:1:secs=300` with `PLAN_SET=F30A`. Pass = both runs
ok, sampler files non-empty, consumer peak memory under its request, node `/configz` shows
`containerLogMaxSize: 200Mi`, and a TERM test: delete the smoke Job before its end without the
marker; the cluster must stay up (no teardown in the log).

Launch after GO: arm the four deadlines at create, create the four clusters in parallel (separate
kubeconfigs via `KUBECONFIG_FILE`, configs via `CLUSTER_CONFIG=cluster.rtm-flink-bench7x.local.yaml`),
install, smoke on A, preflight all four (`eks/stateful/preflight_runner.sh`, before each Job), launch with
`CLUSTER=rtm-flink-bench7x PLAN_SET=F30X BUDGET_S=<34200|35900> RUN_EST_S=1260 SELF_TEARDOWN=1
NO_DEADLINE=1 deploy_runner.sh`, poll in turns of <= 45 min ending in a STATUS line, verify each
self-teardown, `eksctl delete cluster`, disarm, verify nothing is left and the NAT is available.
