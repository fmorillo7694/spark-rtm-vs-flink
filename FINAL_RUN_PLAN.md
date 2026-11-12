# Final lean run (option B): plan (GO given; launched per the procedure below)

The cells behind the post's headline tables and charts (post Appendix B, trimmed), **3 runs per
cell**, the **5-minute measured window** of sessions A-C (60 s warm-up + 300 s), the new
measurement stack, on **two clusters in parallel**. Waiting for the coordinator's go.

Dropped (as decided): the capacity probe (the post keeps the extrapolation, labelled), hot keys,
exactly-once, final-result windows, the checkpoint-interval cells, micro-batch extras, the scaling
top-up (session D/D2 data stays as is).

## Cells (32 cells x 3 runs + O7 x 4 = 100 runs)

| block | cells (runner label without `-rN`) | rate | runs |
|---|---|---|--:|
| stateless p6 | `spark-rtm-java-p6`, `spark-rtm-scala-p6`, `pyspark-rtm-p6`, `flink-p6` (Java), `pyflink-p6`, `spark-rtm-syncckpt-p6` (async progress off, control) | 100k | 18 |
| S1 2x2 + vendor | `spark-rtm-s1` (50 ms), `spark-rtm-sw1-s1` (1 ms), `spark-rtm-vt-s1`, `flink-ds-bt100-s1-update` (100 ms), `flink-ds-s1-update` (0 ms) | 50k | 15 |
| S2 2x2 + vendor + dedup | `spark-rtm-s2`, `spark-rtm-sw1-s2`, `spark-rtm-vt-s2`, `flink-ds-bt100-s2-et`, `flink-ds-s2-et`, plus the two dedup variants the post reports: `flink-sql-s2-ttl60` (SQL 60 s TTL) and `flink-ds-s2` (leaky 5 s TTL, "the trap") | 50k | 21 |
| S3 2x2 + vendor | `spark-rtm-s3`, `spark-rtm-sw1-s3`, `spark-rtm-vt-s3` (first vendor run on `s3`), `flink-ds-bt100-s3`, `flink-ds-s3` | 50k | 15 |
| S4 broadcast join | `spark-rtm-s4`, `flink-ds-bt100-s4`, `flink-ds-s4` | 50k | 9 |
| S5 interval join | `spark-mb-s5-at8k`, `spark-mb1s-s5-at8k`, `flink-ds-bt100-s5-at8k`, `flink-ds-s5-at8k` (`S5_RATE` 1000/pod, ~8k orders + ~6.8k payments/s) | 1000/pod | 12 |
| O7 (approved) | the same 4 S5 arms at 2000/pod (`-at16k`), 1 run each | 2000/pod | 4 |
| recovery | `spark-rtm-s1-kill`, `flink-ds-s1-update-kill`: one worker pod killed 180 s into the load, truth capture on | 50k | 6 |

## Split: two clusters, one runner Job each

| | cluster A `rtm-flink-bench6a` (`PLAN_SET=FA`) | cluster B `rtm-flink-bench6b` (`PLAN_SET=FB`) |
|---|---|---|
| cells | stateless (6), S2 (7), S4 (3); O7 (4 x 1) last | S1 (5), S3 (5), S5 at 1000 (4), recovery (2) |
| runs | 52 | 48 |
| runner estimate (dry run) | 7.7 h | 7.3 h |
| `BUDGET_S` | 30600 (8.5 h) | 28800 (8.0 h) |
| cap (cluster-h, estimate + 25 %) | 11.0 | 10.5 |
| EventBridge deadline | create + 11.0 h (nodegroups; cluster +45 min) | create + 10.5 h |

Order inside each Job: every cell once (r1), then r2, then r3, Spark and Flink arms alternating, so
a budget stop cuts whole rounds, never one engine. Dry runs: `results/final-run/dryrun-FA.txt` and
`dryrun-FB.txt` (every manifest rendered and validated, placement checked, 0 runs cut by the budget,
self-teardown step reached).

**The 2.5 h per cluster in the brief does not hold.** Measured per-run time is 8.5 min for a
5-minute window (session B: 85 runs in 12.0 h; per arm: stateless ~9 min, PyFlink ~10, Spark S3
~12.2, S4 ~9.4 incl. the reference-table load, others ~8.4). 100 runs are ~15 h of runner time,
so two clusters take ~7.5 h each. 2.5 h per cluster would need 6 clusters (~$5 of create/teardown
overhead each; the EC2 quota is 1,152 vCPU and the private subnets have ~8,190 free IPs each, so
6 x 6 nodes fits). Alternatively 1 run per cell on two clusters (~2.5 h each).

## Time and cost

| | A | B | total |
|---|--:|--:|--:|
| create + install (bench5: 27 min) | 0.5 h | 0.5 h | |
| sampler smoke (A only, below) / B waits for it | 0.25 h | 0.25 h | |
| runner | 7.7 h | 7.3 h | 15.0 h |
| self-teardown + `eksctl delete cluster` (bench5: ~20 min) | 0.35 h | 0.35 h | |
| **cluster-h** | **8.8** | **8.4** | **~17.2** |
| **$** at $2.6/h (below) | $23 | $22 | **~$45** |

Rate: 6 x m5.2xlarge on-demand $2.30/h + EKS control plane $0.10/h + EBS (6 x 100 GB root + 3 x
100 GB Kafka gp3) ~$0.10/h + logs / NAT data ~$0.10/h = **~$2.6/h**, the rate session D2 came out
at (~$12 for 4.6 cluster-h). The older "$4-5/h" figure in SCALING_PLAN.md was a rough bound, not a
measurement. **Point estimate ~$45, under the $60 launch limit.**

**Hard cap = estimate + 25 %: ~$56, i.e. 11.0 cluster-h on A and 10.5 on B.** Each cluster's
EventBridge deadline deletes its nodegroups at create + cap (A +11.0 h, B +10.5 h) and the
control plane 45 min later. The runner budgets (8.5 h / 8.0 h) stop starting runs well before.

Both clusters run the default 6 nodes (2 tainted engine nodes + 4 `bench-ng`); no Cluster
Autoscaler (no scaling arms).

## Placement

- Stateful engines: pinned to the 2 tainted engine nodes, one worker per node (as session B).
- **Stateless engines: pinned the same way** (`SL_NODES=dedicated` for `PLAN_SET F*`), load pods
  unpinned on `bench-ng`. Session B ran stateless on one shared 6-node pool to reproduce the
  published 4.3 round; the final run pins them so per-node cAdvisor CPU and the engine-CPU
  attribution use the same footprint as the stateful arms. The post should say the final
  stateless numbers come from pinned engines (2 nodes for 2 workers), so they can differ a little
  from the published shared-pool runs.

## Measurement stack (every run, `MEASURE=1` for `PLAN_SET F*`)

`eks/stateful/sampler.py`, started with the load and stopped with the consumer:

| file | what | how |
|---|---|---|
| `cadvisor.jsonl` | every 2 s, every node: per container of spark / flink / kafka pods cumulative CPU s, working set, RSS, CFS throttled / total periods; node root CPU / working set / RSS | kubelet cAdvisor via the API server node proxy (new RBAC: `nodes/proxy get`) |
| `jvm.jsonl` | every 10 s: Spark driver + executors heap total / used (`jcmd GC.heap_info`); Flink TMs + JM heap, non-heap, direct, metaspace used | `kubectl exec` (Spark), Flink REST metrics |
| `rocksdb.jsonl` | every 10 s, Flink: per vertex sum over subtasks of block-cache usage, all memtables, table readers, live data size | `state.backend.rocksdb.metrics.*` enabled on every Flink stateful arm (keys checked against the 2.3.0 jar), Flink REST |
| `progress.jsonl` | Spark per batch: state-store memory, SST size, plus `rocksdbPinnedBlocksMemoryUsage` / `rocksdbFilesCopiedBytes` / `rocksdbFilesReused` when 4.3 reports them | existing `spark_progress.py`, keys added |
| `ckpt_s3.json` | after the run: objects and bytes of the run's checkpoint prefix, per subdirectory (Spark `state/`, `offsets/`, `commits/`; Flink `chk-*`, `shared/`) | S3 list; the object count is the **PUT lower bound** |
| `top.txt` | `kubectl top` every 5 s, kept for comparison with sessions A-D | unchanged |

**S3 request counts:** only the PUT lower bound above. GET / LIST counts need either S3 CloudWatch
request metrics on the bucket (a bucket configuration change, ~$0.30/metric/month) or client-side
S3A / Flink filesystem statistics; neither is wired. Say which one if the post needs them.

Not verified on a cluster: the sampler's parsers are unit-tested on synthetic cAdvisor and
`jcmd` output, but the API-server proxy, `jcmd` in the executor containers, and the Flink
RocksDB metric ids are only known to work once a run produces them. Hence the smoke.

## Launch procedure (after the go)

1. Credentials (an admin role in <ACCOUNT_ID>). Configs `eks/cluster.rtm-flink-bench6{a,b}.local.yaml`
   (copies of `cluster.local.yaml`, gitignored), separate kubeconfigs via `KUBECONFIG_FILE`.
2. Per cluster, at create: arm its EventBridge deadline (`CLUSTER=... DEADLINE_EPOCH=create+cap
   arm_deadline.sh arm`), then `01_create_cluster.sh`, `02_install.sh`, `02b_irsa_s3.local.sh`.
   Both clusters are created in parallel.
3. **Sampler smoke on A** (one 7-min `flink-ds:s1-update` run + one stateless Spark run,
   `SELF_TEARDOWN=0 NO_DEADLINE=1`): pass = `cadvisor.jsonl`, `jvm.jsonl` and `rocksdb.jsonl`
   non-empty with < 5 % `err` lines, and `ckpt_s3.json` written. Fail -> fix and re-smoke before
   any matrix Job.
4. **Self-teardown preflight on each cluster** (as in D2): Job env `SELF_TEARDOWN=1`, IAM
   simulation of `eks:UpdateNodegroupConfig` / `DescribeNodegroup` on both nodegroups, RBAC
   `can-i` for Kafka / node pool / PVC delete and PV list. Saved to `logs/teardown_preflight.txt`.
5. Launch one runner Job per cluster: `CLUSTER=rtm-flink-bench6a PLAN_SET=FA BUDGET_S=30600
   NO_DEADLINE=1 deploy_runner.sh` and the same for `6b` / `FB` / 28800.
6. Poll in turns of <= 45 min, each ending with a STATUS line (runs done/total per cluster,
   cluster-h, failures).
7. Wrap-up per cluster: after the self-teardown, `eksctl delete cluster --wait`, disarm the
   deadline, verify no bench6a/6b clusters, stacks, instances, tagged volumes or schedules, NAT
   available. Results to `results/final-run/{a,b}/`, `RESULTS.md`, commit (no push).

Untouched throughout: the bench4 leftovers (3 volumes, 9 stacks), the NAT, EIP, vpce,
`00_egress.sh`, `99_teardown.sh`.
