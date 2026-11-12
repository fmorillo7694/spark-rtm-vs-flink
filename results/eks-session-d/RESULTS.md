# EKS session D: scaling under a load step (S1, 50k -> 100k -> 50k evt/s)

Cluster `rtm-flink-bench4` (us-east-1), 2026-11-10 18:53 UTC -> 2026-11-11 ~10:00 UTC (deleted by the
EventBridge deadline). Design: `SCALING_PLAN.md` (base design). Runner prefix
`s3://rtm-flink-bench-ckpt-<acct>/stateful/sessd-20261110T192841Z/`. Images `v43d`.
Per-run tables: `scaling.md` / `scaling.json` (`bench/analyze_scaling_d.py`), `resources.md`
(`bench/resource_summary.py`). The truth-sample files `truth.jsonl` / `maxcnt.json` (~650 MB) are not committed; they stay under the runner prefix in S3.

**Status: complete with session D2.** Session D produced 10 of the 14 planned items (3 smokes + 8
arm runs, with flink-auto-ro as smoke c). The 4 missing ones (`flink-reactive` x2 on this profile,
and the 100k -> 25k scale-in for `flink-auto` and `flink-reactive`) ran in session D2 on a new
cluster; see **Session D2** at the end. The Flink Reactive row in the table below comes from D2.

## Setup

- S1 (`s1-update` on Flink, `s1` on Spark), 24-partition topics, `pipeline.max-parallelism` 120,
  12 Spark state partitions. Load per producer pod x 8: 6250/s for 540 s (60 s warmup + 8 min
  measured), 12500/s for 1020 s, 6250/s for 1220 s; 45 min measured. Truth sample 1 % of users.
- Engine nodes: `engine-ng` m5.2xlarge, Cluster Autoscaler min 1 / max 5 (hard cap), engine pods
  `safe-to-evict=false`. Kafka, load and runner on 4 x m5.2xlarge `bench-ng`.
- Worker pod size everywhere 3 CPU / 6 GiB (Flink TM, Spark executor), as in sessions A-C.

| arm | mechanism | shape |
|---|---|---|
| flink-auto | operator autoscaler, native + adaptive scheduler, in-place rescale | start p=6 (2 TMs), vertices 3..12; stabilization 2 min, window 4 min, scale-down interval 6 min |
| flink-auto-ro | the same, `scaling.enabled: false` (smoke c, 30 min profile) | recommendations only |
| flink-peak | static, sized for 100k | p=12, 4 TMs |
| spark-rtm-peak | Spark RTM static, sized for 100k | 4 executors x 9 slots (24 source + 12 state tasks) |
| spark-rtm-manual | RTM restarted by the runner at each step, same checkpoint | low 2 x 18 slots (6 cores), peak 4 x 9 (12 cores) |
| spark-mb-da | micro-batch (default trigger) + dynamic allocation, shuffle tracking | 1..4 executors x 3 slots; **not RTM** |

## Smokes

| smoke | result |
|---|---|
| (a) self-teardown, short runner Job | **pass**: Kafka CR, node pool and 3 broker PVCs deleted, 0 kafka PVs left, Cluster Autoscaler scaled to 0, both nodegroups to 0 (`logs/smoke_a_verify.txt`). Verifies the region fix and `2996ed3`. Cluster then restored by hand. |
| (b) S4 Spark start gating (`1f06ad2`) | **pass**: input 18,848,434, output 18,471,166; the difference 377,268 = the users missing from the reference table (inner join, 2 % of user ids), the same as session C r2/r3. The start race of C r1 (-302,229 more) is gone. |
| (c) Flink autoscaler report-only, 30 min | ran; recommended scaling **down** (sources 6 -> 3, window/sink 6 -> 4) even during the 100k step: capacity it measured ~130k (window) and ~560k (source) records/s at p=6 vs a ~96k target. Attempt 1 failed: the operator rejects `last-state` without `execution.checkpointing.dir` in the spec (fixed, `55216e9`). |

## Results per arm

Phase columns: 1 = 50k (includes job warm-up), 2 = 100k, 3 = back to 50k. **steady** = s from the step
until every later 60 s window has pipeline p99 < 2 s ("-" = never within the phase). p99 is over
all records of the phase. Node-h = engine nodes over the load; "vs peak" = node-h relative to the
static peak arm of the same engine.

| arm (runs) | steady s 1/2/3 | p99 ms 1/2/3 | worst 1 s p99, phase 2 | rescales | engine nodes mean | node-h vs peak | lost / overcount |
|---|---|---|---|---:|---:|---:|---|
| flink-peak (1) | 59 / 0 / 0 | 450 / 1,140 / 410 | 2.2 s | 0 | 4.0 | 1.0 | 0 / 0 |
| flink-auto (2) | 89-90 / 979, - / 559-690 | ~1,600 / ~2,750 / ~1,800 | 7.5-8.2 s | 2-3 | 2.0 | 0.5 | 0 / 0 |
| flink-auto-ro (1, 30 min) | 60 / 860 / 0 | 1,500 / 2,000 / 1,420 | 3.5 s | 1 (startup) | 2.0 | 0.5 | 0 / 0 |
| flink-reactive (2, D2) | 389-460 / 289-290 / 0 | 11,900-14,200 / 3,900-4,500 / 540-590 | 12-13 s | 3 (6 -> 9 -> 12) | 3.7-3.9 | ~0.95 | 0 / 0 |
| spark-rtm-peak (1) | - / - / - | 71,400 / 6,500 / 4,400 | 9.6 s | n/a | 4.1 | 1.0 | 0 / 0 |
| spark-rtm-manual (2) | - / - / - | ~215,000 / ~170,000 / ~212,000 | ~224 s | 2 restarts | 3.3 | ~0.8 | 0 / 0 |
| spark-mb-da (2) | - / - / - | 12-14 s / 20-31 s / 6.2 s | 31-45 s | 1 (2 -> 3 executors) | 3.0 | 0.73 | 0 / 0 (settled windows) |

Truth check: every run 0 lost and 0 overcounted windows over 0.9-1.3 M settled sampled windows.

What the numbers say:

- **Flink static peak** is the only arm that holds p99 < 2 s through the step, at 2x the engine
  nodes of the autoscaler arm.
- **Flink autoscaler** did not scale **up** for the 100k step. With the shortened timings it
  scaled the job **down** (6 -> 3 sources, 6 -> 4 window) during or after the step, kept 2 engine
  nodes, and paid for it with a phase-2 p99 of ~2.7 s and 7-8 s spikes; r2 never reached steady
  state in phase 2. Half the node-hours of the peak arm. Its capacity estimate at p=6 (~130k/s for
  the window vertex) was above the 100k target, so by its own model no scale-up was needed; the
  latency cost is the checkpoint spikes (avg 3.1 s duration at p=3-4 vs 2.0 s at p=12) it does not
  model. In-place rescales cost 1-3 s output stalls.
- **Spark RTM static peak**: phase 1 is skewed by start-up: executors came up one by one as
  Cluster Autoscaler added nodes (0 -> 1 -> 2 -> 4 executors), so the first minutes ran short of
  slots (71 s p99, 59 s stall). Once at 4 executors it carried 100k with p99 6.5 s; every 60 s
  RTM batch boundary still spikes to 2-14 s, so the 60 s window criterion never holds for any
  Spark RTM arm, at any load (as in sessions B/C, where Spark RTM p99 was ~0.5 s at 50k on a fixed
  2-node shape with no autoscaled start).
- **Spark RTM manual restart**: each restart is a stop -> resubmit -> query start of 25-320 s
  (`manual.txt`), and the backlog built during it takes ~5-10 min to drain at p99 ~200 s. The low
  shape (2 executors, 6 cores for 36 slots) is also slower at 50k than the peak shape. Restarting
  to scale is far more disruptive than the load step itself.
- **Spark micro-batch + DA** works mechanically (DA scaled 2 -> 3 executors) but never caught up:
  p99 6-31 s, and at the end of each run 27-28 M input records (15 %) were still unprocessed when
  the runner's 6-min drain wait ended (output < input in `verify.json`). The settled-window truth
  check is still clean; the shortfall is lag, not loss. Labelled not-RTM.

CPU / memory and checkpoint / commit numbers per arm: `resources.md`.

## Flink Reactive: what broke

`flink-reactive` (standalone application mode + `scheduler-mode: reactive` + HPA) failed both
attempts of r1: the JobManager crash-looped with `ClassNotFoundException: bench.StatefulDataStream`.
In standalone mode the operator mounts an empty `user-lib-dir` volume over `/opt/flink/usrlib`,
hiding the job jar baked into the image. Fix (`7c69c1c`, in `flink-scaling.yaml`, rendered only for
the reactive arm): a JobManager-only initContainer copies the jar into that volume (TaskManagers have
no such volume and fetch code from the JM). Verified on a small diagnostic deployment on this
cluster: the job reached RUNNING at reactive parallelism 3/3. In session D r2 was skipped deliberately
(an S3 placeholder) so the main runner would not crash on it again. Session D2 ran the fix on real
runs: the JobManager started every time, and the HPA did drive the job (below).

## Operations, cost

| item | value |
|---|---|
| cluster lifetime | 18:53 UTC create -> 09:51 UTC delete call (EventBridge deadline), ~15.1 h incl. deletion |
| runner | main Job 19:28 -> 04:06 UTC (8.6 h, `MATRIX DONE elapsed=31014s`); smoke (a) 19:18-19:25 |
| idle | 04:06 -> 09:06 UTC nodes up, no work (5.0 h, ~4 bench + 1 engine node); control plane to ~10:00 |
| engine node-hours over loads | ~20 (`scaling.json`) |
| estimated cost | ~$38 total (~87 node-h x $0.384 + control plane + EBS + logs); of which ~$10 idle |

Estimates, not billing data: the ASGs are deleted, so node-hours outside the measured loads are
reconstructed from the run log and node samples.

Why it idled: the main runner's self-teardown was disabled on purpose at 22:55 UTC (Kafka/PVC
delete RBAC, kube-system binding and the IAM scale-down policy removed) so Kafka would stay up for
the D2 Job; D2 was then never launched and nobody restored the teardown. The EventBridge deadline
(armed at T0 + 14.95 h) deleted the nodegroups at 09:06 and the cluster at 09:51 as designed. It
does not delete PVCs, so the **3 Kafka broker volumes are orphaned** (below).

## Teardown state (2026-11-11 10:50 UTC)

- EKS cluster, nodegroups, instances: gone. EventBridge schedules: gone (deleted after firing).
- Load balancers in the VPC: none. ENIs in the VPC: only the NAT gateway's.
- NAT `<NAT_ID>`: available, untouched. EIP and vpce untouched.
- **Orphaned EBS volumes** (gp3, 100 GB each, `available`), awaiting the user's go-ahead to delete:
  `<VOLUME_ID>` (data-0-bench-brokers-0), `<VOLUME_ID>` (brokers-1),
  `<VOLUME_ID>` (brokers-2). ~$24/month together.
- **CloudFormation stacks left** in CREATE_COMPLETE, termination protection enabled:
  `eksctl-rtm-flink-bench4-cluster`, `-nodegroup-engine-ng`,
  `-nodegroup-bench-ng`, `-addon-vpc-cni`, `-addon-aws-ebs-csi-driver`, and four
  `-addon-iamserviceaccount-*` stacks. Their EKS resources are deleted; what remains is 2 security
  groups, 2 launch templates and IAM roles (no hourly cost). Deleting them needs termination
  protection turned off first, which needs the user's confirmation.

## Session D2: Flink Reactive and the scale-in profile

Cluster `rtm-flink-bench5` (new name, so bench4's protected stacks are not touched), 2026-11-11 11:10
-> 15:45 UTC. Same setup, images, CA cap (engine max 5) and job settings as D. Results:
`results/eks-session-d2/` (`scaling.md`, `scaling.json`, `resources.md`; the smoke is in
`smoke/`). Runner prefix `stateful/sessd2-20261111T115742Z/`. All 4 runs in ONE runner Job, 4/4
ok on the first attempt, 12,459 s.

**Reactive smoke (10 min, 50k -> 100k):** pass. The JobManager started (jar fix `7c69c1c`), the HPA
went 2 -> 3 -> 4 TaskManagers on CPU, Cluster Autoscaler added engine nodes 2 -> 4, and Reactive
Mode rescaled the job 6 -> 9 -> 12 on its own. p99 12.4 s: each rescale is a restart.

**Flink Reactive + HPA, base profile (50k -> 100k -> 50k), x2:**

| run | steady s 1/2/3 | p99 ms 1/2/3 | worst 1 s p99 | path (parallelism, HPA) | engine nodes mean | lost / over |
|---|---|---|---|---|---:|---|
| r1 | 389 / 289 / 0 | 11,900 / 4,500 / 590 | 16.7 s | 6 -> 9 -> 12, HPA 2 -> 4 | 3.9 | 0 / 0 |
| r2 | 460 / 290 / 0 | 14,200 / 3,900 / 540 | 19.6 s | 6 -> 9 -> 12, HPA 2 -> 4 | 3.7 | 0 / 0 |

The HPA added a third TaskManager already in the 50k phase (+2.7 min; CPU over 70 % of 2 x 3
cores at 50k, job 6 -> 9 at +5.5 min) and the fourth 80 s after the 100k step (job at 12 at +3.6
min after the step). Each rescale is a restart (a 10-20 s p99 spike plus catch-up), which is what
the phase-1 and phase-2 p99 pay for. After that, phase 3 at 50k ran at p99 ~0.55 s, the best of
any autoscaled arm. **It never scaled back in**: 4 TaskManagers through the 20 min at 50k
(5-min scale-down window; CPU per TM stayed above what a 3-TM shape would need). Engine
node-hours ~0.95x the static peak (flink-peak, session D), with worse latency than the static peak
in phases 1 and 2.

**Scale-in profile (100k for 17 min incl. warmup, then 25k for 28 min), x1 each:**

| run | steady s 1/2 | p99 ms 1/2 | worst 1 s p99 1/2 | path | engine nodes mean | lost / over |
|---|---|---|---|---|---:|---|
| flink-auto | 589 / 759 | 13,500 / 1,450 | 20.9 s / 4.7 s | parallelism 6 -> 12,8 (up) -> 6 -> 3 (in) | 3.1 | 0 / 0 |
| flink-reactive | 640 / 380 | 17,500 / 29,700 | 22.0 s / 46.0 s | 6 -> 9 -> 12 -> 9, HPA 2 -> 4 -> 3 | 3.4 | 0 / 0 |

- **The Flink autoscaler does scale up** when the job starts directly at 100k (6 -> sources 12,
  window 8, at +8.8 min), and back down in two steps at 25k (6 at +4.9 min, 3 at +11.9 min). In session D's
  50k -> 100k profile it scaled down instead, because the 50k phase had already given it a
  capacity estimate above 100k at p=6. Its 25k phase is good: p99 1.45 s.
- **Reactive + HPA scaled in only one step** (4 -> 3 TMs, +5.4 min) at 25k, and that one rescale cost the
  worst phase of the session: p99 29.7 s, a 46 s spike, ~6 min to recover.
- Both: 0 lost / 0 overcount over ~1.0 M settled windows; output > input by 0.5-1.7 M records
  (at-least-once re-emits after each rescale restart).

**D2 operations:** self-teardown verified before launch (`logs/teardown_preflight.txt`: IAM
simulation for the scale-down calls, RBAC for Kafka / PVC delete and CA scale) and it worked: Kafka
and its PVCs deleted (no `kubernetes.io/cluster/rtm-flink-bench5` volume left), nodegroups at 0 by
15:32. Then `eksctl delete cluster` (15:33 -> 15:45; eksctl 0.225 removes the termination
protection it sets on its own stacks), deadline disarmed. Verified at 15:45 UTC
(`logs/teardown_verify.txt`): no clusters, no bench5 stacks, instances, volumes or schedules, the
NAT gateway available. Cost ~4.6 cluster-h (cap 6.5), ~29 node-h, **~$12** (estimate, not
billing). Idle: none beyond the ~15 min of teardown.

The bench4 leftovers above are unchanged. Because eksctl handles its own termination protection,
`eksctl delete cluster --name rtm-flink-bench4` is the likely way to remove those 9 stacks; not run,
pending the user's decision.
