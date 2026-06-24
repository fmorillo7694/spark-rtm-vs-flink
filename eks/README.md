# EKS phase — Spark RTM vs Flink at scale (~100k evt/s)

Ports the local benchmark to a production-grade EKS cluster: Strimzi Kafka (3 brokers,
KRaft), the Spark-on-K8s operator, and the Flink Kubernetes operator, driving the **same**
Scala RTM and Java DataStream pipelines at ~100k events/sec.

## Account / safety

- Runs in your own AWS account, region `us-east-1` (change in `cluster.yaml`).
- Security baseline: KMS secrets envelope encryption, OIDC/IRSA, IMDSv2 required,
  private node subnets, encrypted EBS, control-plane logging.
- **Before running**, substitute the placeholders in `cluster.yaml` with your values:
  `<YOUR_ACCOUNT_ID>`, `<YOUR_KMS_KEY_ID>`, `<YOUR_VPC_ID>`, and the four subnet IDs.
  If your account has VPC/EIP headroom you can instead delete the whole `vpc:` block and
  let `eksctl` create a fresh VPC (the default). We reused an existing VPC's subnets only
  because our account was at its VPC (5/5) and Elastic-IP limits.
- Everything is tagged `lifecycle: ephemeral-benchmark`. **`99_teardown.sh` removes it all.**

## Flow

```bash
export AWS_PROFILE=default

eks/01_create_cluster.sh        # EKS cluster (rtm-flink-bench3), 6x m5.2xlarge, ~18 min
eks/02_install.sh               # metrics-server + Strimzi(3-broker Kafka) + Flink & Spark operators
eks/03_build_push_images.sh     # build+push spark/flink/pyflink/load images to ECR

eks/04_run_benchmark.sh spark-rtm   # deploy SparkApplication, fan out 100k/s, measure, clean
eks/04_run_benchmark.sh flink       # deploy FlinkDeployment, same load, measure, clean

eks/99_teardown.sh              # delete cluster, KMS, ECR — leaves shared VPC intact
```

Results land in `results/eks/<engine>_consumer.log` (latency summary) and
`results/eks/<engine>_top.txt` (pod CPU/mem samples).

## Sizing

| component | spec | why |
|---|---|---|
| node group | 6x m5.2xlarge (48 vCPU / 192 GiB) | room for Kafka + engine + load |
| Kafka | 3 brokers, repl=3, min.isr=2, acks=1 (producer) | durable, per the spec |
| topics | input/output, 12 partitions | RTM one-core-per-partition headroom |
| Spark RTM | 4 executors x 4 cores | ≥1 core per partition |
| Flink | 3 TMs x 4 slots, parallelism 12 | match partitions |
| load | 8 producer pods x 12.5k/s | ~100k/s aggregate |

## Notes

- Two empty `ROLLBACK_COMPLETE` CFN husks (`rtm-flink-bench`, `rtm-flink-bench2`) remain
  from failed first attempts (VPC/EIP limit, then a KMS region mismatch). They hold no
  resources; delete from the CFN console (they have eksctl termination protection).
- m5 nodes are amd64 — images are built `--platform linux/amd64`.
