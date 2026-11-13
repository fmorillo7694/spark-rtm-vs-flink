#!/usr/bin/env python3
"""
In-cluster measurement sampler for the final run (post Appendix B, "Measurement upgrades"), run by
eks/stateful/run_matrix_incluster.sh next to each load phase. Stdlib only (the load image's python),
plus kubectl for the Spark `jcmd` exec. Every sample is one compact JSON line.

  sample  --out DIR --kind spark|flink --flink-rest URL --spark-pods SELECTOR
      until killed:
        every 2 s  cadvisor.jsonl  per node (all nodes, parallel): for every container of the
                   spark / flink / kafka namespaces [cpu_seconds_total, working_set_bytes, rss_bytes,
                   cfs_throttled_periods, cfs_periods], plus the node root cgroup [cpu_s, ws, rss].
                   Source: kubelet cAdvisor via the API server node proxy
                   (/api/v1/nodes/<n>/proxy/metrics/cadvisor; RBAC nodes/proxy get). CPU rate and
                   throttle % are derived in analysis from consecutive samples.
        every 10 s jvm.jsonl  Spark: `jcmd <pid> GC.heap_info` in the driver and each executor
                   (heap total / used KB). Flink: REST metrics of every TaskManager and the
                   JobManager (Status.JVM.Memory.Heap/NonHeap/Direct/Metaspace .Used).
                   rocksdb.jsonl  Flink only: per job vertex, the sum over subtasks of every metric
                   ending in block-cache-usage, size-all-mem-tables, estimate-table-readers-mem,
                   estimate-live-data-size (enabled by state.backend.rocksdb.metrics.* in the
                   stateful Flink manifests). Spark's state-store RocksDB memory comes from the
                   batch progress instead (spark_progress.py).
  s3sum   --bucket B --prefix P --out FILE
      the checkpoint prefix after the run: objects and bytes per top-level subdirectory
      (Spark: offsets / commits / state / sources; Flink: chk-* / shared / taskowned). The object
      count is the PUT-request lower bound for the run (checkpoint files are written once;
      multipart uploads add parts); GET / LIST counts are not measured.

Errors in one source never stop the others; they are counted in the line (err field).
"""
import argparse
import concurrent.futures as cf
import json
import os
import re
import ssl
import subprocess
import sys
import threading
import time
import urllib.request

SA = "/var/run/secrets/kubernetes.io/serviceaccount"
KEEP = {"container_cpu_usage_seconds_total": 0, "container_memory_working_set_bytes": 1,
        "container_memory_rss": 2, "container_cpu_cfs_throttled_periods_total": 3,
        "container_cpu_cfs_periods_total": 4}
NAMESPACES = ("spark", "flink", "kafka")
LINE = re.compile(r'^(\w+)\{([^}]*)\}\s+(\S+)')
LABEL = re.compile(r'(\w+)="([^"]*)"')
ROCKS = ("block-cache-usage", "size-all-mem-tables", "estimate-table-readers-mem", "estimate-live-data-size")
JVM_TM = ("Status.JVM.Memory.Heap.Used", "Status.JVM.Memory.NonHeap.Used",
          "Status.JVM.Memory.Direct.MemoryUsed", "Status.JVM.Memory.Metaspace.Used")
KNOWN_DIRS = {"shared", "taskowned", "offsets", "commits", "state", "sources", "metadata", "ha"}


def parse_cadvisor(text):
    """cAdvisor exposition text -> ({"ns/pod/container": [5 values]}, root [cpu, ws, rss])."""
    cont, root = {}, [None, None, None]
    for line in text.splitlines():
        if not line.startswith("container_"):
            continue
        m = LINE.match(line)
        if not m or m.group(1) not in KEEP:
            continue
        lab = dict(LABEL.findall(m.group(2)))
        i, v = KEEP[m.group(1)], float(m.group(3))
        if lab.get("id") == "/":
            if i < 3:
                root[i] = v
            continue
        ns, c = lab.get("namespace", ""), lab.get("container", "")
        if ns not in NAMESPACES or not c or c == "POD":
            continue
        k = f"{ns}/{lab.get('pod', '')}/{c}"
        cont.setdefault(k, [None] * 5)[i] = v
    return cont, root


class Api:
    """Kubernetes API over the service-account token (in-cluster)."""
    def __init__(self):
        self.base = f"https://{os.environ['KUBERNETES_SERVICE_HOST']}:{os.environ.get('KUBERNETES_SERVICE_PORT', '443')}"
        self.ctx = ssl.create_default_context(cafile=f"{SA}/ca.crt")

    def get(self, path, timeout=10):
        tok = open(f"{SA}/token").read().strip()   # kubelet rotates it; re-read every call
        req = urllib.request.Request(self.base + path, headers={"Authorization": f"Bearer {tok}"})
        with urllib.request.urlopen(req, timeout=timeout, context=self.ctx) as r:
            return r.read().decode()


def http_json(url, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode())


def emit(fh, lock, obj):
    with lock:
        fh.write(json.dumps(obj, separators=(",", ":")) + "\n")
        fh.flush()


def cadvisor_loop(api, out, lock, stop):
    pool = cf.ThreadPoolExecutor(8)
    nodes, nodes_t = [], 0
    while not stop.is_set():
        t = time.time()
        if t - nodes_t > 60:   # node list refresh (Cluster Autoscaler may add / remove nodes)
            try:
                nodes = [n["metadata"]["name"] for n in json.loads(api.get("/api/v1/nodes"))["items"]]
                nodes_t = t
            except Exception:
                pass

        def one(n):
            try:
                c, r = parse_cadvisor(api.get(f"/api/v1/nodes/{n}/proxy/metrics/cadvisor"))
                return {"t": round(t, 2), "node": n, "root": r, "c": c}
            except Exception as e:
                return {"t": round(t, 2), "node": n, "err": type(e).__name__}
        for rec in pool.map(one, nodes):
            emit(out, lock, rec)
        stop.wait(max(0.0, 2.0 - (time.time() - t)))


def spark_heap(selector):
    """jcmd GC.heap_info in each Spark pod -> {pod: [total_kb, used_kb]}."""
    pods = subprocess.run(["kubectl", "-n", "spark", "get", "pods", "-l", selector, "-o",
                           "jsonpath={.items[*].metadata.name}"], capture_output=True, text=True, timeout=20).stdout.split()
    res = {}
    for p in pods:
        cmd = 'for p in $(jcmd -l | awk "!/JCmd/{print \\$1}"); do jcmd $p GC.heap_info; done'
        r = subprocess.run(["kubectl", "-n", "spark", "exec", p, "--", "sh", "-c", cmd],
                           capture_output=True, text=True, timeout=20).stdout
        m = re.search(r"heap\s+total\s+(\d+)K,\s+used\s+(\d+)K", r)
        res[p] = [int(m.group(1)), int(m.group(2))] if m else None
    return res


def flink_jvm(rest):
    res = {}
    for tm in http_json(f"{rest}/taskmanagers")["taskmanagers"]:
        ms = http_json(f"{rest}/taskmanagers/{tm['id']}/metrics?get={','.join(JVM_TM)}")
        res[tm["id"]] = {m["id"]: float(m["value"]) for m in ms}
    ms = http_json(f"{rest}/jobmanager/metrics?get={','.join(JVM_TM)}")
    res["jobmanager"] = {m["id"]: float(m["value"]) for m in ms}
    return res


def flink_rocksdb(rest):
    jobs = [j for j in http_json(f"{rest}/jobs/overview")["jobs"] if j["state"] == "RUNNING"]
    if not jobs:
        return {}
    jid, res = jobs[0]["jid"], {}
    for v in http_json(f"{rest}/jobs/{jid}")["vertices"]:
        names = [m["id"] for m in http_json(f"{rest}/jobs/{jid}/vertices/{v['id']}/subtasks/metrics")
                 if any(m["id"].endswith(r) for r in ROCKS)]
        if not names:
            continue
        got = {}
        for i in range(0, len(names), 20):   # keep the query string short
            q = ",".join(names[i:i + 20])
            for m in http_json(f"{rest}/jobs/{jid}/vertices/{v['id']}/subtasks/metrics?get={q}&agg=sum"):
                got[m["id"]] = m.get("sum")
        tot = {r: sum(x for k, x in got.items() if k.endswith(r) and x is not None) for r in ROCKS}
        res[v["name"][:60]] = tot
    return res


def jvm_loop(args, jvm_out, rocks_out, lock, stop):
    while not stop.is_set():
        t = time.time()
        try:
            data = spark_heap(args.spark_pods) if args.kind == "spark" else flink_jvm(args.flink_rest)
            emit(jvm_out, lock, {"t": round(t, 2), "kind": args.kind, "jvm": data})
        except Exception as e:
            emit(jvm_out, lock, {"t": round(t, 2), "kind": args.kind, "err": type(e).__name__})
        if args.kind == "flink":
            try:
                emit(rocks_out, lock, {"t": round(t, 2), "rocksdb": flink_rocksdb(args.flink_rest)})
            except Exception as e:
                emit(rocks_out, lock, {"t": round(t, 2), "err": type(e).__name__})
        stop.wait(max(0.0, 10.0 - (time.time() - t)))


def s3sum(bucket, prefix, out):
    import boto3
    s3, agg = boto3.client("s3"), {}
    pre = prefix.strip("/") + "/"
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=pre):
        for o in page.get("Contents", []):
            rest = o["Key"][len(pre):]
            parts = rest.split("/")
            # Flink nests under <job id>/ (and ha/): name the checkpoint part wherever it sits
            top = next(("chk-*" if p.startswith("chk-") else p for p in parts[:-1]
                        if p.startswith("chk-") or p in KNOWN_DIRS), parts[0] if len(parts) > 1 else "(root)")
            a = agg.setdefault(top, [0, 0])
            a[0] += 1
            a[1] += o["Size"]
    tot = [sum(a[0] for a in agg.values()), sum(a[1] for a in agg.values())]
    json.dump({"prefix": pre, "objects": tot[0], "bytes": tot[1],
               "by_dir": {k: {"objects": v[0], "bytes": v[1]} for k, v in sorted(agg.items())}},
              open(out, "w"), indent=1)
    print(f"    s3 checkpoint: {tot[0]} objects, {tot[1] / 1e6:.1f} MB under {pre}")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="mode", required=True)
    s = sub.add_parser("sample")
    s.add_argument("--out", required=True)
    s.add_argument("--kind", choices=("spark", "flink"), required=True)
    s.add_argument("--flink-rest", default="http://flink-stateful-rest.flink.svc:8081")
    s.add_argument("--spark-pods", default="spark-role")
    c = sub.add_parser("s3sum")
    c.add_argument("--bucket", required=True)
    c.add_argument("--prefix", required=True)
    c.add_argument("--out", required=True)
    args = ap.parse_args()
    if args.mode == "s3sum":
        s3sum(args.bucket, args.prefix, args.out)
        return
    stop, lock = threading.Event(), threading.Lock()
    files = [open(os.path.join(args.out, n), "a") for n in ("cadvisor.jsonl", "jvm.jsonl", "rocksdb.jsonl")]
    import signal
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    th = [threading.Thread(target=cadvisor_loop, args=(Api(), files[0], lock, stop), daemon=True),
          threading.Thread(target=jvm_loop, args=(args, files[1], files[2], lock, stop), daemon=True)]
    for x in th:
        x.start()
    while not stop.is_set():
        stop.wait(1)
    for x in th:
        x.join(timeout=15)
    for f in files:
        f.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
