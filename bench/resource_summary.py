#!/usr/bin/env python3
"""
Per-run and per-arm resource and checkpoint numbers for an EKS stateful session directory
(results/eks-stateful, results/eks-stateful-b, results/eks-session-c, ...), from the files every
run already has:

  top.txt           kubectl top every ~5 s ("<epoch> <ns> <pod> <cpu> <mem>"). Engine pods = every
                    pod in the engine namespace except the operator/webhook pods. Per sample the
                    engine pods are summed (cores, GiB); mean and peak are over the samples, which
                    cover warmup + measured load.
  checkpoints.json  Flink REST /jobs/<id>/checkpoints: completed/total, end-to-end duration avg/max,
                    checkpointed size avg (incremental upload per checkpoint) and state size max
                    (full state referenced by a checkpoint).
  progress.jsonl    Spark batch progress (eks/stateful/spark_progress.py; session C onwards): one
                    line per RTM batch = checkpoint. State commit = sum of stateOperators
                    commitTimeMs (summed over partitions), log commit = walCommit + commitOffsets,
                    state memory = max memoryUsedBytes. Sessions A/B did not record it.

Arms = run label without -rN. Per-arm numbers are means over runs (peaks: max over runs).
Usage: python bench/resource_summary.py DIR [DIR ...]   (writes DIR/resources.md and .json)
"""
import collections
import glob
import json
import os
import re
import statistics as st
import sys

OPERATOR = re.compile(r"operator|webhook")


def parse_qty(cpu, mem):
    c = float(cpu[:-1]) / 1000 if cpu.endswith("m") else float(cpu)
    if mem.endswith("Mi"):
        m = float(mem[:-2]) / 1024
    elif mem.endswith("Gi"):
        m = float(mem[:-2])
    elif mem.endswith("Ki"):
        m = float(mem[:-2]) / 1024 / 1024
    else:
        m = float(mem) / 2 ** 30
    return c, m


def top_stats(path):
    """(cpu_mean, cpu_peak, mem_mean, mem_peak) of the summed engine pods, or None."""
    by_ts = collections.defaultdict(lambda: [0.0, 0.0])
    try:
        for line in open(path):
            f = line.split()
            if len(f) < 5 or OPERATOR.search(f[2]):
                continue
            c, m = parse_qty(f[3], f[4])
            by_ts[f[0]][0] += c
            by_ts[f[0]][1] += m
    except (OSError, ValueError):
        return None
    if not by_ts:
        return None
    cpu = [v[0] for v in by_ts.values()]
    mem = [v[1] for v in by_ts.values()]
    return st.mean(cpu), max(cpu), st.mean(mem), max(mem)


def flink_ckpt(path):
    try:
        d = json.load(open(path))
    except (OSError, ValueError):
        return None
    s, c = d.get("summary") or {}, d.get("counts") or {}
    e2e, ck, ss = s.get("end_to_end_duration") or {}, s.get("checkpointed_size") or {}, s.get("state_size") or {}
    return {"completed": c.get("completed"), "total": c.get("total"), "failed": c.get("failed"),
            "dur_avg_ms": e2e.get("avg"), "dur_max_ms": e2e.get("max"),
            "upload_avg_mb": (ck.get("avg") or 0) / 1e6, "state_max_mb": (ss.get("max") or 0) / 1e6}


def spark_progress(path):
    try:
        rows = [json.loads(line) for line in open(path) if line.strip()]
    except (OSError, ValueError):
        return None
    rows = [r for r in rows if r.get("stateOperators")]
    if not rows:
        return None
    commit = [sum(o.get("commitTimeMs") or 0 for o in r["stateOperators"]) for r in rows]
    log = [(r.get("walCommit") or 0) + (r.get("commitOffsets") or 0) for r in rows]
    mem = [sum(o.get("memoryUsedBytes") or 0 for o in r["stateOperators"]) for r in rows]
    wrote = [sum(o.get("rocksdbTotalBytesWritten") or 0 for o in r["stateOperators"]) for r in rows]
    return {"batches": len(rows), "state_commit_avg_ms": st.mean(commit), "state_commit_max_ms": max(commit),
            "log_commit_avg_ms": st.mean(log), "state_mem_max_mb": max(mem) / 1e6,
            "rocksdb_written_avg_mb": st.mean(wrote) / 1e6}


def run_stats(d):
    out = {"label": os.path.basename(d)}
    t = top_stats(os.path.join(d, "top.txt"))
    if t:
        out.update(cpu_mean=t[0], cpu_peak=t[1], mem_mean_gib=t[2], mem_peak_gib=t[3])
    f = flink_ckpt(os.path.join(d, "checkpoints.json"))
    if f:
        out["flink_ckpt"] = f
    s = spark_progress(os.path.join(d, "progress.jsonl"))
    if s:
        out["spark_batches"] = s
    return out


def fmt(x, nd=1):
    return "-" if x is None else f"{x:,.{nd}f}"


def summarize(root):
    runs = [run_stats(d) for d in sorted(glob.glob(os.path.join(root, "runs", "*"))) if os.path.isdir(d)]
    arms = collections.OrderedDict()
    for r in runs:
        arms.setdefault(re.sub(r"-r\d+$", "", r["label"]), []).append(r)

    def mean(rs, k, sub=None):
        v = [(r.get(sub) or {}).get(k) if sub else r.get(k) for r in rs]
        v = [x for x in v if x is not None]
        return st.mean(v) if v else None

    def peak(rs, k, sub=None):
        v = [(r.get(sub) or {}).get(k) if sub else r.get(k) for r in rs]
        v = [x for x in v if x is not None]
        return max(v) if v else None

    lines = [f"# Resources and checkpoints: `{root}`", "",
             "Engine pods only (operators excluded), summed per `kubectl top` sample (~5 s), over warmup + "
             "measured load. Per arm: mean over runs; peaks are the max over runs. Generated by "
             "`bench/resource_summary.py`.", "",
             "| arm | n | CPU mean | CPU peak | mem mean GiB | mem peak GiB | checkpoint / commit |",
             "|---|---:|---:|---:|---:|---:|---|"]
    for arm, rs in arms.items():
        if any("flink_ckpt" in r for r in rs):
            done = sum((r.get("flink_ckpt") or {}).get("completed") or 0 for r in rs)
            tot = sum((r.get("flink_ckpt") or {}).get("total") or 0 for r in rs)
            ck = (f"Flink: {done}/{tot} completed, duration avg {fmt(mean(rs, 'dur_avg_ms', 'flink_ckpt'), 0)} / "
                  f"max {fmt(peak(rs, 'dur_max_ms', 'flink_ckpt'), 0)} ms, upload avg "
                  f"{fmt(mean(rs, 'upload_avg_mb', 'flink_ckpt'))} MB, full state max "
                  f"{fmt(peak(rs, 'state_max_mb', 'flink_ckpt'))} MB")
        elif any("spark_batches" in r for r in rs):
            ck = (f"Spark: {sum((r.get('spark_batches') or {}).get('batches') or 0 for r in rs)} batches, state commit "
                  f"avg {fmt(mean(rs, 'state_commit_avg_ms', 'spark_batches'), 0)} / max "
                  f"{fmt(peak(rs, 'state_commit_max_ms', 'spark_batches'), 0)} ms, log commit avg "
                  f"{fmt(mean(rs, 'log_commit_avg_ms', 'spark_batches'), 0)} ms, state mem max "
                  f"{fmt(peak(rs, 'state_mem_max_mb', 'spark_batches'))} MB")
        else:
            ck = "not recorded"
        lines.append(f"| {arm} | {len(rs)} | {fmt(mean(rs, 'cpu_mean'))} | {fmt(peak(rs, 'cpu_peak'))} | "
                     f"{fmt(mean(rs, 'mem_mean_gib'))} | {fmt(peak(rs, 'mem_peak_gib'))} | {ck} |")
    open(os.path.join(root, "resources.md"), "w").write("\n".join(lines) + "\n")
    json.dump(runs, open(os.path.join(root, "resources.json"), "w"), indent=1)
    print(f"{root}: {len(runs)} runs, {len(arms)} arms -> resources.md")


if __name__ == "__main__":
    for root in sys.argv[1:] or ["results/eks-stateful"]:
        summarize(root)
