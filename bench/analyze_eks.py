#!/usr/bin/env python3
"""
Analyze EKS run artifacts into markdown: latency (from *_consumer.log), peak/mean pod
CPU & memory (from *_top.txt), and max Kafka consumer-group lag (from *_lag.txt).

Usage: python bench/analyze_eks.py [label ...]
Each label expects results/eks/<label>_{consumer.log,top.txt,lag.txt}.
"""
import glob
import json
import os
import re
import statistics as st
import sys

EKS = os.path.join(os.path.dirname(__file__), "..", "results", "eks")


def latency(label):
    # Prefer the clean per-engine summary.json (never overwritten); fall back to the log.
    sj = os.path.join(EKS, f"{label}_summary.json")
    if os.path.exists(sj):
        return json.load(open(sj))
    p = os.path.join(EKS, f"{label}_consumer.log")
    if not os.path.exists(p):
        return None
    m = re.search(r'\{.*\}', open(p).read(), re.S)
    return json.loads(m.group(0)) if m else None


def cpu_mem(label):
    """Parse `kubectl top pods` lines: '<ts> <ns> <pod> <cpu>m <mem>Mi'. Sum engine pods
    per timestamp, then report mean/peak across timestamps."""
    p = os.path.join(EKS, f"{label}_top.txt")
    if not os.path.exists(p):
        return None
    per_ts = {}
    for line in open(p):
        f = line.split()
        if len(f) < 5:
            continue
        ts, ns, pod, cpu, mem = f[0], f[1], f[2], f[3], f[4]
        if ns not in ("spark", "flink"):   # engine namespaces only
            continue
        if not any(k in pod for k in ("rtm-bench", "flink-bench")):
            continue
        try:
            mc = int(cpu.rstrip("m"))
            mi = int(re.sub(r"[^0-9]", "", mem))
        except ValueError:
            continue
        per_ts.setdefault(ts, [0, 0])
        per_ts[ts][0] += mc
        per_ts[ts][1] += mi
    if not per_ts:
        return None
    cpus = [v[0] / 1000.0 for v in per_ts.values()]   # cores
    mems = [v[1] for v in per_ts.values()]             # MiB
    return {"mean_cpu_cores": round(st.mean(cpus), 2), "peak_cpu_cores": round(max(cpus), 2),
            "mean_mem_mb": round(st.mean(mems)), "peak_mem_mb": round(max(mems))}


def backlog(label, filter_pass=0.333):
    """Source-agnostic backlog from topic end-offsets: rows are 'ts in_end out_end'.
    Engine keeps up if output grows at input_rate*filter_pass. Backlog at each sample =
    (input consumed since start) - (output produced since start)/filter_pass, in input
    records. Returns (max_backlog, final_backlog, in_rate, out_rate)."""
    p = os.path.join(EKS, f"{label}_lag.txt")
    if not os.path.exists(p):
        return None
    rows = []
    for line in open(p):
        f = line.split()
        if len(f) != 3:
            continue
        try:
            rows.append((int(f[0]), int(f[1]), int(f[2])))
        except ValueError:
            continue
    if len(rows) < 2:
        return None
    t0, in0, out0 = rows[0]
    bl = []
    for ts, ine, oute in rows:
        din = ine - in0
        dout = oute - out0
        bl.append(din - (dout / filter_pass))
    span = rows[-1][0] - t0 or 1
    in_rate = (rows[-1][1] - in0) / span
    out_rate = (rows[-1][2] - out0) / span
    return {"max_backlog": round(max(bl)), "final_backlog": round(bl[-1]),
            "in_rate": round(in_rate), "out_rate": round(out_rate)}


def main():
    labels = sys.argv[1:] or [
        os.path.basename(x)[:-13] for x in glob.glob(os.path.join(EKS, "*_consumer.log"))]
    labels = sorted(set(labels))

    print("## EKS latency (ms)\n")
    print("| engine | out evt/s | records | pipe p50 | p95 | p99 | max | e2e p50 | p95 | p99 | max |")
    print("|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
    for L in labels:
        s = latency(L)
        if not s:
            continue
        pp, ee = s["pipeline_ms"], s["e2e_ms"]
        print(f"| {L} | {s['throughput_eps']:.0f} | {s['kept_records']} "
              f"| {pp['p50']} | {pp['p95']} | {pp['p99']} | {pp['max']} "
              f"| {ee['p50']} | {ee['p95']} | {ee['p99']} | {ee['max']} |")

    print("\n## EKS resources + backlog (keep-up)\n")
    print("| engine | mean CPU (cores) | peak CPU | mean mem (MB) | peak mem | in evt/s | out evt/s | max backlog | final backlog |")
    print("|---|--:|--:|--:|--:|--:|--:|--:|--:|")
    for L in labels:
        cm = cpu_mem(L) or {}
        bl = backlog(L) or {}
        print(f"| {L} | {cm.get('mean_cpu_cores','—')} | {cm.get('peak_cpu_cores','—')} "
              f"| {cm.get('mean_mem_mb','—')} | {cm.get('peak_mem_mb','—')} "
              f"| {bl.get('in_rate','—')} | {bl.get('out_rate','—')} "
              f"| {bl.get('max_backlog','—')} | {bl.get('final_backlog','—')} |")


if __name__ == "__main__":
    main()
