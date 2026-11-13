#!/usr/bin/env python3
"""Tabulate the local stateful SMOKE runs (results/local-stateful/smoke/*-SMOKE_summary.json
+ *_stats.csv + *_evidence.txt) into a markdown table on stdout. Smoke numbers come from a
laptop at ~3k evt/s for 90 s: they show the pipelines work and roughly how each emits, they
are NOT benchmark results."""
import csv
import glob
import json
import os
import re
import sys

D = sys.argv[1] if len(sys.argv) > 1 else "results/local-stateful/smoke"


def stats(path, engine_family):
    """mean CPU% and mem MB of the engine containers (Kafka excluded), summed per sample."""
    per_ts = {}
    try:
        with open(path) as f:
            for r in csv.DictReader(f):
                if r["container"] == "kafka":
                    continue
                a = per_ts.setdefault(r["epoch"], [0.0, 0.0])
                a[0] += float(r["cpu_pct"] or 0)
                a[1] += float(r["mem_used_mb"] or 0)
    except FileNotFoundError:
        return None, None
    if not per_ts:
        return None, None
    vals = list(per_ts.values())
    return sum(v[0] for v in vals) / len(vals) / 100.0, sum(v[1] for v in vals) / len(vals) / 1024.0


def evidence(path):
    try:
        t = open(path).read()
    except FileNotFoundError:
        return ""
    out = []
    m = re.search(r"async-log-write threads: (\d+)", t)
    if m:
        out.append(f"async-log-write={m.group(1)}")
    m = re.search(r"concurrently with its running pipelined producer", t)
    if "spark" in os.path.basename(path):
        out.append("pipelined=" + ("yes" if m else "not seen"))
        m = re.search(r"\"stateUniqueIds\"", t)
        out.append("sync CommitLog(stateUniqueIds)=" + ("yes" if m else "no"))
    m = re.search(r"\{'restored': \d+, 'total': (\d+), 'in_progress': \d+, 'completed': (\d+), 'failed': (\d+)\}", t)
    if m:
        out.append(f"ckpt total/completed/failed={m.group(1)}/{m.group(2)}/{m.group(3)}")
    m = re.search(r'"state_backend":"(\w+)"', t)
    if m:
        out.append(m.group(1))
    return ", ".join(out)


def f(x):
    return "" if x is None else (f"{x:.1f}" if abs(x) < 10000 else f"{x:.0f}")


rows = []
for p in sorted(glob.glob(os.path.join(D, "*-SMOKE_summary.json"))):
    s = json.load(open(p))
    label = os.path.basename(p).replace("-SMOKE_summary.json", "")
    cpu, mem = stats(os.path.join(D, f"{label}_stats.csv"), label.split("-")[0])
    pm, em, wm = s["pipeline_ms"], s["e2e_ms"], s.get("window_ms") or {}
    rows.append(f"| {label} | {s['kept_records']} | {f(pm['p50'])} | {f(pm['p95'])} | {f(pm['p99'])} | "
                f"{f(pm['max'])} | {f(em['p50'])} | {f(em['p99'])} | {f(wm.get('p50'))} | {f(wm.get('p99'))} | "
                f"{f(cpu)} | {f(mem)} | {evidence(os.path.join(D, f'{label}_evidence.txt'))} |")

print("| arm (SMOKE ONLY) | out records | pipe p50 | p95 | p99 | max | e2e p50 | e2e p99 | "
      "window p50 | window p99 | CPU cores | mem GB | evidence |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
print("\n".join(rows))
