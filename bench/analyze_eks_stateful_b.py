#!/usr/bin/env python3
"""
Session B summary: one row per arm (run label without -rN), median over runs of pipeline
p50/p99/max, kept records, the whole-run correctness invariant and peak engine CPU/memory
(kubectl top, sum over engine pods per sample).

  S2 / S1 update mode / stateless: output records == unique originals (S2) or input (S1)
  S3 and final-only windows: not checkable from counts (local replay covers them)
  kill runs: dup_records = output - input (at-least-once re-emits, not an error)

Usage: python bench/analyze_eks_stateful_b.py [--dir results/eks-stateful-b] > tables.md
"""
import argparse
import collections
import glob
import json
import os
import re
import statistics as st


def load(p):
    try:
        return json.load(open(p))
    except Exception:
        return None


def peak(top):
    """max over samples of summed CPU (cores) and memory (GiB) of engine pods."""
    by_ts = collections.defaultdict(lambda: [0.0, 0.0])
    try:
        for line in open(top):
            f = line.split()
            if len(f) < 5 or "operator" in f[2]:
                continue
            cpu = float(f[3].rstrip("m")) / 1000 if f[3].endswith("m") else float(f[3])
            m = f[4]
            mem = float(m[:-2]) / 1024 if m.endswith("Mi") else float(m[:-2]) if m.endswith("Gi") else 0.0
            by_ts[f[0]][0] += cpu
            by_ts[f[0]][1] += mem
    except OSError:
        return None, None
    if not by_ts:
        return None, None
    return max(v[0] for v in by_ts.values()), max(v[1] for v in by_ts.values())


def correctness(label, v):
    if not v:
        return "-"
    wl = v.get("workload", "")
    out, orig, inp = v.get("output_records"), v.get("originals"), v.get("input_records")
    if "-kill" in label:
        return f"re-emits {out - inp:+,}"
    if label.startswith("spark-mb"):   # micro-batch update mode: one update per key per batch
        return "n/a (fewer updates by design)"
    if wl.startswith("s2") and "-eo-" in label:   # EO sink: end offsets include commit markers
        d = out - orig
        return "exact + markers" if 0 <= d <= 6 * 6 * 9 else f"{d:+,}"
    if wl.startswith("s2"):
        d = out - orig
        return "exact" if d == 0 else f"{d:+,}"
    if wl == "s1" or wl == "s1-update":
        d = out - inp
        return "exact" if d == 0 else f"{d:+,}"
    return "n/a (replay)"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="results/eks-stateful-b")
    a = ap.parse_args()
    rows = collections.OrderedDict()
    for d in sorted(glob.glob(os.path.join(a.dir, "runs", "*"))):
        label = os.path.basename(d)
        arm = re.sub(r"-r\d+$", "", label)
        s = load(os.path.join(d, "summary.json"))
        v = load(os.path.join(d, "verify.json"))
        if not s:
            continue
        cpu, mem = peak(os.path.join(d, "top.txt"))
        p = s["pipeline_ms"]
        rows.setdefault(arm, []).append((p["p50"], p["p99"], p["max"], s["kept_records"],
                                         correctness(label, v), cpu, mem))
    print("| arm | n | p50 ms | p99 ms | max ms | kept (median) | correctness per run | peak CPU | peak mem GiB |")
    print("|---|---:|---:|---:|---:|---:|---|---:|---:|")
    for arm, rs in rows.items():
        med = lambda i: st.median(r[i] for r in rs)  # noqa: E731
        cpus = [r[5] for r in rs if r[5] is not None]
        mems = [r[6] for r in rs if r[6] is not None]
        print(f"| {arm} | {len(rs)} | {med(0):,.1f} | {med(1):,.1f} | {med(2):,.1f} | {int(med(3)):,} | "
              f"{', '.join(r[4] for r in rs)} | {max(cpus):.1f} | {max(mems):.1f} |" if cpus else
              f"| {arm} | {len(rs)} | {med(0):,.1f} | {med(1):,.1f} | {med(2):,.1f} | {int(med(3)):,} | "
              f"{', '.join(r[4] for r in rs)} | - | - |")


if __name__ == "__main__":
    main()
