#!/usr/bin/env python3
"""
Analyze the 4-hour soak: read per-30-min window snapshots for both engines + the resource
time-series, and report STABILITY — does latency/throughput drift, does memory creep, does the
tail degrade over 4 hours? This is the question short runs can't answer.

Usage: analyze_soak.py [results/eks/soak]
"""
import collections
import json
import os
import statistics
import sys

D = sys.argv[1] if len(sys.argv) > 1 else "results/eks/soak"


def load(engine):
    p = os.path.join(D, f"{engine}_soak.jsonl")
    rows = []
    if os.path.exists(p):
        for line in open(p):
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return rows


def drift(vals):
    """First vs last and min..max spread, to quantify degradation."""
    vals = [v for v in vals if v is not None]
    if len(vals) < 2:
        return None
    return {"first": vals[0], "last": vals[-1],
            "min": min(vals), "max": max(vals),
            "delta_pct": round((vals[-1] - vals[0]) / vals[0] * 100, 1) if vals[0] else None}


def report(engine):
    rows = load(engine)
    if not rows:
        print(f"\n### {engine}: no soak data")
        return
    print(f"\n### {engine.upper()} — {len(rows)} windows (~{len(rows)*30} min)")
    print(f"  {'win':>3} {'eps':>7} {'pipe_p50':>9} {'pipe_p99':>9} {'pipe_max':>9} {'e2e_p99':>9}")
    for r in rows:
        pm, em = r["pipeline_ms"], r["e2e_ms"]
        print(f"  {r['window_index']:>3} {r['throughput_eps']:>7.0f} "
              f"{str(pm['p50']):>9} {str(pm['p99']):>9} {str(pm['max']):>9} {str(em['p99']):>9}")
    # Drift summary
    eps = drift([r["throughput_eps"] for r in rows])
    p99 = drift([r["pipeline_ms"]["p99"] for r in rows])
    mx = drift([r["pipeline_ms"]["max"] for r in rows])
    print(f"  DRIFT  throughput: {eps['first']}→{eps['last']} eps ({eps['delta_pct']:+}%)")
    print(f"         pipe p99:   {p99['first']}→{p99['last']} ms ({p99['delta_pct']:+}%), "
          f"range {p99['min']}–{p99['max']}")
    print(f"         pipe max:   {mx['first']}→{mx['last']} ms (range {mx['min']}–{mx['max']})")


def resources():
    p = os.path.join(D, "resources.tsv")
    if not os.path.exists(p):
        print("\n### resources: no data")
        return
    # ts ns pod cpu_m mem_mi  →  per (ts,ns) summed cores+GB, then drift over snapshots
    per = collections.defaultdict(lambda: [0, 0])
    order = []
    for line in open(p):
        f = line.rstrip("\n").split("\t")
        if len(f) < 5:
            continue
        ts, ns, pod, cpu, mem = f[0], f[1], f[2], f[3], f[4]
        try:
            c = int(cpu.rstrip("m")); m = int(mem.rstrip("Mi"))
        except ValueError:
            continue
        key = (ts, ns)
        if key not in per:
            order.append(key)
        per[key][0] += c; per[key][1] += m
    print("\n### RESOURCES over time (summed engine pods per snapshot)")
    print(f"  {'snap_ts':>12} {'engine':>6} {'cores':>6} {'mem_GB':>7}")
    by_ns = collections.defaultdict(list)
    for (ts, ns) in order:
        c = per[(ts, ns)][0] / 1000.0
        g = per[(ts, ns)][1] / 1024.0
        by_ns[ns].append((c, g))
        print(f"  {ts:>12} {ns:>6} {c:>6.1f} {g:>7.1f}")
    for ns, vals in by_ns.items():
        cores = [v[0] for v in vals]; mems = [v[1] for v in vals]
        print(f"  {ns} MEM drift: {mems[0]:.1f}→{mems[-1]:.1f} GB (range {min(mems):.1f}–{max(mems):.1f}) "
              f"| CPU mean {statistics.mean(cores):.1f}c")


if __name__ == "__main__":
    print("=" * 70)
    print("4-HOUR SOAK — Spark RTM vs Flink stability")
    print("=" * 70)
    report("spark")
    report("flink")
    resources()
