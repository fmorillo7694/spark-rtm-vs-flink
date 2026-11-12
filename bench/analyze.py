#!/usr/bin/env python3
"""
Merge per-engine latency summaries + docker-stats CSVs into markdown comparison tables.

Reads results/<engine>_summary.json and results/<engine>_stats.csv for each engine,
computes steady-state mean/peak CPU (cores) and memory (MB), derives a simple cost
estimate, and prints markdown ready to paste into REPORT.md.

Cost model (clearly an approximation):
  - Map aggregate steady-state cores to vCPU and memory to GB.
  - Price against an on-demand m5.* rate (configurable) by max(vCPU/2, GB/8) instances
    -> $/hour. Then $/billion events using the measured output throughput.

Usage: python bench/analyze.py [--rate-per-vcpu-hr 0.048] [engines...]
"""
import argparse
import csv
import glob
import json
import os
import statistics as st

RESULTS = os.path.join(os.path.dirname(__file__), "..", "results")


def load_summaries(engines):
    out = {}
    for e in engines:
        p = os.path.join(RESULTS, f"{e}_summary.json")
        if os.path.exists(p):
            with open(p) as f:
                out[e] = json.load(f)
    return out


def load_stats(engine):
    """Return per-container {mean_cpu_cores, peak_cpu_cores, mean_mem_mb, peak_mem_mb}."""
    p = os.path.join(RESULTS, f"{engine}_stats.csv")
    if not os.path.exists(p):
        return {}
    rows = {}
    with open(p) as f:
        for r in csv.DictReader(f):
            c = r["container"]
            try:
                cpu = float(r["cpu_pct"]) / 100.0   # docker cpu% is per-core-summed; /100 = cores
                mem = float(r["mem_used_mb"])
            except ValueError:
                continue
            rows.setdefault(c, {"cpu": [], "mem": []})
            rows[c]["cpu"].append(cpu)
            rows[c]["mem"].append(mem)
    agg = {}
    for c, v in rows.items():
        if not v["cpu"]:
            continue
        agg[c] = {
            "mean_cpu_cores": round(st.mean(v["cpu"]), 2),
            "peak_cpu_cores": round(max(v["cpu"]), 2),
            "mean_mem_mb": round(st.mean(v["mem"]), 0),
            "peak_mem_mb": round(max(v["mem"]), 0),
        }
    return agg


def engine_totals(stats, exclude=("kafka",)):
    """Sum across the engine's own containers (exclude shared Kafka)."""
    cpu_mean = cpu_peak = mem_mean = mem_peak = 0.0
    for c, v in stats.items():
        if any(x in c for x in exclude):
            continue
        cpu_mean += v["mean_cpu_cores"]
        cpu_peak += v["peak_cpu_cores"]
        mem_mean += v["mean_mem_mb"]
        mem_peak += v["peak_mem_mb"]
    return {"cpu_mean": round(cpu_mean, 2), "cpu_peak": round(cpu_peak, 2),
            "mem_mean": round(mem_mean, 0), "mem_peak": round(mem_peak, 0)}


def fmt(v):
    return "—" if v is None else f"{v:.1f}" if isinstance(v, float) else str(v)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate-per-vcpu-hr", type=float, default=0.048,
                    help="on-demand $/vCPU-hour (m5 ~ $0.096/2vCPU = 0.048)")
    ap.add_argument("engines", nargs="*",
                    default=["spark-rtm", "spark-microbatch", "pyspark-rtm",
                             "flink", "pyflink"])
    args = ap.parse_args()

    summ = load_summaries(args.engines)
    if not summ:
        print("No summaries found in results/. Run bench/run_*.sh first.")
        return

    print("## Latency (ms)\n")
    print("| engine | pipeline p50 | p95 | p99 | max | e2e p50 | p95 | p99 | max | throughput (evt/s) |")
    print("|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
    for e in args.engines:
        if e not in summ:
            continue
        s = summ[e]
        pp, ee = s["pipeline_ms"], s["e2e_ms"]
        print(f"| {e} | {fmt(pp['p50'])} | {fmt(pp['p95'])} | {fmt(pp['p99'])} | {fmt(pp['max'])} "
              f"| {fmt(ee['p50'])} | {fmt(ee['p95'])} | {fmt(ee['p99'])} | {fmt(ee['max'])} "
              f"| {fmt(s['throughput_eps'])} |")

    print("\n## Resource usage (steady state; engine containers only, Kafka excluded)\n")
    print("| engine | mean CPU (cores) | peak CPU | mean mem (MB) | peak mem |")
    print("|---|--:|--:|--:|--:|")
    totals = {}
    for e in args.engines:
        if e not in summ:
            continue
        t = engine_totals(load_stats(e))
        totals[e] = t
        print(f"| {e} | {t['cpu_mean']} | {t['cpu_peak']} | {t['mem_mean']:.0f} | {t['mem_peak']:.0f} |")

    print("\n## Cost estimate (approximation)\n")
    print(f"On-demand rate assumed: ${args.rate_per_vcpu_hr:.3f}/vCPU-hour. "
          "vCPU = mean cores; instances sized by max(vCPU, mem_GB/4 surrogate).\n")
    print("| engine | vCPU | mem (GB) | $/hour | $/billion events |")
    print("|---|--:|--:|--:|--:|")
    for e in args.engines:
        if e not in summ or e not in totals:
            continue
        t = totals[e]
        vcpu = max(0.1, t["cpu_mean"])
        gb = t["mem_mean"] / 1024.0
        usd_hr = vcpu * args.rate_per_vcpu_hr
        eps = summ[e]["throughput_eps"] or 1
        usd_per_billion = usd_hr / (eps * 3600) * 1e9
        print(f"| {e} | {vcpu:.2f} | {gb:.2f} | {usd_hr:.4f} | {usd_per_billion:.2f} |")

    print("\n_Cost is steady-state engine compute only; excludes Kafka/MSK, storage, "
          "networking, and idle headroom. Intended for relative comparison, not billing._")


if __name__ == "__main__":
    main()
