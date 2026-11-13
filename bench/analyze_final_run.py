#!/usr/bin/env python3
"""
Final lean run (FINAL_RUN_PLAN.md): one row per cell over the runs of both clusters
(results/final-run/{a,b}/runs/<label>-rN). Per cell: n, pipeline p50 / p95 / p99 (mean over runs and
min-max), throughput, and from eks/stateful/sampler.py:

  CPU        cadvisor.jsonl: engine containers (spark / flink namespace, operators and webhooks
             excluded), cores = delta cpu_seconds / delta t per container between consecutive 2 s
             samples, summed per sample time; mean and peak over the measured window
             (summary started_at + warmup .. ended_at)
  memory     same samples: summed working set and RSS (GiB), mean / peak
  throttle   CFS throttled periods / periods over the window, engine containers
  JVM heap   jvm.jsonl: Spark used KB from jcmd (driver + executors), Flink Heap.Used (TMs + JM);
             summed per sample, mean / peak (GiB)
  RocksDB    rocksdb.jsonl (Flink): block cache + memtables + table readers, summed over vertices,
             peak (MB); live data size peak (MB)
  checkpoint ckpt_s3.json: bytes and objects under the run's checkpoint prefix (objects = the PUT
             lower bound); Spark progress.jsonl last-batch rocksdbSstFileSize when reported
  recovery   kill runs: bench/recovery_check.py (outage, recover, lost / overcount)

Usage: python bench/analyze_final_run.py results/final-run  (writes cells.md and cells.json there)
       python bench/analyze_final_run.py results/final30 --runs 2 --title "Final run A (30 min)"
       (--runs: runs per cell by design; a cell with fewer is flagged **n=<k>**; default 3)
"""
import collections
import glob
import json
import os
import re
import statistics as st
import sys

sys.path.insert(0, os.path.dirname(__file__))
import recovery_check  # noqa: E402

OPER = re.compile(r"operator|webhook")


def jl(p):
    if not os.path.exists(p):
        return []
    out = []
    for line in open(p):
        try:
            out.append(json.loads(line))
        except ValueError:
            pass
    return out


def cadvisor_stats(d, t0, t1):
    """The kubelet refreshes cAdvisor container stats every ~10-15 s (housekeeping), so a 2 s scrape
    sees the same cumulative value several times and then one jump. CPU rate is therefore taken
    between CHANGES of each container's counter (delta cpu / time since its last change), and the
    resulting per-container rate is held until the next change; memory gauges are used as sampled."""
    rows = [r for r in jl(os.path.join(d, "cadvisor.jsonl")) if "c" in r]
    last, rate, per_t = {}, {}, collections.defaultdict(lambda: [0.0, 0.0, 0.0])
    thr = [0.0, 0.0]
    for r in sorted(rows, key=lambda r: r["t"]):
        t = r["t"]
        for k, v in r["c"].items():
            ns, pod, _ = k.split("/", 2)
            if ns not in ("spark", "flink") or OPER.search(pod) or v[0] is None:
                continue
            p = last.get(k)
            if p is None or v[0] < p[1][0]:          # first sight or counter reset (restart)
                last[k] = (t, v)
            elif v[0] != p[1][0]:
                rate[k] = (v[0] - p[1][0]) / max(t - p[0], 1e-6)
                if t0 <= t <= t1 and None not in (v[3], v[4], p[1][3], p[1][4]):
                    thr[0] += v[3] - p[1][3]
                    thr[1] += v[4] - p[1][4]
                last[k] = (t, v)
            if t0 <= t <= t1:
                b = per_t[round(t)]
                b[0] += rate.get(k, 0.0)
                b[1] += (v[1] or 0) / 2**30
                b[2] += (v[2] or 0) / 2**30
    if not per_t:
        return {}
    vals = list(per_t.values())
    cpu = [v[0] for v in vals]
    return {"cpu_mean": st.mean(cpu), "cpu_peak": max(cpu), "ws_mean": st.mean(v[1] for v in vals),
            "ws_peak": max(v[1] for v in vals), "rss_peak": max(v[2] for v in vals),
            "throttle_pct": 100 * thr[0] / thr[1] if thr[1] else None}


def jvm_stats(d, t0, t1):
    tot = []
    for r in jl(os.path.join(d, "jvm.jsonl")):
        if "jvm" not in r or not (t0 <= r["t"] <= t1):
            continue
        s = 0.0
        for v in r["jvm"].values():
            if isinstance(v, list):          # spark: [total_kb, used_kb]
                s += v[1] * 1024
            elif isinstance(v, dict):
                s += v.get("Status.JVM.Memory.Heap.Used", 0)
        tot.append(s / 2**30)
    return {"heap_mean": st.mean(tot), "heap_peak": max(tot)} if tot else {}


def rocks_stats(d, t0, t1):
    mem, live = [], []
    for r in jl(os.path.join(d, "rocksdb.jsonl")):
        if not r.get("rocksdb") or not (t0 <= r["t"] <= t1):
            continue
        m = sum(v["block-cache-usage"] + v["size-all-mem-tables"] + v["estimate-table-readers-mem"] for v in r["rocksdb"].values())
        mem.append(m / 1e6)
        live.append(sum(v["estimate-live-data-size"] for v in r["rocksdb"].values()) / 1e6)
    return {"rocks_mem_peak_mb": max(mem), "rocks_live_peak_mb": max(live)} if mem else {}


def spark_state_mem(d):
    ps = jl(os.path.join(d, "progress.jsonl"))
    mem = [sum((o.get("memoryUsedBytes") or 0) for o in p.get("stateOperators", [])) for p in ps]
    sst = [sum((o.get("rocksdbSstFileSize") or 0) for o in p.get("stateOperators", [])) for p in ps]
    out = {}
    if any(mem):
        out["spark_state_mem_peak_mb"] = max(mem) / 1e6
    if any(sst):
        out["spark_sst_last_mb"] = sst[-1] / 1e6
    return out


def backlog_slope(rate, t0, t1):
    """Backlog growth over the measured window, in seconds of lag gained per minute: least-squares
    slope of the per-second mean pipeline latency (rate.json: {epoch_s: [count, latency_sum_ms]}).
    ~0 for an engine that keeps up; for one that falls behind at a steady output rate it is
    60 * (1 - out/in), independent of the window length (unlike p99)."""
    pts = [(int(t), v[1] / v[0]) for t, v in rate.items() if v[0] > 0 and t0 <= int(t) <= t1]
    if len(pts) < 30:
        return None
    mx = st.mean(t for t, _ in pts)
    my = st.mean(y for _, y in pts)
    den = sum((t - mx) ** 2 for t, _ in pts)
    return sum((t - mx) * (y - my) for t, y in pts) / den * 60 / 1000 if den else None


def run_row(d):
    s = json.load(open(os.path.join(d, "summary.json")))
    if "started_at" in s:
        t0 = s["started_at"] + s.get("warmup_seconds", 60)
        t1 = s["ended_at"]
    else:
        # stateless p6 consumer (published v43 image) has no timestamps: the sampler starts with the
        # load Job, so measure from its first sample + warmup (+5 s pod start) for the window
        c0 = next((r["t"] for r in jl(os.path.join(d, "cadvisor.jsonl"))), 0)
        t0 = c0 + s.get("warmup_seconds", 60) + 5
        t1 = t0 + s.get("window_seconds", 300)
    r = {"run": os.path.basename(d), "p50": s["pipeline_ms"]["p50"], "p95": s["pipeline_ms"]["p95"],
         "p99": s["pipeline_ms"]["p99"], "eps": s["throughput_eps"]}
    r.update(cadvisor_stats(d, t0, t1))
    r.update(jvm_stats(d, t0, t1))
    r.update(rocks_stats(d, t0, t1))
    r.update(spark_state_mem(d))
    c = os.path.join(d, "ckpt_s3.json")
    if os.path.exists(c):
        cj = json.load(open(c))
        r["ckpt_mb"], r["s3_puts"] = cj["bytes"] / 1e6, cj["objects"]
    if os.path.exists(os.path.join(d, "rate.json")) and not os.path.exists(os.path.join(d, "kill.txt")):
        r["backlog_s_per_min"] = backlog_slope(json.load(open(os.path.join(d, "rate.json"))), t0, t1)
    if os.path.exists(os.path.join(d, "kill.txt")) and os.path.exists(os.path.join(d, "rate.json")):
        kill = float(open(os.path.join(d, "kill.txt")).read().split()[0])
        rate = json.load(open(os.path.join(d, "rate.json")))
        r.update(recovery_check.recovery_metrics(rate, kill))
        if os.path.exists(os.path.join(d, "truth.jsonl")) and os.path.exists(os.path.join(d, "maxcnt.json")):
            truths = [json.loads(l) for l in open(os.path.join(d, "truth.jsonl")) if l.strip()]
            r.update(recovery_check.truth_diff(truths, json.load(open(os.path.join(d, "maxcnt.json"))), s["started_at"]))
    return r


def agg(rows, k):
    v = [r[k] for r in rows if r.get(k) is not None]
    return (st.mean(v), min(v), max(v)) if v else None


def f(x, nd=0):
    if x is None:
        return "-"
    return f"{x:,.{nd}f}"


def rng(a, nd=0):
    if a is None:
        return "-"
    m, lo, hi = a
    return f"{f(m, nd)} ({f(lo, nd)}-{f(hi, nd)})"


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("--runs", type=int, default=3, help="runs per cell by design")
    ap.add_argument("--title", default="Final lean run")
    args = ap.parse_args()
    root = args.root
    cells = collections.OrderedDict()
    for d in sorted(glob.glob(os.path.join(root, "[a-d]", "runs", "*"))):
        if not os.path.exists(os.path.join(d, "summary.json")):
            continue
        cell = re.sub(r"-r\d+$", "", os.path.basename(d))
        cells.setdefault(cell, {"cluster": d.split(os.sep)[-3], "runs": []})["runs"].append(run_row(d))
    out, L = {}, []
    L += [f"# {args.title}: per cell", "",
          "Pipeline latency ms: mean over runs (min-max across runs). CPU cores / memory GiB: engine pods only "
          "(cAdvisor, 2 s, measured window), mean / peak. Heap GiB: JVM heap used, summed over engine JVMs, peak. "
          "RocksDB MB (Flink): block cache + memtables + table readers, peak; Spark: state-store memoryUsedBytes peak. "
          "Checkpoint: MB written under the run's S3 prefix and object count (= PUT lower bound), mean over runs. "
          "Backlog s/min: growth of pipeline latency over the window (least-squares slope of the per-second mean, "
          "rate.json); ~0 = keeps up, else 60 x (1 - out/in), independent of the window length.", "",
          "| cell | cl | n | p50 | p95 | p99 | evt/s out | backlog s/min | CPU mean / peak | WS mean / peak | heap peak | RocksDB / state MB | throttle % | ckpt MB | S3 PUTs |",
          "|---|---|--:|---|---|---|--:|--:|---|---|--:|--:|--:|--:|--:|"]
    for cell, c in cells.items():
        rs = c["runs"]
        a = {k: agg(rs, k) for k in ("p50", "p95", "p99", "eps", "backlog_s_per_min", "cpu_mean", "cpu_peak", "ws_mean", "ws_peak", "heap_peak",
                                     "rocks_mem_peak_mb", "spark_state_mem_peak_mb", "throttle_pct", "ckpt_mb", "s3_puts")}
        out[cell] = {"cluster": c["cluster"], "n": len(rs), "agg": a, "runs": rs}
        flag = f" **n={len(rs)}**" if len(rs) < args.runs and "at16k" not in cell else ""
        mem = a["rocks_mem_peak_mb"] or a["spark_state_mem_peak_mb"]
        g = lambda k, nd=1: f(a[k][0], nd) if a[k] else "-"
        L.append(f"| `{cell}`{flag} | {c['cluster']} | {len(rs)} | {rng(a['p50'], 1)} | {rng(a['p95'])} | {rng(a['p99'])} | "
                 f"{g('eps', 0)} | {g('backlog_s_per_min')} | {g('cpu_mean')} / {g('cpu_peak')} | {g('ws_mean')} / {g('ws_peak')} | {g('heap_peak')} | "
                 f"{f(mem[0], 0) if mem else '-'} | {g('throttle_pct')} | {g('ckpt_mb', 0)} | {g('s3_puts', 0)} |")
    kills = {k: v for k, v in out.items() if k.endswith("-kill")}
    if kills:
        L += ["", "Recovery (worker pod killed 180 s into the load):", "",
              "| cell | n | outage s | recover s | lost / over / settled windows |", "|---|--:|---|---|---|"]
        for k, v in kills.items():
            rs = v["runs"]
            L.append(f"| `{k}` | {len(rs)} | {', '.join(f(r.get('outage_s')) for r in rs)} | {', '.join(f(r.get('recover_s')) for r in rs)} | "
                     + "; ".join(f"{r.get('lost')}/{r.get('overcount')}/{r.get('settled_windows')}" for r in rs) + " |")
    json.dump(out, open(os.path.join(root, "cells.json"), "w"), indent=1, default=float)
    open(os.path.join(root, "cells.md"), "w").write("\n".join(L) + "\n")
    print("\n".join(L))


if __name__ == "__main__":
    main()
