#!/usr/bin/env python3
"""
Aggregate the EKS stateful matrix pulled from S3 into results/eks-stateful/runs/<label>/
(written by eks/stateful/run_matrix_incluster.sh): summary.json, hist.json, evidence.txt,
verify.json, top.txt, checkpoints.json (Flink).

Per run: latency (pipe/e2e/window), boundary split (event second within -1..+3 s of a Spark
batch start = driver "Starting job" line after the first, or a Flink checkpoint trigger),
CPU/mem from kubectl top, sync-commit evidence, and the whole-run correctness invariant:
  S2 (dedup)            output records == unique events produced (sum of producer originals);
                        EO arms: output end offsets include transaction markers, so
                        0 <= output - originals <= 6 sink subtasks * 6 partitions * (checkpoints + 2)
  S1 update mode        output records == input records (one update per input row, no late row)
  S1 final-only, S3     not checkable from counts; reported only (local replay covers them)
Per arm: geomean over runs (n shown). Per workload: the default-vs-tuned flush 2x2.

Usage: python bench/analyze_eks_stateful.py [--dir results/eks-stateful] > tables.md
"""
import argparse
import calendar
import glob
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from analyze_eks43 import geomean, hist_pct, resources  # noqa: E402

ARMS = [  # engine prefix, display, flush setting, update-mode S1?
    ("spark-rtm-sw1", "Spark RTM, shuffle flush 1 ms", "tuned"),
    ("spark-rtm", "Spark RTM, shuffle flush 50 ms (default)", "default"),
    ("spark-mb", "Spark micro-batch", "default"),
    ("flink-ds-bt100", "Flink DataStream, buffer timeout 100 ms (default)", "default"),
    ("flink-ds-eo", "Flink DataStream, EXACTLY_ONCE sink", "tuned"),
    ("flink-ds", "Flink DataStream, buffer timeout 0 ms", "tuned"),
    ("flink-sql-bt100", "Flink SQL, buffer timeout 100 ms (default)", "default"),
    ("flink-sql-eo", "Flink SQL, EXACTLY_ONCE sink", "tuned"),
    ("flink-sql", "Flink SQL, buffer timeout 0 ms", "tuned"),
]
WORKLOADS = ["s1-update", "s1-window", "s1", "s2", "s3"]
PARTITIONS = 6
SINK_SUBTASKS = 6   # Flink sink parallelism; each subtask's transaction spans all partitions


def load(p):
    with open(p) as f:
        return json.load(f)


def split_label(lab):
    """<arm>-<workload>[-at<N>k]-r<run> -> (arm, workload, run, rate label or None)."""
    m = re.fullmatch(r"(.+?)(?:-at(\d+)k)?-r(\d+)", lab)
    base, rate, run = m.group(1), m.group(2), int(m.group(3))
    for pre, _, _ in ARMS:   # longest prefixes first in ARMS order
        if base.startswith(pre + "-") and base[len(pre) + 1:] in WORKLOADS:
            return pre, base[len(pre) + 1:], run, (f"{rate}k" if rate else None)
    return None, None, run, None


def spark_bounds(ev_text):
    starts = [calendar.timegm(time.strptime(m, "%y/%m/%d %H:%M:%S"))
              for m in re.findall(r"(\d\d/\d\d/\d\d \d\d:\d\d:\d\d) INFO SparkContext: Starting job:", ev_text)]
    return starts[1:]


def flink_bounds(path):
    if not os.path.exists(path):
        return []
    return [h["trigger_timestamp"] / 1000 for h in load(path).get("history", []) if h.get("status") == "COMPLETED"]


def boundary_split(hist_path, bounds, lo=-1, hi=3):
    if not os.path.exists(hist_path) or not bounds or os.path.getsize(hist_path) == 0:
        return None
    near, far = {}, {}
    for sec, buckets in load(hist_path).items():
        dst = near if any(b + lo <= int(sec) <= b + hi for b in bounds) else far
        for k, v in buckets.items():
            dst[int(k)] = dst.get(int(k), 0) + v
    n, f = sum(near.values()), sum(far.values())
    return {"boundaries": len(bounds), "near_share_pct": round(100 * n / max(1, n + f), 2),
            "near_p99": hist_pct(near, .99), "near_max": float(max(near)) if near else None,
            "far_p99": hist_pct(far, .99), "far_max": float(max(far)) if far else None}


def invariant(arm, wl, v, ckpts):
    if not v or "output_records" not in v:
        return None, "no verify.json"
    out, orig, inp = v["output_records"], v["originals"], v["input_records"]
    if wl == "s2":
        if arm.endswith("-eo"):
            # one commit marker per (sink subtask x partition it wrote) per committed
            # checkpoint: 6 x 6 x 7 = 252 observed; +2 checkpoints for start/abort markers
            slack = SINK_SUBTASKS * PARTITIONS * (ckpts + 2)
            ok = 0 <= out - orig <= slack
            return ok, f"out {out} - unique {orig} = {out - orig} (markers <= {slack})"
        return out == orig, f"out {out} vs unique {orig} (diff {out - orig})"
    if wl in ("s1-update",) or (wl == "s1" and arm.startswith("spark-rtm")):
        return out == inp, f"updates {out} vs input {inp} (diff {out - inp})"
    return None, f"out {out}, input {inp} (count not checkable)"


def evidence(path):
    if not os.path.exists(path):
        return "", {}
    t = open(path).read()
    g = lambda pat: int(m.group(1)) if (m := re.search(pat, t)) else None
    return t, {"async_log_write": g(r"async-log-write threads: (\d+)"),
               "async_exec": g(r"AsyncProgressTrackingMicroBatchExecution frames: (\d+)"),
               "pipelined": bool(re.search(r"concurrently with its running pipelined", t)),
               "ckpt_counts": (m.group(0) if (m := re.search(r"\{'restored'[^}]*\}", t)) else None)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=os.path.join(os.path.dirname(__file__), "..", "results", "eks-stateful"))
    ap.add_argument("--rate", default="", help="'' = main 50k matrix; e.g. 100k = capacity-probe runs")
    a = ap.parse_args()
    cells = {}
    for d in sorted(glob.glob(os.path.join(a.dir, "runs", "*"))):
        lab = os.path.basename(d)
        arm, wl, run, rate = split_label(lab)
        if not arm or (rate or "") != a.rate or not os.path.exists(os.path.join(d, "summary.json")):
            continue
        s = load(os.path.join(d, "summary.json"))
        txt, ev = evidence(os.path.join(d, "evidence.txt"))
        fb = flink_bounds(os.path.join(d, "checkpoints.json"))
        bounds = spark_bounds(txt) if arm.startswith("spark-rtm") else (fb if arm.startswith("flink") else [])
        v = load(os.path.join(d, "verify.json")) if os.path.exists(os.path.join(d, "verify.json")) else None
        ok, why = invariant(arm, wl, v, len(fb))
        w = s.get("window_ms") or {}
        cells.setdefault((arm, wl), []).append({
            "run": run, "kept": s["kept_records"], "eps": s["throughput_eps"],
            "pipe": s["pipeline_ms"], "e2e": s["e2e_ms"], "win": w,
            "res": resources(os.path.join(d, "top.txt")), "ev": ev,
            "bnd": boundary_split(os.path.join(d, "hist.json"), bounds), "inv": ok, "inv_why": why})
    out_name = "matrix_summary.json" if not a.rate else f"matrix_summary_{a.rate}.json"
    json.dump({f"{k[0]}:{k[1]}": v for k, v in cells.items()}, open(os.path.join(a.dir, out_name), "w"), indent=2)

    f = lambda x: "n/a" if x is None else (f"{x:.1f}" if abs(x) < 10000 else f"{x:.0f}")
    gm = lambda rs, get: geomean([get(r) for r in rs])
    name = {p: n for p, n, _ in ARMS}

    def agg(rs):
        res = [r["res"] for r in rs if r["res"]]
        bs = [r["bnd"] for r in rs if r["bnd"]]
        return {"n": len(rs), "p50": gm(rs, lambda r: r["pipe"]["p50"]), "p99": gm(rs, lambda r: r["pipe"]["p99"]),
                "max": gm(rs, lambda r: r["pipe"]["max"]), "e50": gm(rs, lambda r: r["e2e"]["p50"]),
                "e99": gm(rs, lambda r: r["e2e"]["p99"]),
                "w50": (sum(r["win"]["p50"] for r in rs if r["win"]) / len([r for r in rs if r["win"]])) if any(r["win"] for r in rs) else None,
                "w99": (sum(r["win"]["p99"] for r in rs if r["win"]) / len([r for r in rs if r["win"]])) if any(r["win"] for r in rs) else None,
                "near": geomean([b["near_p99"] for b in bs]), "far": geomean([b["far_p99"] for b in bs]),
                "cpu": (sum(x["cpu_mean"] for x in res) / len(res)) if res else None,
                "mem": (sum(x["mem_mean_gb"] for x in res) / len(res)) if res else None,
                "inv": ("PASS" if all(r["inv"] for r in rs) else "FAIL") if all(r["inv"] is not None for r in rs) else "n/a"}

    order = [(p, w) for w in WORKLOADS for p, _, _ in ARMS if (p, w) in cells]
    print("### Per arm (geomean over runs; window = arithmetic mean, it can be negative)\n")
    print("| workload | arm | runs | pipe p50 | pipe p99 | pipe max | e2e p50 | e2e p99 | window p50 | window p99 | p99 near boundary | p99 elsewhere | CPU cores | mem GB | count invariant |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for k in order:
        g = agg(cells[k])
        print(f"| {k[1]} | {name[k[0]]} | {g['n']} | {f(g['p50'])} | {f(g['p99'])} | {f(g['max'])} | {f(g['e50'])} | "
              f"{f(g['e99'])} | {f(g['w50'])} | {f(g['w99'])} | {f(g['near'])} | {f(g['far'])} | {f(g['cpu'])} | {f(g['mem'])} | {g['inv']} |")

    print("\n### Default vs tuned flush, per workload (pipe p50 / p99 ms, geomean, n = runs)\n")
    pairs = {"Spark RTM": ("spark-rtm", "spark-rtm-sw1"), "Flink DataStream": ("flink-ds-bt100", "flink-ds"),
             "Flink SQL": ("flink-sql-bt100", "flink-sql")}
    for wl_s, wl_f in (("s1", "s1-update"), ("s2", "s2"), ("s3", "s3")):
        print(f"\n**{wl_s.upper()}** (Flink DataStream arm: `{wl_f}`; Flink SQL S1 = TUMBLE, final-only)\n")
        print("| engine | default flush | tuned flush |")
        print("|---|---|---|")
        for eng, (dflt, tuned) in pairs.items():
            wl = wl_s if eng != "Flink DataStream" else wl_f
            row = []
            for arm in (dflt, tuned):
                rs = cells.get((arm, wl))
                row.append(f"{f(agg(rs)['p50'])} / {f(agg(rs)['p99'])} (n={len(rs)})" if rs else "not run")
            if row != ["not run", "not run"]:
                print(f"| {eng} | {row[0]} | {row[1]} |")

    print("\n### Per run\n")
    print("| arm | workload | run | records | eps | pipe p50 / p99 / max | e2e p50 / p99 | window p50 / p99 | near p99 / max | elsewhere p99 / max | CPU mean / peak | mem GB mean / peak | evidence | invariant |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for k in order:
        for r in sorted(cells[k], key=lambda r: r["run"]):
            b, x, ev = r["bnd"], r["res"], r["ev"]
            evs = (f"async-log-write={ev['async_log_write']}, pipelined={'yes' if ev['pipelined'] else 'no'}"
                   if ev.get("async_log_write") is not None else (ev.get("ckpt_counts") or ""))
            print(f"| {k[0]} | {k[1]} | r{r['run']} | {r['kept']} | {r['eps']:.0f} | {f(r['pipe']['p50'])} / {f(r['pipe']['p99'])} / {f(r['pipe']['max'])} | "
                  f"{f(r['e2e']['p50'])} / {f(r['e2e']['p99'])} | {f(r['win'].get('p50'))} / {f(r['win'].get('p99'))} | "
                  f"{(f(b['near_p99']) + ' / ' + f(b['near_max'])) if b else 'n/a'} | {(f(b['far_p99']) + ' / ' + f(b['far_max'])) if b else 'n/a'} | "
                  f"{(f(x['cpu_mean']) + ' / ' + f(x['cpu_peak'])) if x else 'n/a'} | {(f(x['mem_mean_gb']) + ' / ' + f(x['mem_peak_gb'])) if x else 'n/a'} | "
                  f"{evs} | {'PASS' if r['inv'] else ('FAIL' if r['inv'] is False else 'n/a')}: {r['inv_why']} |")


if __name__ == "__main__":
    main()
