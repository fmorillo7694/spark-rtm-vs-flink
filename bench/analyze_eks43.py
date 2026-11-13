#!/usr/bin/env python3
"""
Aggregate the EKS p6 Spark 4.3.0 / Flink 2.3.0 matrix (results/eks-4.3/).

Per run (label = <engine>-p6[-ckpt<N>s]-r<N>), written by eks/04b_run_p6.sh:
  _summary.json     latency_consumer summary (pipeline + e2e percentiles)
  _hist.json        {event-second: {pipeline-latency bucket ms: count}} (steady state)
  _top.txt          kubectl top samples every 3 s (ts ns pod cpu mem)
  _evidence.txt     Spark: driver jstack async markers + S3 offsets/commits listing;
                    Flink: checkpoint counts
  _checkpoints.json Flink REST checkpoint stats (trigger timestamps = boundaries)

Boundary split: a record is "near" a commit boundary when its event second is within
-1..+3 s of a Spark batch start (offsets/<N> LastModified) or a Flink checkpoint trigger.
Percentiles from histograms are bucket lower bounds (1 ms <100 ms, 10 ms <2 s, 100 ms above).

Previous (Spark 4.1.2 / Flink 2.2.0) EKS p6 numbers are read from --prev (results/eks of the
original checkout or a copy).

Usage: python bench/analyze_eks43.py [--dir results/eks-4.3] [--prev <dir with *-p6_summary.json>]
"""
import argparse
import datetime as dt
import glob
import json
import math
import os
import re
import statistics as st

ROOT = os.path.join(os.path.dirname(__file__), "..")
ENGINES = [  # label prefix, display name, previous summary stem
    ("spark-rtm-java-p6", "Spark RTM Java", "spark-rtm-java-p6"),
    ("spark-rtm-scala-p6", "Spark RTM Scala", "spark-rtm-scala-p6"),
    ("pyspark-rtm-p6", "PySpark RTM (native trigger)", "pyspark-rtm-p6"),
    ("spark-rtm-syncckpt-p6", "Spark RTM Java, async tracking OFF", None),
    ("spark-microbatch-p6", "Spark micro-batch (Java)", None),
    ("flink-p6", "Flink DataStream (Java)", "flink-p6"),
    ("flink-sql-p6", "Flink SQL (Java)", "flink-sql-p6"),
    ("pyflink-p6", "PyFlink", "pyflink-p6"),
    ("spark-rtm-java-p6-ckpt5s", "Sweep: Spark RTM Java, 5 s, async ON", None),
    ("spark-rtm-syncckpt-p6-ckpt5s", "Sweep: Spark RTM Java, 5 s, async OFF", None),
    ("flink-p6-ckpt5s", "Sweep: Flink DataStream, 5 s", None),
]
METRICS = [("pipeline_ms", "p50"), ("pipeline_ms", "p99"), ("pipeline_ms", "max"),
           ("e2e_ms", "p50"), ("e2e_ms", "p99"), ("e2e_ms", "max")]


def geomean(xs):
    xs = [x for x in xs if x is not None and x > 0]
    return math.exp(sum(map(math.log, xs)) / len(xs)) if xs else None


def load(p):
    with open(p) as f:
        return json.load(f)


def resources(path):
    per = {}
    if not os.path.exists(path):
        return None
    for line in open(path):
        f = line.split()
        if len(f) < 5 or "operator" in f[2]:
            continue
        try:
            mc = int(f[3].rstrip("m")); mi = int(re.sub(r"[^0-9]", "", f[4]))
        except ValueError:
            continue
        per.setdefault(f[0], [0, 0, 0]); per[f[0]][0] += mc; per[f[0]][1] += mi; per[f[0]][2] += 1
    if not per:
        return None
    full = max(v[2] for v in per.values())          # samples with every engine pod present
    rows = [v for v in per.values() if v[2] == full]
    return {"cpu_mean": round(st.mean(r[0] for r in rows) / 1000, 2),
            "cpu_peak": round(max(r[0] for r in rows) / 1000, 2),
            "mem_mean_gb": round(st.mean(r[1] for r in rows) / 1024, 2),
            "mem_peak_gb": round(max(r[1] for r in rows) / 1024, 2),
            "pods": full, "samples": len(rows)}


def evidence(path):
    if not os.path.exists(path):
        return {}
    t = open(path).read()
    g = lambda pat: int(m.group(1)) if (m := re.search(pat, t)) else None
    ev = {"async_log_write_threads": g(r"async-log-write threads: (\d+)"),
          "async_exec_frames": g(r"AsyncProgressTrackingMicroBatchExecution frames: (\d+)"),
          "tm_pods": g(r"taskmanager pods: (\d+)")}
    m = re.search(r"latest commit log entry[^\n]*\n(.*)", t, re.S)
    ev["last_commit"] = m.group(1).strip().replace("\n", " ") if m else None
    off = re.search(r"\[offsets\]\n(.*?)\[commits\]", t, re.S)
    ev["batch_starts"] = []
    if off:
        for line in off.group(1).splitlines():
            p = line.split()
            if len(p) >= 2 and re.search(r"/offsets/\d+$", p[1]):
                ev["batch_starts"].append(dt.datetime.fromisoformat(p[0].replace("Z", "+00:00")).timestamp())
    ev["n_offsets"] = len(ev["batch_starts"])
    c = re.search(r"\[commits\]\n(.*?)--- latest", t, re.S)
    ev["n_commits"] = len([l for l in c.group(1).splitlines() if re.search(r"/commits/\d+$", l)]) if c else None
    return ev


def flink_boundaries(path):
    if not os.path.exists(path):
        return [], None
    try:
        j = load(path)
        return [h["trigger_timestamp"] / 1000 for h in j.get("history", [])], j.get("counts")
    except (ValueError, KeyError):
        return [], None


def hist_pct(h, q):
    tot = sum(h.values())
    if not tot:
        return None
    acc, target = 0, q * tot
    for b in sorted(h):
        acc += h[b]
        if acc >= target:
            return float(b)
    return float(max(h))


def boundary_split(hist_path, bounds, lo=-1, hi=3):
    if not os.path.exists(hist_path) or not bounds:
        return None
    raw = load(hist_path)
    near, far = {}, {}
    for sec, buckets in raw.items():
        s = int(sec)
        dst = near if any(b + lo <= s <= b + hi for b in bounds) else far
        for k, v in buckets.items():
            dst[int(k)] = dst.get(int(k), 0) + v
    n, f = sum(near.values()), sum(far.values())
    return {"boundaries": len(bounds), "near_share_pct": round(100 * n / max(1, n + f), 2),
            "near_p99": hist_pct(near, .99), "near_max": float(max(near)) if near else None,
            "far_p99": hist_pct(far, .99), "far_max": float(max(far)) if far else None}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=os.path.join(ROOT, "results", "eks-4.3"))
    ap.add_argument("--prev", default=os.path.join(ROOT, "results", "eks"))
    a = ap.parse_args()
    out = {}
    for pre, name, prev in ENGINES:
        runs = []
        for sp in sorted(glob.glob(os.path.join(a.dir, f"{pre}-r*_summary.json"))):
            lab = os.path.basename(sp)[: -len("_summary.json")]
            if not re.fullmatch(re.escape(pre) + r"-r\d+", lab):
                continue
            s = load(sp)
            ev = evidence(os.path.join(a.dir, f"{lab}_evidence.txt"))
            fb, counts = flink_boundaries(os.path.join(a.dir, f"{lab}_checkpoints.json"))
            bounds = ev.get("batch_starts") or fb
            runs.append({"run": lab, "kept_records": s["kept_records"], "throughput_eps": s["throughput_eps"],
                         **{f"{m}_{q}": s[m][q] for m, q in METRICS},
                         "res": resources(os.path.join(a.dir, f"{lab}_top.txt")),
                         "evidence": {k: v for k, v in ev.items() if k != "batch_starts"},
                         "flink_ckpt_counts": counts,
                         "boundary": boundary_split(os.path.join(a.dir, f"{lab}_hist.json"), bounds)})
        if not runs:
            continue
        gm = {f"{m}_{q}": geomean([r[f"{m}_{q}"] for r in runs]) for m, q in METRICS}
        rs = [r["res"] for r in runs if r["res"]]
        gm["cpu_mean"] = st.mean(x["cpu_mean"] for x in rs) if rs else None
        gm["mem_mean_gb"] = st.mean(x["mem_mean_gb"] for x in rs) if rs else None
        bs = [r["boundary"] for r in runs if r["boundary"]]
        gm["near_p99"] = geomean([b["near_p99"] for b in bs])
        gm["far_p99"] = geomean([b["far_p99"] for b in bs])
        pv = None
        if prev and os.path.exists(os.path.join(a.prev, f"{prev}_summary.json")):
            ps = load(os.path.join(a.prev, f"{prev}_summary.json"))
            pv = {f"{m}_{q}": ps[m][q] for m, q in METRICS}
        out[pre] = {"name": name, "runs": runs, "geomean": gm, "previous": pv}
    json.dump(out, open(os.path.join(a.dir, "matrix_summary.json"), "w"), indent=2)

    f1 = lambda x: "n/a" if x is None else f"{x:.1f}"
    print("### Geomean of runs vs previous EKS p6 (Spark 4.1.2 / Flink 2.2.0, single run)\n")
    print("| engine | runs | pipe p50 | pipe p99 | pipe max | e2e p50 | e2e p99 | p99 near boundary | p99 elsewhere | CPU cores | mem GB | prev pipe p50 / p99 | prev e2e p99 |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for e in out.values():
        g, p = e["geomean"], e["previous"]
        print(f"| {e['name']} | {len(e['runs'])} | {f1(g['pipeline_ms_p50'])} | {f1(g['pipeline_ms_p99'])} | "
              f"{f1(g['pipeline_ms_max'])} | {f1(g['e2e_ms_p50'])} | {f1(g['e2e_ms_p99'])} | {f1(g['near_p99'])} | "
              f"{f1(g['far_p99'])} | {f1(g['cpu_mean'])} | {f1(g['mem_mean_gb'])} | "
              f"{(f1(p['pipeline_ms_p50']) + ' / ' + f1(p['pipeline_ms_p99'])) if p else 'n/a'} | "
              f"{f1(p['e2e_ms_p99']) if p else 'n/a'} |")
    print("\n### Per run\n")
    print("| run | records | eps | pipe p50 | pipe p99 | pipe max | e2e p50 | e2e p99 | e2e max | near p99 / max | elsewhere p99 / max | CPU mean / peak | mem GB mean / peak | pods | evidence |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for e in out.values():
        for r in e["runs"]:
            b, x, ev = r["boundary"], r["res"], r["evidence"]
            if ev.get("async_log_write_threads") is not None:
                evs = (f"async-log-write={ev['async_log_write_threads']}, AsyncExec={ev['async_exec_frames']}, "
                       f"offsets/commits={ev['n_offsets']}/{ev['n_commits']}")
            elif r["flink_ckpt_counts"]:
                c = r["flink_ckpt_counts"]
                evs = f"ckpt completed={c.get('completed')} failed={c.get('failed')}, TMs={ev.get('tm_pods')}"
            else:
                evs = ""
            print(f"| {r['run']} | {r['kept_records']} | {r['throughput_eps']:.0f} | {f1(r['pipeline_ms_p50'])} | "
                  f"{f1(r['pipeline_ms_p99'])} | {f1(r['pipeline_ms_max'])} | {f1(r['e2e_ms_p50'])} | "
                  f"{f1(r['e2e_ms_p99'])} | {f1(r['e2e_ms_max'])} | "
                  f"{(f1(b['near_p99']) + ' / ' + f1(b['near_max'])) if b else 'n/a'} | "
                  f"{(f1(b['far_p99']) + ' / ' + f1(b['far_max'])) if b else 'n/a'} | "
                  f"{(f1(x['cpu_mean']) + ' / ' + f1(x['cpu_peak'])) if x else 'n/a'} | "
                  f"{(f1(x['mem_mean_gb']) + ' / ' + f1(x['mem_peak_gb'])) if x else 'n/a'} | "
                  f"{x['pods'] if x else 'n/a'} | {evs} |")


if __name__ == "__main__":
    main()
