#!/usr/bin/env python3
"""
Aggregate the local Spark 4.3 / Flink 2.3 p6 matrix (results/local-4.3/).

Inputs per run (label = <engine>-r<N>):
  <label>_summary.json         latency_consumer output
  <label>_stats.csv            docker stats samples (CPU/mem; kafka excluded from totals)
  <label>_clock.jsonl          Docker-VM-vs-host clock probes taken during the run
  <label>_async_evidence.txt   Spark RTM arms only (jstack + checkpoint logs)
  <label>_checkpoints.json     Flink arms only (REST /jobs/<id>/checkpoints)

Writes matrix_summary.json and prints per-run + geomean markdown tables.
Previous (Spark 4.1.2 / Flink 2.2.0) local p6 numbers come from results/<old>_summary.json.

Usage: python bench/analyze_local43.py [--dir results/local-4.3]
"""
import argparse
import glob
import json
import math
import os
import re
import statistics as st

import analyze  # reuse load_stats/engine_totals so CPU/mem match the original method

ROOT = os.path.join(os.path.dirname(__file__), "..")
ENGINES = [
    # (label, display name, previous-run summary file stem or None)
    ("spark-rtm-java", "Spark RTM Java", None),
    ("spark-rtm-scala", "Spark RTM Scala", "spark-rtm"),
    ("pyspark-rtm", "PySpark RTM (native trigger)", "pyspark-rtm"),
    ("spark-rtm-syncckpt", "Spark RTM Scala, async tracking OFF", None),
    ("spark-microbatch", "Spark micro-batch (floor)", "spark-microbatch"),
    ("flink", "Flink DataStream (Java)", "flink"),
    ("flink-sql", "Flink SQL (Java)", "flink-sql"),
    ("pyflink", "PyFlink", "pyflink"),
]
METRICS = [("pipeline_ms", "p50"), ("pipeline_ms", "p99"), ("e2e_ms", "p50"), ("e2e_ms", "p99")]


def geomean(xs):
    xs = [x for x in xs if x is not None]
    if not xs or any(x <= 0 for x in xs):
        return None
    return math.exp(sum(math.log(x) for x in xs) / len(xs))


def load_json(p):
    with open(p) as f:
        return json.load(f)


def clock_stats(path):
    if not os.path.exists(path):
        return None
    offs = []
    with open(path) as f:
        for line in f:
            try:
                offs.append(json.loads(line)["offset_ms"])
            except (ValueError, KeyError):
                pass
    if not offs:
        return None
    return {"n": len(offs), "mean": round(st.mean(offs), 2), "min": round(min(offs), 2),
            "max": round(max(offs), 2)}


def async_evidence(path):
    if not os.path.exists(path):
        return None
    txt = open(path).read()
    g = lambda pat: int(m.group(1)) if (m := re.search(pat, txt)) else None
    offsets = re.search(r"\[offsets\]\n(.*?)\[commits\]", txt, re.S)
    commits = re.search(r"\[commits\]\n(.*?)(?:--- latest|\Z)", txt, re.S)
    count = lambda m: len([l for l in m.group(1).splitlines() if l.strip()]) if m else None
    return {"async_log_write_threads": g(r"async-log-write threads: (\d+)"),
            "async_exec_frames": g(r"AsyncProgressTrackingMicroBatchExecution frames: (\d+)"),
            "offset_log_entries": count(offsets), "commit_log_entries": count(commits)}


def flink_ckpt(path):
    if not os.path.exists(path):
        return None
    try:
        c = load_json(path)["counts"]
    except (ValueError, KeyError):
        return None
    return {"completed": c.get("completed"), "failed": c.get("failed")}


def boundary_epochs(d, run_label, day_epoch):
    """Epoch seconds of commit boundaries: Spark RTM = offsets/<N> mtimes (start of batch N,
    i.e. end of batch N-1); Flink = checkpoint trigger timestamps. None if unknown."""
    ev = os.path.join(d, f"{run_label}_async_evidence.txt")
    if os.path.exists(ev):
        m = re.search(r"\[offsets\]\n(.*?)\[commits\]", open(ev).read(), re.S)
        if not m:
            return None
        out = []
        for line in m.group(1).splitlines():
            t = re.search(r" (\d\d):(\d\d):(\d\d) \d+$", line)
            if t:  # container clock is UTC
                h, mi, s = map(int, t.groups())
                out.append(day_epoch + h * 3600 + mi * 60 + s)
        return out
    ck = os.path.join(d, f"{run_label}_checkpoints.json")
    if os.path.exists(ck):
        try:
            return [h["trigger_timestamp"] / 1000.0 for h in load_json(ck).get("history", [])]
        except (ValueError, KeyError):
            return None
    return None


def boundary_split(d, run_label, window=(-1.0, 3.0)):
    """Split pipeline latencies by whether the record's ts falls near a commit boundary.
    Returns p99/max inside vs outside the windows, plus share of records inside."""
    import csv
    import gzip
    p = os.path.join(d, f"{run_label}_latency.csv.gz")
    if not os.path.exists(p):
        return None
    rows = [(float(r["ts"]), float(r["pipeline_ms"]), float(r["recv_ts"]))
            for r in csv.DictReader(gzip.open(p, "rt"))]
    if not rows:
        return None
    # The consumer's first 15 s of wall clock are warm-up; keep the same steady-state set.
    t0 = min(r[2] for r in rows)
    rows = [r for r in rows if r[2] - t0 >= 15]
    day = int(rows[0][0]) // 86400 * 86400
    bs = boundary_epochs(d, run_label, day)
    if not bs:
        return None
    lo, hi = window
    near, far = [], []
    for ts, lat, _ in rows:
        (near if any(b + lo <= ts <= b + hi for b in bs) else far).append(lat)
    near.sort(); far.sort()
    q = lambda v, x: round(v[min(len(v) - 1, int(x * (len(v) - 1)))], 1) if v else None
    return {"boundaries": len(bs), "near_share_pct": round(100 * len(near) / len(rows), 2),
            "near_p99": q(near, .99), "near_max": round(near[-1], 1) if near else None,
            "far_p99": q(far, .99), "far_max": round(far[-1], 1) if far else None}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=os.path.join(ROOT, "results", "local-4.3"))
    args = ap.parse_args()
    d = args.dir

    out = {"engines": {}}
    for label, name, old in ENGINES:
        runs = []
        for sp in sorted(glob.glob(os.path.join(d, f"{label}-r*_summary.json"))):
            run_label = os.path.basename(sp)[: -len("_summary.json")]
            s = load_json(sp)
            analyze.RESULTS = d
            tot = analyze.engine_totals(analyze.load_stats(run_label))
            runs.append({
                "run": run_label,
                "kept_records": s["kept_records"],
                "throughput_eps": s["throughput_eps"],
                **{f"{m}_{q}": s[m][q] for m, q in METRICS},
                "pipeline_ms_max": s["pipeline_ms"]["max"],
                "cpu_mean_cores": tot["cpu_mean"], "mem_mean_mb": tot["mem_mean"],
                "clock_offset_ms": clock_stats(os.path.join(d, f"{run_label}_clock.jsonl")),
                "async_evidence": async_evidence(os.path.join(d, f"{run_label}_async_evidence.txt")),
                "flink_checkpoints": flink_ckpt(os.path.join(d, f"{run_label}_checkpoints.json")),
                "boundary_split": boundary_split(d, run_label),
            })
        if not runs:
            continue
        for r in runs:
            # Host-disturbance flag: the Docker VM clock jumped >5 ms vs the host during the
            # run (VM was paused/starved), or a >1 s stall happened away from any commit
            # boundary. Either means the tail reflects the host, not the engine.
            c, b = r["clock_offset_ms"], r["boundary_split"]
            r["host_disturbed"] = bool(
                (c and max(abs(c["min"]), abs(c["max"])) > 5.0)
                or (b and b["far_max"] is not None and b["far_max"] > 1000)
                or (not b and r["pipeline_ms_max"] > 1000))
        gm = {f"{m}_{q}": geomean([r[f"{m}_{q}"] for r in runs]) for m, q in METRICS}
        clean = [r for r in runs if not r["host_disturbed"]]
        gm["clean_runs"] = len(clean)
        gm["clean_pipeline_ms_p99"] = geomean([r["pipeline_ms_p99"] for r in clean])
        gm["cpu_mean_cores"] = st.mean(r["cpu_mean_cores"] for r in runs)
        gm["mem_mean_mb"] = st.mean(r["mem_mean_mb"] for r in runs)
        prev = None
        if old and os.path.exists(os.path.join(ROOT, "results", f"{old}_summary.json")):
            ps = load_json(os.path.join(ROOT, "results", f"{old}_summary.json"))
            analyze.RESULTS = os.path.join(ROOT, "results")
            pt = analyze.engine_totals(analyze.load_stats(old))
            prev = {**{f"{m}_{q}": ps[m][q] for m, q in METRICS},
                    "cpu_mean_cores": pt["cpu_mean"], "mem_mean_mb": pt["mem_mean"]}
        out["engines"][label] = {"name": name, "runs": runs, "geomean": gm, "previous": prev}

    with open(os.path.join(d, "matrix_summary.json"), "w") as f:
        json.dump(out, f, indent=2)

    f1 = lambda x: "n/a" if x is None else f"{x:.1f}"
    print("### Geomean of 3 runs vs previous local p6 (Spark 4.1.2 / Flink 2.2.0, single run)\n")
    print("| engine | pipe p50 | pipe p99 | e2e p50 | e2e p99 | CPU cores | mem MB | pipe p99, clean runs only | prev pipe p50 / p99 | prev e2e p99 |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for label, e in out["engines"].items():
        g, p = e["geomean"], e["previous"]
        prev = f"{f1(p['pipeline_ms_p50'])} / {f1(p['pipeline_ms_p99'])}" if p else "n/a (new arm)"
        print(f"| {e['name']} | {f1(g['pipeline_ms_p50'])} | {f1(g['pipeline_ms_p99'])} | "
              f"{f1(g['e2e_ms_p50'])} | {f1(g['e2e_ms_p99'])} | {g['cpu_mean_cores']:.2f} | "
              f"{g['mem_mean_mb']:.0f} | {f1(g['clean_pipeline_ms_p99'])} ({g['clean_runs']}/{len(e['runs'])}) | "
              f"{prev} | {f1(p['e2e_ms_p99']) if p else 'n/a'} |")
    print("\n### Per run\n")
    print("| run | records | pipe p50 | pipe p99 | pipe max | e2e p50 | e2e p99 | CPU | mem MB | VM clock offset ms (mean, min..max) | evidence |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for e in out["engines"].values():
        for r in e["runs"]:
            c = r["clock_offset_ms"]
            cs = f"{c['mean']} ({c['min']}..{c['max']})" if c else "n/a"
            ev = ""
            if r["async_evidence"]:
                a = r["async_evidence"]
                ev = (f"async-log-write={a['async_log_write_threads']}, AsyncExec frames={a['async_exec_frames']}, "
                      f"offsets/commits={a['offset_log_entries']}/{a['commit_log_entries']}")
            if r["flink_checkpoints"]:
                ev = f"ckpt completed={r['flink_checkpoints']['completed']} failed={r['flink_checkpoints']['failed']}"
            flag = " ⚠ host-disturbed" if r["host_disturbed"] else ""
            print(f"| {r['run']}{flag} | {r['kept_records']} | {f1(r['pipeline_ms_p50'])} | {f1(r['pipeline_ms_p99'])} | "
                  f"{f1(r['pipeline_ms_max'])} | {f1(r['e2e_ms_p50'])} | {f1(r['e2e_ms_p99'])} | "
                  f"{r['cpu_mean_cores']:.2f} | {r['mem_mean_mb']:.0f} | {cs} | {ev} |")
    print("\n### Tail near commit boundaries (record ts within -1s..+3s of a Spark batch start / "
          "Flink checkpoint trigger)\n")
    print("| run | boundaries | % records near | near p99 | near max | elsewhere p99 | elsewhere max |")
    print("|---|---|---|---|---|---|---|")
    for e in out["engines"].values():
        for r in e["runs"]:
            b = r["boundary_split"]
            if b:
                print(f"| {r['run']} | {b['boundaries']} | {b['near_share_pct']} | {f1(b['near_p99'])} | "
                      f"{f1(b['near_max'])} | {f1(b['far_p99'])} | {f1(b['far_max'])} |")


if __name__ == "__main__":
    main()
