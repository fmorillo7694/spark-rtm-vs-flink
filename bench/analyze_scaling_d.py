#!/usr/bin/env python3
"""
Session D (scaling) analysis: one row per run and per arm for a results directory written by
eks/stateful/run_matrix_incluster.sh with PLAN_SET=D (S1, 50k -> 100k -> 50k evt/s).

Per run, from the files the runner keeps:
  steps.txt      producers' "STEP <epoch> <per-pod rate>" lines. A load step starts at the
                 earliest pod's line of that step (pods start within a few seconds).
  hist.json      consumer, per EVENT second: pipeline-latency bucket counts (steady state only).
  rate.json      consumer, per RECEIVE second: [records, sum of e2e ms].
  scaling.txt    every 10 s: engine nodes listed/ready, worker pods running/pending, Flink job
                 state + vertex parallelism, HPA replicas / CPU (flink-reactive).
  manual.txt     spark-rtm-manual restart times (stop / gone / running / query-started).
  truth.jsonl + maxcnt.json + summary.json   the S1 truth check (bench/recovery_check.py).
  verify.json    input / output record counts (dup_records = output - input).

Per load step (phase = from the step to the next one):
  steady_s   seconds from the step until the end of the phase's last 60 s window (windows start
             every 10 s) whose records have pipeline p99 >= 2000 ms, i.e. from then on every
             window is "p99 < 2 s for 60 s" (SCALING_PLAN.md). 0 = no bad window; None = still
             bad at the end of the phase. Phase 1 includes job warm-up.
  degraded_s event seconds in the phase whose own p99 is >= 2000 ms
  p99_max_ms the worst per-second pipeline p99 in the phase (the latency spike)
  p50/p99    over all of the phase's records (bucket lower bounds, so within 1/10/100 ms)
  stall_s    longest run of receive seconds with < 10 % of the step's input rate (output stall)
Per run: rescales (Flink vertex-parallelism changes, Spark executor-count changes or manual
restarts), engine node-hours over the load (scaling.txt, 10 s samples x listed nodes), mean /
max nodes, worker pods max, node $ at m5.2xlarge on-demand $0.384/h, and node-hours vs the
static-peak arm of the same engine (flink-peak / spark-rtm-peak) over the same load length.

Usage: python bench/analyze_scaling_d.py DIR   (writes DIR/scaling.md and DIR/scaling.json)
"""
import collections
import glob
import json
import os
import re
import statistics as st
import sys

sys.path.insert(0, os.path.dirname(__file__))
from recovery_check import truth_diff  # noqa: E402

NODE_USD_H = 0.384      # m5.2xlarge on-demand, us-east-1
PODS = 8                # producer pods (PRODPODS)
P99_OK_MS, HOLD_S = 2000, 60


def load(d, n):
    p = os.path.join(d, n)
    return json.load(open(p)) if os.path.exists(p) and os.path.getsize(p) else None


def steps_of(d):
    """[(epoch, aggregate rate)]: STEP lines clustered (a new step = > 60 s after the last)."""
    p = os.path.join(d, "steps.txt")
    if not os.path.exists(p):
        return []
    out = []
    for line in open(p):
        _, t, r = line.split()
        t, r = float(t), int(r)
        if not out or t - out[-1][0] > 60:
            out.append([t, r * PODS])
    return [tuple(x) for x in out]


def pct(buckets, q):
    n = sum(buckets.values())
    if not n:
        return None
    k, c = q * (n - 1), 0
    for b in sorted(buckets):
        c += buckets[b]
        if c > k:
            return b
    return max(buckets)


def phase_metrics(hist, rate, t0, t1, in_rate):
    secs = [s for s in range(int(t0), int(t1))]
    agg = collections.Counter()
    p99s = {}
    for s in secs:
        h = hist.get(str(s))
        if h:
            c = {int(b): n for b, n in h.items()}
            agg.update(c)
            p99s[s] = pct(c, 0.99)
    # steady: the p99 of all records in [s, s + 60) is < 2 s. Per-second p99 would never settle:
    # every 60 s checkpoint adds a 2-3 s spike (Flink alignment, Spark batch boundary).
    # steady = from the step to the end of the LAST bad 60 s window (windows every 10 s) of the
    # phase: after it, every window stays < 2 s. 0 = never bad; None = still bad at phase end.
    per = {s: {int(b): n for b, n in hist[str(s)].items()} for s in secs if str(s) in hist}
    last_bad, bad_end = None, False
    for s in secs[::10]:
        if s + HOLD_S > t1:
            break
        win = collections.Counter()
        for j in range(HOLD_S):
            win.update(per.get(s + j, {}))
        bad = not sum(win.values()) or pct(win, 0.99) >= P99_OK_MS
        if bad:
            last_bad = s
        bad_end = bad
    steady = None if bad_end else (0 if last_bad is None else max(0, last_bad + HOLD_S - t0))
    degraded = sum(1 for v in p99s.values() if v >= P99_OK_MS)
    stall = run = 0
    for s in secs:
        if rate.get(str(s), [0])[0] < 0.1 * in_rate:
            run += 1; stall = max(stall, run)
        else:
            run = 0
    return {"in_rate": in_rate, "secs": int(t1 - t0),
            "steady_s": None if steady is None else round(steady),
            "p50_ms": pct(agg, 0.5), "p99_ms": pct(agg, 0.99),
            "p99_max_ms": max(p99s.values()) if p99s else None, "stall_s": stall,
            "degraded_s": degraded}


def scaling_samples(d):
    p = os.path.join(d, "scaling.txt")
    rows = []
    if not os.path.exists(p):
        return rows
    for line in open(p):
        f = line.split()
        if not f:
            continue
        kv = dict(x.split("=", 1) for x in f[1:] if "=" in x)
        nodes = kv.get("nodes", "0/0").split("/")
        rows.append({"t": int(f[0]), "nodes": int(nodes[0] or 0), "ready": int(nodes[1] or 0),
                     "running": int(kv.get("running", 0)), "pending": int(kv.get("pending", 0)),
                     "par": kv.get("par"), "job": kv.get("job"), "hpa": kv.get("hpa")})
    return rows


def run_row(d):
    label = os.path.basename(d)
    summ = load(d, "summary.json") or {}
    hist, rate = load(d, "hist.json") or {}, load(d, "rate.json") or {}
    steps = steps_of(d)
    row = {"run": label, "arm": re.sub(r"-r\d+$", "", label), "kept": summ.get("kept_records"),
           "p50_ms": (summ.get("pipeline_ms") or {}).get("p50"), "p99_ms": (summ.get("pipeline_ms") or {}).get("p99")}
    end = summ.get("ended_at") or (max(int(s) for s in rate) if rate else 0)
    phases = []
    for i, (t, r) in enumerate(steps):
        t1 = steps[i + 1][0] if i + 1 < len(steps) else end
        phases.append({"step": i + 1, **phase_metrics(hist, rate, t, t1, r)})
    row["phases"] = phases
    # node-hours / rescales over the load (first step .. consumer end)
    sm = [x for x in scaling_samples(d) if not steps or steps[0][0] <= x["t"] <= end]
    if sm:
        dt = [(sm[i + 1]["t"] - sm[i]["t"]) if i + 1 < len(sm) else 10 for i in range(len(sm))]
        row["node_h"] = round(sum(x["nodes"] * t for x, t in zip(sm, dt)) / 3600, 3)
        row["load_h"] = round(sum(dt) / 3600, 3)
        row["nodes_mean"] = round(row["node_h"] / row["load_h"], 2) if row["load_h"] else None
        row["nodes_max"] = max(x["nodes"] for x in sm)
        row["workers_max"] = max(x["running"] for x in sm)
        row["pending_max"] = max(x["pending"] for x in sm)
        row["node_usd"] = round(row["node_h"] * NODE_USD_H, 2)
        # RUNNING samples only: a job in CREATED (mid-rescale) reports max-parallelism (120)
        pars = [x["par"] for x in sm if x["par"] and x["job"] == "RUNNING"]
        work = [x["running"] for x in sm]
        if "flink" in label and pars:
            row["rescales"] = sum(1 for a, b in zip(pars, pars[1:]) if a != b)
            row["par_path"] = "->".join(p for i, p in enumerate(pars) if i == 0 or p != pars[i - 1])
        else:
            row["rescales"] = sum(1 for a, b in zip(work, work[1:]) if a != b)
            row["workers_path"] = "->".join(str(w) for i, w in enumerate(work) if i == 0 or w != work[i - 1])
        hp = [x["hpa"] for x in sm if x["hpa"]]
        if hp:
            row["hpa_path"] = "->".join(h for i, h in enumerate(hp) if i == 0 or h != hp[i - 1])
    mp = os.path.join(d, "manual.txt")
    if os.path.exists(mp):
        ev = [l.split() for l in open(mp) if l.strip()]
        stops = {e[2]: float(e[0]) for e in ev if e[1] == "stop"}
        started = {e[2]: float(e[0]) for e in ev if e[1] == "query-started"}
        row["manual_restart_s"] = [round(started[k] - stops[k]) for k in stops if k in started]
        row["rescales"] = len(stops)
    truths = [json.loads(l) for l in open(os.path.join(d, "truth.jsonl"))] if os.path.exists(os.path.join(d, "truth.jsonl")) else []
    mx = load(d, "maxcnt.json")
    if truths and mx and summ.get("started_at"):
        row.update(truth_diff(truths, mx, summ["started_at"]))
    v = load(d, "verify.json")
    if v and "output_records" in v:
        row["input_records"], row["output_records"] = v["input_records"], v["output_records"]
        row["dup_records"] = v["output_records"] - v["input_records"]
    p = os.path.join(d, "nodes_start.txt")   # key=value text
    if os.path.exists(p):
        row.update({k: int(v) for k, v in (x.split("=") for x in open(p).read().split())})
    return row


def fmt(x):
    return "-" if x is None else x


def main():
    root = sys.argv[1]
    rows = [run_row(d) for d in sorted(glob.glob(os.path.join(root, "runs", "*")))
            if os.path.exists(os.path.join(d, "steps.txt"))]
    peak = {}
    for r in rows:
        if r["arm"].startswith(("flink-peak", "spark-rtm-peak")) and r.get("nodes_mean"):
            peak.setdefault("flink" if r["arm"].startswith("flink") else "spark", []).append(r["nodes_mean"])
    for r in rows:
        eng = "flink" if r["run"].startswith("flink") else "spark"
        if peak.get(eng) and r.get("load_h"):
            r["node_h_vs_peak"] = round(r["node_h"] / (st.mean(peak[eng]) * r["load_h"]), 2)
    json.dump(rows, open(os.path.join(root, "scaling.json"), "w"), indent=2)
    L = ["# Session D scaling: per run", "",
         "Phases: 1 = 50k (incl. job warm-up), 2 = 100k, 3 = back to 50k evt/s; -scalein rows: 1 = 100k (incl. warm-up), 2 = 25k. steady = s from the step until "
         "every later 60 s window has pipeline p99 < 2 s (- = never in the phase); degraded = seconds with own "
         "p99 >= 2 s; spike = worst per-second p99; stall = longest output gap (< 10 % of input). Node-hours "
         "over the load (engine nodes, 10 s samples).", "",
         "| run | steady s (1/2/3) | degraded s (1/2/3) | spike p99 ms (1/2/3) | p99 ms (1/2/3) | stall s (1/2/3) | rescales | "
         "nodes mean/max | node-h | vs peak | $ | lost / over / settled | dup records |",
         "|---|---|---|---|---|---|---:|---|---:|---:|---:|---|---:|"]
    for r in rows:
        ph = r["phases"]
        col = lambda k: " / ".join(str(fmt(p.get(k))) for p in ph)
        L.append(f"| {r['run']} | {col('steady_s')} | {col('degraded_s')} | {col('p99_max_ms')} | {col('p99_ms')} | {col('stall_s')} | "
                 f"{fmt(r.get('rescales'))} | {fmt(r.get('nodes_mean'))}/{fmt(r.get('nodes_max'))} | {fmt(r.get('node_h'))} | "
                 f"{fmt(r.get('node_h_vs_peak'))} | {fmt(r.get('node_usd'))} | {fmt(r.get('lost'))} / {fmt(r.get('overcount'))} / "
                 f"{fmt(r.get('settled_windows'))} | {fmt(r.get('dup_records'))} |")
    L += ["", "Scaling paths (Flink vertex parallelism / Spark running executors / HPA current/desired):", ""]
    for r in rows:
        extra = r.get("par_path") or r.get("workers_path") or "-"
        L.append(f"- {r['run']}: {extra}" + (f"; HPA {r['hpa_path']}" if r.get("hpa_path") else "")
                 + (f"; manual restart (stop -> query started) s: {r['manual_restart_s']}" if r.get("manual_restart_s") else "")
                 + (f"; start nodes {r.get('start_engine_nodes')} after {r.get('waited_s')} s wait" if "start_engine_nodes" in r else ""))
    open(os.path.join(root, "scaling.md"), "w").write("\n".join(L) + "\n")
    print("\n".join(L))


if __name__ == "__main__":
    main()
