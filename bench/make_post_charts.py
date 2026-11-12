#!/usr/bin/env python3
"""
Charts and numbers for the refreshed piece-1 post (post/piece1-4.3.md): Spark 4.3.0 RTM vs
Flink 2.3.0 on EKS, stateless + stateful. Every number comes from the per-run artifacts:

  results/eks-4.3/<run>_{summary,hist}.json, <run>_top.txt        stateless Spark RTM Java/Scala (4.3 refresh)
  results/eks-stateful/runs/<run>/                                 session A (stateful, 50k evt/s)
  results/eks-stateful-b/runs/<run>/                               session B (stateful + stateless leftovers)

Per cell: geomean over runs of each run's p50/p99 (house rule). CCDFs use the 1/10/100 ms
histogram buckets (lower bounds) of the run whose p99 is the cell's median. Resources are
the engine pods only (operator pods excluded), restricted to the measured window.

Writes post/charts/*.png and post/charts/numbers.json (every value the post quotes).
Usage: <venv>/bin/python bench/make_post_charts.py
"""
import json
import math
import os
import re
import statistics as st
import sys
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from make_charts import C_SPARK, C_SPARK2, C_FLINK, C_FLINK2, C_MB  # noqa: E402,F401  (shared palette + rcParams)
from analyze_eks43 import geomean, hist_pct  # noqa: E402

ROOT = os.path.join(HERE, "..")
OUT = os.path.join(ROOT, "post", "charts")
A = os.path.join(ROOT, "results", "eks-stateful", "runs")
B = os.path.join(ROOT, "results", "eks-stateful-b", "runs")
S43 = os.path.join(ROOT, "results", "eks-4.3")
os.makedirs(OUT, exist_ok=True)
C_VT = "#B23A0E"      # vendor-tuned Spark, darker orange
C_SPARK3 = "#F7C59F"  # Spark 1 ms flush, light orange
PROV = "Final run (5-min windows, 3 runs per cell unless marked n=2)"
NUM = {}


# ---------- loading ----------

class Run:
    """One run's artifacts, whichever layout it was stored in."""

    def __init__(self, base, label):
        self.label = label
        d = os.path.join(base, label)
        if os.path.isdir(d):
            self.f = lambda name: os.path.join(d, name)
        else:  # results/eks-4.3 flat layout: <label>_<name>
            self.f = lambda name: os.path.join(base, f"{label}_{name}")
        with open(self.f("summary.json")) as fh:
            self.s = json.load(fh)
        assert self.s["kept_records"] > 0, f"{label}: empty run"
        self.p50 = self.s["pipeline_ms"]["p50"]
        self.p99 = self.s["pipeline_ms"]["p99"]
        # Measured window: hist.json holds exactly the measured seconds (warm-up already cut);
        # older summaries have no started_at, so take the window from the histogram keys.
        secs = sorted(int(k) for k in (self.load("hist.json") or {}))
        assert secs, f"{label}: empty histogram"
        self.t0, self.t1 = secs[0], secs[-1]

    def load(self, name):
        p = self.f(name)
        if not os.path.exists(p) or os.path.getsize(p) == 0:
            return None
        with open(p) as fh:
            return json.load(fh)

    def text(self, name):
        p = self.f(name)
        return open(p).read() if os.path.exists(p) else ""


def runs(base, prefix, ns):
    return [Run(base, f"{prefix}-r{n}") for n in ns]


class Cell:
    def __init__(self, key, name, color, rs):
        self.key, self.name, self.color, self.runs = key, name, color, rs
        self.p50 = geomean([r.p50 for r in rs])
        self.p99 = geomean([r.p99 for r in rs])
        self.ids = [r.label for r in rs]
        NUM[key] = {"name": name, "n": len(rs), "p50": round(self.p50, 1), "p99": round(self.p99, 1),
                    "runs": self.ids, "per_run_p99": [round(r.p99, 1) for r in rs],
                    "throughput_eps": round(st.mean(r.s["throughput_eps"] for r in rs))}

    def rep(self):
        """Representative run: median p99."""
        return sorted(self.runs, key=lambda r: r.p99)[len(self.runs) // 2]


R3 = (1, 2, 3)
R2 = (1, 2)
FA = os.path.join(ROOT, "results", "final-run", "a", "runs")
FB = os.path.join(ROOT, "results", "final-run", "b", "runs")
CELLS = json.load(open(os.path.join(ROOT, "results", "final-run", "cells.json")))

# ---- Final run (results/final-run, 5-min windows, 3 runs per cell; n=2 cells marked) ----
# Stateless, 100k evt/s in (~33k/s out); engines pinned to the 2 engine nodes in the final run
STATELESS = [
    Cell("sl_flink_ds", "Flink DataStream", C_FLINK, runs(FA, "flink-p6", R3)),
    Cell("sl_flink_sql", "Flink SQL\n(session B)", C_FLINK2, runs(B, "flink-sql-p6", R3)),
    Cell("sl_pyflink", "PyFlink (p=6)", C_FLINK2, runs(FA, "pyflink-p6", R3)),
    Cell("sl_rtm_java", "Spark RTM Java", C_SPARK, runs(FA, "spark-rtm-java-p6", R3)),
    Cell("sl_rtm_scala", "Spark RTM Scala", C_SPARK, runs(FA, "spark-rtm-scala-p6", R3)),
    Cell("sl_pyspark", "PySpark RTM", C_SPARK2, runs(FA, "pyspark-rtm-p6", R3)),
    Cell("sl_rtm_syncoff", "Spark RTM Java,\nasync tracking OFF", C_MB, runs(FA, "spark-rtm-syncckpt-p6", R3)),
]
# Published 4.1.2 / 2.2.0 single-run p99 (results/eks/*-p6_summary.json of the original checkout)
PREV_P99 = {"sl_flink_ds": 29.8, "sl_flink_sql": 31.4, "sl_pyflink": 33.1, "sl_rtm_java": 642.3,
            "sl_rtm_scala": 701.5, "sl_pyspark": 591.2}

# Stateful, 50k evt/s. 2x2 = {Spark, Flink} x {default, tuned} + vendor-tuned Spark.
S1 = [
    Cell("s1_spark_def", "Spark RTM\ndefault (50 ms)", C_SPARK, runs(FB, "spark-rtm-s1", R3)),
    Cell("s1_spark_1ms", "Spark RTM\ntuned (1 ms)", C_SPARK3, runs(FB, "spark-rtm-sw1-s1", R3)),
    Cell("s1_spark_vt", "Spark RTM\nvendor-tuned", C_VT, runs(FB, "spark-rtm-vt-s1", R3)),
    Cell("s1_flink_def", "Flink\ndefault (100 ms)", C_FLINK2, runs(FB, "flink-ds-bt100-s1-update", R3)),
    Cell("s1_flink_0", "Flink\ntuned (0 ms)", C_FLINK, runs(FB, "flink-ds-s1-update", R3)),
]
S2 = [
    Cell("s2_spark_def", "Spark RTM\ndefault (50 ms)", C_SPARK, runs(FA, "spark-rtm-s2", R3)),
    Cell("s2_spark_1ms", "Spark RTM\ntuned (1 ms)", C_SPARK3, runs(FA, "spark-rtm-sw1-s2", R3)),
    Cell("s2_spark_vt", "Spark RTM\nvendor-tuned", C_VT, runs(FA, "spark-rtm-vt-s2", R3)),
    Cell("s2_flink_def", "Flink\ndefault (100 ms)", C_FLINK2, runs(FA, "flink-ds-bt100-s2-et", R3)),
    Cell("s2_flink_0", "Flink\ntuned (0 ms)", C_FLINK, runs(FA, "flink-ds-s2-et", R3)),
]
S2_EXTRA = [
    Cell("s2_flinksql_ttl60_0", "Flink SQL ttl60 0 ms", C_FLINK, runs(FA, "flink-sql-s2-ttl60", R3)),
    Cell("s2_flink_ttl5_0", "Flink DS ttl5 0 ms (leaky)", C_FLINK, runs(FA, "flink-ds-s2", R3)),
]
S3 = [
    Cell("s3_spark_def", "Spark RTM\ndefault (50 ms)", C_SPARK, runs(FB, "spark-rtm-s3", R3)),
    Cell("s3_spark_1ms", "Spark RTM\ntuned (1 ms)", C_SPARK3, runs(FB, "spark-rtm-sw1-s3", R3)),
    Cell("s3_spark_vt", "Spark RTM\nvendor-tuned\n(n=2)", C_VT, runs(FB, "spark-rtm-vt-s3", R2)),
    Cell("s3_flink_def", "Flink\ndefault (100 ms)", C_FLINK2, runs(FB, "flink-ds-bt100-s3", R3)),
    Cell("s3_flink_0", "Flink\ntuned (0 ms)\n(n=2)", C_FLINK, runs(FB, "flink-ds-s3", R2)),
]
KILL = {"spark": runs(FB, "spark-rtm-s1-kill", R2), "flink": runs(FB, "flink-ds-s1-update-kill", R2)}

# ---- Earlier sessions, not part of the final run (labelled as such in the post) ----
# Checkpoint / trigger interval sweep (session B), 60 s points from sessions A/B so the sweep
# stays internally consistent
CK = {
    ("spark", "s1"): {5: runs(B, "spark-rtm-s1-ckpt5s", R2), 30: runs(B, "spark-rtm-s1-ckpt30s", R2),
                      60: runs(A, "spark-rtm-s1", R3), 300: runs(B, "spark-rtm-s1-ckpt300s-720s", R2)},
    ("flink", "s1"): {5: runs(B, "flink-ds-s1-update-ckpt5s", R2), 30: runs(B, "flink-ds-s1-update-ckpt30s", R2),
                      60: runs(A, "flink-ds-s1-update", R3), 300: runs(B, "flink-ds-s1-update-ckpt300s-720s", R2)},
    ("spark", "s2"): {5: runs(B, "spark-rtm-s2-ckpt5s", R2), 30: runs(B, "spark-rtm-s2-ckpt30s", R2),
                      60: runs(A, "spark-rtm-s2", R3) + runs(B, "spark-rtm-s2", (4,))},
    ("flink", "s2"): {5: runs(B, "flink-ds-s2-et-ckpt5s", R2), 30: runs(B, "flink-ds-s2-et-ckpt30s", R2),
                      60: runs(B, "flink-ds-s2-et", R3)},
}


def save(fig, name, caption_ids, src=None):
    fig.text(0.5, -0.02, f"{src or PROV}. Source runs: {caption_ids}", ha="center", va="top",
             fontsize=7, color="#555", wrap=True)
    p = os.path.join(OUT, name)
    fig.tight_layout()
    fig.savefig(p, bbox_inches="tight")
    plt.close(fig)
    print("wrote", p)


def ids_of(cells):
    """Compact run-ID list for captions: arm prefix + run numbers."""
    parts = []
    for c in cells:
        pre = sorted({re.sub(r"-r\d+$", "", i) for i in c.ids})
        nums = ",".join(re.search(r"r(\d+)$", i).group(1) for i in c.ids)
        parts.append(f"{'+'.join(pre)} r{nums}")
    return "; ".join(parts)


def fmt(v):
    return f"{v:,.0f}" if v >= 100 else f"{v:.1f}"


# ---------- 01 stateless ----------

def chart_stateless():
    cells = STATELESS
    fig, ax = plt.subplots(figsize=(10, 4.8))
    x = np.arange(len(cells)); w = 0.27
    p50 = [c.p50 for c in cells]; p99 = [c.p99 for c in cells]
    prev = [PREV_P99.get(c.key, np.nan) for c in cells]
    ax.bar(x - w, p50, w, color=[c.color for c in cells], label="p50 (4.3 / 2.3)")
    ax.bar(x, p99, w, color=[c.color for c in cells], alpha=0.55, label="p99 (4.3 / 2.3)")
    ax.bar(x + w, prev, w, color="none", edgecolor="#444", hatch="//", label="p99, published (4.1.2 / 2.2.0, 1 run)")
    for i in range(len(cells)):
        ax.text(x[i] - w, p50[i] * 1.1, fmt(p50[i]), ha="center", fontsize=7.5)
        ax.text(x[i], p99[i] * 1.1, fmt(p99[i]), ha="center", fontsize=7.5)
        if not math.isnan(prev[i]):
            ax.text(x[i] + w, prev[i] * 1.1, fmt(prev[i]), ha="center", fontsize=7.5, color="#444")
    ax.set_yscale("log"); ax.set_ylim(1, 3000)
    ax.set_xticks(x); ax.set_xticklabels([c.name for c in cells], fontsize=8.5)
    ax.set_ylabel("pipeline latency (ms, log)")
    ax.set_title("Stateless, EKS ~100k evt/s in, 6 partitions, equal resources, 60 s S3 checkpoints\n"
                 "Spark 4.3 async progress tracking closes the RTM tail; turning it off brings it back",
                 fontsize=10.5)
    ax.legend(fontsize=8, loc="upper left")
    save(fig, "01_stateless_eks.png", ids_of(cells) + "; published 4.1.2/2.2.0: results/eks/*-p6_summary.json (original checkout)")


# ---------- 02 stateful 2x2 ----------

def chart_2x2():
    panels = [("S1 windowed count/sum (update mode)", S1), ("S2 dedup on event_id", S2),
              ("S3 sessions with timers (transformWithState / KeyedProcessFunction)", S3)]
    fig, axes = plt.subplots(1, 3, figsize=(15, 5), sharey=True)
    for ax, (title, cells) in zip(axes, panels):
        x = np.arange(len(cells)); w = 0.38
        p50 = [c.p50 for c in cells]; p99 = [c.p99 for c in cells]
        ax.bar(x - w/2, p50, w, color=[c.color for c in cells], label="p50")
        ax.bar(x + w/2, p99, w, color=[c.color for c in cells], alpha=0.5, label="p99")
        for i in range(len(cells)):
            ax.text(x[i] - w/2, p50[i] * 1.12, fmt(p50[i]), ha="center", fontsize=7)
            ax.text(x[i] + w/2, p99[i] * 1.12, fmt(p99[i]), ha="center", fontsize=7)
        ax.set_yscale("log"); ax.set_ylim(1, 1e6)
        ax.set_xticks(x); ax.set_xticklabels([c.name for c in cells], fontsize=7.5)
        ax.set_title(title, fontsize=9.5)
    axes[0].set_ylabel("pipeline latency (ms, log)")
    axes[0].legend(fontsize=8, loc="upper right")
    axes[2].text(0.27, 0.45, "Spark: backlog, not latency\n(cannot sustain 50k evt/s)", transform=axes[2].transAxes,
                 ha="center", va="top", fontsize=8, color=C_SPARK,
                 bbox=dict(facecolor="white", edgecolor=C_SPARK, alpha=0.9))
    fig.suptitle("Stateful, EKS 50k evt/s, 1M keys, RocksDB, 60 s S3 checkpoints: both engines at defaults and both tuned\n"
                 "Spark flush = spark.shuffle.streaming.networkBufferMaxWaitTimeMs; Flink = execution.buffer-timeout; "
                 "vendor-tuned = 10 ms flush + trackTotalNumberOfRows=false", fontsize=10)
    save(fig, "02_stateful_2x2.png", ids_of(S1 + S2 + S3))


# ---------- 03 CCDF ----------

def ccdf(run):
    h = run.load("hist.json") or {}
    agg = {}
    for b in h.values():
        for k, v in b.items():
            agg[int(k)] = agg.get(int(k), 0) + v
    ks = sorted(agg); tot = sum(agg.values())
    cum = np.cumsum([agg[k] for k in ks])
    # fraction of records with latency >= bucket lower bound
    frac = [1.0] + list(1 - cum[:-1] / tot)
    return np.array(ks), np.array(frac[:len(ks)])


def chart_ccdf():
    panels = [("Stateless (100k evt/s)", [STATELESS[0], STATELESS[3], STATELESS[6]]),
              ("S1 windowed agg", [S1[0], S1[2], S1[4]]),
              ("S2 dedup", [S2[0], S2[2], S2[4]]),
              ("S3 sessions", [S3[0], S3[4]])]
    fig, axes = plt.subplots(1, 4, figsize=(16, 4.4), sharey=True)
    reps = []
    for ax, (title, cells) in zip(axes, panels):
        for c in cells:
            r = c.rep(); reps.append(r.label)
            k, f = ccdf(r)
            ax.step(np.maximum(k, 1), f, where="post", color=c.color, lw=1.8, label=f"{c.name.replace(chr(10), ' ')} ({r.label})")
        ax.set_xscale("log"); ax.set_yscale("log"); ax.set_ylim(1e-6, 1.2)
        ax.axhline(0.01, color="#999", lw=0.8, ls="--"); ax.text(1.2, 0.012, "p99", fontsize=7, color="#777")
        ax.set_xlabel("pipeline latency (ms, log)"); ax.set_title(title, fontsize=10)
        ax.legend(fontsize=6.5, loc="lower left")
    axes[0].set_ylabel("fraction of records slower than x")
    fig.suptitle("Latency distribution, one representative run per cell (median-p99 run); 1/10/100 ms histogram buckets", fontsize=10)
    save(fig, "03_latency_ccdf.png", ", ".join(reps))


# ---------- 04 checkpoint interval ----------

def chart_ckpt_interval():
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.4), sharey=True)
    ids = []
    for ax, wl in zip(axes, ("s1", "s2")):
        for eng, col in (("spark", C_SPARK), ("flink", C_FLINK)):
            d = CK[(eng, wl)]; xs = sorted(d)
            p50 = [geomean([r.p50 for r in d[i]]) for i in xs]
            p99 = [geomean([r.p99 for r in d[i]]) for i in xs]
            for i in xs:
                ids += [r.label for r in d[i]]
                NUM[f"ck_{eng}_{wl}_{i}s"] = {"p50": round(geomean([r.p50 for r in d[i]]), 1),
                                             "p99": round(geomean([r.p99 for r in d[i]]), 1),
                                             "runs": [r.label for r in d[i]]}
            lab = "Spark RTM default" if eng == "spark" else "Flink 0 ms"
            ax.plot(xs, p99, "o-", color=col, lw=2, label=f"{lab} p99")
            ax.plot(xs, p50, "s--", color=col, lw=1.3, alpha=0.7, label=f"{lab} p50")
            for xv, yv in zip(xs, p99):
                ax.text(xv, yv * 1.15, fmt(yv), ha="center", fontsize=7, color=col)
        ax.set_xscale("log"); ax.set_yscale("log")
        ax.set_xticks([5, 30, 60, 300]); ax.set_xticklabels(["5 s", "30 s", "60 s", "300 s"])
        ax.set_xlabel("checkpoint interval (Flink) = RTM trigger (Spark)")
        ax.set_title({"s1": "S1 windowed agg", "s2": "S2 dedup"}[wl], fontsize=10)
        ax.legend(fontsize=7.5)
    axes[0].set_ylabel("pipeline latency (ms, log)")
    fig.suptitle("Checkpoint interval trade-off, 50k evt/s. Spark: short batches cost the median, long ones the tail. "
                 "Flink: p50 flat.\n300 s cells measured over 720 s; 5/30/300 s cells are 2 runs each", fontsize=10)
    save(fig, "04_checkpoint_interval.png", ", ".join(sorted(set(ids))),
         src="Sessions A/B (not re-run in the final run; 2 runs per 5/30/300 s cell)")


# ---------- 05 recovery ----------

def chart_recovery():
    fig, axes = plt.subplots(2, 1, figsize=(10, 6.4), sharex=True)
    ids = []
    for eng, col in (("spark", C_SPARK), ("flink", C_FLINK)):
        for i, r in enumerate(KILL[eng]):
            ids.append(r.label)
            rate = r.load("rate.json")
            kill = float(r.text("kill.txt").split()[0])
            rec = next(x for x in CELLS[re.sub(r"-r\d+$", "", r.label)]["runs"] if x["run"] == r.label)
            rec = {k: rec.get(k) for k in ("outage_s", "recover_s", "settled_windows", "lost", "overcount")}
            have = sorted(int(s) for s in rate)
            secs = list(range(have[0], have[-1] + 1))   # a second with no output = 0 records
            t = np.array([s - kill for s in secs])
            n = np.array([rate.get(str(s), [0, 0])[0] for s in secs])
            lat = np.array([rate[str(s)][1] / rate[str(s)][0] if rate.get(str(s), [0])[0] else np.nan
                            for s in secs])
            m = (t >= -60) & (t <= 240)
            lab = (f"{'Spark RTM (executor killed)' if eng == 'spark' else 'Flink (TaskManager killed)'}" if i == 0 else None)
            axes[0].plot(t[m], n[m] / 1000, color=col, lw=1.1, alpha=0.85, label=lab)
            axes[1].plot(t[m], np.maximum(lat[m], 1), color=col, lw=1.1, alpha=0.85, label=lab)
            NUM[f"kill_{r.label}"] = rec
    for ax in axes:
        ax.axvline(0, color="#000", lw=1, ls=":")
    axes[0].text(1, axes[0].get_ylim()[1] * 0.9, "kill", fontsize=8)
    axes[0].set_ylabel("output records/s (k)"); axes[0].legend(fontsize=8)
    axes[1].set_yscale("log"); axes[1].set_ylabel("mean e2e latency per second (ms, log)")
    axes[1].set_xlabel("seconds since one worker pod was deleted (S1, 50k evt/s, at load + 180 s)")
    fig.suptitle("Recovery: Flink restarts from the checkpoint in 9-10 s and is back in 62-70 s; Spark RTM output stops "
                 "90-93 s and is not back by the end of the run\n2 runs per engine overlaid (n=2); at-least-once sinks on "
                 "both; 0 lost / 0 overcounted windows on both", fontsize=10)
    save(fig, "05_recovery_timeline.png", ", ".join(ids))


# ---------- 06 resources per pod ----------

def pod_stats(r):
    per = {}
    for line in r.text("top.txt").splitlines():
        f = line.split()
        if len(f) < 5 or "operator" in f[2]:
            continue
        ts = int(f[0])
        if not (r.t0 <= ts <= r.t1):
            continue
        try:
            mc = int(f[3].rstrip("m")); mi = int(re.sub(r"[^0-9]", "", f[4]))
        except ValueError:
            continue
        role = "worker" if ("exec" in f[2] or "taskmanager" in f[2]) else "coordinator"
        per.setdefault((role, f[2]), []).append((mc / 1000, mi / 1024))
    out = {}
    for role in ("coordinator", "worker"):
        pods = [v for (ro, _), v in per.items() if ro == role]
        if not pods:
            continue
        out[role] = {"cpu_mean": st.mean(st.mean(c for c, _ in p) for p in pods),
                     "cpu_peak": max(max(c for c, _ in p) for p in pods),
                     "mem_mean": st.mean(st.mean(m for _, m in p) for p in pods),
                     "mem_peak": max(max(m for _, m in p) for p in pods), "pods": len(pods)}
    return out


def cell_pods(c):
    agg = {}
    for r in c.runs:
        for role, v in pod_stats(r).items():
            agg.setdefault(role, []).append(v)
    res = {role: {k: (max if "peak" in k else st.mean)(x[k] for x in vs) for k in vs[0] if k != "pods"}
           for role, vs in agg.items()}
    NUM[f"res_{c.key}"] = {role: {k: round(v, 2) for k, v in d.items()} for role, d in res.items()}
    return res


def chart_resources():
    cells = [STATELESS[0], STATELESS[3], S1[4], S1[0], S1[2], S2[4], S2[0], S2[2], S3[4], S3[0]]
    groups = ["stateless"] * 2 + ["S1"] * 3 + ["S2"] * 3 + ["S3"] * 2
    data = [cell_pods(c) for c in cells]
    labels = [f"{g}\n{c.name}" for g, c in zip(groups, cells)]
    fig, axes = plt.subplots(2, 1, figsize=(14, 7.5), sharex=True)
    x = np.arange(len(cells)); w = 0.2
    for ax, met, unit, lim in ((axes[0], "cpu", "cores", 3), (axes[1], "mem", "GiB", 6)):
        for j, (role, alpha) in enumerate((("worker", 1.0), ("coordinator", 0.45))):
            mean = [d.get(role, {}).get(f"{met}_mean", 0) for d in data]
            peak = [d.get(role, {}).get(f"{met}_peak", 0) for d in data]
            off = (j * 2 - 1.5) * w
            ax.bar(x + off, mean, w, color=[c.color for c in cells], alpha=alpha, label=f"{role} pod, mean" if met == "cpu" else None)
            ax.bar(x + off + w, peak, w, color="none", edgecolor=[c.color for c in cells], hatch="..",
                   label=f"{role} pod, peak" if met == "cpu" else None)
            for i in range(len(cells)):
                ax.text(x[i] + off, mean[i] + lim * 0.01, f"{mean[i]:.1f}", ha="center", fontsize=6)
                ax.text(x[i] + off + w, peak[i] + lim * 0.01, f"{peak[i]:.1f}", ha="center", fontsize=6)
        ax.axhline(lim, color="#c00", lw=0.8, ls="--")
        ax.text(len(cells) - 0.5, lim * 1.01, f"worker pod limit {lim} {unit}", ha="right", fontsize=7, color="#c00")
        ax.set_ylabel(f"{met.upper()} per pod ({unit})")
    axes[0].legend(fontsize=7, ncol=4, loc="upper left")
    axes[1].set_xticks(x); axes[1].set_xticklabels(labels, fontsize=7)
    fig.suptitle("Per-pod CPU and memory (kubectl top, ~5 s samples, measured window only; 2 worker pods + 1 coordinator per engine)\n"
                 "Spark worker = executor pod, Flink worker = TaskManager pod; coordinator = driver / JobManager", fontsize=10)
    save(fig, "06_pod_resources.png", ids_of(cells))


# ---------- 07 checkpoints ----------

SPARK_COMMIT = re.compile(r"^(\d\d/\d\d/\d\d \d\d:\d\d:\d\d) INFO MicroBatchExecution: Committed offsets for batch (\d+)", re.M)


def spark_batch_overrun(r, trigger_s=60):
    ts = [datetime.strptime(m.group(1), "%y/%m/%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
          for m in SPARK_COMMIT.finditer(r.text("evidence.txt"))]
    # only batches that end inside the measured window: the final run's driver log also holds the
    # idle batches before the load starts, which run exactly at the trigger
    return [b - a - trigger_s for a, b in zip(ts, ts[1:]) if r.t0 <= b <= r.t1 + trigger_s]


def flink_ckpts(r):
    c = r.load("checkpoints.json") or {}
    h = [x for x in c.get("history", []) if x.get("status") == "COMPLETED"]
    return ([x["end_to_end_duration"] / 1000 for x in h], [x["state_size"] / 2**20 for x in h],
            [x["checkpointed_size"] / 2**20 for x in h])


def chart_checkpoints():
    fl = [("stateless", STATELESS[0]), ("S1", S1[4]), ("S2", S2[4]), ("S3", S3[4])]
    sp = [("S1", S1[0]), ("S2", S2[0]), ("S3", S3[0]), ("S1 vendor", S1[2]), ("S2 vendor", S2[2])]
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.4))
    ids = []
    dur, full, inc = [], [], []
    for _, c in fl:
        d, s, i = [], [], []
        for r in c.runs:
            a, b, k = flink_ckpts(r); d += a; s += b; i += k; ids.append(r.label)
        dur.append(d); full.append(s); inc.append(i)
        NUM[f"ckpt_{c.key}"] = {"n_ckpts": len(d), "dur_s_median": round(st.median(d), 2) if d else None,
                                "dur_s_max": round(max(d), 2) if d else None,
                                "full_mib_median": round(st.median(s), 1) if s else None,
                                "incr_mib_median": round(st.median(i), 1) if i else None}
    labs = [f"{g}\n{c.name.split(chr(10))[0]}" for g, c in fl]
    axes[0].boxplot(dur, tick_labels=labs, showfliers=True)
    axes[0].set_ylabel("seconds"); axes[0].set_title("Flink 0 ms: checkpoint end-to-end duration", fontsize=10)
    x = np.arange(len(fl)); w = 0.38
    axes[1].bar(x - w/2, [st.median(s) if s else 0 for s in full], w, color=C_FLINK, label="full state (state_size)")
    axes[1].bar(x + w/2, [st.median(s) if s else 0 for s in inc], w, color=C_FLINK2, label="incremental upload (checkpointed_size)")
    axes[1].set_xticks(x); axes[1].set_xticklabels(labs, fontsize=8)
    axes[1].set_yscale("symlog", linthresh=1); axes[1].set_ylabel("MiB per checkpoint (median, symlog)")
    axes[1].set_title("Flink 0 ms: checkpoint size", fontsize=10); axes[1].legend(fontsize=7)
    ov = []
    for _, c in sp:
        o = []
        for r in c.runs:
            o += spark_batch_overrun(r); ids.append(r.label)
        ov.append(o)
        NUM[f"overrun_{c.key}"] = {"n_batches": len(o), "median_s": round(st.median(o), 1) if o else None,
                                   "max_s": round(max(o), 1) if o else None}
    axes[2].boxplot([o if o else [np.nan] for o in ov], tick_labels=[g for g, _ in sp])
    axes[2].set_ylabel("seconds beyond the 60 s trigger")
    axes[2].set_title("Spark RTM: batch commit-to-commit minus trigger\n(the synchronous boundary pause; driver log)", fontsize=10)
    fig.suptitle("Checkpoint cost. Spark state-store checkpoint size was not collected (open question)", fontsize=10)
    save(fig, "07_checkpoints.png", ", ".join(sorted(set(ids))))


# ---------- 00 hero ----------

def chart_hero():
    groups = ["Stateless\n(100k evt/s)", "S1 windowed agg\n(50k evt/s)", "S2 dedup\n(50k evt/s)"]
    spark41 = [PREV_P99["sl_rtm_java"], np.nan, np.nan]
    spark = [STATELESS[3].p99, S1[2].p99, S2[2].p99]
    flink = [STATELESS[0].p99, S1[4].p99, S2[4].p99]
    fig, ax = plt.subplots(figsize=(9, 4.6))
    x = np.arange(3); w = 0.27
    ax.bar(x - w, spark41, w, color="none", edgecolor=C_SPARK, hatch="//", label="Spark 4.1.2 RTM (published)")
    ax.bar(x, spark, w, color=C_SPARK, label="Spark 4.3.0 RTM (stateful: vendor-tuned)")
    ax.bar(x + w, flink, w, color=C_FLINK, label="Flink 2.3.0 (tuned, 0 ms)")
    for xs, vs in ((x - w, spark41), (x, spark), (x + w, flink)):
        for xv, v in zip(xs, vs):
            if not math.isnan(v):
                ax.text(xv, v * 1.12, fmt(v), ha="center", fontsize=8.5)
    ax.set_yscale("log"); ax.set_ylim(5, 30000)
    ax.set_xticks(x); ax.set_xticklabels(groups)
    ax.set_ylabel("pipeline p99 (ms, log)")
    ax.set_title("Spark 4.3 fixed the stateless RTM tail. Stateful RTM still commits synchronously.", fontsize=11.5)
    ax.legend(fontsize=8, loc="upper left")
    save(fig, "00_hero.png", ids_of([STATELESS[3], STATELESS[0], S1[2], S1[4], S2[2], S2[4]]))


# ---------- 08-09 resources and cost ----------

# On-demand Linux m5.2xlarge, us-east-1: $0.384/h, 8 vCPU, 32 GiB (instances.vantage.sh and
# instances.usage.ai, read 2026-11-10; re-check aws.amazon.com/ec2/pricing/on-demand before publishing).
NODE_USD_H, NODE_VCPU, NODE_GIB = 0.384, 8, 32
ENGINE_NODES = 2   # stateful engines ran on 2 dedicated m5.2xlarge nodes


def engine_totals(r):
    """Per-sample totals over the engine's own pods (operator excluded), measured window only,
    keeping samples where every engine pod reported."""
    per = {}
    for line in r.text("top.txt").splitlines():
        f = line.split()
        if len(f) < 5 or "operator" in f[2]:
            continue
        ts = int(f[0])
        if not (r.t0 <= ts <= r.t1):
            continue
        try:
            mc = int(f[3].rstrip("m")); mi = int(re.sub(r"[^0-9]", "", f[4]))
        except ValueError:
            continue
        p = per.setdefault(ts, [0, 0, 0]); p[0] += mc / 1000; p[1] += mi / 1024; p[2] += 1
    full = max((v[2] for v in per.values()), default=0)
    return [(ts, v[0], v[1]) for ts, v in sorted(per.items()) if v[2] == full]


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(math.ceil(q * len(xs))) - 1)] if xs else None


# (group, cell, input rate evt/s, held the rate at its latency?)
COST_CELLS = [("stateless", STATELESS[0], 100_000, True), ("stateless", STATELESS[3], 100_000, True),
              ("S1", S1[4], 50_000, True), ("S1", S1[3], 50_000, True), ("S1", S1[0], 50_000, True),
              ("S1", S1[1], 50_000, False), ("S1", S1[2], 50_000, True),
              ("S2", S2[4], 50_000, True), ("S2", S2[3], 50_000, True), ("S2", S2[0], 50_000, True),
              ("S2", S2[1], 50_000, True), ("S2", S2[2], 50_000, True),
              ("S3", S3[4], 50_000, True), ("S3", S3[3], 50_000, True), ("S3", S3[0], 50_000, False),
              ("S3", S3[1], 50_000, False), ("S3", S3[2], 50_000, False)]


def cell_label(c):
    return re.sub(r"-r\d+$", "", c.ids[0])


def cost_rows():
    """Resources from the final run's sampler (results/final-run/cells.json): kubelet cAdvisor CPU and
    working set (engine pods summed; ~14 s effective resolution), CFS throttling, JVM heap, state memory,
    checkpoint bytes and objects. Mean over runs; peaks are the max over runs."""
    rows = []
    for grp, c, rate, held in COST_CELLS:
        lab = cell_label(c); cj = CELLS[lab]; a = cj["agg"]
        g = lambda k, i=0: (a[k][i] if a.get(k) else None)
        row = {"group": grp, "key": c.key, "label": lab, "n": cj["n"], "name": c.name.replace("\n", " "),
               "rate": rate, "held": held, "p99_ms": c.p99,
               "cpu_mean": g("cpu_mean"), "cpu_peak": max(r["cpu_peak"] for r in cj["runs"]),
               "mem_mean": g("ws_mean"), "mem_peak": max(r["ws_peak"] for r in cj["runs"]),
               "throttle_pct": g("throttle_pct"), "heap_peak": max(r["heap_peak"] for r in cj["runs"]),
               "state_mb": g("rocks_mem_peak_mb") if a.get("rocks_mem_peak_mb") else g("spark_state_mem_peak_mb"),
               "ckpt_mb": g("ckpt_mb"), "s3_puts": g("s3_puts")}
        k = rate / 10_000
        row.update({"cpu_mean_10k": row["cpu_mean"] / k, "cpu_peak_10k": row["cpu_peak"] / k,
                    "mem_mean_10k": row["mem_mean"] / k, "mem_peak_10k": row["mem_peak"] / k})
        mev_h = rate * 3600 / 1e6
        row["usd_per_mev_used"] = row["cpu_mean"] * NODE_USD_H / NODE_VCPU / mev_h
        # node-equivalents: the larger of peak CPU (14 s average) and peak working set, as node fractions
        row["node_eq"] = max(row["cpu_peak"] / NODE_VCPU, row["mem_peak"] / NODE_GIB)
        row["min_nodes"] = math.ceil(row["node_eq"]) if held else None
        # EXTRAPOLATED: linear CPU scaling to a full node from the peak, only for cells that held their rate
        row["evt_s_per_node_extrap"] = rate * NODE_VCPU / row["cpu_peak"] if held else None
        row["usd_per_mev_extrap"] = (NODE_USD_H / (row["evt_s_per_node_extrap"] * 3600 / 1e6)) if held else None
        rows.append(row)
        NUM[f"cost_{c.key}"] = {k2: (round(v, 5) if isinstance(v, float) else v) for k2, v in row.items()}
    NUM["cost_assumptions"] = {"node_usd_h": NODE_USD_H, "node_vcpu": NODE_VCPU, "node_gib": NODE_GIB,
                               "engine_nodes": ENGINE_NODES,
                               "engine_pool_usd_per_mev_at_50k": ENGINE_NODES * NODE_USD_H / (50_000 * 3600 / 1e6),
                               "price_source": "instances.vantage.sh / instances.usage.ai m5.2xlarge, us-east-1 "
                                               "on-demand Linux, read 2026-11-10"}
    return rows


def chart_cost_resources(rows):
    fig, axes = plt.subplots(2, 2, figsize=(16, 9.5), sharex=True)
    x = np.arange(len(rows)); w = 0.4
    cols = [next(c.color for _, c, _, _ in COST_CELLS if c.key == r["key"]) for r in rows]
    for ax, met, unit in ((axes[0][0], "cpu", "cores"), (axes[0][1], "mem", "GiB working set")):
        mean = [r[f"{met}_mean_10k"] for r in rows]; peak = [r[f"{met}_peak_10k"] for r in rows]
        ax.bar(x - w/2, mean, w, color=cols, label="mean")
        ax.bar(x + w/2, peak, w, color="none", edgecolor=cols, hatch="..", label="peak (14 s average)")
        for i, r in enumerate(rows):
            ax.text(x[i] - w/2, mean[i] * 1.01, f"{mean[i]:.2f}", ha="center", fontsize=6)
            if not r["held"]:
                ax.text(x[i], peak[i] * 1.05, "did not\nhold rate", ha="center", fontsize=5.5, color="#c00")
        ax.set_ylabel(f"{unit} per 10k input evt/s"); ax.set_title(f"{met.upper()} per 10k evt/s", fontsize=10)
        ax.legend(fontsize=8)
    ax = axes[1][0]
    ax.bar(x, [r["throttle_pct"] or 0 for r in rows], 0.6, color=cols)
    for i, r in enumerate(rows):
        ax.text(x[i], (r["throttle_pct"] or 0) + 1, "-" if r["throttle_pct"] is None else f"{r['throttle_pct']:.0f}",
                ha="center", fontsize=6.5)
    ax.set_ylabel("CFS throttled periods, %"); ax.set_title("CPU throttling (3-core worker limit)", fontsize=10)
    ax = axes[1][1]
    ax.bar(x - w/2, [r["heap_peak"] for r in rows], w, color=cols, label="JVM heap peak, GiB (left)")
    ax2 = ax.twinx()
    ax2.bar(x + w/2, [(r["state_mb"] or 0) / 1024 for r in rows], w, color="none", edgecolor=cols, hatch="//",
            label="state memory peak, GiB (right): Flink RocksDB native, Spark state store")
    ax.set_ylabel("JVM heap peak, GiB"); ax2.set_ylabel("state memory peak, GiB")
    ax.set_title("JVM heap and state memory (not the same quantity per engine)", fontsize=10)
    ax.legend(fontsize=7, loc="upper left"); ax2.legend(fontsize=7, loc="upper right")
    for a_ in axes[1]:
        a_.set_xticks(x); a_.set_xticklabels([f"{r['group']}\n{r['name']}" + (" (n=2)" if r["n"] == 2 else "")
                                              for r in rows], fontsize=6, rotation=90)
    fig.suptitle("Engine pods, final run: kubelet cAdvisor CPU and working set (2 s scrape, ~14 s kubelet refresh), "
                 "CFS throttling, JVM heap, state memory\nStateless at 100k evt/s, stateful at 50k; all engine pods summed",
                 fontsize=10)
    save(fig, "08_resources_per_10k.png", ids_of([c for _, c, _, _ in COST_CELLS]))


def chart_cpu_timeline():
    pairs = [("stateless", STATELESS[3].rep(), STATELESS[0].rep()), ("S1", S1[0].rep(), S1[4].rep())]
    fig, axes = plt.subplots(2, 2, figsize=(14, 6.4), sharey="row")
    ids = []
    for row, (grp, sp, fl) in enumerate(pairs):
        for col, (r, eng) in enumerate(((sp, "spark"), (fl, "flink"))):
            ax = axes[row][col]; ids.append(r.label)
            per = {}
            for line in r.text("top.txt").splitlines():
                f = line.split()
                if len(f) < 5 or "operator" in f[2] or not (r.t0 <= int(f[0]) <= r.t1):
                    continue
                if not ("exec" in f[2] or "taskmanager" in f[2]):
                    continue
                try:
                    per.setdefault(f[2], []).append((int(f[0]) - r.t0, int(f[3].rstrip("m")) / 1000))
                except ValueError:
                    continue
            col_ = C_SPARK if eng == "spark" else C_FLINK
            for i, (pod, pts) in enumerate(sorted(per.items())):
                t, v = zip(*pts)
                ax.step(t, v, where="post", color=col_, alpha=1 if i == 0 else 0.5, lw=1.4,
                        label=f"worker pod {i + 1}")
            if eng == "spark":
                ev = r.text("evidence.txt")
                ts = [datetime.strptime(m.group(1), "%y/%m/%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
                      for m in SPARK_COMMIT.finditer(ev)]
                if not ts:  # stateless runs: S3 LastModified of each offsets/<n> file instead of driver log lines
                    ts = [datetime.fromisoformat(m.group(1)).timestamp()
                          for m in re.finditer(r"^(\S+\+00:00)\t\S+/offsets/\d+$", ev, re.M)]
                marks, lab = [t - r.t0 for t in ts if r.t0 <= t <= r.t1], "batch commit (boundary)"
            else:
                c = r.load("checkpoints.json") or {}
                marks = [h["trigger_timestamp"] / 1000 - r.t0 for h in c.get("history", [])
                         if h.get("status") == "COMPLETED" and r.t0 <= h["trigger_timestamp"] / 1000 <= r.t1]
                lab = "checkpoint trigger"
            for j, m in enumerate(marks):
                ax.axvline(m, color="#333", ls=":", lw=1, label=lab if j == 0 else None)
            ax.axhline(3, color="#c00", lw=0.8, ls="--")
            ax.set_title(f"{grp}: {r.label}", fontsize=9)
            ax.legend(fontsize=7, loc="upper right")
            if col == 0:
                ax.set_ylabel("CPU per worker pod (cores)")
            if row == 1:
                ax.set_xlabel("seconds into the measured window")
    fig.suptitle("CPU over time, one run per engine (worker pods; limit 3 cores dashed). Samples ~5 s, metrics-server "
                 "updates ~15 s, so short spikes are smeared", fontsize=10)
    save(fig, "09_cpu_timeline.png", ", ".join(ids))


def cost_table_md(rows):
    out = ["| workload | arm | n | p99 ms | CPU mean / peak | WS GiB mean / peak | throttle % | heap peak GiB | "
           "state mem MB | ckpt MB | S3 PUTs | $ per M events, CPU used | node-eq (min nodes) | "
           "EXTRAPOLATED evt/s per node | EXTRAPOLATED $ per M events |",
           "|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|"]
    for r in rows:
        held = r["held"]
        per_node = "{:,.0f}".format(r["evt_s_per_node_extrap"]) if held else "n/a: did not hold rate"
        usd_ex = "{:.5f}".format(r["usd_per_mev_extrap"]) if held else "n/a"
        nodes = r["min_nodes"] if held else "n/a"
        thr = "-" if r["throttle_pct"] is None else f"{r['throttle_pct']:.1f}"
        sm = "-" if r["state_mb"] is None else f"{r['state_mb']:,.0f}"
        out.append(f"| {r['group']} | {r['name']} | {r['n']} | {fmt(r['p99_ms'])} | {r['cpu_mean']:.2f} / {r['cpu_peak']:.2f} | "
                   f"{r['mem_mean']:.1f} / {r['mem_peak']:.1f} | {thr} | {r['heap_peak']:.1f} | {sm} | "
                   f"{r['ckpt_mb']:,.0f} | {r['s3_puts']:,.0f} | {r['usd_per_mev_used']:.5f} | {r['node_eq']:.2f} ({nodes}) | "
                   f"{per_node} | {usd_ex} |")
    with open(os.path.join(OUT, "cost_table.md"), "w") as fh:
        fh.write("\n".join(out) + "\n")
    print("wrote", os.path.join(OUT, "cost_table.md"))


# ---------- 10-11 joins (final run) ----------

S4 = [Cell("s4_spark", "Spark RTM\nbroadcast join", C_SPARK, runs(FA, "spark-rtm-s4", R3)),
      Cell("s4_flink_def", "Flink map\n100 ms", C_FLINK2, runs(FA, "flink-ds-bt100-s4", R3)),
      Cell("s4_flink_0", "Flink map\n0 ms", C_FLINK, runs(FA, "flink-ds-s4", R3))]
# S5 at S5_RATE=1000/pod (~8k orders/s + ~6.8k payments/s), n=2; O7 at 2000/pod, n=1
S5 = [Cell("s5_spark_mb", "Spark micro-batch\ndefault trigger", C_MB, runs(FB, "spark-mb-s5-at8k", R2)),
      Cell("s5_spark_mb1s", "Spark micro-batch\n1 s trigger", C_MB, runs(FB, "spark-mb1s-s5-at8k", R2)),
      Cell("s5_flink_def", "Flink intervalJoin\n100 ms", C_FLINK2, runs(FB, "flink-ds-bt100-s5-at8k", R2)),
      Cell("s5_flink_0", "Flink intervalJoin\n0 ms", C_FLINK, runs(FB, "flink-ds-s5-at8k", R2))]
S5_16 = [Cell("s5x2_spark_mb", "Spark micro-batch\ndefault trigger", C_MB, runs(FA, "spark-mb-s5-at16k", (1,))),
         Cell("s5x2_spark_mb1s", "Spark micro-batch\n1 s trigger", C_MB, runs(FA, "spark-mb1s-s5-at16k", (1,))),
         Cell("s5x2_flink_def", "Flink intervalJoin\n100 ms", C_FLINK2, runs(FA, "flink-ds-bt100-s5-at16k", (1,))),
         Cell("s5x2_flink_0", "Flink intervalJoin\n0 ms", C_FLINK, runs(FA, "flink-ds-s5-at16k", (1,)))]


def cj_res(c):
    cj = CELLS[cell_label(c)]
    return (cj["agg"]["cpu_mean"][0], max(r["cpu_peak"] for r in cj["runs"]),
            cj["agg"]["ws_mean"][0], max(r["ws_peak"] for r in cj["runs"]))


def chart_s4():
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.6))
    x = np.arange(len(S4)); w = 0.38
    p50 = [c.p50 for c in S4]; p99 = [c.p99 for c in S4]
    ax = axes[0]
    ax.bar(x - w/2, p50, w, color=[c.color for c in S4], label="p50")
    ax.bar(x + w/2, p99, w, color=[c.color for c in S4], alpha=0.5, label="p99")
    for i in range(len(S4)):
        ax.text(x[i] - w/2, p50[i] * 1.1, fmt(p50[i]), ha="center", fontsize=8)
        ax.text(x[i] + w/2, p99[i] * 1.1, fmt(p99[i]), ha="center", fontsize=8)
    ax.set_yscale("log"); ax.set_ylim(1, 3000); ax.set_ylabel("pipeline latency (ms, log)")
    ax.set_xticks(x); ax.set_xticklabels([c.name for c in S4], fontsize=8); ax.legend(fontsize=8)
    ax.set_title("latency", fontsize=10)
    res = [cj_res(c) for c in S4]
    for ax, (mi, pi), unit in ((axes[1], (0, 1), "cores"), (axes[2], (2, 3), "GiB working set")):
        mean = [r[mi] for r in res]; peak = [r[pi] for r in res]
        NUM.update({f"res_{c.key}_{unit.split()[0]}": [round(m, 2), round(p_, 2)] for c, m, p_ in zip(S4, mean, peak)})
        ax.bar(x - w/2, mean, w, color=[c.color for c in S4], label="mean")
        ax.bar(x + w/2, peak, w, color="none", edgecolor=[c.color for c in S4], hatch="..", label="peak (14 s avg)")
        for i in range(len(S4)):
            ax.text(x[i] - w/2, mean[i] * 1.02, f"{mean[i]:.1f}", ha="center", fontsize=8)
            ax.text(x[i] + w/2, peak[i] * 1.02, f"{peak[i]:.1f}", ha="center", fontsize=8)
        ax.set_xticks(x); ax.set_xticklabels([c.name for c in S4], fontsize=8)
        ax.set_ylabel(f"engine pods, {unit}"); ax.set_title(unit.split()[0].upper() + " (engine pods summed, cAdvisor)", fontsize=10)
        ax.legend(fontsize=8)
    fig.suptitle("S4 stream-static enrichment join, EKS 50k evt/s, 980k-row reference table: same median, "
                 "Spark's tail at the batch boundary", fontsize=10)
    save(fig, "10_s4_broadcast_join.png", ids_of(S4))


def chart_s5():
    fig, axes = plt.subplots(1, 2, figsize=(14, 4.8), sharey=True)
    for ax, cells, title in ((axes[0], S5, "S5_RATE=1000/pod: ~8k orders/s + ~6.8k payments/s (n=2)"),
                             (axes[1], S5_16, "S5_RATE=2000/pod: ~16k orders/s + ~13.6k payments/s (n=1)")):
        x = np.arange(len(cells)); w = 0.38
        p50 = [c.p50 for c in cells]; p99 = [c.p99 for c in cells]
        ax.bar(x - w/2, p50, w, color=[c.color for c in cells], label="p50")
        ax.bar(x + w/2, p99, w, color=[c.color for c in cells], alpha=0.5, label="p99")
        for i in range(len(cells)):
            ax.text(x[i] - w/2, p50[i] * 1.12, fmt(p50[i]), ha="center", fontsize=8)
            ax.text(x[i] + w/2, p99[i] * 1.12, fmt(p99[i]), ha="center", fontsize=8)
        ax.set_yscale("log"); ax.set_ylim(1, 1e5)
        ax.set_xticks(x); ax.set_xticklabels([c.name for c in cells], fontsize=8)
        ax.set_title(title, fontsize=9.5); ax.legend(fontsize=8)
    axes[0].set_ylabel("pipeline latency (ms, log)")
    fig.suptitle("S5 stream-stream interval join (orders x payments, 10 s bound), unsaturated rates.\n"
                 "Not RTM: OSS Spark 4.3 RTM rejects stream-stream joins, so Spark runs micro-batch", fontsize=10)
    save(fig, "11_s5_interval_join.png", ids_of(S5 + S5_16))


# ---------- 12-13 scaling under a load step (session D) ----------

DR = os.path.join(ROOT, "results", "eks-session-d", "runs")
DR2 = os.path.join(ROOT, "results", "eks-session-d2", "runs")
SCALE_ARMS = [  # run dir, display, colour, line style
    (os.path.join(DR, "flink-peak-s1-update-steps-r1"), "Flink static peak (p=12, 4 TMs)", C_FLINK, "-"),
    (os.path.join(DR, "flink-auto-s1-update-steps-r1"), "Flink operator autoscaler", C_FLINK2, "-"),
    (os.path.join(DR2, "flink-reactive-s1-update-steps-r1"), "Flink Reactive + HPA", "#2A9D8F", "-"),
    (os.path.join(DR, "spark-rtm-peak-s1-steps-r1"), "Spark RTM static peak (4 executors)", C_SPARK, "-"),
    (os.path.join(DR, "spark-rtm-manual-s1-steps-r1"), "Spark RTM manual restart per step", C_SPARK3, "-"),
    (os.path.join(DR, "spark-mb-da-s1-steps-r1"), "Spark micro-batch + dynamic allocation", C_MB, "--"),
]
SCALEIN_ARMS = [
    (os.path.join(DR2, "flink-auto-s1-update-scalein-r1"), "Flink operator autoscaler", C_FLINK2, "-"),
    (os.path.join(DR2, "flink-reactive-s1-update-scalein-r1"), "Flink Reactive + HPA", "#2A9D8F", "-"),
]


def scale_series(d):
    t0 = int(float(open(os.path.join(d, "load_start.txt")).read().split()[0]))
    steps = sorted({round(float(l.split()[1]) - t0) for l in open(os.path.join(d, "steps.txt")) if l.startswith("STEP")})
    h = json.load(open(os.path.join(d, "hist.json")))
    win = {}
    for sec, b in h.items():  # 30 s windows, p99 from the 1/10/100 ms buckets
        w = (int(sec) - t0) // 30 * 30
        acc = win.setdefault(w, {})
        for k, v in b.items():
            acc[int(k)] = acc.get(int(k), 0) + v
    ts = sorted(win)
    p99 = [hist_pct(win[w], .99) for w in ts]
    workers, wt = [], []
    for line in open(os.path.join(d, "scaling.txt")):
        m = re.search(r"running=(\d+)", line)
        if m:
            wt.append(int(line.split()[0]) - t0); workers.append(int(m.group(1)))
    return steps, np.array(ts) + 15, np.array(p99, dtype=float), np.array(wt), np.array(workers)


def chart_scaling_timeline():
    fig, axes = plt.subplots(2, 1, figsize=(13, 7.5), sharex=True)
    steps = None
    for label, name, col, ls in SCALE_ARMS:
        steps, t, p99, wt, wk = scale_series(label)
        axes[0].plot(t / 60, np.maximum(p99, 1), color=col, ls=ls, lw=1.6, label=name)
        axes[1].step(wt / 60, wk, where="post", color=col, ls=ls, lw=1.6, label=name)
    for ax in axes:
        for s in steps[1:]:
            ax.axvline(s / 60, color="#000", ls=":", lw=1)
    ymax = 4e5
    for (a, b), lab in zip(zip(steps, steps[1:] + [steps[-1] + 1220]), ["50k evt/s", "100k evt/s", "50k evt/s"]):
        axes[0].text((a + b) / 120, ymax * 0.5, lab, ha="center", fontsize=9)
    axes[0].axhline(2000, color="#c00", lw=0.8, ls="--"); axes[0].text(0.3, 2300, "2 s", color="#c00", fontsize=8)
    axes[0].set_yscale("log"); axes[0].set_ylim(5, ymax)
    axes[0].set_ylabel("pipeline p99 per 30 s (ms, log)"); axes[0].legend(fontsize=7.5, loc="lower right", ncol=2)
    axes[1].set_ylabel("running worker pods\n(TaskManagers / executors)")
    axes[1].set_xlabel("minutes since load start (first 60 s = warm-up, not measured)")
    fig.suptitle("Scaling under a load step, S1 on EKS: 50k -> 100k -> 50k evt/s, 1 run per arm shown\n"
                 "Engine nodes autoscaled 1..5 by Cluster Autoscaler; worker pod 3 CPU / 6 GiB everywhere", fontsize=10)
    save(fig, "12_scaling_timeline.png", ", ".join(os.path.basename(l) for l, *_ in SCALE_ARMS),
         src="Sessions D/D2 (not re-run; 1-2 runs per arm)")


def chart_scaling_summary():
    rows = {r["run"]: r for sess in ("eks-session-d", "eks-session-d2")
            for r in json.load(open(os.path.join(ROOT, "results", sess, "scaling.json")))}
    # D2 rows carry no ratio: compute it against session D's flink-peak run (same profile)
    peak_nh = rows["flink-peak-s1-update-steps-r1"]["node_h"]
    for r in rows.values():
        if r.get("node_h_vs_peak") in (None, "-") and "-steps-" in r["run"]:
            r["node_h_vs_peak"] = r["node_h"] / peak_nh
    arms = [("flink-peak-s1-update-steps", "Flink\nstatic peak", C_FLINK, (1,)),
            ("flink-auto-s1-update-steps", "Flink\nautoscaler", C_FLINK2, (1, 2)),
            ("flink-reactive-s1-update-steps", "Flink Reactive\n+ HPA", "#2A9D8F", (1, 2)),
            ("spark-rtm-peak-s1-steps", "Spark RTM\nstatic peak", C_SPARK, (1,)),
            ("spark-rtm-manual-s1-steps", "Spark RTM\nmanual restart", C_SPARK3, (1, 2)),
            ("spark-mb-da-s1-steps", "Spark micro-batch\n+ dyn. allocation", C_MB, (1, 2))]
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.6))
    x = np.arange(len(arms)); ids = []
    vals = {"p99": [], "spike": [], "nodeh": []}
    for arm, _, _, ns in arms:
        rs = [rows[f"{arm}-r{n}"] for n in ns]; ids += [r["run"] for r in rs]
        vals["p99"].append(st.mean(r["phases"][1]["p99_ms"] for r in rs) / 1000)
        vals["spike"].append(max(r["phases"][1]["p99_max_ms"] for r in rs) / 1000)
        vals["nodeh"].append(st.mean(r["node_h_vs_peak"] for r in rs))
        NUM[f"scale_{arm}"] = {"runs": [r["run"] for r in rs], "phase2_p99_s": round(vals["p99"][-1], 2),
                               "phase2_worst_1s_p99_s": round(vals["spike"][-1], 1),
                               "node_h_vs_peak": round(vals["nodeh"][-1], 2),
                               "lost": sum(r["lost"] for r in rs), "overcount": sum(r["overcount"] for r in rs)}
    cols = [c for _, _, c, _ in arms]; names = [n for _, n, _, _ in arms]
    for ax, key, lab, logy in ((axes[0], "p99", "phase 2 (100k) p99, s", True),
                               (axes[1], "spike", "phase 2 worst 1 s p99, s", True),
                               (axes[2], "nodeh", "engine node-hours vs the same engine's static peak", False)):
        ax.bar(x, vals[key], 0.6, color=cols)
        for i, v in enumerate(vals[key]):
            ax.text(x[i], v * (1.1 if logy else 1.02), f"{v:,.1f}" if v < 100 else f"{v:,.0f}", ha="center", fontsize=8)
        if logy:
            ax.set_yscale("log"); ax.axhline(2, color="#c00", ls="--", lw=0.8)
        ax.set_xticks(x); ax.set_xticklabels(names, fontsize=7.5); ax.set_title(lab, fontsize=9.5)
    fig.suptitle("Scaling summary (mean over runs; 1-2 runs per arm). Every arm: 0 lost, 0 overcounted windows",
                 fontsize=10)
    save(fig, "13_scaling_summary.png", ", ".join(ids), src="Sessions D/D2 (not re-run; 1-2 runs per arm)")


def chart_scalein():
    fig, axes = plt.subplots(2, 1, figsize=(12, 6.5), sharex=True)
    steps = None
    for d, name, col, ls in SCALEIN_ARMS:
        steps, t, p99, wt, wk = scale_series(d)
        axes[0].plot(t / 60, np.maximum(p99, 1), color=col, ls=ls, lw=1.6, label=name)
        axes[1].step(wt / 60, wk, where="post", color=col, ls=ls, lw=1.6, label=name)
    for ax in axes:
        for s_ in steps[1:]:
            ax.axvline(s_ / 60, color="#000", ls=":", lw=1)
    axes[0].text(steps[1] / 120, 2e5, "100k evt/s", ha="center", fontsize=9)
    axes[0].text(steps[1] / 60 + 14, 2e5, "25k evt/s", ha="center", fontsize=9)
    axes[0].axhline(2000, color="#c00", lw=0.8, ls="--")
    axes[0].set_yscale("log"); axes[0].set_ylim(5, 4e5)
    axes[0].set_ylabel("pipeline p99 per 30 s (ms, log)"); axes[0].legend(fontsize=8, loc="lower right")
    axes[1].set_ylabel("running TaskManagers"); axes[1].set_xlabel("minutes since load start")
    fig.suptitle("Scale-in, S1 on EKS: 100k -> 25k evt/s, 1 run per arm (session D2)", fontsize=10)
    save(fig, "14_scalein_timeline.png", ", ".join(os.path.basename(d) for d, *_ in SCALEIN_ARMS),
         src="Session D2 (not re-run; 1 run per arm)")


def main():
    chart_hero(); chart_stateless(); chart_2x2(); chart_ccdf(); chart_ckpt_interval()
    chart_recovery(); chart_checkpoints()
    rows = cost_rows(); chart_cost_resources(rows); chart_cpu_timeline(); cost_table_md(rows)
    chart_s4(); chart_s5(); chart_scaling_timeline(); chart_scaling_summary(); chart_scalein()
    with open(os.path.join(OUT, "numbers.json"), "w") as fh:
        json.dump(NUM, fh, indent=1, sort_keys=True)
    print("wrote", os.path.join(OUT, "numbers.json"))


if __name__ == "__main__":
    main()
