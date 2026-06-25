#!/usr/bin/env python3
"""
Generate PNG charts for the article from the measured results. All numbers are the
figures recorded in REPORT.md / results/. Outputs to docs/charts/.
"""
import os
import re
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

OUT = os.path.join(os.path.dirname(__file__), "..", "docs", "charts")
os.makedirs(OUT, exist_ok=True)

# Consistent palette: Spark = warm, Flink = cool.
C_SPARK = "#E25A1C"   # spark orange
C_SPARK2 = "#F2A057"
C_FLINK = "#1E6FB8"   # flink blue
C_FLINK2 = "#5FA8D3"
C_MB = "#888888"
plt.rcParams.update({"figure.dpi": 130, "font.size": 11, "axes.grid": True,
                     "grid.alpha": 0.25, "axes.axisbelow": True})


def save(fig, name):
    p = os.path.join(OUT, name)
    fig.tight_layout()
    fig.savefig(p, bbox_inches="tight")
    plt.close(fig)
    print("wrote", p)


# 1. The headline: RTM breaks the micro-batch floor (local, e2e ms)
def chart_floor():
    fig, ax = plt.subplots(figsize=(7, 4))
    modes = ["micro-batch", "RTM"]
    p50 = [50.0, 7.9]; p99 = [80.4, 42.1]
    x = range(len(modes)); w = 0.35
    ax.bar([i - w/2 for i in x], p50, w, label="p50", color=[C_MB, C_SPARK])
    ax.bar([i + w/2 for i in x], p99, w, label="p99", color=[C_MB, C_SPARK], alpha=0.55)
    for i, (a, b) in enumerate(zip(p50, p99)):
        ax.text(i - w/2, a + 1, f"{a:g}", ha="center", fontsize=9)
        ax.text(i + w/2, b + 1, f"{b:g}", ha="center", fontsize=9)
    ax.set_xticks(list(x)); ax.set_xticklabels(modes)
    ax.set_ylabel("end-to-end latency (ms)")
    ax.set_title("Spark RTM breaks the micro-batch floor (local, 10k evt/s)")
    ax.legend()
    save(fig, "01_rtm_vs_microbatch.png")


# 2. EKS matrix: median vs p99 (e2e, durable 60s checkpointing), all 6 engines.
# Equal-resource 6-partition run (7 cores / 14 GB per engine, pod-memory matched, output
# keyed by user_id). Log scale so the median tie stays readable next to RTM's checkpoint tail.
def chart_matrix():
    engines = ["Flink\nSQL", "Flink\n(Java)", "PyFlink", "Spark RTM\n(Java)",
               "Spark RTM\n(Scala)", "PySpark\nRTM"]
    p50 = [12.1, 12.0, 16.9, 17.1, 17.1, 17.5]
    p99 = [56, 63, 59, 833, 906, 763]
    colors = [C_FLINK, C_FLINK2, C_FLINK2, C_SPARK, C_SPARK, C_SPARK2]
    fig, ax = plt.subplots(figsize=(8.5, 4.4))
    x = range(len(engines)); w = 0.38
    ax.bar([i - w/2 for i in x], p50, w, label="p50", color=colors)
    ax.bar([i + w/2 for i in x], p99, w, label="p99", color=colors, alpha=0.5)
    ax.set_yscale("log")
    for i, (a, b) in enumerate(zip(p50, p99)):
        ax.text(i - w/2, a * 1.08, f"{a:g}", ha="center", fontsize=8)
        ax.text(i + w/2, b * 1.08, f"{b:g}", ha="center", fontsize=8)
    ax.set_xticks(list(x)); ax.set_xticklabels(engines, fontsize=8.5)
    ax.set_ylabel("end-to-end latency (ms, log scale)")
    ax.set_title("EKS ~100k evt/s, 6 partitions, equal resources + durable S3 checkpointing:\n"
                 "median is close, the tail separates", fontsize=11)
    ax.legend()
    save(fig, "02_eks_matrix.png")


# 3. THE big one: durable checkpointing tail blowup (pipe p99, log scale)
# Equal-resource 6-partition run (7 cores / 14 GB per engine, pod-memory matched, keyed).
def chart_checkpoint():
    engines = ["Flink\nSQL", "Flink\n(Java)", "PyFlink", "Spark RTM\n(Java)",
               "Spark RTM\n(Scala)", "PySpark\nRTM"]
    p99 = [31, 33, 33, 669, 702, 591]
    colors = [C_FLINK, C_FLINK2, C_FLINK2, C_SPARK, C_SPARK, C_SPARK2]
    fig, ax = plt.subplots(figsize=(8.5, 4.2))
    bars = ax.bar(engines, p99, color=colors)
    ax.set_yscale("log")
    ax.set_ylabel("pipeline p99 latency (ms, log scale)")
    ax.set_title("Durable S3 checkpointing: RTM's synchronous commit wrecks the tail\n"
                 "(6 partitions, equal resources: 7 cores / 14 GB per engine)", fontsize=11)
    for b, v in zip(bars, p99):
        ax.text(b.get_x() + b.get_width()/2, v * 1.1, f"{v:g} ms", ha="center", fontsize=9)
    ax.axhspan(0, 100, color=C_FLINK, alpha=0.05)
    ax.text(0.02, 0.93, "~20x gap", transform=ax.transAxes, fontsize=11,
            color=C_SPARK, fontweight="bold")
    save(fig, "03_checkpointing_tail.png")


# 4. Data-rate sweep: latency flat as bytes climb (Flink)
def chart_datarate():
    rate = [15, 103, 770]          # MB/s
    p50 = [15.0, 14.5, 20.5]
    cpu = [1.6, 2.3, 3.9]
    fig, ax1 = plt.subplots(figsize=(7.5, 4.2))
    ax1.plot(rate, p50, "o-", color=C_FLINK, lw=2, label="e2e p50 latency")
    ax1.set_xlabel("ingest data rate (MB/s)")
    ax1.set_ylabel("e2e p50 latency (ms)", color=C_FLINK)
    ax1.set_ylim(0, 40)
    ax1.tick_params(axis="y", labelcolor=C_FLINK)
    for x, y in zip(rate, p50):
        ax1.text(x, y + 1.5, f"{y:g} ms", ha="center", fontsize=9, color=C_FLINK)
    ax2 = ax1.twinx()
    ax2.plot(rate, cpu, "s--", color=C_SPARK, lw=2, label="CPU (cores)")
    ax2.set_ylabel("CPU (cores)", color=C_SPARK)
    ax2.set_ylim(0, 6)
    ax2.tick_params(axis="y", labelcolor=C_SPARK)
    ax1.set_title("Flink: latency flat across 50x data rate; CPU scales with bytes")
    ax1.grid(alpha=0.25)
    save(fig, "04_datarate_sweep.png")


# 5. Efficiency: CPU + memory per engine (EKS steady state)
def chart_efficiency():
    engines = ["Flink", "PyFlink", "Spark RTM\n(Java)", "Spark RTM\n(Scala)"]
    cpu = [1.6, 6.8, 3.4, 3.1]
    mem = [5.0, 6.7, 9.2, 9.3]
    colors = [C_FLINK, C_FLINK2, C_SPARK, C_SPARK]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(9, 4))
    a1.bar(engines, cpu, color=colors); a1.set_title("mean CPU (cores)")
    a1.set_xticklabels(engines, fontsize=8.5)
    for i, v in enumerate(cpu): a1.text(i, v + 0.1, f"{v}", ha="center", fontsize=9)
    a2.bar(engines, mem, color=colors); a2.set_title("mean memory (GB)")
    a2.set_xticklabels(engines, fontsize=8.5)
    for i, v in enumerate(mem): a2.text(i, v + 0.1, f"{v}", ha="center", fontsize=9)
    fig.suptitle("EKS ~100k evt/s: Flink does the same work on ~half the resources")
    save(fig, "05_efficiency.png")


# 6. Recovery downtime on worker kill (EKS, durable checkpoint + operator restart)
def chart_recovery():
    engines = ["Flink (Java)", "Spark RTM (Java)", "PyFlink"]
    downtime = [18.0, 21.5, 24.0]
    colors = [C_FLINK, C_SPARK, C_FLINK2]
    fig, ax = plt.subplots(figsize=(7, 3.6))
    bars = ax.barh(engines, downtime, color=colors)
    ax.set_xlabel("recovery downtime after worker kill (seconds)")
    ax.set_title("Fault tolerance: both recover automatically, ~18-24s (comparable)")
    for b, v in zip(bars, downtime):
        ax.text(v + 0.3, b.get_y() + b.get_height()/2, f"{v:g}s", va="center", fontsize=10)
    ax.set_xlim(0, 28)
    ax.invert_yaxis()
    save(fig, "06_recovery.png")


def _resource_series(label):
    """Per-timestamp summed CPU (cores) and mem (MB) for an engine's own pods,
    parsed from results/eks/<label>_top.txt (format: ts ns pod cpu mem)."""
    import statistics as st
    p = os.path.join(OUT, "..", "..", "results", "eks", f"{label}_top.txt")
    if not os.path.exists(p):
        return None
    per = {}
    for line in open(p):
        f = line.split()
        if len(f) < 5 or f[1] not in ("spark", "flink"):
            continue
        # Match every engine's own pods: Spark/Java/Scala RTM (rtm-bench-*), Flink jobs
        # (flink-bench-*), and PySpark RTM, whose executors are named
        # rtm-pyspark-measured-*-exec-N (driver is rtm-bench-driver).
        if not any(k in f[2] for k in ("rtm-bench", "flink-bench", "rtm-pyspark-measured")):
            continue
        try:
            mc = int(f[3].rstrip("m")); mi = int(re.sub(r"[^0-9]", "", f[4]))
        except ValueError:
            continue
        per.setdefault(f[0], [0, 0]); per[f[0]][0] += mc; per[f[0]][1] += mi
    cpus = [v[0] / 1000 for v in per.values()]; mems = [v[1] for v in per.values()]
    if not cpus:
        return None
    return {"mean_cpu": st.mean(cpus), "peak_cpu": max(cpus),
            "mean_mem": st.mean(mems) / 1024, "peak_mem": max(mems) / 1024}


# 8/9. CPU and memory per engine (EKS), mean + peak bars
def _resource_chart(metric, ylabel, title, fname):
    import numpy as np
    order = [("flink-sql-p6", "Flink\nSQL", C_FLINK), ("flink-p6", "Flink\n(Java)", C_FLINK2),
             ("pyflink-p6", "PyFlink", C_FLINK2),
             ("spark-rtm-java-p6", "Spark RTM\n(Java)", C_SPARK),
             ("spark-rtm-scala-p6", "Spark RTM\n(Scala)", C_SPARK),
             ("pyspark-rtm-p6", "PySpark\nRTM", C_SPARK2)]
    rows = [(lbl, c, _resource_series(k)) for k, lbl, c in order]
    rows = [(lbl, c, r) for lbl, c, r in rows if r]
    labels = [r[0] for r in rows]; colors = [r[1] for r in rows]
    mean = [r[2][f"mean_{metric}"] for r in rows]; peak = [r[2][f"peak_{metric}"] for r in rows]
    fig, ax = plt.subplots(figsize=(9, 4.4))
    x = np.arange(len(labels)); w = 0.38
    ax.bar(x - w/2, mean, w, label="mean", color=colors)
    ax.bar(x + w/2, peak, w, label="peak", color=colors, alpha=0.5)
    for i, (m, p) in enumerate(zip(mean, peak)):
        ax.text(i - w/2, m + max(peak)*0.01, f"{m:.1f}", ha="center", fontsize=8)
        ax.text(i + w/2, p + max(peak)*0.01, f"{p:.1f}", ha="center", fontsize=8)
    ax.set_xticks(x); ax.set_xticklabels(labels, fontsize=8.5); ax.set_ylabel(ylabel)
    ax.set_title(title); ax.legend()
    save(fig, fname)


def chart_cpu():
    _resource_chart("cpu", "CPU (cores)",
                    "EKS ~100k evt/s, 6 partitions, equal allocation (7c/14GB): CPU per engine",
                    "08_cpu.png")


def chart_memory():
    _resource_chart("mem", "memory (GB)",
                    "EKS ~100k evt/s, 6 partitions, equal allocation (7c/14GB): memory per engine",
                    "09_memory.png")


# 10. Scaling: 6 vs 12 partitions/cores. RTM's tail is resource-insensitive (it's the
# synchronous commit); Flink's already-tiny tail only gets tighter with fewer subtasks.
def chart_scaling():
    import numpy as np
    configs = ["6 part\n7 cores", "12 part\n13 cores"]
    flink = [33, 40]          # Flink DataStream (Java), keyed output
    rtm = [669, 829]          # Spark RTM (Java)
    x = np.arange(len(configs)); w = 0.36
    fig, ax = plt.subplots(figsize=(7.5, 4.4))
    b1 = ax.bar(x - w/2, rtm, w, label="Spark RTM (Java)", color=C_SPARK)
    b2 = ax.bar(x + w/2, flink, w, label="Flink (Java)", color=C_FLINK)
    ax.set_yscale("log")
    ax.set_ylabel("pipeline p99 latency (ms, log scale)")
    ax.set_xticks(x); ax.set_xticklabels(configs)
    ax.set_title("Halving partitions + cores barely moves either tail:\n"
                 "RTM stays ~700-830 ms, Flink stays ~30-40 ms", fontsize=11)
    ax.legend(loc="center right", fontsize=10)
    for bars in (b1, b2):
        for b in bars:
            v = b.get_height()
            ax.text(b.get_x()+b.get_width()/2, v*1.1, f"{v:g} ms", ha="center", fontsize=9)
    ax.set_ylim(top=2000)
    fig.text(0.5, -0.01, "Equal resources per engine at each size; 60s durable checkpointing. "
             "RTM's tail is the commit, not a resource shortage.",
             ha="center", fontsize=9, style="italic", color="#444")
    save(fig, "10_scaling_6v12.png")


# 11. Reviewer control: same JSON path + same output keying, the tail gap is unchanged.
# Flink SQL uses schema-based declarative JSON (the Catalyst analogue) and keys by user_id
# exactly like Spark — isolating the engine from the parser implementation and partitioning.
def chart_json_control():
    engines = ["Flink SQL\n(schema JSON)", "Flink DataStream\n(Jackson)",
               "Spark RTM\n(Catalyst JSON)"]
    p99 = [31, 33, 669]
    colors = [C_FLINK, C_FLINK2, C_SPARK]
    fig, ax = plt.subplots(figsize=(8, 4.4))
    bars = ax.bar(engines, p99, color=colors)
    ax.set_yscale("log")
    ax.set_ylabel("pipeline p99 latency (ms, log scale)")
    ax.set_title("Same JSON path, same output keying — the tail gap holds\n"
                 "(6-part, equal resources, all keyed by user_id, durable 60s ckpt)",
                 fontsize=11)
    for b, v in zip(bars, p99):
        ax.text(b.get_x() + b.get_width()/2, v * 1.1, f"{v:g} ms", ha="center", fontsize=9)
    ax.set_ylim(top=1500)
    fig.text(0.5, -0.01, "Flink SQL parses/serializes JSON declaratively (like Spark's "
             "Catalyst) and keys output by user_id — yet is still ~20x faster at the tail.",
             ha="center", fontsize=9, style="italic", color="#444")
    save(fig, "11_json_path_control.png")


def _main():
    chart_hero()
    chart_floor()
    chart_matrix()
    chart_checkpoint()
    chart_datarate()
    chart_efficiency()
    chart_recovery()
    chart_cpu()
    chart_memory()
    chart_scaling()
    chart_json_control()
    print("\nAll charts in", OUT)


# 0. HERO: the one-glance story — median ties, tail diverges, checkpointing blows it open
def chart_hero():
    import numpy as np
    fig, ax = plt.subplots(figsize=(9, 4.8))
    # Production-realistic: BOTH engines with durable S3 checkpointing at a matched 60s
    # interval (no real streaming job runs without it), IDENTICAL resources per engine
    # (7 cores / 14 GB, pod-memory matched), output keyed by user_id, 6 partitions.
    # Median ties; tail diverges ~20x.
    groups = ["median (p50)", "tail (p99)"]
    spark = [6.3, 669]
    flink = [6.0, 31]
    x = np.arange(len(groups)); w = 0.36
    b1 = ax.bar(x - w/2, spark, w, label="Spark RTM (Java)", color=C_SPARK)
    b2 = ax.bar(x + w/2, flink, w, label="Flink (SQL)", color=C_FLINK)
    ax.set_yscale("log")
    ax.set_ylabel("pipeline latency (ms, log scale)")
    ax.set_xticks(x); ax.set_xticklabels(groups)
    ax.set_title("Spark 4.1 Real-time Mode vs Flink, durable S3 checkpointing (60s),\n"
                 "equal resources: tied at the median, ~20x apart at the tail (EKS, ~100k evt/s)",
                 fontsize=12.5, fontweight="bold")
    ax.legend(loc="upper left", fontsize=11)
    for bars in (b1, b2):
        for b in bars:
            v = b.get_height()
            ax.text(b.get_x()+b.get_width()/2, v*1.13, f"{v:g} ms", ha="center", fontsize=10)
    ax.annotate("~20x", xy=(1.18, 669), xytext=(1.5, 200),
                fontsize=14, color=C_SPARK, fontweight="bold",
                arrowprops=dict(arrowstyle="->", color=C_SPARK, lw=1.5))
    ax.set_ylim(top=2500)
    fig.text(0.5, -0.01, "Spark RTM's synchronous checkpoint commit stalls the data path; "
             "Flink's is asynchronous", ha="center", fontsize=9.5, style="italic", color="#444")
    save(fig, "00_hero.png")


if __name__ == "__main__":
    _main()
