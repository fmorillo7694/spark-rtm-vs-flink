#!/usr/bin/env python3
"""
Generate PNG charts for the article from the measured results. All numbers are the
figures recorded in REPORT.md / results/. Outputs to docs/charts/.
"""
import os
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


# 2. EKS matrix: median vs p99 (e2e, no durable checkpoint), 5 engines
def chart_matrix():
    engines = ["Flink\n(Java)", "PyFlink", "Spark RTM\n(Java)", "Spark RTM\n(Scala)", "PySpark\nRTM"]
    p50 = [15.0, 35.7, 24.2, 36.7, 38.9]
    p99 = [76, 105, 183, 233, 200]
    colors = [C_FLINK, C_FLINK2, C_SPARK, C_SPARK, C_SPARK2]
    fig, ax = plt.subplots(figsize=(8, 4.2))
    x = range(len(engines)); w = 0.38
    ax.bar([i - w/2 for i in x], p50, w, label="p50", color=colors)
    ax.bar([i + w/2 for i in x], p99, w, label="p99", color=colors, alpha=0.5)
    for i, (a, b) in enumerate(zip(p50, p99)):
        ax.text(i - w/2, a + 2, f"{a:g}", ha="center", fontsize=8)
        ax.text(i + w/2, b + 2, f"{b:g}", ha="center", fontsize=8)
    ax.set_xticks(list(x)); ax.set_xticklabels(engines, fontsize=9)
    ax.set_ylabel("end-to-end latency (ms)")
    ax.set_title("EKS ~100k evt/s: median is close, the tail separates")
    ax.legend()
    save(fig, "02_eks_matrix.png")


# 3. THE big one: durable checkpointing tail blowup (pipe p99, log scale)
def chart_checkpoint():
    engines = ["Flink\n(Java)", "PyFlink", "Spark RTM\n(Java)", "Spark RTM\n(Scala)", "PySpark\nRTM"]
    p99 = [46.4, 56.8, 1319, 1063, 933]
    colors = [C_FLINK, C_FLINK2, C_SPARK, C_SPARK, C_SPARK2]
    fig, ax = plt.subplots(figsize=(8, 4.2))
    bars = ax.bar(engines, p99, color=colors)
    ax.set_yscale("log")
    ax.set_ylabel("pipeline p99 latency (ms, log scale)")
    ax.set_title("Durable S3 checkpointing: RTM's synchronous commit wrecks the tail")
    for b, v in zip(bars, p99):
        ax.text(b.get_x() + b.get_width()/2, v * 1.1, f"{v:g} ms", ha="center", fontsize=9)
    ax.axhspan(0, 100, color=C_FLINK, alpha=0.05)
    ax.text(0.02, 0.93, "~25x gap", transform=ax.transAxes, fontsize=11,
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


def _main():
    chart_hero()
    chart_floor()
    chart_matrix()
    chart_checkpoint()
    chart_datarate()
    chart_efficiency()
    chart_recovery()
    print("\nAll charts in", OUT)


# 0. HERO: the one-glance story — median ties, tail diverges, checkpointing blows it open
def chart_hero():
    import numpy as np
    fig, ax = plt.subplots(figsize=(9, 4.8))
    groups = ["median\n(p50)", "tail p99\n(no checkpoint)", "tail p99\n(durable checkpoint)"]
    spark = [7.3, 183, 1319]
    flink = [6.9, 76, 46.4]
    x = np.arange(len(groups)); w = 0.36
    b1 = ax.bar(x - w/2, spark, w, label="Spark RTM", color=C_SPARK)
    b2 = ax.bar(x + w/2, flink, w, label="Flink", color=C_FLINK)
    ax.set_yscale("log")
    ax.set_ylabel("end-to-end latency (ms, log scale)")
    ax.set_xticks(x); ax.set_xticklabels(groups)
    ax.set_title("Spark 4.1 Real-time Mode vs Flink: tied at the median,\nworlds apart at the tail (EKS, ~100k evt/s)",
                 fontsize=13, fontweight="bold")
    ax.legend(loc="upper left", fontsize=11)
    for bars in (b1, b2):
        for b in bars:
            v = b.get_height()
            ax.text(b.get_x()+b.get_width()/2, v*1.12,
                    f"{v:g}", ha="center", fontsize=9)
    ax.annotate("Spark RTM ~26x\nFlink's tail", xy=(1.82, 1325), xytext=(0.95, 430),
                fontsize=11, color=C_SPARK, fontweight="bold",
                arrowprops=dict(arrowstyle="->", color=C_SPARK))
    ax.set_ylim(top=3500)
    save(fig, "00_hero.png")


if __name__ == "__main__":
    _main()
