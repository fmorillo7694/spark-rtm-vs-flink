#!/usr/bin/env python3
"""
Charts + table images for the 2-hour soak (stability + spike resilience), styled to match
bench/make_charts.py. Outputs to docs/charts/:
  12_soak_stability.png    pipeline p99 per 30-min window, both engines (does the tail drift?)
  13_spike_resilience.png  input rate + Flink lag over time, spike bands shaded (recovery)
  T10_soak.png             per-window latency table
  T11_spike.png            spike-resilience summary table
"""
import json
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

OUT = os.path.join(os.path.dirname(__file__), "..", "docs", "charts")
D = os.path.join(os.path.dirname(__file__), "..", "results", "eks", "soak")
C_SPARK = "#E25A1C"; C_FLINK = "#1E6FB8"; C_HDR = "#2C3E50"; C_ALT = "#F4F6F8"
plt.rcParams.update({"figure.dpi": 130, "font.size": 11, "axes.grid": True,
                     "grid.alpha": 0.25, "axes.axisbelow": True})


def save(fig, name):
    p = os.path.join(OUT, name)
    fig.tight_layout(); fig.savefig(p, bbox_inches="tight"); plt.close(fig)
    print("wrote", os.path.basename(p))


def load(engine):
    rows = []
    p = os.path.join(D, f"{engine}_soak.jsonl")
    for line in open(p):
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return sorted(rows, key=lambda r: r["window_index"])


# 12. Stability: pipeline p99 per 30-min window (log scale; does the tail creep over 2h?)
def chart_stability():
    sp = load("spark"); fl = load("flink")
    x = [r["window_index"] for r in sp]
    sp99 = [r["pipeline_ms"]["p99"] for r in sp]
    fp99 = [r["pipeline_ms"]["p99"] for r in fl]
    sp50 = [r["pipeline_ms"]["p50"] for r in sp]
    fp50 = [r["pipeline_ms"]["p50"] for r in fl]
    fig, ax = plt.subplots(figsize=(8, 4.4))
    ax.plot(x, sp99, "o-", color=C_SPARK, lw=2, label="Spark RTM p99")
    ax.plot(x, fp99, "o-", color=C_FLINK, lw=2, label="Flink p99")
    ax.plot(x, sp50, "o--", color=C_SPARK, lw=1, alpha=0.5, label="Spark p50")
    ax.plot(x, fp50, "o--", color=C_FLINK, lw=1, alpha=0.5, label="Flink p50")
    ax.set_yscale("log")
    ax.set_xticks(x); ax.set_xticklabels([f"{(i+1)*30}min" for i in x])
    ax.set_ylabel("pipeline latency (ms, log scale)")
    ax.set_title("2-hour soak: the tail gap holds, no drift\n"
                 "(pipeline p99 per 30-min window, concurrent, ~33–42k evt/s + spikes)",
                 fontsize=11)
    ax.legend(ncol=2, fontsize=9, loc="center left")
    ax.set_ylim(4, 1200)
    for xi, v in zip(x, sp99):
        ax.text(xi, v*1.12, f"{v:.0f}", ha="center", fontsize=8, color=C_SPARK)
    for xi, v in zip(x, fp99):
        ax.text(xi, v*1.12, f"{v:.0f}", ha="center", fontsize=8, color=C_FLINK)
    save(fig, "12_soak_stability.png")


# 13. Spike resilience: input rate + Flink lag, spike windows shaded.
def chart_spike():
    rows = []
    for l in open(os.path.join(D, "spike_samples.tsv")):
        f = l.strip().split("\t")
        if len(f) >= 7 and all(f[i] for i in range(7)) and f[0].isdigit():
            try:
                rows.append([int(x) for x in f[:7]])
            except ValueError:
                pass
    t0 = rows[0][0]
    mins, inrate, flag, spk = [], [], [], []
    prev = None
    for r in rows:
        ts, ie, sl, fl_, so, fo, sp = r
        if prev and ts > prev[0]:
            mins.append((ts - t0) / 60.0)
            inrate.append((ie - prev[1]) / (ts - prev[0]) / 1000.0)  # k evt/s
            flag.append(fl_ / 1e6)   # M records lag
            spk.append(sp)
        prev = r
    fig, ax1 = plt.subplots(figsize=(9, 4.4))
    # shade spike bands
    in_spike = False; start = None
    for m, s in zip(mins, spk):
        if s == 1 and not in_spike:
            in_spike = True; start = m
        elif s == 0 and in_spike:
            ax1.axvspan(start, m, color=C_SPARK, alpha=0.10); in_spike = False
    if in_spike:
        ax1.axvspan(start, mins[-1], color=C_SPARK, alpha=0.10)
    ax1.plot(mins, inrate, color="#666", lw=1.5, label="input rate (k evt/s)")
    ax1.set_xlabel("minutes into soak"); ax1.set_ylabel("input rate (k evt/s)", color="#666")
    ax1.set_ylim(0, 180)
    ax2 = ax1.twinx()
    ax2.plot(mins, flag, color=C_FLINK, lw=2, label="Flink lag (M records)")
    ax2.set_ylabel("Flink consumer lag (M records)", color=C_FLINK)
    ax2.set_ylim(0, 10)
    ax1.set_title("Spike resilience: +50% load bursts (shaded) — Flink lag spikes then drains\n"
                  "input → 150k/s during each band; lag recovers to <1M within minutes",
                  fontsize=11)
    save(fig, "13_spike_resilience.png")


def render_table(rows, fname, title=None):
    ncol = len(rows[0]); nrow = len(rows)
    fig, ax = plt.subplots(figsize=(min(13, 1.7*ncol+1), 0.5*nrow+(0.6 if title else 0.3)))
    ax.axis("off")
    if title:
        ax.set_title(title, fontsize=12, fontweight="bold", pad=10, loc="left")
    t = ax.table(cellText=rows[1:], colLabels=rows[0], cellLoc="center", loc="center")
    t.auto_set_font_size(False); t.set_fontsize(10.5); t.scale(1, 1.5)
    for (r, c), cell in t.get_celld().items():
        cell.set_edgecolor("#DDD")
        if r == 0:
            cell.set_facecolor(C_HDR); cell.set_text_props(color="white", fontweight="bold")
        elif r % 2 == 0:
            cell.set_facecolor(C_ALT)
    t.auto_set_column_width(col=list(range(ncol)))
    save(fig, fname)


def tables():
    sp = load("spark"); fl = load("flink")
    rows = [["window", "eps", "Spark p50", "Spark p99", "Flink p50", "Flink p99"]]
    for s, f in zip(sp, fl):
        rows.append([f"{(s['window_index']+1)*30} min", f"{s['throughput_eps']:.0f}",
                     f"{s['pipeline_ms']['p50']:.1f} ms", f"{s['pipeline_ms']['p99']:.0f} ms",
                     f"{f['pipeline_ms']['p50']:.1f} ms", f"{f['pipeline_ms']['p99']:.0f} ms"])
    render_table(rows, "T10_soak.png")

    spike = [
        ["metric", "Spark RTM", "Flink"],
        ["baseline output", "~33k evt/s", "~33k evt/s"],
        ["output during +50% spike", "~50k evt/s", "~50k evt/s"],
        ["kept up with surge?", "yes (no backlog)", "yes (lag drained)"],
        ["lag at spike peak", "n/a (no Kafka group)", "~7.8M records"],
        ["lag after spike", "—", "<1M within minutes"],
        ["mean memory (2h)", "~8.1 GB", "~5.5 GB"],
        ["records processed (2h)", "tied within 0.1%", "tied within 0.1%"],
    ]
    render_table(spike, "T11_spike.png")


if __name__ == "__main__":
    chart_stability()
    chart_spike()
    tables()
    print("\nsoak charts + tables in", OUT)
