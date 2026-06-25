#!/usr/bin/env python3
"""Architecture diagram for the article — Kafka -> engine -> Kafka, 5 engine variants."""
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

OUT = os.path.join(os.path.dirname(__file__), "..", "docs", "charts", "07_architecture.png")
C_KAFKA = "#231F20"; C_ENGINE = "#3C6E9C"; C_PROD = "#4C9A5C"; C_CONS = "#9C4C8C"

fig, ax = plt.subplots(figsize=(11, 4.6))
ax.set_xlim(0, 11); ax.set_ylim(0, 4.6); ax.axis("off")


def box(x, y, w, h, title, sub, color):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.04,rounding_size=0.12",
                                fc=color, ec="none", alpha=0.92))
    ax.text(x + w / 2, y + h * 0.63, title, ha="center", va="center",
            color="white", fontsize=12, fontweight="bold")
    ax.text(x + w / 2, y + h * 0.28, sub, ha="center", va="center",
            color="white", fontsize=8.5, alpha=0.92)


def arrow(x1, x2, y, label):
    ax.add_patch(FancyArrowPatch((x1, y), (x2, y), arrowstyle="-|>",
                                 mutation_scale=20, lw=2, color="#555"))
    ax.text((x1 + x2) / 2, y + 0.22, label, ha="center", fontsize=8, color="#555", style="italic")


# title
ax.text(5.5, 4.3, "Benchmark architecture: Kafka → stateless transform → Kafka",
        ha="center", fontsize=13, fontweight="bold")

# main flow row
y, h = 2.5, 1.0
box(0.2, y, 2.0, h, "Producer", "~10–100k evt/s\nstamps ts", C_PROD)
box(3.0, y, 2.0, h, "Kafka", "input-events\n3–12 partitions", C_KAFKA)
box(5.8, y, 2.4, h, "Engine", "filter · ×1.21 tax\nUPPER · stamp out_ts", C_ENGINE)
box(9.0, y, 1.8, h, "Kafka", "output-events", C_KAFKA)
arrow(2.2, 3.0, y + h / 2, "JSON")
arrow(5.0, 5.8, y + h / 2, "consume")
arrow(8.2, 9.0, y + h / 2, "produce")

# consumer below the output kafka
box(9.0, 0.9, 1.8, 0.95, "Consumer", "measures\nlatency", C_CONS)
ax.add_patch(FancyArrowPatch((9.9, y), (9.9, 1.85), arrowstyle="-|>",
                             mutation_scale=18, lw=2, color="#555"))

# engine-variants caption — placed in the open space lower-left, clear of the consumer box
ax.text(0.2, 1.55, "Engine = one of:", ha="left", fontsize=9.5, color="#333", fontweight="bold")
ax.text(0.2, 1.15, "Spark RTM (Scala · Java · PySpark)\nSpark micro-batch\nFlink (Java) · PyFlink",
        ha="left", va="top", fontsize=9, color="#333")
ax.text(0.2, 0.25, "same pipeline, same workload, measured identically across all five",
        ha="left", fontsize=8.5, color="#888", style="italic")

fig.tight_layout()
fig.savefig(OUT, dpi=130, bbox_inches="tight")
print("wrote", OUT)
