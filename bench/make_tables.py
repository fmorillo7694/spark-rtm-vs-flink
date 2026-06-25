#!/usr/bin/env python3
"""Render each Markdown table in ARTICLE.md as a clean PNG for Medium (which has no tables).
Outputs docs/charts/T1_*.png ... in article order."""
import os, re
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt

ART = os.path.join(os.path.dirname(__file__), "..", "ARTICLE.md")
OUT = os.path.join(os.path.dirname(__file__), "..", "docs", "charts")
C_HDR = "#2C3E50"; C_ALT = "#F4F6F8"

# short slug per table (article order) for filenames
SLUGS = ["language-matrix", "rtm-vs-microbatch", "local-latency", "eks-allocation",
         "eks-results", "datarate-sweep", "recovery"]

def parse_tables(md):
    out=[]; cur=[]
    for ln in md.split("\n"):
        if ln.lstrip().startswith("|"):
            cur.append(ln.strip())
        elif cur:
            out.append(cur); cur=[]
    if cur: out.append(cur)
    tables=[]
    for blk in out:
        rows=[[c.strip() for c in r.strip("|").split("|")] for r in blk
              if not re.match(r'^\|[\s:|\-]+\|?$', r)]
        # strip markdown bold ** in cells
        rows=[[re.sub(r'\*\*(.+?)\*\*', r'\1', c) for c in r] for r in rows]
        tables.append(rows)
    return tables

def render(rows, fname, title=None):
    ncol=len(rows[0]); nrow=len(rows)
    fig_w=min(12, 1.6*ncol+1); fig_h=0.55*nrow+(0.6 if title else 0.3)
    fig, ax=plt.subplots(figsize=(fig_w, fig_h)); ax.axis("off")
    if title:
        ax.set_title(title, fontsize=12, fontweight="bold", pad=10, loc="left")
    tbl=ax.table(cellText=rows[1:], colLabels=rows[0], cellLoc="center", loc="center")
    tbl.auto_set_font_size(False); tbl.set_fontsize(11); tbl.scale(1, 1.5)
    for (r,c),cell in tbl.get_celld().items():
        cell.set_edgecolor("#DDD")
        if r==0:
            cell.set_facecolor(C_HDR); cell.set_text_props(color="white", fontweight="bold")
        elif r%2==0:
            cell.set_facecolor(C_ALT)
        # left-align first column (labels), right-align numeric
        cell.set_text_props(ha="center")
    tbl.auto_set_column_width(col=list(range(ncol)))
    fig.tight_layout()
    p=os.path.join(OUT, fname); fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig)
    print("wrote", os.path.basename(p))

tables=parse_tables(open(ART).read())
for i,(rows,slug) in enumerate(zip(tables, SLUGS), 1):
    render(rows, f"T{i}_{slug}.png")
print(f"\n{len(tables)} table images in {OUT}")
