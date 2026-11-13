#!/usr/bin/env python3
"""
Medium copy of the post: Medium has no tables, so every GFM table in post/piece1-4.3.md is
rendered to a PNG and the copy references the image instead. The GitHub post keeps real tables.

  post/piece1-4.3.md  ->  post/charts/tables/tNN_<slug>.png   (one per table, order of appearance)
                      ->  post/piece1-4.3-medium.md           (tables replaced by ![alt](png))

Images are 1400 px wide (7 in at 200 dpi) so Medium's ~700 px column shows every table at the
same 0.5x scale. Column widths come from measured text: headers wrap first; if the data still
does not fit, the font shrinks (11 pt down to 8 pt) and only then are the widest cells wrapped.
Nothing is clipped. Each cell's rendered text is asserted equal to the source cell minus markup.

Usage: <venv with matplotlib>/bin/python bench/make_table_images.py
"""
import glob
import os
import re
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.font_manager import FontProperties  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from make_charts import C_FLINK  # noqa: E402,F401  (shared palette + rcParams, same default font as the charts)

ROOT = os.path.join(HERE, "..")
SRC = os.path.join(ROOT, "post", "piece1-4.3.md")
DST = os.path.join(ROOT, "post", "piece1-4.3-medium.md")
OUT_REL = "charts/tables"
OUT = os.path.join(ROOT, "post", OUT_REL)

DPI = 200
WIDTH_PX = 1400
MARGIN = 8            # px around the table
PAD_X, PAD_Y = 16, 11  # px inside each cell
FONT_MAX, FONT_MIN, FONT_STEP = 11.0, 8.0, 0.5
LINE_SPACING = 1.32
TEXT = "#222222"
HEADER_FILL = "#E6EEF7"   # light tint of the Flink blue used in the charts
ZEBRA = "#F6F7F9"
RULE = "#C9CED6"
RULE_HEAVY = "#7F8A99"

SEP_RE = re.compile(r"^\|(\s*:?-+:?\s*\|)+\s*$")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")

# One-line alt text per table, keyed by slug. A table without an entry gets a generated one.
ALT = {
    "part_1_stateless_on_4_3_2_3":
        "Table: stateless EKS results per engine on Spark 4.3 / Flink 2.3 - pipeline p50 and p99 ms, "
        "published 4.1.2 / 2.2.0 p99, CPU cores and working set GiB",
    "both_at_defaults_both_tuned":
        "Table: stateful p50 / p99 ms for S1 windowed agg, S2 dedup and S3 sessions, Spark at default, "
        "1 ms and vendor-tuned vs Flink at default and 0 ms buffer timeout",
    "the_checkpoint_interval_is_a":
        "Table: p50 / p99 ms by checkpoint interval (5 s to 300 s) for Spark and Flink on S1 and S2",
    "recovery_kill_a_worker_mid_stream":
        "Table: worker-kill recovery for Flink and Spark RTM - output stop, time back to rate, "
        "records re-emitted, windows checked, lost and overcounted",
    "s4_stream_static_enrichment_rtm":
        "Table: S4 stream-static enrichment - Spark RTM broadcast join vs Flink map, p50 / p99 ms, "
        "CPU, working set and JVM heap",
    "s5_stream_stream_interval_join":
        "Table: S5 stream-stream interval join at 1000 and 2000 evt/s per pod - Spark micro-batch "
        "vs Flink intervalJoin latency, output rate, CPU and throttling",
    "scaling_under_changing_load":
        "Table: scaling under changing load - p99 per phase, worst 1 s p99, longest output stall, "
        "node-hours vs peak and lost / overcounted per arm",
    "resources_and_cost":
        "Table: S1 resources per arm - p99, CPU, working set, CPU throttling, JVM heap, "
        "checkpoint MB/h and S3 PUTs/h",
    "what_rtm_can_t_do_yet_feature":
        "Table: feature gaps, Flink 2.3 vs OSS Spark 4.3 RTM, for joins, CEP, broadcast state, "
        "session windows, Python state, sinks, checkpointing, timers and output modes",
}


# ---------- markdown parsing ----------

def slugify(heading, limit=32):
    s = re.sub(r"[^a-z0-9]+", "_", heading.lower()).strip("_")
    if len(s) <= limit:
        return s
    cut = s[:limit + 1]
    return cut[:cut.rfind("_")] if "_" in cut else s[:limit]


def split_row(line):
    """Cells of a GFM row: split on '|' outside backticks, honour '\\|'."""
    body = line.strip()
    assert body.startswith("|"), line
    body = body[1:-1] if body.endswith("|") and not body.endswith("\\|") else body[1:]
    cells, cur, in_code, i = [], "", False, 0
    while i < len(body):
        c = body[i]
        if c == "\\" and i + 1 < len(body) and body[i + 1] == "|":
            cur += "|"
            i += 2
            continue
        if c == "`":
            in_code = not in_code
        if c == "|" and not in_code:
            cells.append(cur.strip())
            cur = ""
        else:
            cur += c
        i += 1
    cells.append(cur.strip())
    return cells


def alignments(sep_line):
    out = []
    for spec in split_row(sep_line):
        spec = spec.strip()
        if spec.startswith(":") and spec.endswith(":"):
            out.append("center")
        elif spec.endswith(":"):
            out.append("right")
        else:
            out.append("left")
    return out


def segments(cell):
    """Inline markdown -> [(text, bold, italic, mono)]. Handles **bold**, *italic*, `code`."""
    out = []
    for part in re.split(r"(`[^`]*`)", cell):
        if not part:
            continue
        if part.startswith("`") and part.endswith("`") and len(part) >= 2:
            out.append((part[1:-1], False, False, True))
            continue
        for b_i, bpart in enumerate(re.split(r"\*\*", part)):
            bold = b_i % 2 == 1
            for i_i, ipart in enumerate(re.split(r"(?<![\w*])\*(?=\S)|(?<=\S)\*(?![\w*])", bpart)):
                if ipart:
                    out.append((ipart, bold, i_i % 2 == 1, False))
    return out


def plain(cell):
    return re.sub(r"\s+", " ", "".join(s[0] for s in segments(cell))).strip()


def tokens(cell):
    """Words with style and whether a space precedes them (spaces become wrap points)."""
    toks, pending_space = [], False
    for text, bold, italic, mono in segments(cell):
        for piece in re.split(r"(\s+)", text):
            if not piece:
                continue
            if piece.isspace():
                pending_space = True
                continue
            toks.append({"t": piece, "b": bold, "i": italic, "m": mono, "sp": pending_space and bool(toks)})
            pending_space = False
    return toks


def find_tables(lines):
    """[(start, end_exclusive, heading)] for every GFM table outside fenced code."""
    tables, heading, fence, i = [], "", None, 0
    while i < len(lines):
        ln = lines[i]
        m_f = re.match(r"^\s*(```|~~~)", ln)
        if m_f:
            fence = None if fence == m_f.group(1) else (fence or m_f.group(1))
            i += 1
            continue
        if fence:
            i += 1
            continue
        m_h = HEADING_RE.match(ln)
        if m_h:
            heading = m_h.group(2)
        if ln.startswith("|") and i + 1 < len(lines) and SEP_RE.match(lines[i + 1].rstrip()):
            j = i + 2
            while j < len(lines) and lines[j].startswith("|"):
                j += 1
            tables.append((i, j, heading))
            i = j
            continue
        i += 1
    return tables


# ---------- rendering ----------

class Measure:
    def __init__(self):
        self.fig = plt.figure(dpi=DPI)
        self.r = self.fig.canvas.get_renderer()
        self.cache = {}

    def prop(self, size, bold, italic, mono):
        return FontProperties(family="DejaVu Sans Mono" if mono else "DejaVu Sans",
                              weight="bold" if bold else "normal",
                              style="italic" if italic else "normal", size=size)

    def width(self, s, size, bold=False, italic=False, mono=False):
        key = (s, size, bold, italic, mono)
        if key not in self.cache:
            p = self.prop(size, bold, italic, mono)
            self.cache[key] = self.r.get_text_width_height_descent(s, p, ismath=False)[0]
        return self.cache[key]

    def tok_w(self, tok, size):
        return self.width(tok["t"], size, tok["b"], tok["i"], tok["m"])

    def space_w(self, size):
        return self.width("x x", size) - self.width("xx", size)

    def line_w(self, toks, size):
        return sum(self.tok_w(t, size) for t in toks) + sum(self.space_w(size) for t in toks[1:] if t["sp"])


def words(toks):
    """Group tokens glued without a space (e.g. `code` + ')') into unbreakable words."""
    out = []
    for t in toks:
        unit = re.fullmatch(r"(ms|s|k|GiB|MB)[),;]*", t["t"]) and out and out[-1][-1]["t"][-1:].isdigit()
        if out and (not t["sp"] or unit):
            out[-1].append(t)
        else:
            out.append([t])
    return out


def wrap(M, toks, size, width):
    """Greedy wrap at spaces; a word never splits, so width >= widest word by construction."""
    lines, cur = [], []
    for w in words(toks):
        if cur and M.line_w(cur + w, size) > width + 0.5:
            lines.append(cur)
            cur = [dict(w[0], sp=False)] + w[1:]
        else:
            cur += ([dict(w[0], sp=False)] + w[1:]) if not cur else w
    if cur or not lines:
        lines.append(cur)
    return lines


TEXT_COL_ONE_LINE = 0.34  # first try: a text column stays on one line up to this share of the width
UNIT_WORDS = {"ms", "GiB", "MB", "evt", "pod", "na"}


def is_numeric(toks):
    """A number-like cell (e.g. '27.4 (23-33)', '6.9 / 1,575', '10 s, 9 s'): never wrapped."""
    runs = re.findall(r"[A-Za-z]{3,}", " ".join(t["t"] for t in toks))
    return bool(toks) and not [r for r in runs if r not in UNIT_WORDS]


def layout_widths(M, header, rows, aligns, size, text_cap):
    """Natural and minimum content widths per column (no padding).

    Headers always may wrap; numeric cells never; text cells wrap only past text_cap * width
    (text_cap=0: wrap text cells down to their longest word)."""
    n = len(header)
    avail = WIDTH_PX - 2 * MARGIN - 2 * PAD_X * n
    head_nat = [M.line_w(header[c], size) for c in range(n)]
    nat, lo, data_nat = [], [], []
    for c in range(n):
        cells = [r[c] for r in rows]
        mw = max([M.line_w(w, size) for cell in [header[c]] + cells for w in words(cell)] or [0])
        need = mw
        for cell in cells:
            w = M.line_w(cell, size)
            need = max(need, w if is_numeric(cell) else min(w, text_cap * avail))
        data_nat.append(max([M.line_w(cell, size) for cell in cells] or [0]))
        nat.append(max(head_nat[c], data_nat[-1]))
        lo.append(need)
    return avail, nat, lo, data_nat


def waterfill(stages, avail):
    """Column widths summing to avail. stages = [minimum, target1, target2, ...]: every column
    gets its minimum, then the spare width lifts columns toward each target in turn (the widest
    capped at a common level); whatever is left after the last target is shared proportionally."""
    w = list(stages[0])
    assert sum(w) <= avail + 1e-6
    for tgt in stages[1:]:
        tgt = [max(x, t) for x, t in zip(w, tgt)]
        if sum(tgt) <= avail:
            w = tgt
            continue
        a, b = 0.0, max(tgt)
        for _ in range(60):
            c = (a + b) / 2
            if sum(max(x, min(t, c)) for x, t in zip(w, tgt)) > avail:
                b = c
            else:
                a = c
        w = [max(x, min(t, a)) for x, t in zip(w, tgt)]
        break
    tot = sum(w) or 1
    return [x + (avail - sum(w)) * x / tot for x in w]


def choose_layout(M, header, rows, aligns):
    size = FONT_MAX
    while size >= FONT_MIN - 1e-9:
        for cap in (TEXT_COL_ONE_LINE, 0.0):
            avail, nat, lo, data_nat = layout_widths(M, header, rows, aligns, size, cap)
            if sum(lo) <= avail:
                return size, waterfill([lo, data_nat, nat], avail)  # unwrap data first, then headers
        size -= FONT_STEP
    raise AssertionError("table does not fit at the smallest font even with text cells wrapped")


def render(M, header_raw, rows_raw, aligns, path):
    header = [[dict(t, b=True) for t in tokens(c)] for c in header_raw]  # measured as drawn: bold
    rows = [[tokens(c) for c in r] for r in rows_raw]
    n = len(header)
    size, widths = choose_layout(M, header, rows, aligns)
    line_h = size / 72 * DPI * LINE_SPACING
    asc = size / 72 * DPI * 0.93   # baseline offset from the line box top (DejaVu ascent ~0.93 em)

    all_rows = [header] + rows
    wrapped = [[wrap(M, r[c], size, widths[c]) for c in range(n)] for r in all_rows]
    data_wrapped = any(len(cell) > 1 for r in wrapped[1:] for cell in r)
    heights = [max(len(cell) for cell in r) * line_h + 2 * PAD_Y for r in wrapped]
    for r in wrapped:   # no rendered line may exceed its column (no clipping)
        for c in range(n):
            for ln in r[c]:
                assert M.line_w(ln, size) <= widths[c] + 0.5, (path, ln)

    W = WIDTH_PX
    H = int(round(sum(heights) + 2 * MARGIN))
    fig = plt.figure(figsize=(W / DPI, H / DPI), dpi=DPI)
    fig.patch.set_facecolor("white")
    xs = [MARGIN]
    for w in widths:
        xs.append(xs[-1] + w + 2 * PAD_X)

    def fx(px):
        return px / W

    def fy(px_from_top):
        return 1 - px_from_top / H

    rendered = []
    y = MARGIN
    for ri, (r, h) in enumerate(zip(wrapped, heights)):
        fill = HEADER_FILL if ri == 0 else (ZEBRA if ri % 2 == 0 else None)
        if fill:
            fig.patches.append(plt.Rectangle((fx(xs[0]), fy(y + h)), fx(xs[-1] - xs[0]), h / H,
                                             transform=fig.transFigure, color=fill, zorder=0))
        row_text = []
        for c in range(n):
            lines = r[c]
            top = y + (h - len(lines) * line_h) / 2
            for li, ln in enumerate(lines):
                lw = M.line_w(ln, size)
                if aligns[c] == "right":
                    x = xs[c + 1] - PAD_X - lw
                elif aligns[c] == "center":
                    x = xs[c] + PAD_X + (widths[c] - lw) / 2
                else:
                    x = xs[c] + PAD_X
                base = top + li * line_h + asc + (line_h - size / 72 * DPI) / 2
                for k, t in enumerate(ln):
                    if k and t["sp"]:
                        x += M.space_w(size)
                    bold = t["b"]
                    fig.text(fx(x), fy(base), t["t"], fontproperties=M.prop(size, bold, t["i"], t["m"]),
                             color=TEXT, va="baseline", ha="left", parse_math=False)
                    x += M.width(t["t"], size, bold, t["i"], t["m"])
            # What was drawn, as text: a line break stands for the space it replaced.
            s = ""
            for li, ln in enumerate(lines):
                for k, t in enumerate(ln):
                    s += (" " if s and (k == 0 or t["sp"]) else "") + t["t"]
            row_text.append(s)
        rendered.append(row_text)
        y += h
        rule_c, rule_w = (RULE_HEAVY, 1.2) if ri == 0 else (RULE, 0.6)
        fig.add_artist(plt.Line2D([fx(xs[0]), fx(xs[-1])], [fy(y), fy(y)], color=rule_c, lw=rule_w))
    fig.add_artist(plt.Line2D([fx(xs[0]), fx(xs[-1])], [fy(MARGIN), fy(MARGIN)], color=RULE_HEAVY, lw=1.2))
    fig.savefig(path, dpi=DPI, facecolor="white")
    plt.close(fig)
    return rendered, size, data_wrapped, H


def main():
    with open(SRC) as fh:
        lines = fh.read().split("\n")
    tables = find_tables(lines)
    assert tables, "no tables found"
    os.makedirs(OUT, exist_ok=True)
    for old in glob.glob(os.path.join(OUT, "t[0-9][0-9]_*.png")):
        os.remove(old)

    M = Measure()
    repl = {}
    for k, (s, e, heading) in enumerate(tables, 1):
        header = split_row(lines[s])
        aligns = alignments(lines[s + 1])
        rows = [split_row(ln) for ln in lines[s + 2:e]]
        assert len(aligns) == len(header), (s + 1, "separator/header column count")
        for r in rows:
            assert len(r) == len(header), (s + 1, "row column count", r)
        slug = slugify(heading) or "table"
        name = f"t{k:02d}_{slug}.png"
        rendered, size, data_wrapped, H = render(M, header, rows, aligns, os.path.join(OUT, name))
        # Every source cell value must be exactly what was drawn (markup stripped, whitespace collapsed).
        for raw_r, got_r in zip([header] + rows, rendered):
            for raw, got in zip(raw_r, got_r):
                want = plain(raw)
                assert want == got, (name, raw, got)
                assert want == re.sub(r"\s+", " ", re.sub(r"[*`]", "", raw)).strip(), (name, raw)
        alt = ALT.get(slug) or f"Table ({heading}): {', '.join(plain(h) for h in header)}"
        assert "]" not in alt and "\n" not in alt
        repl[s] = (e, f"![{alt}]({OUT_REL}/{name})")
        print(f"{name}: {len(header)} cols x {len(rows)} rows, {size:g} pt, {WIDTH_PX}x{H} px"
              + (", data cells wrapped" if data_wrapped else ""))

    out, i = [], 0
    while i < len(lines):
        if i in repl:
            e, img = repl[i]
            out.append(img)
            i = e
        else:
            out.append(lines[i])
            i += 1
    text = "\n".join(out)

    # Header comment: Medium upload order = every image, in order of appearance.
    m = re.match(r"<!--\n(.*?)\n-->\n", text, re.S)
    assert m, "expected the HTML comment at the top of the post"
    imgs = [os.path.relpath(p, "charts") if p.startswith("charts/") else p
            for p in re.findall(r"!\[[^\]]*\]\(([^)]+)\)", text[m.end():])]
    keep = m.group(1).split("\n\n  Medium upload:")[0]
    order = ["  Medium upload: images in post/charts/ (tables/ = rendered tables), in order of appearance:"]
    for j in range(0, len(imgs), 3):
        order.append("    " + "  ".join(imgs[j:j + 3]))
    note = ("  Medium copy, generated by bench/make_table_images.py from piece1-4.3.md: identical except\n"
            "  each table is an image (Medium has no tables). Do not edit by hand; edit the post and re-run.")
    text = "<!--\n" + keep + "\n\n" + note + "\n\n" + "\n".join(order) + "\n-->\n" + text[m.end():]
    with open(DST, "w") as fh:
        fh.write(text)
    print(f"wrote {os.path.relpath(DST, ROOT)}: {len(tables)} tables replaced, {len(imgs)} images listed")


if __name__ == "__main__":
    main()
