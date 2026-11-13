#!/usr/bin/env python3
"""
Correctness checker for the stateful workloads. Computes the ground truth from a capture of
the INPUT topic (common/topic_capture.py) and diffs each engine's captured OUTPUT topic
against it. Heartbeat events (user_id 0) are ignored everywhere.

  s1  per (user_id, window_start): count and sum(amount) must match exactly (sum compared in
      cents; every amount has 2 decimals). For update-mode engines (Spark RTM / micro-batch,
      Flink s1-update) the FINAL value of a key is its emission with the largest count (counts
      only grow); for final-only engines (Flink window / SQL TUMBLE) every key must be emitted
      exactly once. Both are reported.
  s2  the emitted event_ids must equal the set of distinct input event_ids, each exactly once.
  s3  sessions per user, in arrival order (per-partition offset order): a new event whose
      ts - last_ts > gap closes the open session; last_ts = max(last_ts, ts) because
      duplicates arrive late; every session still open at the end closes by timeout. The
      multiset of (user_id, session_start, session_end, events) must match exactly.
  s4  stream-static join: every input event whose user has a reference row
      (common/joindata.py) is emitted once per occurrence with that user's tier/region;
      events of users without a row are not emitted.
  s5  stream-stream interval join: exactly one output per (order, payment) pair with
      0 <= pay_ts - order_ts <= bound; zero missing, zero spurious (orders without a payment
      and payments outside the bound produce nothing). --input = orders, --payments = payments.

Usage:
  stateful_check.py --workload s1 --input in.jsonl [--window-s 10] [--gap-ms 5000]
                    [--out report.json] NAME=capture.jsonl [NAME=capture.jsonl ...]
Exit code 0 if every engine passes, 1 otherwise.
"""
import argparse
import json
import os
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
from joindata import has_ref, ref_attrs  # noqa: E402


def load(path):
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    rows.sort(key=lambda r: (r["p"], r["o"]))
    return rows


def input_events(rows):
    """Valid, non-heartbeat input events in per-partition arrival order."""
    out = []
    for r in rows:
        v = r["v"]
        if v.get("user_id") in (None, 0) or v.get("ts") is None:
            continue
        out.append(v)
    return out


def cents(x):
    return int(round(float(x) * 100))


# ---------------- S1 ----------------
def truth_s1(events, window_ms):
    t = defaultdict(lambda: [0, 0])
    for e in events:
        ts_ms = int(float(e["ts"]) * 1000)
        ws = ts_ms - ts_ms % window_ms
        a = t[(int(e["user_id"]), ws)]
        a[0] += 1
        a[1] += cents(e["amount"])
    return {k: tuple(v) for k, v in t.items()}


def engine_s1(rows):
    final = {}
    emissions = Counter()
    for r in rows:
        v = r["v"]
        u = int(v["user_id"])
        if u == 0:
            continue
        k = (u, int(round(float(v["window_start"]) * 1000)))
        emissions[k] += 1
        cnt = int(v["cnt"])
        if k not in final or cnt > final[k][0]:
            final[k] = (cnt, cents(v["sum_amount"]))
    return final, emissions


def diff_s1(truth, rows):
    got, emissions = engine_s1(rows)
    missing = [k for k in truth if k not in got]
    extra = [k for k in got if k not in truth]
    cnt_bad = [k for k in truth if k in got and got[k][0] != truth[k][0]]
    sum_bad = [k for k in truth if k in got and got[k][0] == truth[k][0] and got[k][1] != truth[k][1]]
    multi = sum(1 for n in emissions.values() if n > 1)
    rep = {
        "truth_keys": len(truth), "engine_keys": len(got), "output_records": len(rows),
        "missing_keys": len(missing), "extra_keys": len(extra),
        "count_mismatch": len(cnt_bad), "sum_mismatch": len(sum_bad),
        "keys_emitted_more_than_once": multi,
        "max_emissions_per_key": max(emissions.values()) if emissions else 0,
        "samples": {
            "missing": [list(k) + list(truth[k]) for k in missing[:5]],
            "extra": [list(k) + list(got[k]) for k in extra[:5]],
            "count_mismatch": [[*k, "truth", *truth[k], "engine", *got[k]] for k in cnt_bad[:5]],
        },
    }
    rep["pass"] = not (missing or extra or cnt_bad or sum_bad)
    return rep


# ---------------- S2 ----------------
def truth_s2(events):
    return {int(e["event_id"]) for e in events}


def diff_s2(truth, rows):
    c = Counter(int(r["v"]["event_id"]) for r in rows if int(r["v"]["user_id"]) != 0)
    missing = truth - set(c)
    extra = set(c) - truth
    dup = [k for k, n in c.items() if n > 1]
    rep = {
        "unique_input_events": len(truth), "output_records": sum(c.values()),
        "distinct_output_event_ids": len(c), "missing": len(missing), "extra": len(extra),
        "emitted_more_than_once": len(dup),
        "samples": {"missing": sorted(missing)[:5], "dup": sorted(dup)[:5]},
    }
    rep["pass"] = not (missing or extra or dup)
    return rep


# ---------------- S3 ----------------
def truth_s3(events, gap_ms):
    open_s = {}   # user -> [start, last, count]
    closed = Counter()
    for e in events:
        u = int(e["user_id"])
        ts = float(e["ts"])
        s = open_s.get(u)
        if s is None:
            open_s[u] = [ts, ts, 1]
        elif (ts - s[1]) * 1000.0 > gap_ms:
            closed[(u, s[0], s[1], s[2])] += 1
            open_s[u] = [ts, ts, 1]
        else:
            s[1] = max(s[1], ts)
            s[2] += 1
    for u, s in open_s.items():
        closed[(u, s[0], s[1], s[2])] += 1
    return closed


def diff_s3(truth, rows):
    got = Counter()
    reasons = Counter()
    for r in rows:
        v = r["v"]
        if int(v["user_id"]) == 0:
            continue
        got[(int(v["user_id"]), float(v["session_start"]), float(v["session_end"]), int(v["events"]))] += 1
        reasons[v.get("reason")] += 1
    missing = truth - got
    extra = got - truth
    rep = {
        "truth_sessions": sum(truth.values()), "output_sessions": sum(got.values()),
        "missing": sum(missing.values()), "extra": sum(extra.values()),
        "close_reasons": dict(reasons),
        "samples": {"missing": [list(k) for k in list(missing)[:5]],
                    "extra": [list(k) for k in list(extra)[:5]]},
    }
    rep["pass"] = not (missing or extra)
    return rep


# ---------------- S4 ----------------
def truth_s4(events, ref_rows, miss_mod):
    """event_id -> expected (occurrences, tier, region): every input event whose user has a
    reference row is enriched once per occurrence (duplicates included: a stateless join)."""
    occ = Counter(int(e["event_id"]) for e in events if has_ref(e["user_id"], ref_rows, miss_mod))
    user = {int(e["event_id"]): int(e["user_id"]) for e in events}
    return {eid: (n, *ref_attrs(user[eid])) for eid, n in occ.items()}


def diff_s4(truth, rows):
    c = Counter()
    wrong = []
    for r in rows:
        v = r["v"]
        eid = int(v["event_id"])
        c[eid] += 1
        t = truth.get(eid)
        if t is not None and (v.get("tier"), v.get("region")) != t[1:]:
            wrong.append(eid)
    missing = [k for k, t in truth.items() if c[k] < t[0]]
    extra = [k for k in c if k not in truth or c[k] > truth[k][0]]
    rep = {
        "truth_enriched_records": sum(t[0] for t in truth.values()), "output_records": sum(c.values()),
        "missing": len(missing), "extra": len(extra), "wrong_enrichment": len(wrong),
        "samples": {"missing": sorted(missing)[:5], "extra": sorted(extra)[:5], "wrong": wrong[:5]},
    }
    rep["pass"] = not (missing or extra or wrong)
    return rep


# ---------------- S5 ----------------
def truth_s5(orders, payments, bound_s):
    """order_id -> True for every (order, payment) pair with 0 <= pay_ts - order_ts <= bound.
    Payments outside the bound (and orders without a payment) must produce nothing."""
    o_ts = {int(o["order_id"]): float(o["ts"]) for o in orders}
    pairs = set()
    for p in payments:
        oid = int(p["order_id"])
        if oid in o_ts and 0.0 <= float(p["ts"]) - o_ts[oid] <= bound_s:
            pairs.add(oid)
    return pairs


def diff_s5(truth, rows):
    c = Counter(int(r["v"]["order_id"]) for r in rows)
    missing = truth - set(c)
    spurious = set(c) - truth
    dup = [k for k, n in c.items() if n > 1]
    rep = {
        "truth_pairs": len(truth), "output_records": sum(c.values()), "missing": len(missing),
        "spurious": len(spurious), "emitted_more_than_once": len(dup),
        "samples": {"missing": sorted(missing)[:5], "spurious": sorted(spurious)[:5], "dup": sorted(dup)[:5]},
    }
    rep["pass"] = not (missing or spurious or dup)
    return rep


def raw_values(path):
    return [r["v"] for r in load(path)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workload", required=True, choices=["s1", "s2", "s3", "s4", "s5"])
    ap.add_argument("--input", required=True, help="input capture (s5: the ORDERS capture)")
    ap.add_argument("--payments", help="s5: the PAYMENTS capture")
    ap.add_argument("--window-s", type=int, default=10)
    ap.add_argument("--gap-ms", type=float, default=5000)
    ap.add_argument("--ref-rows", type=int, default=1000000)
    ap.add_argument("--ref-miss-mod", type=int, default=50)
    ap.add_argument("--join-bound-s", type=float, default=10.0)
    ap.add_argument("--out")
    ap.add_argument("engines", nargs="+", help="NAME=capture.jsonl")
    args = ap.parse_args()

    if args.workload == "s5":
        orders = raw_values(args.input)
        payments = raw_values(args.payments)
        truth = truth_s5(orders, payments, args.join_bound_s)
        report = {"workload": "s5", "input": args.input, "payments": args.payments,
                  "orders": len(orders), "payments_in": len(payments),
                  "payments_outside_bound": len(payments) - len(truth), "engines": {}}
        return check_engines(args, report, truth, diff_s5)

    events = input_events(load(args.input))
    if args.workload == "s4":
        truth = truth_s4(events, args.ref_rows, args.ref_miss_mod)
        differ = diff_s4
    elif args.workload == "s1":
        truth = truth_s1(events, args.window_s * 1000)
        differ = diff_s1
    elif args.workload == "s2":
        truth = truth_s2(events)
        differ = diff_s2
    else:
        truth = truth_s3(events, args.gap_ms)
        differ = diff_s3

    report = {"workload": args.workload, "input": args.input, "input_events": len(events),
              "input_distinct_event_ids": len({e["event_id"] for e in events}), "engines": {}}
    return check_engines(args, report, truth, differ)


def check_engines(args, report, truth, differ):
    ok = True
    for spec in args.engines:
        name, path = spec.split("=", 1)
        rep = differ(truth, load(path))
        report["engines"][name] = rep
        ok &= rep["pass"]
        print(f"[{args.workload}] {name:24s} {'PASS' if rep['pass'] else 'FAIL'}  "
              + json.dumps({k: v for k, v in rep.items() if k not in ("samples", "pass")}))
    if args.out:
        with open(args.out, "w") as f:
            json.dump(report, f, indent=2)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
