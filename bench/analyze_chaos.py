#!/usr/bin/env python3
"""
Post-process a latency CSV from a CHAOS run (worker killed mid-stream) to report:
  - recovery downtime: the largest gap between consecutive output arrivals (recv_ts),
    i.e. how long output stalled after the kill
  - duplicates: output records sharing an event_id (at-least-once replay signature)
  - distinct vs total records

Usage: python bench/analyze_chaos.py <engine_label>  (reads results/<label>_latency.csv)
The producer is single-process here, so event_id is a unique monotonic sequence within a
run — duplicates therefore mean the sink re-emitted records (at-least-once on replay).
"""
import csv
import sys
import os

label = sys.argv[1]
# Accept either a bare label (results/<label>_latency.csv) or a path-ish label
# (results/eks/<name>) so EKS chaos artifacts can be analyzed in place.
base = os.path.join(os.path.dirname(__file__), "..", "results")
cand = os.path.join(base, f"{label}_latency.csv")
path = cand if os.path.exists(cand) else os.path.join(base, "eks", f"{os.path.basename(label)}_latency.csv")

recv = []
ids = {}
with open(path) as f:
    for r in csv.DictReader(f):
        try:
            t = float(r["recv_ts"]); eid = r["event_id"]
        except (ValueError, KeyError):
            continue
        recv.append(t)
        ids[eid] = ids.get(eid, 0) + 1

recv.sort()
gaps = [(recv[i] - recv[i-1]) for i in range(1, len(recv))]
max_gap = max(gaps) if gaps else 0.0
dupes = sum(c - 1 for c in ids.values() if c > 1)
dup_ids = sum(1 for c in ids.values() if c > 1)

print(f"=== CHAOS: {label} ===")
print(f"total output records : {len(recv)}")
print(f"distinct event_ids   : {len(ids)}")
print(f"duplicate records    : {dupes}  (across {dup_ids} event_ids)")
print(f"duplicate rate       : {100*dupes/len(recv):.3f}%" if recv else "n/a")
print(f"recovery downtime    : {max_gap*1000:.0f} ms  (largest output stall = worker-loss gap)")
print(f"span                 : {recv[-1]-recv[0]:.1f}s" if recv else "n/a")
