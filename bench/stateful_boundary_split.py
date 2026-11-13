#!/usr/bin/env python3
"""Split a smoke run's pipeline latency by commit boundary.

Boundaries: Spark = the driver's "Starting job" log lines after the first (one long-running
job per RTM batch, so each is a batch transition = a synchronous commit + relaunch);
Flink = checkpoint trigger timestamps from the run's evidence file. A record is "near" a
boundary if its event ts lies in [b - 2 s, b + 3 s].

Usage: stateful_boundary_split.py LATENCY_CSV[.gz] (--spark-log SUBMIT_LOG | --flink-evidence EVIDENCE)
Prints one JSON line.
"""
import argparse
import calendar
import gzip
import json
import re
import time


def pct(v, q):
    if not v:
        return None
    v = sorted(v)
    return round(v[min(len(v) - 1, int(q * (len(v) - 1) + 0.5))], 1)


def spark_boundaries(path):
    starts = []
    for line in open(path, errors="replace"):
        m = re.match(r"(\d\d/\d\d/\d\d \d\d:\d\d:\d\d) INFO SparkContext: Starting job:", line)
        if m:
            starts.append(calendar.timegm(time.strptime(m.group(1), "%y/%m/%d %H:%M:%S")))
    return starts[1:]


def flink_boundaries(path):
    return [int(t) / 1000.0 for t in re.findall(r"ckpt \d+ COMPLETED trigger (\d+)", open(path).read())]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv")
    ap.add_argument("--spark-log")
    ap.add_argument("--flink-evidence")
    ap.add_argument("--warmup", type=float, default=20.0, help="drop rows received in the first N s")
    a = ap.parse_args()
    bounds = spark_boundaries(a.spark_log) if a.spark_log else flink_boundaries(a.flink_evidence)
    opener = gzip.open if a.csv.endswith(".gz") else open
    near, far = [], []
    first = None
    with opener(a.csv, "rt") as f:
        next(f)
        for line in f:
            recv, _eid, ts, _out, pipe, _e2e = line.rstrip("\n").split(",")
            recv, ts, pipe = float(recv), float(ts), float(pipe)
            first = recv if first is None else first
            if recv - first < a.warmup:
                continue
            (near if any(b - 2 <= ts <= b + 3 for b in bounds) else far).append(pipe)
    print(json.dumps({"csv": a.csv, "boundaries": len(bounds),
                      "near": {"n": len(near), "p50": pct(near, .5), "p99": pct(near, .99), "max": round(max(near), 1) if near else None},
                      "elsewhere": {"n": len(far), "p50": pct(far, .5), "p99": pct(far, .99), "max": round(max(far), 1) if far else None},
                      "all_p99": pct(near + far, .99)}))


if __name__ == "__main__":
    main()
