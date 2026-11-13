#!/usr/bin/env python3
"""
Latency consumer for the Spark RTM vs Flink benchmark.

Reads the OUTPUT topic and, per record, computes:
  - pipeline latency   = out_ts - ts            (engine: Kafka-in -> transform -> Kafka-out)
  - end-to-end latency = consume_wallclock - ts  (adds the output-topic read)

Drops the first WARMUP_SECONDS of *wall clock* as warmup, then reports
p50/p95/p99/max + throughput. Writes two artifacts under results/:
  - <engine>_latency.csv     one row per kept record (for re-analysis)
  - <engine>_summary.json    the computed percentiles + counts

Env / CLI (CLI overrides env):
  --bootstrap   KAFKA_BOOTSTRAP_HOST   (default localhost:29092)
  --topic       OUTPUT_TOPIC           (default output-events)
  --engine      label for output files (e.g. spark-rtm, spark-microbatch, flink)
  --warmup      WARMUP_SECONDS         (default 15)
  --seconds     RUN_SECONDS            (default 120; total wall time incl. warmup)
  --outdir      results dir            (default ./results)
  --isolation   read_uncommitted|read_committed (default read_uncommitted; use read_committed
                for the Flink EXACTLY_ONCE transactional-sink arm)

Stateful outputs: records carrying `window_end` (epoch seconds, S1 windowed aggregation) also
get window latency = out_ts - window_end, reported as "window_ms" (negative for update-mode
early results emitted before the window closes). See STATEFUL_CONFIG.md for the definitions.
"""
import argparse
import json
import os
import signal
import statistics as st
import sys
import time
from array import array

from confluent_kafka import Consumer, KafkaError

_stop = False


def _handle_sig(_s, _f):
    global _stop
    _stop = True


def pct(sorted_vals, q):
    if not sorted_vals:
        return float("nan")
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    pos = q * (len(sorted_vals) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def hist_bucket(ms):
    """Lower bound of the latency bucket: 1 ms steps <100 ms, 10 ms <2 s, 100 ms beyond."""
    if ms < 100:
        return int(ms // 1)
    if ms < 2000:
        return int(ms // 10 * 10)
    return int(ms // 100 * 100)


def describe(vals):
    """count/p50/p95/p99/max/mean of a float array; linear-interpolated percentiles (same as
    pct()). numpy when installed (EKS load image), sorted() fallback otherwise."""
    if not vals:
        return {"count": 0, "p50": None, "p95": None, "p99": None, "max": None, "mean": None}
    try:
        import numpy as np
        a = np.frombuffer(vals, dtype=np.float32).astype(np.float64)
        p50, p95, p99 = np.percentile(a, [50, 95, 99])
        mx, mean = float(a.max()), float(a.mean())
    except ImportError:
        s = sorted(vals)
        p50, p95, p99 = pct(s, 0.50), pct(s, 0.95), pct(s, 0.99)
        mx, mean = s[-1], st.mean(s)
    return {"count": len(vals), "p50": round(float(p50), 3), "p95": round(float(p95), 3),
            "p99": round(float(p99), 3), "max": round(mx, 3), "mean": round(mean, 3)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_HOST", "localhost:29092"))
    ap.add_argument("--topic", default=os.getenv("OUTPUT_TOPIC", "output-events"))
    ap.add_argument("--engine", required=True)
    ap.add_argument("--warmup", type=int, default=int(os.getenv("WARMUP_SECONDS", "15")))
    ap.add_argument("--seconds", type=int, default=int(os.getenv("RUN_SECONDS", "120")))
    ap.add_argument("--outdir", default="results")
    # Print a compact per-event-second latency histogram (one "HISTJSON {...}" stdout line)
    # so a remote run (EKS pod) can split the tail by checkpoint boundary without shipping
    # the multi-GB per-record CSV off the pod.
    ap.add_argument("--hist", action="store_true")
    ap.add_argument("--isolation", default=os.getenv("ISOLATION_LEVEL", "read_uncommitted"),
                    choices=["read_uncommitted", "read_committed"])
    ap.add_argument("--no-raw", action="store_true",
                    help="skip the per-record CSV (EKS: tens of millions of rows per run)")
    # Recovery test: highest window count seen per (user, window) for user_id % N == 0
    # ("MAXJSON {...}", compared with the producers' TRUTHJSON). 0 = off.
    ap.add_argument("--truth-sample", type=int, default=0)
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)
    os.makedirs(args.outdir, exist_ok=True)

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": f"latency-{args.engine}-{int(time.time())}",
        "auto.offset.reset": "latest",   # only measure records produced during this run
        "enable.auto.commit": False,
        "fetch.wait.max.ms": 10,
        "isolation.level": args.isolation,
    })
    consumer.subscribe([args.topic])

    print(f"[consumer] <- {args.bootstrap} topic={args.topic} engine={args.engine} "
          f"warmup={args.warmup}s window={args.seconds - args.warmup}s", flush=True)

    # Compact float32 arrays (4 B/value): the stateful runs emit ~one record per input
    # (~36M per EKS run), which Python float lists (~32 B/value) do not fit in 2 GiB.
    pipe = array("f")   # pipeline latencies (ms), steady-state only
    e2e = array("f")    # end-to-end latencies (ms), steady-state only
    win = array("f")    # out_ts - window_end (ms), steady-state only, windowed outputs only
    raw_path = os.path.join(args.outdir, f"{args.engine}_latency.csv")
    raw = None
    if not args.no_raw:
        raw = open(raw_path, "w")
        raw.write("recv_ts,event_id,ts,out_ts,pipeline_ms,e2e_ms\n")

    hist = {}   # event-second -> {pipeline-latency bucket (ms, lower bound): count}
    rate = {}   # receive-second -> [records, sum of e2e ms] (recovery: outage + catch-up)
    maxcnt = {}  # "user:window_start_ms" -> highest cnt seen (--truth-sample)
    max_out_ts = 0.0
    started = time.time()
    kept = 0
    seen = 0
    last_report = started
    in_warmup = True

    while not _stop:
        now = time.time()
        if (now - started) >= args.seconds:
            break

        # Batch fetch: per-message poll() cannot keep up with ~100k records/s in one process.
        for msg in consumer.consume(1000, 0.2):
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    pass
                continue

            recv = time.time()
            try:
                evt = json.loads(msg.value())
                ts = float(evt["ts"])
                out_ts = float(evt["out_ts"])
            except (ValueError, KeyError, TypeError):
                continue

            seen += 1
            in_warmup = (recv - started) < args.warmup
            pipeline_ms = (out_ts - ts) * 1000.0
            e2e_ms = (recv - ts) * 1000.0

            if raw is not None:
                raw.write(f"{recv:.6f},{evt.get('event_id','')},{ts:.6f},{out_ts:.6f},"
                          f"{pipeline_ms:.3f},{e2e_ms:.3f}\n")
            if args.hist:
                r = rate.setdefault(int(recv), [0, 0.0])
                r[0] += 1
                r[1] += e2e_ms
            if args.truth_sample > 0 and "window_start" in evt:
                u = int(evt["user_id"])
                if u != 0 and u % args.truth_sample == 0:
                    k = f"{u}:{int(round(float(evt['window_start']) * 1000))}"
                    c = int(evt["cnt"])
                    if c > maxcnt.get(k, 0):
                        maxcnt[k] = c
                    max_out_ts = max(max_out_ts, ts)

            if not in_warmup:
                pipe.append(pipeline_ms)
                e2e.append(e2e_ms)
                if "window_end" in evt:
                    win.append((out_ts - float(evt["window_end"])) * 1000.0)
                kept += 1
                if args.hist:
                    sec = hist.setdefault(int(ts), {})
                    b = hist_bucket(pipeline_ms)
                    sec[b] = sec.get(b, 0) + 1

        if now - last_report >= 5.0:
            print(f"[consumer] seen={seen} kept={kept} "
                  f"({'warmup' if in_warmup else 'steady'})", flush=True)
            last_report = now

    if raw is not None:
        raw.close()
    consumer.close()

    window = max(1e-9, args.seconds - args.warmup)
    summary = {
        "engine": args.engine,
        "topic": args.topic,
        "kept_records": kept,
        "seen_records": seen,
        "window_seconds": args.seconds - args.warmup,
        "warmup_seconds": args.warmup,
        "throughput_eps": round(kept / window, 1),
        "started_at": round(started, 3),
        "ended_at": round(time.time(), 3),
    }

    series = [("pipeline_ms", pipe), ("e2e_ms", e2e)]
    if win:
        series.append(("window_ms", win))
    if args.isolation != "read_uncommitted":
        summary["isolation"] = args.isolation
    for name, vals in series:
        summary[name] = describe(vals)

    out_json = os.path.join(args.outdir, f"{args.engine}_summary.json")
    with open(out_json, "w") as f:
        json.dump(summary, f, indent=2)

    print("\n========== LATENCY SUMMARY ==========", flush=True)
    print(json.dumps(summary, indent=2), flush=True)
    print(f"[consumer] wrote {raw_path if raw is not None else '(no raw csv)'} and {out_json}", flush=True)
    if args.hist:
        print("HISTJSON " + json.dumps(hist, separators=(",", ":")), flush=True)
        print("RATEJSON " + json.dumps({k: [v[0], round(v[1], 1)] for k, v in rate.items()},
                                       separators=(",", ":")), flush=True)
    if args.truth_sample > 0:
        print("MAXJSON " + json.dumps({"max_out_ts": max_out_ts, "max": maxcnt},
                                      separators=(",", ":")), flush=True)

    if kept == 0:
        print("[consumer] WARNING: zero steady-state records — check the pipeline/topic.",
              file=sys.stderr, flush=True)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
