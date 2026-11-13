#!/usr/bin/env python3
"""
Long-run (soak) latency consumer for the 4-hour Spark RTM vs Flink stability test.

Unlike latency_consumer.py (which accumulates everything and emits ONE summary), this
emits a percentile SNAPSHOT every --window-seconds (default 1800 = 30 min) and then CLEARS
the window buffer. That keeps memory bounded over 4h and produces a time-series so we can
see latency drift, throughput decay, and tail degradation over time.

Each window snapshot is appended as one JSON line to <engine>_soak.jsonl. A short warmup at
the very start is skipped. Output topic + consumer group are per-engine so two engines can
run concurrently on the same input.

Args: --topic --engine --window-seconds --total-seconds --warmup --bootstrap --outdir
"""
import argparse
import json
import os
import signal
import time

from confluent_kafka import Consumer, KafkaError

_stop = False


def _sig(_s, _f):
    global _stop
    _stop = True


def pct(sorted_vals, q):
    if not sorted_vals:
        return None
    i = min(len(sorted_vals) - 1, int(q * len(sorted_vals)))
    return sorted_vals[i]


def flush_window(engine, topic, win_idx, t_start, pipe, e2e, seen, out_path):
    """Compute + append one window snapshot, return the snapshot dict."""
    dur = max(1e-9, time.time() - t_start)
    sp = sorted(pipe)
    se = sorted(e2e)
    snap = {
        "engine": engine,
        "window_index": win_idx,
        "elapsed_min": round((win_idx + 1) * (dur / 60.0), 1) if False else None,
        "wall_clock": round(time.time(), 1),
        "window_seconds": round(dur, 1),
        "seen": seen,
        "kept": len(pipe),
        "throughput_eps": round(len(pipe) / dur, 1),
        "pipeline_ms": {
            "p50": round(pct(sp, 0.50), 3) if sp else None,
            "p95": round(pct(sp, 0.95), 3) if sp else None,
            "p99": round(pct(sp, 0.99), 3) if sp else None,
            "max": round(max(sp), 3) if sp else None,
        },
        "e2e_ms": {
            "p50": round(pct(se, 0.50), 3) if se else None,
            "p95": round(pct(se, 0.95), 3) if se else None,
            "p99": round(pct(se, 0.99), 3) if se else None,
            "max": round(max(se), 3) if se else None,
        },
    }
    with open(out_path, "a") as f:
        f.write(json.dumps(snap) + "\n")
    pm, em = snap["pipeline_ms"], snap["e2e_ms"]
    print(f"[soak {engine}] window {win_idx} ({round(dur/60,1)}min): "
          f"kept={len(pipe)} eps={snap['throughput_eps']} | "
          f"pipe p50={pm['p50']} p99={pm['p99']} max={pm['max']} | "
          f"e2e p99={em['p99']}", flush=True)
    return snap


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_HOST", "localhost:29092"))
    ap.add_argument("--topic", default="output-events")
    ap.add_argument("--engine", required=True)
    ap.add_argument("--window-seconds", type=int, default=1800)   # 30 min snapshots
    ap.add_argument("--total-seconds", type=int, default=14400)   # 4 hours
    ap.add_argument("--warmup", type=int, default=60)
    ap.add_argument("--outdir", default="/tmp")
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)
    os.makedirs(args.outdir, exist_ok=True)
    out_path = os.path.join(args.outdir, f"{args.engine}_soak.jsonl")
    open(out_path, "w").close()   # truncate

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": f"soak-{args.engine}-{int(time.time())}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "fetch.wait.max.ms": 10,
    })
    consumer.subscribe([args.topic])
    print(f"[soak {args.engine}] <- {args.bootstrap} topic={args.topic} "
          f"window={args.window_seconds}s total={args.total_seconds}s warmup={args.warmup}s",
          flush=True)

    started = time.time()
    win_idx = 0
    win_start = started
    pipe, e2e = [], []
    seen = 0
    # Warmup: drain without recording until warmup elapses.
    while not _stop and (time.time() - started) < args.warmup:
        msg = consumer.poll(0.2)
        if msg and not msg.error():
            seen += 1
    win_start = time.time()

    while not _stop and (time.time() - started) < args.total_seconds:
        msg = consumer.poll(0.2)
        now = time.time()
        if msg is not None and not msg.error():
            recv = now
            try:
                evt = json.loads(msg.value())
                ts = float(evt["ts"]); out_ts = float(evt["out_ts"])
                pipe.append((out_ts - ts) * 1000.0)
                e2e.append((recv - ts) * 1000.0)
                seen += 1
            except (ValueError, KeyError, TypeError):
                pass
        # Window boundary → flush + reset (bounded memory).
        if (now - win_start) >= args.window_seconds:
            flush_window(args.engine, args.topic, win_idx, win_start, pipe, e2e, seen, out_path)
            win_idx += 1
            win_start = now
            pipe, e2e, seen = [], [], 0

    # Flush the final partial window.
    if pipe:
        flush_window(args.engine, args.topic, win_idx, win_start, pipe, e2e, seen, out_path)
    consumer.close()
    print(f"[soak {args.engine}] done after {round((time.time()-started)/60,1)} min, "
          f"{win_idx+1} windows -> {out_path}", flush=True)


if __name__ == "__main__":
    main()
