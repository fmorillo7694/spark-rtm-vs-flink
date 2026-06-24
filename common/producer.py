#!/usr/bin/env python3
"""
High-rate JSON event producer for the Spark RTM vs Flink benchmark.

Replicates the article's event exactly and stamps `ts` (epoch seconds) at produce
time so the downstream consumer can compute true record latency. Keyed by user_id.

Uses confluent-kafka (librdkafka) so a single Python process can comfortably pace
~10k events/sec. Rate is held with a simple token-bucket sleep loop.

Env / CLI (CLI overrides env):
  --bootstrap   KAFKA_BOOTSTRAP_HOST   (default localhost:29092)
  --topic       INPUT_TOPIC            (default input-events)
  --rate        TARGET_RATE            (default 10000 events/sec)
  --seconds     RUN_SECONDS            (default 120; 0 = run forever)
"""
import argparse
import json
import os
import random
import signal
import sys
import time

from confluent_kafka import Producer

EVENT_TYPES = ["click", "view", "purchase", "add_to_cart", "logout", "login"]
COUNTRIES = ["us", "es", "de", "fr", "br", "in", "jp", "gb"]

_stop = False


def _handle_sigint(_sig, _frame):
    global _stop
    _stop = True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_HOST", "localhost:29092"))
    ap.add_argument("--topic", default=os.getenv("INPUT_TOPIC", "input-events"))
    ap.add_argument("--rate", type=int, default=int(os.getenv("TARGET_RATE", "10000")))
    ap.add_argument("--seconds", type=int, default=int(os.getenv("RUN_SECONDS", "120")))
    # Payload padding for the data-rate sweep: pad each event to ~N bytes with a filler
    # field, so MB/s = rate * payload_bytes. Lets us hold message COUNT fixed and vary
    # DATA RATE (1 MB/s -> 1 GB/s) independent of the latency-anchor fields.
    ap.add_argument("--payload-bytes", type=int, default=int(os.getenv("PAYLOAD_BYTES", "0")))
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _handle_sigint)
    signal.signal(signal.SIGTERM, _handle_sigint)

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 5,                 # small batching window — favors latency
        "batch.num.messages": 10000,
        "compression.type": "lz4",
        "acks": "1",                    # leader ack only (single-node broker anyway)
        "queue.buffering.max.messages": 1_000_000,
    })

    rate = max(1, args.rate)
    # Pace in 10ms slices to keep the rate smooth without busy-spinning.
    slice_s = 0.01
    per_slice = max(1, round(rate * slice_s))

    print(f"[producer] -> {args.bootstrap} topic={args.topic} rate={rate}/s "
          f"seconds={args.seconds or 'inf'} (~{per_slice} msgs/{slice_s*1000:.0f}ms)",
          flush=True)

    # Precompute a filler string once; sliced per event to hit the target payload size.
    pad_pool = ("x" * args.payload_bytes) if args.payload_bytes > 0 else ""

    seq = 0
    sent = 0
    started = time.time()
    next_slice = started
    last_report = started

    while not _stop:
        now = time.time()
        if args.seconds and (now - started) >= args.seconds:
            break

        for _ in range(per_slice):
            seq += 1
            evt = {
                "event_id": seq,
                "user_id": random.randint(1, 100_000),
                "event_type": random.choice(EVENT_TYPES),
                "country": random.choice(COUNTRIES),
                "amount": round(random.uniform(0.0, 500.0), 2),
                "ts": time.time(),
            }
            if pad_pool:
                evt["pad"] = pad_pool   # inflate to the target data rate
            try:
                producer.produce(
                    args.topic,
                    key=str(evt["user_id"]),
                    value=json.dumps(evt, separators=(",", ":")),
                )
            except BufferError:
                producer.poll(0.05)
            sent += 1

        producer.poll(0)  # serve delivery callbacks

        # report once per second
        if now - last_report >= 1.0:
            elapsed = now - started
            print(f"[producer] sent={sent} elapsed={elapsed:5.1f}s "
                  f"avg_rate={sent/elapsed:8.1f}/s", flush=True)
            last_report = now

        # sleep until the next slice boundary
        next_slice += slice_s
        delay = next_slice - time.time()
        if delay > 0:
            time.sleep(delay)
        else:
            next_slice = time.time()  # we fell behind; resync, don't accumulate debt

    print("[producer] flushing...", flush=True)
    producer.flush(30)
    elapsed = max(1e-9, time.time() - started)
    print(f"[producer] DONE sent={sent} in {elapsed:.1f}s avg={sent/elapsed:.1f}/s", flush=True)


if __name__ == "__main__":
    sys.exit(main())
