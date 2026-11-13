#!/usr/bin/env python3
"""
Capture a whole Kafka topic (from the earliest offset) to JSON lines, for the stateful
correctness checks. Stops once no new record has arrived for --idle-seconds (after the first
record, or after --first-timeout if the topic stays empty), or at --max-seconds.

Each output line: {"p": partition, "o": offset, "k": key, "v": <parsed JSON value>}
Lines are written in consumption order; per-partition order equals offset order, which is
the order an engine sees one user's events (each user_id maps to one partition).

Usage: topic_capture.py --topic T --out FILE [--idle-seconds 20] [--max-seconds 600]
                        [--isolation read_committed]
"""
import argparse
import json
import os
import sys
import time
import uuid

from confluent_kafka import Consumer, KafkaError


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_HOST", "localhost:29092"))
    ap.add_argument("--topic", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--idle-seconds", type=float, default=20.0)
    ap.add_argument("--first-timeout", type=float, default=180.0)
    ap.add_argument("--max-seconds", type=float, default=600.0)
    ap.add_argument("--isolation", default="read_uncommitted",
                    choices=["read_uncommitted", "read_committed"])
    args = ap.parse_args()

    c = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": f"capture-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": args.isolation,
    })
    c.subscribe([args.topic])
    started = time.time()
    last_msg = None
    n = 0
    bad = 0
    with open(args.out, "w") as f:
        while True:
            now = time.time()
            if now - started > args.max_seconds:
                print(f"[capture] max-seconds reached", flush=True)
                break
            if last_msg is None and now - started > args.first_timeout:
                print(f"[capture] no record within {args.first_timeout}s", flush=True)
                break
            if last_msg is not None and now - last_msg > args.idle_seconds:
                break
            msg = c.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[capture] error {msg.error()}", file=sys.stderr, flush=True)
                continue
            last_msg = time.time()
            try:
                v = json.loads(msg.value())
            except (ValueError, TypeError):
                bad += 1
                continue
            k = msg.key().decode("utf-8", "replace") if msg.key() is not None else None
            f.write(json.dumps({"p": msg.partition(), "o": msg.offset(), "k": k, "v": v},
                               separators=(",", ":")) + "\n")
            n += 1
            if n % 50000 == 0:
                print(f"[capture] {n} records", flush=True)
    c.close()
    print(f"[capture] topic={args.topic} records={n} unparsable={bad} "
          f"elapsed={time.time()-started:.1f}s -> {args.out}", flush=True)
    return 0 if n > 0 else 2


if __name__ == "__main__":
    sys.exit(main())
