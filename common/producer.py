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
  --rate        TARGET_RATE            (default 10000 events/sec; ORIGINAL events, dups are extra)
  --seconds     RUN_SECONDS            (default 120; 0 = run forever)

Stateful-workload knobs (all default to the original stateless behavior):
  --seed N              deterministic content (user_id, type, country, amount, dup choice/delay)
  --keys N              user_id cardinality, 1..N (default 100000)
  --hot-key-fraction F  fraction of events drawn from the hot set 1..--hot-keys (default 0)
  --hot-keys N          size of the hot set (default 10)
  --dup-rate F          fraction of original events re-sent once as an exact copy (same
                        event_id, same ts, same bytes) after a uniform 0..--dup-max-delay-ms delay
  --dup-max-delay-ms N  max duplicate delay (default 3000; keep it below the engines' watermark
                        bound so duplicates are never late)
  --id-base N           first event_id - 1 (lets parallel producer pods use disjoint id ranges)
  --tail-heartbeat-s S  after the run (and after every pending duplicate is sent), write one
                        heartbeat event (user_id 0, event_type "heartbeat") to EVERY partition
                        with ts = last_ts + S, so event-time windows/watermarks close on both
                        engines. Correctness runs only; consumers/checkers ignore user_id 0.

Join workloads (common/joindata.py holds the shared data rules):
  --ref-rows N          S4: write the static reference table ({user_id, tier, region}, one row
                        per user_id 1..N except user_id % --ref-miss-mod == 0) to --topic as
                        fast as possible, then exit. Use a compacted topic.
  --payments-topic T    S5: --topic receives ORDERS ({order_id, user_id, amount, ts}); for a
                        --match-rate fraction of orders one payment ({payment_id, order_id,
                        user_id, amount, ts}) goes to T, --max-delay-ms U(0, max) after the order;
                        a further --late-rate fraction gets a payment OUTSIDE the join bound,
                        U(--late-min-ms, --late-max-ms) later (must not join). A payment's ts is
                        its send wall clock, like every other event ts. --rate = orders/s.
"""
import argparse
import heapq
import json
import os
import random
import signal
import sys
import time

from confluent_kafka import Producer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from joindata import has_ref, ref_attrs  # noqa: E402

EVENT_TYPES = ["click", "view", "purchase", "add_to_cart", "logout", "login"]
COUNTRIES = ["us", "es", "de", "fr", "br", "in", "jp", "gb"]

_stop = False


def _handle_sigint(_sig, _frame):
    global _stop
    _stop = True


def env_float(name, default):
    return float(os.getenv(name, str(default)))


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
    ap.add_argument("--seed", type=int, default=int(os.getenv("SEED")) if os.getenv("SEED") else None)
    ap.add_argument("--keys", type=int, default=int(os.getenv("KEYS", "100000")))
    ap.add_argument("--hot-key-fraction", type=float, default=env_float("HOT_KEY_FRACTION", 0.0))
    ap.add_argument("--hot-keys", type=int, default=int(os.getenv("HOT_KEYS", "10")))
    ap.add_argument("--dup-rate", type=float, default=env_float("DUP_RATE", 0.0))
    ap.add_argument("--dup-max-delay-ms", type=int, default=int(os.getenv("DUP_MAX_DELAY_MS", "3000")))
    ap.add_argument("--id-base", type=int, default=int(os.getenv("ID_BASE", "0")))
    ap.add_argument("--tail-heartbeat-s", type=float, default=env_float("TAIL_HEARTBEAT_S", 0.0))
    # Recovery-test ground truth: for user_id % N == 0, count events per (user, window) and
    # print one "TRUTHJSON {...}" line at the end (S1 window rule). 0 = off.
    ap.add_argument("--truth-sample", type=int, default=0)
    ap.add_argument("--truth-window-ms", type=int, default=10000)
    ap.add_argument("--ref-rows", type=int, default=0)
    ap.add_argument("--ref-miss-mod", type=int, default=50)
    ap.add_argument("--payments-topic", default="")
    ap.add_argument("--match-rate", type=float, default=0.8)
    ap.add_argument("--late-rate", type=float, default=0.05)
    ap.add_argument("--max-delay-ms", type=int, default=9500)   # 0.5 s inside the 10 s bound
    ap.add_argument("--late-min-ms", type=int, default=11000)
    ap.add_argument("--late-max-ms", type=int, default=14000)
    # Load steps (scaling test): "RATE:SECONDS,RATE:SECONDS,..." per pod, from the producer's
    # start; the last rate holds until --seconds ends. Each step change prints "STEP <epoch> <rate>".
    ap.add_argument("--rate-steps", default="")
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _handle_sigint)
    signal.signal(signal.SIGTERM, _handle_sigint)

    if not (0.0 <= args.hot_key_fraction <= 1.0 and 0.0 <= args.dup_rate <= 1.0):
        sys.exit("--hot-key-fraction and --dup-rate must be in [0, 1]")
    if args.keys < 1 or args.hot_keys < 1:
        sys.exit("--keys and --hot-keys must be >= 1")

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 5,                 # small batching window — favors latency
        "batch.num.messages": 10000,
        "compression.type": "lz4",
        "acks": "1",                    # leader ack only (single-node broker anyway)
        "queue.buffering.max.messages": 1_000_000,
    })

    if args.ref_rows > 0:
        return write_ref_table(producer, args)
    if args.payments_topic and not (0.0 <= args.match_rate and 0.0 <= args.late_rate
                                    and args.match_rate + args.late_rate <= 1.0):
        sys.exit("--match-rate and --late-rate must be >= 0 and sum to <= 1")

    # One RNG for all content decisions: with --seed the event content sequence (and the
    # duplicate choices/delays) is identical run to run; only `ts` follows the wall clock.
    rng = random.Random(args.seed)
    hot_keys = min(args.hot_keys, args.keys)

    rate = max(1, args.rate)
    # Pace in 10ms slices to keep the rate smooth without busy-spinning.
    slice_s = 0.01
    per_slice = max(1, round(rate * slice_s))

    print(f"[producer] -> {args.bootstrap} topic={args.topic} rate={rate}/s "
          f"seconds={args.seconds or 'inf'} (~{per_slice} msgs/{slice_s*1000:.0f}ms) "
          f"seed={args.seed} keys={args.keys} hot={args.hot_key_fraction}@{hot_keys} "
          f"dup={args.dup_rate}<= {args.dup_max_delay_ms}ms id_base={args.id_base}",
          flush=True)

    # Precompute a filler string once; sliced per event to hit the target payload size.
    pad_pool = ("x" * args.payload_bytes) if args.payload_bytes > 0 else ""

    def send(key, value, partition=None, topic=None):
        while True:
            try:
                if partition is None:
                    producer.produce(topic or args.topic, key=key, value=value)
                else:
                    producer.produce(topic or args.topic, key=key, value=value, partition=partition)
                return
            except BufferError:
                producer.poll(0.05)

    pending = []        # duplicate heap: (due_wallclock, seq, key, value)
    truth = {} if args.truth_sample > 0 else None   # "user:window_start_ms" -> event count
    seq = args.id_base
    sent = 0
    originals = 0
    dups = 0
    last_ts = 0.0
    started = time.time()
    next_slice = started
    last_report = started

    pay_pending = []    # S5 payment heap: (due_wallclock, order_id, payment dict without ts)
    payments = 0
    late_payments = 0

    def send_due_dups(now):
        nonlocal sent, dups, payments
        while pending and pending[0][0] <= now:
            _, _, k, v = heapq.heappop(pending)
            send(k, v)
            sent += 1
            dups += 1
        while pay_pending and pay_pending[0][0] <= now:
            _, _, pay = heapq.heappop(pay_pending)
            pay["ts"] = time.time()   # the payment exists from the moment it is sent
            send(str(pay["user_id"]), json.dumps(pay, separators=(",", ":")), topic=args.payments_topic)
            sent += 1
            payments += 1

    steps = parse_steps(args.rate_steps)   # [(start_offset_s, rate)], empty = constant --rate
    step_i = -1

    while not _stop:
        now = time.time()
        while step_i + 1 < len(steps) and now - started >= steps[step_i + 1][0]:
            step_i += 1
            per_slice = max(1, round(steps[step_i][1] * slice_s))
            print(f"STEP {now:.3f} {steps[step_i][1]}", flush=True)
        if args.seconds and (now - started) >= args.seconds:
            break

        for _ in range(per_slice):
            seq += 1
            if args.hot_key_fraction > 0 and rng.random() < args.hot_key_fraction:
                user_id = rng.randint(1, hot_keys)
            else:
                user_id = rng.randint(1, args.keys)
            last_ts = time.time()
            if args.payments_topic:   # S5: an order, maybe followed by one payment
                amount = round(rng.uniform(0.0, 500.0), 2)
                order = {"order_id": seq, "user_id": user_id, "amount": amount, "ts": last_ts}
                send(str(user_id), json.dumps(order, separators=(",", ":")))
                sent += 1
                originals += 1
                r = rng.random()
                if r < args.match_rate:
                    delay = rng.uniform(0.0, args.max_delay_ms / 1000.0)
                elif r < args.match_rate + args.late_rate:
                    delay = rng.uniform(args.late_min_ms / 1000.0, args.late_max_ms / 1000.0)
                    late_payments += 1
                else:
                    continue
                heapq.heappush(pay_pending, (last_ts + delay, seq, {
                    "payment_id": seq, "order_id": seq, "user_id": user_id, "amount": amount}))
                continue
            evt = {
                "event_id": seq,
                "user_id": user_id,
                "event_type": rng.choice(EVENT_TYPES),
                "country": rng.choice(COUNTRIES),
                "amount": round(rng.uniform(0.0, 500.0), 2),
                "ts": last_ts,
            }
            if pad_pool:
                evt["pad"] = pad_pool   # inflate to the target data rate
            key = str(user_id)
            value = json.dumps(evt, separators=(",", ":"))
            send(key, value)
            sent += 1
            originals += 1
            dup = args.dup_rate > 0 and rng.random() < args.dup_rate
            if dup:
                delay = rng.uniform(0.0, args.dup_max_delay_ms / 1000.0)
                heapq.heappush(pending, (last_ts + delay, seq, key, value))
            if truth is not None and user_id % args.truth_sample == 0:
                # Same window rule as the engines: int(ts * 1000) floored to the window; a
                # duplicate is one more event to an aggregation.
                ts_ms = int(last_ts * 1000)
                k = f"{user_id}:{ts_ms - ts_ms % args.truth_window_ms}"
                truth[k] = truth.get(k, 0) + (2 if dup else 1)

        send_due_dups(time.time())
        producer.poll(0)  # serve delivery callbacks

        # report once per second
        if now - last_report >= 1.0:
            elapsed = now - started
            print(f"[producer] sent={sent} originals={originals} dups={dups} elapsed={elapsed:5.1f}s "
                  f"avg_rate={sent/elapsed:8.1f}/s", flush=True)
            last_report = now

        # sleep until the next slice boundary
        next_slice += slice_s
        delay = next_slice - time.time()
        if delay > 0:
            time.sleep(delay)
        else:
            next_slice = time.time()  # we fell behind; resync, don't accumulate debt

    # Drain scheduled duplicates at their due time so none is sent later than its bound.
    while (pending or pay_pending) and not _stop:
        wait = min(h[0][0] for h in (pending, pay_pending) if h) - time.time()
        if wait > 0:
            time.sleep(min(wait, 0.05))
        send_due_dups(time.time())
        producer.poll(0)

    heartbeats = 0
    if args.tail_heartbeat_s > 0:
        producer.flush(30)
        meta = producer.list_topics(args.topic, timeout=10).topics[args.topic]
        hb_ts = (last_ts or time.time()) + args.tail_heartbeat_s
        for p in sorted(meta.partitions):
            seq += 1
            hb = {"event_id": seq, "user_id": 0, "event_type": "heartbeat", "country": "xx",
                  "amount": 0.0, "ts": hb_ts}
            send("0", json.dumps(hb, separators=(",", ":")), partition=p)
            heartbeats += 1

    print("[producer] flushing...", flush=True)
    producer.flush(30)
    elapsed = max(1e-9, time.time() - started)
    if truth is not None:
        print("TRUTHJSON " + json.dumps(truth, separators=(",", ":")), flush=True)
    pay = f" payments={payments} late_payments={late_payments}" if args.payments_topic else ""
    print(f"[producer] DONE sent={sent} originals={originals} dups={dups} heartbeats={heartbeats}{pay} "
          f"in {elapsed:.1f}s avg={sent/elapsed:.1f}/s", flush=True)


def parse_steps(spec):
    """'6250:540,12500:900,6250:1020' -> [(0, 6250), (540, 12500), (1440, 6250)]."""
    out, t = [], 0.0
    for part in filter(None, spec.split(",")):
        rate, secs = part.split(":")
        out.append((t, max(1, int(rate))))
        t += float(secs)
    return out


def write_ref_table(producer, args):
    """S4 static reference table, as fast as the broker takes it; exits when flushed."""
    t0 = time.time()
    rows = 0
    for u in range(1, args.ref_rows + 1):
        if not has_ref(u, args.ref_rows, args.ref_miss_mod):
            continue
        tier, region = ref_attrs(u)
        v = json.dumps({"user_id": u, "tier": tier, "region": region}, separators=(",", ":"))
        while True:
            try:
                producer.produce(args.topic, key=str(u), value=v)
                break
            except BufferError:
                producer.poll(0.05)
        rows += 1
        if rows % 10000 == 0:
            producer.poll(0)
    left = producer.flush(60)
    print(f"[producer] DONE ref_rows={rows} of 1..{args.ref_rows} (miss_mod={args.ref_miss_mod}) "
          f"topic={args.topic} unflushed={left} in {time.time() - t0:.1f}s", flush=True)
    return 0 if left == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
