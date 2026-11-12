#!/usr/bin/env python3
"""
Recovery-test analysis for one run directory (eks/stateful/run_matrix_incluster.sh, knob
kill=<s>): the runner deletes one engine worker pod mid-run and writes kill.txt.

Time to recover, from the consumer's per-receive-second series (rate.json: {sec: [records,
sum e2e ms]}):
  pre_rate      median records/s over [kill-60, kill-5]
  outage_s      seconds from the kill until output first resumes (>= 10 % of pre_rate)
  recover_s     seconds from the kill until output is caught up: the first second after which
                5 consecutive seconds each have >= 90 % of pre_rate and mean e2e < 1000 ms
                (None if never within the run)

Correctness for the at-least-once sinks (S1 update mode, sampled users), from the producers'
TRUTHJSON (exact per-(user, window) event counts) and the consumer's MAXJSON (highest count
seen per sampled (user, window)). Only "settled" windows are judged: window_end at least 30 s
before the newest sampled output ts, and window_end after the consumer started + 15 s.
  lost          settled windows whose highest count is below the truth (an update never
                reached the output, or state lost an event)
  overcount     settled windows above the truth (state counted an event twice: an exactly-
                once state violation)
  dup_records   output records minus input records (verify.json): at-least-once re-emits

Usage: recovery_check.py RUN_DIR  (prints one JSON line, writes RUN_DIR/recovery.json)
"""
import json
import os
import statistics as st
import sys


def recovery_metrics(rate, kill, need=5):
    secs = sorted(int(s) for s in rate)
    pre = [rate[str(s)][0] for s in secs if kill - 60 <= s <= kill - 5]
    if not pre:
        return {"pre_rate": None, "outage_s": None, "recover_s": None}
    pre_rate = st.median(pre)
    after = [s for s in range(int(kill), (secs[-1] if secs else int(kill)) + 1)]
    get = lambda s: rate.get(str(s), [0, 0.0])
    outage = next((s - kill for s in after if get(s)[0] >= 0.1 * pre_rate and s > kill + 1), None)
    ok = lambda s: get(s)[0] >= 0.9 * pre_rate and get(s)[0] > 0 and get(s)[1] / get(s)[0] < 1000
    recover = None
    for s in after:
        if all(ok(s + j) for j in range(need)):
            recover = s - kill
            break
    return {"pre_rate": pre_rate, "outage_s": outage, "recover_s": recover}


def truth_diff(truths, seen, started, window_ms=10000):
    truth = {}
    for t in truths:
        for k, v in t.items():
            truth[k] = truth.get(k, 0) + v
    hi = (seen["max_out_ts"] - 30) * 1000
    lo = (started + 15) * 1000
    settled = {k: v for k, v in truth.items()
               if lo <= int(k.split(":")[1]) + window_ms <= hi}
    got = seen["max"]
    lost = sum(1 for k, v in settled.items() if got.get(k, 0) < v)
    over = sum(1 for k, v in settled.items() if got.get(k, 0) > v)
    return {"settled_windows": len(settled), "lost": lost, "overcount": over}


def main():
    d = sys.argv[1]
    rd = lambda n: json.load(open(os.path.join(d, n)))
    kill = float(open(os.path.join(d, "kill.txt")).read().split()[0])
    out = {"run": os.path.basename(d), "kill_epoch": kill, **recovery_metrics(rd("rate.json"), kill)}
    if os.path.exists(os.path.join(d, "truth.jsonl")) and os.path.exists(os.path.join(d, "maxcnt.json")):
        truths = [json.loads(l) for l in open(os.path.join(d, "truth.jsonl")) if l.strip()]
        out.update(truth_diff(truths, rd("maxcnt.json"), rd("summary.json")["started_at"]))
    if os.path.exists(os.path.join(d, "verify.json")):
        v = rd("verify.json")
        out["dup_records"] = v["output_records"] - v["input_records"]
    json.dump(out, open(os.path.join(d, "recovery.json"), "w"), indent=2)
    print(json.dumps(out))


if __name__ == "__main__":
    main()
