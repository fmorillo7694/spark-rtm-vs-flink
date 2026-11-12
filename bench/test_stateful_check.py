#!/usr/bin/env python3
"""Unit tests for bench/stateful_check.py. Run: python3 bench/test_stateful_check.py"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stateful_check as sc  # noqa: E402


def ev(p, o, eid, user, ts, amount=1.25):
    return {"p": p, "o": o, "k": str(user),
            "v": {"event_id": eid, "user_id": user, "ts": ts, "amount": amount}}


INPUT = [
    ev(0, 0, 1, 7, 100.0, 1.10),
    ev(0, 1, 2, 7, 103.0, 2.20),
    ev(0, 2, 1, 7, 100.0, 1.10),   # duplicate of event 1, arrives late
    ev(0, 3, 3, 7, 111.0, 3.30),   # gap 8 s > 5 s -> new session; new window
    ev(1, 0, 4, 9, 105.0, 4.40),
    ev(1, 1, 99, 0, 999.0, 0.0),   # heartbeat: ignored
]


def out(p, o, v):
    return {"p": p, "o": o, "k": None, "v": v}


class S1(unittest.TestCase):
    def setUp(self):
        self.truth = sc.truth_s1(sc.input_events(INPUT), 10_000)

    def test_truth(self):
        self.assertEqual(self.truth[(7, 100_000)], (3, 440))   # 1.10 + 2.20 + 1.10 (dup counted)
        self.assertEqual(self.truth[(7, 110_000)], (1, 330))
        self.assertEqual(self.truth[(9, 100_000)], (1, 440))
        self.assertNotIn((0, 990_000), self.truth)

    def test_update_mode_final_value_passes(self):
        rows = [out(0, i, {"user_id": 7, "window_start": 100.0, "cnt": c, "sum_amount": s})
                for i, (c, s) in enumerate([(1, 1.1), (2, 3.3000000000000003), (3, 4.4)])]
        rows += [out(0, 9, {"user_id": 7, "window_start": 110.0, "cnt": 1, "sum_amount": 3.3}),
                 out(1, 0, {"user_id": 9, "window_start": 100.0, "cnt": 1, "sum_amount": 4.4})]
        rep = sc.diff_s1(self.truth, rows)
        self.assertTrue(rep["pass"], rep)
        self.assertEqual(rep["max_emissions_per_key"], 3)

    def test_count_mismatch_fails(self):
        rows = [out(0, 0, {"user_id": 7, "window_start": 100.0, "cnt": 2, "sum_amount": 3.3}),
                out(0, 1, {"user_id": 7, "window_start": 110.0, "cnt": 1, "sum_amount": 3.3}),
                out(1, 0, {"user_id": 9, "window_start": 100.0, "cnt": 1, "sum_amount": 4.4})]
        rep = sc.diff_s1(self.truth, rows)
        self.assertFalse(rep["pass"])
        self.assertEqual(rep["count_mismatch"], 1)


class S2(unittest.TestCase):
    def test_dedup(self):
        truth = sc.truth_s2(sc.input_events(INPUT))
        self.assertEqual(truth, {1, 2, 3, 4})
        good = [out(0, i, {"event_id": e, "user_id": 7}) for i, e in enumerate([1, 2, 3, 4])]
        self.assertTrue(sc.diff_s2(truth, good)["pass"])
        dup = good + [out(0, 9, {"event_id": 1, "user_id": 7})]
        rep = sc.diff_s2(truth, dup)
        self.assertFalse(rep["pass"])
        self.assertEqual(rep["emitted_more_than_once"], 1)


class S3(unittest.TestCase):
    def test_sessions(self):
        truth = sc.truth_s3(sc.input_events(INPUT), 5000)
        # user 7: {100,103,100(dup)} closes on the 8 s gap (last_ts stays 103), then {111}.
        self.assertEqual(truth, {(7, 100.0, 103.0, 3): 1, (7, 111.0, 111.0, 1): 1,
                                 (9, 105.0, 105.0, 1): 1})
        rows = [out(0, 0, {"user_id": 7, "session_start": 100.0, "session_end": 103.0,
                           "events": 3, "reason": "gap"}),
                out(0, 1, {"user_id": 7, "session_start": 111.0, "session_end": 111.0,
                           "events": 1, "reason": "timeout"}),
                out(1, 0, {"user_id": 9, "session_start": 105.0, "session_end": 105.0,
                           "events": 1, "reason": "timeout"})]
        rep = sc.diff_s3(truth, rows)
        self.assertTrue(rep["pass"], rep)
        self.assertFalse(sc.diff_s3(truth, rows[:2])["pass"])


class S4(unittest.TestCase):
    def test_enrichment(self):
        events = sc.input_events([ev(0, 0, 1, 7, 100.0), ev(0, 1, 2, 50, 101.0),
                                  ev(0, 2, 1, 7, 100.0), ev(1, 0, 3, 9, 102.0)])
        truth = sc.truth_s4(events, ref_rows=100, miss_mod=50)
        self.assertEqual(set(truth), {1, 3})        # user 50 has no reference row
        self.assertEqual(truth[1][0], 2)            # duplicate: enriched twice

        def enr(eid, user):
            tier, region = sc.ref_attrs(user)
            return out(0, eid, {"event_id": eid, "user_id": user, "tier": tier, "region": region})
        good = [enr(1, 7), enr(1, 7), enr(3, 9)]
        self.assertTrue(sc.diff_s4(truth, good)["pass"])
        self.assertEqual(sc.diff_s4(truth, good[1:])["missing"], 1)
        self.assertEqual(sc.diff_s4(truth, good + [enr(2, 50)])["extra"], 1)
        bad = [enr(1, 7), enr(1, 7), out(0, 3, {"event_id": 3, "user_id": 9, "tier": "x", "region": "y"})]
        self.assertEqual(sc.diff_s4(truth, bad)["wrong_enrichment"], 1)


class S5(unittest.TestCase):
    def test_interval_pairs(self):
        orders = [{"order_id": 1, "ts": 100.0}, {"order_id": 2, "ts": 100.0}, {"order_id": 3, "ts": 100.0}]
        payments = [{"order_id": 1, "ts": 109.5},     # inside the 10 s bound
                    {"order_id": 2, "ts": 112.0}]     # outside: must not join; order 3: none
        truth = sc.truth_s5(orders, payments, 10.0)
        self.assertEqual(truth, {1})
        self.assertTrue(sc.diff_s5(truth, [out(0, 0, {"order_id": 1})])["pass"])
        self.assertEqual(sc.diff_s5(truth, [])["missing"], 1)
        rep = sc.diff_s5(truth, [out(0, 0, {"order_id": 1}), out(0, 1, {"order_id": 2})])
        self.assertEqual(rep["spurious"], 1)
        self.assertEqual(sc.diff_s5(truth, [out(0, 0, {"order_id": 1})] * 2)["emitted_more_than_once"], 1)


if __name__ == "__main__":
    unittest.main()
