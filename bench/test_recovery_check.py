#!/usr/bin/env python3
"""Unit tests for bench/recovery_check.py. Run: python3 bench/test_recovery_check.py"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import recovery_check as rc  # noqa: E402


class Recovery(unittest.TestCase):
    def test_outage_and_catch_up(self):
        kill = 1000
        rate = {}
        for s in range(900, 1000):
            rate[str(s)] = [100, 100 * 20.0]          # steady: 100/s, 20 ms e2e
        for s in range(1000, 1012):
            rate[str(s)] = [0, 0.0]                   # 12 s outage
        for s in range(1012, 1020):
            rate[str(s)] = [300, 300 * 8000.0]        # replaying backlog, e2e 8 s
        for s in range(1020, 1100):
            rate[str(s)] = [100, 100 * 25.0]          # caught up
        m = rc.recovery_metrics(rate, kill)
        self.assertEqual(m["pre_rate"], 100)
        self.assertEqual(m["outage_s"], 12)
        self.assertEqual(m["recover_s"], 20)

    def test_never_recovers(self):
        rate = {str(s): [100, 2000.0] for s in range(900, 1000)}
        rate.update({str(s): [0, 0.0] for s in range(1000, 1050)})
        self.assertIsNone(rc.recovery_metrics(rate, 1000)["recover_s"])

    def test_truth_diff(self):
        truths = [{"100:1000000": 3, "200:1000000": 2}, {"100:1000000": 1, "300:2065000": 5}]
        seen = {"max_out_ts": 2100.0, "max": {"100:1000000": 4, "200:1000000": 1}}
        d = rc.truth_diff(truths, seen, started=900.0)
        # 300:2065000 ends at 2075 s > 2100 - 30: not settled. 100 exact, 200 lost one.
        self.assertEqual(d, {"settled_windows": 2, "lost": 1, "overcount": 0})


if __name__ == "__main__":
    unittest.main()
