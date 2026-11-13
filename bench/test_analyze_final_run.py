#!/usr/bin/env python3
"""Unit tests for analyze_final_run.backlog_slope. Run: python3 bench/test_analyze_final_run.py"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analyze_final_run as a  # noqa: E402


class BacklogSlope(unittest.TestCase):
    def test_keeps_up(self):
        rate = {str(t): [50, 50 * 40.0] for t in range(1000, 2800)}          # flat 40 ms
        self.assertAlmostEqual(a.backlog_slope(rate, 1000, 2800), 0.0, places=6)

    def test_falls_behind(self):
        # input 50k/s, output 24k/s: the event leaving at t was produced at t * 24/50,
        # so latency = t * (1 - 0.48) and the slope is 60 * 0.52 = 31.2 s of lag per minute
        rate = {str(1000 + s): [24000, 24000 * s * 0.52 * 1000] for s in range(1800)}
        self.assertAlmostEqual(a.backlog_slope(rate, 1000, 2800), 31.2, places=3)

    def test_window_and_empty_seconds(self):
        rate = {str(t): [0, 0.0] for t in range(1000, 1100)}                  # outside / empty only
        self.assertIsNone(a.backlog_slope(rate, 1000, 1100))


if __name__ == "__main__":
    unittest.main()
