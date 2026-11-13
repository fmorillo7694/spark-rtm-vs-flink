#!/usr/bin/env python3
"""describe() must match the original sorted()+pct() percentiles. Run: python3 common/test_latency_consumer.py"""
import os
import random
import sys
import unittest
from array import array

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import latency_consumer as lc  # noqa: E402


class Describe(unittest.TestCase):
    def test_matches_reference(self):
        rng = random.Random(1)
        vals = [rng.expovariate(1 / 7.0) for _ in range(20001)]
        a = array("f", vals)
        s = sorted(a)   # same float32-rounded values as the array
        d = lc.describe(a)
        for q, k in ((0.5, "p50"), (0.95, "p95"), (0.99, "p99")):
            self.assertAlmostEqual(d[k], round(lc.pct(s, q), 3), places=2)
        self.assertAlmostEqual(d["max"], round(s[-1], 3), places=2)
        self.assertEqual(d["count"], 20001)

    def test_empty(self):
        self.assertIsNone(lc.describe(array("f"))["p50"])


if __name__ == "__main__":
    unittest.main()
