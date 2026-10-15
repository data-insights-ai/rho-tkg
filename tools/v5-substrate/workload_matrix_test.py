#!/usr/bin/env python3
"""Authored scheduling/admission guards; not a workload or transport result."""
import tempfile
from pathlib import Path
import unittest
import workload_matrix as matrix


class WorkloadMatrixGuards(unittest.TestCase):
    def test_full_dimensions_and_fail_before_effects(self):
        counts = {}
        for cell in matrix.matrix():
            family = cell["workload"]["family"]
            counts[family] = counts.get(family, 0) + 1
        self.assertEqual(counts, dict.fromkeys(matrix.FAMILIES, 1440))
        spec = {"version": 1, "family": "contention-empty-range", "facts": 20, "crossPercent": 50, "writers": 32, "layout": "columns"}
        run = "01020304050607080910111213141516"
        self.assertEqual(matrix.preflight(spec, run), spec)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "must-not-exist"
            unsupported = next(matrix.matrix(("transfer-retained-pins",), ("local",)))
            with self.assertRaises(ValueError):
                matrix.execute(unsupported, output, 180, "small", 4096, 1 << 30, 8 << 20)
            self.assertFalse(output.exists())
        selectors = {**spec, "family": "snapshot-selectors", "facts": 40}
        self.assertEqual(matrix.preflight(selectors, run), selectors)
        self.assertTrue(next(matrix.matrix(("snapshot-selectors",), ("local",)))["implemented"])
        feed = {**selectors, "family": "snapshot-to-feed"}
        self.assertEqual(matrix.preflight(feed, run), feed)
        self.assertTrue(next(matrix.matrix(("snapshot-to-feed",), ("local",)))["implemented"])
        for changed, nonce in (({**spec, "family": "snapshot-selectors"}, run), ({**spec, "family": "snapshot-to-feed"}, run), ({**spec, "family": "transfer-retained-pins"}, run), (spec, "00" * 16), ({**spec, "writers": True}, run)):
            with self.assertRaises(ValueError):
                matrix.preflight(changed, nonce)
        with self.assertRaises(ValueError):
            matrix.preflight(spec, run, transport="rtt-1ms")
        for facts, writers in ((1_000_000, 1), (10_000, 32)):
            with self.assertRaises(ValueError):
                matrix.comparison_grant(facts, writers, None, None)
        self.assertEqual(matrix.comparison_grant(1_000_000, 32, 1 << 30, 8 << 20), (1 << 30, 8 << 20, "explicit-caller"))


if __name__ == "__main__":
    unittest.main()
