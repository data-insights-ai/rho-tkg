#!/usr/bin/env python3
"""Authored scheduling/admission guards; not a workload or transport result."""
import tempfile
from pathlib import Path
import unittest
import workload_matrix as matrix


class WorkloadMatrixGuards(unittest.TestCase):
    def test_fresh_fdb_transport_is_explicit_and_refuses_before_effects(self):
        spec={"version":2,"family":"snapshot-to-feed","facts":1_000_000,"crossPercent":50,"writers":1,"layout":"columns"}
        run="01"*16
        self.assertEqual(matrix.preflight(spec,run,"foundationdb","rtt-100ms"),spec)
        options={"QueuePackets":1024,"SampleMillis":60000,"MaxSamples":525602,"MaxArtifactBytes":1<<44}
        self.assertEqual(matrix.owned_transport_input(spec,run,{"kind":"injected-rtt","targetRTTMillis":100,"jitterMillis":0,"seed":20261009},31536000,options)["Spec"],spec)
        with self.assertRaises(ValueError): matrix.preflight({**spec,"version":1},run,"foundationdb","rtt-100ms")
        with self.assertRaises(ValueError): matrix.preflight(spec,run,"paired","rtt-100ms")
        with tempfile.TemporaryDirectory() as directory:
            destination=Path(directory)/"must-not-exist"
            cell=next(matrix.matrix(("snapshot-to-feed",),("rtt-100ms",)))
            with self.assertRaises(ValueError): matrix.execute(cell,destination,1,"small",4096,1<<30,8<<20,owned_options=options)
            self.assertFalse(destination.exists())

    def test_native_owned_rtt_and_paired_fdb_nonlocal_boundary(self):
        self.assertEqual(matrix.run_seconds(None, "foundationdb"), 180)
        self.assertEqual(matrix.run_seconds(365*24*60*60, "native-raft"), 365*24*60*60)
        for seconds, engine in ((None, "native-raft"), (1801, "foundationdb"), (1801, "paired")):
            with self.assertRaises(ValueError): matrix.run_seconds(seconds, engine)
        spec = {"version": 1, "family": "contention-empty-range", "facts": 20, "crossPercent": 50, "writers": 32, "layout": "columns"}
        run = "01020304050607080910111213141516"
        self.assertEqual(matrix.preflight(spec, run, "native-raft", "rtt-100ms"), spec)
        self.assertEqual(matrix.preflight(spec, run, "paired", "local"), spec)
        count, repetitions, facts, source_implemented = 0, set(), set(), True
        for cell in matrix.matrix(("contention-empty-range",), ("rtt-100ms",), ("native-raft",)):
            count += 1
            repetitions.add(cell["repetition"]); facts.add(cell["workload"]["facts"])
            source_implemented &= cell["implemented"]
        self.assertEqual(count, 360)
        self.assertEqual(repetitions, set(range(1, 6)))
        self.assertEqual(facts, set(matrix.FACTS))
        self.assertTrue(source_implemented)
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory)/"must-not-exist"
            unsupported = next(matrix.matrix(("contention-empty-range",), ("rtt-100ms",), ("paired",)))
            with self.assertRaises(ValueError):
                matrix.execute(unsupported, destination, 180, "small", 4096, 1 << 30, 8 << 20)
            self.assertFalse(destination.exists())
        for engine, transport in (("foundationdb", "rtt-1ms"), ("paired", "rtt-1ms")):
            with self.assertRaises(ValueError): matrix.preflight(spec, run, engine, transport)
        with self.assertRaises(ValueError): matrix.preflight({**spec, "family": "snapshot-selectors"}, run, "native-raft", "local")

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
        edges = {**spec, "family": "cross-edge-endpoint-uniqueness"}
        self.assertEqual(matrix.preflight(edges, run), edges)
        self.assertTrue(next(matrix.matrix((edges["family"],), ("local",)))["implemented"])

        for engine in ("native-raft", "paired"):
            self.assertEqual(matrix.preflight(edges, run, engine, "local"), edges)
        for delay in ("rtt-1ms", "rtt-20ms", "rtt-100ms"):
            self.assertEqual(matrix.preflight(edges, run, "native-raft", delay), edges)
            with self.assertRaises(ValueError): matrix.preflight(edges, run, "paired", delay)
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
