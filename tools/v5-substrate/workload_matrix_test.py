#!/usr/bin/env python3
"""Authored scheduling/admission guards; not a workload or transport result."""
import tempfile
from pathlib import Path
import unittest
import workload_matrix as matrix


class WorkloadMatrixGuards(unittest.TestCase):
    def test_explicit_guarded_endpoint_profiles_preserve_old_defaults(self):
        for family in ("snapshot-to-feed","snapshot-selectors"):
            ordinary=list(matrix.matrix((family,),("local",),("native-raft",)))
            self.assertTrue(all(c["workload"]["version"]==1 for c in ordinary))
            fresh=list(matrix.matrix((family,),tuple(matrix.TRANSPORTS),("native-raft",),guarded_endpoints=True))
            self.assertEqual(len(fresh),1440);self.assertTrue(all(c["implemented"] and c["workload"]["version"]==2 for c in fresh))
            self.assertEqual({c["workload"]["facts"] for c in fresh},set(matrix.FACTS))
            self.assertEqual({c["workload"]["crossPercent"] for c in fresh},set(matrix.CROSS))
            self.assertEqual({c["workload"]["writers"] for c in fresh},set(matrix.WRITERS))
            self.assertEqual({c["workload"]["layout"] for c in fresh},set(matrix.LAYOUTS))
            paired=list(matrix.matrix((family,),("local",),("paired",),guarded_endpoints=True))
            self.assertEqual(len(paired),360)
            for c in paired:matrix.preflight(c["workload"],"01"*16,"paired","local")
            for delay in ("rtt-1ms","rtt-20ms","rtt-100ms"):
                with self.assertRaises(ValueError):matrix.preflight(paired[0]["workload"],"01"*16,"paired",delay)
        with self.assertRaises(ValueError):list(matrix.matrix(("snapshot-to-feed",),("local",),("native-raft",),retained_only=True,guarded_endpoints=True))
        with self.assertRaises(ValueError):list(matrix.matrix(("immutable-columns",),("local",),("native-raft",),guarded_endpoints=True))

    def test_native_half_correction_full_dimensions_and_local_pair_boundary(self):
        family="late-correction-old-index"
        spec={"version":1,"family":family,"facts":80,"crossPercent":50,"writers":32,"layout":"columns"}
        for transport in matrix.TRANSPORTS:self.assertEqual(matrix.preflight(spec,"01"*16,"native-raft",transport),spec)
        self.assertEqual(matrix.preflight(spec,"01"*16,"paired","local"),spec)
        for transport in ("rtt-1ms","rtt-20ms","rtt-100ms"):
            with self.assertRaises(ValueError):matrix.preflight(spec,"01"*16,"paired",transport)
        with self.assertRaises(ValueError):matrix.preflight(spec|{"facts":40},"01"*16,"native-raft","local")
        cells=list(matrix.matrix((family,),tuple(matrix.TRANSPORTS),("native-raft",)))
        self.assertEqual(len(cells),1440)
        self.assertTrue(all(c["implemented"] and c["workload"]["version"]==1 for c in cells))
        self.assertEqual({c["workload"]["facts"] for c in cells},set(matrix.FACTS))
        self.assertEqual({c["workload"]["crossPercent"] for c in cells},set(matrix.CROSS))
        self.assertEqual({c["workload"]["writers"] for c in cells},set(matrix.WRITERS))
        self.assertEqual({c["workload"]["layout"] for c in cells},set(matrix.LAYOUTS))
        self.assertEqual({c["repetition"] for c in cells},set(range(1,6)))
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/"absent"
            unsupported=next(c for c in cells if c["transport"]["targetRTTMillis"]==100)
            with self.assertRaises(ValueError):matrix.execute(unsupported|{"engine":"paired"},output,1800,"small",4096,1<<30,8<<20)
            self.assertFalse(output.exists())

    def test_native_immutable_columns_actual_route_and_local_pair_only(self):
        spec={"version":1,"family":"immutable-columns","facts":40,"crossPercent":50,"writers":32,"layout":"columns"}
        for transport in matrix.TRANSPORTS:self.assertEqual(matrix.preflight(spec,"01"*16,"native-raft",transport),spec)
        self.assertEqual(matrix.preflight(spec,"01"*16,"paired","local"),spec)
        for transport in ("rtt-1ms","rtt-20ms","rtt-100ms"):
            with self.assertRaises(ValueError):matrix.preflight(spec,"01"*16,"paired",transport)
        cells=list(matrix.matrix(("immutable-columns",),("rtt-100ms",),("native-raft",)))
        self.assertEqual(len(cells),360);self.assertTrue(all(c["implemented"] and c["workload"]["version"]==1 for c in cells))
        self.assertEqual({c["repetition"] for c in cells},set(range(1,6)))
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/"missing"
            with self.assertRaises(ValueError):matrix.execute({**cells[0],"engine":"paired"},output,1800,"small",4096,1<<30,8<<20)
            self.assertFalse(output.exists())

    def test_retained_only_feed_v3_requires_real_fdb_owner_adapter_and_local_pair(self):
        spec={"version":3,"family":"snapshot-to-feed","facts":40,"crossPercent":50,"writers":32,"layout":"columns"}
        for transport in matrix.TRANSPORTS:
            self.assertEqual(matrix.preflight(spec,"01"*16,"native-raft",transport),spec)
            self.assertEqual(matrix.preflight(spec,"01"*16,"foundationdb",transport),spec)
        self.assertEqual(matrix.preflight(spec,"01"*16,"paired","local"),spec)
        for transport in ("rtt-1ms","rtt-20ms","rtt-100ms"):
            with self.assertRaises(ValueError):matrix.preflight(spec,"01"*16,"paired",transport)
        for engine in matrix.ENGINES:
            cells=list(matrix.matrix(("snapshot-to-feed",),("local",),(engine,),owned=engine!="native-raft",retained_only=True))
            self.assertEqual(len(cells),360);self.assertTrue(all(c["workload"]["version"]==3 for c in cells))
        self.assertTrue(all(c["workload"]["version"]==1 for c in matrix.matrix(("snapshot-to-feed",),("local",),("native-raft",))))
        with self.assertRaises(ValueError):list(matrix.matrix(("snapshot-to-feed",),("rtt-1ms",),("paired",),owned=True,retained_only=True))
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"refused"
            cell=next(matrix.matrix(("snapshot-to-feed",),("local",),("foundationdb",),owned=True,retained_only=True))
            with self.assertRaises(ValueError):matrix.execute(cell,path,1800,"small",4096,1<<30,8<<20)
            self.assertFalse(path.exists())

    def test_create_update_local_baseline_and_explicit_cross_variants(self):
        family="partition-local-create-update"
        for cross in matrix.CROSS:
            spec={"version":1,"family":family,"facts":40,"crossPercent":cross,"writers":32,"layout":"columns"}
            self.assertEqual(matrix.preflight(spec,"01"*16),spec)
            self.assertEqual(matrix.preflight(spec|{"version":2},"01"*16,"foundationdb","rtt-100ms"),spec|{"version":2})
            for transport in matrix.TRANSPORTS:
                self.assertEqual(matrix.preflight(spec,"01"*16,"native-raft",transport),spec)
                if transport=="local":self.assertEqual(matrix.preflight(spec,"01"*16,"paired",transport),spec)
                else:
                    with self.assertRaises(ValueError):matrix.preflight(spec,"01"*16,"paired",transport)
        cells=list(matrix.matrix((family,),tuple(matrix.TRANSPORTS),owned=True))
        self.assertEqual(len(cells),1440)
        self.assertTrue(all(c["implemented"] and c["workload"]["version"]==2 for c in cells))
        native=list(matrix.matrix((family,),tuple(matrix.TRANSPORTS),("native-raft",)))
        self.assertEqual(len(native),1440)
        self.assertTrue(all(c["implemented"] and c["workload"]["version"]==1 for c in native))
        with self.assertRaises(ValueError):matrix.preflight(spec|{"facts":20},"01"*16)
    def test_native_selectors_complete_dimensions_and_local_pairing(self):
        spec={"version":1,"family":"snapshot-selectors","facts":40,"crossPercent":0,"writers":32,"layout":"columns"}
        for transport in matrix.TRANSPORTS:
            self.assertEqual(matrix.preflight(spec,"01"*16,"native-raft",transport),spec)
        self.assertEqual(matrix.preflight(spec,"01"*16,"paired","local"),spec)
        for transport in ("rtt-1ms","rtt-20ms","rtt-100ms"):
            with self.assertRaises(ValueError):matrix.preflight(spec,"01"*16,"paired",transport)
        cells=list(matrix.matrix(("snapshot-selectors",),("local",),("native-raft",)))
        self.assertEqual(len(cells),360)
        self.assertTrue(all(c["implemented"] and c["workload"]["version"]==1 for c in cells))
        self.assertEqual({c["workload"]["crossPercent"] for c in cells},{0,10,50,100})

    def test_transfer_requires_real_fresh_adapter_and_explicit_native_grant(self):
        spec={"version":2,"family":"transfer-retained-pins","facts":40,"crossPercent":50,"writers":32,"layout":"columns"}
        for transport in matrix.TRANSPORTS:
            self.assertEqual(matrix.preflight(spec,"01"*16,"foundationdb",transport),spec)
            self.assertFalse(matrix.implemented(spec["family"],"native-raft",transport,True))
            self.assertFalse(matrix.implemented(spec["family"],"paired",transport,True))
        cells=list(matrix.matrix((spec["family"],),("rtt-100ms",),("foundationdb",),owned=True))
        self.assertEqual(len(cells),360)
        self.assertTrue(all(c["implemented"] and c["workload"]["version"]==2 for c in cells))
        with tempfile.TemporaryDirectory() as directory:
            destination=Path(directory)/"unfunded-transfer"
            options={"QueuePackets":1024,"SampleMillis":60000,"MaxSamples":525602,"MaxArtifactBytes":1<<44}
            with self.assertRaises(ValueError):matrix.execute(cells[0],destination,31536000,"small",4096,1<<40,8<<20,owned_options=options)
            self.assertFalse(destination.exists())
        with self.assertRaises(ValueError):matrix.preflight(spec|{"version":1},"01"*16)

    def test_native_retained_snapshot_feed_actual_route_and_nonlocal_pair_boundary(self):
        spec={"version":1,"family":"snapshot-to-feed","facts":40,"crossPercent":0,"writers":32,"layout":"columns"}
        for transport in matrix.TRANSPORTS:
            self.assertEqual(matrix.preflight(spec,"01"*16,"native-raft",transport),spec)
        self.assertEqual(matrix.preflight(spec,"01"*16,"paired","local"),spec)
        for transport in ("rtt-1ms","rtt-20ms","rtt-100ms"):
            with self.assertRaises(ValueError):matrix.preflight(spec,"01"*16,"paired",transport)
        cells=list(matrix.matrix(("snapshot-to-feed",),("local",),("native-raft",)))
        self.assertEqual(len(cells),360)
        self.assertTrue(all(c["implemented"] and c["workload"]["version"]==1 for c in cells))
        self.assertEqual({c["workload"]["crossPercent"] for c in cells},{0,10,50,100})

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

    def test_other_three_fresh_owned_families_preserve_ordinary_and_paired_refusals(self):
        for family in ("contention-empty-range","snapshot-selectors","cross-edge-endpoint-uniqueness","immutable-columns","late-correction-old-index"):
            spec={"version":2,"family":family,"facts":1_000_000,"crossPercent":50,"writers":32,"layout":"columns"}
            self.assertEqual(matrix.preflight(spec,"01"*16,"foundationdb","rtt-100ms"),spec)
            options={"QueuePackets":1024,"SampleMillis":60000,"MaxSamples":525602,"MaxArtifactBytes":1<<44}
            matrix.owned_transport_input(spec,"01"*16,{"kind":"injected-rtt","targetRTTMillis":100,"jitterMillis":0,"seed":20261009},31536000,options)
            cells=list(matrix.matrix((family,),("rtt-100ms",),("foundationdb",),owned=True))
            self.assertEqual(len(cells),360)
            self.assertTrue(all(cell["implemented"] and cell["workload"]["version"]==2 for cell in cells))
            for engine, version in (("paired",2),("foundationdb",1)):
                with self.assertRaises(ValueError):matrix.preflight({**spec,"version":version},"01"*16,engine,"rtt-100ms")
            with tempfile.TemporaryDirectory() as directory:
                destination=Path(directory)/"unfunded"
                with self.assertRaises(ValueError):matrix.execute(cells[0],destination,180,"small",4096,1<<30,8<<20)
                self.assertFalse(destination.exists())
                with self.assertRaises(ValueError):matrix.execute(cells[0],destination,1,"small",4096,1<<30,8<<20,owned_options=options)
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
