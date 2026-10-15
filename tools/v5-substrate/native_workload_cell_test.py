#!/usr/bin/env python3
"""Authored controller refusals and one finite shared-spool ownership guard."""
from pathlib import Path
import tempfile
import unittest
from comparison_spool import Policy, Spool, SpoolError
import native_workload_cell as cell


class NativeWorkloadCellGuards(unittest.TestCase):
    def test_repository_monitor_default_keeps_binary_explicit(self):
        self.assertEqual(cell.WATCHDOG, Path(cell.__file__).with_name("process_watchdog.py"))
        with self.assertRaises(ValueError):
            cell.executables(None, None)

    def test_owned_transport_and_paired_nonlocal_refuse_before_effects(self):
        spec = {"version": 1, "family": "contention-empty-range", "facts": 20, "crossPercent": 50, "writers": 32, "layout": "kv"}
        for transport, delay in cell.TRANSPORTS.items():
            self.assertEqual(cell.preflight(spec, transport, 180, 4096)["targetRTTMillis"], delay)
        with self.assertRaises(ValueError): cell.preflight(spec, "rtt-100ms", 180, 4096, "paired")
        for seconds, memory in ((True, 4096), (180, 4095), (cell.native.MAX_SECONDS+1, 4096)):
            with self.assertRaises(ValueError): cell.preflight(spec, "local", seconds, memory)
        required = {**spec, "facts": 1_000_000, "crossPercent": 100, "layout": "columns"}
        self.assertEqual(cell.preflight(required, "rtt-100ms", cell.native.MAX_SECONDS, 4096)["targetRTTMillis"], 100)
        with self.assertRaises(ValueError): cell.preflight(required, "rtt-100ms", 1800, 4096)
        with self.assertRaises(ValueError): cell.preflight(required, "local", 1801, 4096, "paired")
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory)/"must-not-exist"
            with self.assertRaises(ValueError):
                cell.execute({**spec, "family": "snapshot-to-feed"}, "local", destination, "/missing-binary", "/missing-watchdog", 180, 4096, Policy(1 << 20, 65536, 4096, 512 << 10))
            self.assertFalse(destination.exists())

    def test_two_engine_joins_share_one_finite_spool_and_preserve_duplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = Policy(1 << 20, 65536, 4096, 512 << 10)
            with Spool(directory, policy) as spool:
                native = cell.NamespacedSpool(spool, b"native/")
                fdb = cell.NamespacedSpool(spool, b"fdb/")
                native.insert(b"state", b"same-logical-role", b"native-original")
                fdb.insert(b"state", b"same-logical-role", b"fdb-original")
                self.assertEqual(native.lookup(b"state", b"same-logical-role"), b"native-original")
                self.assertEqual(fdb.lookup(b"state", b"same-logical-role"), b"fdb-original")
                self.assertEqual(native.report()["policy"], fdb.report()["policy"])
                self.assertEqual(list(native.walk(b"state")), [(b"same-logical-role", b"native-original")])
                with self.assertRaises(SpoolError): native.insert(b"state", b"same-logical-role", b"replacement")
            self.assertEqual(list(Path(directory).iterdir()), [])


if __name__ == "__main__":
    unittest.main()
