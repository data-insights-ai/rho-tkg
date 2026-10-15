#!/usr/bin/env python3
"""Compact authored monitor admission/ownership guards; no database substitute."""
from pathlib import Path
import json
import os
import sys
import tempfile
import unittest
import process_watchdog as monitor


class WatchdogGuards(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "owned POSIX process session")
    def test_actual_owned_child_deadline_is_reaped_and_never_completed(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory)/"monitor.json"
            result = monitor.run([sys.executable, "-c", "import time; time.sleep(30)"],
                                 seconds=1, rss_mib=4096, report=report)
            receipt = json.loads(report.read_bytes())
            self.assertEqual(result, 124)
            self.assertEqual(receipt["reason"], "deadline")
            self.assertTrue(receipt["all_owned_exited"])
            self.assertTrue(receipt["forced_cleanup"])
            self.assertLess(receipt["exit_code"], 0)
            self.assertTrue(receipt["observed_processes"])

    def test_original_absolute_deadline_and_checked_process_budget(self):
        now = 1_800_000_000_000_000_000
        chosen = now+17_000_000_000
        self.assertEqual(monitor.admission(180, 4096, chosen, now=now), (chosen, 17))
        self.assertEqual(monitor.admission(monitor.MAX_SECONDS, 4096, None, now=now)[0],
                         now+monitor.MAX_SECONDS*1_000_000_000)
        for seconds, memory, deadline in ((True, 4096, chosen), (0, 4096, chosen),
                                          (180, 0, chosen), (180, 4096, now),
                                          (180, 4096, now+181_000_000_000),
                                          (monitor.MAX_SECONDS+1, 4096, chosen)):
            with self.assertRaises(ValueError):
                monitor.admission(seconds, memory, deadline, now=now)

    def test_bounded_metadata_and_only_actual_owned_process_group(self):
        rows = monitor.parse_processes(b"101 90 101 2048 S Thu Oct 15 12:00:00 2026\n102 101 101 1024 Z Thu Oct 15 12:00:01 2026\n103 90 103 4096 R Thu Oct 15 12:00:02 2026\n")
        self.assertEqual(set(monitor.owned_rows(rows, 101)), {101, 102})
        self.assertTrue(monitor.live(monitor.owned_rows(rows, 101)))
        self.assertFalse(monitor.live({102: rows[102]}))
        for wire in (b"101 90 101 2048 S\n", b"x 90 101 2048 S Thu Oct 15 12:00:00 2026\n",
                     b"a"*(monitor.MAX_PS_BYTES+1)):
            with self.assertRaises(ValueError):
                monitor.parse_processes(wire)

    def test_owned_disk_counts_actual_bytes_and_refuses_foreign_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/"native").mkdir()
            (root/"native"/"wal").write_bytes(b"genuine-owned-bytes")
            receipt = monitor.disk_snapshot(root, 1 << 20)
            self.assertEqual(receipt["names"], 2)
            self.assertGreaterEqual(receipt["logicalBytes"], 19)
            with self.assertRaises(ValueError):
                monitor.disk_snapshot(root, 1)
            (root/"foreign").symlink_to(root/"native", target_is_directory=True)
            with self.assertRaises(ValueError):
                monitor.disk_snapshot(root, 1 << 20)


if __name__ == "__main__":
    unittest.main()
