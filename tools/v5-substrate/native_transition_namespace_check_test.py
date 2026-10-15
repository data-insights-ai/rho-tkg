#!/usr/bin/env python3
"""Compact namespace receipt refusals; fixture metadata is not process proof."""
import copy
import hashlib
import unittest
import native_transition_namespace_check as namespace


def receipt():
    empty = list(hashlib.sha256(b"").digest())
    return dict(Version=1, Profile=namespace.C.PROFILE, Cookie=[1]*16, Member=1, PID=12, Sequence=1, Arms=1,
                Kind="manifest-directory-sync-after", Nth=1, Seen=1, Armed=False, Matched=True,
                IO=dict(Kind="sync", Path=".", Directory=True, Requested=0, Written=0, InputSHA256=empty, WrittenSHA256=empty,
                        Error="declared native storage transition SIGKILL", Caller=namespace.CALLER,
                        Publication="marker.manifest.000003.MANIFEST-000007", PublicationClosed=True,
                        Transition=dict(Operation="manifest-directory-sync", Side="after", Returned=True, Guaranteed=True)))


class NamespaceGuards(unittest.TestCase):
    def test_namespace_inventory_does_not_relabel_generic_directory_or_snapshot_io(self):
        self.assertEqual(len(namespace.CASES), 8)
        self.assertNotIn("namespace-directory-sync-after", namespace.CASES)
        self.assertNotIn("namespace-snapshot-atomic-kv-sync", namespace.CASES)
        for value in ({}, dict(Version=1, Profile=namespace.PROFILE), dict.fromkeys(namespace.HEADER)):
            with self.assertRaises(ValueError):
                namespace.check(value)

    def test_marker_sync_requires_actual_frame_successful_close_and_guarantee(self):
        valid = receipt()
        namespace.io_receipt(valid, "namespace-manifest-directory-sync-after")
        for field, replacement in (("Caller", "unrelated-directory-sync"), ("PublicationClosed", False),
                                   ("Publication", "marker.format-version.000003.020"), ("Path", "../other"), ("Directory", False)):
            altered = copy.deepcopy(valid)
            altered["IO"][field] = replacement
            with self.subTest(field=field), self.assertRaises(ValueError):
                namespace.io_receipt(altered, "namespace-manifest-directory-sync-after")
        for field in ("Guaranteed", "Returned"):
            altered = copy.deepcopy(valid)
            altered["IO"]["Transition"][field] = False
            with self.subTest(field=field), self.assertRaises(ValueError):
                namespace.io_receipt(altered, "namespace-manifest-directory-sync-after")
        for field in ("Matched", "Seen", "PID"):
            altered = copy.deepcopy(valid)
            altered[field] = False
            with self.subTest(field=field), self.assertRaises(ValueError):
                namespace.io_receipt(altered, "namespace-manifest-directory-sync-after")
        with self.assertRaises(ValueError):
            namespace.processes([], valid, "namespace-manifest-directory-sync-after")


if __name__ == "__main__":
    unittest.main()
