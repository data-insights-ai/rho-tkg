#!/usr/bin/env python3
"""Compact receipt/frame refusals only; no generated graph source or campaign."""
import copy
import hashlib
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location("native_transition", Path(__file__).with_name("native_transition_check.py"))
C = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(C)


class NativeTransitionGuards(unittest.TestCase):
    def test_logical_recipe_refuses_foreign_or_unfunded_native_phase(self):
        valid = dict(Version=1, Seed=37, Phase=3, Partition=8, Side="after-applied", Actions=[dict(Member=1, Ticks=2)])
        self.assertEqual(C.recipe(valid), (3, 8, "after-applied", valid["Actions"]))
        for update in ({"Partition": 3}, {"Phase": 5}, {"Actions": [dict(Member=7, Ticks=1)]},
                       {"Actions": [dict(Member=1, Ticks=3)]*6}, {"Actions": "not-actions"},
                       {"Seed": True}, {"Side": "after-submit"}):
            with self.subTest(update=update), self.assertRaises(ValueError):
                C.recipe({**valid, **update})
        with self.assertRaises(C.SemanticFailure) as failure:
            C.semantic(False, "atomic-edge-state")
        self.assertEqual(failure.exception.code, "atomic-edge-state")
        with self.assertRaises(ValueError) as refusal:
            C.require(False, "missing actual source")
        self.assertNotIsInstance(refusal.exception, C.SemanticFailure)

    def receipt(self):
        empty = list(hashlib.sha256(b"").digest())
        return dict(Version=1, Profile=C.C.PROFILE, Cookie=[1] + [0]*15,
                    Member=1, PID=123, Sequence=1, Arms=1, Kind="wal-sync-data-after",
                    Nth=1, Seen=1, Armed=False, Matched=True,
                    IO=dict(Kind="sync-data", Path="000001.log", Directory=False,
                            Requested=0, Written=0, InputSHA256=empty, WrittenSHA256=empty,
                            Error="declared native storage transition SIGKILL",
                            Transition=dict(Operation="wal-sync-data", Side="after", Returned=True, Guaranteed=True)))

    def test_actual_sync_result_is_required_and_distinct_from_write_or_queued_sync(self):
        receipt = self.receipt()
        C.io_receipt(receipt, "storage-wal-sync-data-after")
        for field, value in (("Guaranteed", False), ("Returned", False), ("Side", "before"), ("Operation", "wal-write"), ("ReturnError", "input/output error")):
            bad = copy.deepcopy(receipt)
            bad["IO"]["Transition"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                C.io_receipt(bad, "storage-wal-sync-data-after")
        for update in ({"Matched": False}, {"Seen": 0}, {"Cookie": [0]*16}, {"PID": 0}, {"Armed": True}):
            bad = copy.deepcopy(receipt)
            bad.update(update)
            with self.subTest(update=update), self.assertRaises(ValueError):
                C.io_receipt(bad, "storage-wal-sync-data-after")
        bad = copy.deepcopy(receipt)
        bad["IO"]["Path"] = "../foreign.log"
        with self.assertRaises(ValueError):
            C.io_receipt(bad, "storage-wal-sync-data-after")

    def test_unimplemented_selector_or_old_entry_point_cannot_accept_new_case(self):
        with self.assertRaisesRegex(ValueError, "finite implemented"):
            C.check({"Case": "storage-wal-sync-to-after"})
        with self.assertRaisesRegex(ValueError, "finite case"):
            C.C.check(dict(Version=1, Profile=C.C.PROFILE, Case="storage-wal-sync-data-after"))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            C.decoded('{"Matched":true,"Matched":false}')


if __name__ == "__main__":
    unittest.main()
