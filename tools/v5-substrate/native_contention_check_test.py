#!/usr/bin/env python3
"""Compact source-authored codec/admission guards; no workload qualification."""
import copy
import hashlib
import struct
import tempfile
from pathlib import Path
import unittest
import native_contention_check as check


class NativeContentionGuards(unittest.TestCase):
    def test_profile_operation_and_independent_caller_coordinates(self):
        s = {"version": 1, "family": check.FAMILY, "facts": 20, "crossPercent": 50, "writers": 32, "layout": "columns"}
        self.assertEqual(check.shape(s), s)
        self.assertEqual(check.target(s, "hot", 0), 0)
        self.assertEqual(check.target(s, "hot", 1), 1)
        self.assertEqual(check.target(s, "claim", 9), 19)
        self.assertGreater(check.frame_limit({**s, "facts": 1_000_000}), 1_000_000)
        dataset = hashlib.sha256(b"\1"+struct.pack(">QQ", 20, 50)+b"graph-v2").digest()
        op = {"Dataset": list(dataset), "Family": check.FAMILY, "Phase": "claim", "Fact": 9, "Claimant": 31, "Ordinal": 4+30+9*32+31}
        self.assertEqual(check.operation(s, op), ("claim", 9, 31, op["Ordinal"]))
        with self.assertRaises(ValueError): check.operation(s, {**op, "Claimant": 32})
        for changed in ({**s, "writers": True}, {**s, "family": "snapshot-to-feed"}, {**s, "facts": 21}):
            with self.assertRaises(ValueError): check.shape(changed)
        required = {**s, "facts": 1_000_000}
        self.assertEqual(check.causal_minimum(required, 100), 150_000_300)
        end = {"spec": s, "run": [1]+[0]*15, "Transport": [{"Spec": {"kind": "local", "targetRTTMillis": 0, "jitterMillis": 0, "seed": 1}}],
               "BudgetSeconds": 180, "CausalMinimumMillis": 0, "DeadlineUnixNanos": 1}
        self.assertNotEqual(check.admission_digest(end), check.admission_digest({**end, "BudgetSeconds": 181}))
        self.assertNotEqual(check.admission_digest(end), check.admission_digest({**end, "DeadlineUnixNanos": 2}))
        self.assertEqual(check.owner_limits(end), (16, 39, 3))

    def test_cumulative_grant_and_whole_frame_identity_do_not_reset(self):
        a = dict.fromkeys(check.WORK, 1)
        b = dict.fromkeys(check.WORK, 2)
        policy = dict.fromkeys(check.WORK_MAX, 2)
        check.monotone(a, b, policy)
        with self.assertRaises(ValueError): check.monotone(b, a, policy)
        with self.assertRaises(ValueError): check.monotone(a, {**b, "RPCs": 3}, policy)
        row = dict.fromkeys(check.columns_codec.ROW, 0)
        row.update({"Name": "p", "Type": "edge", "Axis": "", "Scope": "",
                    "Before": dict.fromkeys(check.columns_codec.CELL, 0), "After": dict.fromkeys(check.columns_codec.CELL, 0)})
        frame = {"Partition": 3, "Index": 2, "Root": [0]*172, "Outcome": "", "CDC": "", "Registration": None,
                 "OriginalOutcome": None, "OriginalCDC": None, "Rows": [row]}
        reordered = {**frame, "Rows": [dict(reversed(tuple(row.items())))]}
        self.assertEqual(check.frame_digest(frame), check.frame_digest(reordered))
        changed = copy.deepcopy(frame); changed["Rows"][0]["After"]["Revision"] = 1
        self.assertNotEqual(check.frame_digest(frame), check.frame_digest(changed))
        self.assertNotEqual(check.frame_digest(frame), check.frame_digest({**frame, "Index": 3}))

    def test_present_empty_truncation_and_canonical_eof(self):
        # A bounded codec guard does not manufacture a native original or run.
        body = b"GCS\1"+bytes.fromhex(check.raw.GRAPH)+struct.pack(">Q", 3)+bytes([1])*16+bytes([2])*32+struct.pack(">BQQQBB", 3, 2, 0, 7, 3, 0)
        wire = body+hashlib.sha256(body).digest()
        actual = check.cross_status(wire)
        self.assertTrue(actual["commit"])
        self.assertEqual(actual["index"], 2)
        with self.assertRaises(ValueError): check.cross_status(wire[:-1])
        reserved = body[:-1]+b"\7"
        with self.assertRaises(ValueError): check.cross_status(reserved+hashlib.sha256(reserved).digest())
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"row.jsonl"
            path.write_bytes(b'{"n":1}\n')
            self.assertEqual(list(check.lines(path, 1)), [{"n": 1}])
            path.write_bytes(b'{"n":1}\n{"n":2}\n')
            with self.assertRaises(ValueError): list(check.lines(path, 1))
            path.write_bytes(b'{"n":1}')
            with self.assertRaises(ValueError): list(check.lines(path, 1))


if __name__ == "__main__":
    unittest.main()
