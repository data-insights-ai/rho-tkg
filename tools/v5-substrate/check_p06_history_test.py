#!/usr/bin/env python3
"""Replay explicit genuine P06 captures and finite offline sensitivity controls.

Pass the four actual artifact paths. This launches no Go or backend, and altered
copies below are checker controls rather than claims about native executions.
"""
import base64
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import struct
import unittest

SPEC = importlib.util.spec_from_file_location("p06_bridge", Path(__file__).with_name("check-p06-history.py"))
BRIDGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BRIDGE)
CAPTURES = []


class P06Replay(unittest.TestCase):
    def test_four_genuine_cells_have_independent_serial_witnesses(self):
        self.assertEqual({(c["Coordinator"], c["Commit"]) for c in CAPTURES}, {(3, True), (3, False), (8, True), (8, False)})
        self.assertEqual(len(CAPTURES), 4)
        for capture in CAPTURES:
            with self.subTest(coordinator=capture["Coordinator"], commit=capture["Commit"]):
                self.assertEqual(len(BRIDGE.check(capture)["serial_witness"]), 2)

    def test_equal_counts_missing_duplicate_reordered_originals_refuse(self):
        for capture in CAPTURES:
            for name in ("duplicate", "reorder", "missing-business"):
                data = copy.deepcopy(capture)
                if name == "duplicate":
                    data["Records"][1] = copy.deepcopy(data["Records"][0])
                elif name == "reorder":
                    data["Records"][0], data["Records"][1] = data["Records"][1], data["Records"][0]
                else:
                    for row in data["Records"]:
                        wire = BRIDGE.raw(row["Outcome"])
                        if wire[:4] == b"GCS\1":
                            status = BRIDGE.status(wire)
                            if status["phase"] == 4 and status["index"] == row["Index"]:
                                row["Outcome"], row["CDC"] = None, None
                                break
                with self.subTest(coordinator=capture["Coordinator"], commit=capture["Commit"], mutation=name):
                    self.assertEqual(len(data["Records"]), len(capture["Records"]))
                    with self.assertRaises(ValueError):
                        BRIDGE.check(data)

    def test_raw_seals_resume_binding_and_current_as_old_refuse(self):
        for name in ("proposal-seal", "changed-business", "resume", "current-as-old", "wrong-after"):
            data = copy.deepcopy(next(c for c in CAPTURES if c["Commit"]))
            if name == "current-as-old":
                data["Retained"]["Native"] = copy.deepcopy(data["Current"]["Native"])
            elif name == "wrong-after":
                data["Current"]["Native"]["entities"][2]["property"]["i64"] = 99
            elif name == "changed-business":
                wire = BRIDGE.raw(data["Original"])
                inner = bytearray(BRIDGE.pgq(wire)["inner"][:-32])
                inner[60] ^= 1  # Same request key, different real provenance.
                inner = bytes(inner) + hashlib.sha256(inner).digest()
                outer = wire[:141] + struct.pack(">I", len(inner)) + inner
                data["Original"] = base64.b64encode(outer + hashlib.sha256(outer).digest()).decode()
            else:
                field = "Original" if name == "proposal-seal" else "Pending"
                wire = bytearray(BRIDGE.raw(data[field]))
                offset = 29 if field == "Original" else bytes(wire).index(b"GCR1") + 4 + 4 + 156
                wire[offset] ^= 1
                data[field] = base64.b64encode(wire).decode()
            with self.subTest(mutation=name), self.assertRaises(ValueError):
                BRIDGE.check(data)

    def test_existing_oracle_rejects_stale_key_write_skew_and_empty_range(self):
        search = BRIDGE.oracle()
        for predicate in ("key", "range"):
            initial = {"A/guard": 0}
            remote = {"kind": "key", "key": "B/guard", "exists": False, "value": None} if predicate == "key" else {"kind": "range", "lo": "B/", "hi": "B0", "values": {}}
            history = {"initial": initial, "transactions": {
                "a": {"reads": [remote], "writes": {"A/guard": {"op": "put", "value": 1}}},
                "b": {"reads": [{"kind": "key", "key": "A/guard", "exists": True, "value": 0}], "writes": {"B/guard": {"op": "put", "value": 1}}}}}
            with self.subTest(predicate=predicate):
                witness, _ = search(history, {"a", "b"}, {"a": 1, "b": 2}, [], [])
                self.assertIsNone(witness)


if __name__ == "__main__":
    if len(sys.argv) != 5:
        raise SystemExit("Pass exactly four genuine P06 artifact paths; no fixture skip or backend execution")
    CAPTURES[:] = [json.loads(Path(p).read_bytes()) for p in sys.argv[1:]]
    unittest.main(argv=[sys.argv[0]])
