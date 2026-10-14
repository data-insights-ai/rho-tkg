#!/usr/bin/env python3
"""Replay explicit genuine P03 captures; altered copies are offline controls."""
import base64
import copy
import importlib.util
from pathlib import Path
import sys
import unittest

SPEC = importlib.util.spec_from_file_location("p03_bridge", Path(__file__).with_name("check-p03-history.py"))
BRIDGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BRIDGE)
CAPTURES = []


class P03Replay(unittest.TestCase):
    def test_genuine_captures_preserve_both_coordinators_and_serial_cuts(self):
        self.assertEqual(len(CAPTURES), 6)
        self.assertEqual([c["rawCounts"]["coordinatorPartition"] for c in CAPTURES].count(3), 3)
        self.assertEqual([c["rawCounts"]["coordinatorPartition"] for c in CAPTURES].count(8), 3)
        for capture in CAPTURES:
            result = BRIDGE.check(capture)
            self.assertEqual(len(result["serial_witness"]), 6)
            self.assertFalse(result["fullFourFaultAccepted"])

    def test_mapping_missing_cdc_current_as_old_and_bad_cut_refuse(self):
        for mutation in ("coordinator", "missing-cdc", "current-as-old", "cut-seal"):
            data = copy.deepcopy(CAPTURES[0])
            if mutation == "coordinator":
                data["operationMapping"][4]["Coordinator"] = 8 if data["rawCounts"]["coordinatorPartition"] == 3 else 3
            elif mutation == "missing-cdc":
                data["steps"][4]["LogicalCDC"] = []
            elif mutation == "current-as-old":
                data["oldReads"][0]["View"] = copy.deepcopy(data["steps"][-1]["After"])
            else:
                wire = bytearray(BRIDGE.blob(data["steps"][0]["Cut"]["Certificates"][0]))
                wire[-1] ^= 1
                data["steps"][0]["Cut"]["Certificates"][0] = base64.b64encode(wire).decode()
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                BRIDGE.check(data)


if __name__ == "__main__":
    if len(sys.argv) != 7:
        raise SystemExit("Pass six genuine normal/race/coverage A/B captures; no backend execution")
    CAPTURES[:] = [BRIDGE.FAULT.load(p)[0] for p in sys.argv[1:]]
    unittest.main(argv=[sys.argv[0]])
