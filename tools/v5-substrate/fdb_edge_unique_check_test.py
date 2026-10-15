#!/usr/bin/env python3
"""Inert authored codec/footprint guards, never native workload evidence."""
import base64
import copy
import unittest

import fdb_edge_unique_check as checker


class EdgeUniqueOracleGuards(unittest.TestCase):
    def test_fixed_life_and_old_incumbent_close_footprint(self):
        p = {"facts": 20, "crossPercent": 100, "writers": 32, "layout": "columns"}
        old = checker.edge(p, 0, 0)
        replacement = checker.edge(p, 0, 1, True)
        self.assertNotEqual(old["ID"], replacement["ID"])
        self.assertEqual(old["Value"], replacement["Value"])
        self.assertNotEqual(old["Target"]["Life"], replacement["Target"]["Life"])
        phase, fact, command, lifecycle, effects, reads = checker.contract(p, 4+5*p["facts"], 0)
        self.assertEqual((phase, fact, command["Kind"]), ("close", 0, "edge-close"))
        self.assertFalse(any(row["Key"] == checker.unique_key(0) for row in effects[0]))
        self.assertIn(checker.unique_key(0), reads[0])
        self.assertNotIn("Definition", lifecycle)
        _, _, _, _, releasing, _ = checker.contract(p, 4+4*p["facts"], 0, True)
        self.assertIn({"Key": checker.unique_key(0), "Value": 0, "Delete": True}, releasing[0])
        with self.assertRaises(ValueError):
            checker.candidate_from_command(p, {"Edge": {"ID": 6}})

    def test_same_native_capture_binds_absent_key_and_empty_range(self):
        p = {"facts": 20, "crossPercent": 100}
        c = {"Graph": "inert-capture", "Topology": 1, "Epochs": [1, 1]}
        _, _, command, _, _, reads = checker.contract(p, 4, 0)
        encode = lambda value: base64.b64encode(value).decode()
        point = lambda g, key: {"Key": encode(checker.raw.key(c, g, "current", key)), "Value": None}
        key = checker.raw.key(c, 0, "current", checker.unique_key(0))
        observation = {"Metadata": [{"Generation": 7}, {"Generation": 9}], "ReadVersion": 123,
                       "Reads": [[point(g, key) for key in reads.get(g, [])] for g in range(2)],
                       "UniqueRange": None, "Begin": encode(key), "End": encode(key+b"\0")}
        tx = {"GraphChange": command, "Participants": [
            {"Group": g, "Generation": observation["Metadata"][g]["Generation"],
             "Reads": [{"Key": key, "Version": 0} for key in reads[g]]} for g in sorted(reads)]}
        attempt = {"Fact": 0, "Capture": observation}
        checker.capture(attempt, tx, {"Config": c}, reads)
        for change in ("phantom-range", "revision", "generation"):
            broken, altered = copy.deepcopy(attempt), copy.deepcopy(tx)
            if change == "phantom-range":
                broken["Capture"]["UniqueRange"] = [{"Key": encode(key), "Value": encode(b"phantom")}]
            elif change == "revision":
                altered["Participants"][0]["Reads"][0]["Version"] = 1
            else:
                altered["Participants"][0]["Generation"] += 1
            with self.assertRaises(ValueError):
                checker.capture(broken, altered, {"Config": c}, reads)


if __name__ == "__main__":
    unittest.main()
