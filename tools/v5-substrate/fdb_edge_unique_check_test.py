#!/usr/bin/env python3
"""Inert authored codec/footprint guards, never native workload evidence."""
import base64
import copy
import unittest
import tempfile

import fdb_edge_unique_check as checker


class EdgeUniqueOracleGuards(unittest.TestCase):
    def test_every_declared_writer_and_strict_integer_counters(self):
        p = {"version": 1, "facts": 20, "crossPercent": 100, "writers": 8}
        total = 6*p["facts"]+7
        actual = [(total+7-i)//8 for i in range(8)]+[0]*24
        checker.writer_census(p, actual)
        checker.writer_census({**p, "version": 2}, actual)
        with self.assertRaises(ValueError):
            checker.raw.transport.family_record({}, {**p, "version": 2}, {})
        for broken in ([total]+[0]*31, [True]+actual[1:]):
            with self.assertRaises(ValueError):
                checker.writer_census(p, broken)
        with self.assertRaises(ValueError):
            checker.writer_census({**p, "writers": True}, actual)

    def test_complete_operation_timing_rejects_missing_retries_and_bad_spans(self):
        end = {"spec": {"facts": 20, "writers": 8}, "TotalNanos": 1000}
        row = {"Phase": "claim", "Fact": 0, "Candidate": 0, "Ordinal": 4, "Client": 3,
               "StartNanos": 100, "StopNanos": 800, "ElapsedNanos": 700, "AdmissionNanos": 200,
               "Attempts": 2, "Submissions": 3, "ReadCallbacks": 5, "SDKCallbacks": 4, "Commit": True}
        with tempfile.TemporaryDirectory() as directory:
            with checker.Spool(directory, checker.Policy(1 << 30, 8 << 20, 4096, 512 << 10)) as spool:
                metrics = {key: row[key] for key in ("Attempts", "Submissions", "ReadCallbacks", "SDKCallbacks", "Commit")}
                spool.insert(b"timing-attempts", checker.u64(4), checker.go_json(metrics))
                for field, value in (("Submissions", 1), ("ElapsedNanos", 200), ("AdmissionNanos", 701), ("Client", 0), ("StopNanos", True)):
                    with self.assertRaises(ValueError):
                        checker.timing_row({**row, field: value}, end, spool)
                checker.timing_row(row, end, spool)

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
        metadata = lambda generation: {"Floor": 0, "Generation": generation, "Versions": 0, "Transactions": 0, "Certificates": 0, "Bytes": 0, "Groups": 0}
        observation = {"Metadata": [metadata(7), metadata(9)], "ReadVersion": 123,
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
