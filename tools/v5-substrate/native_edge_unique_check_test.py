#!/usr/bin/env python3
"""Authored finite source guards; these are not real native trace evidence."""
import copy
import unittest

import native_edge_unique_check as checker
import native_edge_unique_cell as controller


class NativeEdgeUniqueGuards(unittest.TestCase):
    def setUp(self):
        self.spec = {"version": 1, "family": checker.FAMILY, "facts": 20,
                     "crossPercent": 100, "writers": 8, "layout": "columns"}

    def test_exact_ordinal_caller_and_diagnostic_exception(self):
        q = {"Version": 1, "Family": checker.FAMILY, "Phase": "claim", "Fact": 1,
             "Candidate": 0, "Ordinal": 6, "Reopened": False}
        self.assertEqual(checker.caller(self.spec, q, "claim", 5), 5)
        self.assertEqual(checker.caller(self.spec, q, "occupied", 1), 1)
        for role, client in (("claim", 0), ("claim", True), ("occupied", 5)):
            with self.assertRaises(ValueError): checker.caller(self.spec, q, role, client)
        n, w = 6*self.spec["facts"]+7, self.spec["writers"]
        actual = [(n+w-1-c)//w+(self.spec["facts"]+w-1-c)//w for c in range(w)]+[0]*24
        checker.writer_census(self.spec, actual, actual)
        for broken in ([sum(actual)]+[0]*31, [True]+actual[1:], actual[:-1]+[False]):
            with self.assertRaises(ValueError): checker.writer_census(self.spec, actual, broken)

    def test_truncated_or_extra_candidate_sets_refuse(self):
        pair = [{"candidate": 0}, {"candidate": 1}]
        self.assertIs(checker.candidate_pair(pair), pair)
        for broken in (None, [], pair[:1], pair+[{}]):
            with self.assertRaises(ValueError): checker.candidate_pair(broken)

    def test_duplicate_joins_full_operation_and_body_not_only_decision(self):
        a = {"Operation": {"Fact": 2, "Phase": "close"}, "Proposal": "actual-canonical-business"}
        proof = {"Outcome": "retained-decision", "Manifest": "retained-manifest"}
        terminal = {"Operation": a["Operation"], "Proposal": a["Proposal"], "Decision": proof["Outcome"], "Manifest": proof["Manifest"]}
        checker.terminal_original(a, proof, terminal)
        for field, value in (("Operation", {"Fact": 3, "Phase": "close"}), ("Proposal", "different-business")):
            broken = copy.deepcopy(a);broken[field] = value
            with self.assertRaises(ValueError): checker.terminal_original(broken, proof, terminal)

    def test_strict_fresh_dimensions_and_distinct_endpoint_lives(self):
        checker.shape(self.spec)
        self.assertNotEqual(checker.endpoint(self.spec, 0, False), checker.endpoint(self.spec, 0, True))
        for field in ("version", "facts", "crossPercent", "writers"):
            with self.assertRaises(ValueError): checker.shape({**self.spec, field: True})

    def test_unsupported_transport_refuses_before_controller_effects(self):
        controller.preflight(self.spec, "local", 1800, 4096, "paired")
        for transport, seconds, engine in (("rtt-1ms", 1800, "native-raft"), ("local", True, "native-raft"), ("local", 1801, "paired")):
            with self.assertRaises(ValueError): controller.preflight(self.spec, transport, seconds, 4096, engine)


if __name__ == "__main__":
    unittest.main()
