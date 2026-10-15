#!/usr/bin/env python3
"""Inert first-Prepare codec guards, never native YES or source authority."""
import hashlib
import json
from pathlib import Path
import struct
import tempfile
import unittest
import native_transition_first_prepare_check as F
import native_transition_conjunction_check_test as common


class FirstPrepareGuards(unittest.TestCase):
    def test_finite_registry_distinguishes_only_the_actual_installed_no_reply_case(self):
        rows = [dict(Coordinator=co, Commit=commit, Kind=kind, Side=side)
                for co in (3, 8) for commit in (True, False) for kind in (1, 2) for side in ("before", "after")]
        self.assertEqual(len({F.recipe(r) for r in rows}), 16)
        self.assertEqual(sum(F.lost_yes(r) for r in rows), 4)
        for changed in ({**rows[0], "Stage":"recovery"}, {**rows[0], "Side":"after-write"}):
            with self.assertRaises(ValueError):
                F.recipe(changed)

    def test_survivor_record_must_be_the_same_selected_original_not_a_new_ack(self):
        tx, digest = bytes([2])*16, bytes([3])*32
        body = F.P.GRAPH+struct.pack(">Q", 3)+tx+digest+b"\2"+struct.pack(">QQQ", 7, 1, 0)+b"\1\0"
        outcome = common.sealed(b"GCS\1", body)
        records = {(3, 7):dict(Outcome=common.encoded(outcome), CDC=None)}
        self.assertEqual(F.stored_vote(outcome, b"", dict(Index=7), tx, digest, records)["index"], 7)
        for event, actual_digest, census, cdc in ((dict(Index=8), digest, records, b""),
                                                  (dict(Index=7), bytes(32), records, b""),
                                                  (dict(Index=7), digest, {}, b""),
                                                  (dict(Index=7), digest, records, b"invented effect")):
            with self.assertRaises(ValueError):
                F.stored_vote(outcome, cdc, event, tx, actual_digest, census)

    def test_missing_first_prepare_reply_never_becomes_a_successful_source_reply(self):
        row = dict(Request=common.encoded(b"inert actual-request-shaped bytes"), Reply=None)
        pending = common.encoded(b"inert invocation, never decoded as GCR")
        F.unanswered_yes(row, row, pending, pending)
        for boundary, later in (({**row, "Reply":common.encoded(b"fabricated GSR")}, pending),
                                (row, common.encoded(b"different invocation"))):
            with self.assertRaises(ValueError):
                F.unanswered_yes(boundary, row, pending, later)

    def test_truncated_or_foreign_prefix_never_completes_prepare_evidence(self):
        header = dict.fromkeys(F.HEADER); header["P06"] = dict.fromkeys(F.C.P06)
        header["P06"]["Through"] = [1, 1]
        first = json.dumps(dict(Type="header", Artifact=header)).encode()+b"\n"
        footer = dict(Type="footer", Rows=0, Through=[1, 1], PayloadSHA256=hashlib.sha256(first).hexdigest())
        last = json.dumps(footer).encode()+b"\n"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"inert.jsonl"; path.write_bytes(first+last)
            self.assertEqual(F.load(path)["P06"]["Records"], [])
            for wire in (first, first+last+last, first+last[:-1], first+json.dumps({**footer, "Rows":1}).encode()+b"\n"):
                path.write_bytes(wire)
                with self.assertRaises(ValueError): F.load(path)


if __name__ == "__main__":
    unittest.main()
