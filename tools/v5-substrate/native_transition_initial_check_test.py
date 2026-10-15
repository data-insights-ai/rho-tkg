#!/usr/bin/env python3
"""Inert initial framing/counter guards; no fixture supplies source authority."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import native_transition_initial_check as I
import native_transition_conjunction_check_test as common


class NativeInitialRegisterGuards(unittest.TestCase):
    def test_closed_registry_keeps_actual_first_yes_voter_after_initial_election(self):
        rows = [dict(Coordinator=co, Commit=commit, Kind=kind, Side=side)
                for co in (3, 8) for commit in (True, False)
                for kind in (1, 2) for side in ("before", "after")]
        self.assertEqual(len({I.recipe(r) for r in rows}), 16)
        self.assertEqual(I.routing(rows[0]), (3, 1, 2, 2, 1, 4))
        self.assertEqual(I.routing(rows[8]), (5, 4, 6, 1, 2, 6))
        for changed in ({**rows[0], "Phase":2}, {**rows[0], "Coordinator":1}, {**rows[0], "Side":"after-YES"}):
            with self.assertRaises(ValueError):
                I.recipe(changed)

    def test_initial_unanswered_register_cannot_borrow_a_successful_prepare_reply(self):
        row, scopes, manifest, command = common.unanswered(phase=1, partition=3)
        self.assertEqual(I.C.unknown_attempt(row, scopes, 3, 3, manifest, 1, 3)[0], command)
        for changed in ({**row, "Reply":common.encoded(b"inert fabricated success")}, {**row, "Partition":8}):
            with self.assertRaises(ValueError):
                I.C.unknown_attempt(changed, scopes, 3, 3, manifest, 1, 3)
        charge = dict(Candidates=1, Attempts=1, Work=4096+32*(2 << 20), Output=(128 << 10)+8192, Retained=len(I.P.raw(row["Request"])))
        I.charge(charge, [row])
        for changed in ({**charge, "Attempts":2}, {**charge, "Retained":0}, {**charge, "Work":0}):
            with self.assertRaises(ValueError):
                I.charge(changed, [row])

    def test_partial_or_foreign_prefix_cannot_complete_initial_storage_evidence(self):
        # Only envelope framing is exercised; these null fields never enter
        # check(), a native source door, or the independent semantic oracle.
        header = dict.fromkeys(I.HEADER)
        header["P06"] = dict.fromkeys(I.C.P06)
        header["P06"]["Through"] = [1, 1]
        first = json.dumps(dict(Type="header", Artifact=header)).encode()+b"\n"
        footer = dict(Type="footer", Rows=0, Through=[1, 1], PayloadSHA256=hashlib.sha256(first).hexdigest())
        last = json.dumps(footer).encode()+b"\n"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"inert.jsonl"
            path.write_bytes(first+last)
            self.assertEqual(I.load(path)["P06"]["Records"], [])
            for wire in (first, first+last+last, first+last[:-1], first+json.dumps({**footer, "Through":[2, 1]}).encode()+b"\n"):
                path.write_bytes(wire)
                with self.assertRaises(ValueError):
                    I.load(path)


if __name__ == "__main__":
    unittest.main()
