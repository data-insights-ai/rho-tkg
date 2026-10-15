#!/usr/bin/env python3
"""Compact inert format guards; none is native source/decision authority."""
import base64
import copy
import hashlib
import json
import struct
import tempfile
import unittest
from pathlib import Path
import native_transition_conjunction_check as C
import native_transition_batch_check_test as batch_guards


def sealed(magic, body):
    wire = magic+body
    return wire+hashlib.sha256(wire).digest()


def field(wire):
    return struct.pack(">I", len(wire))+wire


def unanswered(phase=3, partition=3):
    scope = bytes([1])*120
    manifest = sealed(b"GCM\1", b"inert codec guard, never a manifest authority")
    command = sealed(b"GCT\1", bytes([phase])+C.P.GRAPH+struct.pack(">Q", partition)+field(manifest)+bytes(4))
    proposal = sealed(b"GSC\1", field(sealed(b"APS\1", scope))+field(command))
    request = sealed(b"GSQ\1", bytes([8])+struct.pack(">Q", 7)+bytes([3])*16+bytes(48)+bytes(12)+field(proposal)+bytes(17))
    return dict(Origin=3, Partition=partition, Request=base64.b64encode(request).decode(), Reply=None), {partition: scope}, manifest, command


class NativeP06ConjunctionGuards(unittest.TestCase):
    def test_partial_extra_and_foreign_eof_never_supply_a_complete_prefix(self):
        # Framing only: these inert null fields are never passed to check() and
        # supply neither a native original nor a source/certificate authority.
        header = dict.fromkeys(C.HEADER)
        header["P06"] = dict.fromkeys(C.P06)
        header["P06"]["Through"] = [1, 1]
        first = json.dumps(dict(Type="header", Artifact=header)).encode()+b"\n"
        footer = dict(Type="footer", Rows=0, Through=[1, 1], PayloadSHA256=hashlib.sha256(first).hexdigest())
        last = json.dumps(footer).encode()+b"\n"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"inert.jsonl"
            path.write_bytes(first+last)
            self.assertEqual(C.load(path)["P06"]["Records"], [])
            for wire in (first, first+last+last, first+last[:-1],
                         first+json.dumps({**footer, "Rows":1}).encode()+b"\n",
                         first+json.dumps({**footer, "Through":[1, 2]}).encode()+b"\n"):
                path.write_bytes(wire)
                with self.assertRaises(ValueError):
                    C.load(path)

    def test_closed_sixteen_cell_registry_refuses_nominal_extra_dimensions(self):
        names = {C.recipe(dict(Coordinator=co, Commit=commit, Kind=kind, Side=side))
                 for co in (3, 8) for commit in (True, False) for kind in (1, 2) for side in ("before", "after")}
        self.assertEqual(len(names), 16)
        valid = dict(Coordinator=3, Commit=True, Kind=1, Side="before")
        for changed in ({**valid, "Coordinator":1}, {**valid, "Commit":1}, {**valid, "Kind":True},
                        {**valid, "Side":"after-write"}, {**valid, "Traffic":"duplicate"}, {**valid, "Occurrence":2}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                C.recipe(changed)

    def test_recovery64_adds_only_actual_participant_prepare_resolve_occurrences(self):
        rows = [dict(Coordinator=co, Commit=commit, Kind=kind, Side=side, Phase=phase, Partition=part)
                for co in (3, 8) for commit in (True, False) for kind in (1, 2)
                for side in ("before", "after") for phase in (2, 4) for part in (3, 8)]
        self.assertEqual(len({C.recipe(row, 2) for row in rows}), 64)
        for row in ({**rows[0], "Phase":1}, {**rows[0], "Phase":3}, {**rows[0], "Phase":0},
                    {**rows[0], "Partition":0}, {**rows[0], "Partition":4}, {**rows[0], "Occurrence":2}):
            with self.assertRaises(ValueError):
                C.recipe(row, 2)
        with self.assertRaises(ValueError):
            C.recipe(rows[0])  # Old Decide16 grammar stays exact.

    def test_actual_abort_terminal_skip_is_not_a_synthetic_success(self):
        order = [(2, 3), (2, 8), (3, 3), (4, 3), (4, 8)]
        C.recovery_order(order, 3, True, 2, 3)
        C.recovery_order(order[:1]+order, 3, False, 2, 8)
        C.recovery_order(order[:3]+order[4:], 3, False, 4, 3)
        C.recovery_order(order[:4], 3, False, 4, 8)
        C.recovery_order(order[:4]+order[3:], 3, True, 4, 8)
        for successful, commit, phase, partition in ((order[:4], True, 4, 8),
                                                    (order[:3], False, 4, 3),
                                                    (order[:4]+order[3:], False, 4, 8),
                                                    (order[:1]+order[2:], True, 2, 8)):
            with self.subTest(successful=successful), self.assertRaises(ValueError):
                C.recovery_order(successful, 3, commit, phase, partition)

    def test_actual_rotated_target_and_same_native_after_batch_are_required(self):
        arm, receipt, run, command = batch_guards.sample()
        arm["Member"] = receipt["Member"] = 2
        C.batch_pair(arm, receipt, run, 1000, 2, 2, "after", command)
        for mutate in (lambda r: r.update(Member=1),
                       lambda r: r["Events"][1].update(Synced=False),
                       lambda r: r["Events"][0].update(Matches=2),
                       lambda r: r["Events"][1]["BatchPayloadSHA256"].__setitem__(0, 9),
                       lambda r: r.update(Output=arm["Output"]),
                       lambda r: r.update(DeadlineUnixNanos=1001)):
            changed = copy.deepcopy(receipt)
            mutate(changed)
            with self.assertRaises(ValueError):
                C.batch_pair(arm, changed, run, 1000, 2, 2, "after", command)

    def test_unanswered_decide_never_becomes_a_fifth_successful_reply(self):
        row, scopes, manifest, command = unanswered()
        self.assertEqual(C.unknown_attempt(row, scopes, 3, 3, manifest)[0], command)
        for changed in ({**row, "Reply":base64.b64encode(b"fabricated GSR").decode()},
                        {**row, "Origin":5}, {**row, "Partition":8}):
            with self.assertRaises(ValueError):
                C.unknown_attempt(changed, scopes, 3, 3, manifest)
        wrong, scopes, manifest, _ = unanswered(phase=2)
        with self.assertRaises(ValueError):
            C.unknown_attempt(wrong, scopes, 3, 3, manifest)
        for phase in (2, 4):
            for partition in (3, 8):
                row, scopes, manifest, command = unanswered(phase, partition)
                self.assertEqual(C.unknown_attempt(row, scopes, 3, 3, manifest, phase, partition)[0], command)
                with self.assertRaises(ValueError):
                    C.unknown_attempt({**row, "Reply":base64.b64encode(b"invented success").decode()}, scopes, 3, 3, manifest, phase, partition)

    def test_native_refusal_or_expiry_cannot_qualify_target_transport_loss(self):
        C.unknown_invocation(dict(code=3, mask=128), "typed loss separately authenticated by actual Go target/receipt")
        for code, mask in ((0, 0), (1, 128), (2, 128), (4, 128), (5, 0), (6, 128), (3, 0), (3, 129)):
            with self.subTest(code=code, mask=mask), self.assertRaises(ValueError):
                C.unknown_invocation(dict(code=code, mask=mask), "inert")
        with self.assertRaises(ValueError):
            C.unknown_invocation(dict(code=6, mask=0), "")


if __name__ == "__main__":
    unittest.main()
