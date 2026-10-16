#!/usr/bin/env python3
"""Finite P05 bridge source/fixture guards; no native trace is synthesized.

With --captures, replay exactly the five separately qualified native artifacts.
The sealed values below exercise inert codec boundaries and the existing oracle,
not issuer authentication, actual Prepare/decision or disk/process acceptance.
"""
import argparse
import base64
import copy
import hashlib
import importlib.util
from pathlib import Path
import struct
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("p05_bridge", Path(__file__).with_name("check-p05-history.py"))
BRIDGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BRIDGE)
CAPTURES = []
U64 = lambda n: struct.pack(">Q", n)


def fixture():
    graph, digest, request = BRIDGE.GRAPH, b"d" * 32, b"r" * 16
    scopes, certificates = {}, []
    for partition in (3, 8):
        scope = graph + U64(partition) + b"g" * 16 + U64(1) + U64(1) + digest + b"s" * 32
        scopes[partition] = scope
        certificates.append(BRIDGE.seal(b"GSC\1" + scope + digest + U64(7) + U64(11) + U64(6) + U64(10) + b"i" * 172))
    inner_request = BRIDGE.seal(b"PGO\1" + b"\1" + graph + U64(3) + request + U64(8) + U64(23) + b"opaque typed request")
    original = BRIDGE.seal(b"PGQ\1" + b"\1" + graph + U64(3) + request + digest * 3 + BRIDGE.field(inner_request))
    tx = hashlib.sha256(b"rho-tkg:cross-transaction-id:v1\0" + graph + U64(3) + request).digest()[:16]
    parts = b""
    for partition in (3, 8):
        effect = b"opaque kernel body"
        kernel = BRIDGE.seal(b"GPK\1" + tx + graph + U64(partition) + U64(1) + digest * 4 + hashlib.sha256(effect).digest() + U64(8) + U64(23) + BRIDGE.field(b"opaque axis") + BRIDGE.field(effect))
        parts += U64(partition) + U64(7) + BRIDGE.field(kernel)
    old = BRIDGE.seal(b"GCM\1" + tx + U64(3) + BRIDGE.field(original) + digest * 4 + struct.pack(">I", 2) + parts)
    condition = BRIDGE.seal(b"GUC\1" + struct.pack(">I", 2) + b"".join(BRIDGE.field(c) + U64(12) + b"a" * 32 for c in certificates))
    outer = BRIDGE.seal(b"GCM\2" + BRIDGE.field(old) + BRIDGE.field(condition))
    return old, condition, outer


def cut_frame():
    _, condition, _ = fixture()
    scopes = {BRIDGE.certificate(p[0])["partition"]: BRIDGE.certificate(p[0])["scope"] for p in BRIDGE.condition(condition)}
    scope, configuration, nonce = scopes[3], b"d"*32, b"n"*16
    command = BRIDGE.seal(b"GSQ\1"+b"\1"+scope+configuration+U64(12))
    outcome = BRIDGE.seal(b"GSO\1"+bytes([1, 1])+U64(12)+U64(21))
    proposal = BRIDGE.seal(b"GSC\1"+BRIDGE.field(BRIDGE.seal(b"APS\1"+scope))+BRIDGE.field(command))
    request = BRIDGE.seal(b"GSQ\1"+b"\10"+U64(7)+nonce+bytes(48)+BRIDGE.field(b"")*3+BRIDGE.field(proposal)+bytes(17))
    reply = BRIDGE.seal(b"GSR\1"+BRIDGE.field(BRIDGE.seal(b"APS\1"+scope))+U64(1)+b"i"*16+b"\10"+U64(7)+nonce+bytes(2)+BRIDGE.field(outcome))
    encoded = lambda value: base64.b64encode(value).decode()
    return scopes, dict(Origin=2, Partition=3, Request=encoded(request), Reply=encoded(reply)), command, outcome


class P05SourceGuards(unittest.TestCase):
    def test_submitted_cut_control_is_typed_without_becoming_business(self):
        scopes, frame, _, _ = cut_frame()
        manifests, replies, conflicts, captures = BRIDGE.frames([frame], scopes)
        self.assertEqual((manifests, replies, conflicts, captures), ({}, [], 0, set()))

    def test_cut_controls_bind_scope_configuration_and_actual_retained_output(self):
        scopes, frame, command, outcome = cut_frame()
        controls = []
        BRIDGE.frames([frame], scopes, controls=controls)
        self.assertEqual(len(controls), 1)
        record = dict(Partition=3, Index=21, Outcome=base64.b64encode(outcome).decode(), CDC=None)
        BRIDGE.bind_cut_controls(controls, {(3, 21): record}, b"d"*32)
        # The old cross-phase codec stays closed to cut controls.
        with self.assertRaises(ValueError):
            BRIDGE.gct(command)
        alter = lambda wire, start, value: BRIDGE.seal(wire[:-32][:start] + value + wire[:-32][start+len(value):])
        for bad_command, bad_outcome in (
            (alter(command, 5, b"x"), outcome),
            (alter(command, 4, b"\3"), outcome),
            (alter(command, 125, bytes(32)), outcome),
            (alter(command, 157, U64(0)), outcome),
            (command, alter(outcome, 4, b"\2")),
            (command, alter(outcome, 5, b"\5")),
            (command, alter(outcome, 6, U64(13))),
            (command, alter(outcome, 14, U64(0))),
            (command, outcome[:-1]),
        ):
            with self.subTest(command=bad_command[:8], outcome=bad_outcome[:8]), self.assertRaises(ValueError):
                BRIDGE.cut_control(bad_command, bad_outcome, scopes[3])
        for mutated in ({}, {(3, 21): {**record, "Outcome": base64.b64encode(alter(outcome, 6, U64(13))).decode()}},
                        {(3, 21): {**record, "CDC": base64.b64encode(b"unexpected").decode()}}):
            with self.subTest(record=mutated), self.assertRaises(ValueError):
                BRIDGE.bind_cut_controls(controls, mutated, b"d"*32)
        with self.assertRaises(ValueError):
            BRIDGE.bind_cut_controls(controls, {(3, 21): record}, b"x"*32)
        for offset, replacement in ((164, U64(2)), (172, bytes(16))):
            bad = copy.deepcopy(frame)
            bad["Reply"] = base64.b64encode(alter(BRIDGE.raw(frame["Reply"]), offset, replacement)).decode()
            with self.subTest(offset=offset), self.assertRaises(ValueError):
                BRIDGE.frames([bad], scopes)
        forged = BRIDGE.cut_control(command, alter(outcome, 14, U64(22)), scopes[3])
        with self.assertRaises(ValueError):
            BRIDGE.bind_cut_controls([forged], {(3, 21): record}, b"d"*32)

    def test_register_reply_normalizes_only_native_register_floor(self):
        manifest, _, _ = fixture()
        m = BRIDGE.manifest(manifest)
        registered = (BRIDGE.GRAPH + U64(3) + m["tx"] + m["digest"] + b"\1" +
                      U64(21) + U64(m["floors"][3]) + U64(0) + bytes(2))
        original = BRIDGE.seal(b"GCS\1" + registered)
        projected = BRIDGE.seal(b"GCS\1" + registered[:81] + bytes(8) + registered[89:])
        record = dict(Partition=3, Index=21, Outcome=base64.b64encode(original).decode(), CDC=None)
        BRIDGE.bind_phase_reply(BRIDGE.status(projected), projected, record, m)
        alter = lambda wire, start, value: BRIDGE.seal(wire[:-32][:start] + value + wire[:-32][start+len(value):])
        for wrong in (original, alter(projected, 4+73, U64(22)), alter(projected, 4+98, b"\1")):
            with self.subTest(reply=wrong[-8:]), self.assertRaises(ValueError):
                BRIDGE.bind_phase_reply(BRIDGE.status(wrong), wrong, record, m)
        for start, value in ((4+81, U64(8)), (4+89, U64(1)), (4+97, b"\1"), (4+98, b"\1")):
            bad = {**record, "Outcome": base64.b64encode(alter(original, start, value)).decode()}
            with self.subTest(offset=start), self.assertRaises(ValueError):
                BRIDGE.bind_phase_reply(BRIDGE.status(projected), projected, bad, m)
        prepare = alter(original, 4+72, b"\2")
        raw_prepare = {**record, "Outcome": base64.b64encode(prepare).decode()}
        BRIDGE.bind_phase_reply(BRIDGE.status(prepare), prepare, raw_prepare, m)
        with self.assertRaises(ValueError):
            BRIDGE.bind_phase_reply(BRIDGE.status(prepare), alter(prepare, 4+81, bytes(8)), raw_prepare, m)

    def test_fixed_writes_refuse_collateral_or_wrong_scalar(self):
        before, expected = {"A/winner": 0, "B/loser": 0}, {"A/winner": 5}
        writes = BRIDGE.checked_writes(before, {"A/winner": 5, "B/loser": 0}, expected)
        self.assertEqual(writes, {"A/winner": {"op": "put", "value": 5}})
        for after in ({"A/winner": 5, "B/loser": 1}, {"A/winner": 6, "B/loser": 0}):
            with self.subTest(after=after), self.assertRaises(ValueError):
                BRIDGE.checked_writes(before, after, expected)

    def test_condition_uses_own_cut_not_later_planning_read(self):
        _, condition, _ = fixture()
        certs = [p[0] for p in BRIDGE.condition(condition)]
        own_cut = {"Certificates": [base64.b64encode(c).decode() for c in certs]}
        own = {"A/read": 0, "B/read": 0}
        # This mock isolates the binding guard; neither view has native authority.
        with patch.object(BRIDGE, "projected", side_effect=lambda value: value):
            self.assertEqual(BRIDGE.conditional_reads(condition, own_cut, own, own)[0]["values"], own)
            with self.assertRaises(ValueError):
                BRIDGE.conditional_reads(condition, own_cut, own, {**own, "B/read": 1})
            changed_cut = copy.deepcopy(own_cut)
            changed_cut["Certificates"][0] = base64.b64encode(BRIDGE.seal(certs[0][:-32][:164] + U64(12) + certs[0][172:-32])).decode()
            with self.assertRaises(ValueError):
                BRIDGE.conditional_reads(condition, changed_cut, own, own)

    def test_fixed_all_state_refuses_hidden_history(self):
        # Inert SC1 bytes exercise a literal fixture check, not an authenticated read.
        header = b"SC\1\2" + b"a" * 16 + b"d" * 32
        scope = b"TS\1\2" + header[4:] + b"\6"
        old = header + struct.pack(">I", 0)
        new = header + struct.pack(">I", 1) + BRIDGE.field(scope) + b"\1\2" + struct.pack(">QQQQ", 5, 9, 7, 101)
        cells = lambda wire: [dict(Owner=1, Life=2, Name="p", Point=at, State=base64.b64encode(wire).decode(), Cell={}) for at in (0, 2)]
        step = {"Before": {"Cells": cells(old)}, "After": {"Cells": cells(new)}}
        op = dict(owner=1, life=2, name=b"p", scope=scope)
        BRIDGE.scalar_states(step, op, 7, 101, 5)
        step["After"]["Cells"] = cells(new + b"hidden")
        with self.assertRaises(ValueError):
            BRIDGE.scalar_states(step, op, 7, 101, 5)

    def test_exact_inert_outer_inner_and_business_binding(self):
        old, condition, outer = fixture()
        parsed = BRIDGE.manifest(outer)
        self.assertEqual((parsed["inner"], parsed["condition"]), (old, condition))
        self.assertEqual(parsed["digest"], hashlib.sha256(outer).digest())
        self.assertNotEqual(parsed["digest"], BRIDGE.manifest(old)["digest"])
        self.assertNotEqual(parsed["business"], BRIDGE.manifest(old)["business"])
        self.assertEqual([BRIDGE.certificate(p[0])["partition"] for p in BRIDGE.condition(condition)], [3, 8])

    def test_broken_nested_seal_and_same_key_changed_original_refuse(self):
        old, condition, outer = fixture()
        broken = old[:-1] + bytes([old[-1] ^ 1])
        changed = bytearray(old[:-32]); changed[4] ^= 1
        for wire in (outer[:-1], BRIDGE.seal(b"GCM\2" + BRIDGE.field(broken) + BRIDGE.field(condition)), BRIDGE.seal(b"GCM\2" + BRIDGE.field(BRIDGE.seal(bytes(changed))) + BRIDGE.field(condition))):
            with self.subTest(wire_length=len(wire)), self.assertRaises(ValueError):
                BRIDGE.manifest(wire)

    def test_existing_oracle_unchanged_commit_and_stale_absence_range_aba(self):
        search = BRIDGE.oracle()
        initial = {"A/guard": 0, "B/value": {"scalar": 5, "life": 2, "revision": 1}}
        unchanged = {"reads": [{"kind": "range", "lo": "A/", "hi": "B0", "values": initial}], "writes": {"A/guard": {"op": "put", "value": 1}}}
        witness, _ = search({"initial": initial, "transactions": {"unchanged": unchanged}}, {"unchanged"}, {"unchanged": 2}, [], [])
        self.assertEqual(witness, ["unchanged"])
        for kind in ("absent-key", "empty-range", "life-revision-aba"):
            key = "B/phantom" if kind != "life-revision-aba" else "B/value"
            read = {"kind": "range", "lo": "B/phantom", "hi": "B/phantom0", "values": {}} if kind == "empty-range" else {"kind": "key", "key": key, "exists": key in initial, "value": initial.get(key)}
            actual = {"scalar": 5, "life": 7, "revision": 3} if kind == "life-revision-aba" else 1
            transactions = {"dependent": {"reads": [read], "writes": {"A/guard": {"op": "put", "value": 1}}}, "intervening": {"reads": [], "writes": {key: {"op": "put", "value": actual}}}}
            with self.subTest(kind=kind):
                witness, _ = search({"initial": initial, "transactions": transactions}, set(transactions), {"intervening": 1, "dependent": 2}, [], [])
                self.assertIsNone(witness)


class P05GenuineReplay(unittest.TestCase):
    def test_exact_five_supplied_genuine_captures(self):
        import json
        data = [json.loads(p.read_bytes()) for p in CAPTURES]
        self.assertEqual(len(data), 5)
        self.assertEqual({d["Schedule"] for d in data}, BRIDGE.SCHEDULES)
        for capture in data:
            with self.subTest(schedule=capture["Schedule"]):
                self.assertIsInstance(BRIDGE.check(capture)["serial_witness"], list)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--captures", nargs=5, type=Path)
    CAPTURES[:] = parser.parse_args().captures or []
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(P05SourceGuards)
    if CAPTURES:
        suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(P05GenuineReplay))
    else:
        print("SOURCE/FIXTURE GUARDS ONLY: no genuine P05 captures supplied", flush=True)
    raise SystemExit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
