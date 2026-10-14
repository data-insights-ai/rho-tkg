#!/usr/bin/env python3
"""Finite P05 bridge source/fixture guards; no native trace is synthesized.

With --captures, replay exactly the five separately qualified native artifacts.
The sealed values below exercise inert codec boundaries and the existing oracle,
not issuer authentication, actual Prepare/decision or disk/process acceptance.
"""
import argparse
import hashlib
import importlib.util
from pathlib import Path
import struct
import unittest

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


class P05SourceGuards(unittest.TestCase):
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
