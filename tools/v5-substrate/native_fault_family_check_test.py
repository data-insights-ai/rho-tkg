#!/usr/bin/env python3
"""Compact fail-closed grammar guards; no synthetic native qualification run."""
import hashlib
import base64
import importlib.util
import json
from pathlib import Path
import tempfile
import struct
import unittest

SPEC = importlib.util.spec_from_file_location("native_fault_family", Path(__file__).with_name("native_fault_family_check.py"))
F = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(F)


class NativeFaultFamilyGuards(unittest.TestCase):
    def test_label_cdc_scope_uses_its_one_based_axis_and_rejects_absent_or_foreign_axis(self):
        # Inert primary-codec bytes exercise only this parser. They are not a
        # native artifact, process result, authority or passing source witness.
        field = lambda value: struct.pack(">I", len(value)) + value
        axis_id, profile, reference, unit = b"a"*16, 2, b"declared-axis", b"unit"
        descriptor = axis_id + struct.pack(">BH", profile, 1) + field(reference) + field(unit)
        definition = hashlib.sha256(b"rho-tkg:axis:v1\0" + descriptor).digest()
        scope = b"TS\1" + bytes([profile]) + axis_id + definition + bytes([6])
        changes = (b"CC\1" + bytes([profile]) + axis_id + definition + struct.pack(">I", 1)
                   + field(scope) + bytes(34) + bytes([1, 1]) + struct.pack(">4Q", 0, 0, 7, 71))
        for partition, owner in ((3, 1), (8, 17)):
            def encoded(axis_index):
                body = (b"GCD\5" + F.P.GRAPH + struct.pack(">QB3QII", partition, 0, 0, 0, 0, 0, 1)
                        + descriptor + struct.pack(">4I", 0, 0, 0, 1)
                        + struct.pack(">QQB", owner, owner+1, 2) + field(b"family-conflict")
                        + struct.pack(">QI", 0, axis_index) + field(scope) + field(changes))
                return body + hashlib.sha256(body).digest()
            seen = set()
            F.label_changes(encoded(1), partition, 7, definition, seen)
            self.assertEqual(seen, {owner})
            for index in (0, 2):
                with self.subTest(partition=partition, index=index), self.assertRaises(ValueError):
                    F.label_changes(encoded(index), partition, 7, definition, set())

    def test_range_profile_split_publication_ends_at_one_not_sixteen(self):
        def sealed(magic, body):
            wire = magic+body
            return wire+hashlib.sha256(wire).digest()
        configuration = b"c"*32
        owner = sealed(b"ROF\1", F.P.GRAPH+struct.pack(">3Q", 1, 3, 2)+configuration+b"\0")
        def frame(last):
            publication = sealed(b"RPB\1", F.P.GRAPH+struct.pack(">5Q", 1, 1, last, 3, 1)+configuration+struct.pack(">2Q", 3, 1))
            return base64.b64encode(struct.pack(">I", len(publication))+publication+struct.pack(">I", len(owner))+owner).decode()
        _, actual = F.placement(frame(1))
        self.assertEqual(actual["partition"], 3)
        self.assertEqual(actual["epoch"], 2)
        with self.assertRaises(ValueError):
            F.placement(frame(16))

    def test_profile_and_declared_inventory_do_not_relabel_old_or_partial_cases(self):
        for profile, case in ((F.C.PROFILE, "old-leader-packet"), (F.PROFILE, "metadata-only")):
            with self.assertRaises(ValueError):
                F.check(dict(Version=1, Profile=profile, Case=case))
        self.assertEqual(len(F.EXPLORATIONS), 6)
        self.assertIn("explore-B-prepared-fence-true", F.EXPLORATIONS)
        self.assertNotIn("explore-B-prepared-fence-false", F.EXPLORATIONS)

    def test_terminal_hash_and_eof_are_mandatory_even_for_framing_only(self):
        header = dict(Type="header", Artifact=dict.fromkeys(F.HEADER_KEYS))
        header["Artifact"]["Through"] = [1, 1]
        line = json.dumps(header, separators=(",", ":")).encode() + b"\n"
        footer = dict(Type="footer", Rows=0, Through=[1, 1], PayloadSHA256=hashlib.sha256(line).hexdigest())
        tail = json.dumps(footer, separators=(",", ":")).encode() + b"\n"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "framing-only.jsonl"
            for bad in (line, line + tail + b"{}\n", line + tail.replace(footer["PayloadSHA256"].encode(), b"0" * 64)):
                path.write_bytes(bad)
                with self.assertRaises(ValueError):
                    F.load(path)
            path.write_bytes(line + tail)
            self.assertEqual(F.load(path)["Records"], [])  # Framing grants no source authority.

    def test_model_pages_cannot_move_after_actual_prefix(self):
        header = dict.fromkeys(F.HEADER_KEYS)
        header.update(Model=dict(Image=None), Through=[1, 1])
        rows = [dict(Type="header", Artifact=header),
                dict(Type="record", Record={}), dict(Type="model-image", Sequence=0, Data="YQ==")]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "misordered.jsonl"
            path.write_bytes(b"".join(json.dumps(row).encode() + b"\n" for row in rows))
            with self.assertRaises(ValueError):
                F.load(path)

    def test_fixed_arrays_and_native_names_refuse_ambiguous_ownership(self):
        for wire in ([0] * 16, [1] * 15, [True] * 16):
            with self.assertRaises(ValueError):
                F.octets(wire, 16, True)
        for path in ("../native", "/native", "x/../native", "x\\native", "."):
            with self.assertRaises(ValueError):
                F.safe_name(path)

    def test_actual_active_manifest_uses_marker_iteration_not_filename_order(self):
        names = {"marker.manifest.000002.MANIFEST-000003": {},
                 "marker.manifest.000001.MANIFEST-000099": {},
                 "MANIFEST-000003": {"Directory": False},
                 "MANIFEST-000099": {"Directory": False}}
        self.assertEqual(F.active_manifest(names), "MANIFEST-000003")
        del names["MANIFEST-000003"]
        with self.assertRaises(ValueError):
            F.active_manifest(names)
        with self.assertRaises(ValueError):
            F.active_manifest({"marker.manifest.not-an-iteration.MANIFEST-000001": {}})

    def test_catalog_cookie_and_policy_are_bound_before_recovery(self):
        ledger = dict(Operations=0, Files=0, Versions=0, ShadowBytes=0, WorkBytes=0, CopyReadBytes=0, CopyWriteBytes=0)
        image = dict(Version=1, Profile=F.MODEL, Cookie=[0] * 16, Member=1, Policy=F.POLICY, Ledger=ledger, Names=[], Files=[])
        body = json.dumps(image, separators=(",", ":")).encode()
        wire = body + b"\n" + hashlib.sha256(body).hexdigest().encode()
        with self.assertRaises(ValueError):
            F.model_image(wire, F.POLICY)
        with self.assertRaises(ValueError):
            F.model_image(wire, dict(F.POLICY, Files=2048))

    def test_truncated_process_and_foreign_snapshot_versions_never_qualify(self):
        with self.assertRaises(ValueError):
            F.process(b"not-a-process")
        with self.assertRaises(ValueError):
            F.snapshot_manifest(b"AS\7\0", {})
        with self.assertRaises(ValueError):
            F.snapshot_chunk(b"\x00\x00\x00\x07", {}, {})
        with self.assertRaises(ValueError):
            F.protobuf_fields(b"\x20\x80")
        with self.assertRaises(ValueError):
            F.label_changes(b"GCD\5", 3, 1, bytes(32), set())

    def test_missing_native_prefix_cannot_be_reported_as_present_empty(self):
        data = dict(Records=[dict(Partition=3, Index=3, Outcome=None, CDC=None)], From=[1, 1], Through=[3, 1])
        with self.assertRaises(ValueError):
            F.prefix(data)


if __name__ == "__main__":
    unittest.main()
