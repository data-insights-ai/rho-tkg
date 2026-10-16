#!/usr/bin/env python3
"""Compact fail-closed grammar guards; no synthetic native qualification run."""
import hashlib
import copy
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
    def test_corrupted_wal_requires_actual_resolve_at_certificate_source(self):
        # Exact source bytes from the genuine .log RED artifact. These inert
        # codec fixtures are not a new process or corruption qualification.
        wire = base64.b64decode("R0NTAQEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAx+ZvCIHNZwJYsMljn8hfamXy6yH7dkwhySe1kINmDGxFh5/+yTtdIYf79sxmJTPQgQAAAAAAAAAGQAAAAAAAAAEAAAAAAAAAAYCAL79aXIyZr3son/jilMpxsJQoZSSKn4QDo3k07QgA68c")
        certificate = F.P.body(base64.b64decode("R1NDAQEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAwQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAJ5YC6nkH1VeCJ9gWUrGypZl0GVgWG9BK0C2EVZGWuqsJBIlGt6z/F1VDg6BAEAJP0UI1Pgtd0/tLFH8VjXQSDxbmxsd6FADuA9DY875OJZY71rgP6GlHHOCs6i03bF9QoAAAAAAAAAAYAAAAAAAAAGwAAAAAAAAAGAAAAAAAAABlHUgMFAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAADAAAAAAAAAAIAAAAAAAAABQAAAAAAAAAIOTyV74zR45q5btxtlawXmF0Im3tgrSE9oQKGpAhPYBAAAAAAAAAACQAAAAAAAAABAAAAAAAAAAPlgLqeQfVV4In2BZSsbKlmXQZWBYb0ErQLYRVkZa6qwsY+eM2bR9S+5R2o/y6F9T1w0SuuIrUStb4bZ9zWdLKZlsUMepRlkILqQhg0X7EkBewZSLuTb9aiirMcaS0RsSM="), b"GSC\1")
        F.required_wal_outcome(wire, certificate)
        body = F.P.body(wire, b"GCS\1")
        def encoded(offset, replacement):
            changed = b"GCS\1" + body[:offset] + replacement + body[offset + len(replacement):]
            return changed + hashlib.sha256(changed).digest()
        for offset, replacement in ((72,b"\1"),(72,b"\2"),(72,b"\3"),
                                    (97,b"\0"),(97,b"\1"),(98,b"\1"),
                                    (16,struct.pack(">Q",8)),(0,b"x"*16),
                                    (73,struct.pack(">Q",26)),(89,struct.pack(">Q",7)),
                                    (24,bytes(16)),(40,bytes(32))):
            with self.subTest(offset=offset,replacement=replacement), self.assertRaises(ValueError):
                F.required_wal_outcome(encoded(offset,replacement), certificate)
        for changed in (wire[:-1], wire[:-1]+bytes([wire[-1]^1])):
            with self.assertRaises(ValueError): F.required_wal_outcome(changed, certificate)
        cut_control = b"GSO\1\1\1" + struct.pack(">2Q", 6, 25)
        with self.assertRaises(ValueError):
            F.required_wal_outcome(cut_control + hashlib.sha256(cut_control).digest(), certificate)

    def test_sender_loss_has_exact_separate_profile_and_two_inflight_boundaries(self):
        self.assertEqual(F.SENDER_CASES, {"snapshot-sender-before-append", "snapshot-sender-after-append"})
        self.assertTrue(F.SENDER_CASES.isdisjoint(F.CASES | F.EXPLORATIONS))
        for version, profile, case in ((1,F.PROFILE,"snapshot-sender-before-append"),
                                       (3,F.SENDER_PROFILE,"snapshot-chunk-interruption"),
                                       (3,F.SENDER_PROFILE,"snapshot-sender-after-activate")):
            with self.subTest(version=version,case=case), self.assertRaises(ValueError):
                F.check(dict(Version=version,Profile=profile,Case=case))

    def test_sender_partial_import_cannot_publish_active_or_phantom_state(self):
        before = dict(code=0,mask=0,outcome=b"",changes=b"",incarnation=b"r"*16,applied=4,image=b"old",cut=b"c"*32,stats=[0]*28)
        before["stats"][1] = 4
        partial, closed = copy.deepcopy(before), copy.deepcopy(before)
        partial["stats"][23] = 1
        F.sender_active_root(before,partial,closed)
        for mutate in (lambda p,c:p.update(image=b"partial-new"), lambda p,c:p.update(applied=5),
                       lambda p,c:p["stats"].__setitem__(24,1), lambda p,c:c["stats"].__setitem__(23,1),
                       lambda p,c:c.update(incarnation=b"x"*16), lambda p,c:c["stats"].__setitem__(1,5)):
            p,c=copy.deepcopy(partial),copy.deepcopy(closed);mutate(p,c)
            with self.assertRaises(ValueError): F.sender_active_root(before,p,c)

    def test_sender_leader_attribution_needs_actual_readindex_not_member_or_vote_label(self):
        read = dict(code=0,mask=0,aux=b"native-configuration",incarnation=b"s"*16,sequence=2,applied=4)
        state = dict(code=0,mask=0,ready=1,incarnation=b"s"*16,sequence=3,applied=4,stats=[0]*28)
        state["stats"][2:4] = [1,1]
        request = base64.b64encode(b"s"*16+struct.pack(">QBQQIQQBI",2,29,0,1,0,0,0,0,0)).decode()
        F.sender_leader_read(read,state,request)
        for mutate in (lambda r,s:r.update(code=3),lambda r,s:s.update(ready=0),
                       lambda r,s:r.update(incarnation=b"x"*16),lambda r,s:r.update(sequence=4),
                       lambda r,s:s["stats"].__setitem__(2,0)):
            r,s=copy.deepcopy(read),copy.deepcopy(state);mutate(r,s)
            with self.assertRaises(ValueError): F.sender_leader_read(r,s,request)

    def test_sender_stale_handle_requires_exact_actual_refusal_not_replacement_authority(self):
        # Inert process codec guard only; it is not an executed sender loss.
        wire = b"n"*16+struct.pack(">QBQQIQQBI",2,15,7,0,0,0,0,0,0)
        encoded = base64.b64encode(wire).decode()
        reply = dict(incarnation=b"n"*16,sequence=2,code=1,mask=1<<4,handles=[],aux=b"",outcome=b"",changes=b"",packets=[])
        F.sender_stale_request(encoded,reply,7)
        for mutate in (lambda r:r.update(mask=1<<7), lambda r:r.update(code=0),
                       lambda r:r.update(aux=b"new-handle"), lambda r:r.update(handles=[7]),
                       lambda r:r.update(incarnation=b"o"*16), lambda r:r.update(sequence=3)):
            bad=copy.deepcopy(reply);mutate(bad)
            with self.assertRaises(ValueError): F.sender_stale_request(encoded,bad,7)
        for bad in (wire[:-1],wire+b"x",wire[:24]+b"\x0e"+wire[25:]):
            with self.assertRaises(ValueError): F.sender_stale_request(base64.b64encode(bad).decode(),reply,7)

    def test_sender_local_publication_keeps_cut_binding_distinct_from_import_audit(self):
        # Inert state-binding guard only. Full native manifest/packet/chunk
        # authentication remains mandatory in snapshot_sender before this door.
        sender = dict(code=0,mask=0,stats=[0]*28,applied=31,image=b"published-image",cut=bytes(32),manifest=bytes(32))
        sender["stats"][1] = 31
        manifest = dict(index=31,image=b"published-image",cut=b"p"*32)
        receiver = dict(applied=29)
        F.sender_published_source(sender,manifest,receiver)
        for mutate in (lambda s,m,r:s.update(code=3),lambda s,m,r:s.update(mask=1<<7),
                       lambda s,m,r:s["stats"].__setitem__(1,30),lambda s,m,r:s.update(applied=32),
                       lambda s,m,r:s.update(image=b"current-image"),lambda s,m,r:r.update(applied=31),
                       lambda s,m,r:m.update(cut=bytes(32)),lambda s,m,r:m.update(index=30),
                       lambda s,m,r:s["stats"].__setitem__(5,31),lambda s,m,r:s.update(cut=m["cut"]),
                       lambda s,m,r:s.update(manifest=b"imported-audit")):
            s,m,r=copy.deepcopy(sender),copy.deepcopy(manifest),copy.deepcopy(receiver)
            mutate(s,m,r)
            with self.assertRaises(ValueError): F.sender_published_source(s,m,r)

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
