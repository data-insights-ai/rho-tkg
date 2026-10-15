#!/usr/bin/env python3
"""Finite inert grammar guards; native ownership is exercised by the Go parent."""
import base64
import copy
import hashlib
import json
import tempfile
import importlib.util
from pathlib import Path
import struct
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("campaign_state_guard", Path(__file__).with_name("native_protocol_campaign_state_check.py"))
C = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(C)


def encoded(wire):
    return base64.b64encode(wire).decode()


def component(handle, revision, provenance, *, size=0):
    axis_id, definition = bytes([1])*16, bytes([2])*32
    scope = b"TS\1\2"+axis_id+definition+b"\6"
    return (b"SC\1\2"+axis_id+definition+struct.pack(">II", 1, len(scope))+scope
            +b"\1\2"+struct.pack(">QQQQ", handle, size, revision, provenance)).hex()


def snapshot_sample():
    d = {"parts": {3: {"certificate": b"actual-captured-A"}, 8: {"certificate": b"actual-captured-B"}}}
    value = {"Cut": {"Certificates": [encoded(d["parts"][p]["certificate"]) for p in (3, 8)]}, "Facts": [], "Entities": []}
    rows = {}
    for owner in range(1, 33):
        found = owner in (1, 3, 17)
        value["Facts"].append(dict(Exists=found, Active=found, Life=owner+1 if found else 0,
                                  Source=1 if owner == 3 else 0, Target=17 if owner == 3 else 0,
                                  Labels=[], Incident=[3] if owner in (1, 17) else []))
    zero = dict(present=False, null=False, handle=0, revision=0, provenance=0, kind="", i64=0)
    for owner in (1, 3, 7, 17):
        found = owner != 7
        e = dict(id=owner, found=found, kind=2 if owner == 3 else 1 if found else 0,
                 source=1 if owner == 3 else 0, target=17 if owner == 3 else 0,
                 mode=1 if owner == 3 else 0, type="cross-edge" if owner == 3 else "",
                 effective=found, effectiveLife=owner+1 if found else 0, labels=[], presence=zero.copy(), property=zero.copy())
        if found:
            row = dict(axis=(bytes([2])*32).hex(), presence=component(owner+1, 3, 23))
            e["presence"] = C.original_cell(row["presence"], row["axis"], "life")
            if owner != 3:
                handle, number = (5, 7) if owner == 1 else (19, 13)
                row.update(property=component(handle, 4, 23, size=9), value=number, valueID=handle, revision=4, provenance=23)
                e["property"] = C.original_cell(row["property"], row["axis"], "i64", number)
            rows[f"{'A' if owner < 17 else 'B'}/fact/{owner}"] = row
        value["Entities"].append(e)
    return value, d, rows


class NativeProtocolStateGuard(unittest.TestCase):
    def test_actual_139_selector_grammar_rejects_unreachable_or_nominal_inputs(self):
        valid = dict(Family="pin", Phase=7, Partition=3, Page=0, Kind=1, Side="after")
        C.recipe(valid)
        for update in (dict(Partition=8), dict(Phase=True), dict(Kind=0), dict(Page=1), dict(Family="cross")):
            with self.assertRaises(ValueError): C.recipe(valid | update)
        C.recipe(dict(Family="gc", Phase=0, Partition=3, Page=0, Kind=0, Side="before"))
        with self.assertRaises(ValueError):
            C.recipe(dict(Family="publication-race", Phase=0, Partition=3, Page=0, Kind=0, Side="before"))

    def test_held_snapshot_joins_literal_full_set_and_actual_old_revisions(self):
        value, d, rows = snapshot_sample()
        with mock.patch.object(C, "cut", return_value=({}, 4, rows)):
            C.snapshot(value, d, {})
            changes = (
                lambda v: v["Cut"]["Certificates"].reverse(),
                lambda v: v["Facts"][0]["Labels"].append("healthy"),
                lambda v: v["Entities"][0]["property"].update(revision=5),
                lambda v: v["Entities"][0]["property"].update(i64=13),
                lambda v: v["Entities"][0]["presence"].update(handle=18),
                lambda v: v["Entities"][2].update(found=True),
                lambda v: v["Entities"].pop(),
                lambda v: v["Facts"][0].update(Life=True),
            )
            for mutate in changes:
                bad = copy.deepcopy(value); mutate(bad)
                with self.assertRaises(ValueError): C.snapshot(bad, d, {})

    def test_component_scope_payload_and_revision_not_magic_only(self):
        raw = component(5, 4, 23, size=9)
        C.original_cell(raw, (bytes([2])*32).hex(), "i64", 7)
        for bad in (component(5, 0, 23, size=9), component(5, 4, 23), raw[:-2], raw[:4]+"02"+raw[6:]):
            with self.assertRaises(ValueError): C.original_cell(bad, (bytes([2])*32).hex(), "i64", 7)

    def test_exact_stream_eof_census_and_payload_binding(self):
        header = {name: None for name in C.HEADER}; header["Through"] = [0, 0]
        first = (json.dumps(dict(Type="header", Artifact=header), separators=(",", ":"))+"\n").encode()
        footer = dict(Type="footer", Rows=0, Calls=0, GC=0, Through=[0, 0], PayloadSHA256=hashlib.sha256(first).hexdigest())
        last = (json.dumps(footer, separators=(",", ":"))+"\n").encode()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"state.jsonl"
            path.write_bytes(first+last)
            self.assertEqual(C.load(path)["Records"], [])
            for invalid in (first, first+last[:-1], first+last+last,
                            first+last.replace(b'"Rows":0', b'"Rows":true'), first.replace(b'"Through":[0,0]', b'"Through":[0,1]')+last):
                path.write_bytes(invalid)
                with self.assertRaises(ValueError): C.load(path)

    def test_physical_record_census_is_not_active_byte_statistic(self):
        before = [{"stats": [0]*28} for _ in range(6)]
        for row in before: row["stats"][18], row["stats"][19] = 1000, 20
        after = copy.deepcopy(before); after[0]["stats"][19] = 19
        C.reclaimed(before, after)
        after = copy.deepcopy(before); after[0]["stats"][18] = 900
        with self.assertRaises(ValueError): C.reclaimed(before, after)

    def test_streamed_prefix_requires_every_native_index_and_present_empty(self):

        a = dict(From=[0, 0], Through=[2, 1], Records=[dict(Partition=p, Index=i, Outcome="", CDC="") for p, n in ((3, 2), (8, 1)) for i in range(1, n+1)])
        C.prefix(a)
        for mutate in (lambda x: x["Records"].pop(0), lambda x: x["Records"][0].update(CDC=None), lambda x: x["Through"].__setitem__(0, True)):
            bad = copy.deepcopy(a); mutate(bad)
            with self.assertRaises(ValueError): C.prefix(bad)


if __name__ == "__main__":
    unittest.main()
