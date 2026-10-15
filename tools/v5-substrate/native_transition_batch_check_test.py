#!/usr/bin/env python3
"""Compact inert codec guards; actual native receipts come only from children."""
import copy
import hashlib
import importlib.util
import struct
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("batch_guard_owner", Path(__file__).with_name("native_transition_batch_check.py"))
B = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(B)


def sample():
    def seal(magic, body):
        wire = magic + body
        return wire + hashlib.sha256(wire).digest()
    manifest = seal(b"GCM\1", b"inert-format-only")
    body = b"\2" + B.P.GRAPH + struct.pack(">Q", 3) + struct.pack(">I", len(manifest)) + manifest + bytes(4)
    command = seal(b"GCT\1", body)
    digest, cookie, run = list(hashlib.sha256(command).digest()), [1]*16, bytes([2]*16)
    c = dict(Version=1, Action="arm", Cookie=cookie, Sequence=1, Kind=2, Side="after", CommandSHA256=digest)
    e = dict(Selection={k: c[k] for k in ("Cookie", "Sequence", "CommandSHA256")}, Kind=2, Stage=1, Synced=False, Matches=1, EntryCount=1, Position=1, FirstIndex=7, LastIndex=7, Index=7, Term=3, EntryType=0, CommandBytes=len(command), CommandSHA256=digest, EntryManifestSHA256=list(hashlib.sha256(b"rho-tkg:stable-storage-entry-manifest:v1\0" + struct.pack(">QQQQQ", 1, 7, 3, 0, len(command)) + bytes(digest)).digest()), BatchBytes=128, BatchPayloadSHA256=[3]*32, Work=dict(Entries=2, Bytes=4096, OutputBytes=1024))
    zero = {k: 0 for k in B.EVENT}
    zero.update(Selection=dict(Cookie=[0]*16, Sequence=0, CommandSHA256=[0]*32), Synced=False, CommandSHA256=[0]*32, EntryManifestSHA256=[0]*32, BatchPayloadSHA256=[0]*32, Work=dict(Entries=0, Bytes=0, OutputBytes=0))
    arm = dict(Version=1, Profile=B.PROFILE, Cookie=cookie, Run=list(run), Member=1, PID=123, Sequence=1, Control=c, State="armed", DeadlineUnixNanos=1000, Attempts=0, Work=0, Entries=0, Output=24576, Count=0, Events=[copy.deepcopy(zero), copy.deepcopy(zero)])
    receipt = {**arm, "State": "observing", "Attempts": 1, "Work": 4096, "Entries": 2, "Output": 25600, "Count": 2, "Events": [e, copy.deepcopy({**e, "Stage": 2, "Synced": True})]}
    return arm, receipt, run, command


class ExactBatchGuard(unittest.TestCase):
    def test_same_native_entry_and_guaranteed_after_pair_required(self):
        arm, receipt, run, command = sample()
        B.receipt_pair(arm, receipt, run, 1000, 2, 3, 2, "after", command)
        changed = copy.deepcopy(receipt)
        changed["Events"][1]["Synced"] = False
        with self.assertRaises(ValueError):
            B.receipt_pair(arm, changed, run, 1000, 2, 3, 2, "after", command)
        changed = copy.deepcopy(receipt)
        changed["Events"][1]["BatchPayloadSHA256"][0] ^= 1
        with self.assertRaises(ValueError):
            B.receipt_pair(arm, changed, run, 1000, 2, 3, 2, "after", command)

    def test_ambiguous_membership_and_unfunded_json_refuse(self):
        arm, receipt, run, command = sample()
        changed = copy.deepcopy(receipt)
        changed["Events"][0]["Matches"] = 2
        with self.assertRaises(ValueError):
            B.receipt_pair(arm, changed, run, 1000, 2, 3, 2, "after", command)
        changed = copy.deepcopy(receipt)
        changed["Output"] = arm["Output"]
        with self.assertRaises(ValueError):
            B.receipt_pair(arm, changed, run, 1000, 2, 3, 2, "after", command)

    def test_original_deadline_and_exact_owner_never_substituted(self):
        arm, receipt, run, command = sample()
        changed = copy.deepcopy(receipt)
        changed["DeadlineUnixNanos"] += 1
        with self.assertRaises(ValueError):
            B.receipt_pair(arm, changed, run, 1000, 2, 3, 2, "after", command)
        changed = copy.deepcopy(receipt)
        changed["PID"] += 1
        with self.assertRaises(ValueError):
            B.receipt_pair(arm, changed, run, 1000, 2, 3, 2, "after", command)


if __name__ == "__main__":
    unittest.main()
