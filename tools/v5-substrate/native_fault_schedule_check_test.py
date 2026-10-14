#!/usr/bin/env python3
"""Compact private grammar guards; no generated graph histories or processes."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SPEC=importlib.util.spec_from_file_location("native_fault_schedule",Path(__file__).with_name("native_fault_schedule_check.py"))
C=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(C)


class NativeFaultGrammar(unittest.TestCase):
    def receipt(self):
        return dict(Version=1,Profile=C.PROFILE,Cookie=[1]+[0]*15,Member=1,PID=321,Sequence=1,Arms=1,
                    Kind="short-write",Nth=1,Seen=1,Armed=False,Matched=True,
                    IO=dict(Kind="write",Path="000001.log",Directory=False,Requested=3,Written=2,
                            InputSHA256=list(hashlib.sha256(b"abc").digest()),
                            WrittenSHA256=list(hashlib.sha256(b"ab").digest()),Error="short write"))

    def test_actual_operation_receipt_rejects_arm_only_foreign_method_and_path(self):
        receipt=self.receipt()
        C.io_receipt(receipt,"wal-short-write")
        for update in ({"Matched":False},{"Armed":True},{"Seen":0},{"Member":2},{"Sequence":2},{"Cookie":[0]*16}):
            bad=copy.deepcopy(receipt);bad.update(update)
            with self.subTest(update=update),self.assertRaises(ValueError):
                C.io_receipt(bad,"wal-short-write")
        for update in ({"Path":"../other.log"},{"Path":"/other.log"},{"Kind":"sync"},{"Written":3},{"Directory":True}):
            bad=copy.deepcopy(receipt);bad["IO"].update(update)
            with self.subTest(update=update),self.assertRaises(ValueError):
                C.io_receipt(bad,"wal-short-write")

    def test_startup_sync_cannot_be_relabelled_commit_or_file_sync(self):
        receipt=self.receipt();receipt["Kind"]="directory-sync"
        receipt["IO"].update(Kind="sync",Path=".",Directory=True,Requested=0,Written=0,Error="input/output error")
        C.io_receipt(receipt,"directory-sync-during-reopen")
        with self.assertRaises(ValueError):
            C.io_receipt(receipt,"wal-file-sync")
        receipt["IO"]["Directory"]=False
        with self.assertRaises(ValueError):
            C.io_receipt(receipt,"directory-sync-during-reopen")

    def test_stream_envelope_requires_complete_hash_bound_terminal_eof(self):
        # This tests framing only. A complete envelope alone does not certify
        # any native source, graph operation, fault or process observation.
        header={"Type":"header","Artifact":{"Records":None,"Through":[4,5]}}
        row={"Type":"record","Record":{"Partition":3,"Index":4}}
        line=lambda value:json.dumps(value,separators=(",",":")).encode()+b"\n"
        prefix=line(header)+line(row)
        footer={"Type":"footer","Rows":1,"Through":[4,5],"PayloadSHA256":hashlib.sha256(prefix).hexdigest()}
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"capture.jsonl"
            path.write_bytes(prefix+line(footer))
            self.assertEqual(C.load(path)["Records"],[row["Record"]])
            for wire in (prefix,prefix+line(footer)+line(row),prefix.replace(b'"Index":4',b'"Index":3')+line(footer)):
                path.write_bytes(wire)
                with self.assertRaises(ValueError): C.load(path)
            path.write_bytes(prefix+line({**footer,"Rows":2}))
            with self.assertRaises(ValueError): C.load(path)

    def test_unknown_case_has_no_success_or_timeout_fallback(self):
        with self.assertRaisesRegex(ValueError,"finite case"):
            C.check({"Version":1,"Profile":C.PROFILE,"Case":"timeout-abort"})


if __name__=="__main__":
    unittest.main()
