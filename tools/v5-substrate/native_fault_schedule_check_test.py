#!/usr/bin/env python3
"""Compact private grammar guards; no generated graph histories or processes."""
import copy
import base64
import hashlib
import importlib.util
import json
import struct
from pathlib import Path
import tempfile
import unittest

SPEC=importlib.util.spec_from_file_location("native_fault_schedule",Path(__file__).with_name("native_fault_schedule_check.py"))
C=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(C)


class NativeFaultGrammar(unittest.TestCase):
    def test_program_decision_recovery_requires_actual_successor_request_and_same_original(self):
        # Canonical process-codec guard only, never native election/history
        # qualification. The full parent checks actual raw prefix and processes.
        encoded = lambda wire: base64.b64encode(wire).decode()
        field = lambda wire: struct.pack(">I", len(wire))+wire
        def response(incarnation, sequence, *, ready=1, code=0, mask=0, applied=30):
            stats = [0]*28
            stats[1], stats[2], stats[3], stats[4] = applied, 3, 5, applied
            return (incarnation+struct.pack(">28Q", *stats)+bytes(64)+struct.pack(">QI", 0, 0)+field(b"")
                    +struct.pack(">BQQBH", ready, sequence, applied, code, mask)+field(b"")
                    +bytes(8)+field(b"same-native-root")+field(b"")+field(b"")
                    +struct.pack(">BQI", 0, 0, 0)+field(b""))
        proof = dict(Member=5, PID=321, EventID=12, Instance="voter5/pid321/"+(b"s"*16).hex(),
                     ReadRequest=encoded(b"s"*16+struct.pack(">QBQQIQQBI", 2, 29, 0, 1, 0, 0, 0, 0, 0)),
                     Configuration=encoded(response(b"s"*16, 2, ready=0)), State=encoded(response(b"s"*16, 3)))
        data = dict(OriginalB=encoded(b"same-business-original"),
                    Processes=[{},dict(EventID=11, Volume="/owned/cohort/6", RecoveryRaw=encoded(response(b"r"*16, 8)),
                                      Raw=encoded(json.dumps(dict(DecisionRecovery=proof)).encode()))],
                    Duplicates=[{}, {}, dict(Actor=5, Original=encoded(b"same-business-original"), Response=encoded(response(b"s"*16, 20)))])
        cohort=[]
        for member in range(1,7):
            pid=321 if member==5 else 400+member
            instance=proof["Instance"] if member==5 else f"voter{member}/pid{pid}/"+bytes([member]*16).hex()
            raw=dict(Member=member,PID=pid,Instance=instance,Volume=f"/owned/cohort/{member}",Reaped=True,Signaled=False,Signal="clean-exit",ExitCode=0,Success=True)
            cohort.append(dict(Member=member,EventID=20+member,Action="stop",Instance=instance,Volume=raw["Volume"],Reaped=True,Signal="clean-exit",Raw=encoded(json.dumps(raw).encode())))
        self.assertEqual(C.decision_recovery_actor(data,6,program_roles=dict(leaders=(2,6)),cohort=cohort),5)
        for mutate in (lambda p,d:p.update(Member=6), lambda p,d:p.update(PID=0),
                       lambda p,d:p.update(EventID=10), lambda p,d:p.update(Instance="voter5/pid321/"+(b"x"*16).hex()),
                       lambda p,d:p.update(Configuration=encoded(response(b"s"*16,2,code=3,mask=128))),
                       lambda p,d:p.update(State=encoded(response(b"s"*16,3,ready=0))),
                       lambda p,d:p.update(ReadRequest=encoded(b"s"*16+struct.pack(">QBQQIQQBI",2,29,0,2,0,0,0,0,0))),
                       lambda p,d:d["Duplicates"][2].update(Actor=6),
                       lambda p,d:d["Duplicates"][2].update(Original=encoded(b"changed-original")),
                       lambda p,d:d["Duplicates"][2].update(Response=encoded(response(b"x"*16,20))),
                       lambda p,d:p.update(State=encoded(response(b"s"*16,3,applied=31)))):
            bad, changed = copy.deepcopy(data), copy.deepcopy(proof)
            mutate(changed,bad)
            bad["Processes"][1]["Raw"]=encoded(json.dumps(dict(DecisionRecovery=changed)).encode())
            with self.assertRaises(ValueError): C.decision_recovery_actor(bad,6,program_roles=dict(leaders=(2,6)),cohort=cohort)
        for mutate in (lambda c:c.pop(), lambda c:c[4].update(Member=6), lambda c:c[4].update(Instance="voter5/pid999/"+(b"s"*16).hex()),
                       lambda c:c[4].update(Volume="/foreign/5"), lambda c:c[4].update(EventID=10), lambda c:c[4].update(Reaped=False)):
            bad=copy.deepcopy(cohort);mutate(bad)
            with self.assertRaises(ValueError): C.decision_recovery_actor(data,6,program_roles=dict(leaders=(2,6)),cohort=bad)
        literal=copy.deepcopy(data)
        literal["Processes"][1]["Raw"]=encoded(b"{}")
        self.assertEqual(C.decision_recovery_actor(literal,4),4)
        with self.assertRaises(ValueError): C.decision_recovery_actor(data,4)
        with self.assertRaises(ValueError): C.decision_recovery_actor(literal,6,program_roles=dict(leaders=(2,6)),cohort=cohort)
        with self.assertRaises(ValueError): C.decision_recovery_actor(data,6,program_roles=dict(leaders=(2,6)))

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

    def test_queued_order_requires_both_admissions_and_actual_native_inputs(self):
        # Grammar-only refusals: these arbitrary bytes never grant an original,
        # completed capacity read, private stamp or genuine history authority.
        encoded = lambda wire: base64.b64encode(wire).decode()
        queue = dict(Version=1, Profile=C.DECISION_PROFILE, Order="commit-first",
                     Cookie=[1]+[0]*15, Sequence=1, Member=4, PID=321, Mask=3,
                     Queued=True, Closed=False, Accepted=[True,True], Errors=[0,0],
                     Controls=6, PrepaidBytes=20<<20, BeforeApplied=10,
                     AfterApplied=10, BeforeLastIndex=10, AfterLastIndex=12,
                     Candidates=[encoded(b"abort"),encoded(b"commit")],
                     EntryWires=[encoded(b"commit"),encoded(b"abort")])
        for update, reason in (({"Accepted":[True,False]},"both genuine"),
                               ({"AfterApplied":11},"before first application"),
                               ({"AfterLastIndex":11},"two persisted"),
                               ({"Cookie":[0]*16},"live owner"),
                               ({"Sequence":2},"lifecycle"),
                               ({"EntryWires":queue["Candidates"]},"native log inputs"),
                               ({"Closed":True},"lifecycle")):
            bad=copy.deepcopy(queue);bad.update(update)
            with self.subTest(update=update), self.assertRaisesRegex(ValueError,reason):
                C.decision_queue(bad,"commit-first")
        with self.assertRaisesRegex(ValueError,"finite order"):
            C.decision_queue(queue,"timeout-abort")
        with self.assertRaisesRegex(ValueError,"fresh queued version"):
            C.check({"Version":1,"Profile":C.DECISION_PROFILE,"Case":"commit-first","Queue":queue})


if __name__=="__main__":
    unittest.main()
