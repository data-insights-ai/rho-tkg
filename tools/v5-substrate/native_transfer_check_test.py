#!/usr/bin/env python3
"""Compact authored guards. Inert codecs never qualify actual transfer cells."""
import copy
import hashlib
import struct
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import native_transfer_check as check
import native_transfer_cell as cell
import paired_workload_check as paired
from native_columns_check_test import NativeMovedColumnGuards
from comparison_spool import Policy,Spool,u64


class NativeTransferGuards(unittest.TestCase):
    def setUp(self):
        self.spec={"version":2,"family":"transfer-retained-pins","facts":40,"crossPercent":50,"writers":32,"layout":"columns"}
        self.grants={"MaxPages":1000,"DescribeBytes":1<<20,"ImportBytes":1<<28,"UndoBytes":1<<28}

    def test_actual_family_public_adapter_full_dimensions_and_finite_grants(self):
        native,controller,_=paired.registry(self.spec)
        self.assertIs(native,check);self.assertIs(controller,cell)
        for n in (1000,100_000,1_000_000):
            for w in (1,8,32):
                for cross in (0,10,50,100):
                    for layout in ("kv","columns"):
                        spec={**self.spec,"facts":n,"writers":w,"crossPercent":cross,"layout":layout}
                        self.assertEqual(check.shape(spec),spec)
                        for label,rtt in check.TRANSPORTS.items():
                            self.assertEqual(cell.preflight(spec,label,1800,4096,native_grants=self.grants)["targetRTTMillis"],rtt)
        self.assertEqual(cell.validate_grants(self.grants),self.grants)
        for bad in (None,{},dict(self.grants,MaxPages=True),dict(self.grants,MaxPages=4_000_001),dict(self.grants,UndoBytes=1<<40),dict(self.grants,ImportBytes=0)):
            with self.assertRaises((ValueError,TypeError)):cell.validate_grants(bad)
        for bad in (dict(self.spec,version=1),dict(self.spec,facts=True),dict(self.spec,writers=2)):
            with self.assertRaises(ValueError):check.shape(bad)
        with self.assertRaises(ValueError):cell.preflight(self.spec,"rtt-100ms",True,4096,native_grants=self.grants)
        with self.assertRaises(ValueError):cell.preflight(self.spec,"local",1800,4096)

    def test_real_ownership_boundary_refuses_link_before_binary_or_workload_effects(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);linked=root/"linked";linked.symlink_to(root/"absent")
            with self.assertRaises(ValueError):cell.ownership.owned_output(linked)
            self.assertFalse((root/"absent").exists())
            with self.assertRaises(ValueError):cell.hash_artifacts(root,"columns")
        with patch.object(cell,"executables",side_effect=AssertionError("must not open binary")):
            with self.assertRaises(ValueError):cell.execute(self.spec,"local","unused","unused",None,1800,4096,Policy(1<<20,65536,4096,check.LINE),native_grants={})

    def test_actual_protocol_choice_and_moved_footprint_are_independent(self):
        # K always Coordinate, even when its actual business footprint is local.
        for cross in (0,10,50):
            spec={**self.spec,"crossPercent":cross}
            self.assertEqual(check.placement(spec,"update",0),[3])
            self.assertEqual(check.placement(spec,"update",1),[8])
        self.assertEqual(check.placement({**self.spec,"crossPercent":100},"create",0),[3,8])
        for wire in (b"",b"GMR2",b"GCM\1"+bytes(512),b"GMQ\3"+bytes(1024)):
            with self.assertRaises(ValueError):
                (check.manifest if wire.startswith(b"GCM") else check.move_command if wire.startswith(b"GMQ") else check.move_result)(wire)

    def test_old_current_and_ack_proof_are_all_required_before_normalization(self):
        spec={**self.spec,"facts":20,"writers":1}
        def populate(spool):
            for fact in range(spec["facts"]):
                target=2*spec["facts"]+7 if check.scalar.cross(spec,fact) else 3
                old=[7+2*fact,8+2*fact,1,target,2,target+1,5,5,4+fact]
                current=[*old[:6],6,6,4+spec["facts"]+fact]
                spool.insert(b"transfer/old",u64(fact),check.encoded(old));spool.insert(b"transfer/current",u64(fact),check.encoded(current))
                spool.insert(b"transfer/updates",u64(fact),check.encoded({"before":old,"after":current}))
        with tempfile.TemporaryDirectory() as tmp:
            with Spool(tmp,Policy(4<<20,65536,4096,check.LINE)) as spool:
                with self.assertRaises(ValueError):check.transfer_answers(spec,spool)
            with Spool(tmp,Policy(4<<20,65536,4096,check.LINE)) as spool:
                populate(spool)
                with self.assertRaises(ValueError):check.transfer_answers(spec,spool)
            with Spool(tmp,Policy(4<<20,65536,4096,check.LINE)) as spool:
                populate(spool)
                for fact in range(spec["facts"]):spool.insert(b"transfer/update-authority",u64(fact),b"inert-joined-original-ACK")
                first,count=check.transfer_answers(spec,spool);self.assertEqual(count,40)
                key=u64(0);prior=spool.lookup(b"transfer/current",key);changed=check.decoded(prior);changed[3]=999
                spool.replace(b"transfer/current",key,prior,check.encoded(changed))
                with self.assertRaises(ValueError):check.transfer_answers(spec,spool)
                self.assertEqual(len(first),64)

    def test_imported_original_requires_complete_native_namespace_and_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            with Spool(tmp,Policy(1<<20,65536,4096,check.LINE)) as spool:
                for key,wire,control in ((b"",b"",False),(b"ga\4"+bytes(40),b"GRO\4"+bytes(116),False),(b"GPR\1"+bytes(40),b"GCM\1"+bytes(400),True)):
                    with self.assertRaises(ValueError):check.original_record(key,wire,control,spool)
        def sealed(body):return body+hashlib.sha256(body).digest()
        start=sealed(b"GEC\1"+b"\1"+struct.pack(">II3Q",0,0,0,0,0)+hashlib.sha256(b"rho-tkg:range-move-replay:v1\0").digest()+b"\0"+struct.pack(">IIQ",0,0,0))
        self.assertEqual(check.replay_cursor(start)["sequence"],0)
        body=bytearray(start[:-32]);body[4]=11
        with self.assertRaises(ValueError):check.replay_cursor(sealed(body))
        with self.assertRaises(ValueError):check.replay_cursor(start[:-1])

    def test_move_unknown_new_call_preserves_original_and_spends_global_retry(self):
        def sealed(body):return body+hashlib.sha256(body).digest()
        def field(body):return struct.pack(">I",len(body))+body
        def encoded(body):return check.scalar.base64.b64encode(body).decode()
        graph=bytes.fromhex(check.raw.GRAPH);request=b"r"*16
        source=graph+u64(3)+b"g"*16+u64(1)+u64(1)+b"d"*32+check.AGREEMENT
        target=source[:16]+u64(8)+source[24:]
        publication=sealed(b"RPB\1"+graph+struct.pack(">5Q",1,1,100,3,1)+b"c"*32+u64(3)+u64(2))
        intent=sealed(b"GMI\3"+request+source+target+field(publication)+struct.pack(">QIII3Q",1,100,32,65536,1<<20,1<<20,1<<20))
        parsed=check.column.pooled_move_intent(check.raw,intent,check.AGREEMENT)
        def status(part,phase,index):
            return sealed(b"GMO\3"+graph+u64(part)+parsed["move"]+parsed["digest"]+bytes((phase,1))+struct.pack(">6Q",index,1,2,0,3,4)+b"d"*32+b"\0"+u64(0)+bytes(32))
        done=b"GMR2"+field(sealed(b"APS\1"+source))+request+parsed["move"]+parsed["digest"]+b"\0\1"+field(status(3,8,11))+field(status(8,8,12))+field(status(8,6,10))+b"\0\0"
        self.assertEqual(check.move_result(done)["move"],parsed["move"])
        def q(action):
            return {"Version":1,"Action":action,"Request":[0]*16,"Sequence":0,"Count":0,"Cursor":0,"Range":0,"Origin":0,"Original":None,"Key":None,"Control":False,"Feed":None,"Query":None}
        prepare=q("move-prepare");prepare.update(Request=list(request),Range=1)
        run=q("move-run");run["Original"]=encoded(intent)
        def row(call,question,body,error=None):
            # A decoded service error preserves its canonical wrapper even
            # without a result body; an absent transport frame is distinct.
            reply={"Version":1,"Action":question["Action"],"Body":encoded(body) if body is not None else None,"Nanos":1}
            value={"Stage":"move-1","Member":1,"Call":call,"Attempt":1,"Input":question,"Reply":reply,"AttemptNanos":1,"OperationNanos":1}
            if error:value["Error"]=error
            return value
        rows=[row(1,prepare,intent),row(2,run,None,"actual unknown transport"),row(3,run,done)]
        end={"spec":self.spec,"FramesCeiling":1000,"Admission":{"BudgetSeconds":1800},"ControlExtraInvocations":1}
        with patch.object(check,"lines",return_value=iter(rows)):
            self.assertEqual(len(list(check.controls(Path("inert"),end))),3)
        # Canonical producer codec fixture, not captured OS evidence:
        # nativeTransferClient.exchange resets its reply before decoding aux;
        # graphProcessMux.send can return errLimit with no response when the
        # existing pending-call grant is full. SAME move input stays unchanged.
        absent=copy.deepcopy(rows)
        absent[1]["Reply"]={"Version":0,"Action":"","Body":None,"Nanos":0}
        absent[1]["Error"]="graphapply: resource limit"
        with patch.object(check,"lines",return_value=iter(absent)):
            self.assertEqual(len(list(check.controls(Path("inert"),end))),3)
        with patch.object(check,"lines",return_value=iter(absent)):
            with self.assertRaises(ValueError):list(check.controls(Path("inert"),{**end,"ControlExtraInvocations":0}))
        with patch.object(check,"lines",return_value=iter(absent[:2])):
            with self.assertRaises(ValueError):list(check.controls(Path("inert"),end))
        malformed=[]
        for name,value in (("Version",False),("Action","move-run"),("Body",""),("Body",encoded(done)),("Nanos",False),("Nanos",1),("Failure",0),("Failure",2)):
            bad=copy.deepcopy(absent);bad[1]["Reply"][name]=value;malformed.append(bad)
        for error in (None,"",False,1):
            bad=copy.deepcopy(absent)
            if error is None:del bad[1]["Error"]
            else:bad[1]["Error"]=error
            malformed.append(bad)
        bad=copy.deepcopy(rows);bad[1]["Reply"]["Action"]="foreign";malformed.append(bad)
        for bad in malformed:
            with self.subTest(reply=bad[1]["Reply"],error=bad[1].get("Error")):
                with patch.object(check,"lines",return_value=iter(bad)):
                    with self.assertRaises(ValueError):list(check.controls(Path("inert"),end))
        with patch.object(check,"lines",return_value=iter(rows)):
            with self.assertRaises(ValueError):list(check.controls(Path("inert"),{**end,"ControlExtraInvocations":0}))
        changed=copy.deepcopy(rows);changed[-1]["Input"]["Original"]=encoded(intent[:-1])
        with patch.object(check,"lines",return_value=iter(changed)):
            with self.assertRaises(ValueError):list(check.controls(Path("inert"),end))

    def test_missing_raw_eof_or_terminal_never_qualifies(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(OSError):check.verify(tmp,tmp,Policy(1<<20,65536,4096,check.LINE))
            Path(tmp,"result.json").write_bytes(b"{}\n")
            with self.assertRaises((ValueError,KeyError)):check.verify(tmp,tmp,Policy(1<<20,65536,4096,check.LINE))
        self.assertEqual(check.MINIMUM_CONTRACT["snapshotAfterRelease"],"feed-token-refused")
        self.assertNotEqual(check.AGREEMENT,check.retained.AGREEMENT)


if __name__=="__main__":unittest.main()
