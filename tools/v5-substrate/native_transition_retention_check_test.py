#!/usr/bin/env python3
"""Compact inert codec/refusal guards; these do not create native evidence."""
import base64
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import struct
import unittest

SPEC=importlib.util.spec_from_file_location("retention_guard",Path(__file__).with_name("native_transition_retention_check.py"))
R=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(R)


def sealed(magic,body):
    wire=magic+body
    return wire+hashlib.sha256(wire).digest()


def control(action="inspect",**fields):
    q=dict(Version=1,Action=action,Cookie=[0]*16,Sequence=0,Request=[0]*16,Floors=[0]*6,Index=0,Original=None)
    q.update(fields)
    return b"NSB1NGR1"+json.dumps(q,separators=(",",":")).encode()


class RetentionGuard(unittest.TestCase):
    def test_empty_root_is_only_exact_native_failure_shape(self):
        empty=dict(Generation=0,Index=0,Image=None,ImageHash=[0]*32)
        R.root(empty,empty=True)
        for update in (dict(Image=""),dict(Index=1),dict(ImageHash=[1]*32)):
            with self.assertRaises(ValueError):R.root({**empty,**update},empty=True)
        image=b"x"*172
        actual=dict(Generation=1,Index=7,Image=base64.b64encode(image).decode(),ImageHash=list(hashlib.sha256(image).digest()))
        self.assertEqual(R.root(actual),image)
        with self.assertRaises(ValueError):R.root(actual,empty=True)
        with self.assertRaises(ValueError):R.root({**actual,"ImageHash":[0]*32})

    def test_native_original_scope_seal_and_installed_coordinates(self):
        scope=b"s"*120
        wire=sealed(b"GGC\1",scope+b"i"*16+b"r"*16+b"c"*32+bytes(32)+struct.pack(">6Q",7,7,8,7,7,5))
        parsed=R.ggc(wire,scope)
        self.assertEqual(parsed["floors"],[7,7,8,7,7,5])
        self.assertEqual(parsed["id"],hashlib.sha256(wire).digest())
        with self.assertRaises(ValueError):R.ggc(wire,b"f"*120)
        with self.assertRaises(ValueError):R.ggc(wire[:-1]+bytes([wire[-1]^1]),scope)
        with self.assertRaises(ValueError):R.ggc(sealed(b"GGC\1",wire[4:-80]+struct.pack(">6Q",(1<<64)-2,7,8,7,7,5)),scope)
        result=sealed(b"GGO\1",b"\1"+parsed["id"]+b"h"*32+struct.pack(">3Q",9,8,5))
        self.assertEqual(R.ggo(result)["source"],8)
        for status,index,source in ((2,9,8),(1,8,8),(1,9,0)):
            with self.assertRaises(ValueError):R.ggo(sealed(b"GGO\1",bytes([status])+parsed["id"]+b"h"*32+struct.pack(">3Q",index,source,5)))

    def test_control_ignored_fields_do_not_acquire_authority(self):
        R.control(control())
        R.control(control("open",Cookie=[1]*16,Sequence=1,Index=7))
        for wire in (control(Index=1),control(Cookie=[1]*16),control("submit",Original="eA=="),control("recover"),control("prepare"),control("open")):
            with self.assertRaises(ValueError):R.control(wire)
        with self.assertRaises(ValueError):R.control(control().replace(b"NGR1",b"NGR2",1))

    def test_incomplete_owner_or_missing_prefix_cannot_qualify(self):
        with self.assertRaises(ValueError):R.calls(dict(Calls=[],OwnerCalls=1),b"s"*120,{}, {})
        result=dict(index=2)
        with self.assertRaises(ValueError):R.prefix(dict(Records=[],From=[1,1],Through=[2,1]),[],result)
        # An empty present record does not certify the missing installed GGO.
        rows=[dict(Partition=3,Index=2,Outcome="",CDC="")]
        with self.assertRaises(ValueError):R.prefix(dict(Records=rows,From=[1,1],Through=[2,1]),[],result)

    def test_gc_diagnostic_binds_actual_plan_source_and_synced_batch(self):
        spec=importlib.util.spec_from_file_location("retention_state_guard",Path(__file__).with_name("native_transition_state_check_test.py"))
        guard=importlib.util.module_from_spec(spec);spec.loader.exec_module(guard)
        arm,receipt=guard.sample()
        for value in (arm,receipt):
            value["Member"]=1;value["Control"]["Kind"]=7
            value["Control"]["Filter"].update(SourceCutID=[0]*32,ManifestID=[0]*32,PlanID=[9]*32,Sequence=0,MatchSequence=False)
        for event in receipt["Events"]:
            event["Kind"]=7
            event["Selection"]["Identity"].update(SourceCutID=[0]*32,ManifestID=[0]*32,PlanID=[9]*32,Sequence=0,Section=1,SourceImageSHA256=[10]*32)
        R.S.pair(arm,receipt,bytes([8]*16),1000,member=1)
        for mutate in (lambda x:x["Events"][1]["Selection"]["Identity"].update(PlanID=[8]*32),
                       lambda x:x["Events"][1]["Selection"]["Identity"].update(SourceIndex=8),
                       lambda x:x["Events"][1].update(Synced=False),
                       lambda x:x.update(State="unavailable")):
            bad=copy.deepcopy(receipt);mutate(bad)
            with self.assertRaises(ValueError):R.S.pair(arm,bad,bytes([8]*16),1000,member=1)


if __name__=="__main__":unittest.main()
