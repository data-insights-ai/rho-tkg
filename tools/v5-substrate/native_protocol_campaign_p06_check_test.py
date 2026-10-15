#!/usr/bin/env python3
"""Inert role/grammar/packet guards; no fabricated full native success."""
import base64
import copy
import hashlib
import json
import struct
import tempfile
import unittest
from pathlib import Path

import native_protocol_campaign_p06_check as N


def u64(value):return struct.pack(">Q",value)
def field(value):return struct.pack(">I",len(value))+value


def process(sequence,packets=()):
    wire=bytes([7])*16+bytes(28*8+32+32)+u64(0)+struct.pack(">I",0)
    wire+=field(b"")+b"\1"+u64(sequence)+u64(0)+bytes(3)+field(b"")+struct.pack(">I",len(packets))
    for sender,receiver,payload in packets:wire+=u64(sender)+u64(receiver)+b"\0"+field(payload)
    return wire+struct.pack(">I",0)+field(b"")*3+b"\0"+u64(0)+struct.pack(">I",0)+field(b"")


class P06ProgramChecks(unittest.TestCase):
    def refused(self,fn,*args,**kwargs):
        with self.assertRaises(ValueError):fn(*args,**kwargs)

    def test_actual_role_vectors_keep_first_and_selected_failover_distinct(self):
        for initial in ((1,4),(2,6)):
            for family in ("decide","recovery","terminal","register","first-prepare","initial-batch"):
                for part in (3,8):
                    selected=list(initial)
                    if family in ("decide","recovery","terminal"):selected[0]=3-selected[0]
                    after=list(selected);group=0 if part==3 else 1
                    after[group]=3-after[group] if group==0 else 10-after[group]
                    expected=dict(initial=initial,selected=tuple(selected),after=tuple(after))
                    self.assertEqual(N.role_vectors(initial,selected,after,family,part),expected)
                    self.refused(N.role_vectors,initial,selected,selected,family,part)
                    bad=after.copy();bad[1-group]=10-bad[1-group] if group==0 else 3-bad[1-group]
                    self.refused(N.role_vectors,initial,selected,bad,family,part)
        for bad in ({},dict(initial=(True,4),selected=(2,4),after=(1,4)),dict(initial=(2,6),selected=(1,5),after=(2,5)),dict(initial=[2,6],selected=(1,6),after=(2,6))):
            self.refused(N.C.program_voters,bad)

    def test_old_loaders_reject_program_metadata_unless_explicit_fresh_entry(self):
        for module in (N.C,N.I,N.Q,N.B):
            header={key:None for key in module.HEADER}
            if module is not N.B:header["P06"]={key:None for key in N.C.P06}
            header.update(Version=1,Profile=module.PROFILE,Program={})
            line=(json.dumps(dict(Type="header",Artifact=header),sort_keys=True)+"\n").encode()
            footer=(json.dumps(dict(Type="footer",Rows=0,Through=None,PayloadSHA256=hashlib.sha256(line).hexdigest()))+"\n").encode()
            with tempfile.TemporaryDirectory() as directory:
                path=Path(directory)/"inert.jsonl";path.write_bytes(line+footer)
                # Shape-only loader acceptance is not a completed oracle proof.
                loaded=module.load(path,program=True)
                self.assertIn("Program",loaded)
                self.refused(module.load,path)
                self.refused(module.check,{key:value for key,value in loaded.items() if key!="Program"})

    def test_ready_uses_actual_tick_frames_not_selected_command_emission(self):
        scope=bytes(range(120));packets=[];frames=[]
        for to in (2,3):
            message=bytes([16,to,24,1,32,2])
            payload=b"RP\1\0"+scope[:40]+scope[88:120]+struct.pack(">I",len(message))+message
            packets.append((1,to,payload));frames.append(dict(From=1,To=to,Snapshot=False,Payload=base64.b64encode(payload).decode()))
        event=dict(Action="reorder",From=1,To=2,Ordinal=1,Bytes=len(packets[0][2]),SHA256=list(hashlib.sha256(packets[0][2]).digest()),ConfiguredDelayMillis=0,ObservedDelayNanos=0)
        program=dict(Spec=dict(Message="reorder"),Frames=frames,Packets=[event],FrameCandidates=2,TickResponse=base64.b64encode(process(9,packets)).decode(),MessageFrom=1,MessageTo=2)
        wrapper=dict(Mechanism="emitted-predecessor-tick-packets",AfterPredecessor=base64.b64encode(process(10)).decode())
        state=dict(incarnation=bytes([7])*16,sequence=8,stats=[0,0,2])
        N.packets(wrapper,program,{3:scope},state,1,1)
        self.refused(N.packets,{**wrapper,"Mechanism":"selected-command-native-packets"},program,{3:scope},state,1,1)
        for key,value in (("MessageFrom",4),("MessageTo",3),("FrameCandidates",True),("Frames",frames[:1])):
            self.refused(N.packets,wrapper,{**program,key:value},{3:scope},state,1,1)
        changed=copy.deepcopy(program);changed["Packets"][0]["SHA256"]=list(bytes(32))
        self.refused(N.packets,wrapper,changed,{3:scope},state,1,1)
        self.refused(N.packets,wrapper,program,{3:scope},{**state,"incarnation":bytes([8])*16},1,1)

    def test_literal_never_claims_delay_or_retained_packet(self):
        wrapper=dict(Mechanism="literal",AfterPredecessor=None)
        program=dict(Spec=dict(Message="literal"),Frames=None,Packets=None,FrameCandidates=0,TickResponse=None)
        N.packets(wrapper,program,{},None,1,1)
        self.refused(N.packets,wrapper,{**program,"TickResponse":base64.b64encode(b"inert").decode()},{},None,1,1)


if __name__=="__main__":unittest.main()
