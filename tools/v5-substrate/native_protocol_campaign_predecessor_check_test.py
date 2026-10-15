#!/usr/bin/env python3
"""Inert codec/admission guards; only the full parent supplies native evidence."""
import base64
import copy
import hashlib
import struct
import unittest

import native_protocol_campaign_predecessor_check as C


def u64(value):
    return struct.pack(">Q", value)


def field(value):
    return struct.pack(">I", len(value))+value


def seal(magic, value):
    wire = magic+value
    return wire+hashlib.sha256(wire).digest()


def process(aux=b"", *, sequence=9, packets=(), handles=(), image=b""):
    # Exact process response shape, explicitly NOT a real process observation.
    wire = bytes([7])*16+bytes(28*8+32+32)+u64(0)+struct.pack(">I", len(handles))
    wire += b"".join(u64(value) for value in handles)
    wire += field(aux)+b"\1"+u64(sequence)+u64(0)+bytes(3)+field(b"")
    wire += struct.pack(">I", len(packets))
    for sender, receiver, payload in packets:
        wire += u64(sender)+u64(receiver)+b"\0"+field(payload)
    wire += struct.pack(">I", 0)+field(image)+field(b"")*2+b"\0"+u64(0)+struct.pack(">I", 0)+field(b"")
    return wire


def cache_fixture():
    scope, tx, nonce, issuer = bytes(range(120)), bytes([2])*16, bytes([3])*16, bytes([4])*16
    manifest = b"inert-manifest-codec-fixture"
    status = seal(b"GCS\1", C.P.GRAPH+u64(3)+tx+hashlib.sha256(manifest).digest()+b"\2"+u64(11)+u64(5)+u64(0)+b"\1\0")
    request = seal(b"GSQ\1", b"\6"+u64(8)+nonce+bytes(48)+field(b"")*4+tx+b"\2")
    authority = seal(b"GSE\1", field(seal(b"APS\1", scope))+issuer+u64(6)+tx+nonce+b"\2"+u64(13)+field(status)+field(manifest))
    reply = seal(b"GSR\1", field(seal(b"APS\1", scope))+u64(1)+issuer+b"\6"+u64(8)+nonce+bytes(2)+field(authority))
    return request, reply, process(reply), {3: scope}, 3, 1, {"manifest":manifest, "proof_wires":[status]}, {(3,11):(status,b"")}


class PredecessorChecks(unittest.TestCase):
    def refused(self, fn, *args):
        with self.assertRaises(ValueError):
            fn(*args)

    def test_exact_twenty_ready_recipes_and_honest_120_mechanisms(self):
        recipes = []
        for side in ("before", "after"):
            recipes.append(dict(Family="p02-local-fence", Fence=dict(Schedule="persisted-fence-before-application", Phase=0, Partition=3, Kind=1, Side=side)))
            for co in (3,8):
                for commit in (False,True):
                    for phase in ((2,3) if co==8 else (2,)):
                        recipes.append(dict(Family="p06-first-live", Live=dict(Coordinator=co, Commit=commit, Phase=phase, Kind=1, Side=side)))
                recipes.append(dict(Family="p03-recovery", Coordinator=dict(Coordinator=co, Phase=4, Partition=co, Kind=1, Side=side)))
            recipes.append(dict(Family="partial-recovery", Partial=dict(Phase=4, Partition=8, Kind=1, Side=side)))
        self.assertEqual(len(recipes),20)
        cells = set()
        for recipe in recipes:
            for layout in ("original","rotated"):
                for mode in ("delay","reorder","duplicate"):
                    spec = dict(Version=1, Recipe=recipe, Layout=layout, Mode=mode, Output="/inert/predecessor.json")
                    family, _, name, mechanism = C.spec(spec)
                    cells.add((name,layout,mode))
                    self.assertEqual(mechanism, "emitted-predecessor-tick-packets" if family=="p02-local-fence" or mode=="reorder" else "selected-service-request-delay" if mode=="delay" else "completed-predecessor-service-cache-duplicate")
                    for bad in ({**spec,"Version":True}, {**spec,"Mode":"selected-submit-duplicate"}, {**spec,"Layout":"nominal"}, {**spec,"Output":"predecessor.json"}):
                        self.refused(C.spec,bad)
        self.assertEqual(len(cells),120)
        for bad in ({}, {**recipes[0],"Live":recipes[1].get("Live",{})}, dict(Family="p03-recovery", Coordinator=dict(Coordinator=3, Phase=2, Partition=3, Kind=1, Side="before")), dict(Family="partial-recovery", Partial=dict(Phase=4, Partition=8, Kind=2, Side="before"))):
            self.refused(C.recipe,bad)

    def test_complete_exact_action_order_not_nominal_labels(self):
        actions = ["causal-predecessor-service-reply-complete", "same-service-cache-retransmit", "byte-identical-cache-reply-received", "selected-service-request-observed"]
        rows = [dict(Ordinal=i,Action=action) for i,action in enumerate(actions,1)]
        C.chronology(rows,actions)
        for bad in (rows[:-1],list(reversed(rows)),[{**rows[0],"Ordinal":True}]+rows[1:],rows+[rows[-1]]):
            self.refused(C.chronology,bad,actions)

    def test_exact_cached_predecessor_codec_then_changed_identity_and_original(self):
        args = cache_fixture()
        first, duplicate, status = C.cached_reply(*args)
        self.assertEqual((first["sequence"], duplicate["sequence"], status["index"]),(8,9,11))
        for field_index, value in ((0,args[0][:-1]+b"x"),(1,args[1][:-1]+b"x"),(2,process(b"different-reply")),(4,8),(5,2),(6,{"manifest":b"different", "proof_wires":args[6]["proof_wires"]}),(7,{(3,11):(args[7][3,11][0],b"phantom-CDC")}),(7,{})):
            bad=list(args);bad[field_index]=value
            self.refused(C.cached_reply,*bad)
        wrong_proof=list(args);wrong_proof[6]={**args[6],"proof_wires":[]}
        self.refused(C.cached_reply,*wrong_proof)
        # Keep the envelope and its duplicate mutually identical and correctly
        # sealed: only the actual GSE issuer join can reject this mutation.
        body=C.P.body(args[1],b"GSR\1")
        changed=seal(b"GSR\1",body[:168]+bytes([body[168]^1])+body[169:])
        wrong_issuer=list(args);wrong_issuer[1]=changed;wrong_issuer[2]=process(changed)
        self.refused(C.cached_reply,*wrong_issuer)

    def test_real_tick_payload_shape_and_exact_existing_scheduler_hash(self):
        scope = bytes(range(120))
        packets=[]
        for to in (2,3):
            message=bytes([16,to,24,1,32,2])
            payload=b"RP\1\0"+scope[:40]+scope[88:120]+struct.pack(">I",len(message))+message
            packets.append((1,to,payload))
        event=dict(Action="reorder", From=1, To=2, Ordinal=1, Bytes=len(packets[0][2]), SHA256=list(hashlib.sha256(packets[0][2]).digest()), ConfiguredDelayMillis=0, ObservedDelayNanos=0)
        evidence=dict(TickResponse=base64.b64encode(process(sequence=9,packets=packets)).decode(), AfterState=base64.b64encode(process(sequence=10,handles=(0,0),image=bytes(172))).decode(), Spec=dict(Mode="reorder"), Voter=1, Packets=[event])
        state=dict(incarnation=bytes([7])*16, sequence=8, stats=[0,0,2])
        tick, after=C.tick_program(evidence,{3:scope},state)
        self.assertEqual((len(tick["packets"]),after["sequence"]),(2,10))
        for key,value in (("From",4),("To",3),("Ordinal",True),("SHA256",list(bytes(32))),("Action","duplicate")):
            bad=copy.deepcopy(evidence);bad["Packets"][0][key]=value
            self.refused(C.tick_program,bad,{3:scope},state)
        self.refused(C.tick_program,evidence,{3:scope},{**state,"incarnation":bytes([8])*16})
        bad={**evidence,"TickResponse":base64.b64encode(process(sequence=9,packets=packets[:1])).decode()}
        self.refused(C.tick_program,bad,{3:scope},state)
        # State's two fields are worker/relay counts, not SnapshotSend handles.
        bounded={**evidence,"AfterState":base64.b64encode(process(sequence=10,handles=(5,16),image=bytes(172))).decode()}
        C.tick_program(bounded,{3:scope},state)
        for handles,image in (((),bytes(172)),((1,),bytes(172)),((6,0),bytes(172)),((0,17),bytes(172)),((0,0),bytes(171))):
            bad={**evidence,"AfterState":base64.b64encode(process(sequence=10,handles=handles,image=image)).decode()}
            self.refused(C.tick_program,bad,{3:scope},state)
        for key,wire in (("AfterState",process(sequence=10,handles=(0,0),image=bytes(172),packets=packets)),("AfterState",process(b"unexpected offers",sequence=10,handles=(0,0),image=bytes(172))),("TickResponse",process(sequence=9,packets=packets,handles=(1,)))):
            self.refused(C.tick_program,{**evidence,key:base64.b64encode(wire).decode()},{3:scope},state)


if __name__ == "__main__":
    unittest.main()
