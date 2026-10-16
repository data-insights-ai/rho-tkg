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


def sealed(body): return body+hashlib.sha256(body).digest()


def field(body): return struct.pack(">I", len(body))+body


def pooled_sample():
    u = lambda n: struct.pack(">Q", n)
    source = C.P.GRAPH+u(3)+b"g"*16+u(1)+u(1)+b"d"*32+C.POOLED_SEMANTIC
    target = source[:16]+u(8)+source[24:]
    publication = sealed(b"RPB\1"+C.P.GRAPH+struct.pack(">5Q",1,1,32,3,1)+b"c"*32+u(3)+u(2))
    intent = sealed(b"GMI\3"+b"r"*16+source+target+field(publication)+struct.pack(">QIII3Q",1,128,8,16384,1<<20,1<<20,1<<20))
    command = sealed(b"GMQ\3"+source+b"\1"+u(0)+field(intent)+field(b"")+struct.pack(">I",0))
    return command, C.pooled_command(command)


def native_response(aux, root, *, incarnation=b"i"*16, stats=None, ready=1, sequence=1, applied=1):
    u = lambda n: struct.pack(">Q", n)
    counters = bytes(28*8) if stats is None else b"".join(u(n) for n in stats)
    body = (incarnation+counters+bytes(64)+u(1)+bytes(4)+field(aux)+bytes([ready])+u(sequence)+u(applied)+bytes(3)+field(b"")
            +bytes(8)+field(root)+field(b"")+field(b"")+b"\0"+u(0)+bytes(4)+field(b""))
    return encoded(body)


def catchup_sample():
    # Inert exact process/root codec vectors, not an accepted native run. The
    # full checker authenticates Arm/Batch and the actual selected command first.
    image = sealed(b"GR\3\5"+C.P.GRAPH+struct.pack(">Q",3)+bytes(112))
    stats = [0]*28; stats[1],stats[2],stats[3],stats[4] = 7,3,1,7
    def state(member, sequence):
        return native_response(b"",image,incarnation=bytes([member])*16,stats=stats,ready=0,sequence=sequence,applied=7)
    instance = lambda member: f"voter{member}/pid{100+member}/"+(bytes([member])*16).hex()
    command,_ = pooled_sample()
    a = dict(Recipe=dict(Family="move",Partition=3),Arm={"native":"arm"},
             Batch=dict(Control=dict(Kind=1),Events=[dict(Index=99)]),Command=encoded(command))
    proof = dict(Version=1,Actor=1,Arm=a["Arm"],Batch=a["Batch"],Command=a["Command"],ActorState=state(1,10),
                 Voters=[dict(Member=m,PID=100+m,Instance=instance(m),State=state(m,11)) for m in (1,2,3)])
    restored = dict(Member=3,Instance=instance(3),PreviousInstance="old-voter3",Volume="owned/3",RecoveryRaw=state(3,12))
    raw = dict(Member=3,PID=103,Instance=instance(3),PreviousInstance="old-voter3",Volume="owned/3",Ready=True,
               ReadyKind="native-committed-prefix-and-applied-checkpoints",RecoveryRaw=restored["RecoveryRaw"],
               LocallyRestoredRaw=state(3,1),Catchup=proof)
    restored["Raw"] = encoded(json.dumps(raw,separators=(",",":")).encode())
    return a,restored,raw


class NativeProtocolStateGuard(unittest.TestCase):
    def test_native_move_catchup_is_not_a_config_certificate_or_selected_commit(self):
        a,restored,raw = catchup_sample()
        self.assertTrue(C.replacement_catchup(a,restored))  # Ready index99 need not survive committed prefix7.
        for change in (
            lambda a,r,p:p.update(ReadyKind="invented"),
            lambda a,r,p:p["Catchup"]["Voters"].pop(),
            lambda a,r,p:p["Catchup"]["Voters"][0].update(Member=True),
            lambda a,r,p:p["Catchup"].update(Actor=2),
            lambda a,r,p:p["Catchup"].update(Leader=1),
            lambda a,r,p:p["Catchup"].update(Command=encoded(b"foreign-original")),
            lambda a,r,p:p["Catchup"].update(Batch={**a["Batch"],"Events":[dict(Index=8)]}),
            lambda a,r,p:p["Catchup"].update(Arm={"foreign":"owner"}),
            lambda a,r,p:p["Catchup"]["Voters"][2].update(Instance="retired-incarnation"),
            lambda a,r,p:p["Catchup"].update(ActorState=native_response(b"",C.R.response(p["Catchup"]["ActorState"])["image"],ready=1)),
            lambda a,r,p:p["Catchup"]["Voters"][0].update(State=p["Catchup"]["ActorState"]),
            lambda a,r,p:a["Recipe"].update(Family="pin"),
            lambda a,r,p:p.update(PID=999),
        ):
            aa,rr,pp = copy.deepcopy((a,restored,raw));change(aa,rr,pp)
            rr["Raw"] = encoded(json.dumps(pp,separators=(",",":")).encode())
            with self.assertRaises(ValueError): C.replacement_catchup(aa,rr)
        aa,rr,pp = copy.deepcopy((a,restored,raw))
        aa["Batch"]["Control"]["Kind"] = pp["Catchup"]["Batch"]["Control"]["Kind"] = 2
        rr["Raw"] = encoded(json.dumps(pp,separators=(",",":")).encode())
        with self.assertRaises(ValueError): C.replacement_catchup(aa,rr)  # Application selection is already committed.
        ordinary = copy.deepcopy(raw); ordinary.pop("Catchup"); ordinary["ReadyKind"] = "restored-and-caught-up-under-leader-read-index"
        restored["Raw"] = encoded(json.dumps(ordinary,separators=(",",":")).encode())
        with self.assertRaises(ValueError): C.replacement_catchup(a,restored)  # ready0 is never an old config certificate.
        previous = C.R.response(ordinary["RecoveryRaw"])
        ordinary["RecoveryRaw"] = native_response(b"",previous["image"],incarnation=previous["incarnation"],stats=previous["stats"],ready=1,sequence=12,applied=7)
        restored["RecoveryRaw"] = ordinary["RecoveryRaw"]
        restored["Raw"] = encoded(json.dumps(ordinary,separators=(",",":")).encode())
        self.assertFalse(C.replacement_catchup(a,restored))

    def test_explicit_pooled_profile_stream_census_cannot_upgrade_historical_artifact(self):
        header = {name: None for name in C.HEADER}
        header.update(Version=2, Profile=C.POOLED_PROFILE, Through=[0,0],
                      Pooled=dict(Version=1, Semantic=list(C.POOLED_SEMANTIC), Frames=None))
        C.state_profile(header)
        for mutate in (lambda v:v.update(Version=1), lambda v:v.update(Profile=C.PROFILE), lambda v:v.pop("Pooled")):
            bad = copy.deepcopy(header); mutate(bad)
            with self.assertRaises(ValueError): C.state_profile(bad)
        first = (json.dumps(dict(Type="header",Artifact=header),separators=(",",":"))+"\n").encode()
        footer = dict(Type="footer",Rows=0,Calls=0,GC=0,Pooled=0,Through=[0,0],PayloadSHA256=hashlib.sha256(first).hexdigest())
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"state.jsonl"
            last=lambda v:(json.dumps(v,separators=(",",":"))+"\n").encode()
            path.write_bytes(first+last(footer)); self.assertEqual(C.load(path)["Pooled"]["Frames"],[])
            for bad in (dict(footer,Pooled=True),dict(footer,Pooled=1),{k:v for k,v in footer.items() if k!="Pooled"}):
                path.write_bytes(first+last(bad))
                with self.assertRaises(ValueError): C.load(path)

    def test_owning_pooled_command_and_first_describe_refusal_identity(self):
        command, parsed = pooled_sample()
        self.assertEqual((parsed["phase"],parsed["sequence"]),(1,0))
        def status(failed_sequence=0):
            return sealed(b"GMO\3"+C.P.GRAPH+struct.pack(">Q",3)+parsed["intent"]["move"]+parsed["intent"]["digest"]
                          +bytes((2,5))+struct.pack(">6Q",11,1,0,0,0,0)+bytes(32)+b"\3"+struct.pack(">Q",failed_sequence)+hashlib.sha256(command).digest())
        actual=C.move_status(status(),C.POOLED_SEMANTIC)
        self.assertEqual((actual["phase"],actual["sequence"],actual["failedPhase"],actual["failedSequence"]),(2,0,3,0))
        for wire in (status(1),status()[:-1],sealed(b"GMO\2"+status()[4:-32])):
            with self.assertRaises(ValueError): C.move_status(wire,C.POOLED_SEMANTIC)
        with self.assertRaises(ValueError): C.move_status(status())
        with self.assertRaises(ValueError): C.pooled_command(sealed(b"GMQ\2"+command[4:-32]))

    def test_actual_submit_reply_nonce_scope_and_unknown_stay_distinct(self):
        wire,command=pooled_sample();u=lambda n:struct.pack(">Q",n);nonce=b"n"*16
        scope=sealed(b"APS\1"+command["scope"])
        proposal=sealed(b"GSC\1"+field(scope)+field(wire))
        request=sealed(b"GSQ\1"+b"\10"+u(1)+nonce+bytes(40)+struct.pack(">II",1<<20,0)+field(b"")*3+field(proposal)+bytes(17))
        result=sealed(b"GMO\3"+C.P.GRAPH+u(3)+command["intent"]["move"]+command["intent"]["digest"]
                      +bytes((1,1))+struct.pack(">6Q",11,1,0,0,0,0)+bytes(32)+b"\0"+u(0)+bytes(32))
        def reply(nonce_=nonce,mask=0,body=result):
            return encoded(sealed(b"GSR\1"+field(scope)+u(1)+b"i"*16+b"\10"+u(1)+nonce_+struct.pack(">H",mask)+field(body)))
        item=dict(Origin=2,Partition=3,Request=encoded(request),Reply=reply(),Before=None,After=None)
        self.assertEqual(C.pool_exchange(item)[2],result)
        self.assertEqual(C.pool_exchange(dict(item,Reply=None))[2],b"") # Not an installed-result verdict.
        for bad in (dict(item,Reply=reply(b"x"*16)),dict(item,Reply=reply(mask=1)),dict(item,Partition=8)):
            with self.assertRaises(ValueError): C.pool_exchange(bad)

    def test_actual_native_control_missing_and_exact_undo_full_bound(self):
        _, command = pooled_sample(); key=b"actual\0key"
        missing=dict(Found=False,Key=encoded(key),Index=0,Value=None)
        self.assertIsNone(C.pool_control(missing,key,5))
        for bad in (dict(missing,Found=True),dict(missing,Index=1),dict(missing,Value=encoded(b"invented"))):
            with self.assertRaises(ValueError): C.pool_control(bad,key,5)
        rows=[(b"ga\0k",b"old",0),(b"ga\0z",b"",1)]
        ns=command["scope"][:24]; move=command["intent"]["move"]
        keys=[b"ga"+bytes((tag,))+ns+move for tag in (19,18)]
        vk=b"GMV\1"+ns+move+struct.pack(">I",0x40000000)
        cost=lambda k,n:len(k)+k.count(0)+47+n
        bound=135+172+231+sum(cost(k,len(v)) for k,v,_ in rows)+cost(keys[0],32768)+cost(keys[1],231)+cost(vk,263)+3069+2*len(vk)
        def undo(values=rows,total=bound):
            return sealed(b"GMU\2"+struct.pack(">QI",total,len(values))+b"".join(field(k)+field(v)+bytes((d,)) for k,v,d in values))
        self.assertEqual(C.pool_undo_bound(undo(),command),bound)
        self.assertEqual(C.pool_undo_bound(undo(rows[::-1]),command),bound)
        for wire in (undo(total=bound-1),undo([rows[0],rows[0]]),undo([(b"x",b"nonempty tombstone",1)]),undo()[:-1]):
            with self.assertRaises(ValueError): C.pool_undo_bound(wire,command)

    def test_capture_and_compact_progress_keep_native_generation_and_placement_distinct(self):
        _,command=pooled_sample();u=lambda n:struct.pack(">Q",n);i=command["intent"]
        root=lambda p:sealed(b"GR\3\5"+C.P.GRAPH+u(p)+bytes(112))
        capture=sealed(b"GMC\1"+command["scope"]+u(1)+u(1)+u(2)+hashlib.sha256(root(3)).digest()+field(root(3)))
        self.assertEqual(C.pool_progress(capture,command)["debt"],0)
        def progress(generation=0):
            configuration=i["publication"][56:88]
            ownership=lambda p,e:sealed(b"ROF\1"+C.P.GRAPH+u(i["range"])+u(p)+u(e)+configuration+b"\0")
            captured=lambda scope,p:scope[:40]+scope[88:]+u(generation)+u(2)+root(p)+hashlib.sha256(root(p)).digest()
            descriptor=sealed(b"RMD\1"+field(sealed(b"RPB\1"+i["publication"]))+field(ownership(3,1))+field(ownership(8,2))
                              +captured(i["source"],3)+captured(i["target"],8)+b"d"*32+configuration+field(b"inert-codec-axis")+u(0)+u(0)+b"h"*32+b"\1")
            graph_cursor=sealed(b"RMC\1"+b"b"*32+b"\1"+bytes(8)+b"\0"+bytes(320+48)+b"h"*32)
            empty_replay=sealed(b"GEC\1"+bytes(82))
            reference=b"GMH\1"+C.P.GRAPH+u(3)+i["move"]+bytes(8)
            return sealed(b"GMP\6"+field(descriptor)+field(graph_cursor)+field(b"")+field(empty_replay)+field(empty_replay)
                          +bytes(24)+field(reference)+b"h"*32+b"p"*32+u(10)+u(1)+bytes(24)+b"\0"+field(b""))
        self.assertEqual(C.pool_progress(progress(),command)["debt"],0)
        bad_publication = bytearray(i["publication"]);bad_publication[56] ^= 1
        changed = {**command,"intent":{**i,"publication":bytes(bad_publication)}}
        with self.assertRaises(ValueError): C.pool_progress(progress(),changed)
        with self.assertRaises(ValueError):C.pool_progress(progress(1),command)
        for wire in (capture[:-1],progress()[:-1],sealed(b"GMP\5"+progress()[4:-32])):
            with self.assertRaises(ValueError):C.pool_progress(wire,command)

    def test_proof_observation_is_actual_same_scope_owner_and_command_not_status_summary(self):
        wire, command=pooled_sample();root=sealed(b"GR\3\5"+C.P.GRAPH+struct.pack(">Q",3)+bytes(112))
        run=[1]*16;cookie=[2]*16
        q=dict(Version=1,Cookie=cookie,Sequence=1,Action="move-proof",Request=[0]*16,
               Original=encoded(wire),Token=None,Boundary=None,Digest=[0]*32)
        missing=lambda k:dict(Found=False,Key=encoded(k),Index=0,Value=None)
        ns=C.P.GRAPH+struct.pack(">Q",3);move=command["intent"]["move"]
        proof=dict(Version=1,CommandSHA256=list(hashlib.sha256(wire).digest()),Scope=encoded(command["scope"]),RootIndex=1,
                   Root=list(root),Frame=None,PreviousRoot=[0]*172,IntentControl=missing(b"GMI\1"+ns+move),
                   OriginalControl=missing(b"GMV\1"+ns+move+bytes(4)),DecisionControl=missing(b"GMD\1"+ns+move),StatusFound=False,Status=None,
                   ProgressFound=False,Progress=None,UndoIndices=[0],Undo=[missing(b"GMU\1"+ns+move+bytes(8))],Rows=1,Bytes=1,Output=1,Complete=True)
        reply=dict(Version=1,Profile=C.PROFILE,Cookie=cookie,Run=run,Sequence=1,Calls=1,TerminalCalls=0,
                   GrantedWork=4*(64<<20)+2*(2<<20),Member=1,PID=99,DeadlineUnixNanos=100,Action="move-proof",Result="applied",
                   Original=None,Token=None,Page=None,Digest=[0]*32,Snapshot=None,Completed=None)
        def item(p):
            page=json.dumps(p,separators=(",",":")).encode()
            r=dict(reply,Page=encoded(page),Digest=list(hashlib.sha256(page).digest()))
            return dict(Member=1,Input=q,Response=native_response(json.dumps(r,separators=(",",":")).encode(),root))
        a=dict(Run=run,DeadlineUnixNanos=100)
        C.pool_observation(item(proof),wire,command,a)
        for mutate in (lambda p:p.update(Complete=False),lambda p:p.update(CommandSHA256=[3]*32),
                       lambda p:p.update(Frame={}),lambda p:p.update(UndoIndices=[1]),lambda p:p.update(StatusFound=True)):
            bad=copy.deepcopy(proof);mutate(bad)
            with self.assertRaises((ValueError,KeyError)): C.pool_observation(item(bad),wire,command,a)

    def test_live_debt_prefix_survives_retained_consumed_original_and_same_replay(self):
        _, command=pooled_sample();command=dict(command,scope=command["intent"]["target"],phase=4,sequence=1)
        ns=command["scope"][:24];move=command["intent"]["move"]
        pk,sk=[b"ga"+bytes((tag,))+ns+move for tag in (19,18)];vk=b"GMV\1"+ns+move+struct.pack(">I",0x40000000)
        cost=lambda k,n:len(k)+k.count(0)+47+n
        bound=135+172+231+cost(pk,32768)+cost(sk,231)+cost(vk,263)+3069+2*len(vk)
        undo=sealed(b"GMU\2"+struct.pack(">QI",bound,0));inventory={1:(11,undo)}
        def frame(phase,index):
            out=sealed(b"GMO\3"+C.P.GRAPH+struct.pack(">Q",8)+move+command["intent"]["digest"]+bytes((phase,1))
                       +struct.pack(">6Q",index,1,0,1,0,0)+bytes(32)+b"\0"+struct.pack(">Q",0)+bytes(32))
            return dict(Index=index,Outcome=encoded(out))
        before=dict(frame=None,progress=dict(total=0,next=0,debt=0),proof=dict(RootIndex=10,Progress=None))
        after=dict(frame=frame(4,11),progress=dict(total=1,next=0,debt=bound),proof=dict(RootIndex=11,Progress="actual-import"))
        C.pool_transition(command,before,after,inventory)
        bad=copy.deepcopy(after);bad["progress"]["debt"]-=1
        with self.assertRaises(ValueError):C.pool_transition(command,before,bad,inventory)
        prior=dict(after,frame=None,proof=dict(RootIndex=12,Progress="actual-import"))
        undone=dict(frame=frame(9,13),progress=dict(total=1,next=1,debt=0),proof=dict(RootIndex=13,Progress="actual-undo"))
        abort=dict(command,phase=9,sequence=1)
        C.pool_transition(abort,prior,undone,inventory)
        self.assertEqual(inventory[1],(11,undo)) # Reverse install retires debt, never claims physical GMU deletion.
        C.pool_transition(abort,undone,copy.deepcopy(undone),inventory)
        changed=copy.deepcopy(undone);changed["proof"]["Progress"]="refilled"
        with self.assertRaises(ValueError):C.pool_transition(abort,undone,changed,inventory)
        bad=copy.deepcopy(undone);bad["progress"]["next"]=2
        with self.assertRaises(ValueError):C.pool_transition(abort,prior,bad,inventory)

    def test_inspect_is_retired_work_not_an_operation_sequence_or_free_restart(self):
        run,cookie=[1]*16,[2]*16;grant=4*(64<<20)+2*(2<<20)
        q=dict(Version=1,Cookie=[0]*16,Sequence=0,Action="inspect",Request=[0]*16,
               Original=None,Token=None,Boundary=None,Digest=[0]*32)
        reply=dict(Version=1,Profile=C.PROFILE,Cookie=cookie,Run=run,Sequence=0,Calls=1,TerminalCalls=0,
                   GrantedWork=grant,Member=2,PID=99,DeadlineUnixNanos=100,Action="inspect",Result="applied",
                   Original=None,Token=None,Page=None,Digest=[0]*32,Snapshot=None,Completed=None)
        def call(question,r):return dict(Member=2,Input=question,Response=native_response(json.dumps(r,separators=(",",":")).encode(),b""))
        first=call(q,reply)
        next_q=dict(q,Action="begin",Sequence=1,Cookie=cookie,Request=[3]*16)
        second=call(next_q,dict(reply,Action="begin",Sequence=1,Calls=2,GrantedWork=2*grant))
        a=dict(Recipe=dict(Family="allocation"),Run=run,DeadlineUnixNanos=100,Calls=[first,second],
               Pooled=dict(Version=1,Semantic=list(C.POOLED_SEMANTIC),Frames=[]))
        C.pooled(a,{})
        C.pooled(dict(a,Calls=[first,second,second]),{}) # Exact cached control replay spends no second grant.
        for calls in ([second],[first,call(next_q,dict(reply,Action="begin",Sequence=1,Calls=2,GrantedWork=grant))]):
            with self.assertRaises(ValueError): C.pooled(dict(a,Calls=calls),{})

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
        header = {name: None for name in C.HEADER}; header.update(Version=1,Profile=C.PROFILE,Through=[0,0])
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
