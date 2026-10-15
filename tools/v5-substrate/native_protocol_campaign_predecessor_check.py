#!/usr/bin/env python3
"""Exact Ready-loss companions; source/packet observations grant no authority.

The complete unchanged program/semantic/original/CDC/old-cut oracle runs first.
Only then are the separately admitted service/predecessor observations joined.
Selected-command pre-Ready Raft emission is deliberately never asserted.
"""
import argparse
import base64
import hashlib
import json
from pathlib import Path

import native_protocol_campaign_program_check as K

H, P, F = K.H, K.P, K.F
require, integer, fixed = K.require, K.integer, K.fixed
PROFILE = "native-protocol-predecessor-composition-v1"
FIELDS = set("Version Profile Spec BaseDirectory BasePath Mechanism DeadlineUnixNanos Run Candidates WorkGrant OutputGrant RetainedBytes Actor Partition Voter CommandSHA256 SelectedRequest PredecessorRequest PredecessorReply DuplicateResponse TickResponse AfterState Packets ConfiguredDelayMillis ObservedDelayNanos Events".split())
SPEC = {"Version", "Recipe", "Layout", "Mode", "Output"}
FAMILIES = {"p02-local-fence":"Fence", "p06-first-live":"Live", "p03-recovery":"Coordinator", "partial-recovery":"Partial"}


def recipe(row):
    require(type(row) is dict and row.get("Family") in FAMILIES and set(row)=={"Family",FAMILIES[row["Family"]]}, "exact one real local Ready recipe")
    family, r = row["Family"], row[FAMILIES[row["Family"]]]
    if family=="p02-local-fence":
        name=H.fence_recipe(r)
        require(r["Schedule"]=="persisted-fence-before-application" and r["Phase"]==0 and r["Partition"]==3,"actual direct local Fence")
    elif family=="p06-first-live":name=H.live_recipe(r)
    elif family=="p03-recovery":
        name=H.coordinator_recipe(r)
        require(r["Phase"]==4 and r["Partition"]==r["Coordinator"],"actual recovering coordinator-local Resolve")
    else:
        name=H.partial_recipe(r)
        require(r["Phase"]==4 and r["Partition"]==8,"actual partial B recovery Resolve")
    require(type(r["Kind"]) is int and r["Kind"]==1,"Ready only; install already has real emitted message programs")
    return family,r,name


def spec(value):
    require(type(value) is dict and set(value)==SPEC and type(value["Version"]) is int and value["Version"]==1 and value["Layout"] in ("original","rotated") and value["Mode"] in ("delay","reorder","duplicate"),"exact finite companion admission")
    family,r,name=recipe(value["Recipe"])
    require(type(value["Output"]) is str and Path(value["Output"]).is_absolute() and Path(value["Output"]).name=="predecessor.json","owned explicit diagnostic output")
    mechanism="emitted-predecessor-tick-packets" if family=="p02-local-fence" or value["Mode"]=="reorder" else "selected-service-request-delay" if value["Mode"]=="delay" else "completed-predecessor-service-cache-duplicate"
    return family,r,name,mechanism


def chronology(rows,actions):
    require(type(rows) is list and len(rows)==len(actions),"complete exact companion action census")
    for ordinal,(row,action) in enumerate(zip(rows,actions),1):
        require(type(row) is dict and set(row)=={"Ordinal","Action"} and type(row["Ordinal"]) is int and row["Ordinal"]==ordinal and row["Action"]==action,"actual bounded predecessor/forward/Ready order")


def read_question(wire):
    q=P.Cursor(P.body(wire,b"GSQ\1"))
    kind,sequence,nonce=q.number(1),q.number(),q.take(16)
    require(q.take(48)==bytes(48) and all(q.field()==b"" for _ in range(4)),"ordinary exact predecessor CrossRead, no alternate command")
    tx,phase=q.take(16),q.number(1);q.end()
    require(kind==6 and sequence>0 and any(nonce) and any(tx) and phase in (2,3),"genuine correlated predecessor vote/decision read")
    return kind,sequence,nonce,tx,phase


def cached_reply(request,reply,duplicate,scopes,partition,voter,selected,originals):
    kind,sequence,nonce,tx,phase=read_question(request)
    first=H.source_reply(reply)
    require(partition in scopes and first["kind"]==kind and first["sequence"]==sequence and first["nonce"]==nonce and first["scope"]==scopes[partition] and first["voter"]==voter,"configured predecessor endpoint, scope and complete correlation")
    g=P.Cursor(P.body(first["body"],b"GSE\1"))
    scope=P.body(g.field(156),b"APS\1")
    issuer,read_sequence=g.take(16),g.number()
    actual_tx,actual_nonce,actual_phase=g.take(16),g.take(16),g.number(1)
    index,status_wire,manifest=g.number(),g.field(256),g.field();g.end()
    status=P.status(status_wire)
    require(scope==first["scope"] and issuer==first["issuer"] and read_sequence>0 and (actual_tx,actual_nonce,actual_phase)==(tx,nonce,phase),"actual GSE native read scope/session/question")
    require(manifest==selected["manifest"] and hashlib.sha256(manifest).digest()==status["digest"] and status["partition"]==partition and status["tx"]==tx and status["phase"]==phase and 0<status["index"]<=index and status_wire in selected["proof_wires"],"actual causal predecessor is an exact selected native proof")
    require(originals.get((partition,status["index"]))==(status_wire,b""),"genuine retained predecessor original, no effect/CDC invented")
    actual=F.process(duplicate)
    require(actual["code"]==actual["mask"]==0 and actual["aux"]==reply and not actual["packets"] and not actual["reads"] and not actual["handles"],"byte-identical immediate same-service cached reply, no new native read/Submit")
    return first,actual,status


def tick_program(e,scopes,selected_state):
    tick,after=(F.process(P.raw(e[k])) for k in ("TickResponse","AfterState"))
    require(tick["code"]==tick["mask"]==after["code"]==after["mask"]==0 and tick["incarnation"]==after["incarnation"]==selected_state["incarnation"] and selected_state["sequence"]<tick["sequence"]<after["sequence"],"real same configured leader Tick and later completed pipe state")
    require(1<=len(tick["packets"])<=2 and not tick["handles"] and not tick["reads"] and not after["reads"] and not after["packets"],"at most two genuinely emitted predecessor frames")
    # This response is the paired source child's processStateOp, not Offers:
    # handles is [workers+paused, relay.pending]. Its constructor admits two
    # normal workers, one terminal worker, two paused calls and sixteen relays.
    require(len(after["handles"])==2 and after["handles"][0]<=5 and after["handles"][1]<=16 and len(after["image"])==172 and not after["aux"] and not after["outcome"] and not after["changes"],"exact bounded paired worker/relay census and checkpoint, no snapshot/result authority")
    require(e["Spec"]["Mode"]!="reorder" or len(tick["packets"])==2,"reorder needs a real next frame")
    frames=[]
    for row in tick["packets"]:
        # process() returns owned bytes; frame() expects the ordinary JSON form.
        wire,message=K.frame({**row,"Payload":base64.b64encode(row["Payload"]).decode()},scopes)
        require(len(wire)<=64<<10 and row["From"]==e["Voter"] and message.get(4,[0])[0]>=selected_state["stats"][2],"actual predecessor term/leader and finite frame")
        frames.append(wire)
    require(type(e["Packets"]) is list and len(e["Packets"])==1,"one actually matched existing packet scheduler")
    event=e["Packets"][0]
    require(type(event) is dict and set(event)==K.PACKET_EVENT and event["Action"]==e["Spec"]["Mode"] and type(event["From"]) is type(event["To"]) is type(event["Ordinal"]) is type(event["Bytes"]) is int and event["From"]==tick["packets"][0]["From"] and event["To"]==tick["packets"][0]["To"] and event["Ordinal"]==1 and event["Bytes"]==len(frames[0]) and fixed(event["SHA256"],32,True)==hashlib.sha256(frames[0]).digest(),"actual emitted predecessor bytes, not selected-command packet fabrication")
    require(type(event["ConfiguredDelayMillis"]) is int and type(event["ObservedDelayNanos"]) is int and (event["ConfiguredDelayMillis"]==20 and event["ObservedDelayNanos"]>=20_000_000 if e["Spec"]["Mode"]=="delay" else event["ConfiguredDelayMillis"]==event["ObservedDelayNanos"]==0),"actual scheduler delay/order/duplication receipt")
    return tick,after


def check(path):
    with Path(path).open("rb") as source:wire=source.read((1<<20)+1)
    require(0<len(wire)<=1<<20,"bounded complete companion artifact")
    a=H.T.decoded(wire)
    require(type(a) is dict and set(a)=={"Version","Profile","Evidence","BaseSHA256","BaseBytes"} and type(a["Version"]) is int and a["Version"]==1 and a["Profile"]==PROFILE,"explicit separate companion wrapper")
    e=a["Evidence"]
    require(type(e) is dict and set(e)==FIELDS and type(e["Version"]) is int and e["Version"]==1 and e["Profile"]==PROFILE,"exact owned companion record")
    family,r,name,mechanism=spec(e["Spec"])
    directory,base=Path(e["BaseDirectory"]),Path(e["BasePath"])
    require(directory.is_absolute() and base.is_absolute() and not base.is_symlink() and base.parent.resolve(strict=True)==directory.resolve(strict=True) and base.name==name+".jsonl","same actual cohort bounded artifact path")
    digest=hashlib.sha256();total=0
    with base.open("rb") as source:
        while part:=source.read(64<<10):
            total+=len(part);require(total<=16<<20,"existing full oracle artifact bound");digest.update(part)
    require(type(a["BaseBytes"]) is int and 0<a["BaseBytes"]==total and fixed(a["BaseSHA256"],32,True)==digest.digest(),"complete unchanged semantic artifact/EOF hash")
    original=H.load(base,program=True)
    complete=K.check(original)  # Full semantic/original/CDC/old-cut oracle FIRST.
    native=original["Evidence"]
    require(original["Case"]==name and native["Recipe"]==r and native["Program"]["Spec"]=={"Version":1,"Layout":e["Spec"]["Layout"],"Message":"literal"},"same real excluded recipe/layout with literal native storage case")
    require(fixed(e["Run"],16,True)==fixed(native["Run"],16,True) and type(e["DeadlineUnixNanos"]) is int and e["DeadlineUnixNanos"]==native["DeadlineUnixNanos"] and e["Mechanism"]==mechanism,"same original run/deadline and honest actual mechanism")
    require(type(e["Candidates"]) is int and 0<=e["Candidates"]<=128 and type(e["WorkGrant"]) is int and e["WorkGrant"]==128*(4096+32*(2<<20))+12*(2<<20)+(16<<20) and type(e["OutputGrant"]) is int and e["OutputGrant"]==1<<20,"full finite pre-admission diagnostic grants; not measured cost")
    require(type(e["Actor"]) is type(e["Partition"]) is type(e["Voter"]) is int and e["Actor"]==native["Actor"] and e["Partition"]==r.get("Partition",8) and e["Voter"]==native["Target"] and fixed(e["CommandSHA256"],32,True)==hashlib.sha256(P.raw(native["SelectedCommand"])).digest(),"actual native command/member/Ready receipt join")
    semantic=native["Completion"]["P06"] if family=="p06-first-live" else H.M.FAULT.lc.normalize(native["P03"]) if family=="p03-recovery" else native["Partial"] if family=="partial-recovery" else native["P02"]
    observation=dict(Cut=semantic["steps"][3]["Cut"]) if family=="p03-recovery" else semantic["Before"] if family in ("p06-first-live","partial-recovery") else semantic["Old"]
    scopes,_=P.cut(observation)
    election=native["Program"]["Elections"][0 if e["Partition"]==3 else 1]
    for successor in native["Program"]["Successors"] or []:
        if (successor["Leader"]<=3)==(e["Partition"]==3):election=successor
    state=F.process(P.raw(election["State"]))
    require(election["Leader"]==e["Voter"] and election["Owner"]["Cookie"]==native["Arm"]["Cookie"],"SAME actual observed current native leader, never nominal role")
    prefix=["actual-elections-and-reconnect-complete"]
    if mechanism!="emitted-predecessor-tick-packets":
        if family=="partial-recovery":prefix.append("actual-recovery-successor-observed")
        prefix.append("service-observer-attached")
        selected,decoded,correlation=H.C.source_request(dict(Origin=e["Actor"],Partition=e["Partition"],Request=e["SelectedRequest"],Reply=None),scopes,e["Actor"])
        require(selected==P.raw(native["SelectedCommand"]) and decoded["phase"]==r["Phase"],"exact actual selected GSQ, no reconstructed Submit")
        require(not P.raw(e["TickResponse"]) and not P.raw(e["AfterState"]) and e["Packets"] is None,"service mechanism cannot claim predecessor Raft packets")
        if mechanism=="selected-service-request-delay":
            chronology(e["Events"],prefix+["selected-service-request-observed","selected-service-delay-begin","selected-service-delay-end","selected-command-native-arm-complete","actual-selected-ready-loss-observed"])
            require(type(e["ConfiguredDelayMillis"]) is int and e["ConfiguredDelayMillis"]==20 and type(e["ObservedDelayNanos"]) is int and e["ObservedDelayNanos"]>=20_000_000 and all(not P.raw(e[k]) for k in ("PredecessorRequest","PredecessorReply","DuplicateResponse")),"actual delay ended before native selected request proceeds")
        else:
            chronology(e["Events"],prefix+["causal-predecessor-service-reply-complete","same-service-cache-retransmit","byte-identical-cache-reply-received","selected-service-request-observed","selected-command-native-arm-complete","actual-selected-ready-loss-observed"])
            rows=native["Records"] if family=="p03-recovery" else semantic["Records"]
            originals={(row["Partition"],row["Index"]):(P.raw(row["Outcome"]),P.raw(row["CDC"])) for row in rows}
            part=3 if family=="p06-first-live" and r["Phase"]==2 else 8 if family in ("p06-first-live","partial-recovery") else r["Coordinator"]
            serving=next(row for row in native["Program"]["Elections"] if (row["Leader"]<=3)==(part==3))
            for row in native["Program"]["Successors"] or []:
                if (row["Leader"]<=3)==(part==3):serving=row
            # A first-live held YES belongs to the INITIAL peer, before its
            # required death; it is not refreshed through the later successor.
            if family=="p06-first-live" and r["Phase"]==2:serving=native["Program"]["Elections"][0]
            first,duplicate,status=cached_reply(P.raw(e["PredecessorRequest"]),P.raw(e["PredecessorReply"]),P.raw(e["DuplicateResponse"]),scopes,part,serving["Leader"],decoded,originals)
            serving_state=F.process(P.raw(serving["State"]))
            require(duplicate["incarnation"]==serving_state["incarnation"] and duplicate["sequence"]>serving_state["sequence"],"same actual serving native process before Ready loss")
            if part==e["Partition"]:
                require(correlation[1]==first["sequence"]+1 and correlation[2]!=first["nonce"],"next actual request in SAME endpoint stream follows immediate cached predecessor")
            require(type(e["ConfiguredDelayMillis"]) is type(e["ObservedDelayNanos"]) is int and e["ConfiguredDelayMillis"]==e["ObservedDelayNanos"]==0,"cache duplicate cannot invent a delay")
    else:
        chronology(e["Events"],prefix+["predecessor-native-tick-returned-emitted-packets","predecessor-packets-delivered-by-native-scheduler","selected-command-native-arm-complete","actual-selected-ready-loss-observed"])
        require(all(not P.raw(e[k]) for k in ("SelectedRequest","PredecessorRequest","PredecessorReply","DuplicateResponse")) and type(e["ConfiguredDelayMillis"]) is type(e["ObservedDelayNanos"]) is int and e["ConfiguredDelayMillis"]==e["ObservedDelayNanos"]==0,"packet predecessor is not a selected service delivery")
        tick_program(e,scopes,state)
    retained=sum(len(P.raw(e[k])) for k in ("SelectedRequest","PredecessorRequest","PredecessorReply","DuplicateResponse","TickResponse","AfterState"))
    require(type(e["RetainedBytes"]) is int and e["RetainedBytes"]==retained and retained<=(384<<10) and (retained<=128<<10 if mechanism!="emitted-predecessor-tick-packets" else True),"one bounded RPC pair or at most two emitted native packet observations")
    return dict(case=name,layout=e["Spec"]["Layout"],mode=e["Spec"]["Mode"],mechanism=mechanism,semantic=complete,scope="exact Ready-loss companion source; runtime/capacity/full D qualification is separate")


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("artifact",type=Path)
    args=parser.parse_args();print(json.dumps(check(args.artifact),sort_keys=True))


if __name__=="__main__":main()
