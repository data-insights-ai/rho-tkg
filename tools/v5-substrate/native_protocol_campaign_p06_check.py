#!/usr/bin/env python3
"""P06 elected-role/message attachment followed by complete original oracles.

No member, scope, source request, native result or semantic value is rewritten.
The optional role context is admitted only after genuine process/native-owner
and election evidence joins; ordinary old checker entry points stay strict.
"""
import argparse
import hashlib
import json
from pathlib import Path

import native_protocol_campaign_program_check as K
import native_protocol_campaign_predecessor_check as R
import native_transition_conjunction_check as C
import native_transition_initial_check as I
import native_transition_first_prepare_check as Q
import native_transition_batch_check as B

P,F,T=K.P,K.F,K.T
require,integer,fixed=K.require,K.integer,K.fixed
PROFILE="native-p06-causal-program-v1"
EXTRA={"Profile","Mechanism","Program","AfterPredecessor","AfterLoss","Cohort"}


def select(profile):
    if profile in (C.PROFILE,C.RECOVERY_PROFILE,C.TERMINAL_PROFILE):return C
    if profile==I.PROFILE:return I
    if profile==Q.PROFILE:return Q
    require(profile==B.PROFILE,"existing implemented P06/initial-batch profile")
    return B


def coordinates(a):
    if a["Profile"]==B.PROFILE:return "initial-batch",a["Phase"],a["Partition"],a["Kind"],a["Side"]
    r=a["Recipe"]
    if a["Profile"]==I.PROFILE:return "register",1,r["Coordinator"],r["Kind"],r["Side"]
    if a["Profile"]==Q.PROFILE:return "first-prepare",2,3,r["Kind"],r["Side"]
    family="terminal" if a["Version"]==3 else "recovery" if a["Version"]==2 else "decide"
    phase,part=(3,r["Coordinator"]) if family=="decide" else (r["Phase"],r["Partition"])
    return family,phase,part,r["Kind"],r["Side"]


def role_vectors(initial,selected,after,family,partition):
    require(family in ("decide","recovery","terminal","register","first-prepare","initial-batch") and type(partition) is int and partition in (3,8),"exact existing semantic family and selected partition")
    roles=C.program_voters(dict(initial=tuple(initial),selected=tuple(selected),after=tuple(after)))
    expected=list(initial)
    if family in ("decide","recovery","terminal"):expected[0]=3-initial[0]
    require(tuple(expected)==roles["selected"],"only the genuine first prepared-A leader loss precedes selection")
    group=0 if partition==3 else 1
    expected[group]=3-expected[group] if group==0 else 10-expected[group]
    require(tuple(expected)==roles["after"],"actual selected-group successor; other group cannot be relabelled")
    return roles


def roster(a,cohort):
    require(type(cohort) is list and len(cohort)==6,"complete actual terminal six-process cohort")
    instances={};volumes={}
    for member,row in enumerate(cohort,1):
        raw=T.decoded(P.raw(row["Raw"]))
        require(type(row["Member"]) is type(raw["Member"]) is int and row["Member"]==raw["Member"]==member and row["Action"]=="stop" and row["Reaped"] is raw["Reaped"] is raw["Success"] is True and row["Signal"]==raw["Signal"]=="clean-exit" and raw["Signaled"] is False and type(raw["ExitCode"]) is int and raw["ExitCode"]==0 and row["Instance"]==raw["Instance"] and row["Volume"]==raw["Volume"],"actual clean wait/identity, not an invented terminal process")
        pid=integer(raw["PID"],minimum=1)
        instances[member,pid]=row["Instance"];volumes[member]=row["Volume"]
    for rows in (a["Processes"],a.get("P06",{}).get("Processes")):
        for row in rows or []:
            raw=T.decoded(P.raw(row["Raw"]))
            member=integer(row["Member"],6,1);pid=integer(raw["PID"],minimum=1)
            require(row["Volume"]==raw["Volume"]==volumes[member] and row["Instance"].startswith(f"voter{member}/pid{pid}/"),"same actual durable volume and native process")
            key=member,pid
            require(key not in instances or instances[key]==row["Instance"],"ambiguous serving process incarnation")
            instances[key]=row["Instance"]
    return instances


def election(row,prior,member,run,deadline,instances):
    require(type(row) is dict and set(row)==K.ELECTION and type(row["Prior"]) is type(row["Leader"]) is int and (row["Prior"],row["Leader"])==(prior,member),"actual configured election identity")
    configuration,state=(F.process(P.raw(row[key])) for key in ("Configuration","State"))
    pid=integer(row["Owner"]["PID"],minimum=1)
    K.idle_owner(row["Owner"],run,deadline,member,pid)
    require(configuration["code"]==configuration["mask"]==state["code"]==state["mask"]==0 and state["ready"]==1 and configuration["sequence"]<state["sequence"] and configuration["incarnation"]==state["incarnation"] and state["stats"][2]>0 and state["stats"][3]==member and instances.get((member,pid),"").endswith("/"+state["incarnation"].hex()),"genuine native election/read-index/owner belongs to actual reaped cohort")
    return state


def packets(wrapper,program,scopes,state,target,kind):
    message=program["Spec"]["Message"]
    frames,events=program["Frames"] or [],program["Packets"] or []
    mechanism="literal" if message=="literal" else "emitted-predecessor-tick-packets" if kind==1 else "selected-command-native-packets"
    require(wrapper["Mechanism"]==mechanism,"actual selected native frames versus already-emitted predecessor")
    if message=="literal":
        require(not frames and not events and type(program["FrameCandidates"]) is int and program["FrameCandidates"]==0 and not P.raw(program["TickResponse"]) and not P.raw(wrapper["AfterPredecessor"]),"literal cannot claim fault traffic")
        return
    require(type(frames) is list and 1<=len(frames)<=2 and type(events) is list and len(events)==1 and integer(program["FrameCandidates"],128,1),"bounded actual native frame/receipt census")
    if kind==1:
        tick,_=R.tick_program(dict(TickResponse=program["TickResponse"],AfterState=wrapper["AfterPredecessor"],Spec=dict(Mode=message),Voter=target,Packets=events),scopes,state)
        require(program["FrameCandidates"]==len(frames)==len(tick["packets"]),"full actual Tick frame set")
        for frame,actual in zip(frames,tick["packets"]):
            wire,_=K.frame(frame,scopes)
            require((frame["From"],frame["To"],frame["Snapshot"],wire)==(actual["From"],actual["To"],actual["Snapshot"],actual["Payload"]),"no invented or omitted emitted predecessor")
    else:
        require(not P.raw(wrapper["AfterPredecessor"]),"only actual Ready predecessor admits post-Tick observation")
        event=events[0]
        require(type(event) is dict and set(event)==K.PACKET_EVENT and event["Action"]==message and all(type(event[key]) is int for key in ("From","To","Ordinal","Bytes","ConfiguredDelayMillis","ObservedDelayNanos")) and event["Ordinal"]==1,"actual existing scheduler action")
        wire,decoded=K.frame(frames[0],scopes)
        require(frames[0]["From"]==event["From"]==target and frames[0]["To"]==event["To"] and integer(event["Bytes"],2<<20,1)==len(wire) and fixed(event["SHA256"],32,True)==hashlib.sha256(wire).digest() and decoded.get(4,[0])[0]>=state["stats"][2],"exact scheduled native packet/term/hash")
        require(type(event["ConfiguredDelayMillis"]) is type(event["ObservedDelayNanos"]) is int and event["ConfiguredDelayMillis"]==(20 if message=="delay" else 0) and (event["ObservedDelayNanos"]>=20_000_000 if message=="delay" else event["ObservedDelayNanos"]==0),"actual bounded delay rather than a nominal label")
        for frame in frames[1:]:K.frame(frame,scopes)
        if message=="reorder":
            tick=F.process(P.raw(program["TickResponse"]))
            require(len(frames)==2 and tick["code"]==tick["mask"]==0 and tick["incarnation"]==state["incarnation"] and tick["sequence"]>state["sequence"] and tick["packets"],"actual bounded next Tick for selected-command reorder")
        else:require(not P.raw(program["TickResponse"]),"only reorder admits a next native Tick")
    require(type(program["MessageFrom"]) is type(program["MessageTo"]) is int and (program["MessageFrom"],program["MessageTo"])==(frames[0]["From"],frames[0]["To"]),"actual program route matches observed immutable frame")


def authenticate(a):
    wrapper=a["Program"]
    require(type(wrapper) is dict and set(wrapper)==EXTRA and wrapper["Profile"]==PROFILE,"explicit P06 program wrapper")
    p=wrapper["Program"]
    require(type(p) is dict and set(p)==K.PROGRAM and p["Profile"]==K.PROFILE,"unchanged native program evidence codec")
    initial=K.spec(p["Spec"])
    require((p["Spec"]["Layout"],p["Spec"]["Message"])!=("original","literal"),"original literal184 already has an existing parent")
    run,deadline=fixed(a["Run"],16,True),integer(a["DeadlineUnixNanos"],(1<<63)-1,1)
    require(type(p["DeadlineUnixNanos"]) is int and p["DeadlineUnixNanos"]==deadline and p["WorkGrant"]==(128+13)*32*(2<<20) and p["OutputGrant"]==16<<20,"same original deadline and full fixed pre-admitted diagnostic grant")
    family,phase,part,kind,side=coordinates(a)
    require(type(part) is type(phase) is type(kind) is int and part in (3,8) and 1<=phase<=4 and kind in (1,2) and side in ("before","after"),"actual finite selected native coordinates")
    instances=roster(a,wrapper["Cohort"])
    require(type(p["Elections"]) is list and len(p["Elections"])==2,"both genuine initial native groups")
    selected=list(initial);observations=list(p["Elections"])
    for group,row in enumerate(p["Elections"]):election(row,(1,4)[group],initial[group],run,deadline,instances)
    successors=p["Successors"] or []
    require(type(successors) is list and len(successors)==(1 if family in ("decide","recovery","terminal") else 0),"only the required earlier prepared-A failover")
    if successors:
        row=successors[0];next_a=3-initial[0]
        election(row,initial[0],next_a,run,deadline,instances)
        selected[0]=next_a;observations[0]=row
    group=0 if part==3 else 1;target=selected[group]
    require(a["Batch"]["Member"]==target and observations[group]["Owner"]["Cookie"]==a["Arm"]["Cookie"] and observations[group]["Owner"]["PID"]==a["Arm"]["PID"],"actual selected serving leader, not caller/role relabelling")
    command=P.raw(a["SelectedCommand"])
    require(0<len(command)<=64<<10 and fixed(p["CommandSHA256"],32,True)==hashlib.sha256(command).digest(),"program attached to exact actual native command")
    C.batch_pair(a["Arm"],a["Batch"],run,deadline,target,kind,side,command)
    B.processes(a["Processes"],None,a["Case"],a["Batch"])
    after=list(selected);after[group]=3-target if group==0 else 10-target
    election(wrapper["AfterLoss"],target,after[group],run,deadline,instances)
    require(observations[group]["Owner"]["PID"]==a["Batch"]["PID"] and wrapper["AfterLoss"]["Owner"]["PID"]!=a["Batch"]["PID"],"real successor belongs to surviving different process")
    before=a["Before"] if family=="initial-batch" else a["P06"]["Before"]
    scopes,_=P.cut(before)
    packets(wrapper,p,scopes,F.process(P.raw(observations[group]["State"])),target,kind)
    return role_vectors(initial,selected,after,family,part)


def check(path):
    with Path(path).open("rb") as source:
        line=source.readline((512<<10)+1)
    require(0<len(line)<=512<<10 and line.endswith(b"\n"),"bounded existing semantic header")
    first=T.decoded(line)
    require(type(first) is dict and set(first)=={"Type","Artifact"} and first["Type"]=="header","actual existing streamed artifact header")
    oracle=select(first["Artifact"]["Profile"])
    a=oracle.load(Path(path),program=True)
    roles=authenticate(a)
    # Only the separate optional metadata is removed. Every native/source/CDC/
    # history/process byte and actual member remains unchanged for the full oracle.
    semantic=oracle.check({key:value for key,value in a.items() if key!="Program"},program_roles=roles)
    return dict(case=a["Case"],program=a["Program"]["Program"]["Spec"],mechanism=a["Program"]["Mechanism"],semantic=semantic,scope="finite P06 actual leader/message source; qualification remains separate")


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("artifact",type=Path)
    args=parser.parse_args();print(json.dumps(check(args.artifact),sort_keys=True))


if __name__=="__main__":main()
