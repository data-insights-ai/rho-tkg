#!/usr/bin/env python3
"""Native create/update source oracle; no runtime or efficiency acceptance.

Complete immutable PGO/claims, actual local/cross originals, all-voter source
effects and five retained old/current/reopen observations precede any digest or
throughput result. Growing joins/latencies use one caller-granted BLOB spool.
"""
from contextlib import nullcontext
from pathlib import Path

import native_selectors_check as source
from comparison_spool import Spool,u64

scalar,codec,raw,column=source.scalar,source.codec,source.raw,source.column
require,integer,fields,blob,array,decoded,encoded,lines=source.require,source.integer,source.fields,source.blob,source.array,source.decoded,source.encoded,source.lines
digest=source.digest
FAMILY,FORMAT,LINE="partition-local-create-update","native-create-update-workload-v1",source.LINE
TRANSPORTS,MAX_SECONDS=source.TRANSPORTS,source.MAX_SECONDS
STAGES=("old-fresh","same-pin-old-after-update","fresh-current","reopened-old","reopened-final")


def shape(s, family=FAMILY):
    require(family in (FAMILY,"immutable-columns"),"explicit implemented guarded scalar family")
    fields(s,"version family facts crossPercent writers layout")
    require(integer(s["version"],1,1) and s["family"]==family and integer(s["facts"],20,1_000_000)%20==0 and integer(s["crossPercent"]) in (0,10,50,100) and integer(s["writers"]) in (1,8,32) and s["facts"]>s["writers"] and s["layout"] in ("kv","columns"),"fresh native guarded scalar dimensions")
    return s


def frames(s, family=FAMILY):return (16*shape(s,family)["facts"]+3)*10+64*10+4096


def phase(p,n,cross,total):
    fields(p,"Phase StartNanos StopNanos WallNanos SingleParticipant MultiParticipant ReadParticipants WriteParticipants")
    require(p["Phase"] in ("create","update"),"actual complete business phase")
    start=integer(p["StartNanos"],0,total);stop=integer(p["StopNanos"],start,total)
    require(integer(p["WallNanos"],1,total)==stop-start and integer(p["SingleParticipant"],0,n)+integer(p["MultiParticipant"],0,n)==n and p["MultiParticipant"]*100==n*cross,"actual phase wall/local-versus-cross census, setup excluded")
    require(p["ReadParticipants"]==[n,p["MultiParticipant"]] and p["WriteParticipants"]==[n,p["MultiParticipant"] if p["Phase"]=="create" else 0],"genuine guarded endpoint reads versus logical property/presence writes; empty protocol kernels are separate")


def proposal(a,s,spool):
    phase_,fact,ordinal=scalar.operation(s,a["Operation"])
    body=blob(a["Proposal"])
    # Reuse the existing bounded original grammar, not a second graph reducer.
    typed=codec.typed
    parsed=typed.pgq(body);provenance,ops=typed.operations(body,allow_present_correction=True)
    require(parsed["kind"]==6 and provenance==ordinal,"actual original operation/provenance")
    if phase_=="setup":
        owner=(1,3,2*s["facts"]+7)[fact]
        retained=decoded(spool.lookup(b"row",u64(ordinal)+u64(owner)+b"\1"))
        require(len(ops)==1 and (ops[0]["kind"],ops[0]["owner"],ops[0]["life"])==(1,owner,owner+1) and ops[0]["record"]==(0,0,0,b"",0,0,0,3,1) and ops[0]["binding"]==(0,0,0,0) and ops[0]["scope"]==blob(retained["Scope"]) and not ops[0]["name"] and not ops[0]["scalar"] and ops[0]["value"]==ops[0]["present"]==0,"actual complete setup original")
        return None,None
    owner=7+2*fact
    retained=decoded(spool.lookup(b"row",u64(4+fact)+u64(owner)+b"\1"))
    target=retained["Target"]
    require(retained["Owner"]==owner and retained["Source"]==1 and retained["SourceLife"]==2 and retained["TargetLife"]==target+1 and target in (3,2*s["facts"]+7),"actual retained original full endpoint/life descriptor")
    if phase_=="create":
        require(len(ops)==2 and [(o["kind"],o["owner"],o["life"]) for o in ops]==[(2,owner,owner+1),(8,owner,owner+1)] and ops[0]["record"]==(0,0,0,b"edge",1,target,1,6,2) and ops[0]["binding"]==(0,0,2,target+1) and not ops[0]["name"] and not ops[0]["scalar"] and ops[0]["value"]==0,"actual create graph footprint")
    else:
        require(len(ops)==2 and [(o["kind"],o["owner"],o["life"]) for o in ops]==[(8,owner,owner+1),(5,owner,owner+1)] and ops[1]["present"]==1 and ops[1]["record"]==(0,0,0,b"",0,0,0,0,0) and ops[1]["binding"]==(0,0,0,0) and not ops[1]["name"] and not ops[1]["scalar"] and ops[1]["value"]==0,"every update requires existing relation through Set6 then authentic live-endpoint reaffirmation; neither revival nor dummy remote read")
    value=5 if phase_=="create" else 6
    set_=ops[-1] if phase_=="create" else ops[0]
    require(set_["name"]==b"p" and set_["scalar"]==b"\4"+u64(value) and set_["value"]==value and set_["present"]==0 and set_["record"]==(0,0,0,b"",0,0,0,0,0) and set_["binding"]==(0,0,0,0) and all(o["scope"]==blob(retained["Scope"]) for o in ops),"actual fixed all-scope scalar update; no dummy remote read or hidden structure")
    business=[3] if target==3 else [3,8]
    protocol=[3] if a["Manifest"] is None else sorted(codec.manifest(blob(a["Manifest"]))["floors"])
    require((a["Manifest"] is None)==(len(business)==1),"local business must use actual local Submit; cross uses actual Run")
    require(parsed["partition"]==3 and set(business)<=set(protocol),"actual source and protocol participants distinguished")
    return business,protocol


def business(directory,end,spool):
    s=end["spec"];n=s["facts"]
    require(end["SetupTransactions"]==3 and end["RowSemantics"]==("strict-partition-local-create-update" if s["crossPercent"]==0 else "mixed-or-cross-partition-create-update") and end["BusinessFootprint"]=="complete actual graph operation/read/write/claim footprint under authenticated installed ranges; setup excluded" and end["Coordination"]=="local Submit for one business partition; cross Run for multi-participant business; actual manifest participants/empty control coordination reported separately","actual row semantics; setup and coordination separate")
    phases=end["BusinessPhases"]
    require(type(phases) is list and len(phases)==2,"two actual complete phase walls")
    for name,p in zip(("create","update"),phases,strict=True):phase(p,n,s["crossPercent"],end["TotalNanos"]);require(p["Phase"]==name,"ordered actual phases")
    require(phases[0]["StopNanos"]<=phases[1]["StartNanos"] and sum(p["WallNanos"] for p in phases)==end["BusinessNanos"],"actual business walls, never sum of parallel operation latency")
    counts={name:{"single":0,"multi":0,"attempts":0,"aborted":0,"protocolParticipantInstalls":0} for name in ("create","update")}
    for a in lines(directory/"attempts.jsonl",16*n+4):
        phase_,fact,ordinal=scalar.operation(s,a["Operation"])
        participants,protocol=proposal(a,s,spool)
        if phase_=="setup":continue
        c=counts[phase_];c["attempts"]+=1
        if a["Classification"]!="commit":c["aborted"]+=1;continue
        c["single" if len(participants)==1 else "multi"]+=1;c["protocolParticipantInstalls"]+=len(protocol)
        spool.insert(b"business-useful",u64(ordinal),encoded(participants))
    timing={"create":0,"update":0}
    for operation in lines(directory/"operations.jsonl",2*n+4):
        phase_,fact,ordinal=scalar.operation(s,operation["Sample"]["operation"])
        if phase_=="setup":continue
        p=phases[0 if phase_=="create" else 1]
        require(p["StartNanos"]<=operation["StartNanos"]<operation["StopNanos"]<=p["StopNanos"] and operation["ElapsedNanos"]<=p["WallNanos"],"every original includes its actual queue/retry/wait inside the measured phase")
        spool.lookup(b"business-useful",u64(ordinal))
        spool.insert(b"business-latency-"+phase_.encode(),u64(operation["ElapsedNanos"])+u64(ordinal),b"1");timing[phase_]+=1
    require(end["LogicalCross"]==sum(c["multi"] for c in counts.values()) and end["LogicalLocal"]==sum(c["single"] for c in counts.values()) and end["LogicalCross"]+end["LogicalLocal"]==2*n,"reported ratio joins actual original business footprint, setup excluded")
    result=[]
    for p in phases:
        name=p["Phase"];c=counts[name]
        require((c["single"],c["multi"])==(p["SingleParticipant"],p["MultiParticipant"]) and timing[name]==n,"observed source counter independently matches actual originals")
        percentiles={};position=0
        for position,(coordinate,_) in enumerate(spool.walk(b"business-latency-"+name.encode()),1):
            for q in (50,95,99):
                if position==(q*n+99)//100:percentiles[f"p{q}Nanos"]=int.from_bytes(coordinate[:8],"big")
        require(position==n and len(percentiles)==3,"complete original-bound useful latency census")
        result.append({**p,**c,**percentiles,"usefulTransactionsPerSecond":n*1_000_000_000/p["WallNanos"]})
    return result


def read_timings(samples):
    require([s["name"] for s in samples]==list(STAGES),"only actual old/current/reopen observations")
    result=[]
    for call,key in (("Open","OpenNanos"),("Scan","ScanNanos"),("Points","PointsNanos")):
        values=sorted(integer(s[key],1) for s in samples)
        result.append({"call":call,"samples":len(values),**{f"p{p}Nanos":values[(p*len(values)+99)//100-1] for p in (50,95,99)}})
    return {"samples":samples,"nearestRank":result,"scope":"actual complete source operations through EOF/retry/wait; scan-based Points, no indexed/per-key claim"}


def verify(directory,spool_directory,policy,*,binary_sha=None,expected_spec=None,expected_run=None,expected_deadline=None,expected_seconds=None,shared_spool=None):
    return _verify(directory,spool_directory,policy,binary_sha=binary_sha,expected_spec=expected_spec,expected_run=expected_run,expected_deadline=expected_deadline,expected_seconds=expected_seconds,shared_spool=shared_spool)


def _verify(directory,spool_directory,policy,*,binary_sha=None,expected_spec=None,expected_run=None,expected_deadline=None,expected_seconds=None,shared_spool=None,family=FAMILY):
    require(family in (FAMILY,"immutable-columns"),"explicit implemented guarded scalar oracle")
    immutable=family=="immutable-columns"
    format_="native-immutable-columns-workload-v1" if immutable else FORMAT
    directory=Path(directory).resolve(strict=True)
    with (directory/"result.json").open("rb") as stream:body=stream.read(LINE+1)
    require(body.endswith(b"\n") and len(body)<=LINE,"bounded complete native result")
    end=decoded(body[:-1]);s=shape(end["spec"],family);a=end["Admission"]
    ceiling=frames(s,family)
    require(integer(end["version"],1,1) and end["format"]==format_ and end["engine"]=="native-raft" and end["FramesCeiling"]==ceiling and end["RetentionContract"]=="snapshot-C-retained-full-prefix-v1","fresh actual native profile9/10 family")
    require(integer(a["Version"],1,1) and a["Family"]==family and a["Spec"]==s and digest(encoded(a))==array(end["AdmissionSHA256"],32) and integer(a["BudgetSeconds"],1,MAX_SECONDS)*1000>integer(a["CausalMinimumMillis"]) and a["CausalMinimumMillis"]==5*a["Transport"]["targetRTTMillis"],"actual immutable native source/run/binary/budget admission")
    require(type(a["Run"]) is str and len(a["Run"])==32 and bytes.fromhex(a["Run"]).hex()==a["Run"] and any(bytes.fromhex(a["Run"])) and integer(a["DeadlineUnixNanos"],1,(1<<63)-1),"same original deadline/run")
    for expected,actual in ((binary_sha,a["BinarySHA256"]),(expected_spec,s),(expected_run,a["Run"]),(expected_deadline,a["DeadlineUnixNanos"]),(expected_seconds,a["BudgetSeconds"])):require(expected is None or expected==actual,"controller/source admission mismatch")
    cuts={name:source.cut(end[name]) for name in ("Initial","Old","Final","Reopened")}
    require(cuts["Final"]==cuts["Reopened"] and all(len(v)==2 for v in cuts.values()),"complete genuine retained/restored cuts")
    for group in range(2):
        first=cuts["Initial"][group]
        require(all(v[group]["scope"]==first["scope"] and v[group]["configuration"]==first["configuration"] for v in cuts.values()) and array(a["Graph"],16)==first["scope"][:16] and a["Topology"]==int.from_bytes(first["scope"][48:56],"big") and array(a["Declaration"],32)==first["scope"][56:88],"actual graph/configuration/routing topology")
    require(Path(end["ArtifactDirectory"]).resolve(strict=True)==directory and Path(end["Directory"]).resolve(strict=True).parent==directory,"owned actual source directory")
    require(type(end["Completed"]) is list and len(end["Completed"])==32 and all(type(v) is int and v>=0 for v in end["Completed"]),"actual client census")
    require(all(type(end[n]) is int and end[n]>=0 for n in ("SetupNanos","BusinessNanos","DiagnosticNanos","TotalNanos","OriginalCaptures","ExtraInvocations","QueryExtraInvocations")) and end["SetupNanos"]+end["BusinessNanos"]+end["DiagnosticNanos"]==end["TotalNanos"] and end["BusinessNanos"]>0,"whole business and separate diagnostic accounting")
    transport_options={"maximum_frames":ceiling*1024} if immutable else {}
    selected=codec.owned_transport(end["Transport"],{"version":1,"family":codec.FAMILY,"facts":s["facts"],"crossPercent":s["crossPercent"],"writers":s["writers"],"layout":s["layout"]},**transport_options)
    require(selected==a["Transport"],"actual native owned scheduler, no nominal RTT")
    for cohort in end["Transport"]:
        require(all(cohort[n]["Admitted"]<=ceiling*1024 for n in ("Raft","SourceRequest","SourceReply")) and all(cohort[n]["AttemptedFrames"]<=ceiling*1024 for n in ("PipeReads","PipeWrites")),"same finite actual source/frame grants")
    with (nullcontext(shared_spool) if shared_spool is not None else Spool(spool_directory,policy)) as spool:
        source.grants(directory,end,spool);source.claims(directory,end,spool)
        scalar._scalar_attempts(directory,end,spool)
        scalar._scalar_sources(directory,end,spool,source.cut,ceiling,guarded_updates=True)
        timing=source.reads(directory,end,spool,family=family)
        scalar._scalar_columns(directory,end,spool,source.cut,ceiling,end["Old"])
        source.processes(directory,end,spool)
        business_timing=business(directory,end,spool)
        answer=source.logical_answers(end,spool,guarded_updates=True)
        additional={}
        if immutable:
            import native_immutable_columns_check as columns
            additional=columns.observations(directory,end,spool)
        else:require("ImmutableColumns" not in end,"fresh column observations cannot relabel old9")
        scope="genuine native local Submit versus explicit mixed/cross variants, complete original/source/voter/old-current/column/cache/reopen joins; setup excluded; planned installed-grant IDs, not Recipient.Next; no efficiency/capacity/full-matrix or nonlocal engine-parity acceptance"
        if immutable:scope+="; KV is source-scan reference; actual immutable derived bytes retained/counted, no physical derived-GC claim"
        return {"format":format_,"family":family,"spec":s,"run":a["Run"],"logicalAnswersSHA256":answer,"retentionContract":end["RetentionContract"],"transport":selected,"rowSemantics":end["RowSemantics"],"businessTiming":business_timing,"sourceTiming":read_timings(timing),"spool":spool.report(),**additional,"scope":scope}
