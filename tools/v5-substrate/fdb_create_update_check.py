#!/usr/bin/env python3
"""Exact create/update business-footprint oracle over genuine source originals.

The canonical family has a strict-local0 baseline; nonzero rows are explicitly
mixed/cross variants. Native FDB global atomic coordination is not rho 2PC and
is not inferred from the logical participant vector. One finite caller spool
holds all graph/attempt/latency state; no graph-sized Python collection.
"""
import hashlib
import json
import sys

import fdb_columns_check as common

source=common.source
require,fields,integer=source.require,source.fields,source.integer
decoded,blob,go_json,u64=source.decoded,source.blob,source.go_json,source.u64
FAMILY,FORMAT="partition-local-create-update","fdb-create-update-v1"


def row_semantics(cross):
    return "strict-partition-local-create-update" if cross==0 else "mixed-or-cross-partition-create-update"


def footprint(tx,spec):
    command=tx["GraphChange"]
    require(command["Kind"] in ("edge-create","edge-update"),"actual business command")
    edge=command["Edge"]
    fact=(integer(edge["ID"],7)-7)//2
    require(edge["ID"]==7+2*fact and fact<spec["facts"],"actual declared business identity")
    ordinal=4+fact+(spec["facts"] if command["Kind"]=="edge-update" else 0)
    expected,_,effects,keys=source.codec.fdb_contract(spec,ordinal)
    require(source.same(command,expected),"complete actual graph command")
    reads=[0,0];writes=[0,0]
    require(type(tx["Participants"]) is list and [p["Group"] for p in tx["Participants"]]==sorted(effects),"complete business participants; no empty coordination group")
    for part in tx["Participants"]:
        g=integer(part["Group"],0,1)
        require(type(part["Reads"]) is list and type(part["Effects"]) is list and part["Reads"] and part["Effects"],"business reads/effects cannot be empty")
        require(source.same(part["Effects"],effects[g]) and [r["Key"] for r in part["Reads"]]==keys[g],"all actual business reads and effects, not remote endpoint density")
        reads[g],writes[g]=1,1
    return reads,writes


def phase(p,n,cross):
    fields(p,"Phase Attempts Aborted Committed SingleParticipant MultiParticipant ReadParticipants WriteParticipants StartedUnixNanos FinishedUnixNanos WallNanos")
    require(p["Phase"] in ("create","update"),"actual business phase")
    for name in ("Attempts","Aborted","Committed","SingleParticipant","MultiParticipant"):
        integer(p[name],0,64*n)
    require(p["Committed"]==n and p["Attempts"]==n+p["Aborted"] and p["SingleParticipant"]+p["MultiParticipant"]==n and p["MultiParticipant"]*100==n*cross,"actual useful COMMIT footprint ratio, setup excluded")
    require(p["ReadParticipants"]==p["WriteParticipants"]==[n,p["MultiParticipant"]],"complete actual read/write participant totals")
    for name in ("StartedUnixNanos","FinishedUnixNanos","WallNanos"):integer(p[name],1,(1<<63)-1)
    require(p["FinishedUnixNanos"]>=p["StartedUnixNanos"],"wall-clock observation cannot move backwards")


def validate_summary(end):
    value=end["CreateUpdate"]
    fields(value,"RowSemantics IDAdmission Coordination SetupExcluded Phases")
    require(value["RowSemantics"]==row_semantics(end["Spec"]["crossPercent"]) and value["IDAdmission"]=="fixed declared fixture IDs; no allocator issuance claim" and value["Coordination"]=="one ordinary native FDB transaction per submission across the actual business prefixes; no rho 2PC; SDK retries/attempts recorded separately" and value["SetupExcluded"] is True and len(value["Phases"])==2,"fresh explicit row/ID/coordination semantics")
    for name,p in zip(("create","update"),value["Phases"],strict=True):
        phase(p,end["Spec"]["facts"],end["Spec"]["crossPercent"])
        require(p["Phase"]==name,"ordered complete business phases")
        if end["Version"]==2:
            admission=end["Transport"]["Policy"]
            require(admission["AdmittedUnixNanos"]<=p["StartedUnixNanos"]<=p["FinishedUnixNanos"]<=admission["DeadlineUnixNanos"],"same original native transport lifetime")
    require(value["Phases"][0]["FinishedUnixNanos"]<=value["Phases"][1]["StartedUnixNanos"],"old-cut barrier separates actual create/update phases")
    return value


def phase_event(frame,end,spool,completions,observations,inventories):
    fields(frame,"Type RowSemantics Phase")
    value=validate_summary(end)
    p=frame["Phase"];index=0 if p["Phase"]=="create" else 1
    require(p==value["Phases"][index] and frame["RowSemantics"]==value["RowSemantics"] and completions==3+(index+1)*end["Spec"]["facts"] and observations==index and inventories==index,"phase wall bound to actual drained commits/old-cut inventory")
    if index:spool.lookup(b"business-phase",b"create")
    spool.insert(b"business-phase",p["Phase"].encode(),go_json(p))


def business(directory,end,spool):
    value=validate_summary(end)
    counts={name:{k:0 for k in ("Attempts","Aborted","Committed","SingleParticipant","MultiParticipant")} | {"ReadParticipants":[0,0],"WriteParticipants":[0,0]} for name in ("create","update")}
    digest,footer=hashlib.sha256(),False
    for frame,line in source.lines(directory/"fdb-create-update-attempts.jsonl",end["Attempts"]+1):
        require(not footer,"changed/trailing attempt evidence")
        if frame["Type"]=="footer":
            require(frame["PayloadSHA256"]==digest.hexdigest()==end["AttemptSHA256"],"second-pass actual raw attempt seal")
            footer=True;continue
        digest.update(line)
        a=frame["Attempt"]
        if a["Operation"]["Phase"]=="setup":continue
        p=counts[a["Operation"]["Phase"]]
        tx=decoded(blob(a["Proposal"]))
        reads,writes=footprint(tx,end["Spec"])
        # The first shared pass validated every read/effect/key/body and BOTH
        # retained originals; bind this pass back to that exact native pair.
        for record,kind in zip(a["Originals"],(b"id",b"request"),strict=True):
            require(blob(record["Value"])==spool.lookup(b"attempt-"+kind,blob(record["Key"])),"same authenticated original")
        p["Attempts"]+=1
        if a["Classification"]=="aborted":p["Aborted"]+=1;continue
        require(a["Classification"]=="committed","unknown attempt cannot become useful throughput")
        p["Committed"]+=1
        p["MultiParticipant" if sum(reads)==2 else "SingleParticipant"]+=1
        for g in range(2):p["ReadParticipants"][g]+=reads[g];p["WriteParticipants"][g]+=writes[g]
    require(footer,"missing actual attempt EOF")
    for p in value["Phases"]:
        require(all(p[k]==v for k,v in counts[p["Phase"]].items()) and spool.lookup(b"business-phase",p["Phase"].encode())==go_json(p),"reported phase count is not actual immutable attempts")
    latency_counts={"create":0,"update":0};maximum={"create":0,"update":0}
    for sample,_ in source.lines(directory/"fdb-create-update-latency.jsonl",end["Committed"]):
        if sample["Phase"]=="setup":continue
        name=sample["Phase"];nanos=integer(sample["Nanos"],1,(1<<63)-1)
        require(name in latency_counts and type(sample["Fact"]) is int and 0<=sample["Fact"]<end["Spec"]["facts"],"actual phase/fact latency")
        ordinal=4+sample["Fact"]+(end["Spec"]["facts"] if name=="update" else 0)
        original=decoded(spool.lookup(b"useful-attempt",u64(ordinal)))
        require(spool.lookup(b"latency-seen",u64(ordinal))==b"1" and original["Operation"]["Phase"]==name and original["Operation"]["Fact"]==sample["Fact"] and original["ClientID"]==sample["Client"] and sample["Access"]=="actual admission through business attempt/replan/verification; not selector point latency","actual complete-operation timing")
        spool.insert(b"business-latency-seen",u64(ordinal),b"1")
        spool.insert(b"business-latency-"+name.encode(),u64(nanos)+u64(ordinal),b"1")
        maximum[name]=max(maximum[name],nanos);latency_counts[name]+=1
    output=[]
    for p in value["Phases"]:
        name=p["Phase"];n=end["Spec"]["facts"]
        require(latency_counts[name]==n and maximum[name]<=p["WallNanos"],"whole actual phase includes seed, parallel queue/retry/wait and verification")
        ranks={q:(q*n+99)//100 for q in (50,95,99)};percentiles={}
        position=0
        for position,(coordinate,_) in enumerate(spool.walk(b"business-latency-"+name.encode()),1):
            for q,rank in ranks.items():
                if position==rank:percentiles["p"+str(q)+"Nanos"]=int.from_bytes(coordinate[:8],"big")
        require(position==n and len(percentiles)==3,"complete ordered external latency census")
        output.append({**p,"usefulTransactionsPerSecond":n*1_000_000_000/p["WallNanos"],**percentiles})
    return {"rowSemantics":value["RowSemantics"],"businessPhases":output,"businessScope":"setup3 excluded; actual business read/write participants, one ordinary global FDB commit path; complete phase wall, never sum of parallel operation latencies"}


def verify(directory,spool_directory,policy=None,*,shared_spool=None):
    return common.verify(directory,spool_directory,policy,family=FAMILY,shared_spool=shared_spool)


if __name__=="__main__":
    try:
        if len(sys.argv) not in (3,5):raise ValueError("usage: fdb_create_update_check.py ARTIFACTS OWNED_SPOOL_DIR [DISK_BYTES CACHE_BYTES]")
        policy=None if len(sys.argv)==3 else source.Policy(int(sys.argv[3]),int(sys.argv[4]),4096,source.raw.LINE)
        print(json.dumps(verify(sys.argv[1],sys.argv[2],policy),indent=2))
    except (ValueError,KeyError,TypeError,OSError) as error:
        print("create/update evidence refused: "+str(error),file=sys.stderr);sys.exit(1)
