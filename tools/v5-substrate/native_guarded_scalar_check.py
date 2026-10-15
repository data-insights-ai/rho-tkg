#!/usr/bin/env python3
"""Fresh endpoint-guarded retained-C feed/selectors, exact original-first oracle.

Private raw helpers authenticate the complete native prefix before interpreting
fixed Set6-then-same-life-Correct commands. No copied DTO or Stage reducer grants
source authority; all growing joins use the caller's single finite BLOB spool.
"""
from contextlib import nullcontext
from pathlib import Path
import native_feed_check as feed
import native_selectors_check as selectors
from native_create_update_check import proposal
from comparison_spool import Policy,Spool,u64

require,integer,fields,decoded,encoded,lines=feed.require,feed.integer,feed.fields,feed.decoded,feed.encoded,feed.lines
LINE,TRANSPORTS,MAX_SECONDS=feed.LINE,feed.TRANSPORTS,feed.MAX_SECONDS
FORMATS={"snapshot-to-feed":"native-snapshot-feed-guarded-endpoints-workload-v2","snapshot-selectors":"native-snapshot-selectors-guarded-endpoints-workload-v2"}


def shape(s):
    fields(s,"version family facts crossPercent writers layout")
    require(integer(s["version"],2,2) and s["family"] in FORMATS and integer(s["facts"],20,1_000_000)%20==0 and integer(s["crossPercent"]) in (0,10,50,100) and integer(s["writers"]) in (1,8,32) and s["facts"]>s["writers"] and s["layout"] in ("kv","columns"),"exact fresh guarded endpoints dimensions")
    return s


def frames(s):
    n=shape(s)["facts"];count=(16*n+3)*10+64*10+4096
    if s["family"]=="snapshot-to-feed":count+=(32*n+256)*5+4*((n+15)//16)
    return count


def business(directory,end,spool):
    s=shape(end["spec"]);n=s["facts"];phases=end["BusinessPhases"]
    require(type(phases) is list and len(phases)==2,"two actual business-only phase walls")
    previous=end["SetupNanos"];wall=0
    for name,p in zip(("create","update"),phases,strict=True):
        fields(p,"Phase StartNanos StopNanos WallNanos Useful")
        require(p["Phase"]==name and integer(p["Useful"],n,n) and integer(p["StartNanos"],previous,end["TotalNanos"])<integer(p["StopNanos"],p["StartNanos"]+1,end["TotalNanos"]) and integer(p["WallNanos"],1)==p["StopNanos"]-p["StartNanos"],"setup excluded; whole actual worker wall, not summed concurrent durations")
        previous=p["StopNanos"];wall+=p["WallNanos"]
    require(wall==end["BusinessNanos"],"actual complete useful throughput denominator")
    census={name:{"local":0,"cross":0,"attempts":0,"aborted":0,"protocolParticipants":0} for name in ("create","update")}
    for a in lines(directory/"attempts.jsonl",16*n+4):
        phase,fact,ordinal=feed.operation(s,a["Operation"])
        participants,protocol=proposal(a,s,spool)
        if phase=="setup":continue
        c=census[phase];c["attempts"]+=1
        if a["Classification"]!="commit":c["aborted"]+=1;continue
        c["local" if len(participants)==1 else "cross"]+=1;c["protocolParticipants"]+=len(protocol)
        spool.insert(b"guarded-business",u64(ordinal),encoded(participants))
    samples=[0,0]
    for event in lines(directory/"operations.jsonl",2*n+4):
        phase,fact,ordinal=feed.operation(s,event["Sample"]["operation"])
        if phase=="setup":continue
        i=0 if phase=="create" else 1;p=phases[i]
        require(p["StartNanos"]<=event["StartNanos"]<event["StopNanos"]<=p["StopNanos"] and integer(event["ElapsedNanos"],1)==event["StopNanos"]-event["StartNanos"],"complete queue/admission/SAME/replan/wait operation lies inside actual phase")
        spool.lookup(b"guarded-business",u64(ordinal));samples[i]+=1
        spool.insert(b"guarded-duration/"+phase.encode(),u64(event["ElapsedNanos"])+u64(ordinal),b"1")
    require(samples==[n,n],"all useful operations, no setup in business latency")
    out=[]
    for p in phases:
        c=census[p["Phase"]];require(c["local"]+c["cross"]==n and c["cross"]==n*s["crossPercent"]//100,"actual command endpoint footprint fraction, no empty remote coordination label")
        values={};count=0
        for count,(coordinate,_) in enumerate(spool.walk(b"guarded-duration/"+p["Phase"].encode()),1):
            for q in (50,95,99):
                if count==(n*q+99)//100:values[f"p{q}Nanos"]=int.from_bytes(coordinate[:8],"big")
        require(count==n and len(values)==3,"complete disk-ordered whole-operation percentiles")
        out.append({**p,**c,**values,"readParticipants":[n,c["cross"]],"writeParticipants":[n,c["cross"] if p["Phase"]=="create" else 0]})
    return {"setupTransactions":3,"phases":out,"scope":"actual Set6 verifies existing relation, then Correct verifies SAME life and both endpoint Entity/Life/Presence; extra presence CDC/columns charged, ordinary Set unchanged"}


def verify(directory,spool_directory,policy,*,binary_sha=None,expected_spec=None,expected_run=None,expected_deadline=None,expected_seconds=None,shared_spool=None):
    directory=Path(directory).resolve(strict=True)
    with (directory/"result.json").open("rb") as source:body=source.read(LINE+1)
    require(body.endswith(b"\n") and len(body)<=LINE,"bounded complete actual result")
    end=decoded(body[:-1]);s=shape(end["spec"])
    require(policy is not None,"explicit finite shared original/row/latency scratch grant");policy.validate()
    args=dict(binary_sha=binary_sha,expected_spec=expected_spec,expected_run=expected_run,expected_deadline=expected_deadline,expected_seconds=expected_seconds,_shape=shape,_format=FORMATS[s["family"]],_frames=frames,guarded_updates=True)
    with (nullcontext(shared_spool) if shared_spool is not None else Spool(spool_directory,policy)) as spool:
        if s["family"]=="snapshot-to-feed":proof=feed._verify_feed(directory,spool_directory,policy,shared_spool=spool,**args)
        else:proof=selectors._verify_selectors(directory,spool_directory,policy,shared_spool=spool,**args)
        # The proposal grammar is independent of observed effects. All bodies
        # including durable ABORTs join the fixed operation footprint here.
        measured=business(directory,end,spool)
        return {**proof,"business":measured,"sourceProfile":13 if s["family"]=="snapshot-to-feed" else 14,"spool":spool.report(),"scope":"complete fresh guarded native source/original/CDC/index/column/cache/retained-C/reopen joins; all native transports; local pairing separate, nonlocal paired completeness remains open; source implementation is not qualification"}
