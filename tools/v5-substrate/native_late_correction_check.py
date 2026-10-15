#!/usr/bin/env python3
"""Actual half-correction/native retained posting oracle; no execution claim."""
import hashlib

import native_create_update_check as common

source,scalar,column,codec=common.source,common.scalar,common.column,common.codec
require,integer,fields,blob,array,decoded,encoded,lines,u64=common.require,common.integer,common.fields,common.blob,common.array,common.decoded,common.encoded,common.lines,common.u64
FAMILY,FORMAT,LINE="late-correction-old-index","native-late-correction-workload-v1",common.LINE
TRANSPORTS,MAX_SECONDS=common.TRANSPORTS,common.MAX_SECONDS
ACCESS="complete actual native out/in incident-posting pages with typed scalar residuals and original-bound hydration; one shared candidate walk per endpoint for5/6/phantom7, not value-index latency"
WORK="Records Bytes DirectoryPages CheckpointPages PatchPages DecodedCells".split()
INDEX_STAGES=("same-pin-old-after-update","fresh-current")


def shape(s):return common.shape(s,FAMILY)
def frames(s):return common.frames(s,FAMILY)


def index_page(trace,end,spool,owner,observation):
    s=end["spec"];stage=trace["Stage"];q,r=trace["Input"],trace["Reply"]
    require(stage in INDEX_STAGES and observation["points"] and "timing" in observation and owner["eof"] and owner["token"] is not None,"actual retained completed source owner before index")
    state=observation.setdefault("index",{"endpoint":0,"counts":[0,0,0],"last":0,"work":observation["pointWork"],"nanos":0,"totals":[[0,0] for _ in range(6)]})
    index=state["endpoint"];require(index<3,"index after complete native EOF")
    seq=integer(q["Sequence"],1)
    require(q["IDs"] is None and r["Sequence"]==seq==owner["sequence"]+1 and r["Handle"]==q["Handle"],"same private owner/sequence, no caller matching-ID authority")
    meta=r["Index"];fields(meta,"Endpoint Kind Equals Matches EndpointEOF Complete")
    require(type(meta["Matches"]) is list and len(meta["Matches"])==3 and all(type(v) is int and 0<=v<=s["facts"] for v in meta["Matches"]) and meta["Matches"][2]==0,"fixed scalar cardinalities with exact-empty phantom7")
    endpoint=(1,3,2*s["facts"]+7)[index];kind="out" if index==0 else "in"
    require(meta["Endpoint"]==endpoint and meta["Kind"]==kind and meta["Equals"]==[5,6,7] and type(meta["EndpointEOF"]) is type(meta["Complete"]) is bool and meta["EndpointEOF"] is r["EOF"] and meta["Complete"]==(index==2 and r["EOF"]),"three actual endpoint EOFs are distinct from whole-index completion")
    require(len(r["Points"] or [])==len(r["Candidates"] or [])<=16 and integer(r["Starts"],1) and integer(r["Calls"],1),"bounded genuine incident page/hydration")
    fields(r["Work"]," ".join(WORK))
    for name in WORK:require(integer(r["Work"][name])>=state["work"][name],"same native owner cumulative index work cannot reset")
    fields(r["Charged"]," ".join(column.WORK))
    starts=integer(r["Starts"],1)
    bound=9*((s["facts"]+15)//16+end["Admission"]["BudgetSeconds"]//60+16)+4*((frames(s)+3)//4)+64
    require(starts<=bound and integer(r["Calls"],1,bound*8) and r["Charged"]==dict(zip(column.WORK,(starts*16384,starts*(16<<20),0,starts*(32<<20),starts*1024,starts*(4<<30)))),"fresh11-only finite nine-pass source grants; actual work separate")
    for identity,entity in zip(r["Candidates"] or [],r["Points"] or [],strict=True):
        identity=integer(identity,7,7+2*(s["facts"]-1));fact=(identity-7)//2
        require(identity==7+2*fact and identity==entity["id"] and identity>state["last"] and (entity["source"] if index==0 else entity["target"])==endpoint,"actual outgoing/incoming posting identity/order/endpoint")
        state["last"]=identity
        source.point(s,entity,5 if stage==INDEX_STAGES[0] else 6,spool,guarded_updates=True)
        value=entity["property"]["i64"];require(value in (5,6),"unknown scalar cannot become a missing predicate row")
        state["counts"][value-5]+=1
        spool.insert(b"correction-index-"+stage.encode(),kind.encode()+u64(identity),encoded(entity))
    require(meta["Matches"]==state["counts"] and state["counts"][2]==0,"actual cumulative scalar predicates and exact-empty7")
    owner["sequence"],owner["last"]=seq,(q,r)
    state["work"],state["nanos"]=r["Work"],state["nanos"]+trace["OperationNanos"]
    if r["EOF"]:
        base,g=(0,0) if index==0 else (3,0 if index==1 else 1)
        for i,n in enumerate(state["counts"]):state["totals"][base+i][g]=n
        spool.insert(b"correction-endpoint",stage.encode()+bytes([index]),encoded({"Handle":q["Handle"],"Index":meta,"Work":r["Work"]}))
        state["endpoint"]+=1;state["counts"]=[0,0,0];state["last"]=0
    if meta["Complete"]:
        require(state["endpoint"]==3 and sum(sum(v) for v in state["totals"])==2*s["facts"],"all outgoing and incoming native postings, not count-only closure")
        spool.insert(b"correction-index-eof",stage.encode(),encoded({"Handle":q["Handle"],"Cut":owner["wire"],"Matches":state["totals"],"Work":state["work"],"Nanos":state["nanos"]}))


def indexes(directory,end,spool):
    summary=end["Correction"];fields(summary,"Version IndexObservations SHA256 Access")
    require(integer(summary["Version"],1,1) and integer(summary["IndexObservations"],2,2) and summary["Access"]==ACCESS,"fresh complete half-correction summary")
    seal=hashlib.sha256();header=footer=False;stage=0;timing=[]
    for frame in lines(directory/"indexes.jsonl",4):
        require(not footer,"trailing index artifact")
        if frame.get("Type")=="footer":
            require(header and stage==2 and frame=={"Type":"footer","End":summary} and seal.digest()==array(summary["SHA256"],32),"both complete native index EOFs and source seal")
            footer=True;continue
        seal.update(encoded(frame)+b"\n")
        if not header:
            require(frame=={"Type":"header","Format":FORMAT,"Spec":end["spec"],"AdmissionSHA256":end["AdmissionSHA256"]},"exact actual source admission")
            header=True;continue
        fields(frame,"Type Name Stage Access Handle Cut Matches Work Nanos")
        require(stage<2 and frame["Type"]=="index-eof" and frame["Name"]==("old","final")[stage] and frame["Stage"]==INDEX_STAGES[stage] and frame["Access"]==ACCESS,"actual old/current index schedule")
        actual=decoded(spool.lookup(b"correction-index-eof",frame["Stage"].encode()))
        require(all(frame[k]==actual[k] for k in ("Handle","Cut","Matches","Work")) and frame["Cut"]==end["Old" if stage==0 else "Final"] and integer(frame["Nanos"],actual["Nanos"],end["TotalNanos"])>0,"genuine native owner/fullEOF/work binding; complete API time includes retries/callbackIO")
        n=end["spec"]["facts"];counts=[[0,0] for _ in range(6)]
        for fact in range(n):
            identity=7+2*fact
            value=5 if stage==0 or fact>=n//2 else 6
            ordinal=4+fact+(n if value==6 else 0)
            row=decoded(spool.lookup(b"row",u64(ordinal)+u64(identity)+b"\3"))
            for kind in ("out","in"):
                entity=decoded(spool.lookup(b"correction-index-"+frame["Stage"].encode(),kind.encode()+u64(identity)))
                source.point(end["spec"],entity,value,spool,guarded_updates=True)
                require(entity["property"]["provenance"]==ordinal and entity["source"]==row["Source"] and entity["target"]==row["Target"],"every actual corrected/unchanged original has BOTH complete native postings")
                g=0 if kind=="out" or row["Target"]==3 else 1
                counts[(0 if kind=="out" else 3)+value-5][g]+=1
        require(frame["Matches"]==counts and counts[2]==counts[5]==[0,0],"exact independent old5/currenthalf6/unchanged5/phantom7 predicate sets")
        for endpoint in range(3):spool.lookup(b"correction-endpoint",frame["Stage"].encode()+bytes([endpoint]))
        timing.append({"call":"IndexScan","name":frame["Name"],"cut":frame["Cut"],"matches":counts,"nanos":frame["Nanos"],"access":ACCESS});stage+=1
    require(header and footer and stage==2,"missing/partial index cannot complete family")
    values=sorted(v["nanos"] for v in timing)
    return {"indexTiming":timing,"indexNearestRank":{"samples":2,**{f"p{p}Nanos":values[(p*2+99)//100-1] for p in (50,95,99)}}}


def verify(directory,spool_directory,policy,**keywords):
    return common._verify(directory,spool_directory,policy,family=FAMILY,**keywords)
