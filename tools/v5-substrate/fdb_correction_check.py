#!/usr/bin/env python3
"""Actual late scalar correction/retained posting oracle, source-only until run."""
import hashlib
import json
from pathlib import Path
import sys

import fdb_columns_check as shared

source=shared.source
require,fields,integer=shared.require,shared.fields,shared.integer
go_json,blob,decoded=shared.go_json,shared.blob,shared.decoded
key,prefix,u64=shared.key,shared.prefix,shared.u64
FAMILY="late-correction-old-index"
PREDICATES=[{"Kind":kind,"Equals":value} for kind in ("out","in") for value in (5,6,7)]
ACCESS="ONE complete retained source Scan for six predicates including native retries/wait and caller row IO; not indexed latency"


def index_match(frame,name,end,spool,previous,counts):
    fields(frame,"Type Name Match")
    require(frame["Type"]=="match" and frame["Name"]==name,"actual index batch scope")
    match=frame["Match"]
    fields(match,"Selector Row")
    index=integer(match["Selector"],0,5)
    row=match["Row"]
    predicate=PREDICATES[index]
    require(row["Key"].startswith(predicate["Kind"]+"/") and row["Value"]["Value"]==predicate["Equals"],"exact native out/in scalar predicate")
    order=row["Group"],row["Key"]
    require(previous is None or previous<order,"unique deterministic index stream, no duplicate ID set in RAM")
    # Both observations happen AFTER correction. Old must follow genuine6->5
    # predecessors; unchanged IDs must remain5, and each raw index row joins its
    # complete actual source/native physical posting rather than a fake model.
    source.snapshot_row(row,end["Old" if name=="old" else "Final"],end,spool,"index-"+name)
    native=key(end["Config"],row["Group"],"current",row["Key"])
    spool.insert(b"index-match-"+name.encode(),native,u64(index))
    counts[index][row["Group"]]+=1
    return order


def indexes(directory,end,spool):
    digest,started,footer=hashlib.sha256(),False,False
    names=("old","final")
    stage,previous,counts=0,None,[[0,0] for _ in range(8)]
    timing=[]
    c,p=end["Config"],end["Spec"]
    for frame,line in source.lines(directory/"fdb-correction-indexes.jsonl",4*p["facts"]+4):
        require(not footer,"trailing index frame")
        kind=frame.get("Type")
        if kind=="footer":
            require(stage==2 and frame=={"Type":"footer","PayloadSHA256":digest.hexdigest()},"both actual index EOFs and raw seal required")
            footer=True
            continue
        digest.update(line)
        if not started:
            fields(frame,"Type Format Access Spec Config Binding Old Final Predicates Owners")
            expected={"Type":"header","Format":end["Format"],"Access":"declared out/in postings; batched scan-based selection, not a value index","Spec":p,"Config":c,"Binding":end["BindingRecord"],"Old":end["Old"],"Final":end["Final"],"Predicates":PREDICATES,
                      "Owners":[decoded(spool.lookup(b"acquired-owner",n)) for n in (b"old-fresh",b"fresh-current")]}
            require(frame==expected,"exact original source owners/binding/native cut and predicate union")
            started=True
            continue
        require(stage<2,"index data after complete cuts")
        name=names[stage]
        if kind=="match":
            previous=index_match(frame,name,end,spool,previous,counts)
        elif kind=="index-eof":
            fields(frame,"Type Name Access End Nanos")
            answer=frame["End"]
            fields(answer,"Selectors Matches Source")
            require(frame["Name"]==name and frame["Access"]==ACCESS and answer["Selectors"]==6 and answer["Matches"]==counts,"actual timed complete batch, no count-only success")
            native=answer["Source"]
            fields(native,"Cut Rows DirectoryRows OriginalEffects Work")
            require(native["Cut"]==end[name.title()] and native["Rows"]==native["DirectoryRows"]==end["Sources"][1]["Rows"] and native["OriginalEffects"]==decoded(spool.lookup(b"source-count",name.encode()))[1],"one complete source/current directory and original census at selected cut")
            expected=[[0,0] for _ in range(8)]
            for native_key,body in spool.walk(b"fdb-old" if name=="old" else b"fdb-current"):
                group=0 if native_key.startswith(prefix(c,0)) else 1
                logical=bytes.fromhex(native_key.split(b"/current/",1)[1].decode()).decode()
                value=decoded(body)
                if logical.startswith(("out/","in/")):
                    predicate={"Kind":logical.split("/",1)[0],"Equals":value["Value"]}
                    require(predicate in PREDICATES,"unsupported correction scalar value")
                    index=PREDICATES.index(predicate)
                    require(spool.lookup(b"index-match-"+name.encode(),native_key)==u64(index),"every actual selected native posting present, no omitted/extra/wrong-value index row")
                    expected[index][group]+=1
            require(counts==expected and counts[2]==counts[5]==[0,0],"exact old/current/phantom predicate sets")
            # Source values are independently derived from original COMMITs.
            # This extra exact cohort check proves a real correction: ALL old5;
            # precisely the FIRST half6 now, and the OTHER half still5.
            for fact in range(p["facts"]):
                command=source.codec.fdb_contract(p,4+fact)[0]
                edge=command["Edge"]
                logical="edge/"+"/".join(f"{n:016x}" for n in (edge["ID"],edge["Source"]["Node"],edge["Source"]["Life"],edge["Target"]["Node"],edge["Target"]["Life"]))
                body=spool.lookup(b"fdb-old" if name=="old" else b"fdb-current",key(c,0,"current",logical))
                require(decoded(body)["Value"]==(6 if name=="final" and fact<p["facts"]//2 else 5),"divergent corrected/unchanged cohort, current substitution must fail")
            source.work(native["Work"],end["Binding"]["Read"],"Fresh")
            before=decoded(spool.lookup(b"source-timing",b"held-old-after-update"))["End"]["Work"] if name=="old" else decoded(spool.lookup(b"source-points",b"final"))["Work"]
            require(all(native["Work"][field]>before[field] for field in ("Rows","Bytes","Callbacks")) and
                    all(native["Work"][field]==before[field] for field in ("OpeningRows","OpeningBytes","OpeningCallbacks","CleanupRows","CleanupBytes","CleanupCallbacks")),"same source owner cumulative index debit cannot reset or reacquire an allowance")
            integer(frame["Nanos"],1,(1<<63)-1)
            timing.append({"call":"IndexScan","name":name,"cut":native["Cut"],"predicates":PREDICATES,"matches":counts,"sourceRows":native["Rows"],"nanos":frame["Nanos"],"access":ACCESS})
            stage+=1;previous=None;counts=[[0,0] for _ in range(8)]
        else:
            require(False,"unknown index artifact")
    require(started and footer and stage==2,"partial/missing index EOF cannot complete family")
    participants=[0,0]
    for ordinal,body in spool.walk(b"fdb-operation"):
        if int.from_bytes(ordinal,"big")<=3: continue
        group=decoded(body)
        actual=group["Tx"]["Participants"]
        require(len(actual) in (1,2) and all(part["Reads"] and part["Effects"] for part in actual),"actual business footprint, not endpoint labels or empty coordinator participation")
        participants[len(actual)-1]+=1
    useful=p["facts"]+p["facts"]//2
    require(sum(participants)==useful and participants[1]*100==useful*p["crossPercent"],"intended cross transaction percentage must match actual native business participants")
    values=sorted(sample["nanos"] for sample in timing)
    return {"indexTiming":timing,"indexNearestRank":{"samples":2,**{f"p{p}Nanos":values[(p*2+99)//100-1] for p in (50,95,99)}},
            "businessParticipants":{"setupTransactionsExcluded":3,"logicalRemoteEdgeIntentPercent":p["crossPercent"],"usefulTransactions":useful,"actualSinglePartition":participants[0],"actualCrossPartition":participants[1],"scope":"authentic native read/effect participants; physical SDK/server messages are separate transport evidence"}}


def verify(directory,spool_directory,policy=None,*,shared_spool=None):
    return shared.verify(directory,spool_directory,policy,FAMILY,shared_spool=shared_spool)


if __name__=="__main__":
    try:
        if len(sys.argv) not in (3,5):raise ValueError("usage: fdb_correction_check.py ARTIFACTS OWNED_SPOOL_DIR [DISK_BYTES CACHE_BYTES]")
        policy=None if len(sys.argv)==3 else source.Policy(int(sys.argv[3]),int(sys.argv[4]),4096,source.raw.LINE)
        print(json.dumps(verify(sys.argv[1],sys.argv[2],policy),indent=2))
    except (ValueError,KeyError,TypeError,OSError) as error:
        print("correction evidence refused: "+str(error),file=sys.stderr);sys.exit(1)
