#!/usr/bin/env python3
"""Actual native publication Sync, compaction boundary and old-cut recovery.

This does not invent a second application checkpoint Sync, new graph certificate,
portable native cut or transfer from process-local diagnostic observations.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path

SPEC=importlib.util.spec_from_file_location("publication_state_owner",Path(__file__).with_name("native_transition_state_check.py"))
S=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(S)
T,C,P,F=S.T,S.C,S.P,S.F
require,integer,fixed=T.require,T.integer,T.fixed
PROFILE="native-actual-publication-sync-process-v1"
HEADER=T.HEADER|set("Side Run DeadlineUnixNanos Arm State BeforeStorage RestoredStorage ReceiptReads ReceiptGrantBytes".split())


def load(path):
    digest,total,header,footer,records=hashlib.sha256(),0,None,None,[]
    with path.open("rb") as source:
        while line:=source.readline((512<<10)+1):
            total+=len(line)
            require(len(line)<=512<<10 and total<=16<<20 and line.endswith(b"\n"),"publication artifact admission")
            row=T.decoded(line)
            require(type(row) is dict and footer is None,"publication row after EOF")
            if header is None:
                require(set(row)=={"Type","Artifact"} and row["Type"]=="header","publication header row")
                header=row["Artifact"]
                require(type(header) is dict and set(header)==HEADER and header["Records"] is None,"fresh publication header")
            elif row.get("Type")=="record":
                require(set(row)=={"Type","Record"} and len(records)<512,"publication complete-prefix admission")
                records.append(row["Record"])
            else:
                require(set(row)=={"Type","Rows","Through","PayloadSHA256"} and row["Type"]=="footer","publication EOF row")
                require(type(row["Rows"]) is int and row["Rows"]==len(records) and row["PayloadSHA256"]==digest.hexdigest(),"publication EOF hash/census")
                footer=row
            if footer is None:
                digest.update(line)
    require(header is not None and footer is not None and footer["Through"]==header["Through"],"missing/foreign publication EOF")
    header["Records"]=records
    return header


def storage(wire):
    response=F.process(P.raw(wire))
    require(response["code"]==response["mask"]==0 and not response["outcome"] and not response["changes"] and not response["packets"] and not response["reads"],"publication diagnostic contains native refusal or business output")
    value=T.decoded(response["aux"])
    require(type(value) is dict and set(value)=={"FirstIndex","LastIndex","SnapshotOffers"},"actual bounded native prefix diagnostic")
    bounds(value)
    return response,value


def bounds(value):
    require(type(value) is dict and set(value)=={"FirstIndex","LastIndex","SnapshotOffers"},"native prefix grammar")
    first,last=integer(value["FirstIndex"],minimum=1),integer(value["LastIndex"],(1<<64)-2)
    require(first<=last+1 and integer(value["SnapshotOffers"],2)>=0,"invalid native prefix/offers")


def boundary(before,restored,index,side):
    bounds(before)
    bounds(restored)
    integer(index,(1<<64)-2,1)
    require(side in ("before","after") and before["FirstIndex"]<=index and restored["LastIndex"]>=before["LastIndex"],"publication requires an actual uncompacted prefix")
    expected=index+1 if side=="after" else before["FirstIndex"]
    require(restored["FirstIndex"]==expected,"wrong actual before/after Sync compaction boundary")


def compaction(before,restored,ident,side):
    old,a=storage(before)
    local,b=storage(restored)
    require(old["stats"][1]==ident["Applied"]==ident["SourceIndex"]==local["stats"][1] and old["applied"]==ident["Applied"] and local["applied"]>=ident["Applied"],"actual captured checkpoint/applied equality")
    boundary(a,b,ident["SourceIndex"],side)
    require(hashlib.sha256(old["image"]).digest()==fixed(ident["ImageSHA256"],32,True)==fixed(ident["SourceImageSHA256"],32,True) and local["image"]==old["image"],"native publication changed captured reducer image")
    require(fixed(ident["SourceCutID"],32,True)==fixed(ident["CurrentPublicationID"],32,True) and not any(ident["ManifestID"]+ident["PlanID"]+ident["ProgressSHA256"]) and ident["ActiveBank"]==ident["TargetBank"] and ident["ActiveGeneration"]==ident["TargetGeneration"] and ident["Final"] is False and ident["Verified"] is False and ident["Sequence"]==ident["Section"]==0,"publication mislabeled import/retention/checkpoint")
    return old,local


def processes(rows,receipt,before,local):
    require(type(rows) is list and len(rows)==2,"actual publication self-kill/replacement pair")
    failed,restored=rows
    raw=T.decoded(P.raw(failed["Raw"]))
    require(set(raw)==set("Member PID ExitCode SignalNumber Signaled Reaped Volume Stderr Receipt".split()) and raw["Receipt"]==receipt and failed["Action"]=="native-state-transition-kill" and failed["Member"]==raw["Member"]==receipt["Member"]==1 and raw["PID"]==receipt["PID"] and raw["Signaled"] is True and raw["SignalNumber"]==9 and raw["ExitCode"]==-1 and raw["Reaped"] is True and failed["Reaped"] is True and failed["Signal"]=="SIGKILL" and failed["Volume"]==raw["Volume"],"actual selected native publication self-SIGKILL")
    recovered=T.decoded(P.raw(restored["Raw"]))
    require(restored["Action"]=="reopen" and restored["Ready"] is True and restored["Member"]==recovered["Member"]==1 and recovered["PID"]!=receipt["PID"] and restored["PreviousInstance"]==recovered["PreviousInstance"]==failed["Instance"] and restored["Volume"]==recovered["Volume"]==failed["Volume"] and restored["RecoveryRaw"]==recovered["RecoveryRaw"],"same-directory native publication replacement")
    startup=F.process(P.raw(recovered["LocallyRestoredRaw"]))
    final=F.process(P.raw(recovered["RecoveryRaw"]))
    require(before["incarnation"]!=local["incarnation"]==startup["incarnation"]==final["incarnation"] and before["stats"][1]==startup["stats"][1] and final["applied"]>=local["applied"],"native pre-heal local diagnostic/process lineage")


def check(data):
    require(type(data) is dict and set(data)==HEADER and type(data["Version"]) is int and data["Version"]==5 and data["Profile"]==PROFILE,"fresh publication artifact")
    side=data["Side"]
    require(side in ("before","after") and data["Case"]=="state-publication-"+side,"finite actual publication inventory")
    require(data["IO"] is None and not any(data[k] for k in ("PartialReads","PacketEvents","Refused","RefusedReplay")) and not P.raw(data["PendingCaller"]),"foreign refusal/network case")
    for k in ("InstalledA","WithheldB"):
        require(type(data[k]) is dict and set(data[k])=={"Request","Reply"} and not any(P.raw(v) for v in data[k].values()),"foreign phase case")
    run,deadline=fixed(data["Run"],16,True),integer(data["DeadlineUnixNanos"],(1<<63)-1,1)
    require(data["Arm"]["Control"]["Kind"]==2 and data["Arm"]["Control"]["Side"]==side and data["Arm"]["Sequence"]==1,"actual publication selector")
    require(integer(data["ReceiptReads"],129,1)*131072==integer(data["ReceiptGrantBytes"],129*131072,131072),"publication receipt authentication funding")
    ident=S.pair(data["Arm"],data["State"],run,deadline,member=1)
    before,local=compaction(data["BeforeStorage"],data["RestoredStorage"],ident,side)
    processes(data["Processes"],data["State"],before,local)
    require(any(r["Partition"]==3 and r["Index"]==ident["SourceIndex"] for r in data["Records"]),"publication checkpoint absent from actual complete application prefix")
    result=C.check_history(data,process_check=lambda rows,io,case:processes(rows,data["State"],before,local),semantic_require=T.semantic)
    T.literal_edge_answers(data)
    result["scope"]="actual native publication Commit(Sync)/compaction and same-original/local-old recovery; application checkpoint co-installs separately, no invented second Sync or whole-D qualification"
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts",nargs="+",type=Path)
    args=parser.parse_args()
    require(len(args.artifacts)<=2,"finite publication artifact invocation")
    print(json.dumps([check(load(path)) for path in args.artifacts],indent=2))


if __name__=="__main__":
    main()
