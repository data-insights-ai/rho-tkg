#!/usr/bin/env python3
"""Private complete immutable-column workload oracle; no capacity acceptance.

The shared raw/typed codecs authenticate native bytes. This family has its own
schedule, filename namespace and retained source observations, not synthetic
Fresh/Stable/After observations borrowed from a selector receipt. All graph-sized
state occupies one caller-granted disposable comparison spool.
"""
import hashlib
import json
from pathlib import Path
import sys

import fdb_selectors_check as source

raw, codec = source.raw, source.codec
require, fields, integer = source.require, source.fields, source.integer
same, go_json, blob, decoded = source.same, source.go_json, source.blob, source.decoded
key, prefix, u64 = source.key, source.prefix, source.u64
FAMILY, FORMAT = "immutable-columns", "fdb-immutable-columns-v1"
OBSERVATIONS = ("old-fresh", "held-old-after-update", "fresh-current")


def validate_result(end):
    version = integer(end["Version"], 1, 2)
    fields(end, "Version Format Spec Config Binding BindingRecord Initial Old Final Attempts Aborted Committed Setup Creates Updates Observations Clients Sources AttemptSHA256 EventSHA256" + (" Columns" if "Columns" in end else "") + (" Transport" if version == 2 else ""))
    require(end["Format"] == FORMAT.replace("-v1", f"-v{version}"), "dedicated immutable-column result")
    p, c = end["Spec"], end["Config"]
    fields(p, "version family facts crossPercent writers layout")
    n = integer(p["facts"], 20, 1_000_000)
    require(type(p["version"]) is int and p["version"] == version and p["family"] == FAMILY and n % 20 == 0 and type(p["writers"]) is int and p["writers"] in (1, 8, 32) and n > p["writers"] and type(p["crossPercent"]) is int and p["crossPercent"] in (0, 10, 50, 100) and p["layout"] in ("kv", "columns"), "actual family dimensions")
    graph_prefix = f"graph-workload-columns-v{version}-{p['layout']}-"
    run = c["Graph"].removeprefix(graph_prefix)
    require(c["Graph"].startswith(graph_prefix) and len(run) == 32 and bytes.fromhex(run).hex() == run and bytes.fromhex(run) != bytes(16), "fresh native graph/run identity")
    originals = 3 + 2*n*64
    expected = {"Format": 2, "Graph": c["Graph"], "Topology": 1, "Epochs": [7, 11],
                "Limits": {"Bytes": (64 << 20) + n*(64 << 10) + originals*2048, "Transactions": originals, "Versions": 3+6*n, "Certificates": 1024},
                "Nodes": [1, 7+2*n], "Cost": {"version": 6, "facts": n, "crossPercent": p["crossPercent"], "writers": p["writers"]}, "CostScope": codec.SCOPE}
    require(same(c, expected), "fresh Cost6 cannot reinterpret old persisted profiles")
    binding = {"Version": 1, "Config": c, "Dataset": list(hashlib.sha256(b"\x01"+u64(n)+u64(p["crossPercent"])+b"graph-v2").digest()),
               "Window":32,"Attempts":64,"Submissions":8,"Callbacks":16,
               "Read":{"PageRows":8,"PageBytes":4<<20,"InventoryBytes":4<<20,"WorkRows":1024*n+4096,"WorkBytes":(64<<20)+n*(2<<20),"ChainSteps":128,
                       "Cleanup":{"Rows":1024,"Bytes":64<<20,"Millis":30000}},
               "History":{"MaxOwners":64,"MaxControlBytes":4<<20,"PageRows":32,"PageBytes":4<<20,"MaxWorkRows":32*n+4096,"MaxWorkBytes":(64<<20)+n*(1<<20),"MaxChainSteps":128,"MaxOutputBytes":4096}}
    if version == 2:
        binding.update(Version=2, Admission=raw.transport.family_record(end["Transport"],p,c))
    require(same(end["Binding"],binding), "authentic bounded native work/cleanup admission")
    source.raw_record(end["BindingRecord"],key(c,0,f"workload-columns-v{version}","binding"),go_json(binding))
    for name, groups in (("Initial",[0,0]),("Old",[2+n,1]),("Final",[2+2*n,1])):
        cut=end[name]
        fields(cut,"Graph Topology Epochs Scope Round Groups")
        require(cut["Graph"]==c["Graph"] and cut["Topology"]==1 and cut["Epochs"]==c["Epochs"] and blob(cut["Scope"])==b"\0\1" and cut["Groups"]==groups, "complete native cut membership")
        integer(cut["Round"],1)
    require(end["Initial"]["Round"]<end["Old"]["Round"]<end["Final"]["Round"],"mutation then retained old-cut query")
    require(end["Setup"]==3 and end["Creates"]==end["Updates"]==n and end["Committed"]==3+2*n and end["Observations"]==3 and integer(end["Aborted"])+end["Committed"]==end["Attempts"]<=originals,"useful/ABORT/source observation census")
    require(type(end["Clients"]) is list and len(end["Clients"])==32 and all(type(v) is int and v>=2 for v in end["Clients"][:p["writers"]]) and all(type(v) is int and v==0 for v in end["Clients"][p["writers"]:]) and sum(end["Clients"])==2*n,"genuine independent callers in both phases")
    require(("Columns" in end)==(p["layout"]=="columns"),"KV source reference is not native columns")
    for digest in (end["AttemptSHA256"],end["EventSHA256"]):
        require(type(digest) is str and len(digest)==64 and bytes.fromhex(digest).hex()==digest,"canonical sealed digest")


def point_values(frame,end,spool):
    fields(frame,"Type Name Access Points Work Nanos")
    require(frame["Name"]=="fresh-current" and "not an index" in frame["Access"] and len(frame["Points"])==3,"fixed batched scan-based source probes")
    for index,point in enumerate(frame["Points"]):
        fields(point,"Key Found Value")
        fact=(0,end["Spec"]["facts"]-1,end["Spec"]["facts"]+16)[index]
        target=end["Config"]["Nodes"][1] if (fact+1)*end["Spec"]["crossPercent"]//100>fact*end["Spec"]["crossPercent"]//100 else 3
        logical="edge/"+"/".join(f"{n:016x}" for n in (7+2*fact,1,2,target,target+1))
        require(point["Key"]=={"Group":0,"Key":logical} and type(point["Found"]) is bool and point["Found"]==(index<2),"point/phantom exact identity")
        expected=decoded(spool.lookup(b"fdb-current",key(end["Config"],0,"current",logical))) if index<2 else {"Value":0,"Version":0,"Round":0,"Deleted":False,"TxID":""}
        require(point["Value"]==expected,"source probe exact retained revision or exact empty")
    source.work(frame["Work"],end["Binding"]["Read"],"Fresh")


def timing(frame,end,spool):
    fields(frame,"Type Call Name Access Cut Rows Nanos")
    require(end["Spec"]["layout"]=="columns" and frame["Access"]=="complete actual caller operation through EOF including retry/wait and artifact IO; scan-based points are not indexed latency","actual complete-call timing")
    call,name=frame["Call"],frame["Name"]
    allowed={ ("PublishAndHold","old"),("Publish","final"),*( ("Cache",n) for n in ("old","final")),*( ("Walk",n) for n in ("old","old-cache","final","final-cache")),*( ("ScanPoint",f"{n}-{owner}") for n in ("old","final") for owner in (7,7+2*(end["Spec"]["facts"]-1),5)) }
    require((call,name) in allowed,"unknown or relabeled column call")
    generation="old" if name.startswith("old") else "final"
    require(frame["Cut"]==end[generation.title()] and type(frame["Rows"]) is int and frame["Rows"]==(3+end["Spec"]["facts"] if generation=="old" else end["Committed"]),"timing binds actual generation cut/cardinality")
    integer(frame["Nanos"],1,(1<<63)-1)
    spool.insert(b"column-timing",(call+"/"+name).encode(),go_json(frame))


def events(directory,end,spool,header):
    digest,started,footer=hashlib.sha256(),False,False
    active,selected,rows=None,None,[0,0]
    observations,completions,inventories,points,opens,timings=0,0,0,0,0,0
    inv_stage,inv_meta,inv_counts,previous=None,None,[[0,0],[0,0]],{}
    source_owner,closed=None,False
    c=end["Config"]
    for frame,line in source.lines(directory/"fdb-columns-events.jsonl",80+6*(3+3*end["Spec"]["facts"])+4*end["Attempts"]):
        require(not footer,"trailing family event")
        kind=frame.get("Type")
        if kind=="footer":
            require(active is None and inv_stage is None and closed and frame=={"Type":"footer","End":end} and digest.hexdigest()==end["EventSHA256"],"complete native/source close and event seal")
            footer=True
            continue
        digest.update(line)
        if not started:
            require(frame=={"Type":"header","Format":end["Format"],"Spec":end["Spec"],"Config":c,"Binding":end["Binding"],"Record":end["BindingRecord"],"Initial":end["Initial"]},"dedicated actual native admission header")
            started=True
        elif kind=="committed":
            fields(frame,"Type Operation Original Outcome")
            ordinal,_,_=source.operation(end["Spec"],frame["Operation"])
            body=spool.lookup(b"fdb-operation",u64(ordinal))
            actual=decoded(spool.lookup(b"useful-attempt",u64(ordinal)))
            require(frame["Original"]==actual["Original"] and blob(frame["Original"]["Value"])==body and frame["Outcome"]==decoded(body)["Outcome"],"complete actual business original")
            spool.insert(b"completion-seen",u64(ordinal),b"1")
            completions+=1
        elif kind=="selector-open":
            fields(frame,"Type Name Mode Access Cut Nanos")
            require(active is None and frame["Name"] in ("old-fresh","fresh-current","raw-source-held") and frame["Mode"]==("At" if frame["Name"]=="raw-source-held" else "Fresh") and frame["Access"]=="actual Open through retained selection/acquisition including retries/wait; no data rows read","actual source open, no synthetic selector schedule")
            integer(frame["Nanos"],1,(1<<63)-1)
            spool.insert(b"selector-open",frame["Name"].encode(),go_json(frame))
            opens+=1
        elif kind=="selection":
            fields(frame,"Type Name Selection Policy")
            require(active is None and observations<3 and frame["Name"]==OBSERVATIONS[observations] and frame["Policy"]==end["Binding"]["Read"],"three actual source observations only")
            active,selected=frame["Name"],frame["Selection"]
            source.selection(selected,active,end,spool)
            if active!="held-old-after-update":
                opening=decoded(spool.lookup(b"selector-open",active.encode()))
                require(opening["Cut"]==selected["Selected"] and opening["Mode"]=="Fresh","opening binds returned genuine source owner")
            rows=[0,0]
        elif kind=="snapshot-row":
            fields(frame,"Type Name Row")
            require(active==frame["Name"],"source rows require an acquired owner")
            source.snapshot_row(frame["Row"],selected["Selected"],end,spool,active)
            rows[frame["Row"]["Group"]]+=1
        elif kind=="snapshot-eof":
            fields(frame,"Type Name Access End Nanos")
            require(active==frame["Name"] and frame["End"]["Cut"]==selected["Selected"] and frame["End"]["Rows"]==rows and "not indexed" in frame["Access"],"authentic source EOF")
            fields(frame["End"],"Cut Rows DirectoryRows OriginalEffects Work")
            expected=[0,0]
            phase="final" if active=="fresh-current" else "old"
            for native,body in spool.walk(b"fdb-current" if phase=="final" else b"fdb-old"):
                require(spool.lookup(b"snapshot-seen-"+active.encode(),native)==body,"complete exact source answers at retained cut")
                expected[0 if native.startswith(prefix(c,0)) else 1]+=1
            require(rows==expected==frame["End"]["DirectoryRows"] and frame["End"]["OriginalEffects"]==decoded(spool.lookup(b"source-count",phase.encode()))[1],"no omitted/extra source key or physical original")
            source.work(frame["End"]["Work"],end["Binding"]["Read"],"Fresh")
            integer(frame["Nanos"],1,(1<<63)-1)
            spool.insert(b"source-timing",active.encode(),go_json(frame))
            observations+=1;active=None
        elif kind=="points-eof":
            require(observations==3 and points==0,"one final fixed source probe call")
            point_values(frame,end,spool)
            integer(frame["Nanos"],1,(1<<63)-1)
            spool.insert(b"source-points",b"final",go_json(frame));points+=1
        elif kind=="column-timing":
            timing(frame,end,spool);timings+=1
        elif kind=="originals-header":
            fields(frame,"Type Stage Cut Metadata Records")
            require(inv_stage is None and inventories<2 and frame["Stage"]==("old","final")[inventories] and frame["Cut"]==end["Old" if inventories==0 else "Final"],"drained actual native inventory barrier")
            inv_stage,inv_meta,inv_counts,previous=frame["Stage"],frame["Metadata"],[[0,0],[0,0]],{}
            for g in range(2):
                source.raw_record(frame["Records"][g],key(c,g,"meta","state"),go_json(inv_meta[g]))
                require(inv_meta[g]["Groups"]==frame["Cut"]["Groups"][g] and inv_meta[g]["Floor"]==frame["Cut"]["Round"],"actual phase watermark")
            spool.insert(b"barrier-metadata",inv_stage.encode(),go_json(inv_meta))
        elif kind=="original":
            fields(frame,"Type Stage Kind Group Records")
            require(inv_stage==frame["Stage"] and frame["Kind"] in ("id","request"),"actual paired original inventory")
            index,g=("id","request").index(frame["Kind"]),integer(frame["Group"],0,1)
            native=blob(frame["Records"][0]["Key"])
            require((index,g) not in previous or previous[index,g]<native,"ordered unique original prefix")
            previous[index,g]=native
            require(len(frame["Records"])==2,"both actual original identities")
            outcome=decoded(blob(frame["Records"][0]["Value"]))
            require(outcome["Coordinator"]==g,"actual original coordinator prefix")
            for record,kind in zip(frame["Records"],(frame["Kind"],"request" if index==0 else "id"),strict=True):
                source.raw_record(record,key(c,g,kind,outcome["ID" if kind=="id" else "Request"]),go_json(outcome))
                require(blob(record["Value"])==spool.lookup(b"attempt-"+kind.encode(),blob(record["Key"])),"actual original equals preserved immutable attempt")
                if inv_stage=="old":
                    require(int.from_bytes(spool.lookup(b"attempt-membership-"+kind.encode(),blob(record["Key"])),"big")<=3+end["Spec"]["facts"],"no post-barrier original in old generation")
            spool.insert(b"inventory-"+inv_stage.encode()+b"-"+frame["Kind"].encode(),native,blob(frame["Records"][0]["Value"]))
            inv_counts[index][g]+=1
        elif kind=="originals-eof":
            fields(frame,"Type Stage Rows Metadata")
            require(inv_stage==frame["Stage"] and inv_counts==frame["Rows"] and inv_meta==frame["Metadata"] and inv_counts[0]==inv_counts[1]==[m["Transactions"] for m in inv_meta]==decoded(spool.lookup(b"original-cost",inv_stage.encode()))[0],"complete native original pairs including ABORTs")
            inventories+=1;inv_stage=None
        elif kind=="source-held":
            fields(frame,"Type Selection Policy")
            require(observations==3 and points==1 and inventories==2 and source_owner is None and frame["Policy"]==end["Binding"]["Read"],"actual raw-source acquisition before data")
            source.selection(frame["Selection"],"raw-source-held",end,spool)
            source_owner=decoded(blob(frame["Selection"]["AcquiredOwner"]["Value"]))
            sample=decoded(spool.lookup(b"selector-open",b"raw-source-held"))
            require(sample["Cut"]==end["Final"] and sample["Mode"]=="At","actual raw-source open timing")
        elif kind=="source-closed":
            fields(frame,"Type Access Owner Work")
            require(source_owner is not None and not closed and frame["Access"]=="Work covers selector opening/cleanup only; complete raw history uses the separately bound History policy","separate source walk work admission")
            source.raw_record(frame["Owner"],prefix(c)+b"source-owner-v1/"+source_owner["Request"].encode().hex().encode(),go_json(source_owner|{"State":2,"TerminalRequest":"close"}))
            source.work(frame["Work"],end["Binding"]["Read"],"At")
            require(frame["Work"]["CleanupCallbacks"]>0,"authenticated source Close")
            closed=True
        else:
            require(False,"unknown family event")
    require(started and footer and closed and opens==3 and observations==3 and points==1 and inventories==2 and completions==end["Committed"] and timings==(14 if end["Spec"]["layout"]=="columns" else 0),"incomplete actual family lifecycle/timings")


def metadata(end,spool,header):
    c=end["Config"]
    maximum=dict.fromkeys(("Floor","Generation","Versions","Transactions","Certificates","Bytes","Groups"),(1<<64)-1)
    for phase,certificates in (("old",(end["Initial"],end["Old"])),("final",(end["Initial"],end["Old"],end["Final"]))):
        transactions,pair_bytes=decoded(spool.lookup(b"original-cost",phase.encode()))
        generations,versions=decoded(spool.lookup(b"source-count",phase.encode()))
        costs=decoded(spool.lookup(b"source-cost",phase.encode()))
        actual=decoded(spool.lookup(b"barrier-metadata",phase.encode()))
        expected=[]
        for g in range(2):
            size=len(key(c,g,"meta","config"))+len(go_json(c))+len(key(c,g,"meta","state"))+len(go_json(maximum))+pair_bytes[g]+sum(part[g] for part in costs)
            if g==0:
                size+=len(blob(end["BindingRecord"]["Key"]))+len(blob(end["BindingRecord"]["Value"]))
                size+=sum(len(key(c,0,"cut",go_json(cert).hex()))+len(go_json(cert)) for cert in certificates)
            expected.append({"Floor":end[phase.title()]["Round"],"Generation":generations[g],"Versions":versions[g],"Transactions":transactions[g],"Certificates":len(certificates) if g==0 else 0,"Bytes":size,"Groups":end[phase.title()]["Groups"][g]})
        require(actual==expected,"exact native bytes/versions/originals/certificates, no ABORT omission")
        if phase=="final": require(actual==header["Metadata"],"raw final EOF native metadata")
    for name in (*OBSERVATIONS,"raw-source-held"):
        phase="old" if name in ("old-fresh","held-old-after-update") else "final"
        require(spool.lookup(b"selection-metadata",name.encode())==spool.lookup(b"barrier-metadata",phase.encode()),"actual opening metadata at exact generation barrier")


def verify(directory,spool_directory,policy=None):
    directory=Path(directory).resolve(strict=True)
    with (directory/"fdb-columns-result.json").open("rb") as stream:
        body=stream.read(raw.LINE+1)
    require(len(body)<=raw.LINE and body.endswith(b"\n"),"bounded exclusive result")
    end=decoded(body[:-1]);validate_result(end)
    require(policy is not None or end["Spec"]["facts"]<1_000_000 and end["Spec"]["writers"]!=32,"large cell requires explicit finite comparison grant")
    policy=source.POLICY if policy is None else policy
    with source.Spool(spool_directory,policy) as spool:
        source.attempts(directory,end,spool,"fdb-columns-attempts.jsonl","columns")
        header=source.source_fold(directory,end,spool,"fdb-columns")
        events(directory,end,spool,header)
        metadata(end,spool,header)
        source.columns(directory,end,spool,"fdb-columns-columns.jsonl")
        source.latencies(directory,end,spool,"fdb-columns-latency.jsonl")
        transport=raw.transport.verify(directory,end["Transport"]["Policy"],spool) if end["Version"]==2 else None
        # Fixed <=14 column calls and six source calls, not per-row samples.
        samples=[decoded(value) for _,value in spool.walk(b"column-timing")]
        source_samples=[]
        for name in OBSERVATIONS:
            scan=decoded(spool.lookup(b"source-timing",name.encode()))
            source_samples.append({"Call":"SourceScan","Name":name,"Cut":scan["End"]["Cut"],"Rows":scan["End"]["Rows"],"Nanos":scan["Nanos"]})
            if name!="held-old-after-update":
                opening=decoded(spool.lookup(b"selector-open",name.encode()))
                source_samples.append({"Call":"SourceOpen","Name":name,"Cut":opening["Cut"],"Rows":scan["End"]["Rows"],"Nanos":opening["Nanos"]})
        point=decoded(spool.lookup(b"source-points",b"final"))
        source_samples.append({"Call":"SourceScanPoints","Name":"fresh-current","Cut":end["Final"],"Probes":3,"Nanos":point["Nanos"]})
        percentiles=[]
        for call in sorted({v["Call"] for v in (*samples,*source_samples)}):
            values=sorted(v["Nanos"] for v in (*samples,*source_samples) if v["Call"]==call)
            percentiles.append({"call":call,"samples":len(values),**{f"p{p}Nanos":values[(p*len(values)+99)//100-1] for p in (50,95,99)}})
        report=spool.report()
    out={"format":end["Format"],"family":FAMILY,"spec":end["Spec"],"runGraph":end["Config"]["Graph"],"attempts":end["Attempts"],"committed":end["Committed"],"aborted":end["Aborted"],"spool":report,"columnTiming":samples,"sourceTiming":source_samples,"nearestRankTiming":percentiles,
         "scope":"one actual source-scan reference or genuine immutable-column publication/native+cache scan lifecycle; source KV retain-all, derived GC only; no native parity/full matrix/capacity acceptance"}
    if transport is not None:
        transport["nativeRetainedBindingBytes"]=len(blob(end["Transport"]["Record"]["Key"]))+len(blob(end["Transport"]["Record"]["Value"]))
        out["transport"]=transport
    return out


if __name__=="__main__":
    try:
        if len(sys.argv) not in (3,5): raise ValueError("usage: fdb_columns_check.py ARTIFACTS OWNED_SPOOL_DIR [DISK_BYTES CACHE_BYTES]")
        policy=None if len(sys.argv)==3 else source.Policy(int(sys.argv[3]),int(sys.argv[4]),4096,raw.LINE)
        print(json.dumps(verify(sys.argv[1],sys.argv[2],policy),indent=2))
    except (ValueError,KeyError,TypeError,OSError) as error:
        print("immutable-columns evidence refused: "+str(error),file=sys.stderr);sys.exit(1)
