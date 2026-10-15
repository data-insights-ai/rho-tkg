#!/usr/bin/env python3
"""Fresh detached FDB feed/physical-GC oracle; originals, not DTOs, are authority.

One caller-granted finite BLOB spool holds every growing join. Native physical
history, detached CDC, owner/cursor issuance and conservative native rolling
anchors are distinct costs/availability. This module executes no engine.
"""
from contextlib import nullcontext
import hashlib
import json
import os
from pathlib import Path
import struct
import sys

import fdb_feed_check as feed
from comparison_spool import Spool, Policy, u64

raw, codec, transport = feed.raw, feed.codec, feed.transport
require, fields, integer, same, go_json, blob = feed.require, feed.fields, feed.integer, feed.same, feed.go_json, feed.blob
decoded, lines, key, prefix = feed.decoded, feed.lines, feed.key, feed.prefix
FORMAT = "fdb-snapshot-feed-retained-only-workload-v1"
CONTRACT = "snapshot-relinquished-complete-feed-v1"
STAGES = ("snapshot", "before-gc", "after-gc", "checkpoint", "final")
LINE = 512 << 10


def shape(s):
    fields(s,"version family facts crossPercent writers layout")
    require(integer(s["version"],3,3) and s["family"]=="snapshot-to-feed" and integer(s["facts"],20,1_000_000)%20==0 and integer(s["writers"],1,32) in (1,8,32) and s["facts"]>s["writers"] and integer(s["crossPercent"]) in (0,10,50,100) and s["layout"] in ("kv","columns"),"exact fresh retained-only dimensions")
    return s


def validate_grant(v,s):
    shape(s);fields(v,"MaxRecords MaxControlBytes MaxGrants MaxBlock Census")
    n=s["facts"];blocks=integer(v["MaxGrants"],4,64);block=integer(v["MaxBlock"],8,1<<20)
    require(block%2==0 and (2*n+6+block-1)//block+(2+block-1)//block<=blocks,"genuine finite HOME/recipient grant geometry")
    records=integer(v["MaxRecords"],2*(3+2*n)*64+2*n+4*blocks+64,1<<32)
    require(records<=integer(v["MaxControlBytes"],512<<10,1<<50)//128,"declared native owner byte grant")
    c=v["Census"];fields(c,"PageRows PageBytes HeadRows HeadBytes MaxRows MaxBytes MaxCalls MaxWorkRows MaxWorkBytes")
    integer(c["PageRows"],1,128);integer(c["PageBytes"],64<<10,4<<20);integer(c["HeadRows"],8,512);integer(c["HeadBytes"],4096,48<<10)
    rows=integer(c["MaxRows"],33+21*n,1<<40);size=integer(c["MaxBytes"],1<<20,1<<55);integer(c["MaxCalls"],8,1<<32);integer(c["MaxWorkRows"],rows,1<<48);integer(c["MaxWorkBytes"],size,1<<62)
    return {**{k:v[k] for k in ("MaxRecords","MaxControlBytes","MaxGrants","MaxBlock")},"Census":{k:c[k] for k in "PageRows PageBytes HeadRows HeadBytes MaxRows MaxBytes MaxCalls MaxWorkRows MaxWorkBytes".split()}}


def ns(name): return b"fr/"+name.encode()
def put(spool,name,k,v):
    prior=spool.find(ns(name),k)
    if prior is None:spool.insert(ns(name),k,v)
    else:spool.replace(ns(name),k,prior,v)
def native(spool,stage,k):return spool.lookup(ns("native/"+stage),k)
def owner_key(c,kind,request):return key(c,0,"owner-catalog-v1",kind+"/"+request)
def feed_key(c,kind,request):return key(c,0,"feed-v1/"+kind,request)
def history_key(c,g,logical,v):return prefix(c,g)+f'history/{v["Round"]:016x}/'.encode()+logical.encode().hex().encode()
def zero_outcome():return {"Commit":False,"Round":0,"Digest":[0]*32,"ID":"","Request":"","Reason":"","Coordinator":0,"Sequence":0}


def caller_census(s,values):
    require(type(values) is list and len(values)==32 and all(type(v) is int for v in values),"strict real caller counters")
    for client,count in enumerate(values):
        expected=2*((s["facts"]-1-client)//s["writers"]+1) if client<s["writers"] else 0
        require(count==expected,"every exact fact%W caller, not merely positive counts")


def result(directory):
    body=transport.admitted(Path(directory)/"fdb-feed-retained-end.json",LINE)
    e=decoded(body);fields(e,"Version Format Spec Config Transport Catalog Initial Snapshot BeforeGC Checkpoint Final Original Token Transition Retired Clients Attempts Aborted Committed Issued Pages FeedRows SetupNanos BusinessNanos TotalNanos Business Processes KilledReaped ReplacementReaped EventSHA256 Retention Allocator");s=shape(e["Spec"]);n=s["facts"];c=e["Config"];p=transport.validate_policy(e["Transport"]["Policy"])
    require(e["Version"]==1 and type(e["Version"]) is int and e["Format"]==FORMAT and p["Version"]==3 and p["Spec"]==s,"new format/profile only")
    transport.family_record(e["Transport"],s,c)
    originals=(3+2*n)*64
    require(same(c,{"Format":2,"Graph":c["Graph"],"Topology":1,"Epochs":[7,11],"Limits":{"Bytes":(64<<20)+n*(64<<10)+originals*2048,"Transactions":originals,"Versions":3+6*n,"Certificates":1024},"Nodes":[1,7+2*n],"Cost":{"version":10,"facts":n,"crossPercent":s["crossPercent"],"writers":s["writers"]},"CostScope":codec.SCOPE}),"exact fresh native graph/limits")
    b=e["Catalog"];fields(b,"Version Config Source Authorities Policy Transfer");t=b["Transfer"]
    grant=validate_grant({k:t[k] for k in ("MaxRecords","MaxControlBytes","MaxGrants","MaxBlock","Census")},s)
    require(b["Version"]==2 and b["Config"]==t["Config"]==c and t["Version"]==3 and t["Admission"]==p and b["Policy"]=={k:grant[k] for k in ("MaxRecords","MaxControlBytes")} and all(t[k]==p[k] for k in ("Run","BudgetSeconds","AdmittedUnixNanos","DeadlineUnixNanos")),"same genuine catalog2/Transfer3 admission")
    require(all(type(t[k]) is int and t[k]==v for k,v in {"MaxProposalBytes":32768,"MaxAttempts":64,"MaxSubmissions":8,"MaxCallbacks":16,"CleanupMillis":30000,"MaxCleanupCalls":8}.items()),"original and terminal work never reset")
    require([v["ID"] for v in b["Authorities"]]==["retained-owner-0","retained-owner-1"] and all(any(codec.sha_array(v["SecretHash"])) for v in b["Authorities"]),"actual original processing principals")
    history={"MaxOwners":64,"MaxControlBytes":4<<20,"PageRows":32,"PageBytes":4<<20,"MaxWorkRows":32*n+4096,"MaxWorkBytes":(64<<20)+n*(1<<20),"MaxChainSteps":128,"MaxOutputBytes":4096}
    require(b["Source"]=={"Version":1,"Config":c,"Initial":e["Initial"],"Policy":history},"same finite native source binding")
    for name,groups in (("Initial",[0,0]),("Snapshot",[n+2,1]),("BeforeGC",[n+n//2+2,1]),("Checkpoint",[n+n//2+3,1]),("Final",[2*n+2,1])):
        v=e[name];fields(v,"Graph Topology Epochs Scope Round Groups")
        require(v["Graph"]==c["Graph"] and v["Topology"]==1 and v["Epochs"]==[7,11] and blob(v["Scope"])==b"\0\1" and same(v["Groups"],groups) and integer(v["Round"],1),"authentic complete cut vector")
    require(e["Initial"]["Round"]<e["Snapshot"]["Round"]<e["BeforeGC"]["Round"]<e["Checkpoint"]["Round"]<e["Final"]["Round"],"real mutations and post-GC checkpoint")
    require(integer(e["Committed"],3+2*n,3+2*n)+integer(e["Aborted"],0,originals)==integer(e["Attempts"],3+2*n,originals) and integer(e["Issued"],2*n+8,2*n+8),"complete useful/attempt/actual Next census")
    caller_census(s,e["Clients"])
    require(type(e["Business"]) is list and len(e["Business"])==4 and integer(e["SetupNanos"],1) and integer(e["BusinessNanos"],1) and integer(e["TotalNanos"],e["SetupNanos"]+e["BusinessNanos"]),"business excludes setup/diagnostics; monitor owns earlier initialization")
    return e


def stream(directory,end):
    # Native full-prefix snapshots dominate the declared output. This finite
    # frame ceiling is distinct from the artifact writer's actual byte cap.
    return lines(Path(directory)/"fdb-feed-retained-events.jsonl",64+32*end["Config"]["Limits"]["Transactions"]+128*end["Spec"]["facts"])


def inventory(directory,e,spool):
    h=hashlib.sha256();stage_index=group_index=0;previous=None;rows=size=0;header=False;count=0
    for f,line in stream(directory,e):
        require(len(line)<=256<<10,"actual finite producer frame")
        h.update(line);count+=1;k=f.get("Type")
        require(k in {"header","native","native-eof","grant-original","grant","next-node","next-value","next-edge","cursor-closed","prepared","submission","terminal","business-phase","published","columns-manifest","columns-row","columns-eof","columns-retire","cache-fill","feed-page","retained-only","source-collect","refusal","child-killed","child-replaced","snapshot-begin","snapshot-row","snapshot-eof"},"unknown event cannot be silently empty protocol")
        if not header:
            require(k=="header" and f["Header"]["Spec"]==e["Spec"] and f["Header"]["Catalog"]==e["Catalog"] and f["Header"]["Transport"]==e["Transport"] and f["Header"]["Initial"]==e["Initial"],"same immutable source header");header=True;continue
        if k=="native":
            require(stage_index<len(STAGES) and f["Stage"]==STAGES[stage_index] and integer(f["Group"],group_index,group_index)==group_index,"ordered native group/stage")
            r=f["Record"];fields(r,"Key Value");nk,nv=blob(r["Key"]),blob(r["Value"])
            require(0<len(nk)<=2048 and 0<len(nv)<=64<<10 and nk.startswith(prefix(e["Config"],group_index)) and (previous is None or previous<nk),"complete ordered native keys/no duplicate")
            spool.insert(ns("native/"+f["Stage"]),nk,nv);previous=nk;rows+=1;size+=len(nk)+len(nv)
            if f["Stage"]=="final" and nk.startswith(prefix(e["Config"],group_index)+b"changes/"):
                g=decoded(nv);o=g["Outcome"];require(o["Coordinator"]==group_index and integer(o["Sequence"],1),"native CDC coordinator")
                spool.insert(ns("cdc"),bytes([group_index])+u64(o["Sequence"]),nv)
        elif k=="native-eof":
            require(f["Stage"]==STAGES[stage_index] and f["Group"]==group_index and type(f["Rows"]) is int and type(f["Bytes"]) is int and (f["Rows"],f["Bytes"])==(rows,size),"native full prefix EOF/census")
            m=decoded(native(spool,f["Stage"],key(e["Config"],group_index,"meta","state")))
            require(same(m,f["Metadata"]) and native(spool,f["Stage"],key(e["Config"],group_index,"meta","config"))==go_json(e["Config"]),"actual native metadata/configuration")
            spool.insert(ns("native-eof"),bytes([stage_index,group_index]),go_json(f));group_index+=1
            if group_index==2:stage_index+=1;group_index=0
            previous=None;rows=size=0
    require(header and stage_index==len(STAGES) and group_index==0 and h.hexdigest()==e["EventSHA256"],"complete genuine stage EOFs/raw hash")
    c=e["Config"]
    require(native(spool,"final",owner_key(c,"binding","immutable"))==go_json(e["Catalog"]) and native(spool,"final",blob(e["Transport"]["Record"]["Key"]))==blob(e["Transport"]["Record"]["Value"]),"native owner/transport bytes, not caller labels")
    for name in ("Initial","Snapshot","BeforeGC","Checkpoint","Final"):
        require(native(spool,"final",key(c,0,"cut",go_json(e[name]).hex()))==go_json(e[name]),"actual certificate identity retained")


def chronology(events,e):
    # Fixed-size milestones bind the native deletion proof to the later unread
    # Page. Independently valid snapshots cannot prove this ordering alone.
    before=transition=collected=after=checkpoint=unread=killed=replaced=False
    deleted=[0,0];deleted_bytes=[0,0];calls=work_rows=work_bytes=0
    for f in events:
        k=f["Type"]
        if k=="feed-page" and f["Delivery"]["Page"]["Boundary"]==e["BeforeGC"]:
            require(not before and not unread and f["Delivery"]["Page"]["EOF"] is False and f["Delivery"]["Page"]["Rows"],"actual acknowledged unread prefix before collection")
            unread=True
        elif k=="native-eof" and f["Group"]==1:
            if f["Stage"]=="before-gc":require(unread and not before,"one pre-collection full native EOF");before=True
            elif f["Stage"]=="after-gc":require(collected and not after,"physical collection completes before post-GC native EOF");after=True
            elif f["Stage"]=="checkpoint":require(after and not checkpoint,"genuine post-GC checkpoint EOF");checkpoint=True
        elif k=="retained-only":
            require(before and not transition and f["Record"]==e["Transition"],"all-owner transition follows pre-GC EOF");transition=True
        elif k=="source-collect":
            require(transition and not collected and not after,"collector pages inside the actual irreversible transition")
            p=f["Progress"];fields(p,"Phase WorkRows WorkBytes Deleted DeletedBytes Done Seal")
            integer(p["Phase"],1,3);integer(p["WorkRows"],0,32);integer(p["WorkBytes"],0,4<<20)
            require(type(p["Done"]) is bool and type(p["Deleted"]) is list and type(p["DeletedBytes"]) is list and len(p["Deleted"])==len(p["DeletedBytes"])==2,"actual bounded physical collector page")
            for g in range(2):deleted[g]+=integer(p["Deleted"][g],0,32);deleted_bytes[g]+=integer(p["DeletedBytes"][g],0,4<<20)
            calls+=1;work_rows+=p["WorkRows"];work_bytes+=p["WorkBytes"]
            require(calls<=32*e["Spec"]["facts"]+4097,"finite original collection loop")
            if p["Done"]:
                require(same(p["Seal"],e["Retired"]) and deleted==e["Retired"]["Deleted"] and deleted_bytes==e["Retired"]["DeletedBytes"] and sum(deleted)>0,"actual completed deletion before unread continuation")
                collected=True
        elif k=="prepared" and f["Operation"]["Phase"]=="update" and f["Operation"]["Fact"]>=e["Spec"]["facts"]//2:
            require(after,"no checkpoint or later business write before actual post-GC EOF")
        elif k=="child-killed":require(checkpoint and not killed and f["Result"]["Delivery"]["Page"]["Rows"],"unread native seal occurs after physical GC and checkpoint EOF");killed=True
        elif k=="child-replaced":require(killed and not replaced,"replacement follows real pending seal/kill");replaced=True
    require(before and transition and collected and after and checkpoint and unread and killed and replaced,"complete ordered native collection/unread/replacement history")
    return {"calls":calls,"workRows":work_rows,"workBytes":work_bytes,"scope":"actual collector page work; separate from controller/SDK callback and monitor costs"}


def intent(value,e):
    fields(value,"Body Digest Routes Allocated");body=blob(value["Body"])
    require(0<len(body)<=32768 and codec.sha_array(value["Digest"])==hashlib.sha256(body).digest(),"canonical frozen intent/body/hash")
    x=decoded(body);fields(x,"Graph Topology ID Request Coordinator Dependency Participants GraphChange Lifecycle")
    require(body==go_json(x) and x["Graph"]==e["Config"]["Graph"] and x["Topology"]==1 and x["Request"]=="request/"+x["ID"] and x["Dependency"]==0 and value["Routes"]==[{"Owner":"retained-owner-0","Epoch":1},{"Owner":"retained-owner-1","Epoch":1}],"same immutable intent/native owner routes")
    command=x["GraphChange"]
    if command["Kind"]=="node-create":ids={command["Node"]["Node"],command["Node"]["Life"]}
    else:
        edge=command["Edge"];ids={edge["ID"],x["Lifecycle"]["EdgeLife"],edge["Source"]["Node"],edge["Source"]["Life"],edge["Target"]["Node"],edge["Target"]["Life"],x["Lifecycle"]["Definition"]["ID"]}
    require(type(value["Allocated"]) is list and len(value["Allocated"])<=8 and [v["ID"] for v in value["Allocated"]]==sorted(ids),"complete factory ID/life/value/endpoint claim footprint")
    return x,body


def attempts(directory,e,spool):
    n=e["Spec"]["facts"];counts=[0,0,0];clients=[0]*32;phase_bounds=[]
    for f,_ in stream(directory,e):
        k=f["Type"]
        if k=="prepared":
            ordinal,phase,fact=feed.operation(e["Spec"],f["Operation"]);client=integer(f["Client"],0,e["Spec"]["writers"]-1);number=integer(f["Attempt"],1,64)
            require(client==(0 if phase=="setup" else fact%e["Spec"]["writers"]),"exact independent writer assignment")
            x,body=intent(f["Intent"],e);command,lifecycle,effects,reads=codec.fdb_contract(e["Spec"],ordinal)
            require(x["ID"]==f"retained-{phase}-{fact}-c{client}-a{number}" and same(x["GraphChange"],command) and same(x["Lifecycle"],lifecycle) and [p["Group"] for p in x["Participants"]]==sorted(effects),"fixed useful footprint and complete business participants")
            for p in x["Participants"]:
                g=integer(p["Group"],0,1);require(p["Epoch"]==e["Config"]["Epochs"][g] and same(p["Effects"],effects[g]) and [r["Key"] for r in p["Reads"]]==reads[g],"genuine endpoint read/write footprint");integer(p["Generation"])
                for r in p["Reads"]:fields(r,"Key Version");integer(r["Version"])
            integer(f["CaptureCallbacks"],1,16)
            old=spool.find(ns("chain"),u64(ordinal));prior=None if old is None else decoded(old)
            require((prior is None and number==1 and f["Previous"]=="") or prior is not None and not prior["Proof"]["Outcome"]["Commit"] and number==prior["Attempt"]+1 and f["Previous"]==prior["Proof"]["Outcome"]["Request"],"new body only after genuine ABORT, unknown never replans")
            require(native(spool,"final",owner_key(e["Config"],"intent-request",x["Request"]))==go_json(f["Intent"])==native(spool,"final",owner_key(e["Config"],"intent-id",x["ID"])),"both actual native intent originals")
            spool.insert(ns("prepared"),x["Request"].encode(),go_json(f))
        elif k=="submission":
            q=decoded(spool.lookup(ns("prepared"),f["Request"].encode()));prior=spool.find(ns("same"),f["Request"].encode());expected=1 if prior is None else int.from_bytes(prior,"big")+1
            require(integer(f["Same"],1,8)==expected and type(f["SubmitError"]) is type(f["OriginalError"]) is str,"bounded SAME intent invocation chain")
            integer(f["Callbacks"],0 if f["SubmitError"] else 1,16);integer(f["ReadCallbacks"],0 if f["OriginalError"] else 1,16);put(spool,"same",f["Request"].encode(),u64(expected))
            if f["OriginalError"]=="":put(spool,"observed",f["Request"].encode(),go_json(f["Proof"]))
        elif k=="terminal":
            ordinal,phase,fact=feed.operation(e["Spec"],f["Operation"]);x,body=intent(f["Intent"],e);q=decoded(spool.lookup(ns("prepared"),x["Request"].encode()))
            require(all(same(f[name],q[name]) for name in ("Operation","Client","Attempt","Previous","Intent")) and spool.lookup(ns("observed"),x["Request"].encode())==go_json(f["Proof"]),"actual observed original belongs to prepared invocation")
            proof=f["Proof"];fields(proof,"Outcome Pair Group Original");o=proof["Outcome"]
            require(type(o["Commit"]) is bool and o["ID"]==x["ID"] and o["Request"]==x["Request"] and codec.sha_array(o["Digest"])==hashlib.sha256(body).digest(),"native original exact command identity")
            for r,kind,ident in zip(proof["Pair"],("id","request"),(x["ID"],x["Request"]),strict=True):
                nk=key(e["Config"],x["Coordinator"],kind,ident);raw.raw_record(r,nk,go_json(o));require(native(spool,"final",nk)==go_json(o),"terminal pairs join actual native controls")
            spool.insert(ns("terminal-request"),x["Request"].encode(),go_json(f))
            counts[0]+=1;counts[2 if o["Commit"] else 1]+=1
            if o["Commit"]:
                group=proof["Group"];require(same(group["Tx"],x) and same(group["Outcome"],o) and proof["Original"]["Value"] is not None,"complete committed original")
                nk=prefix(e["Config"],o["Coordinator"])+f'changes/{o["Sequence"]:016x}/'.encode()+o["Request"].encode().hex().encode();raw.raw_record(proof["Original"],nk,go_json(group));require(native(spool,"final",nk)==go_json(group),"actual CDC original")
                spool.insert(ns("useful"),u64(ordinal),go_json(f));spool.insert(ns("useful-request"),x["Request"].encode(),u64(ordinal))
                if phase!="setup":
                    clients[f["Client"]]+=1;start=integer(f["StartNanos"],0,e["TotalNanos"]);stop=integer(f["StopNanos"],start+1,e["TotalNanos"]);spool.insert(ns("latency"),u64(stop-start)+u64(ordinal),b"1")
            else:require(o["Reason"] in ("generation","revision") and o["Round"]==o["Sequence"]==0 and proof["Original"]=={"Key":None,"Value":None},"real ABORT, not native failure/unknown")
            put(spool,"chain",u64(ordinal),go_json(f))
        elif k=="business-phase":phase_bounds.append(f)
    require(counts==[e["Attempts"],e["Aborted"],e["Committed"]] and clients==e["Clients"],"complete original/caller census")
    for request,_ in spool.walk(ns("prepared")):spool.lookup(ns("terminal-request"),request)
    for ordinal in range(1,2*n+4):spool.lookup(ns("useful"),u64(ordinal))
    require(len(phase_bounds)==4 and [f["Phase"] for f in phase_bounds]==e["Business"],"actual business phase raw/result join")
    total=0;previous=e["SetupNanos"]
    for f,interval in zip(phase_bounds,(("create",0,n),("update",0,n//2),("update",n//2,n//2+1),("update",n//2+1,n)),strict=True):
        p=f["Phase"];require((p["Phase"],f["First"],f["Last"])==interval and integer(p["StartNanos"],previous)<integer(p["StopNanos"],p["StartNanos"]+1,e["TotalNanos"]) and p["WallNanos"]==p["StopNanos"]-p["StartNanos"] and p["Useful"]==f["Last"]-f["First"],"whole business walls exclude diagnostic/GC gaps")
        for fact in range(f["First"],f["Last"]):
            ordinal=4+fact+(n if p["Phase"]=="update" else 0);a=decoded(spool.lookup(ns("useful"),u64(ordinal)));require(p["StartNanos"]<=a["StartNanos"]<a["StopNanos"]<=p["StopNanos"],"whole-operation wait/retry within real phase wall")
        previous=p["StopNanos"];total+=p["WallNanos"]
    require(total==e["BusinessNanos"],"business throughput denominator is actual disjoint wall time")


def originals(e,spool):
    n,c=e["Spec"]["facts"],e["Config"];positions=[0,0];generations=[0,0];definitions=set();physical=codec.StoredState(spool,ns("physical"));versions=[0,0]
    for _ in range(3+2*n):
        heads=[]
        for g in range(2):
            if positions[g]<e["Final"]["Groups"][g]:
                body=spool.lookup(ns("cdc"),bytes([g])+u64(positions[g]+1));group=decoded(body);o=group["Outcome"];heads.append(((o["Round"],g,o["Request"]),body,group))
        require(heads,"complete original prefix, no fabricated empty")
        _,body,group=min(heads,key=lambda h:h[0]);o=group["Outcome"];ordinal=integer(group["Tx"]["Lifecycle"]["Provenance"],1,3+2*n)
        terminal=decoded(spool.lookup(ns("useful"),u64(ordinal)))
        require(same(terminal["Proof"]["Group"],group) and blob(terminal["Proof"]["Original"]["Value"])==body,"raw original complete terminal roster")
        codec.fdb_group(group,e["Spec"],ordinal,c,physical,generations,positions,definitions,parallel=True)
        for write in group["Writes"]:
            g,logical,after=write["Group"],write["Key"],write["After"];hk=history_key(c,g,logical,after);nk=key(c,g,"current",logical);versions[g]+=1
            spool.insert(ns("history"),hk,go_json(after));put(spool,"current",nk,go_json(after))
            if o["Round"]<=e["Snapshot"]["Round"]:put(spool,"old",nk,go_json(after));put(spool,"replica",nk,go_json(after))
            if o["Round"]<=e["BeforeGC"]["Round"]:put(spool,"gc-anchor",nk,go_json(after))
            if o["Round"]<=e["Checkpoint"]["Round"]:put(spool,"checkpoint",nk,go_json(after))
        for g in range(2):
            for name in ("Initial","Snapshot","BeforeGC","Checkpoint","Final"):
                require((o["Round"]<=e[name]["Round"]) == (o["Sequence"]<=e[name]["Groups"][o["Coordinator"]]),"authentic original cut vector/round membership")
        spool.insert(ns("group-ordinal"),u64(ordinal),body)
    require(positions==e["Final"]["Groups"] and definitions=={5,6},"full coordinator prefix and genuine typed definitions")
    for g in range(2):
        m=decoded(native(spool,"final",key(c,g,"meta","state")));require(integer(m["Groups"],0)==positions[g] and integer(m["Generation"],0)==generations[g],"real graph generation and useful sequence census")
    # Exact current and historical physical sets are joined independently of
    # the producer's summary. Collected rows must be genuinely absent.
    for stage in ("snapshot","before-gc","after-gc","checkpoint","final"):
        hist=current=0
        for nk,body in spool.walk(ns("native/"+stage)):
            g=0 if nk.startswith(prefix(c,0)) else 1
            if nk.startswith(prefix(c,g)+b"current/"):
                logical=bytes.fromhex(nk.rsplit(b"/",1)[1].decode()).decode();v=decoded(body)
                expected=spool.lookup(ns("old" if stage=="snapshot" else "current" if stage=="final" else "checkpoint" if stage=="checkpoint" else "gc-anchor"),nk)
                if expected is not None:require(body==expected,"actual stage current key/revision")
                require(spool.lookup(ns("history"),history_key(c,g,logical,v))==body,"native current has exact original lineage");current+=1
            elif nk.startswith(prefix(c,g)+b"history/"):
                require(spool.lookup(ns("history"),nk)==body,"native physical history not authenticated by original");hist+=1
        require(current==3+3*n,"full current endpoint/index directory")
        target=3+3*n if stage=="after-gc" else 3+3*n+3 if stage=="checkpoint" else 3+3*n+3*(n-n//2) if stage=="final" else 3+3*n+(3*(n//2) if stage=="before-gc" else 0)
        require(hist==target,"exact physical history retention census")
    # Every promised original remains in the active native prefix after GC.
    for g in range(2):
        for seq in range(1,e["Final"]["Groups"][g]+1):
            group=decoded(spool.lookup(ns("cdc"),bytes([g])+u64(seq)));o=group["Outcome"]
            for kind,ident in (("id",o["ID"]),("request",o["Request"])):require(native(spool,"final",key(c,g,kind,ident))==go_json(o),"retirement erased dedup originals")


def snapshot_row(row,at,e,spool,stage):
    fields(row,"Group Key Value Current History Chain");g=integer(row["Group"],0,1);logical=row["Key"];nk=key(e["Config"],g,"current",logical)
    expected=spool.lookup(ns("old" if stage=="snapshot-after-update" else "current"),nk)
    require(go_json(row["Value"])==expected,"historical value/revision/life/provenance exact raw row")
    head=decoded(blob(row["Current"]["Value"]));raw.raw_record(row["Current"],nk,go_json(head));require(native(spool,"before-gc" if stage=="snapshot-after-update" else "final",nk)==go_json(head),"actual retained current directory at scan");require(type(row["Chain"]) is list and 1<=len(row["Chain"])<=128,"bounded complete predecessor chain")
    v=head
    for i,r in enumerate(row["Chain"]):
        body=blob(r["Value"]);group=decoded(body);o=group["Outcome"]
        require(r["Group"]==o["Coordinator"] and body==spool.lookup(ns("cdc"),bytes([o["Coordinator"]])+u64(o["Sequence"])),"actual source original chain")
        ck=prefix(e["Config"],o["Coordinator"])+f'changes/{o["Sequence"]:016x}/'.encode()+o["Request"].encode().hex().encode();require(blob(r["Key"])==ck,"actual CDC key")
        for name in ("ID","Request"):raw.raw_record({"Key":r[name+"Key"],"Value":r[name+"Value"]},key(e["Config"],o["Coordinator"],name.lower(),o[name]),go_json(o))
        matches=[w for w in group["Writes"] if w["Group"]==g and w["Key"]==logical and same(w["After"],v)];require(len(matches)==1,"exact presence/property/posting chain")
        if i+1==len(row["Chain"]):require(same(v,row["Value"]) and v["Round"]<=at["Round"] and o["Sequence"]<=at["Groups"][o["Coordinator"]] and v["Deleted"] is False,"exact latest revision at requested old/current cut")
        else:require(v["Round"]>at["Round"] and matches[0].get("Before") is not None,"no skipped past revision");v=matches[0]["Before"]
    hk=history_key(e["Config"],g,logical,v);raw.raw_record(row["History"],hk,spool.lookup(ns("history"),hk))
    spool.insert(ns("snapshot/"+stage),nk,go_json(v))


def snapshots(directory,e,spool):
    stages=("snapshot-after-update","current");which=0;counts=[0,0];begin=False
    for f,_ in stream(directory,e):
        if f["Type"]=="snapshot-begin":
            require(which<2 and f["Stage"]==stages[which] and f["Cut"]==e["Snapshot" if which==0 else "Final"] and not begin,"actual old/current snapshot owner");begin=True
        elif f["Type"]=="snapshot-row":
            require(begin and f["Stage"]==stages[which],"snapshot outside its genuine lifetime");snapshot_row(f["Row"],e["Snapshot" if which==0 else "Final"],e,spool,stages[which]);counts[f["Row"]["Group"]]+=1
        elif f["Type"]=="snapshot-eof":
            at=e["Snapshot" if which==0 else "Final"];require(begin and f["Stage"]==stages[which] and f["End"]["Cut"]==at and f["End"]["Rows"]==counts and sum(counts)==3+3*e["Spec"]["facts"],"full exact-set snapshot EOF")
            expected=[0,0]
            for nk,body in spool.walk(ns("old" if which==0 else "current")):
                require(spool.lookup(ns("snapshot/"+stages[which]),nk)==body,"missing endpoint life/posting/phantom snapshot row");expected[0 if nk.startswith(prefix(e["Config"],0)) else 1]+=1
            require(counts==expected,"no snapshot omission or duplicate");which+=1;begin=False;counts=[0,0]
    require(which==2 and not begin,"both complete historical/current observations")


def allocation(directory,e,spool):
    blocks={};used={};next_home=1;nodes=values=edges=0;next_global=1;initial=e["Catalog"]["Authorities"];c=e["Config"];n=e["Spec"]["facts"]
    def claim(v):
        fields(v,"ID Offset Grant Session");g,s=blocks[v["Grant"]];offset=integer(v["Offset"],0,g["Count"]-1)
        require(integer(v["ID"],1)==g["First"]+offset and v["Session"]==s["Request"] and offset==used[v["Grant"]],"actual contiguous local Cursor.Next without reinstalls")
        used[v["Grant"]]+=1
    for f,_ in stream(directory,e):
        kind=f["Type"]
        if kind=="grant":
            g,s=f["Installed"]["Grant"],f["Session"];name=g["Request"];require(len(blocks)<64 and name not in blocks and g==f["Reservation"]["Grant"],"same native HOME reservation/installed grant")
            require(g["Version"]==1 and g["Graph"]==c["Graph"] and integer(g["First"],1)==next_global and integer(g["Count"],1,e["Catalog"]["Transfer"]["MaxBlock"])==g["Last"]-g["First"]+1 and g["Range"]=="allocation-range-"+name and any(codec.sha_array(g["SecretHash"])) and g["Bucket"] in (0,1),"authentic inclusive block/no range gap")
            require(g["Home"]=={"Owner":initial[0]["ID"],"Epoch":1} and g["Recipient"]=={"Owner":initial[g["Bucket"]]["ID"],"Epoch":1} and s["Grant"]==name and s["Route"]==g["Recipient"] and any(codec.sha_array(s["Secret"])) and s["Version"]==1,"recipient session actual native authority")
            require(native(spool,"final",owner_key(c,"allocation-grant",name))==go_json(g)==native(spool,"final",owner_key(c,"allocation-index",f'{g["Last"]:016x}')),"native grant and ordered highwater index")
            for receipt in (f["Reservation"],f["Installed"]):
                slot=receipt["Slot"];require(slot["Grant"]==name and slot["State"] in (0,1) and slot["Consumed"]==0,"actual native reserved/installed state")
                for r,k,v in zip(receipt["Records"],("allocation-grant","allocation-slot"),(g,slot),strict=True):raw.raw_record(r,owner_key(c,k,name),go_json(v))
            require(f["Installed"]["Slot"]=={"Grant":name,"Session":s,"State":1,"Consumed":0},"actual installation before first Next")
            blocks[name]=(g,s);used[name]=0;next_global=g["Last"]+1
        elif kind=="next-node":
            expected=codec.fixture(e["Spec"],nodes+1)[2];require(nodes<3 and f["Node"]==expected and len(f["Claims"])==2,"actual endpoint Node factory")
            for v in f["Claims"]:claim(v)
            require([v["ID"] for v in f["Claims"]]==[expected["Node"],expected["Life"]],"Node/Life came from actual Next");nodes+=1
        elif kind=="next-value":
            require(values<2 and f["Claim"]["ID"]==5+values,"actual typed definition issuance");claim(f["Claim"]);values+=1
        elif kind=="next-edge":
            require(integer(f["Fact"],0,n-1)==edges and f["Edge"]==codec.fixture(e["Spec"],4+edges)[3] and f["Life"]==f["Edge"]["ID"]+1 and len(f["Claims"])==2,"actual complete edge/life factory")
            for v in f["Claims"]:claim(v)
            require([v["ID"] for v in f["Claims"]]==[f["Edge"]["ID"],f["Life"]],"not numeric IDs without actual Next");edges+=1
    require((nodes,values,edges,next_global)==(3,2,n,2*n+9) and sum(used.values())==e["Issued"],"complete genuine Next outputs")
    for name,(g,s) in blocks.items():
        slot=decoded(native(spool,"final",owner_key(c,"allocation-slot",name)))
        require(slot=={"Grant":name,"Session":s,"State":2,"Consumed":used[name]} and used[name]==g["Count"],"durably closed consumed prefix; no burned ID reuse")
        cleanup=decoded(native(spool,"final",owner_key(c,"cleanup","grant/"+name)))
        require(cleanup["Request"]=="grant/"+name and integer(cleanup["Calls"],1,8) and 0<cleanup["DeadlineUnixNanos"]<=e["Transport"]["Policy"]["DeadlineUnixNanos"]+30_000_000_000,"funded finite terminal cleanup independent of normal quota")
    state=decoded(native(spool,"final",owner_key(c,"allocation-state","current")))
    require(state=={"Next":next_global,"Grants":len(blocks),"Exhausted":False},"complete native highwater/index census")
    issued=0
    for nk,body in spool.walk(ns("native/final")):
        if not nk.startswith(owner_key(c,"allocation-issued","")):continue
        issued+=1;r=decoded(body);v=r["Claim"];g,s=blocks[v["Grant"]]
        require(v["ID"]==g["First"]+v["Offset"] and 0<=integer(v["Offset"])<used[v["Grant"]] and v["Session"]==s["Request"] and nk==owner_key(c,"allocation-issued",f'{v["ID"]:016x}'),"native first-use issuance exact grant/session")
        intent_=decoded(native(spool,"final",owner_key(c,"intent-request",r["Request"])))
        require(v in intent_["Allocated"] and r["Digest"]==intent_["Digest"] and decoded(blob(intent_["Body"]))["ID"]==r["Transaction"],"first use bound to actual original business intent")
        o=decoded(native(spool,"final",key(c,r["Coordinator"],"request",r["Request"])))
        require(o["Commit"] is True and o["ID"]==r["Transaction"] and o["Digest"]==r["Digest"],"issuance needs actual durable COMMIT, not prepared body")
    require(issued==2*n+8,"exact native issued-ID roster")
    return {"grants":len(blocks),"issued":issued,"api":"actual HOME reserve + recipient session/install + local Cursor.Next; distinct from native installed-grant references"}


def feed_authority(e,spool):
    c=e["Config"];o,t=e["Original"],e["Token"];n=e["Spec"]["facts"]
    require(o["Version"]==3 and o["Request"]==t["Request"]=="retained-workload" and o["Secret"]==t["Secret"] and any(codec.sha_array(o["Secret"])) and t["Original"]==list(hashlib.sha256(go_json(o)).digest()) and o["Catalog"]==list(hashlib.sha256(go_json(e["Catalog"])).digest()) and "Admission" not in o and o["Cut"]==e["Snapshot"] and o["DeadlineNanos"]==t["DeadlineNanos"]==e["Transport"]["Policy"]["DeadlineUnixNanos"],"native original full catalog/deadline identity")
    zero_cut={"Graph":"","Topology":0,"Epochs":[0,0],"Scope":None,"Round":0,"Groups":[0,0]}
    require(o["Selection"]=={"Mode":"Fresh","Scope":"AAE=","At":zero_cut},"genuine full-scope Fresh factory, no supplied later cut")
    require(native(spool,"final",feed_key(c,"original",o["Request"]))==go_json(o),"actual persisted original; copied token grants nothing")
    pages=3+2*n;calls=8*(2*pages+32)
    read={"PageRows":8,"PageBytes":4<<20,"InventoryBytes":4<<20,"WorkRows":1024*n+4096,"WorkBytes":(64<<20)+n*(2<<20),"ChainSteps":128,"Cleanup":{"Rows":1024,"Bytes":64<<20,"Millis":30000}}
    policy={"PageRows":8,"PageBytes":64<<10,"MaxPages":pages,"MaxCalls":calls,"MaxControlBytes":(64<<20)+pages*(2*(64<<10)+8192),"MaxWorkRows":calls*(4096+128*8),"MaxWorkBytes":calls*(128<<20),"Read":read}
    require(o["Policy"]==policy,"finite cumulative feed/source policy")
    pin,snapshot=o["Pin"],o["Snapshot"]
    for owner,kind in ((pin,"pin"),(snapshot,"read")):
        require(owner["Kind"]==kind and owner["State"]==1 and owner["Cut"]==e["Snapshot"] and owner["Required"]==e["Initial"] and owner["TerminalRequest"]=="" and not any(codec.sha_array(owner["Lineage"])),"actual initial physical owner required-prefix binding")
    require(pin["Request"]=="feed-"+o["Request"] and bytes(pin["Secret"])==hashlib.sha256(b"rho-fdb-feed-v1/pin"+bytes(o["Secret"])).digest() and pin["PinRequest"]=="" and not any(pin["PinDigest"]) and snapshot["PinRequest"]==pin["Request"] and bytes(snapshot["PinDigest"])==hashlib.sha256(go_json({"Request":pin["Request"],"Cut":pin["Cut"],"Secret":pin["Secret"]})).digest(),"same snapshot pin native factory")
    phase=decoded(blob(e["Transition"]["Value"]));raw.raw_record(e["Transition"],feed_key(c,"retained-phase",o["Request"]),go_json(phase))
    require(native(spool,"after-gc",blob(e["Transition"]["Key"]))==go_json(phase)==native(spool,"final",blob(e["Transition"]["Key"])) and phase["Version"]==1 and phase["Original"]==t["Original"] and phase["Request"]=="retained-only","actual immutable all-owner mode transition")
    owner_prefix=prefix(c,0)+b"source-owner-v1/";chain=bytes(32);count=live=0
    for nk,body in spool.walk(ns("native/before-gc")):
        if not nk.startswith(owner_prefix):continue
        owner=decoded(body);require(nk==owner_prefix+owner["Request"].encode().hex().encode(),"canonical complete owner roster")
        chain=hashlib.sha256(chain+u64(len(nk))+nk+u64(len(body))+body).digest();count+=1;live+=owner["State"]==1
        if owner["Request"]==snapshot["Request"]:require(owner==snapshot|{"State":2,"TerminalRequest":"close"} and bytes(phase["SnapshotSHA256"])==hashlib.sha256(body).digest(),"snapshot genuinely closed before transition")
        elif owner.get("PinRequest")==pin["Request"]:require(owner["State"]!=1,"physical token reader remains active")
        if owner["Request"]==pin["Request"]:require(owner==pin,"original pin still active until atomic transition")
    require(count==phase["OwnerRows"] and chain==bytes(phase["OwnersSHA256"]) and live==1,"complete exact physical owner census, not all-absent inference")
    after_pin=native(spool,"after-gc",owner_prefix+pin["Request"].encode().hex().encode())
    require(decoded(after_pin)==pin|{"State":2,"TerminalRequest":"retained-only"} and bytes(phase["PinSHA256"])==hashlib.sha256(after_pin).digest(),"native terminal pin record and transition hash")
    return policy


def feed_page(delivery,boundary,position,last,e,spool):
    fields(delivery,"Page Record Replay");p=delivery["Page"];fields(p,"Version Request Original From Next Boundary Last Rows EOF")
    require(integer(p["Version"],1,1) and p["Original"]==e["Token"]["Original"] and p["Boundary"]==boundary and type(p["EOF"]) is bool and type(delivery["Replay"]) is bool and type(p["Rows"]) is list and len(p["Rows"])<=8,"complete immutable page shape")
    body=go_json(p);require(len(body)<=64<<10 and blob(delivery["Record"]["Value"])==body,"bounded exact page bytes")
    nk=feed_key(e["Config"],"page/"+e["Token"]["Request"],p["Request"]);raw.raw_record(delivery["Record"],nk,body);require(native(spool,"final",nk)==body,"page joins native immutable seal")
    old=spool.find(ns("page"),p["Request"].encode())
    if old is not None:require(old==body and delivery["Replay"],"SAME page identity after loss");return p,False,last
    require(p["From"]==position,"gap-free acknowledged vector, not caller cursor")
    next_=list(position)
    for r in p["Rows"]:
        body=blob(r["Value"]);g=integer(r["Group"],0,1);group=decoded(body);o=group["Outcome"]
        require(body==spool.lookup(ns("cdc"),bytes([g])+u64(next_[g]+1)) and o["Sequence"]==next_[g]+1 and o["Coordinator"]==g and e["Snapshot"]["Round"]<o["Round"]<=boundary["Round"],"full genuine original, no half-group or skipped native sequence")
        ck=prefix(e["Config"],g)+f'changes/{o["Sequence"]:016x}/'.encode()+o["Request"].encode().hex().encode();require(blob(r["Key"])==ck,"CDC actual stable native key")
        order=(o["Round"],g,o["Request"])
        require(last["Round"]==0 or (last["Round"],last["Coordinator"],last["Request"])<order,"monotone complete-envelope order")
        for other in range(2):
            if next_[other]<boundary["Groups"][other]:
                head=decoded(spool.lookup(ns("cdc"),bytes([other])+u64(next_[other]+1)))["Outcome"];require(order<=(head["Round"],other,head["Request"]),"earlier independent head omitted")
        for name in ("ID","Request"):raw.raw_record({"Key":r[name+"Key"],"Value":r[name+"Value"]},key(e["Config"],g,name.lower(),o[name]),go_json(o))
        for w in group["Writes"]:
            nk=key(e["Config"],w["Group"],"current",w["Key"]);before=spool.lookup(ns("replica"),nk)
            require(w.get("Before") is not None and before==go_json(w["Before"]),"exact snapshot+feed predecessor/payload, never latest substitution")
            spool.replace(ns("replica"),nk,before,go_json(w["After"]))
        spool.insert(ns("feed-consumed"),ck,body);next_[g]+=1;last=o
    require(p["Next"]==next_ and p["Last"]==last and p["EOF"] is (next_==boundary["Groups"]) and (p["Rows"] or p["EOF"]),"true page EOF/terminal witness")
    spool.insert(ns("page"),p["Request"].encode(),go_json(p));return p,True,last


def ack(value,page,completed,e,spool,fresh):
    fields(value,"Ack Record Cursor");a=value["Ack"];fields(a,"Request Page From Next Completed")
    expected=page["Boundary"] if page["EOF"] else completed
    require(a=={"Request":page["Request"],"Page":list(hashlib.sha256(go_json(page)).digest()),"From":page["From"],"Next":page["Next"],"Completed":expected},"ACK exact original page/hash and complete boundary only")
    nk=feed_key(e["Config"],"ack/"+e["Token"]["Request"],a["Request"]);raw.raw_record(value["Record"],nk,go_json(a));require(native(spool,"final",nk)==go_json(a),"actual native ACK")
    cursor=value["Cursor"];require(cursor["Original"]==e["Token"]["Original"] and cursor["Version"]==1 and cursor["State"]==1 and cursor["Positions"]==a["Next"] and cursor["Completed"]==expected and cursor["Pending"]=="","partial advance never publishes later cut")
    integer(cursor["Calls"],1,e["Original"]["Policy"]["MaxCalls"]);require(cursor["WorkRows"]==cursor["Calls"]*(4096+128*8) and cursor["WorkBytes"]==cursor["Calls"]*(128<<20),"native cumulative work cannot reset across process loss")
    if fresh:spool.insert(ns("ack"),a["Request"].encode(),go_json(a))
    else:require(spool.lookup(ns("ack"),a["Request"].encode())==go_json(a),"SAME ACK immutable even after restart")
    return expected


def feeds(directory,e,spool):
    policy=feed_authority(e,spool);position=list(e["Snapshot"]["Groups"]);completed=e["Snapshot"];last=zero_outcome();pages=rows=0;killed=None;replaced=False;refusals=[]
    for f,_ in stream(directory,e):
        k=f["Type"]
        if k=="feed-page":
            boundary=f["Delivery"]["Page"]["Boundary"];require(boundary in (e["BeforeGC"],e["Checkpoint"],e["Final"]),"actual admitted boundary")
            p,fresh,last=feed_page(f["Delivery"],boundary,position,last,e,spool);require(fresh,"new request distinct from replay")
            completed=ack(f["Ack"],p,completed,e,spool,True)
            if "ReplayAck" in f:ack(f["ReplayAck"],p,completed,e,spool,False)
            position=p["Next"];pages+=1;rows+=len(p["Rows"])
        elif k=="child-killed":
            require(killed is None and f["Signal"]=="SIGKILL" and f["Reaped"] is True,"genuine owned client kill/reap")
            q,r=f["Input"],f["Result"];require(q["Catalog"]==e["Catalog"] and q["Policy"]==policy and q["Token"]==e["Token"] and q["Boundary"]==e["Checkpoint"] and q["Reader"]=="retained-process-reader" and q["Page"]=="retained-process-page","SAME saved original/cursor/deadline, no journal authority")
            for principal,authority in zip(e["Catalog"]["Authorities"],q["Authorities"],strict=True):require(authority["ID"]==principal["ID"] and list(hashlib.sha256(codec.sha_array(authority["Secret"])).digest())==principal["SecretHash"],"actual SDK authenticates original logical keepers")
            require(r.get("Ack") is None and r["Delivery"]["Replay"] is False and integer(r["PID"],1)==e["Processes"][0],"crash strictly after seal before ACK")
            p,fresh,last=feed_page(r["Delivery"],e["Checkpoint"],position,last,e,spool);require(fresh and p["Rows"],"genuine unread post-GC page, not empty replay label");killed=r
        elif k=="child-replaced":
            r=f["Result"];require(killed is not None and not replaced and f["Reaped"] is True and integer(r["PID"],1)==e["Processes"][1]!=killed["PID"] and r["Identity"]==killed["Identity"] and r["Delivery"]["Record"]==killed["Delivery"]["Record"] and r["Delivery"]["Replay"] is True,"actual replacement and exact native pending page")
            p,fresh,_=feed_page(r["Delivery"],e["Checkpoint"],position,last,e,spool);require(not fresh,"replacement cannot create new page")
            completed=ack(r["Ack"],p,completed,e,spool,True);position=p["Next"];pages+=1;rows+=len(p["Rows"]);replaced=True
            require(r["Work"]["CleanupRows"]==1024 and r["Work"]["CleanupBytes"]==64<<20,"independent prepaid native cleanup lane")
        elif k=="refusal":require(f["Expired"] is True,"truthful explicit expiry");refusals.append(f["Door"])
    require(killed is not None and replaced and e["KilledReaped"] is e["ReplacementReaped"] is True and position==e["Final"]["Groups"] and completed==e["Final"] and rows==e["Spec"]["facts"]==e["FeedRows"] and pages==e["Pages"],"full gap-free feed EOF and real process inventory")
    require(refusals==["token-snapshot","generic-at-C","retained-reader-after-terminal"],"all promised explicit refusal doors")
    for nk,body in spool.walk(ns("current")):require(spool.lookup(ns("replica"),nk)==body,"complete replica equals actual current, no phantom/no-op approximation")
    cursor=decoded(native(spool,"final",feed_key(e["Config"],"cursor",e["Token"]["Request"])))
    require(cursor["State"]==3 and cursor["Positions"]==position and cursor["Completed"]==completed and cursor["Pending"]=="" and cursor["TerminalRequest"]=="retained-expire" and cursor["Pages"]==pages and 1<=cursor["Calls"]<=policy["MaxCalls"] and cursor["WorkRows"]==cursor["Calls"]*(4096+128*8) and cursor["WorkBytes"]==cursor["Calls"]*(128<<20),"actual terminal/attempt/work persistence")
    for name in ("retained-process-reader","tail-reader"):
        r=decoded(native(spool,"final",feed_key(e["Config"],"retained-reader/"+e["Token"]["Request"],name)))
        require(r["State"]==2 and r["Identity"]["Original"]==e["Token"]["Original"] and r["Identity"]["Request"]==name and integer(r["Cleanup"]["Calls"],1,8) and 0<r["Cleanup"]["DeadlineUnixNanos"]<=e["Token"]["DeadlineNanos"]+30_000_000_000,"actual native terminal reader cleanup")
    return {"pages":pages,"completeLogicalUpdates":rows,"pendingReplayAfterGC":True,"snapshotReopenRefused":True,"nativeRetainedCursor":cursor}


def collection(e,spool):
    c=e["Config"];seal=e["Retired"];before=e["BeforeGC"]
    require(seal["Request"]=="retained-gc" and seal["Cut"]==before and native(spool,"final",key(c,0,"source-retention-v1","seal/retained-gc"))==go_json(seal),"genuine native retirement seal/original")
    chain=bytes(32);originals=[0,0];deleted=[0,0];deleted_bytes=[0,0];anchors=[0,0]
    for g in range(2):
        for seq in range(1,before["Groups"][g]+1):
            body=spool.lookup(ns("cdc"),bytes([g])+u64(seq));group=decoded(body);o=group["Outcome"];nk=prefix(c,g)+f'changes/{seq:016x}/'.encode()+o["Request"].encode().hex().encode()
            chain=hashlib.sha256(chain+u64(len(nk))+nk+u64(len(body))+body).digest()
            for w in group["Writes"]:originals[w["Group"]]+=1
    for hk,body in spool.walk(ns("history")):
        v=decoded(body)
        if v["Round"]>before["Round"]:continue
        g=0 if hk.startswith(prefix(c,0)) else 1;logical=bytes.fromhex(hk.rsplit(b"/",1)[1].decode()).decode();anchor=spool.lookup(ns("gc-anchor"),key(c,g,"current",logical))
        require(native(spool,"before-gc",hk)==body,"pre-GC full exact original history")
        actual=spool.find(ns("native/after-gc"),hk)
        if body==anchor:require(actual==body,"genuine latest predecessor anchor omitted");anchors[g]+=1
        else:require(actual is None and v["Round"]<decoded(anchor)["Round"],"actual physical deletion, not retain-all labelled collection");deleted[g]+=1;deleted_bytes[g]+=len(hk)+len(body)
    require(seal["Originals"]==originals and seal["Deleted"]==deleted and seal["DeletedBytes"]==deleted_bytes and seal["Anchors"]==anchors and seal["CumulativeDeleted"]==deleted and bytes(seal["CensusSHA256"])==chain and sum(deleted)>0,"exact original/physical anchor/deletion census and native chain")
    for g in range(2):
        old=decoded(native(spool,"before-gc",key(c,g,"meta","state")));new=decoded(native(spool,"after-gc",key(c,g,"meta","state")))
        require(seal["Before"][g]==old and seal["After"][g]==new and new==old|{"Versions":old["Versions"]-deleted[g],"Bytes":old["Bytes"]-deleted_bytes[g]},"atomic native physical accounting, no fake business round/CDC")
    return {"deleted":deleted,"deletedKeyValueBytes":deleted_bytes,"anchors":anchors,"retention":"detached CDC/paired originals/certificates + current native anchors; differs from conservative rolling-ACK predecessor"}


def column_row(r,e,spool):
    fields(r,"Kind Owner Life SourceNode SourceLife TargetNode TargetLife Value ValueID Provenance Round Sequence Coordinator Original")
    body=blob(r["Original"]);g=decoded(body);ordinal=integer(g["Tx"]["Lifecycle"]["Provenance"],1,3+2*e["Spec"]["facts"])
    require(body==spool.lookup(ns("group-ordinal"),u64(ordinal)),"column full original already independently authenticated")
    x,o=g["Tx"],g["Outcome"];command=x["GraphChange"];edge,node=command["Edge"],command["Node"];is_node=command["Kind"]=="node-create"
    expected={"Kind":1 if is_node else 2 if command["Kind"]=="edge-create" else 3,"Owner":node["Node"] if is_node else edge["ID"],"Life":node["Life"] if is_node else x["Lifecycle"]["EdgeLife"],"SourceNode":edge["Source"]["Node"],"SourceLife":edge["Source"]["Life"],"TargetNode":edge["Target"]["Node"],"TargetLife":edge["Target"]["Life"],"Value":1 if is_node else edge["Value"],"ValueID":0 if is_node else x["Lifecycle"]["Definition"]["ID"],"Provenance":ordinal,"Round":o["Round"],"Sequence":o["Sequence"],"Coordinator":o["Coordinator"],"Original":r["Original"]}
    require(same(r,expected),"exact typed owner/life/endpoints/value/definition/provenance/revision")
    return g


def chunk_rows(body,generation,binding,ordinal,chain,policy,e,spool):
    require(114<=len(body)<=policy["ChunkBytes"] and body[:8]==b"RHOCLM03" and body[8:40]==binding and body[40:72]==hashlib.sha256(go_json(generation)).digest() and body[72:80]==u64(ordinal) and body[80:112]==chain,"fresh exact interval format/generation/chain")
    count=struct.unpack_from(">H",body,112)[0];require(1<=count<=policy["ChunkRows"]==16,"moderate bounded chunks")
    arena=114+count*98;require(arena<=len(body),"all typed numeric columns present");offset=0
    names="Owner Life SourceNode SourceLife TargetNode TargetLife Value ValueID Provenance Round Sequence".split()
    for i in range(count):
        values={name:struct.unpack_from(">Q",body,114+2*count+(j*count+i)*8)[0] for j,name in enumerate(names)}
        if values["Value"]>=1<<63:values["Value"]-=1<<64
        start=struct.unpack_from(">I",body,114+90*count+i*4)[0];length=struct.unpack_from(">I",body,114+94*count+i*4)[0]
        require(start==offset and 0<length<=len(body)-arena-offset,"canonical bounded original arena")
        original=body[arena+offset:arena+offset+length];r={"Kind":body[114+i],**values,"Coordinator":body[114+count+i],"Original":feed.raw.base64.b64encode(original).decode()}
        group=column_row(r,e,spool);require(generation["Initial"]["Round"]<r["Round"]<=generation["Cut"]["Round"] and generation["Initial"]["Groups"][r["Coordinator"]]<r["Sequence"]<=generation["Cut"]["Groups"][r["Coordinator"]],"authenticated exact interval, not full-prefix relabel")
        yield r,group;offset+=length
    require(arena+offset==len(body),"no trailing/missing chunk bytes")


def columns(directory,e,spool):
    if e["Spec"]["layout"]=="kv":
        require(all(f["Type"] not in ("columns-manifest","columns-row","columns-eof","cache-fill","columns-retire") for f,_ in stream(directory,e)),"no hidden column lane in KV cell");return None
    c,n=e["Config"],e["Spec"]["facts"];manifests={};binding=None;stage=None;rows=0;sequence=None;history=None;previous=None;completed=[];retired_chunks=retired_bytes=fill_count=0
    expected_stages=("snapshot-native","checkpoint-native","checkpoint-cache","checkpoint-reopened")
    def ck(id_,kind):return prefix(c,0)+b"columns-v1/generation/"+id_.encode().hex().encode()+b"/"+kind.encode()
    for f,_ in stream(directory,e):
        k=f["Type"]
        if k=="columns-manifest":
            b,m=f["Binding"],f["Manifest"];g=m["Generation"];name=g["ID"]
            require(name in ("snapshot-columns","checkpoint-columns") and name not in manifests,"two genuinely different generation intervals")
            if binding is None:binding=b
            require(b==binding and b["Version"]==3 and b["Layout"]=="scalar-feed-checkpoint-i64-columns-v1" and b["Schema"]=="authenticated-initial-groups;node-presence;edge-life-bound;property-p:i64;canonical-interval-original:v1" and b["Config"]==c and b["Scope"]==codec.SCOPE,"fresh immutable binding/schema")
            p=b["Policy"];require(p=={"ChunkRows":16,"ChunkBytes":64<<10,"MaxChunks":2*((2*n+3+15)//16)+16,"MaxGenerations":2,"MaxRetainedBytes":(64<<20)+n*(32<<10),"MaxWorkRows":2*n+3,"MaxWorkBytes":(64<<20)+n*(64<<10),"Version":2},"finite exact generation policy")
            first,last=(e["Initial"],e["Snapshot"]) if name=="snapshot-columns" else (e["Checkpoint"],e["Final"])
            require(g=={"Version":2,"ID":name,"Binding":list(hashlib.sha256(go_json(b)).digest()),"Initial":first,"Cut":last},"exact native initial vector and terminal cut")
            require(native(spool,"final",ck(name,"manifest"))==go_json(m),"actual immutable manifest")
            native_stage="snapshot" if name=="snapshot-columns" else "final";chain=bytes(32);count=retained=0;seq=list(first["Groups"]);census=[0,0];previous_order=None
            for ordinal in range(integer(m["Chunks"],0,p["MaxChunks"])):
                body=native(spool,native_stage,ck(name,f"chunk/{ordinal:016x}"));retained+=len(ck(name,f"chunk/{ordinal:016x}"))+len(body)
                for r,group in chunk_rows(body,g,bytes(g["Binding"]),ordinal,chain,p,e,spool):
                    o=group["Outcome"];require(o["Sequence"]==seq[o["Coordinator"]]+1,"complete interval per-source vector");order=(o["Round"],o["Coordinator"],o["Request"]);require(previous_order is None or previous_order<order,"actual original order in chunks");previous_order=order;seq[o["Coordinator"]]+=1;count+=1
                    for w in group["Writes"]:census[w["Group"]]+=1
                    spool.insert(ns("column/"+name),u64(r["Provenance"]),go_json(r))
                chain=hashlib.sha256(chain+hashlib.sha256(body).digest()).digest()
                if name=="checkpoint-columns":
                    cache_name=f"column-{hashlib.sha256(go_json(g)).hexdigest()}-{ordinal:016x}.bin";cache=Path(directory)/"retained-column-cache"/cache_name
                    require(not cache.is_symlink() and cache.is_file() and transport.admitted(cache,p["ChunkBytes"])==body,"actual bounded entire-generation cache bytes")
            require(seq==last["Groups"] and count==m["Rows"]==sum(last["Groups"])-sum(first["Groups"]) and census==m["History"] and retained==m["Bytes"] and list(chain)==m["Chain"],"full native original/Before/After interval census/EOF")
            manifests[name]=m;stage=f["Stage"];rows=0
        elif k=="columns-row":
            require(f["Stage"] in expected_stages and f["Stage"] not in completed,"owned column walk before EOF")
            if stage!=f["Stage"]:require(stage is None,"interleaved column walks");stage=f["Stage"];rows=0
            name="snapshot-columns" if stage=="snapshot-native" else "checkpoint-columns";r=f["Row"];column_row(r,e,spool)
            require(spool.lookup(ns("column/"+name),u64(r["Provenance"]))==go_json(r),"walk equals actual interval binary original")
            spool.insert(ns("walk/"+stage),u64(r["Provenance"]),go_json(r));rows+=1
        elif k=="columns-eof":
            require(stage==f["Stage"]==expected_stages[len(completed)],"complete native/cache/reopen order")
            name="snapshot-columns" if stage=="snapshot-native" else "checkpoint-columns";m=manifests[name]
            require(f["Manifest"]==m and rows==m["Rows"],"complete actual typed walk EOF")
            for ordinal,r in spool.walk(ns("column/"+name)):require(spool.lookup(ns("walk/"+stage),ordinal)==r,"column walk omitted or duplicated row")
            completed.append(stage);stage=None
        elif k=="cache-fill":
            require(f["Generation"]=="checkpoint-columns" and integer(f["Ordinal"],0)==fill_count,"actual ordered fill of same genuine generation")
            require(blob(f["Body"])==native(spool,"final",ck("checkpoint-columns",f"chunk/{fill_count:016x}")),"cache-fill actual original binary bytes");fill_count+=1
        elif k=="columns-retire":
            p=f["Progress"];require(p["Generation"]=="snapshot-columns" and integer(p["Cursor"],retired_chunks+1)<=manifests["snapshot-columns"]["Chunks"] and integer(p["DeletedBytes"],retired_bytes+1) and type(p["Done"]) is bool,"real bounded derived-column deletion/cursor")
            retired_chunks=p["Cursor"];retired_bytes=p["DeletedBytes"]
    require(completed==list(expected_stages) and stage is None and fill_count==manifests["checkpoint-columns"]["Chunks"] and retired_chunks==manifests["snapshot-columns"]["Chunks"] and retired_bytes==manifests["snapshot-columns"]["Bytes"],"actual old owners retired and all fresh column stages complete")
    generation=manifests["checkpoint-columns"]["Generation"];hash_=hashlib.sha256(go_json(generation)).hexdigest();count=0
    with os.scandir(Path(directory)/"retained-column-cache") as entries:
        for entry in entries:
            require(count<manifests["checkpoint-columns"]["Chunks"] and not entry.is_symlink() and entry.is_file(follow_symlinks=False) and entry.name.startswith("column-"+hash_+"-") and entry.name.endswith(".bin"),"no foreign/trailing derived cache files")
            ordinal=int(entry.name.removeprefix("column-"+hash_+"-").removesuffix(".bin"),16)
            require(entry.name==f"column-{hash_}-{ordinal:016x}.bin" and ordinal<manifests["checkpoint-columns"]["Chunks"],"canonical bounded cache ordinal");count+=1
    require(count==manifests["checkpoint-columns"]["Chunks"],"complete cache directory EOF")
    old=manifests["snapshot-columns"];guard=decoded(native(spool,"final",ck("snapshot-columns","owners")))
    require(guard=={"Manifest":list(hashlib.sha256(go_json(old)).digest()),"State":3,"References":0,"Pins":0,"Readers":0,"Cursor":old["Chunks"],"DeletedBytes":old["Bytes"],"Chain":old["Chain"]},"durable owner-free retired generation, no fake collected label")
    for ordinal in range(old["Chunks"]):require(spool.find(ns("native/final"),ck("snapshot-columns",f"chunk/{ordinal:016x}")) is None,"old physical chunks still present")
    return {"oldInitialGroups":e["Initial"]["Groups"],"newInitialGroups":e["Checkpoint"]["Groups"],"oldRows":old["Rows"],"checkpointRows":manifests["checkpoint-columns"]["Rows"],"retiredChunkBytes":retired_bytes,"scope":"actual interval groups; no initial-C availability claim for checkpoint columns"}


def logical_answers(e,spool):
    answer=hashlib.sha256();n=e["Spec"]["facts"]
    for offset,stage in ((0,"snapshot-after-update"),(n,"current")):
        for fact in range(n):
            group=decoded(spool.lookup(ns("group-ordinal"),u64(4+offset+fact)));x=group["Tx"];edge=x["GraphChange"]["Edge"];life=x["Lifecycle"]
            for w in group["Writes"]:
                nk=key(e["Config"],w["Group"],"current",w["Key"]);require(spool.lookup(ns("snapshot/"+stage),nk)==go_json(w["After"]),"parity answer requires full actual old/current EOF")
                if offset:require(spool.lookup(ns("replica"),nk)==go_json(w["After"]),"parity answer requires actual complete acknowledged feed")
            coordinates=(fact,edge["ID"],life["EdgeLife"],edge["Source"]["Node"],edge["Target"]["Node"],edge["Source"]["Life"],edge["Target"]["Life"],life["Definition"]["ID"])
            answer.update(b"edge-i64-lifebound-v1\0"+b"".join(u64(v) for v in coordinates)+struct.pack(">q",life["Definition"]["I64"])+u64(life["Provenance"]))
    return answer.hexdigest()


def verify(directory,spool_directory,policy,*,shared_spool=None):
    directory=Path(directory).resolve(strict=True);e=result(directory)
    require(policy is not None,"explicit finite caller-owned verifier grant required");policy.validate()
    with (nullcontext(shared_spool) if shared_spool is not None else Spool(spool_directory,policy)) as spool:
        inventory(directory,e,spool);chronology_=chronology((f for f,_ in stream(directory,e)),e);attempts(directory,e,spool);originals(e,spool)
        allocation_=allocation(directory,e,spool);snapshots(directory,e,spool);feed_=feeds(directory,e,spool);gc=collection(e,spool);column_=columns(directory,e,spool)
        answers=logical_answers(e,spool);timing={};samples=0;n=e["Spec"]["facts"]
        for samples,(coordinate,_) in enumerate(spool.walk(ns("latency")),1):
            for p in (50,95,99):
                if samples==(2*n*p+99)//100:timing[f"p{p}Nanos"]=int.from_bytes(coordinate[:8],"big")
        require(samples==2*n and len(timing)==3,"all complete logical-operation nearest-rank percentiles")
        physical={stage:{"records":0,"keyValueBytes":0} for stage in STAGES}
        for stage in STAGES:
            for _,body in spool.walk(ns("native-eof")):
                f=decoded(body)
                if f["Stage"]==stage:physical[stage]["records"]+=f["Rows"];physical[stage]["keyValueBytes"]+=f["Bytes"]
        transport_=transport.verify(directory,e["Transport"]["Policy"],spool)
        return {"format":FORMAT,"spec":e["Spec"],"runGraph":e["Config"]["Graph"],"run":bytes(e["Transport"]["Policy"]["Run"]).hex(),"retentionContract":CONTRACT,"logicalAnswersSHA256":answers,"minimumContract":{"completeOldCurrent":True,"snapshotTokenReopenRefused":True,"completeUpdates":n,"unreadAfterRealGC":True,"samePageAckAfterClientReplacement":True},"feed":feed_,"physicalCollection":gc|{"chronology":chronology_},"physicalNativePrefixes":physical,"columns":column_,"allocator":allocation_,"transport":transport_,"timing":{**timing,"businessNanos":e["BusinessNanos"],"usefulTransactionsPerSecond":2*n*1_000_000_000/e["BusinessNanos"],"artifactWindowNanos":e["TotalNanos"],"scope":"business-only disjoint phase walls; full logical Next/queue/admission/retry/replan/wait operations; artifact window starts after harness/transport/catalog creation; actual monitor includes initialization/helpers/verification"},"spool":spool.report(),"scope":"source-implemented detached FDB retained-only workload; native rolling-anchor availability/storage differs; no equal physical GC, nonlocal parity or capacity/full-matrix acceptance"}


if __name__=="__main__":
    try:
        require(len(sys.argv)==5,"usage: fdb_feed_retained_check.py ARTIFACTS OWNED_SPOOL_DIR DISK_BYTES CACHE_BYTES")
        print(json.dumps(verify(sys.argv[1],sys.argv[2],Policy(int(sys.argv[3]),int(sys.argv[4]),4096,LINE)),indent=2))
    except (ValueError,KeyError,TypeError,OSError) as error:raise SystemExit("retained-only FDB evidence refused: "+str(error)) from error
