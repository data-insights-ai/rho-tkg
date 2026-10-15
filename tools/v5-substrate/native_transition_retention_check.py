#!/usr/bin/env python3
"""Genuine AS7 late reader, installed original and resumable local GC batches.

Only member1's physical collection is claimed; all A voters install the actual
floor through ordinary Raft. No journal, inert original or image anchor creates
an old data cut or a new deletion authority. This is a finite source fixture.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import struct

SPEC=importlib.util.spec_from_file_location("retention_state_owner",Path(__file__).with_name("native_transition_state_check.py"))
S=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(S)
T,P,F=S.T,S.P,S.F
require,integer,fixed=T.require,T.integer,T.fixed
PROFILE="native-actual-retained-reader-gc-process-v1"
HEADER=set("Version Profile Side Run DeadlineUnixNanos Old Current Reopened Attempts Records From Through Calls Arm State Processes BeforeState LocalState AfterState OwnerCalls ReceiptReads ReceiptGrantBytes".split())
CONTROL=set("Version Action Cookie Sequence Request Floors Index Original".split())
RECEIPT=set("Version Profile Cookie Run Member PID DeadlineUnixNanos Sequence Calls Pages GrantedRows GrantedBytes GrantedOutput Action State ViewIndex Original Outcome CDC Found Plan Root Progress Work".split())
ROOT=set("Generation Index Image ImageHash".split())
PLAN=set("ID PreviousID SourceHash RetiredCutID SourceIndex InstalledIndex RootFloor FeedFloor OutcomeFloor RecoveryFloor DataFloor Roots".split())
PROGRESS=set("PlanID Generation SourceIndex InstalledIndex CursorBytes CursorRecords Section Complete".split())


def load(path):
    digest,total,header,footer,records,calls=hashlib.sha256(),0,None,None,[],[]
    with path.open("rb") as source:
        while line:=source.readline((512<<10)+1):
            total+=len(line)
            require(len(line)<=512<<10 and total<=16<<20 and line.endswith(b"\n"),"retention artifact admission")
            row=T.decoded(line);require(type(row) is dict and footer is None,"retention row after EOF")
            if header is None:
                require(set(row)=={"Type","Artifact"} and row["Type"]=="header","retention header")
                header=row["Artifact"];require(type(header) is dict and set(header)==HEADER and header["Records"] is None and header["Calls"] is None,"fresh retention grammar")
            elif row.get("Type")=="record":
                require(set(row)=={"Type","Record"} and len(records)<256,"retention raw prefix bound");records.append(row["Record"])
            elif row.get("Type")=="call":
                require(set(row)=={"Type","Sequence","Call"} and type(row["Sequence"]) is int and row["Sequence"]==len(calls)<128,"retention call order/bound");calls.append(row["Call"])
            else:
                require(set(row)=={"Type","Rows","Calls","Through","PayloadSHA256"} and row["Type"]=="footer" and type(row["Rows"]) is int and row["Rows"]==len(records) and type(row["Calls"]) is int and row["Calls"]==len(calls) and row["PayloadSHA256"]==digest.hexdigest(),"retention exact EOF/hash/census");footer=row
            if footer is None:digest.update(line)
    require(header is not None and footer is not None and footer["Through"]==header["Through"],"missing/foreign retention EOF")
    header["Records"],header["Calls"]=records,calls;return header


def response(value):
    """Exact existing process frame; fresh durable profile admits codes0..9."""
    wire=P.raw(value);require(len(wire)<=2<<20,"native response bound");c=P.Cursor(wire)
    incarnation,stats=c.take(16),[c.number() for _ in range(28)]
    cut,manifest,generation=c.take(32),c.take(32),c.number()
    n=c.number(4);require(n<=2,"native handle bound");handles=[c.number() for _ in range(n)]
    aux,ready,sequence,applied,code,mask,detail=c.field(1<<20),c.number(1),c.number(),c.number(),c.number(1),c.number(2),c.field(4096)
    n=c.number(4);require(n<=128,"native packet bound");packets=[F.packet(c) for _ in range(n)]
    n=c.number(4);require(n<=16,"native reads bound");reads=[(c.number(),c.field(1024)) for _ in range(n)]
    image,outcome,changes=c.field(65536),c.field(65536),c.field(1<<20)
    flags,life,n=c.number(1),c.number(),c.number(4);require(flags<=3 and n<=16,"native fact bound")
    labels=[c.field(256) for _ in range(n)];answer=c.field(256);c.end()
    require(any(incarnation) and sequence>0 and ready<=1 and code<=9,"actual native response identity/class")
    return dict(incarnation=incarnation,stats=stats,cut=cut,manifest=manifest,generation=generation,handles=handles,aux=aux,ready=ready,sequence=sequence,applied=applied,code=code,mask=mask,detail=detail,packets=packets,reads=reads,image=image,outcome=outcome,changes=changes,flags=flags,life=life,labels=labels,answer=answer)


def root(value,empty=False):
    require(type(value) is dict and set(value)==ROOT,"native root grammar")
    for k in ("Generation","Index"):integer(value[k])
    image=P.raw(value["Image"]);digest=fixed(value["ImageHash"],32)
    if empty:require(value["Generation"]==value["Index"]==0 and value["Image"] is None and digest==bytes(32),"failure fabricated empty/current root")
    else:require(value["Generation"]>0 and value["Index"]>0 and len(image)==172 and hashlib.sha256(image).digest()==digest,"actual native root hash/coordinates")
    return image


def point(value,want):
    require(type(value) is dict and set(value)=={"Cut","Response"},"actual point/cut grammar")
    scopes,round_=P.cut({"Cut":value["Cut"]})
    certs={}
    for wire in value["Cut"]["Certificates"]:
        b=P.body(P.raw(wire),b"GSC\1");part=int.from_bytes(b[16:24],"big")
        certs[part]=dict(scope=b[:120],configuration=b[120:152],round=round_,index=int.from_bytes(b[160:168],"big"),source=int.from_bytes(b[176:184],"big"),image=b[184:356])
    r=response(value["Response"]);require(r["code"]==r["mask"]==0 and not r["outcome"] and not r["changes"],"point native refusal")
    e=T.decoded(r["aux"])
    require(type(e) is dict and set(e)==set("id found kind source target mode type presence effective effectiveLife labels property".split()) and e["id"]==7 and e["found"] is True and e["kind"]==2 and e["source"]==1 and e["target"]==9 and e["mode"]==1 and e["type"]=="edge" and e["effective"] is True and e["effectiveLife"]==8 and e["labels"] in (None,[]),"independent literal edge/life/endpoint answer")
    cell=e["property"];require(set(cell)==set("present null handle revision provenance kind i64".split()) and cell["present"] is True and cell["null"] is False and cell["kind"]=="i64" and cell["i64"]==cell["handle"]==want and cell["provenance"]==(4 if want==5 else 5) and integer(cell["revision"],minimum=1),"literal old/current scalar, revision and provenance")
    return scopes,certs,round_,e


def ggc(wire,scope):
    c=P.Cursor(P.body(wire,b"GGC\1"));require(c.take(120)==scope,"actual GGC native scope")
    issuer,request,configuration,previous=c.take(16),c.take(16),c.take(32),c.take(32)
    floors=[c.number() for _ in range(6)];c.end()
    require(any(issuer) and any(request) and any(configuration) and all(v<(1<<64)-2 for v in floors),"actual GGC original/floors")
    return dict(id=hashlib.sha256(wire).digest(),issuer=issuer,request=request,configuration=configuration,previous=previous,floors=floors)


def ggo(wire):
    c=P.Cursor(P.body(wire,b"GGO\1"));status,id_,source_hash=c.number(1),c.take(32),c.take(32)
    index,source,round_=c.number(),c.number(),c.number();c.end()
    require(status==1 and any(id_) and any(source_hash) and 0<source<index,"actual applied native retention original")
    return dict(id=id_,source_hash=source_hash,index=index,source=source,round=round_)


def original_attempt(a,scopes):
    require(a["Engine"]=="native-raft" and a["Classification"]=="commit" and a["ClientID"]==0 and a["AttemptOrdinal"]==1 and not any(fixed(a["PreviousRequest"],16)),"fixed actual useful original")
    proposal,manifest=P.raw(a["Proposal"]),P.raw(a["Manifest"])
    source=P.pgq(proposal);c=P.Cursor(P.body(manifest,b"GCM\1"))
    tx,co,original_wire=c.take(16),c.number(),c.field(32768)
    original=P.pgq(original_wire)
    require(co==original["partition"]==source["partition"] and original["request"]==source["request"]==fixed(a["Request"],16,True) and P.business_hash(proposal,co)==P.business_hash(original_wire)==fixed(a["BusinessSHA256"],32,True),"actual immutable complete useful request")
    configuration,declaration,routing,template=(c.take(32) for _ in range(4))
    require((configuration,declaration,routing)==(source["configuration"],source["declaration"],source["routing"]) and any(template),"actual original full configuration")
    count=c.number(4);require(1<=count<=2,"actual useful participant count");parts=[]
    for _ in range(count):
        part,floor,k=c.number(),c.number(),P.Cursor(P.body(c.field(32768),b"GPK\1"))
        require(part in scopes and (not parts or part>parts[-1]) and k.take(16)==tx and k.take(16)==P.GRAPH and k.number()==part and k.number()==int.from_bytes(scopes[part][40:48],"big"),"actual owned prepared kernel")
        require(tuple(k.take(32) for _ in range(4))==(configuration,declaration,routing,template),"prepared kernel layout")
        effect=k.take(32);require(k.number()>floor,"provisional native revision");k.number();require(bool(k.field()) and hashlib.sha256(k.field()).digest()==effect,"native axis/effect digest");k.end();parts.append(part)
    c.end()
    digest=hashlib.sha256(manifest).digest();d=P.status(P.raw(a["Outcome"]))
    expected=hashlib.sha256(b"rho-tkg:cross-transaction-id:v1\0"+P.GRAPH+struct.pack(">Q",co)+source["request"]).digest()[:16]
    require(tx==expected==fixed(a["Transaction"],16,True) and digest==fixed(a["DecisionDigest"],32,True) and hashlib.sha256(proposal).digest()==fixed(a["BodySHA256"],32,True) and d["phase"]==3 and d["commit"] and d["partition"]==co and d["tx"]==tx and d["digest"]==digest and d["index"]==a["OriginalIndex"] and d["round"]==a["Round"] and a["CDC"] is not None and not P.raw(a["CDC"]),"actual first durable useful decision")
    return dict(tx=tx,digest=digest,co=co,parts=parts,decision=d,ordinal=a["Operation"]["Ordinal"])


def prefix(data,descriptors,plan_result):
    require(type(data["Records"]) is list and len(data["Records"])<=256 and len(data["From"])==len(data["Through"])==2,"finite complete native prefix")
    rows={};phases={d["tx"]:set() for d in descriptors};known={d["tx"]:d for d in descriptors}
    for r in data["Records"]:
        require(set(r)=={"Partition","Index","Outcome","CDC"} and r["Partition"] in (3,8) and type(r["Index"]) is int and (r["Partition"],r["Index"]) not in rows and r["Outcome"] is not None and r["CDC"] is not None,"actual present native original frame")
        rows[r["Partition"],r["Index"]]=r
    for g,part in enumerate((3,8)):
        lo,hi=integer(data["From"][g],128,1),integer(data["Through"][g],128,1)
        require(lo<=hi and [r["Index"] for r in data["Records"] if r["Partition"]==part]==list(range(lo+1,hi+1)),"complete declared native prefix, including helper/control rows")
    for (part,index),r in rows.items():
        outcome,cdc=P.raw(r["Outcome"]),P.raw(r["CDC"])
        if not outcome:require(not cdc,"missing actual original");continue
        if outcome[:4]==b"GSO\1":
            b=P.body(outcome,b"GSO\1");require(len(b)==18 and b[0] in (1,2) and b[1] in (1,2,3,4) and int.from_bytes(b[2:10],"big")>0 and int.from_bytes(b[10:18],"big")==index and not cdc,"actual cut control original");continue
        if outcome[:4]==b"GGO\1":
            require(part==3 and index==plan_result["index"] and ggo(outcome)==plan_result and not cdc,"orphan GGO/CDC");continue
        s=P.status(outcome);require(s["tx"] in known,"unknown native original")
        d=known[s["tx"]];require(s["digest"]==d["digest"] and s["partition"]==part and s["index"]<=index,"actual useful original association")
        if s["index"]<index:require(not cdc and rows.get((part,s["index"]),{}).get("Outcome")==r["Outcome"],"replay lacks earlier genuine original");continue
        key=part,s["phase"];require(key not in phases[s["tx"]],"duplicate first original phase");phases[s["tx"]].add(key)
        require(s["yes"] if s["phase"]==2 else True,"unexpected useful NO")
        if s["phase"]==3:require(s==d["decision"] and not cdc,"different first decision")
        elif s["phase"]==4:
            require(s["commit"] and s["round"]==d["decision"]["round"] and bool(cdc),"missing actual participant effects")
            c=P.Cursor(P.body(cdc,b"GCE\1"));require(c.number(1)==6 and c.take(16)==s["tx"] and c.take(32)==s["digest"] and c.number()==index,"actual CDC original identity")
            logical=P.body(c.field(32768),b"GCD\5");c.end();require(logical[:16]==P.GRAPH and int.from_bytes(logical[16:24],"big")==part,"actual logical effect namespace")
        else:require(not cdc,"control phase business CDC")
    for d in descriptors:
        expected={(d["co"],1),(d["co"],3)}|{(p,k) for p in d["parts"] for k in (2,4)}
        require(phases[d["tx"]]==expected,"incomplete actual useful protocol phases")
    installed=rows.get((3,plan_result["index"]),{}).get("Outcome")
    require(installed is not None and P.raw(installed).startswith(b"GGO\1"),"installed original omitted from complete prefix")


def control(wire):
    require(wire.startswith(b"NSB1NGR1") and len(wire)<=4096,"fresh native owner route")
    q=T.decoded(wire[8:]);require(type(q) is dict and set(q)==CONTROL and type(q["Version"]) is int and q["Version"]==1,"native owner control grammar")
    fixed(q["Cookie"],16);fixed(q["Request"],16);require(type(q["Floors"]) is list and len(q["Floors"])==6,"floor vector")
    for v in q["Floors"]:integer(v)
    integer(q["Index"]);integer(q["Sequence"],128)
    require(len(P.raw(q["Original"]))<=512 and q["Action"] in ("inspect","prepare","submit","recover","open","root","close","original","collect","current","anchor"),"native owner action/bound")
    allowed={"inspect":set(),"prepare":{"Request","Floors"},"recover":{"Original"},"open":{"Index"},"root":{"Index"}}.get(q["Action"],set())
    for k in {"Request","Floors","Index","Original"}-allowed:
        require(not any(q[k]) if type(q[k]) is list else not q[k],"ignored owner control field")
    if q["Action"]=="inspect":require(not any(q["Cookie"]) and q["Sequence"]==0,"inspect guessed owner")
    if q["Action"]=="prepare":fixed(q["Request"],16,True)
    if q["Action"]=="recover":require(bool(P.raw(q["Original"])),"recover lacks actual original")
    if q["Action"] in ("open","root"):integer(q["Index"],minimum=1)
    require(wire[8:]==json.dumps(q,separators=(",",":"),ensure_ascii=False).encode(),"noncanonical native owner request")
    return q


def calls(data,scope,old_cert,current_cert):
    require(type(data["Calls"]) is list and len(data["Calls"])==integer(data["OwnerCalls"],128,1),"whole-query owner calls including unknown/refused")
    by_pid={};seen=[];lost=[];original=None;installed=None;late=None;held=None;blocked=None;closed=False;completed=False;last_section=0;plan=None;anchor=None;expired=False;recovery=None
    active_pid=None;counts={};sequences={};pages={};bad_cookie=0
    for number,row in enumerate(data["Calls"]):
        require(type(row) is dict and set(row)=={"Member","Request","Response","Lost"} and row["Member"]==1 and type(row["Lost"]) is bool,"actual native call record")
        q=control(P.raw(row["Request"]));action=q["Action"];seen.append(action)
        if row["Lost"]:
            require(action=="collect" and len(P.raw(row["Response"]))==381 and not any(P.raw(row["Response"])) and closed and not lost and active_pid is not None and q["Sequence"]==sequences[active_pid]+1 and fixed(q["Cookie"],16)==by_pid[active_pid][0],"only actual selected collection may lose process");lost.append(number);counts[active_pid]+=1;continue
        r=response(row["Response"])
        require(not r["outcome"] and not r["changes"] and (action=="submit" or not r["packets"]),"control fabricated business/quorum output")
        if not r["aux"]:
            require(lost and action=="recover" and r["code"]==3 and P.raw(q["Original"])==original and len(by_pid)==2 and fixed(q["Cookie"],16)==next(iter(by_pid.values()))[0] and q["Sequence"]==1 and bad_cookie==0,"unexpected native error/refusal");counts[active_pid]+=1;bad_cookie+=1;continue
        value=T.decoded(r["aux"]);require(len(r["aux"])<16384 and type(value) is dict and set(value)==RECEIPT and value["Version"]==1 and value["Profile"]==PROFILE and fixed(value["Run"],16,True)==fixed(data["Run"],16,True) and value["Member"]==1 and integer(value["PID"],minimum=1) and value["DeadlineUnixNanos"]==data["DeadlineUnixNanos"],"actual native owner receipt")
        require(value["Sequence"]==q["Sequence"] and value["Action"]==action and value["State"] in ("view-held","no-view"),"native owner request/response association")
        pid=value["PID"];cookie=fixed(value["Cookie"],16,True);native_calls=integer(value["Calls"],128,1)
        for field in ("GrantedRows","GrantedBytes","GrantedOutput"):integer(value[field])
        if pid not in by_pid:
            require(action=="inspect" and len(by_pid)<2 and native_calls==1 and cookie not in [o[0] for o in by_pid.values()],"new owner without actual inspection");by_pid[pid]=(cookie,native_calls,value["GrantedRows"],value["GrantedBytes"]);counts[pid]=0;sequences[pid]=0;pages[pid]=0;active_pid=pid
        require(pid==active_pid,"returned retired process owner")
        counts[pid]+=1
        require(native_calls==counts[pid] and (q["Sequence"]==0 if action=="inspect" else q["Sequence"]==sequences[pid]+1),"owner invocation/sequence reset or refund")
        if action!="inspect":sequences[pid]=q["Sequence"]
        expected_cookie,_,row_grant,byte_grant=by_pid[pid]
        require(cookie==expected_cookie and (action=="inspect" or fixed(q["Cookie"],16,True)==cookie) and value["GrantedRows"]==native_calls*row_grant and value["GrantedBytes"]==native_calls*byte_grant and value["GrantedOutput"]==native_calls*(2*(2<<20)+2*16384) and row_grant>0 and byte_grant>0,"spent finite source/serialization grants")
        require(type(value["Pages"]) is int and 0<=value["Pages"]<=64 and type(value["Found"]) is bool,"bounded native page owner")
        if action=="collect":pages[pid]+=1
        require(value["Pages"]==pages[pid],"attempted native collection page refunded/reset")
        work=value["Work"];require(type(work) is dict and set(work)=={"Records","Bytes"} and integer(work["Records"])>=0 and integer(work["Bytes"])>=0,"actual native work")
        if action=="prepare":
            require(r["code"]==0 and original is None,"prepare replayed another factory");original=P.raw(value["Original"]);parsed=ggc(original,scope)
            require(parsed["floors"]==[current_cert["source"],current_cert["source"],current_cert["source"]+1,current_cert["source"],current_cert["source"],current_cert["index"]] and parsed["configuration"]==current_cert["configuration"],"genuine admitted floors/certificate config")
        elif action=="submit":require(r["code"]==0 and bool(r["packets"]) and P.raw(value["Original"])==original,"no actual proposal/quorum packets")
        elif action=="open" and not lost:
            require(r["code"]==0 and not late and root(value["Root"])==old_cert["image"] and value["Root"]["Index"]==old_cert["source"] and value["ViewIndex"]>=old_cert["source"],"genuine pre-commit captured old root");late=value
        elif action=="original":
            require(r["code"]==0 and value["Found"] is True and value["CDC"] is not None and not P.raw(value["CDC"]),"actual installed original/empty CDC");installed=ggo(P.raw(value["Outcome"]))
            require(installed["id"]==hashlib.sha256(original).digest() and installed["index"]>late["ViewIndex"] and installed["source"]>=late["ViewIndex"],"floor committed after genuine reader acquisition")
        elif action=="root":require(r["code"]==0 and installed is not None and root(value["Root"])==root(late["Root"]),"floor revoked captured historical root");held=value
        elif action=="collect" and not lost:
            require(held is not None and r["code"]==2 and r["mask"]&(1<<2) and work=={"Records":0,"Bytes":0} and value["Progress"]["PlanID"]==[0]*32,"native owner did not block collection");blocked=value
        elif action=="close":require(r["code"]==0 and value["State"]=="no-view","native reader release failed");closed=True
        elif action=="recover":
            require(r["code"]==0 and lost and value["Found"] is True and P.raw(value["Original"])==original and ggo(P.raw(value["Outcome"]))==installed and value["CDC"] is not None and not P.raw(value["CDC"]),"same installed original changed after restart");recovery=value
        elif action=="collect":
            p=value["Progress"];require(recovery is not None and r["code"]==0 and type(p) is dict and set(p)==PROGRESS and fixed(p["PlanID"],32)==installed["id"] and p["SourceIndex"]==installed["source"] and p["InstalledIndex"]==installed["index"] and type(p["Complete"]) is bool and last_section<=integer(p["Section"],6)<=6 and work["Records"]<=16 and work["Bytes"]<=1<<20,"actual persisted cursor under same installed plan")
            # These are the currently owned cursor row's storage bytes/count,
            # not cumulative work. Native EOF retires that row to exactly zero.
            records,bytes_=integer(p["CursorRecords"],1),integer(p["CursorBytes"],1<<20)
            require(not completed and integer(p["Generation"],minimum=1) and (records==0)==(bytes_==0),"cursor ownership or pages after EOF")
            last_section=p["Section"];completed=p["Complete"];require(completed==(last_section==6),"native EOF section mismatch")
            require(not completed or records==bytes_==0,"native EOF retained cursor row")
        elif action in ("current","anchor"):
            require(r["code"]==0 and completed and type(value["Plan"]) is dict and set(value["Plan"])==PLAN,"current/anchor before true GC EOF")
            require(fixed(value["Plan"]["ID"],32)==installed["id"] and value["Plan"]["SourceIndex"]==installed["source"] and value["Plan"]["InstalledIndex"]==installed["index"] and fixed(value["Plan"]["SourceHash"],32)==installed["source_hash"],"authenticated native plan changed")
            root(value["Root"])
            if action=="current":plan=value
            else:
                require(len(value["Plan"]["Roots"])==1,"exact semantic image anchor");a=value["Plan"]["Roots"][0];require(set(a)=={"Index","ImageHash","Data"} and a["Data"] is False and value["Root"]["Index"]==a["Index"]<current_cert["source"] and value["Root"]["ImageHash"]==a["ImageHash"],"image anchor falsely retained whole old data cut");anchor=value
        elif action=="open":require(completed and r["code"]==9 and q["Index"]==old_cert["source"],"below-floor root refused with wrong class");root(value["Root"],empty=True);expired=True
        else:require(action=="inspect" and r["code"]==0,"unknown native control/result")
    require(len(by_pid)==2 and len(lost)==1 and installed is not None and blocked is not None and closed and completed and anchor is not None and expired and seen[-1]=="close","incomplete genuine owner/reader/floor/restart lifecycle")
    require(plan is not None,"missing actual retained plan/current root")
    require(seen[:12]==["inspect","prepare","submit","open","original","root","collect","close","collect","inspect","recover","recover"] and seen[-4:]==["current","anchor","open","close"] and 1<=len(seen[12:-4])<=64 and all(a=="collect" for a in seen[12:-4]) and bad_cookie==1,"exact finite reader/restart/page lifecycle")
    require(plan["Plan"]==anchor["Plan"] and [plan["Plan"][k] for k in ("RootFloor","DataFloor","FeedFloor","OutcomeFloor","RecoveryFloor")]==[current_cert["source"],current_cert["source"],current_cert["source"]+1,current_cert["source"],current_cert["source"]],"same genuine native floor/anchor plan")
    return original,installed,plan,by_pid


def check(data):
    require(type(data) is dict and set(data)==HEADER and data["Version"]==1 and data["Profile"]==PROFILE and data["Side"] in ("before","after"),"fresh native retention artifact")
    run,deadline=fixed(data["Run"],16,True),integer(data["DeadlineUnixNanos"],(1<<63)-1,1)
    scopes,old,old_round,old_entity=point(data["Old"],5)
    actual,current,current_round,current_entity=point(data["Current"],6)
    reopened,reopened_certs,reopened_round,reopened_entity=point(data["Reopened"],6)
    require(scopes==actual==reopened and reopened_certs==current and reopened_round==current_round and current_entity==reopened_entity and old[3]["source"]<current[3]["source"],"actual distinct old/current cut and same-cut reopen")
    original,installed,plan,owners=calls(data,scopes[3],old[3],current[3])
    require(data["Arm"]["Control"]["Kind"]==7 and data["Arm"]["Control"]["Side"]==data["Side"],"actual collection Sync selector")
    ident=S.pair(data["Arm"],data["State"],run,deadline,member=1)
    require(fixed(ident["PlanID"],32)==installed["id"] and ident["SourceIndex"]==installed["source"] and fixed(ident["SourceImageSHA256"],32)==installed["source_hash"] and 1<=ident["Section"]<=6 and ident["Final"] is (ident["Section"]==6) and not any(ident["SourceCutID"]+ident["ManifestID"]),"actual local cursor/plan identity, not source/portable authority")
    require(data["ReceiptReads"]==1 and data["ReceiptGrantBytes"]==131072,"prepaid receipt read/authentication")
    before,local,after=[response(data[k]) for k in ("BeforeState","LocalState","AfterState")]
    require(before["code"]==local["code"]==after["code"]==0 and before["applied"]==local["applied"]==after["applied"]==installed["index"] and before["image"]==local["image"]==after["image"]==root(plan["Root"]) and hashlib.sha256(before["image"]).digest()==installed["source_hash"] and before["stats"][0]==local["stats"][0]==after["stats"][0] and local["stats"][7]==after["stats"][7]==0 and after["stats"][19]<before["stats"][19],"real physical collection/current/reader cleanup and logical state preservation")
    require(type(data["Processes"]) is list and len(data["Processes"])==2,"actual native killed/replacement pair")
    failed,restored=data["Processes"];kill=T.decoded(P.raw(failed["Raw"]));restore=T.decoded(P.raw(restored["Raw"]))
    require(failed["Action"]=="native-state-transition-kill" and failed["Member"]==kill["Member"]==1 and failed["Signal"]=="SIGKILL" and failed["Reaped"] is True and kill["Receipt"]==data["State"] and kill["PID"]==data["State"]["PID"] and kill["Signaled"] is True and kill["SignalNumber"]==9 and kill["Reaped"] is True and kill["ExitCode"]==-1 and failed["Volume"]==kill["Volume"],"actual matched native GC self-kill/reap")
    require(restored["Action"]=="native-retention-local-reopen" and restored["Ready"] is True and restore["Ready"] is True and restored["Member"]==restore["Member"]==1 and restore["PID"]!=kill["PID"] and restored["PreviousInstance"]==restore["PreviousInstance"]==failed["Instance"] and restored["Volume"]==restore["Volume"]==failed["Volume"] and response(restore["LocallyRestoredRaw"])["incarnation"]==local["incarnation"] and set(owners)=={kill["PID"],restore["PID"]},"same-directory actual local restored native authority")
    require(type(data["Attempts"]) is list and len(data["Attempts"])==5,"exact five useful originals")
    descriptors=[original_attempt(a,scopes) for a in data["Attempts"]]
    require([d["ordinal"] for d in descriptors]==[1,2,3,4,5] and len({d["tx"] for d in descriptors})==5,"declared useful operation/original identity")
    prefix(data,descriptors,installed)
    update=descriptors[-1]["decision"];require(old_entity["property"]["revision"]==descriptors[-2]["decision"]["round"] and current_entity["property"]["revision"]==update["round"] and old_round<update["round"]<=current_round,"old/current useful transaction cut membership")
    initial={"A/edge7/value":5,"A/edge7/life":8,"B/edge7/target":9};final={**initial,"A/edge7/value":6}
    transaction={"update":{"reads":[P.read_key(initial,"A/edge7/value")],"writes":P.writes(initial,final,{"A/edge7/value"})}}
    witness,detail=P.oracle()({"initial":initial,"transactions":transaction},{"update"},{"update":update["round"]},[{"event":"old","scope":["A","B"],"round":old_round,"values":initial},{"event":"current","scope":["A","B"],"round":current_round,"values":final},{"event":"reopened","scope":["A","B"],"round":reopened_round,"values":final}],[{"event":"final","scope":["A","B"],"values":final}])
    T.semantic(witness is not None,"independent exact serial/cut witness: "+str(detail))
    return dict(side=data["Side"],records=len(data["Records"]),owner_calls=data["OwnerCalls"],serial_witness=witness,scope="actual AS7 member1 physical GC before/after Sync under genuine all-A floor; explicit finite reader/original/cursor proof, no whole-D or runtime qualification")


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("artifacts",nargs="+",type=Path);args=parser.parse_args()
    require(len(args.artifacts)<=2,"finite native retention invocation");print(json.dumps([check(load(p)) for p in args.artifacts],indent=2))


if __name__=="__main__":main()
