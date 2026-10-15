#!/usr/bin/env python3
"""Independent native BOTH-HOME original/cut/column/feed transfer joins.

All growing joins use the caller's finite BLOB spool. Movement is authenticated
native lineage, never a new business row or a claim of equal physical FDB costs.
"""
from contextlib import nullcontext
import hashlib
import os
from pathlib import Path
import stat
import struct

import native_feed_check as scalar
import native_feed_retained_check as retained
import native_create_update_check as guarded
import native_selectors_check as selector
from comparison_spool import Spool,u64

codec,raw,column=scalar.codec,scalar.raw,scalar.column
require,integer,fields,blob,array,checked,cursor,digest,decoded,encoded,lines=(scalar.require,scalar.integer,scalar.fields,scalar.blob,scalar.array,scalar.checked,scalar.cursor,scalar.digest,scalar.decoded,scalar.encoded,scalar.lines)
FAMILY,FORMAT="transfer-retained-pins","native-both-home-transfer-workload-v1"
LINE,TRANSPORTS,MAX_SECONDS=scalar.LINE,scalar.TRANSPORTS,scalar.MAX_SECONDS
AGREEMENT=retained.AGREEMENT
for _extension in (b"range-move=compact-recovery-spans-v1\0retention=immutable-native-reference-v1\0replay=bounded-original-batches32-v1\0",
                   b"graph=bounded-native-rmp-batches32-component8-family-boundary-v1\0progress=gmp5-native-graph-count-v1\0",
                   b"range-move=hard-pooled-retained-v1\0undo=before-image-debt-v1\0capacity=nonterminal-exact-original-v1\0"):
    AGREEMENT=digest(AGREEMENT+_extension)
MINIMUM_CONTRACT={"snapshotHeldThrough":"handoff-target-restart-old-owner-reclamation","snapshotAfterRelease":"feed-token-refused",
                  "feedAfterRelease":"complete-unread-originals","originalRetry":"same-result-and-ids"}


def shape(s):
    fields(s,"version family facts crossPercent writers layout")
    require(integer(s["version"],2,2) and s["family"]==FAMILY and integer(s["facts"],20,1_000_000)%20==0
            and integer(s["crossPercent"]) in (0,10,50,100) and integer(s["writers"]) in (1,8,32)
            and s["facts"]>s["writers"] and s["layout"] in ("kv","columns"),"explicit actual Cost12 dimensions, no legacy upgrade")
    return s


def validate_grants(g):
    fields(g,"MaxPages DescribeBytes ImportBytes UndoBytes")
    p=integer(g["MaxPages"],1,4_000_000)
    d,i,u=[integer(g[k],65537 if k!="UndoBytes" else 1,1<<40) for k in ("DescribeBytes","ImportBytes","UndoBytes")]
    cap=(64<<10)+16*(64<<10)+16384;window=64*(4<<20)
    require(2*p+6<=10_000_000 and 82086*p+1083890<=1<<40 and d<=(128<<10)*(p+1)
            and i<=cap*p and u<=cap*p and d+1376256+window<=1<<40 and i+u+3473408+window<=1<<40,
            "separate finite source/import/undo/count/log/native-pending-window admission; not a workload fit claim")
    return {k:g[k] for k in ("MaxPages","DescribeBytes","ImportBytes","UndoBytes")}


def frames(s,g):
    n=shape(s)["facts"];p=validate_grants(g)["MaxPages"]
    return 10*(16*n+8)+640+5*(64*n+1024)+4*p+16*((n+15)//16)+16384+128


def cut(wire):
    result=codec.cut(wire)
    require(len(result)==2 and [c["partition"] for c in result]==[3,8] and all(c["scope"][88:]==AGREEMENT for c in result),"actual pooled Recovery4/Contract7 full-cut agreement")
    return result


def placement(s,phase,fact):
    if phase=="setup":return [8] if fact==2 else [3]
    if phase=="update" and fact>0:return [8]
    return [3,8] if scalar.cross(s,fact) else [3]


def operation(s,q):return scalar.operation(s,q,version=12)


def manifest(wire):
    """Exact actual GCM1/GPK1; genuine one-participant Coordinate is permitted."""
    c=cursor(checked(wire,b"GCM\1",32768));tx,coordinator,original=c.take(16),c.number(),c.field(16384)
    parsed=codec.typed.pgq(original);hashes=[c.take(32) for _ in range(4)]
    require(coordinator==parsed["partition"]==3 and tx==digest(b"rho-tkg:cross-transaction-id:v1\0"+bytes.fromhex(raw.GRAPH)+u64(coordinator)+parsed["request"])[:16]
            and hashes[:3]==[parsed[k] for k in ("configuration","declaration","routing")] and all(any(h) for h in hashes),"actual original manifest request/configuration")
    floors={};writes={};kernels={}
    for _ in range(integer(c.number(4),1,2)):
        part,floor,kernel=c.number(),c.number(),c.field(32768)
        require(part in (3,8) and (not floors or part>max(floors)),"ordered genuine participant roster")
        k=cursor(checked(kernel,b"GPK\1",32768))
        require(k.take(16)==tx and k.take(16).hex()==raw.GRAPH and k.number()==part and k.number()>0 and [k.take(32) for _ in range(4)]==hashes,"actual kernel identity/full immutable descriptor")
        effect,revision,provenance=k.take(32),k.number(),k.number();guard=k.field(32768);effect_body=k.field(32768);k.done()
        require(revision>floor and provenance>0 and digest(effect_body)==effect and guard,"actual complete prospective kernel/effect seal")
        floors[part]=floor;writes[part]=effect_body;kernels[part]=kernel
    c.done();require(coordinator in floors,"coordinator missing native participation")
    return dict(tx=tx,coordinator=coordinator,original=original,request=parsed["request"],digest=digest(wire),floors=floors,kernels=writes,nativeKernels=kernels,condition=None)


def manifest_check(wire,request_,tx,hash_,proposal):
    m=manifest(wire)
    require((m["request"],m["tx"],m["digest"])==(request_,tx,hash_) and m["original"][4:141]==proposal[4:141]
            and codec.typed.operations(m["original"],allow_present_correction=True)==codec.typed.operations(proposal,allow_present_correction=True),"same immutable business/claims semantics through genuine native recapture")


def move_command(wire):
    c=cursor(checked(wire,b"GMQ\3",1<<20));scope,phase,sequence=c.take(120),c.number(1),c.number()
    original,data=c.field(1024),c.field((1<<20)-2048)
    intent=column.pooled_move_intent(raw,original,AGREEMENT)
    proofs=[column.pooled_move_status(raw,c.field(256)) for _ in range(integer(c.number(4),0,2))];c.done()
    require(scope in (intent["source"],intent["target"]) and 1<=phase<=9 and sequence<=intent["pages"]
            and all(p["move"]==intent["move"] and p["intent"]==intent["digest"] for p in proofs),"actual bounded pooled command/intent/proof roster")
    return {"scope":scope,"phase":phase,"sequence":sequence,"intent":intent,"original":original,"data":data,"proofs":proofs}


def logical(frame,terminal,previous,spool):
    scalar.logical(frame,terminal,previous,spool,guarded_updates=True,operation_version=12,
                   placement=lambda phase,fact:placement(terminal["spec"],phase,fact))


def retention_original(command,out,part):
    c=cursor(checked(command,b"GGC\1",512));scope,issuer,request_=c.take(120),c.take(16),c.take(16)
    configuration,previous=c.take(32),c.take(32);floors=[c.number() for _ in range(6)];c.done()
    b=checked(out,b"GGO\1",256)
    require(len(b)==89 and scope[:16].hex()==raw.GRAPH and scope[16:24]==u64(part) and scope[88:]==AGREEMENT and any(issuer) and any(request_) and any(configuration)
            and b[0] in (1,2,3) and b[1:33]==digest(command) and any(b[33:65]),"actual pooled native GC original, not a caller floor")
    index,source,round_=struct.unpack_from(">QQQ",b,65)
    require(0<source<index,"actual original retention predecessor")
    return dict(scope=scope,issuer=issuer,request=request_,configuration=configuration,previous=previous,floors=floors,id=b[1:33],sourceHash=b[33:65],index=index,source=source,round=round_,status=b[0])


def original_index(out):
    if out.startswith(b"GMO\3"):return column.pooled_move_status(raw,out)["index"]
    return retained.result_index(out)


def authenticate_frame(frame,previous,s,spool,fresh,before_ack,ack):
    fields(frame,"Partition Index Root Outcome CDC Registration OriginalOutcome OriginalCDC Rows")
    part,index=integer(frame["Partition"]),integer(frame["Index"],1)
    out,cdc=blob(frame["Outcome"]),blob(frame["CDC"]);column.root(raw,array(frame["Root"],172),part)
    if out.startswith(b"GMO\3"):
        if not codec.optional(frame["Registration"]):
            s_=column.pooled_move_status(raw,out)
            command=move_command(spool.lookup(b"move-command",u64(part)+u64(index)))
            require(s_["phase"]==1 and s_["result"] in (2,3) and s_["index"]==index and s_["move"]==command["intent"]["move"] and s_["intent"]==command["intent"]["digest"]
                    and not cdc and not frame["Rows"] and array(frame["Root"],172)==previous,"uninstalled fence observation creates no authority/effects")
            return
        status,intent=column.pooled_move_frame(raw,frame,previous,AGREEMENT,spool)
        require(intent["pages"]==int.from_bytes(spool.lookup(b"transfer-grants",b"pages"),"big") and status["result"]!=5,"budget refusal is a failed mandatory cell, never completed transfer")
        if fresh and cdc:scalar.put(spool,b"move-lineage",status["move"]+u64(part),out+cdc)
        return
    if before_ack:
        original=original_index(out)
        require(original is not None and 0<original<ack and original<index and not cdc and not frame["Rows"] and array(frame["Root"],172)==previous,"ACK-prefix original is genuine and below actual floor")
        old=decoded(spool.lookup(b"frame",u64(part)+u64(original)))
        require(retained.same_outcome(out,blob(old["Outcome"])) and blob(frame["OriginalOutcome"])==blob(old["Outcome"])
                and codec.optional(frame["OriginalCDC"]) in (b"",blob(old["CDC"])),"no fabricated expiry or original substitution")
        return
    if out.startswith(b"GRO\2"):
        b=checked(out,b"GRO\2",120);original=int.from_bytes(b[73:81],"big")
        proof=spool.find(b"allocation-original",u64(part)+u64(original))
        if proof is not None:
            actual=decoded(proof)
            require(out==blob(actual["Outcome"]) and not cdc and not frame["Rows"] and original==index
                    and array(frame["Root"],172)==blob(actual["Root"]),"exact allocation original/root, never business effects")
            return
    if out.startswith((b"GFO\2",b"GGO\1")):
        original=original_index(out)
        if original<index:
            old=decoded(spool.lookup(b"frame",u64(part)+u64(original)))
            require(blob(old["Outcome"])==out and blob(frame["OriginalOutcome"])==out and not cdc and not frame["Rows"] and array(frame["Root"],172)==previous,"retained metadata SAME original")
            return
        require(original==index and not cdc and not frame["Rows"] and array(frame["Root"],172)==previous,"zero-row native retention/feed control")
        c=cursor(checked(blob(frame["Registration"]),b"NCO1"));key,at,value=c.field(4096),c.number(),c.field(32768);c.done()
        require(len(key)==59 and key[:2]==b"ga" and key[3:19].hex()==raw.GRAPH and key[19:27]==u64(part) and at==index,"same exact native immutable control")
        if out.startswith(b"GFO\2"):
            b=checked(out,b"GFO\2",256);x=checked(value,b"GFX\1",512)
            require(len(b)==130 and b[128]==5 and b[129] in (1,3,4,5,6) and key[2]==35 and key[-16:]==b[96:112] and x==b[32:64]+out,"actual irreversible feed-only original")
            if fresh:scalar.put(spool,b"feed-result",u64(part)+b[32:64],out)
        else:
            c=cursor(checked(value,b"GGX\1",1024));command,result=c.field(512),c.field(256);c.done();r=retention_original(command,out,part)
            require(result==out and key[2]==25 and key[27:43]==r["issuer"] and key[43:]==r["request"] and r["source"]==index-1 and r["sourceHash"]==digest(previous),"actual native sparse plan source root/hash")
            if fresh:scalar.put(spool,b"retention-original",u64(part)+r["id"],command+out)
        return
    scalar.authenticate_frame(frame,previous,s,spool,fresh,guarded_updates=True,logical_check=logical,manifest_decode=manifest)


def moves(directory,end,spool):
    total=0;owned=0
    for row in lines(directory/"moves.jsonl",8*(4*end["Admission"]["Grants"]["MaxPages"]+128)):
        fields(row,"Ordinal Origin Partition Request Reply CommandSHA256")
        total+=1;require(integer(row["Ordinal"],1)==total and integer(row["Origin"],1,6) and row["Partition"] in (3,8),"complete actual move-submit stream")
        request_,reply=blob(row["Request"]),blob(row["Reply"]);owned+=512+2*(len(request_)+len(reply))
        require(owned<=1<<40,"actual all-move diagnostic byte grant")
        c=cursor(checked(request_,b"GSQ\1",2<<20));kind,sequence,nonce=c.number(1),c.number(),c.take(16)
        cookie=c.take(40);maximum,rows=c.number(4),c.number(4)
        key,upper,after=[c.field(65536) for _ in range(3)];proposal=c.field(1<<20);tx,phase=c.take(16),c.number(1);c.done()
        require(kind==8 and sequence>0 and any(nonce) and cookie==bytes(40) and not key+upper+after and not any(tx) and phase==0 and rows==0 and 0<maximum<=2<<20,"actual native Submit source request, not a printable command")
        p=cursor(checked(proposal,b"GSC\1",1<<20));scope=checked(p.field(256),b"APS\1",156);command=p.field(1<<20);p.done()
        parsed=move_command(command);part=int.from_bytes(scope[16:24],"big")
        require(scope==parsed["scope"] and part==row["Partition"] and array(row["CommandSHA256"],32)==digest(command),"full command bytes/target identity")
        c=cursor(checked(reply,b"GSR\1",2<<20));actual_scope=checked(c.field(256),b"APS\1",156)
        voter,issuer,rkind,rsequence,rnonce,mask=c.number(),c.take(16),c.number(1),c.number(),c.take(16),c.number(2)
        body=c.field((2<<20)-512);c.done()
        require(actual_scope==scope and voter==(1 if part==3 else 4) and any(issuer) and (rkind,rsequence,rnonce)==(kind,sequence,nonce),"actual configured source reply/nonce/sequence")
        scalar.put(spool,b"move-submit",digest(command),command)
        if mask:
            require(not body,"refused source cannot invent a result")
            continue
        status=column.pooled_move_status(raw,body)
        require(status["partition"]==part and status["move"]==parsed["intent"]["move"] and status["intent"]==parsed["intent"]["digest"],"actual immutable move original result")
        if status["result"]==5:
            require(status["failedCommand"]==digest(command),"exact attempted capacity refusal")
            raise ValueError("native transfer mandatory success refused by authentic pooled capacity")
        scalar.put(spool,b"move-command",u64(part)+u64(status["index"]),command)
        scalar.put(spool,b"move-submitted-status",u64(part)+u64(status["index"]),body)
        scalar.put(spool,b"move-intent",status["move"],parsed["original"])
    require(total>0,"missing actual Submit/GMQ3 stream")
    return {"actualSubmittedFrames":total,"diagnosticByteCharge":owned}


def sources(directory,end,spool):
    stages=("full-before-held-gc","after-held-gc-suffix","post-gc-final-suffix")
    initial=cut(end["Initial"]);floors=[integer(v,1) for v in end["SparseFrom"]]
    stage=group=0;after=initial[0]["sourceIndex"];previous=initial[0]["image"];counts=[[0,0] for _ in stages];tip=None;distinct_rows=0
    for row in lines(directory/"sources.jsonl",6*(end["FramesCeiling"]+1)):
        require(stage<3 and row["Stage"]==stages[stage] and row["Member"]==1+3*group,"exact complete/sparse/reopen source stages")
        outer=row["Page"];fields(outer,"Page BeforeACK");p=outer["Page"];part=3+5*group
        fields(p,"Partition After Next Through Root Complete Frames")
        require(p["Partition"]==part and p["After"]==after and integer(p["Through"],after)>=integer(p["Next"],after)
                and p["Next"]-after==len(p["Frames"])<=4 and len(outer["BeforeACK"])==len(p["Frames"])
                and p["Complete"] is (p["Next"]==p["Through"]),"bounded exact native prefix/EOF")
        if tip is None:tip=(p["Through"],p["Root"])
        require((p["Through"],p["Root"])==tip and len(row["Voters"])==3*len(p["Frames"]),"immutable page tip and every voter")
        for offset,f in enumerate(p["Frames"]):
            require(f["Partition"]==part and f["Index"]==after+offset+1 and type(outer["BeforeACK"][offset]) is bool,"source gap/forged expiry")
            for v in range(3):require(row["Voters"][3*offset+v]=={"Member":1+3*group+v,"Index":f["Index"],"Outcome":f["Outcome"],"CDC":f["CDC"]},"actual voter original/CDC equality")
            key=u64(part)+u64(f["Index"]);prior=spool.find(b"frame",key)
            if prior is not None:
                old=decoded(prior)
                require(all(f[n]==old[n] for n in ("Partition","Index","Root","Outcome","CDC","Registration","Rows")),"reopen changed native frame/lineage")
            ack=integer(end["GC"][group*3]["Plan"]["FeedFloor"],1)
            authenticate_frame(f,previous,end["spec"],spool,prior is None,outer["BeforeACK"][offset],ack)
            if prior is None:
                spool.insert(b"frame",key,encoded(f));spool.insert(b"original",key,digest(blob(f["Outcome"]))+digest(blob(f["CDC"])))
                distinct_rows+=len(f["Rows"] or [])
            counts[stage][group]+=len(f["Rows"] or []);previous=array(f["Root"],172)
        after=p["Next"]
        if p["Complete"]:
            require(previous==blob(p["Root"]),"actual native original/root EOF")
            spool.insert(b"source-eof",bytes([stage,group]),u64(after));group+=1;tip=None
            if group==2:stage,group=stage+1,0
            if stage<3:
                after=initial[group]["sourceIndex"] if stage==0 else end["HeldSparseFrom"][group] if stage==1 else floors[group]
                previous=initial[group]["image"] if stage==0 else array(decoded(spool.lookup(b"frame",u64(3+5*group)+u64(after)))["Root"],172)
        else:require(p["Frames"],"missing source EOF/stalled page")
    require(stage==3 and counts==end["SourceRows"] and distinct_rows==3+4*end["spec"]["facts"] and sum(counts[0])==3+3*end["spec"]["facts"],"complete global business history once; movement adds zero business rows")
    n=end["spec"]["facts"]
    for ordinal in range(1,2*n+4):
        a=decoded(spool.lookup(b"useful",u64(ordinal)));phase,fact,_=operation(end["spec"],a["Operation"])
        owner=(1,3,2*n+7)[fact] if phase=="setup" else 7+2*fact
        for component in ((1,) if phase=="setup" else (1,3)):spool.lookup(b"row",u64(ordinal)+u64(owner)+bytes([component]))
    for tx,value in spool.walk(b"terminal"):
        a=decoded(value);m=manifest(blob(a["Manifest"]));d=codec.status(blob(a["Outcome"]))
        coordinator=m["coordinator"]
        require(spool.lookup(b"phase",u64(coordinator)+tx+b"\3")==blob(a["Outcome"]) and codec.status(spool.lookup(b"phase",u64(coordinator)+tx+b"\1"))["digest"]==d["digest"],"native original Register/Decide complete association")
        for part,floor in m["floors"].items():
            prepare=spool.find(b"phase",u64(part)+tx+b"\2");resolve=spool.find(b"phase",u64(part)+tx+b"\4")
            if d["commit"]:
                require(prepare is not None and resolve is not None,"committed original missing participant")
                p,r=codec.status(prepare),codec.status(resolve)
                require(p["yes"] and p["floor"]>=floor and d["round"]>p["floor"] and r["commit"] and r["round"]==d["round"],"actual voted round/complete installation")
            elif resolve is not None:require(not codec.status(resolve)["commit"],"ABORT acquired business effects")
    return counts


def controls(directory,end):
    """One real owner counter; bounded SAME retries keep the entire input."""
    call=attempt=elapsed=extra=0;prior=None;settled=True;move_owners={};active_move=None
    maximum=6*(8*end["FramesCeiling"]+end["spec"]["facts"]//8+end["Admission"]["BudgetSeconds"]//60+2048)+128*14
    for row in lines(directory/"controls.jsonl",8*maximum):
        if row.get("Type")=="whole-observation-timing":
            fields(row,"Stage Type Rows WallNanos");integer(row["Rows"]);integer(row["WallNanos"],1);yield row;continue
        fields(row,"Stage Member Call Attempt Input Reply AttemptNanos OperationNanos"+(" Error" if "Error" in row else ""))
        q,r=row["Input"],row["Reply"];number=integer(row["Call"],1,maximum);n=integer(row["Attempt"],1,8)
        fields(q,"Version Action Request Sequence Count Cursor Range Origin Original Key Control Feed Query")
        fields(r,"Version Action Body Nanos"+(" Failure" if "Failure" in r else ""))
        require(integer(row["Member"],1,6) and integer(q["Version"],1,1) and integer(r["Version"],0,1) and integer(r.get("Failure",0),0,9)>=0,"actual native wrapper identity")
        if number==call:
            require(prior is not None and prior.get("Error") and encoded(q)==encoded(prior["Input"]) and n==attempt+1,"unknown retry cannot replace input/original/request")
            extra+=1
        else:
            require(settled and number==call+1 and n==1,"control call/attempt omission or unresolved previous original")
            call,attempt,elapsed=number,0,0
        require(n==attempt+1 and 0<integer(row["AttemptNanos"])<=integer(row["OperationNanos"]) and row["OperationNanos"]>=elapsed,"complete cumulative operation retry/wait timing")
        attempt,elapsed=n,row["OperationNanos"];prior=row
        if not row.get("Error"):
            require(r["Version"]==1 and r["Action"]==q["Action"] and r.get("Failure",0)==0,"actual successful wrapper result")
            settled=True
        else:
            settled=r.get("Failure",0) in (7,8,9)
        if q["Action"]=="move-prepare" and not row.get("Error"):
            intent=column.pooled_move_intent(raw,blob(r["Body"]),AGREEMENT)
            require(intent["request"]==array(q["Request"],16) and intent["range"]==q["Range"] and intent["move"] not in move_owners and len(move_owners)<2,"exact immutable prepared move authority")
            move_owners[intent["move"]]={"original":blob(r["Body"]),"intent":intent,"latest":None,"states":[None,None],"stalled":0,"finished":False}
        elif q["Action"] in ("move-run","move-resume"):
            supplied=blob(q["Original"])
            move_=column.pooled_move_intent(raw,supplied,AGREEMENT)["move"] if q["Action"]=="move-run" else move_result(supplied)["move"]
            require(move_ in move_owners and (active_move in (None,move_) or move_owners[active_move]["finished"]),"one genuine HOME owner at a time")
            active_move=move_;owner=move_owners[move_]
            require(not owner["finished"] and supplied==(owner["original"] if owner["latest"] is None else owner["latest"])
                    and q["Action"]==("move-run" if owner["latest"] is None else "move-resume"),"unknown/progress retries preserve exact original/resume bytes")
            result=move_result(blob(r["Body"])) if codec.optional(r["Body"]) else None;progress=False
            if result is not None:
                require(result["move"]==move_ and result["request"]==owner["intent"]["request"] and result["intent"]==owner["intent"]["digest"],"move result cannot replace durable identity")
                for slot,status_ in enumerate(result["states"][:2]):
                    if status_ is None:continue
                    require(status_["phase"]!=9 and status_["result"]!=5,"safe terminal/capacity refusal fails mandatory cell")
                    if status_["result"]!=1:continue
                    old=owner["states"][slot]
                    require(status_["partition"]==3+5*slot and status_["sequence"]<=owner["intent"]["pages"] and (old is None or status_["index"]>=old["index"] and (status_["phase"],status_["sequence"])>=(old["phase"],old["sequence"])),"native semantic page/phase progression, not increasing blocked physical index")
                    if old is None or (status_["phase"],status_["sequence"])>(old["phase"],old["sequence"]):owner["states"][slot]=status_;progress=True
                done=not row.get("Error") and result["pending"]==result["mask"]==0 and result["committed"]==1 and all(s is not None and s["phase"]==8 and s["result"]==1 for s in result["states"][:2])
                if done:owner["finished"]=True
                else:require(row.get("Error") and (result["pending"] or result["mask"]),"nominal nonterminal move success refused")
            if not owner["finished"]:
                if progress:owner["stalled"]=0;owner["latest"]=blob(r["Body"])
                else:owner["stalled"]+=1;extra+=1;require(owner["stalled"]<8 and extra<=64,"SAME no-progress bounded global uncertainty pool")
            settled=True
        if r.get("Body") is not None and q["Action"] in ("feed","query","acquire","next","lookup","freeze"):
            decoded(blob(r["Body"]))
        yield row
    require(settled and all(o["finished"] for o in move_owners.values()) and extra==integer(end["ControlExtraInvocations"],0,64),"all bounded wrapper originals settled, no hidden retry reset")


def allocation(directory,end,spool):
    cursors={};count=next_calls=replays=0;successor=False
    for row in controls(directory,end):
        if "Input" not in row or row["Input"]["Action"] not in ("acquire","next"):continue
        if row.get("Error"):continue # Exact SAME retry is authenticated by controls(); no issuance is inferred here.
        q=row["Input"];r=decoded(blob(row["Reply"]["Body"]));member=row["Member"];alias=integer(r["Cursor"],1,3)
        key=(member,alias)
        if q["Action"]=="acquire":
            require(member in (1,4) and key not in cursors and len(cursors)<4,"actual bounded distinct Recipient/Cursor owners")
            request_=r["Request"];fields(request_,"Graph Session Sequence Count");session=request_["Session"];fields(session,"ID Incarnation Epoch")
            session_wire=array(session["ID"],16)+array(session["Incarnation"],16)+u64(integer(session["Epoch"],1))
            require(array(request_["Graph"],16).hex()==raw.GRAPH and request_["Sequence"]==q["Sequence"] and request_["Count"]==q["Count"],"exact original Recipient.Acquire request")
            grant=decoded(spool.lookup(b"installed-grant",session_wire+u64(request_["Sequence"])))
            require(grant["home"]==(3 if member==1 else 8) and (r["First"],r["Last"])==(grant["first"],grant["last"])
                    and r["Last"]-r["First"]+1==q["Count"] and r["Consumed"]==0 and not r.get("IDs"),"Acquire is backed by native installed reservation, not the receipt alone")
            if row["Stage"]=="allocation-successor":require(member==1 and r==end["Successor"] and int.from_bytes(bytes.fromhex(grant["authority"])[16:],"big")>1,"real successor allocator authority/epoch");successor=True
            else:require(row["Stage"] in ("allocation-home","allocation-remote"),"foreign allocation stage")
            cursors[key]={"receipt":r,"grant":grant,"session":session_wire,"sequence":0,"consumed":0,"last":None}
        else:
            require(key in cursors and q["Cursor"]==alias and 1<=q["Count"]<=32,"actual cursor owner and finite Next window")
            c=cursors[key];acquired=c["receipt"]
            require((r["First"],r["Last"])==(acquired["First"],acquired["Last"]),"Next cannot substitute another grant")
            if q["Sequence"]==c["sequence"]:
                require(c["last"]==(q,r),"SAME Next changed reply/consumed suffix");replays+=1;continue
            require(q["Sequence"]==c["sequence"]+1 and r["Consumed"]==c["consumed"]+q["Count"]<=acquired["Request"]["Count"]
                    and len(r["IDs"])==q["Count"],"genuine bounded Cursor.Next exact progress")
            for offset,identity in enumerate(r["IDs"]):
                require(integer(identity,1)==acquired["First"]+c["consumed"]+offset,"issued ID duplicate/gap")
                spool.insert(b"issued",u64(identity),c["session"]+u64(acquired["Request"]["Sequence"]))
                count+=1
            c.update(sequence=q["Sequence"],consumed=r["Consumed"],last=(q,r));next_calls+=1
    homes=(6+2*end["spec"]["facts"]+(1<<20)-1)//(1<<20)
    require(successor and len(cursors)==homes+2 and count==2*end["spec"]["facts"]+10 and replays==len(cursors)
            and all(c["consumed"]==c["receipt"]["Request"]["Count"] for c in cursors.values()),"complete real HOME/remote/successor cursor issuance; unused candidates remain counted")
    for a in lines(directory/"attempts.jsonl",16*end["spec"]["facts"]+9):
        phase,fact,_=operation(end["spec"],a["Operation"]);body=blob(a["Proposal"]);inner=checked(body[145:-32],b"PGO\1",1<<20)
        owner=(1,3,2*end["spec"]["facts"]+7)[fact] if phase=="setup" else 7+2*fact
        expected=[(1,0,owner),(2,owner,owner+1)] if phase in ("setup","create") else []
        if phase!="setup" and fact==0:expected.append((3,0,5 if phase=="create" else 6))
        start=len(inner)-65*len(expected)-4
        require(int.from_bytes(inner[start:start+4],"big")==len(expected),"exact actual fresh-claim roster")
        c=cursor(inner[start+4:])
        for role,claim_owner,identity in expected:
            require((c.number(1),c.number(),c.number())==(role,claim_owner,identity) and c.take(40)+u64(c.number())==spool.lookup(b"issued",u64(identity)),"factory uses actual consumed cursor ID and installed session/sequence")
        c.done()
    return {"actualIssuedIDs":count,"actualNextCalls":next_calls,"sameNextReplays":replays,"installedBlocks":len(cursors),"scope":"exclusive real Recipient.Acquire/Cursor.Next issuance; opaque reserved IDs do not grant processing-owner authority"}


def response(wire):
    r=codec.response(wire);c=cursor(blob(wire));c.take(16+28*8+64);c.number()
    for _ in range(c.number(4)):c.number()
    c.field(1<<20);c.number(1);c.number();c.number();c.number(1);c.number(2);c.field(4096)
    for _ in range(c.number(4)):c.take(17);c.field(1<<20)
    for _ in range(c.number(4)):c.number();c.field(1<<20)
    c.field(65536);r["outcome"],r["changes"]=c.field(65536),c.field(1<<20)
    return r


def coordinator(wire,proposal,spool,*,commit):
    r=response(wire);require(r["code"]==r["cause"]==0,"actual successful SAME coordinator response")
    c=cursor(bytes.fromhex(r["aux"]));require(c.take(4)==b"GCR1","actual coordinator result")
    scope=checked(c.field(256),b"APS\1",156);request_,tx,business,pending=c.take(16),c.take(16),c.take(32),c.number(1)
    outcome=c.field(256);c.done();st=codec.status(outcome)
    original=codec.typed.pgq(proposal)
    require(scope[:16].hex()==raw.GRAPH and scope[16:24]==u64(3) and scope[88:]==AGREEMENT and request_==original["request"]
            and tx==digest(b"rho-tkg:cross-transaction-id:v1\0"+scope[:24]+request_)[:16] and any(business) and pending==0
            and st["phase"]==3 and st["part"]==3 and st["tx"]==tx.hex() and st["commit"] is commit,"exact request/transaction/terminal decision; no timeout ABORT")
    proof=decoded(r["changes"]);fields(proof,"Manifest Outcome CDC")
    m=manifest(blob(proof["Manifest"]))
    manifest_check(blob(proof["Manifest"]),request_,tx,m["digest"],proposal)
    require(blob(proof["Outcome"])==outcome and proof["CDC"] is not None and not blob(proof["CDC"])
            and st["digest"]==m["digest"].hex(),"complete actual immutable Register/Decide proof, empty Decide CDC present")
    return r,proof,st


def certified(wire):
    c=cursor(blob(wire));scope=checked(c.field(256),b"APS\1",156);body=c.field(16384);c.done()
    require(scope[:16].hex()==raw.GRAPH and scope[88:]==AGREEMENT,"actual certified business scope")
    return scope,body


def retry_originals(end,spool):
    scope,k=certified(end["K"]);a=decoded(spool.lookup(b"useful",u64(4+end["spec"]["facts"])))
    require(k==blob(a["Proposal"]),"lost K is the actual useful immutable original")
    _,proof,st=coordinator(end["KOriginal"],k,spool,commit=True)
    require(blob(proof["Manifest"])==blob(a["Manifest"]) and blob(proof["Outcome"])==blob(a["Outcome"]),"recovered K equals complete attempt archive")
    _,reopened,again=coordinator(end["KReopened"],k,spool,commit=True)
    require(proof==reopened and st==again,"physical GC/reopen cannot refresh K index/digest/outcome")
    changed=blob(end["ChangedCaller"]);cq=codec.typed.pgq(changed);kq=codec.typed.pgq(k)
    refused=response(end["ChangedReply"])
    require(cq["request"]==kq["request"] and changed!=k and refused["code"]!=0 and refused["cause"]!=0,
            "same identity changed payload is an actual refusal, not a new useful completion")
    frozen=decoded(blob(end["Frozen"]));fields(frozen,"Version Scope Business Manifest BusinessSHA256 ManifestSHA256 Calls Bytes Rows OutputUsed")
    body=blob(frozen["Business"]);m=manifest(blob(frozen["Manifest"]))
    require(frozen["Version"]==1 and digest(body)==array(frozen["BusinessSHA256"],32)
            and digest(blob(frozen["Manifest"]))==array(frozen["ManifestSHA256"],32),"frozen exact body before source fence")
    _,aborted,stale=coordinator(end["Aborted"],body,spool,commit=False)
    require(blob(aborted["Manifest"])==blob(frozen["Manifest"]) and stale["digest"]==m["digest"].hex(),"actual retained frozen ABORT; never missing-reply authority")
    parsed=codec.typed.pgq(body);ordinal=5+end["spec"]["facts"]
    raw.native_parallel_proposal(body,parsed["request"],ordinal)
    record={**a,"Operation":{**a["Operation"],"Fact":1,"Ordinal":ordinal},"Request":list(parsed["request"]),"Proposal":frozen["Business"],
            "Manifest":frozen["Manifest"],"Outcome":aborted["Outcome"],"Classification":"authoritative-abort"}
    scalar.put(spool,b"terminal",m["tx"],encoded(record))
    # Lost reply evidence must contain the exact actual Submit/Decide exchange.
    c=cursor(checked(blob(end["LostRequest"]),b"GSQ\1",2<<20));kind,seq,nonce=c.number(1),c.number(),c.take(16);cookie=c.take(40);maximum,rows=c.number(4),c.number(4)
    keys=[c.field(65536) for _ in range(3)];wire=c.field(1<<20);tx,phase=c.take(16),c.number(1);c.done()
    p=cursor(checked(wire,b"GSC\1",1<<20));actual_scope=checked(p.field(256),b"APS\1",156);command=p.field(1<<20);p.done()
    p=cursor(checked(command,b"GCT\1",1<<20));decide,namespace=p.number(1),p.take(24);registration=p.field(32768)
    votes=[codec.status((lambda b:b+digest(b))(b"GCS\1"+p.take(99))) for _ in range(integer(p.number(4),1,2))];p.done()
    require(kind==8 and seq>0 and any(nonce) and not any(cookie) and rows==0 and maximum>0 and not any(keys) and not any(tx) and phase==0
            and actual_scope==scope and decide==3 and namespace==scope[:24] and registration==blob(a["Manifest"])
            and all(v["phase"]==2 and v["yes"] and v["digest"]==st["digest"] and v["tx"]==st["tx"] for v in votes),"K loss is actual immutable Decide invocation with native YES votes")
    c=cursor(checked(blob(end["LostReply"]),b"GSR\1",2<<20));reply_scope=checked(c.field(256),b"APS\1",156)
    voter,issuer,rkind,rseq,rnonce,mask=c.number(),c.take(16),c.number(1),c.number(),c.take(16),c.number(2);reply=c.field((2<<20)-512);c.done()
    require(reply_scope==scope and voter==1 and any(issuer) and (rkind,rseq,rnonce)==(kind,seq,nonce) and mask==0 and reply==blob(a["Outcome"]),
            "K loss archives exact matched committed reply, never a synthetic unknown code")
    return {"KOriginalIndex":st["index"],"frozenAbortIndex":stale["index"]}


def lookup_originals(directory,end,spool):
    k=decoded(spool.lookup(b"useful",u64(4+end["spec"]["facts"])));tx=array(k["Transaction"],16)
    key=b"GPD\1"+bytes.fromhex(raw.GRAPH)+u64(3)+tx;st=codec.status(blob(k["Outcome"]));seen=set()
    for trace in controls(directory,end):
        if "Input" not in trace or trace["Input"]["Action"]!="lookup":continue
        q=trace["Input"];stage=trace["Stage"]
        if trace.get("Error"):continue
        r=decoded(blob(trace["Reply"]["Body"]))
        fields(r,"Origin Key Value OriginalIndex EvaluationIndex Found")
        require(stage in ("K-original-after-BOTH-HOME","K-original-after-physical-GC-reopen") and stage not in seen and trace["Member"]==1
                and q["Control"] is True and q["Origin"]==r["Origin"]==3 and blob(q["Key"])==blob(r["Key"])==key
                and q["Original"]==end["Moves"][end["MoveCount"]-1] and r["Found"] is True and blob(r["Value"])==blob(k["Outcome"])
                and integer(r["OriginalIndex"]) in (0,st["index"]) and integer(r["EvaluationIndex"],1),"completed target LookupOriginal preserves full original namespace/body; copied controls do not mint a first-install index")
        spool.lookup(b"frame",u64(8)+u64(r["EvaluationIndex"]))
        seen.add(stage)
    require(len(seen)==2,"both target original lookups before and after physical GC/reopen required")


def authenticate_cut(selected,end,spool):
    for group,cert in enumerate(selected):
        first=cut(end["Initial"])[group]
        require(cert["scope"]==first["scope"] and cert["configuration"]==first["configuration"] and cert["sourceIndex"]>=first["sourceIndex"],"same exact native certificate namespace/configuration")
        actual=first["image"] if cert["sourceIndex"]==first["sourceIndex"] else array(decoded(spool.lookup(b"frame",u64(cert["partition"])+u64(cert["sourceIndex"])))["Root"],172)
        require(cert["image"]==actual,"certificate binds its actual original root; no current substitution")


def move_result(wire):
    c=cursor(wire);require(c.take(4)==b"GMR2","actual move coordinator receipt")
    scope=checked(c.field(256),b"APS\1",156);request_,move,intent=c.take(16),c.take(16),c.take(32)
    pending,committed=c.number(1),c.number(1);states=[]
    for _ in range(3):
        status_=c.field(256);states.append(column.pooled_move_status(raw,status_) if status_ else None)
    mask=c.number(2);c.done()
    require(scope[88:]==AGREEMENT and pending in (0,1) and committed in (0,1) and move==digest(b"rho-tkg:range-move-id:v1\0"+scope[:24]+request_)[:16]
            and all(s is None or s["move"]==move and s["intent"]==intent for s in states),"exact same bounded native move/original association")
    return dict(scope=scope,request=request_,move=move,intent=intent,pending=pending,committed=committed,states=states,mask=mask)


def transfer(directory,end,spool):
    homes=(6+2*end["spec"]["facts"]+(1<<20)-1)//(1<<20)
    require(integer(end["MoveCount"],1,2)==homes and len(end["Moves"])==2,"ALL actual HOME publications, no capped first-range substitute")
    ranges=[];moves_=[]
    for i in range(homes):
        r=move_result(blob(end["Moves"][i]));s,t,d=r["states"]
        intent=column.pooled_move_intent(raw,spool.lookup(b"move-intent",r["move"]),AGREEMENT)
        require(r["pending"]==r["mask"]==0 and r["committed"]==1 and all(v is not None and v["result"]==1 for v in (s,t,d))
                and s["phase"]==t["phase"]==8 and d["phase"]==6 and s["round"]==t["round"]==d["round"]>0,"full successful Finish/Finish/COMMIT; safe ABORT is not workload completion")
        require(intent["request"]==scalar.request(array(end["AdmissionSHA256"],32),"HOME-move",i) and intent["pages"]==end["Admission"]["Grants"]["MaxPages"],"caller-established original move identity/policy")
        grant=decoded(spool.lookup(b"installed-grant",array(end["InitialHOME"][i]["Request"]["Session"]["ID"],16)+array(end["InitialHOME"][i]["Request"]["Session"]["Incarnation"],16)+u64(end["InitialHOME"][i]["Request"]["Session"]["Epoch"])+u64(i+1)))
        require((intent["first"],intent["last"])==(grant["first"],grant["last"]) and intent["range"]==grant["first"],"real allocator publication, not a fabricated logical transfer")
        for part,state in ((3,s),(8,t),(3,d)):
            first=decoded(spool.lookup(b"frame",u64(part)+u64(state["index"])))
            require(column.pooled_move_status(raw,blob(first["Outcome"]))==state,"terminal status joins exact installed original/immutable controls")
        for part in (3,8):spool.lookup(b"move-lineage",r["move"]+u64(part))
        ranges.append((intent["first"],intent["last"]));moves_.append(r["move"].hex())
    require(ranges[0][0]==1 and ranges[-1][1]==6+2*end["spec"]["facts"] and all(ranges[i-1][1]+1==ranges[i][0] for i in range(1,homes)),"complete contiguous HOME coverage; remote range stays native8")
    for unused in end["Moves"][homes:]:require(not codec.optional(unused),"undeclared extra move")
    return {"actualHOMEPublications":homes,"moves":moves_,"logicalHomes":[3,8],"currentProcessingOwnerAfterBOTH":8,"physicalLocality":"native target store8; FDB stable-home transactional namespaces are a different physical implementation"}


def queries(directory,end,spool):
    owners={};completed=set();token=None;generic=None;timings=[]
    for trace in controls(directory,end):
        stage=trace["Stage"]
        if trace.get("Type")=="whole-observation-timing":
            require(len(owners)==1 and stage in ("final-current","reopened","generic-At-C-after-release"),"whole observed query owner")
            o=next(iter(owners.values()))
            require(o["stage"]==stage and o["rows"]==trace["Rows"]==end["spec"]["facts"] and o["points"] and not o["timed"],"whole timing only after actual complete index/points")
            o["timed"]=True;timings.append(trace);continue
        if "Input" not in trace or trace["Input"]["Action"]!="query":continue
        q=trace["Input"]["Query"]
        require(trace["Member"]==2 and stage in ("final-current","reopened","generic-At-C-after-release"),"actual bounded native query stages/caller")
        if trace.get("Error"):
            if trace["Reply"].get("Failure")==9:
                require(stage=="generic-At-C-after-release" and q["Action"]=="prepare" and q["Mode"]==3 and q["Cut"]==end["Snapshot"] and not owners,"actual generic At(C) expiry; distinct from feed-token refusal")
                generic="expired-actual-native-history-floor";completed.add(stage)
            continue
        r=decoded(blob(trace["Reply"]["Body"]));action=q["Action"]
        require(r["Version"]==q["Version"]==1 and r["Action"]==action and not r.get("Failure"),"actual typed source response")
        if action in ("prepare","reopen"):
            require(not owners and stage not in completed and integer(r["Handle"],1),"one genuine native query owner")
            expected=end["Snapshot"] if stage=="generic-At-C-after-release" else end["Final"]
            require(r["Cut"]==expected and (action=="reopen")== (stage=="reopened"),"At/reopen never substituted current/empty")
            selected=cut(r["Cut"]);authenticate_cut(selected,end,spool)
            if action=="prepare":require(q["Mode"]==3 and q["Cut"]==expected,"actual caller At certificate")
            else:require(q["Token"]==r["Token"]==token,"same durable pin across full cohort replacement")
            pin,hash_=selector.pin(r["Original"],r["Token"] if action=="reopen" else None,selected,spool)
            owners[r["Handle"]]={"stage":stage,"cut":selected,"original":r["Original"],"token":r["Token"],"pin":pin,"hash":hash_,"seq":0,"last":None,"rows":0,"eof":False,"points":False,"timed":False,"charged":None}
            continue
        require(q["Handle"]==r["Handle"] and q["Handle"] in owners,"exact query handle ownership")
        o=owners[q["Handle"]];value=5 if stage=="generic-At-C-after-release" else 6
        if action=="publish":
            selector.pin(o["original"],r["Token"],o["cut"],spool);o["token"]=r["Token"]
            if stage=="final-current":token=r["Token"]
        elif action in ("scan","points"):
            seq=integer(q["Sequence"],1)
            require(r["Sequence"]==seq and o["token"] is not None,"source page before genuine pin publication")
            if seq==o["seq"]:
                require(action=="points" and o["last"]==(q,r),"SAME points changed/readvanced source");continue
            require(seq==o["seq"]+1 and len(r["Points"] or [])<=16,"bounded exact query continuation")
            o["seq"],o["last"]=seq,(q,r)
            starts=integer(r["Starts"],1,end["FramesCeiling"]+end["Admission"]["BudgetSeconds"]//60+128)
            require(r["Charged"]==dict(zip(column.WORK,(starts*16384,starts*(16<<20),0,starts*(32<<20),starts*1024,starts*(4<<30)))),"actual native source allowance; no refill on replay")
            if o["charged"] is not None:require(all(r["Charged"][k]>=o["charged"][k] for k in column.WORK),"query work regression")
            o["charged"]=r["Charged"]
            if action=="scan":
                require(not o["eof"] and len(r["Points"] or [])==len(r["Candidates"] or []),"real type/incident posting hydration/EOF")
                for entity,candidate in zip(r["Points"] or [],r["Candidates"] or [],strict=True):
                    fact=o["rows"];require(candidate==entity["id"]==7+2*fact and fact<end["spec"]["facts"],"exact posting set/order, no phantom")
                    selector.point(end["spec"],entity,value,spool,guarded_updates=True)
                    spool.insert(b"query/"+stage.encode(),u64(fact),encoded(entity));o["rows"]+=1
                require(type(r["EOF"]) is bool and (r["EOF"] or r["Points"]),"no partial/nonprogress successful source")
                o["eof"]=r["EOF"]
                if o["eof"]:require(o["rows"]==end["spec"]["facts"],"whole actual index EOF")
            else:
                ids=[1,3,2*end["spec"]["facts"]+7,7,7+2*(end["spec"]["facts"]-1),6+2*end["spec"]["facts"]]
                require(o["eof"] and not o["points"] and q["IDs"]==ids and len(r["Points"])==6,"actual fixed point and phantom set after index EOF")
                for identity,entity in zip(ids,r["Points"],strict=True):require(entity["id"]==identity,"point order");selector.point(end["spec"],entity,value,spool,guarded_updates=True)
                o["points"]=True
        elif action in ("close","release"):
            require(o["eof"] and o["points"] and o["timed"],"owner close cannot bless partial query")
            if action=="release":
                for cert in o["cut"]:spool.lookup(b"pin-terminal",u64(cert["partition"])+o["pin"]+o["hash"])
            completed.add(stage);del owners[q["Handle"]]
            if stage=="generic-At-C-after-release":generic="available-actual-At-C-complete-typed-index-read"
        else:raise ValueError("unrecognized transfer query action")
    require(not owners and completed=={"final-current","reopened","generic-At-C-after-release"} and generic==end["GenericAtAfterRelease"],"all native current/reopen/generic-At observations complete")
    return {"genericAtAfterRelease":generic,"observations":timings,"access":"actual type/incident posting scan plus hydrated Points; complete caller retry/wait, no per-key indexed-latency claim"}


def definition(end,spool):
    wire=blob(end["Original"]);c=cursor(checked(wire,b"GFD\1",32768));manifest_=c.field(32768)
    policy=(c.number(4),c.number(4),c.number(),c.number(),c.number(4),c.number(4),c.number(4),c.number());c.done()
    pages=64*end["spec"]["facts"]+1024
    require(policy==(65536,16,pages,4*pages+end["spec"]["facts"]//8+1024,16384,16<<20,32<<20,end["Admission"]["DeadlineUnixNanos"]),"immutable native feed budget including original deadline")
    t=cursor(checked(blob(end["Token"]),b"GFT\1",65536));require(t.field(32768)==wire,"same native feed original");token=t.field(33792);t.done()
    pin,hash_=selector.pin(scalar.base64.b64encode(manifest_).decode(),scalar.base64.b64encode(token).decode(),cut(end["Snapshot"]),spool,
                             scalar.request(array(end["AdmissionSHA256"],32),"transfer-snapshot",0))
    c=cursor(checked(manifest_,b"GPM\1",32768));scope,issuer=c.take(120),c.take(16)
    require(scope==cut(end["Snapshot"])[0]["scope"],"feed exact source native scope")
    return wire,manifest_,issuer,pin,hash_,[cert["sourceIndex"]+1 for cert in cut(end["Snapshot"])]


def feed(directory,end,spool):
    original,manifest_,issuer,pin,manifest_hash,positions=definition(end,spool)
    previous=bytes(32);sequence=business=progress=replays=0;retained_only=False;released=False;opened=0;expired=set();snapshots={};acknowledged=None;retained_vector=None;pending_replayed=False;timings={};last_eof=False;lineage_pages=set()
    for trace in controls(directory,end):
        if "Input" not in trace or trace["Input"]["Action"]!="feed":continue
        q=trace["Input"]["Feed"];stage=trace["Stage"]
        if q["Action"]=="records":continue # Independently decoded original/voter source stream owns these rows.
        require(trace["Member"]==2,"actual native feed owner caller")
        if trace.get("Error"):
            failure=trace["Reply"].get("Failure",0)
            if failure==9:
                require(retained_only and q["Action"]=="snapshot" and stage in ("snapshot-refused-after-release","snapshot-refused-after-reopen"),"token snapshot refusal comes from actual retained-only owner state")
                expired.add(stage)
            elif failure==8:
                require(released and stage=="terminal-token-refusal" and q["Action"]=="open" and q["Token"]==end["Token"],"actual keeper released-token refusal")
                expired.add(stage)
            continue
        a=decoded(blob(trace["Reply"]["Body"]));action=q["Action"]
        require(a["Version"]==q["Version"]==1 and a["Action"]==action and not a.get("Failure"),"actual successful native feed reply")
        timings[action]=timings.get(action,0)+1
        spool.insert(b"transfer-feed-latency/"+action.encode(),u64(trace["OperationNanos"])+u64(trace["Call"]),b"1")
        if action=="begin":require(a["Original"]==end["Original"] and a["Initial"]==end["Snapshot"],"factory-issued actual snapshot C")
        elif action=="publish":require(a["Token"]==end["Token"],"actual native feed publication")
        elif action=="open":require(q["Token"]==end["Token"],"same immutable token through process/cohort replacement");opened+=1
        elif action=="snapshot":
            require(not retained_only and stage in ("held-C-before-K","held-C-after-moves","held-C-after-restart","held-C-after-actual-GC"),"real C held through moves/restart/old-owner collection")
            count=snapshots.get(stage,0)
            require(q["From"]==count and a["Next"]==min(count+16,end["spec"]["facts"]) and len(a["Points"])==a["Next"]-count and a["EOF"] is (a["Next"]==end["spec"]["facts"]),"complete exact old-C snapshot continuation")
            for offset,point_ in enumerate(a["Points"]):
                fact=scalar.point(end["spec"],point_,5,spool,guarded_updates=True)
                require(fact==count+offset,"snapshot omission/duplication/current substitution")
                scalar.put(spool,b"snapshot-old",u64(fact),encoded(point_["Entity"]))
            snapshots[stage]=a["Next"]
        elif action=="page":
            wire=blob(a["Page"]);p=scalar.page(wire);hash_=digest(wire);boundary=cut(q["Boundary"])
            authenticate_cut(boundary,end,spool)
            require(p["pin"]==pin and p["request"]==array(q["Request"],16) and p["boundary"]==boundary and array(a["Digest"],32)==hash_ and a["EOF"] is bool(p["eof"]),"exact full-vector native page/boundary/seal")
            existing=spool.find(b"feed-page",p["request"])
            if existing is not None:
                require(a["Replay"] is True and existing==wire,"SAME Page immutable across ACK/client loss")
                if stage=="same-page-after-GC-client-loss":require(wire==blob(end["PendingPage"]) and p["request"]==array(end["PendingRequest"],16) and p["kind"] in (1,2),"actual pending BUSINESS page replacement replay");pending_replayed=True
                replays+=1;continue
            sequence+=1
            require(a["Replay"] is False and p["sequence"]==sequence and p["previous"]==previous and p["from"]==positions
                    and p["request"]==scalar.request(array(end["AdmissionSHA256"],32),"transfer-page",sequence),"complete immutable native cursor/page chain; no whole-run reset")
            covered={};fact_=None
            for f in p["frames"]:
                part,index=f["Partition"],f["Index"];group=0 if part==3 else 1
                require(part in (3,8) and group not in covered and index==positions[group],"exact next native physical original")
                actual=decoded(spool.lookup(b"frame",u64(part)+u64(index)))
                require(blob(actual["Outcome"])==f["Outcome"] and blob(actual["CDC"])==f["CDC"] and array(actual["Root"],172)==f["Root"],"page is independently raw/native-original joined")
                next_=f["ProgressNext"] or index+1
                require(next_==p["next"][group] and index<next_<=index+32,"bounded actual progress")
                covered[group]=next_
                if f["ProgressNext"]:
                    require(p["kind"]==0 and any(f["Census"]),"progress cannot hide business or handoff")
                    for at in range(index,next_):
                        skipped=decoded(spool.lookup(b"frame",u64(part)+u64(at)))
                        require(not blob(skipped["CDC"]).startswith((b"GCE\1",b"GML\1")) and not skipped["Rows"],"native census skipped genuine business/lineage")
                elif p["kind"]==3:
                    status_=column.pooled_move_status(raw,f["Outcome"])
                    require(f["CDC"].startswith(b"GML\1") and status_["phase"]==7 and status_["result"]==1 and status_["move"]==p["tx"] and status_["round"]==p["round"] and not actual["Rows"],"complete authenticated move-lineage envelope, zero business rows")
                    spool.lookup(b"move-lineage",p["tx"]+u64(part))
                elif p["kind"]:
                    require(f["CDC"].startswith(b"GCE\1") and f["Round"]==p["round"],"actual complete useful original")
                    if f["Outcome"].startswith(b"GCS\1"):
                        st=codec.status(f["Outcome"]);terminal=decoded(spool.lookup(b"terminal",p["tx"]))
                        require(st["phase"]==4 and st["commit"] and st["tx"]==p["tx"].hex() and f["Registration"]==blob(terminal["Manifest"]) and f["Decision"] in (b"",blob(terminal["Outcome"])),"coordinated business proof independent of participant cardinality")
                    else:
                        b=raw.fixed_body(f["Outcome"],b"GRO\4",120)
                        require(digest(b"rho-tkg:cross-transaction-id:v1\0"+bytes.fromhex(raw.GRAPH)+u64(part)+b[25:41])[:16]==p["tx"],"actual local request-qualified feed identity")
                    for row in actual["Rows"] or []:
                        fact=integer((row["Owner"]-7)//2,0,end["spec"]["facts"]-1)
                        require(fact_ in (None,fact) and row==decoded(spool.lookup(b"row",u64(4+end["spec"]["facts"]+fact)+u64(row["Owner"])+bytes([row["Component"]]))),"complete actual property and guarded presence original")
                        spool.insert(b"feed-update" if row["Component"]==3 else b"feed-presence",u64(fact),encoded(row));fact_=fact
            require(all(p["next"][g]==covered.get(g,positions[g]) for g in range(2)),"unrepresented source cannot advance cursor")
            if p["kind"] in (2,3):
                terminal_=decoded(spool.lookup(b"terminal",p["tx"])) if p["kind"]==2 else None
                expected_parts=set(manifest(blob(terminal_["Manifest"]))["floors"]) if terminal_ is not None else {3,8}
                require({3+5*g for g in covered}==expected_parts,"half transaction or half handoff cannot publish")
            if p["kind"] in (1,2):require(fact_ is not None,"empty useful update");scalar.payload_catalog(a,end["spec"],fact_);business+=1
            else:
                require(a["Catalog"] is None and a["Schemas"] is None,"protocol/move cannot invent scalar payload")
                if p["kind"]==3:require(p["tx"] not in lineage_pages,"duplicate move lineage");lineage_pages.add(p["tx"])
                else:progress+=1
            for group,cert in enumerate(boundary):
                command,action_id=retained.feed_command(cert["scope"],issuer,p["request"],2,original,wire)
                b=checked(spool.lookup(b"feed-result",u64(cert["partition"])+digest(command)),b"GFO\1",256)
                require(b[:32]==pin and b[64:96]==hash_ and b[96:112]==action_id and b[128:]==b"\2\1","actual installed complete Page seal in every keeper")
                tip=decoded(spool.lookup(b"frame",u64(cert["partition"])+u64(p["tips"][group][0])))
                require(array(tip["Root"],172)==p["tips"][group][1],"actual physical feed tip/root")
            spool.insert(b"feed-page",p["request"],wire);spool.insert(b"feed-sequence",u64(sequence),p["request"])
            previous,positions,last_eof=hash_,p["next"],bool(p["eof"])
        elif action=="ack":
            wire=spool.lookup(b"feed-page",array(q["Request"],16));p=scalar.page(wire)
            require(array(q["Digest"],32)==digest(wire),"exact original ACK digest")
            for cert in p["boundary"]:
                command,action_id=retained.feed_command(cert["scope"],issuer,p["request"],3,original,wire)
                b=checked(spool.lookup(b"feed-result",u64(cert["partition"])+digest(command)),b"GFO\1",256)
                require(b[:32]==pin and b[64:96]==digest(wire) and b[96:112]==action_id and b[128:]==b"\3\1","actual all-owner immutable ACK original")
            complete=cut(a["Completed"])
            if p["eof"]:require(complete==p["boundary"],"complete boundary only after all originals/lineage")
            else:require(complete[0]["round"]<p["boundary"][0]["round"],"nonterminal page blessed full boundary")
            if acknowledged is None or p["sequence"]>=acknowledged[0]:acknowledged=(p["sequence"],p["next"])
            scalar.put(spool,b"feed-ack",p["request"],encoded(a))
        elif action=="retain":
            require(not retained_only and acknowledged is not None and business==2,"real existing unread update suffix before irreversible release")
            retained_vector=acknowledged[1]
            for cert in cut(end["Snapshot"]):
                command,action_id=retained.feed_command(cert["scope"],issuer,array(q["Request"],16),5,original,b"")
                b=checked(spool.lookup(b"feed-result",u64(cert["partition"])+digest(command)),b"GFO\2",256)
                require(b[:32]==pin and b[96:112]==action_id and b[128:]==b"\5\1","actual irreversible native retention original; not a local label")
            retained_only=True
        elif action=="release":
            require(retained_only and last_eof,"terminal release before complete final EOF")
            for part in (3,8):spool.lookup(b"pin-terminal",u64(part)+pin+manifest_hash)
            released=True
        else:require(action in ("snapshot-close","close","state"),"unknown native feed operation")
    require(sequence==end["Pages"] and business==end["spec"]["facts"] and business+progress+len(lineage_pages)==end["BusinessPages"]+end["ProgressPages"]==end["Pages"]
            and len(lineage_pages)==end["MoveCount"] and end["BusinessPages"]==business+end["MoveCount"] and end["ProgressPages"]==progress and replays>=sequence and pending_replayed and opened>=3 and retained_only and released and last_eof,
            "all business updates and complete handoff groups, exact pending replay, terminal lifecycle")
    require(snapshots=={s:end["spec"]["facts"] for s in ("held-C-before-K","held-C-after-moves","held-C-after-restart","held-C-after-actual-GC")}
            and expired=={"snapshot-refused-after-release","snapshot-refused-after-reopen","terminal-token-refusal"},"actual full C retention schedule and explicit new-open refusals")
    require(array(end["PendingDigest"],32)==digest(blob(end["PendingPage"])) and scalar.page(blob(end["PendingPage"]))["request"]==array(end["PendingRequest"],16),"actual interrupted pending page identity")
    for group in range(2):require(retained_vector[group]==end["HeldGC"][3*group]["Plan"]["FeedFloor"]==end["GC"][3*group]["Plan"]["FeedFloor"],"GC floor is actual acknowledged full vector")
    for fact in range(end["spec"]["facts"]):spool.lookup(b"feed-update",u64(fact));spool.lookup(b"feed-presence",u64(fact))
    return timings


def gc(directory,end,spool):
    owners={};finished=set();censuses={};pages=0
    for row in lines(directory/"grants.jsonl",64):
        c=cursor(blob(row["Proposal"]));scope=checked(c.field(256),b"APS\1",156)
        scalar.put(spool,b"bootstrap-root",scope[16:24]+u64(row["Index"]),blob(row["Root"]))
    maximum=2*6*(260*end["FramesCeiling"]+65536+134)
    for trace in lines(directory/"gc.jsonl",maximum):
        if trace.get("Type")=="census":
            fields(trace,"Type Stage Member Response");member=integer(trace["Member"],1,6)
            stage=trace["Stage"];cycle=0 if stage.startswith("gc-0-") else 1
            require(stage in (f"gc-{cycle}-before-collect",f"gc-{cycle}-after-collect"),"unknown physical census stage")
            r=codec.response(trace["Response"])
            require(r["code"]==r["cause"]==r["reads"]==r["packets"]==0,"actual native state census, not synthetic counters")
            key=(cycle,member)
            if stage.endswith("before-collect"):
                require(key not in censuses and key not in finished,"one pre-collection census")
                censuses[key]=r
            else:
                require(key in censuses,"physical after without before")
                before=censuses[key]
                require(r["incarnation"]==before["incarnation"] and r["applied"]==before["applied"] and r["image"]==before["image"]
                        and r["stats"][18]<before["stats"][18] and r["stats"][19]<before["stats"][19],"genuine reclaimed native application bytes AND records; metadata labels are insufficient")
                censuses[key]=None
            continue
        fields(trace,"Member Input Reply")
        member=integer(trace["Member"],1,6);group=(member-1)//3;part=3+5*group;q,r=trace["Input"],trace["Reply"];action=q["Action"]
        require(q["Version"]==r["Version"]==1 and r["Profile"]==FORMAT and r["Member"]==member and r["Action"]==action
                and r["DeadlineUnixNanos"]==end["Admission"]["DeadlineUnixNanos"] and array(r["Run"],16)==bytes.fromhex(end["Admission"]["Run"]) and integer(r["PID"],1),"actual collector identity/deadline before physical mutation")
        old=owners.get(member)
        if action=="inspect":
            cycle=0 if (0,member) not in finished else 1
            require(cycle==0 and old is None or cycle==1 and (0,member) in finished,"ordered two real native collection plans")
            require(q["Sequence"]==r["Sequence"]==0 and any(array(r["Cookie"],16)),"genuine native collector factory identity")
            old={"cycle":cycle,"r":r,"seq":0,"section":0,"pages":0,"original":None,"complete":False};owners[member]=old
        else:
            require(old is not None and q["Cookie"]==r["Cookie"]==old["r"]["Cookie"] and q["Sequence"]==r["Sequence"]==old["seq"]+1 and r["PID"]==old["r"]["PID"],"same genuine collector owner/cumulative sequence")
            old["seq"]+=1
            for name in ("Calls","Pages","GrantedRows","GrantedBytes","GrantedOutput"):require(r[name]>=old["r"][name],"native collection cannot refund work")
        cycle=old["cycle"];expected=end["HeldGC" if cycle==0 else "GC"][member-1]
        require(integer(r["Calls"],1)<=260*end["FramesCeiling"]+65536+134 and r["State"]=="no-view","finite native collector excludes live local view")
        if action=="prepare":
            require(member in (1,4),"actual leader prepares immutable native plan")
            command=blob(r["Original"]);c=cursor(checked(command,b"GGC\1",512));scope=c.take(120);c.take(16);request_=c.take(16);c.take(32);c.take(32);floors=[c.number() for _ in range(6)];c.done()
            cert=cut(end["Snapshot"])[group];ack=integer(expected["Plan"]["FeedFloor"],3)
            want=[cert["sourceIndex"],cert["sourceIndex"],ack,ack,cert["sourceIndex"],cert["index"]] if cycle==0 else [ack-1,ack-1,ack,ack,ack-1,1]
            require(request_==array(q["Request"],16)==scalar.request(array(end["AdmissionSHA256"],32),"transfer-physical-gc",2*cycle+group)
                    and floors==q["Floors"]==want and scope==cut(end["Initial"])[group]["scope"],"actual held-C versus rolling-ACK floors, no caller expiry authority")
            old["original"]=command
        elif action in ("original","recover"):
            command=blob(r["Original"]);out=blob(r["Outcome"]);actual=retention_original(command,out,part)
            require(r["Found"] is True and r["CDC"] is not None and not blob(r["CDC"]) and actual["status"]==1
                    and spool.lookup(b"retention-original",u64(part)+actual["id"])==command+out,"native GC original joined immutable native source controls")
            if action=="recover":require(command==blob(q["Original"]),"follower recovers SAME installed original")
            else:require(command==old["original"],"prepared native GC original changed")
            old["original"]=command;old["outcome"]=out
        elif action=="collect":
            p=r["Progress"]
            require(old["original"] is not None and array(p["PlanID"],32)==digest(old["original"]) and integer(p["Generation"],1)
                    and p["SourceIndex"]+1==p["InstalledIndex"]==expected["Plan"]["InstalledIndex"] and p["Section"]>=old["section"]
                    and type(p["Complete"]) is bool and p["Complete"] is (p["Section"]==6) and r["Pages"]==old["pages"]+1,"actual monotone partial collection cursor/EOF")
            require(integer(r["Work"]["Records"],0,512)>=0 and integer(r["Work"]["Bytes"],0,1<<20)>=0,"bounded actual physical page work")
            old.update(section=p["Section"],pages=r["Pages"],complete=p["Complete"]);pages+=1
            if p["Complete"]:require(p["CursorBytes"]==p["CursorRecords"]==0,"native completed cursor must be gone")
        elif action=="current":
            require(old["complete"] and r==expected and r["Found"] is True and censuses.get((cycle,member),True) is None,"current plan only after genuine completed/reclaimed native collection")
            command=old["original"];actual=retention_original(command,old["outcome"],part);plan=r["Plan"]
            require(array(plan["ID"],32)==actual["id"] and array(plan["PreviousID"],32)==actual["previous"] and array(plan["SourceHash"],32)==actual["sourceHash"]
                    and plan["SourceIndex"]==actual["source"] and plan["InstalledIndex"]==actual["index"]
                    and [plan[k] for k in ("RootFloor","DataFloor","FeedFloor","OutcomeFloor","RecoveryFloor")]==actual["floors"][:5],"actual plan/cursor roots/floors exact original")
            prior=0
            for anchor in plan["Roots"] or []:
                index=integer(anchor["Index"],prior+1,plan["SourceIndex"]);prior=index
                frame=spool.find(b"frame",u64(part)+u64(index));image=array(decoded(frame)["Root"],172) if frame is not None else spool.lookup(b"bootstrap-root",u64(part)+u64(index))
                require(array(anchor["ImageHash"],32)==digest(image) and type(anchor["Data"]) is bool,"actual retained root/data anchor")
            require(len(plan["Roots"] or [])<=64,"finite retained anchor inventory")
            floor=plan["RootFloor"] if cycle else max(plan["RootFloor"],plan["FeedFloor"]-1,plan["OutcomeFloor"]-1)
            require(end["SparseFrom" if cycle else "HeldSparseFrom"][group]==floor,"raw suffix floor distinct from held physical C")
            finished.add((cycle,member))
        else:require(action in ("inspect","submit"),"unknown native GC action")
        old["r"]=r
    require(finished==set((c,m) for c in range(2) for m in range(1,7)) and set(censuses)==finished and all(v is None for v in censuses.values()) and pages>=12,"both physical collections completed/reclaimed on ALL six voters")
    return {"members":6,"actualPages":pages,"collectionPasses":2,"contract":"C root/data held through first GC; genuine released-token rolling-ACK native reclamation before unread Page; actual retained application and file bytes remain counted"}


def columns(directory,end,spool):
    n=end["spec"]["facts"]
    require(end["CheckpointFirst"]==n//2+1,"real post-GC checkpoint, no replay of expired initial source")
    expected={"old-seal/native","old-seal/fill","old-seal/cached","old-after-BOTH-moves","checkpoint-seal/native","checkpoint-seal/fill","checkpoint-seal/cached","checkpoint-reopened-false","checkpoint-reopened-true"}
    def generation(stage):
        if stage.startswith(("checkpoint-","retire-checkpoint-")):return 1
        require(stage.startswith(("old-","retire-all-C-dependent-columns")),"explicit transfer generation stage")
        return 0
    if end["spec"]["layout"]=="columns":
        for group in range(2):
            old,new=end["ColumnManifests"][0][group],end["ColumnManifests"][1][group]
            require(old["Rows"]==([2+2*n,1][group]) and new["Rows"]==([0,2*(n-end["CheckpointFirst"])][group]),"actual old and post-GC target8 typed native row counts")
            require(cut(end["Checkpoint"])[group]["sourceIndex"]>=end["SparseFrom"][group],"checkpoint starts in actual retained native suffix")
    scalar._scalar_columns(directory,end,spool,cut,end["FramesCeiling"],end["Snapshot"],initial_cuts=(end["Initial"],end["Checkpoint"]),expected_stages=expected,generation_for_stage=generation)


def processes(directory,end,spool):
    killed=set();restored=clean=0
    for trace in lines(directory/"processes.jsonl",24):
        stage=trace.get("Stage","")
        if stage in ("target-replacement","pending-page-client-replacement"):
            member=4 if stage.startswith("target") else 2
            require(stage not in killed and trace["Member"]==member and trace["Signal"]=="SIGKILL" and trace["Reaped"] is True
                    and integer(trace["PID"],1)!=integer(trace["ReplacementPID"],1),"actual target/client process killed and reaped")
            before,after=codec.response(trace["Before"]),codec.process_state(trace["Restored"])
            require(before["incarnation"]!=after["incarnation"] and before["applied"]==after["applied"] and before["image"]==after["image"],"same-volume replacement preserves actual full native state")
            killed.add(stage)
        elif stage=="full-cohort-restore-before-election":
            before,after=codec.process_state(trace["Before"]),codec.process_state(trace["Restored"])
            require(trace["Member"]==restored+1 and before["incarnation"]!=after["incarnation"] and before["applied"]==after["applied"] and before["image"]==after["image"]
                    and all(before["stats"][i]==after["stats"][i] for i in (0,4)),"all six genuine same-directory restore prefixes before election")
            restored+=1
        else:
            cohort,offset=divmod(clean,6);member=offset+1
            require(trace["Cohort"]==cohort+1 and trace["Member"]==member and trace["Reaped"] is True and trace["WaitStatus"]==trace["ExitCode"]==0
                    and integer(trace["PID"],1) and Path(trace["Volume"])==Path(end["Directory"])/str(member),"actual bounded owned child terminal census")
            spool.insert(b"incarnation",array(trace["Incarnation"],16),b"1");clean+=1
    require(len(killed)==2 and restored==6 and clean==12 and end["ProcessInstances"]==end["Reaped"]==14,"no omitted descendant/replacement cleanup")
    logical=allocated=0
    for member in range(1,7):
        for parent,dirs,files in os.walk(Path(end["Directory"])/str(member),followlinks=False):
            require(all(not (Path(parent)/name).is_symlink() for name in dirs),"owned volume cannot contain foreign links")
            for name in files:
                st=(Path(parent)/name).lstat();require(stat.S_ISREG(st.st_mode),"actual owned native regular file")
                logical+=st.st_size;allocated+=st.st_blocks*512
    require((logical,allocated)==(end["PersistedFileBytes"],end["AllocatedFileBytes"]),"all-voter source/control/import/fragment/undo/cache retained file cost census")


def transfer_answers(spec,spool):
    h=hashlib.sha256()
    for namespace in (b"transfer/old",b"transfer/current"):
        for fact in range(shape(spec)["facts"]):
            v=decoded(spool.lookup(namespace,u64(fact)))
            require(type(v) is list and len(v)==9 and v[:6]==[7+2*fact,8+2*fact,1,2*spec["facts"]+7 if scalar.cross(spec,fact) else 3,2,(2*spec["facts"]+8 if scalar.cross(spec,fact) else 4)],"complete canonical immutable logical identity/lives, independent of physical placement")
            h.update(b"edge-i64-lifebound-v1\0"+u64(fact)+b"".join(u64(x) for x in v[:7])+struct.pack(">q",v[7])+u64(v[8]))
    for fact in range(spec["facts"]):
        update=decoded(spool.lookup(b"transfer/updates",u64(fact)))
        require(update=={"before":decoded(spool.lookup(b"transfer/old",u64(fact))),"after":decoded(spool.lookup(b"transfer/current",u64(fact)))} and spool.lookup(b"transfer/update-authority",u64(fact)),"complete original/ACK authority before normalization")
    return h.hexdigest(),2*spec["facts"]


def answers(end,spool):
    n=end["spec"]["facts"]
    for fact in range(n):
        old=decoded(spool.lookup(b"snapshot-old",u64(fact)))
        current=decoded(spool.lookup(b"query/final-current",u64(fact)))
        require(current==decoded(spool.lookup(b"query/reopened",u64(fact))),"full native current/reopened typed equality")
        values=[]
        for value,point_,namespace in ((5,old,b"transfer/old"),(6,current,b"transfer/current")):
            ordinal=4+fact+(n if value==6 else 0);row=decoded(spool.lookup(b"row",u64(ordinal)+u64(7+2*fact)+b"\3"))
            scalar.point(end["spec"],{"Fact":fact,"Entity":point_},value,spool,guarded_updates=True)
            v=[row[k] for k in ("Owner","Life","Source","Target","SourceLife","TargetLife")]+[row["After"][k] for k in ("Handle","Value","Provenance")]
            scalar.put(spool,namespace,u64(fact),encoded(v));values.append(v)
        scalar.put(spool,b"transfer/updates",u64(fact),encoded({"before":values[0],"after":values[1]}))
        scalar.put(spool,b"transfer/update-authority",u64(fact),encoded({"property":decoded(spool.lookup(b"feed-update",u64(fact))),"presence":decoded(spool.lookup(b"feed-presence",u64(fact)))}))
    return transfer_answers(end["spec"],spool)[0]


def prospective_writes(body,part):
    """Actual immutable PEF1 write roster; no business endpoint proxy."""
    c=cursor(body);require(c.take(4)==b"PEF\1" and c.number()==part,"actual native kernel effect namespace")
    before=c.field(65536);c.number();c.number();c.take(32);base=c.field(65536);after=c.field(65536)
    require(before==base and len(before)==len(after)==172,"actual prospective graph roots")
    count=integer(c.number(4),0,4096);owned=0
    for _ in range(count):
        key,value=c.field(65536),c.field(65536);owned+=len(key)+len(value)
        require(key,"empty native prospective key")
        for _ in range(integer(c.number(4),0,16384)):c.number(4)
    return count,owned


def replay_cursor(wire):
    c=cursor(checked(wire,b"GEC\1",129<<10));family=c.number(1);after,key=c.field(65536),c.field(65536)
    sequence,rows,size=c.number(),c.number(),c.number();hash_=c.take(32);complete=c.number(1);offset,total,start=c.number(4),c.number(4),c.number();c.done()
    require(1<=family<=11 and complete in (0,1) and bool(complete)==(family==11) and rows<=sequence and any(hash_)
            and offset<=total<=1<<20 and (not complete or not after+key and offset==total==start==0),"bounded original replay cursor/EOF")
    require((offset==0 and not key and total==start==0) or 0<offset<total and key and 0<start<=sequence,"exact owned fragment continuation")
    return dict(family=family,after=after,key=key,sequence=sequence,rows=rows,size=size,digest=hash_,complete=bool(complete),offset=offset,total=total,start=start)


def original_record(key,value,control,spool):
    start=4 if control else 3
    require(len(key)==44 if control else len(key)==43,"bijective original key width")
    require(key[:2]==(b"GP" if control else b"ga") and (not control or key[3]==1) and key[start:start+16].hex()==raw.GRAPH,"qualified native original, not target namespace substitution")
    part=int.from_bytes(key[start+16:start+24],"big");identity=key[start+24:];tag=key[2]
    require(part in (3,8) and any(identity),"declared original namespace/identity")
    if not control and tag in (4,5):
        require(value.startswith((b"GRO\2",b"GRO\4")),"actual native immutable request original")
        b=checked(value,value[:4],120);index=int.from_bytes(b[73:81],"big")
        proof=spool.find(b"allocation-original",u64(part)+u64(index)) if value.startswith(b"GRO\2") else None
        actual=decoded(proof) if proof is not None else decoded(spool.lookup(b"frame",u64(part)+u64(index)))
        require(b[1:25]==bytes.fromhex(raw.GRAPH)+u64(part) and b[25:41]==identity and blob(actual["Outcome"])==value,"imported immutable request equals authentic original archive")
    elif not control and tag==16:
        c=cursor(checked(value,b"GSL\1",256));scope,request_,hash_=c.take(120),c.take(16),c.take(32);round_,index=c.number(),c.number();c.done()
        a=decoded(spool.lookup(b"terminal-local",u64(part)+identity))
        require(scope[88:]==AGREEMENT and scope[16:24]==u64(part) and request_==identity and hash_==array(a["BodySHA256"],32)
                and index==a["OriginalIndex"] and round_==a["Round"],"imported local receipt retains exact original round/index/digest")
    elif not control and tag==11 or control and tag in (ord("P"),ord("D")):
        st=codec.status(value)
        require(st["part"]==part and st["tx"]==identity.hex() and spool.lookup(b"phase",u64(part)+identity+bytes([st["phase"]]))==value,"imported native phase joins exact first original")
    elif control and tag in (ord("R"),ord("K")):
        a=decoded(spool.lookup(b"terminal",identity));m=manifest(blob(a["Manifest"]))
        require(value==(blob(a["Manifest"]) if tag==ord("R") else m["nativeKernels"][part]),"whole imported Register/prepared kernel byte equality, not only command labels")
    elif control and tag==ord("A"):
        a=decoded(spool.lookup(b"terminal",identity));m=manifest(blob(a["Manifest"]));c=cursor(checked(value,b"GCF\1",1024))
        scope=c.take(120);configuration,routing=c.take(32),c.take(32);generation,index=c.number(),c.number();image=c.take(32)
        desired,desired_hash,desired_owner=c.take(16),c.take(32),c.number();holder,holder_hash,holder_owner=c.take(16),c.take(32),c.number()
        prepare,nonce,session,sequence=c.take(32),c.take(16),c.take(16),c.number();c.done()
        loser=(desired,desired_hash,desired_owner) if desired>holder else (holder,holder_hash,holder_owner)
        require(a["Classification"]=="authoritative-abort" and loser==(identity,m["digest"],m["coordinator"]) and desired!=holder
                and scope[88:]==AGREEMENT and any(configuration) and any(routing) and generation>0 and index>0 and all(any(x) for x in (image,prepare,nonce,session)) and sequence>0,"actual native global TxID capacity owner/refusal proof, not timeout ABORT")
    else:raise ValueError("unsupported imported original family")


def imported_originals(directory,end,spool):
    """One bounded fragment at a time, two fixed replay owners, disk joins."""
    states={};counts={};seen_pages=0
    for row in lines(directory/"moves.jsonl",8*(4*end["Admission"]["Grants"]["MaxPages"]+128)):
        q=cursor(checked(blob(row["Request"]),b"GSQ\1",2<<20));q.take(1+8+16+40+4+4)
        for _ in range(3):q.field(65536)
        p=cursor(checked(q.field(1<<20),b"GSC\1",1<<20));p.field(256);command=p.field(1<<20);p.done()
        cmd=move_command(command)
        if cmd["phase"]!=4:continue
        move=cmd["intent"]["move"];sequence=cmd["sequence"];key=move+u64(sequence)
        old=spool.find(b"import-command",key)
        if old is not None:require(old==command,"SAME imported native phase changed body");continue
        spool.insert(b"import-command",key,command)
        if sequence==0:
            c=cursor(checked(cmd["data"],b"GMP\6",(1<<20)-128));descriptor=c.field(16384);c.field(129<<10);require(not c.field((1<<20)-1024),"initial handoff has no substituted target state")
            final=replay_cursor(c.field(129<<10));empty=c.field(129<<10)
            require(final["complete"] and checked(empty,b"GEC\1",129<<10)==bytes(82),"source complete replay census before target import")
            require(move not in states and len(states)<2,"actual at-most-two independent HOME streams")
            states[move]={"final":final,"cursor":None,"fragment":None};counts[move]=0
            continue
        if not cmd["data"] or cmd["data"][0]!=3:continue
        require(move in states,"replay before source-authenticated Import0")
        state=states[move];b=cursor(checked(cmd["data"][1:],b"GEB\1",cmd["intent"]["bytes"]));total=integer(b.number(4),1,32)
        for offset in range(total):
            c=cursor(checked(b.field(cmd["intent"]["bytes"]),b"GEP\1",1<<20));before_wire,next_wire=c.field(129<<10),c.field(129<<10)
            before,next_=replay_cursor(before_wire),replay_cursor(next_wire);original,value=c.field(65536),c.field(1<<20)
            control,complete,done=c.number(1),c.number(1),c.number(1);start,size=c.number(4),c.number(4);c.done()
            require(control in (0,1) and complete in (0,1) and done in (0,1) and bool(complete)==next_["complete"] and start==before["offset"] and size<=1<<20 and start+len(value)<=size
                    and next_["sequence"]==before["sequence"]+1,"canonical exact fragment/page counters")
            if state["cursor"] is None:require(before["family"]==1 and before["sequence"]==before["rows"]==before["size"]==0 and before["digest"]==digest(b"rho-tkg:range-move-replay:v1\0"),"genuine original replay start")
            else:require(before==state["cursor"],"original replay cursor gap/overlap")
            if not original:
                require(not value and not control and not done and start==size==0 and next_["rows"]==before["rows"] and next_["size"]==before["size"] and next_["digest"]==before["digest"]
                        and (next_["family"]==before["family"] and next_["after"]>before["after"] or next_["family"]==before["family"]+1 and not next_["after"]),"empty scan transition cannot hide original bytes")
            else:
                require(before["family"]==next_["family"]<11,"original record cannot skip a native family")
                if start==0:require(state["fragment"] is None,"unfinished original cannot be replaced");state["fragment"]=(original,bool(control),bytearray())
                key_,control_,owned=state["fragment"]
                require(key_==original and control_==bool(control) and len(owned)==start,"same whole-record identity/offset across installed batches")
                owned.extend(value)
                if done:
                    require(len(owned)==size and next_["rows"]==before["rows"]+1 and next_["size"]==before["size"]+len(original)+size+17
                            and next_["digest"]==digest(before["digest"]+bytes([control])+u64(len(original))+original+u64(size)+bytes(owned)),"whole original equality/digest/census including fragments")
                    original_record(original,bytes(owned),bool(control),spool)
                    scalar.put(spool,b"imported-original/"+move,original,digest(owned));state["fragment"]=None;counts[move]+=1
                else:require(offset==total-1 and len(owned)<size and next_["offset"]==len(owned) and next_["digest"]==before["digest"] and next_["rows"]==before["rows"] and next_["size"]==before["size"],"nonfinal record ends the native batch; no invisible own-write continuation")
            state["cursor"]=next_;seen_pages+=1
        b.done()
    require(len(states)==end["MoveCount"] and all(s["fragment"] is None and s["cursor"]==s["final"] and counts[m]==s["final"]["rows"] for m,s in states.items()),"ALL complete original groups/fragment EOF for every HOME import")
    return {"completeOriginalRecords":sum(counts.values()),"logicalReplayPages":seen_pages,"streams":len(states)}


def business(directory,end,spool):
    n=end["spec"]["facts"];names=("create","K","first-half-update","checkpoint-update","remaining-update")
    phases=end["BusinessPhases"];require(type(phases) is list and len(phases)==5,"five disjoint genuine business walls")
    counters=[{"operations":0,"single":0,"multi":0,"readParticipants":[0,0],"writeParticipants":[0,0],"protocolParticipants":[0,0],"attempts":0,"nativeProspectiveWrites":0,"nativeProspectiveBytes":0} for _ in names]
    for i,p in enumerate(phases):
        fields(p,"Phase StartNanos StopNanos WallNanos SingleParticipant MultiParticipant ReadParticipants WriteParticipants")
        require(p["Phase"]==names[i] and 0<=integer(p["StartNanos"])<integer(p["StopNanos"])<=end["TotalNanos"]
                and p["WallNanos"]==p["StopNanos"]-p["StartNanos"] and (i==0 or phases[i-1]["StopNanos"]<=p["StartNanos"]),"actual nonoverlapping phase wall, not sum of parallel latencies")
        # The fresh Go record intentionally leaves diagnostic participant slots
        # zero; derive them below from originals and real postmove ownership.
        require(p["SingleParticipant"]==p["MultiParticipant"]==0 and p["ReadParticipants"]==p["WriteParticipants"]==[0,0],"unmeasured producer counters cannot become footprint authority")
    def phase_index(phase,fact):
        return 0 if phase=="create" else 1 if fact==0 else 2 if fact<n//2 else 3 if fact==n//2 else 4
    for a in lines(directory/"attempts.jsonl",16*n+9):
        phase,fact,ordinal=operation(end["spec"],a["Operation"])
        body=blob(a["Proposal"]);parsed=codec.typed.pgq(body);provenance,ops=codec.typed.operations(body,allow_present_correction=True)
        require(provenance==ordinal and parsed["kind"]==6,"actual complete fixed-slice command/provenance")
        if phase=="setup":
            require(len(ops)==1 and ops[0]["kind"]==1,"actual setup original");continue
        owner=7+2*fact;target=2*n+7 if scalar.cross(end["spec"],fact) else 3
        retained_=decoded(spool.lookup(b"row",u64(4+fact)+u64(owner)+b"\1"));scope=blob(retained_["Scope"])
        if phase=="create":
            require(len(ops)==2 and [(o["kind"],o["owner"],o["life"]) for o in ops]==[(2,owner,owner+1),(8,owner,owner+1)]
                    and ops[0]["record"]==(0,0,0,b"edge",1,target,1,6,2) and ops[0]["binding"]==(0,0,2,target+1),"actual immutable relationship/life/endpoints")
        else:
            require(len(ops)==2 and [(o["kind"],o["owner"],o["life"]) for o in ops]==[(8,owner,owner+1),(5,owner,owner+1)]
                    and ops[1]["present"]==1 and ops[1]["record"]==(0,0,0,b"",0,0,0,0,0) and ops[1]["binding"]==(0,0,0,0)
                    and not ops[1]["name"] and not ops[1]["scalar"] and ops[1]["value"]==0,"Set first requires existing relation; Correct then authenticates live endpoints, one useful transaction")
        set_=ops[1] if phase=="create" else ops[0];value=5 if phase=="create" else 6
        require(set_["name"]==b"p" and set_["value"]==value and set_["scalar"]==b"\4"+u64(value) and set_["present"]==0
                and all(o["scope"]==scope for o in ops),"complete actual guarded scalar semantics, no dummy remote operation")
        idx=phase_index(phase,fact);c=counters[idx];c["attempts"]+=1
        actual=placement(end["spec"],phase,fact);protocol=actual
        if a["Manifest"] is not None:
            m=manifest(blob(a["Manifest"]));protocol=sorted(m["floors"])
            for part,effect in m["kernels"].items():
                count,size=prospective_writes(effect,part)
                if a["Classification"]=="commit":
                    c["nativeProspectiveWrites"]+=count;c["nativeProspectiveBytes"]+=size
                    c["writeParticipants"][0 if part==3 else 1]+=bool(count)
        elif a["Classification"]=="commit":c["writeParticipants"][0 if actual[0]==3 else 1]+=1
        if a["Classification"]!="commit":continue
        c["operations"]+=1;c["single" if len(actual)==1 else "multi"]+=1
        for part in actual:c["readParticipants"][0 if part==3 else 1]+=1
        for part in protocol:c["protocolParticipants"][0 if part==3 else 1]+=1
        spool.insert(b"transfer-business",u64(ordinal),bytes([idx]))
    expected=(n,1,n//2-1,1,n//2-1)
    for record in lines(directory/"operations.jsonl",2*n+4):
        phase,fact,ordinal=operation(end["spec"],record["Sample"]["operation"])
        if phase=="setup":continue
        i=spool.lookup(b"transfer-business",u64(ordinal))[0];p=phases[i]
        require(p["StartNanos"]<=record["StartNanos"]<record["StopNanos"]<=p["StopNanos"] and record["ElapsedNanos"]==record["StopNanos"]-record["StartNanos"],"whole operation queue/retry/wait fits actual measured phase")
        spool.insert(b"transfer-business-latency/"+bytes([i]),u64(record["ElapsedNanos"])+u64(ordinal),b"1")
    result=[]
    for i,c in enumerate(counters):
        require(c["operations"]==expected[i] and c["single"]+c["multi"]==expected[i],"all useful complete native business originals")
        if i>=2:require(c["readParticipants"]==c["writeParticipants"]==[0,expected[i]],"actual BOTH-moved endpoints/relations are native8 local; do not infer pre-move cross percentage")
        percentiles={};count=0
        for count,(key,_) in enumerate(spool.walk(b"transfer-business-latency/"+bytes([i])),1):
            for p in (50,95,99):
                if count==(expected[i]*p+99)//100:percentiles[f"p{p}Nanos"]=int.from_bytes(key[:8],"big")
        require(count==expected[i] and len(percentiles)==3,"complete phase latency census")
        result.append({"phase":names[i],**c,**percentiles,"wallNanos":phases[i]["WallNanos"],"usefulTransactionsPerSecond":count*1_000_000_000/phases[i]["WallNanos"]})
    require(sum(p["WallNanos"] for p in phases)==end["BusinessNanos"],"throughput uses actual disjoint business walls")
    return result


def verify(directory,spool_directory,policy,*,binary_sha=None,expected_spec=None,expected_run=None,expected_deadline=None,expected_seconds=None,shared_spool=None):
    directory=Path(directory).resolve(strict=True)
    with (directory/"result.json").open("rb") as stream:body=stream.read(LINE+1)
    require(body.endswith(b"\n") and len(body)<=LINE,"bounded complete native Cost12 terminal")
    end=decoded(body[:-1]);s=shape(end["spec"]);a=end["Admission"];g=validate_grants(a["Grants"])
    fields(a,"Version Family Run Spec Transport BudgetSeconds CausalMinimumMillis DeadlineUnixNanos Graph Topology Declaration BinarySHA256 Grants")
    require(integer(end["version"],1,1) and integer(a["Version"],1,1) and end["format"]==FORMAT and end["engine"]=="native-raft" and a["Family"]==FAMILY and a["Spec"]==s
            and end["RetentionContract"]=="held-C-through-BOTH-HOME-target-restart-and-eligible-old-owner-GC-then-rolling-ACK-feed-only-v1"
            and end["FramesCeiling"]==frames(s,g) and digest(encoded(a))==array(end["AdmissionSHA256"],32),"explicit fresh pooled transfer/admission, no legacy relabel")
    require(integer(a["BudgetSeconds"],1,MAX_SECONDS)*1000>integer(a["CausalMinimumMillis"])==8*a["Transport"]["targetRTTMillis"]
            and integer(a["DeadlineUnixNanos"],1,(1<<63)-1) and type(a["Run"]) is str and len(a["Run"])==32
            and bytes.fromhex(a["Run"]).hex()==a["Run"] and any(bytes.fromhex(a["Run"])),"same immutable original finite run/deadline/causal floor")
    for expected,actual in ((binary_sha,a["BinarySHA256"]),(expected_spec,s),(expected_run,a["Run"]),(expected_deadline,a["DeadlineUnixNanos"]),(expected_seconds,a["BudgetSeconds"])):require(expected is None or expected==actual,"actual controller/source immutable input mismatch")
    certs={name:cut(end[name]) for name in ("Initial","Snapshot","Checkpoint","Final")}
    for group in range(2):
        first=certs["Initial"][group]
        require(array(a["Graph"],16)==first["scope"][:16] and a["Topology"]==int.from_bytes(first["scope"][48:56],"big") and array(a["Declaration"],32)==first["scope"][56:88]
                and all(c[group]["scope"]==first["scope"] and c[group]["configuration"]==first["configuration"] for c in certs.values()),"actual graph/bucket/configuration/cut lineage throughout ownership handoff")
        require([certs[k][group]["sourceIndex"] for k in certs]==sorted(certs[k][group]["sourceIndex"] for k in certs),"genuine ordered capture boundaries")
    require(Path(end["ArtifactDirectory"]).resolve(strict=True)==directory and Path(end["Directory"]).resolve(strict=True).parent==directory and Path(end["Directory"]).name.startswith("rho-native-transfer-"),"exact actual owned engine directory")
    require(type(end["Completed"]) is list and len(end["Completed"])==32 and all(type(v) is int and v>=0 for v in end["Completed"]),"actual independent caller census")
    for name in ("SetupNanos","BusinessNanos","TransferNanos","DiagnosticNanos","TotalNanos","Pages","BusinessPages","ProgressPages","OriginalCaptures","ExtraInvocations","ControlExtraInvocations"):integer(end[name],0,(1<<63)-1)
    require(end["BusinessNanos"]>0 and end["TransferNanos"]>0 and sum(end[k] for k in ("SetupNanos","BusinessNanos","TransferNanos","DiagnosticNanos"))==end["TotalNanos"],"all actual business/engine-control/certification/diagnostic costs retained")
    selected=codec.owned_transport(end["Transport"],{"version":1,"family":codec.FAMILY,"facts":s["facts"],"crossPercent":s["crossPercent"],"writers":s["writers"],"layout":s["layout"]},maximum_frames=end["FramesCeiling"]*1024)
    require(selected==a["Transport"] and len(end["Transport"])==2,"both real native scheduler cohorts retain exact transport admission")
    with (nullcontext(shared_spool) if shared_spool is not None else Spool(spool_directory,policy)) as spool:
        spool.insert(b"transfer-grants",b"pages",u64(g["MaxPages"]))
        scalar._scalar_grants(directory,end,spool,AGREEMENT,cut,transfer=True)
        scalar._scalar_claims(directory,end,spool,operation_version=12)
        scalar._scalar_attempts(directory,end,spool,business_only=True,operation_version=12,
                                placement=lambda phase,fact:placement(s,phase,fact),
                                coordinated=lambda phase,fact:phase=="update" and fact==0 or phase=="create" and scalar.cross(s,fact),
                                manifest_decode=manifest,manifest_check=manifest_check)
        originals=retry_originals(end,spool);submitted=moves(directory,end,spool);sources(directory,end,spool)
        imported=imported_originals(directory,end,spool);lookup_originals(directory,end,spool)
        for selected_cut in certs.values():authenticate_cut(selected_cut,end,spool)
        issuance=allocation(directory,end,spool);handoff=transfer(directory,end,spool);query=queries(directory,end,spool)
        feed_calls=feed(directory,end,spool);collection=gc(directory,end,spool);columns(directory,end,spool);processes(directory,end,spool)
        footprints=business(directory,end,spool);answer=answers(end,spool)
        return {"format":FORMAT,"spec":s,"run":a["Run"],"logicalAnswersSHA256":answer,"nativeGrants":g,"transport":selected,
                "minimumContract":MINIMUM_CONTRACT,"retentionContract":end["RetentionContract"],"genericAtAfterRelease":query["genericAtAfterRelease"],
                "actualBusinessFootprints":footprints,"operationWalls":{"setupNanos":end["SetupNanos"],"businessNanos":end["BusinessNanos"],"transferNanos":end["TransferNanos"],"diagnosticNanos":end["DiagnosticNanos"],"totalNanos":end["TotalNanos"]},
                "physicalReclamation":collection["contract"],"physicalCollection":collection,"sourceQueries":query,"feedCalls":feed_calls,"issuance":issuance,"handoff":handoff,"originals":originals,"moveSubmissions":submitted,"importedOriginals":imported,
                "persistedFileBytes":end["PersistedFileBytes"],"allocatedFileBytes":end["AllocatedFileBytes"],"spool":spool.report(),
                "scope":"complete actual BOTH-HOME native transfer originals/CDC/typed old/current/type-index/columns/cache/feed/ACK/GC/reopen and process joins; physical participant/cost differences preserved. Safe refusal never completes a mandatory cell; runtime/capacity/full matrix acceptance remains unexecuted."}
