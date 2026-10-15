#!/usr/bin/env python3
"""Actual native rolling-ACK feed/GC artifacts; copied recipes grant no authority.

The pre-collection full prefix and subsequent sparse suffixes are independently
joined to retained originals, every voter, complete pages, physical collector
cursors and genuinely different column intervals. All growing joins use one
caller-owned finite spool. No Stage reducer or family acceptance wrapper.
"""
from contextlib import nullcontext
import hashlib
from pathlib import Path

import native_feed_check as scalar
import native_create_update_check as guarded
from comparison_spool import Spool, u64

codec,raw,column=scalar.codec,scalar.raw,scalar.column
require,integer,fields,blob,array,checked,cursor,digest,decoded,encoded,lines=(scalar.require,scalar.integer,scalar.fields,scalar.blob,scalar.array,scalar.checked,scalar.cursor,scalar.digest,scalar.decoded,scalar.encoded,scalar.lines)
FORMAT="native-snapshot-feed-retained-only-workload-v1"
FAMILY,LINE,TRANSPORTS,MAX_SECONDS=scalar.FAMILY,scalar.LINE,scalar.TRANSPORTS,scalar.MAX_SECONDS
CONTRACT="snapshot-token-relinquished-native-rolling-ack-prefix-v1"
AGREEMENT=digest(scalar.AGREEMENT+b"feed-retention=irreversible-rolling-ack-anchor-v1\0snapshot-reopen=authenticated-expired-v1\0payload-anchor=exact-plm-round-image-v1\0")
SOURCE_STAGES=("before-gc-complete-prefix","post-gc-retained-suffix","reopened-retained-suffix")
COLUMN_STAGES={"old-seal/native","old-seal/fill","old-seal/cached","old-after-first-update-wave","actual-post-GC-checkpoint-seal/native","actual-post-GC-checkpoint-seal/fill","actual-post-GC-checkpoint-seal/cached","reopened-checkpoint-false","reopened-checkpoint-true"}


def shape(s):
    fields(s,"version family facts crossPercent writers layout")
    require(integer(s["version"],3,3) and s["family"]==FAMILY and integer(s["facts"],20,1_000_000)%20==0 and integer(s["crossPercent"]) in (0,10,50,100) and integer(s["writers"]) in (1,8,32) and s["facts"]>s["writers"] and s["layout"] in ("kv","columns"),"explicit fresh retained-only dimensions")
    return s


def frames(s):
    n=shape(s)["facts"]
    return (16*n+3)*10+64*10+(64*n+1024)*5+8*((n+15)//16)+8192


def cut(wire):
    c=codec.cut(wire)
    require(len(c)==2 and [p["partition"] for p in c]==[3,8] and all(p["scope"][88:]==AGREEMENT for p in c),"exact fresh rolling-ACK cut scope")
    return c


def definition(end):
    wire=blob(end["Original"]);c=cursor(checked(wire,b"GFD\1",32768));manifest=c.field(32768)
    policy=(c.number(4),c.number(4),c.number(),c.number(),c.number(4),c.number(4),c.number(4),c.number());c.done()
    n=end["spec"]["facts"];pages=64*n+1024
    require(policy==(65536,16,pages,4*pages+n//8+1024,16384,16<<20,32<<20,end["Admission"]["DeadlineUnixNanos"]),"exact fresh durable cumulative admission/no renewed transient grant")
    c=cursor(checked(manifest,b"GPM\1",32768));scope,issuer,req=c.take(120),c.take(16),c.take(16)
    epoch,expires,count=c.number(),c.number(),c.number(4)
    require(scope[88:]==AGREEMENT and any(issuer) and count==2 and epoch<expires==100 and req==scalar.request(array(end["AdmissionSHA256"],32),"feed-owner",0),"exact original issuance/replicated policy epoch")
    certs,starts=[],[]
    for _ in range(2):
        cert_wire=c.field(512);cert=column.certificate(raw,scalar.base64.b64encode(cert_wire).decode());start=c.number();lineage=c.take(32)
        require(start==cert["sourceIndex"]+1 and lineage==digest(b"rho-tkg:retained-pin-source-lineage:v1\0"+cert_wire+u64(start)),"full initial feed prefix/lineage")
        certs.append(cert);starts.append(start)
    c.done();require(certs==cut(end["Snapshot"]),"exact actual captured snapshot")
    pin=digest(b"rho-tkg:retained-graph-pin:v1\0"+scope+issuer+req)
    return wire,manifest,issuer,pin,starts


def result_index(out):
    if out.startswith(b"GFO\2"):return int.from_bytes(checked(out,b"GFO\2",256)[112:120],"big")
    if out.startswith(b"GGO\1"):return int.from_bytes(checked(out,b"GGO\1",256)[65:73],"big")
    return scalar.result_index(out)


def retention_original(command,out,part):
    c=cursor(checked(command,b"GGC\1",512));scope,issuer,request=c.take(120),c.take(16),c.take(16)
    configuration,previous=c.take(32),c.take(32);floors=[c.number() for _ in range(6)];c.done()
    b=checked(out,b"GGO\1",256)
    require(len(b)==89 and scope[:16].hex()==raw.GRAPH and scope[16:24]==u64(part) and scope[88:]==AGREEMENT and any(issuer) and any(request) and any(configuration) and b[0] in (1,2,3) and b[1:33]==digest(command) and any(b[33:65]),"actual native retention original full command/scope/hash")
    index,source,round_=int.from_bytes(b[65:73],"big"),int.from_bytes(b[73:81],"big"),int.from_bytes(b[81:89],"big")
    require(0<source<index and all(f<(1<<64)-2 for f in floors),"actual retention original coordinates")
    return {"scope":scope,"issuer":issuer,"request":request,"configuration":configuration,"previous":previous,"floors":floors,"id":b[1:33],"sourceHash":b[33:65],"index":index,"source":source,"round":round_,"status":b[0]}


def same_outcome(out,earlier):
    if out.startswith((b"GRO\4",b"GRO\2")):
        a,b=checked(out,out[:4],120),checked(earlier,earlier[:4],120)
        return out[:4]==earlier[:4] and a[:81]==b[:81] and a[82:]==b[82:] and a[81]==2 and b[81]==1
    return out==earlier


def original_before_ack(frame,previous,spool,ack):
    part,index=frame["Partition"],frame["Index"];out=blob(frame["Outcome"]);original=result_index(out)
    require(original is not None and 0<original<ack and original<index and not blob(frame["CDC"]) and not frame["Rows"] and array(frame["Root"],172)==previous,"only authenticated earlier original below actual ACK can omit reclaimed sidecars")
    first=decoded(spool.lookup(b"frame",u64(part)+u64(original)))
    require(result_index(blob(first["Outcome"]))==original and same_outcome(out,blob(first["Outcome"])),"ACK-prefix replay joins exact actual first installation, never a guessed no-op")
    require(codec.optional(frame["OriginalOutcome"])==blob(first["Outcome"]) and (not codec.optional(frame["OriginalCDC"]) or blob(frame["OriginalCDC"])==blob(first["CDC"])),"immutable replay original changed")


def authenticate_frame(frame,previous,s,spool,fresh,before_ack,ack):
    out,cdc=blob(frame["Outcome"]),blob(frame["CDC"]);part,index=frame["Partition"],frame["Index"]
    if before_ack:
        original_before_ack(frame,previous,spool,ack);return
    if not out.startswith((b"GFO\2",b"GGO\1")):
        scalar.authenticate_frame(frame,previous,s,spool,fresh,guarded_updates=True);return
    original=result_index(out)
    if original<index:
        earlier=codec.optional(frame["OriginalOutcome"])
        require(earlier==out and not cdc and not frame["Rows"] and array(frame["Root"],172)==previous and spool.lookup(b"original",u64(part)+u64(original))==digest(earlier)+digest(codec.optional(frame["OriginalCDC"])),"fresh metadata same-original replay exact sidecars")
        return
    require(original==index and not cdc and not frame["Rows"] and not codec.optional(frame["OriginalOutcome"]) and not codec.optional(frame["OriginalCDC"]) and array(frame["Root"],172)==previous,"zero-business metadata binds actual original index/root")
    reg=cursor(checked(blob(frame["Registration"]),b"NCO1"));control,at,value=reg.field(4096),reg.number(),reg.field(32768);reg.done()
    require(at==index and len(control)==59 and control[:2]==b"ga" and control[3:19].hex()==raw.GRAPH and control[19:27]==u64(part),"actual immutable metadata control namespace")
    if out.startswith(b"GFO\2"):
        b=checked(out,b"GFO\2",256);x=checked(value,b"GFX\1",512)
        require(len(b)==130 and b[128]==5 and b[129] in (1,3,4,5,6) and any(b[:32]) and any(b[32:64]) and any(b[96:112]) and control[2]==35 and control[-16:]==b[96:112] and x[:32]==b[32:64] and x[32:]==out,"fresh phase5 only exact GFO2/GFX original")
        if fresh:scalar.put(spool,b"feed-result",u64(part)+b[32:64],out)
    else:
        x=cursor(checked(value,b"GGX\1",1024));command,result=x.field(512),x.field(256);x.done()
        r=retention_original(command,out,part)
        require(result==out and control[2]==25 and control[27:43]==r["issuer"] and control[43:]==r["request"] and r["source"]==index-1 and r["sourceHash"]==digest(previous),"GGO authentic preceding native root and original request")
        if fresh:scalar.put(spool,b"retention-original",u64(part)+r["id"],command+out)


def sources(directory,end,spool):
    initial=cut(end["Initial"]);floor=[integer(v,1) for v in end["SparseFrom"]]
    stage=group=0;after=initial[0]["sourceIndex"];previous=initial[0]["image"];counts=[[0,0] for _ in range(3)];tip=None
    for row in lines(directory/"sources.jsonl",6*(frames(end["spec"])+1)):
        require(stage<3 and row["Stage"]==SOURCE_STAGES[stage] and row["Member"]==1+3*group,"ordered genuine complete/sparse/reopen source owners")
        outer=row["Page"];fields(outer,"Page BeforeACK");p=outer["Page"];part=3+5*group
        fields(p,"Partition After Next Through Root Complete Frames")
        require(p["Partition"]==part and p["After"]==after and integer(p["Through"],after)>=integer(p["Next"],after) and p["Next"]-after==len(p["Frames"])<=4 and len(outer["BeforeACK"])==len(p["Frames"]) and p["Complete"] is (p["Next"]==p["Through"]),"bounded actual retained source prefix/EOF")
        requested=cut(end["BeforeGC"] if stage==0 else end["Final"])[group]
        require(p["Through"]>=requested["index"],"source tip precedes actual requested boundary")
        if tip is None:tip=(p["Through"],p["Root"])
        require((p["Through"],p["Root"])==tip,"physical source tip changed during bounded pagination")
        require(len(row["Voters"])==3*len(p["Frames"]),"three actual voter originals per frame")
        for offset,f in enumerate(p["Frames"]):
            require(f["Partition"]==part and f["Index"]==after+offset+1 and type(outer["BeforeACK"][offset]) is bool,"sparse prefix gap/flag type")
            for voter in range(3):require(row["Voters"][3*offset+voter]=={"Member":1+3*group+voter,"Index":f["Index"],"Outcome":f["Outcome"],"CDC":f["CDC"]},"actual voter outcome/CDC disagreement")
            key=u64(part)+u64(f["Index"]);prior=spool.find(b"frame",key)
            if prior is not None:
                old=decoded(prior)
                require(all(f[k]==old[k] for k in ("Partition","Index","Root","Outcome","CDC","Registration","Rows")),"physical reopen changed retained native data")
            ack=integer(end["GC"][3*group]["Plan"]["FeedFloor"],1)
            authenticate_frame(f,previous,end["spec"],spool,prior is None,outer["BeforeACK"][offset],ack)
            if prior is None:
                spool.insert(b"frame",key,encoded(f));spool.insert(b"original",key,digest(blob(f["Outcome"]))+digest(blob(f["CDC"])))
            counts[stage][group]+=len(f["Rows"] or []);previous=array(f["Root"],172)
        after=p["Next"]
        if p["Complete"]:
            require(previous==blob(p["Root"]),"actual completed source tip image")
            spool.insert(b"source-eof",bytes([stage,group]),u64(after));group+=1;tip=None
            if group==2:stage,group=stage+1,0
            if stage<3:
                after=initial[group]["sourceIndex"] if stage==0 else floor[group]
                previous=initial[group]["image"] if stage==0 else array(decoded(spool.lookup(b"frame",u64(3+5*group)+u64(after)))["Root"],172)
        else:require(p["Frames"],"nonterminal source made no progress")
    require(stage==3 and group==0 and counts==end["SourceRows"],"complete real three-stage raw counts, no full-prefix claim after GC")
    # Every useful scalar effect was authenticated once, including the extra
    # endpoint-guarded presence revision. No dataset-sized in-memory row set.
    n=end["spec"]["facts"]
    for ordinal in range(1,2*n+4):
        a=decoded(spool.lookup(b"useful",u64(ordinal)));phase,fact,_=scalar.operation(end["spec"],a["Operation"])
        owner=(1,3,2*n+7)[fact] if phase=="setup" else 7+2*fact
        for component in ((1,) if phase=="setup" else (1,3)):
            spool.lookup(b"row",u64(ordinal)+u64(owner)+bytes([component]))
    for tx,wire in spool.walk(b"terminal"):
        a=decoded(wire);m=codec.manifest(blob(a["Manifest"]));d=codec.status(blob(a["Outcome"]))
        require(spool.lookup(b"phase",u64(3)+tx+b"\3")==blob(a["Outcome"]) and codec.status(spool.lookup(b"phase",u64(3)+tx+b"\1"))["digest"]==d["digest"],"complete actual original Register/Decide roster")
        for part,floor_ in m["floors"].items():
            prep=spool.find(b"phase",u64(part)+tx+b"\2");resolve=spool.find(b"phase",u64(part)+tx+b"\4")
            if d["commit"]:
                require(prep is not None and resolve is not None,"no half commit")
                p,r=codec.status(prep),codec.status(resolve)
                require(p["yes"] and p["floor"]>=floor_ and d["round"]>p["floor"] and r["commit"] and r["round"]==d["round"] and r["digest"]==d["digest"],"actual voted floor/complete decision")
            elif resolve is not None:require(not codec.status(resolve)["commit"],"ABORT cannot become COMMIT")
    return counts


def feed_command(scope,issuer,request_,phase,definition_,page_):
    action=digest(b"rho-tkg:retained-feed-action:v1\0"+request_+bytes([phase]))[:16]
    body=(b"GFL\2" if phase==5 else b"GFL\1")+scope+issuer+action+bytes([phase])+scalar.field(definition_)+scalar.field(page_)
    return body+digest(body),action


def reply_identity(q,a,error):
    require(integer(q["Version"],1,1),"canonical original feed request")
    integer(a["Version"],0,1)
    if a["Version"]==0:
        require(error and a["Action"]=="" and all(v is None for k,v in a.items() if k in ("Original","Token","Initial","Boundary","Page","Catalog","Schemas","Points","Completed","Cursor","Root")) and not any(array(a["Digest"],32)) and a["Replay"] is False and a["EOF"] is False and a["Next"]==a["Index"]==a["Nanos"]==a.get("Failure",0)==0,"unknown empty RPC reply grants no native outcome")
        return False
    require(integer(a["Version"],1,1) and a["Action"]==q["Action"],"same actual native reply action/version")
    return True


def feed(directory,end,spool):
    original,manifest,issuer,pin,positions=definition(end)
    token=cursor(checked(blob(end["Token"]),b"GFT\1",65536))
    require(token.field(32768)==original,"token changed original feed definition")
    native_token=cursor(checked(token.field(32768),b"GPT\1",32768));token.done()
    require(native_token.field(32768)==manifest and native_token.number(4)==2,"full original pin/keeper roster")
    for part in (3,8):
        publication=native_token.number();actual=decoded(spool.lookup(b"pin-result",u64(part)+u64(publication)))
        require(actual["phase"]==actual["status"]==1 and actual["publication"]==publication and actual["pin"]==pin.hex() and actual["digest"]==digest(manifest).hex(),"token must join every actual installed native publication")
    native_token.done()
    snapshot=cut(end["Snapshot"]);boundaries=(cut(end["BeforeGC"]),cut(end["Final"]));expected_boundary=boundaries[0]
    previous=bytes(32);sequence=0;newpages=business=progress=0;snapshot_next=0;replays=0;opened=0;released=False;retained=False;expired=set();eofs=set();acknowledged=None;before_gc_ack=None
    call=0;attempt=0;elapsed=0;extra=0;timings={};settled=True;original_call=None
    for r in lines(directory/"feed.jsonl",16*(end["PageCeiling"]+end["spec"]["facts"]//16+64)):
        q,a=r["Input"],r["Reply"]
        require(r["Member"]==2,"actual native feed caller")
        reply_present=reply_identity(q,a,r.get("Error"))
        n=integer(r["Call"],1);i=integer(r["Attempt"],1,8)
        if n!=call:
            require(settled and n==call+1 and i==1,"no next logical call before preceding exact result")
            call,attempt,elapsed=n,0,0;original_call=encoded(q);settled=False
        require(encoded(q)==original_call,"SAME retry cannot change input/deadline/page/token")
        require(i==attempt+1 and 0<integer(r["AttemptNanos"])<=integer(r["OperationNanos"]) and r["OperationNanos"]>=elapsed,"SAME operation retry/wait duration cannot reset")
        attempt,elapsed=i,r["OperationNanos"];extra+=i>1
        if r.get("Error"):
            if r["Stage"]=="actual-keeper-released-token":require(reply_present and a["Failure"]==3 and q["Token"]==end["Token"],"genuine native original-keeper terminal refusal");released=True;settled=True
            elif r["Stage"] in ("snapshot-token-expired-before-physical-collection","snapshot-token-expired-after-physical-GC"):
                require(reply_present and retained and q["Action"]=="snapshot" and a["Failure"]==5,"authentic BOTH-history/native expiry classification, not local journal state")
                expired.add(r["Stage"]);settled=True
            else:require(not reply_present or a.get("Failure",0) in (1,7),"unknown/invalid failure cannot be normalized to success")
            continue
        require(reply_present and a.get("Failure",0)==0,"successful transport cannot certify a failure result")
        settled=True;action=q["Action"]
        timings[action]=timings.get(action,0)+1
        spool.insert(b"feed-latency/"+action.encode(),u64(r["OperationNanos"])+u64(call),b"1")
        if action=="begin":require(a["Original"]==end["Original"] and a["Initial"]==end["Snapshot"],"original before first native publication")
        elif action=="publish":require(a["Token"]==end["Token"],"actual portable feed token")
        elif action=="open":require(q["Token"]==end["Token"],"same token, never journal metadata as authority");opened+=1
        elif action=="snapshot":
            require(q["From"]==snapshot_next and a["Next"]==min(snapshot_next+16,end["spec"]["facts"])
                    and len(a["Points"])==a["Next"]-snapshot_next and a["EOF"] is (a["Next"]==end["spec"]["facts"]),"exact held snapshot-C pagination")
            for offset,p in enumerate(a["Points"]):
                fact=scalar.point(end["spec"],p,5,spool,guarded_updates=True);require(fact==snapshot_next+offset,"snapshot omission/duplication");spool.insert(b"snapshot-point",u64(fact),b"1")
            snapshot_next=a["Next"]
        elif action=="page":
            wire=blob(a["Page"]);hash_=digest(wire);p=scalar.page(wire)
            expected_boundary=cut(q["Boundary"])
            require(expected_boundary in boundaries and (expected_boundary==boundaries[0] or retained),"only actual mid/final business boundaries")
            require(hash_==array(a["Digest"],32) and p["request"]==array(q["Request"],16) and p["pin"]==pin
                    and p["boundary"]==expected_boundary and a["EOF"] is bool(p["eof"]),"actual requested complete page/boundary")
            old=spool.find(b"feed-page",p["request"])
            if old is not None:
                require(a["Replay"] is True and old==wire,"SAME request after ACK/restart changed immutable result");replays+=1;continue
            sequence+=1;newpages+=1
            require(a["Replay"] is False and p["sequence"]==sequence and p["previous"]==previous and p["from"]==positions
                    and p["request"]==scalar.request(array(end["AdmissionSHA256"],32),"page",sequence),"full original page chain/vector")
            for group,cert in enumerate(expected_boundary):
                command,action_id=feed_command(cert["scope"],issuer,p["request"],2,original,wire)
                result=spool.lookup(b"feed-result",u64(cert["partition"])+digest(command))
                b=checked(result,b"GFO\1",256)
                require(b[:32]==pin and b[64:96]==hash_ and b[96:112]==action_id and b[128:]==b"\2\1", "actual all-participant native Seal result binds complete canonical page")
                require(p["next"][group]>=positions[group] and p["tips"][group][0]>=cert["index"] and p["tips"][group][2] in (0,1),"authentic physical tip distinct immutable GSC")
                tip=decoded(spool.lookup(b"frame",u64(cert["partition"])+u64(p["tips"][group][0])))
                require(array(tip["Root"],172)==p["tips"][group][1],"actual physical native tip root")
            covered={};business_fact=None
            for f in p["frames"]:
                part,index=f["Partition"],f["Index"];group=0 if part==3 else 1
                require(part in (3,8) and index==positions[group] and group not in covered,"exact next physical head")
                source=decoded(spool.lookup(b"frame",u64(part)+u64(index)))
                require(blob(source["Outcome"])==f["Outcome"] and blob(source["CDC"])==f["CDC"] and array(source["Root"],172)==f["Root"],"feed raw envelope binds actual native frame")
                next_=f["ProgressNext"] or index+1
                require(next_==p["next"][group] and index<next_<=index+32,"bounded full-prefix progress")
                covered[group]=next_
                if f["ProgressNext"]:
                    require(p["kind"]==0 and any(f["Census"]),"census cannot hide a business envelope")
                    for i in range(index,next_):
                        actual=decoded(spool.lookup(b"frame",u64(part)+u64(i)))
                        require(not blob(actual["CDC"]).startswith((b"GCE\1",b"GML\1")) and not actual["Rows"],"protocol census crossed genuine business delta")
                elif p["kind"]:
                    require(f["CDC"].startswith(b"GCE\1") and f["Round"]==p["round"],"business page has actual native business head")
                    if p["kind"]==2:
                        st=codec.status(f["Outcome"]);require(st["phase"]==4 and st["commit"] and st["tx"]==p["tx"].hex() and st["round"]==p["round"],"complete cross transaction envelope")
                        require(f["Registration"]==blob(source["Registration"]),"actual original cross registration payload")
                        terminal=decoded(spool.lookup(b"terminal",p["tx"]))
                        require(f["Decision"]==blob(terminal["Outcome"]) if part==3 else f["Decision"] in (b"",blob(terminal["Outcome"])),"actual retained immutable decision proof")
                    else:
                        b=raw.fixed_body(f["Outcome"],b"GRO\4",120)
                        require(digest(b"rho-tkg:cross-transaction-id:v1\0"+bytes.fromhex(raw.GRAPH)+u64(part)+b[25:41])[:16]==p["tx"],"actual local business request-qualified envelope identity")
                    for row in source["Rows"] or []:
                        fact=integer((row["Owner"]-7)//2,0,end["spec"]["facts"]-1)
                        require(business_fact is None or business_fact==fact,"one complete useful original per page")
                        ordinal=4+end["spec"]["facts"]+fact;component=integer(row["Component"])
                        require(component in (1,3) and row==decoded(spool.lookup(b"row",u64(ordinal)+u64(row["Owner"])+bytes([component]))),"actual full Set6+Correct presence delta, no collateral update")
                        spool.insert(b"feed-update" if component==3 else b"feed-presence",u64(fact),encoded(row));business_fact=fact
            for group in range(2):require(p["next"][group]==covered.get(group,positions[group]),"omitted participant advanced cursor")
            if p["kind"]==2:require(set(covered)=={0,1},"half transaction published")
            require(p["kind"] in (0,1,2),"this family has no manufactured move event")
            if p["kind"]:
                require(business_fact is not None and snapshot[0]["round"]<p["round"]<=expected_boundary[0]["round"],"actual business lies strictly after C and within requested complete boundary")
                scalar.payload_catalog(a,end["spec"],business_fact)
            else:require(a["Catalog"] is None and a["Schemas"] is None,"protocol progress invented semantic payload")
            business+=bool(p["kind"]);progress+=not p["kind"]
            for part,index,root,lookups in p["payloads"]:
                native=decoded(spool.lookup(b"frame",u64(part)+u64(index)));require(array(native["Root"],172)==root,"payload/dictionary lookup bound exact original root; configured but unused source may have no lookups")
            spool.insert(b"feed-page",p["request"],wire);spool.insert(b"feed-sequence",u64(sequence),p["request"])
            previous=hash_;positions=p["next"]
        elif action=="ack":
            wire=spool.lookup(b"feed-page",array(q["Request"],16));p=scalar.page(wire);hash_=digest(wire);expected_boundary=p["boundary"]
            require(array(q["Digest"],32)==hash_,"same complete ACK identity")
            for cert in expected_boundary:
                command,action_id=feed_command(cert["scope"],issuer,p["request"],3,original,wire)
                b=checked(spool.lookup(b"feed-result",u64(cert["partition"])+digest(command)),b"GFO\1",256)
                require(b[:32]==pin and b[64:96]==hash_ and b[96:112]==action_id and b[128:]==b"\3\1","actual all-participant durable ACK")
            complete=cut(a["Completed"])
            require(complete==expected_boundary if p["eof"] else complete in (snapshot,*boundaries),"ACK complete cut never half transaction")
            if p["eof"]:
                which=0 if expected_boundary==boundaries[0] else 1
                for fact in range(end["spec"]["facts"]//2 if which==0 else end["spec"]["facts"]):
                    spool.lookup(b"feed-update",u64(fact));spool.lookup(b"feed-presence",u64(fact))
                eofs.add(which)
            if acknowledged is None or p["sequence"]>=acknowledged[0]:acknowledged=(p["sequence"],p["next"])
        elif action=="retain":
            require(not retained and business==1 and acknowledged is not None,"irreversible transition with genuine unread business suffix and no unacknowledged page")
            for cert in snapshot:
                command,action_id=feed_command(cert["scope"],issuer,array(q["Request"],16),5,original,b"")
                b=checked(spool.lookup(b"feed-result",u64(cert["partition"])+digest(command)),b"GFO\2",256)
                require(b[:32]==pin and b[64:96]==bytes(32) and b[96:112]==action_id and b[128:]==b"\5\1","all keeper native irreversible GFO2 originals before logical snapshot expiry")
            before_gc_ack=list(acknowledged[1]);retained=True
    require(settled and extra==integer(end["FeedExtraInvocations"],0,64) and newpages==end["Pages"] and business==end["BusinessPages"]==end["spec"]["facts"] and progress==end["ProgressPages"]
            and snapshot_next==end["spec"]["facts"] and replays>=2 and opened>=2 and released and retained and len(expired)==2 and eofs=={0,1},"complete genuine snapshot/feed/loss/replay/terminal lifecycle")
    for fact in range(end["spec"]["facts"]):spool.lookup(b"snapshot-point",u64(fact));spool.lookup(b"feed-update",u64(fact));spool.lookup(b"feed-presence",u64(fact))
    last=scalar.page(spool.lookup(b"feed-page",spool.lookup(b"feed-sequence",u64(sequence))))
    require(last["eof"]==1,"no completed final feed boundary")
    for part in (3,8):spool.lookup(b"pin-terminal",u64(part)+pin+digest(manifest))
    require(before_gc_ack==[integer(end["GC"][g*3]["Plan"]["FeedFloor"],1) for g in range(2)],"native GC protects actual acknowledged full vector, not caller supplied age")
    return timings


def points(directory,end,spool):
    counts={name:0 for name in ("old-before-gc","final","reopened-final")}
    for r in lines(directory/"points.jsonl",3*end["spec"]["facts"]+1):
        stage=r["Stage"];require(stage in counts,"exact actual old/current/reopened observation")
        expected=end["Snapshot"] if stage=="old-before-gc" else end["Final"]
        require(r["Cut"]==expected,"point changed exact requested cut")
        fact=scalar.point(end["spec"],r["Point"],5 if stage=="old-before-gc" else 6,spool,guarded_updates=True)
        spool.insert(b"point",stage.encode()+u64(fact),b"1");counts[stage]+=1
    require(all(v==end["spec"]["facts"] for v in counts.values()),"all actual old/current/reopened cells including exact presence/revision")


def gc(directory,end,spool):
    owners={};complete=set();final=set();physical_pages=0
    # This finite bootstrap roster was already authenticated by _scalar_grants;
    # genesis anchors need not appear in the later sparse/full business prefix.
    for row in lines(directory/"grants.jsonl",64):
        c=cursor(blob(row["Proposal"]));scope=checked(c.field(256),b"APS\1",156)
        scalar.put(spool,b"bootstrap-root",scope[16:24]+u64(integer(row["Index"],1)),blob(row["Root"]))
    for trace in lines(directory/"gc.jsonl",6*(260*frames(end["spec"])+65536+134)):
        fields(trace,"Member Input Reply")
        member=integer(trace["Member"],1,6);group=(member-1)//3;part=3+5*group
        q,r=trace["Input"],trace["Reply"];action=q["Action"]
        require(integer(q["Version"],1,1) and integer(r["Version"],1,1) and r["Profile"]==FORMAT and r["Member"]==member and r["Action"]==action and r["DeadlineUnixNanos"]==end["Admission"]["DeadlineUnixNanos"] and array(r["Run"],16)==bytes.fromhex(end["Admission"]["Run"]) and integer(r["PID"],1),"actual collector profile/run/deadline/process")
        old=owners.get(member)
        if old is None:
            require(action=="inspect" and q["Sequence"]==r["Sequence"]==0 and any(array(r["Cookie"],16)),"actual native collector owner before mutation")
            owners[member]={"r":r,"sequence":0,"section":0,"pages":0,"original":None};old=owners[member]
        else:
            require(q["Cookie"]==r["Cookie"]==old["r"]["Cookie"] and q["Sequence"]==r["Sequence"]==old["sequence"]+1 and r["PID"]==old["r"]["PID"],"same immutable native owner/cursor, no imported caller capability")
            old["sequence"]+=1
            for field in ("Calls","Pages","GrantedRows","GrantedBytes","GrantedOutput"):require(integer(r[field])>=integer(old["r"][field]),"collector cumulative grant refunded")
        require(integer(r["Calls"],1)<=260*frames(end["spec"])+65536+134 and r["State"]=="no-view", "finite fresh collector source/physical owner exclusion")
        if action=="prepare":
            require(member in (1,4) and any(array(q["Request"],16)),"actual leader Host preparation")
            command=blob(r["Original"]);c=cursor(checked(command,b"GGC\1",512));scope=c.take(120);c.take(16);request_=c.take(16);c.take(32);c.take(32);floors=[c.number() for _ in range(6)];c.done()
            expected=integer(end["GC"][3*group]["Plan"]["FeedFloor"],3)
            require(request_==array(q["Request"],16)==scalar.request(array(end["AdmissionSHA256"],32),"physical-gc",group) and q["Floors"]==floors==[expected-1,expected-1,expected,expected,expected-1,1] and scope==cut(end["Initial"])[group]["scope"],"genuine native proposal binds exact requested ACK floors/profile")
            old["original"]=command
        elif action in ("original","recover"):
            command=blob(r["Original"])
            require(r["Found"] is True and r["CDC"] is not None and not blob(r["CDC"]),"retained GGO must be present, exact empty CDC")
            if action=="recover":require(blob(q["Original"])==command,"follower recovery binds same original, never recreates proposal")
            elif old["original"] is not None:require(command==old["original"],"Host prepared original changed")
            out=blob(r["Outcome"]);original=retention_original(command,out,part)
            require(original["status"]==1 and spool.lookup(b"retention-original",u64(part)+original["id"])==command+out,"collector joins actual native immutable installed GGC/GGO, not descriptive receipt alone")
            old["original"]=command;scalar.put(spool,b"gc-command",u64(part),command+out)
        elif action=="collect":
            require(old["original"] is not None,"no physical collection before actual original authentication")
            p=r["Progress"];require(array(p["PlanID"],32)==digest(old["original"]) and integer(p["Generation"],1) and integer(p["SourceIndex"],1)+1==integer(p["InstalledIndex"],2) and integer(p["Section"],old["section"],6)>=old["section"] and type(p["Complete"]) is bool and p["Complete"] is (p["Section"]==6),"actual exact native plan/generation/resumable cursor")
            require(integer(r["Work"]["Records"],0,512)>=0 and integer(r["Work"]["Bytes"],0,1<<20)>=0 and r["Pages"]==old["pages"]+1,"row/byte bounded genuine physical collection")
            expected=end["GC"][member-1]["Plan"]
            require((p["SourceIndex"],p["InstalledIndex"])==(expected["SourceIndex"],expected["InstalledIndex"]),"collector cursor belongs to this exact installed plan")
            old["pages"]=r["Pages"];old["section"]=p["Section"];physical_pages+=1
            if p["Complete"]:require(p["CursorBytes"]==p["CursorRecords"]==0,"no completed cursor orphan");complete.add(member)
        elif action=="current":
            require(member in complete and r["Found"] is True and r==end["GC"][member-1],"final actual native plan captured after physical completion")
            command,out=blob(r["Original"]),spool.lookup(b"gc-command",u64(part))[len(blob(r["Original"])):]
            actual=retention_original(command,out,part);plan=r["Plan"]
            require(array(plan["ID"],32)==actual["id"] and array(plan["PreviousID"],32)==actual["previous"] and array(plan["SourceHash"],32)==actual["sourceHash"] and plan["SourceIndex"]==actual["source"] and plan["InstalledIndex"]==actual["index"] and [plan[n] for n in ("RootFloor","DataFloor","FeedFloor","OutcomeFloor","RecoveryFloor")]==actual["floors"][:5] and plan["RootFloor"]==end["SparseFrom"][group],"actual sparse floor/census plan matches immutable source-bound proposal")
            require(actual["configuration"]==cut(end["Initial"])[group]["configuration"] and len(plan["Roots"] or [])<=64,"same actual current configuration and bounded complete anchor roster")
            prior=0
            for anchor in plan["Roots"] or []:
                index=integer(anchor["Index"],prior+1,plan["SourceIndex"]);prior=index
                source=spool.find(b"frame",u64(part)+u64(index))
                image=array(decoded(source)["Root"],172) if source is not None else spool.lookup(b"bootstrap-root",u64(part)+u64(index))
                require(array(anchor["ImageHash"],32)==digest(image) and type(anchor["Data"]) is bool,"plan exact real predecessor/image anchors")
            image=blob(r["Root"]["Image"])
            require(array(r["Root"]["ImageHash"],32)==digest(image) and r["Root"]["Index"]>=plan["InstalledIndex"],"actual post-collection root/hash")
            final.add(member)
        else:require(action in ("inspect","submit"),"unexpected collector operation")
        old["r"]=r
    require(set(owners)==complete==final==set(range(1,7)) and physical_pages>=6,"all six actual physical collectors completed")
    require(end["SparseFrom"][0]>cut(end["Snapshot"])[0]["sourceIndex"],"real root floor passed C, no retain-all GC label")
    return {"actualPages":physical_pages,"members":6,"source":"native Store physical collection; rolling predecessor/unread prefix conservatively retained"}


def column_generation(stage):
    if stage.startswith(("actual-post-GC-checkpoint-seal","reopened-checkpoint","retire-checkpoint-owner")):return 1
    require(stage.startswith(("old-seal","old-after-first-update-wave","retire-all-C-dependent-columns")),"explicit genuine column lifetime phase")
    return 0


def columns(directory,end,spool):
    require(integer(end["CheckpointFirst"],1)==end["spec"]["facts"]//2+1,"actual genuine post-GC checkpoint interval, not original initial history")
    if end["spec"]["layout"]=="columns":
        for group in range(2):
            initial=cut(end["Checkpoint"])[group];final=cut(end["Final"])[group]
            require(initial["sourceIndex"]>=end["SparseFrom"][group] and initial["round"]<=final["round"],"authentic retained checkpoint/source coordinates")
        for group in range(2):
            old,new=end["ColumnManifests"][0][group],end["ColumnManifests"][1][group]
            expected=(2+2*end["spec"]["facts"]) if group==0 else 1
            require(old["Rows"]==expected and new["Rows"]==(2*(end["spec"]["facts"]-end["CheckpointFirst"]) if group==0 else 0),"checkpoint columns contain only genuine retained delta prefix, no empty-to-C relabel")
    return scalar._scalar_columns(directory,end,spool,cut,frames(end["spec"]),end["Snapshot"],initial_cuts=(end["Initial"],end["Checkpoint"]),expected_stages=COLUMN_STAGES,generation_for_stage=column_generation)


def business(directory,end,spool):
    n=end["spec"]["facts"];counts={"create":0,"update":0};cross_count=0
    for a in lines(directory/"attempts.jsonl",16*n+4):
        participants,_=guarded.proposal(a,end["spec"],spool)
        phase,_,ordinal=scalar.operation(end["spec"],a["Operation"])
        if phase=="setup" or a["Classification"]!="commit":continue
        counts[phase]+=1;cross_count+=len(participants)>1
    require(counts=={"create":n,"update":n} and integer(end["SetupTransactions"],3,3) and end["LogicalCross"]==cross_count==2*(n*end["spec"]["crossPercent"]//100) and end["LogicalLocal"]==2*n-cross_count,"actual business read/write/claim footprints, setup/protocol traffic separate")
    for r in lines(directory/"operations.jsonl",2*n+4):
        phase,_,ordinal=scalar.operation(end["spec"],r["Sample"]["operation"])
        if phase=="setup":continue
        require(r["StartNanos"]>=end["SetupNanos"] and r["ElapsedNanos"]<=end["BusinessNanos"],"full logical wait/retry duration, business wall excludes setup and diagnostics")
        spool.insert(b"business-latency",u64(r["ElapsedNanos"])+u64(ordinal),b"1")
    latency={};count=0
    for count,(key,_) in enumerate(spool.walk(b"business-latency"),1):
        for p in (50,95,99):
            if count==(2*n*p+99)//100:latency[f"p{p}Nanos"]=int.from_bytes(key[:8],"big")
    require(count==2*n and len(latency)==3,"all useful complete-operation percentiles from bounded disk ordering")
    return {**latency,"logicalOperations":count,"setupTransactions":3,"businessNanos":end["BusinessNanos"],"usefulTransactionsPerSecond":count*1_000_000_000/end["BusinessNanos"]}


def verify(directory,spool_directory,policy,*,binary_sha=None,expected_spec=None,expected_run=None,expected_deadline=None,expected_seconds=None,shared_spool=None):
    directory=Path(directory).resolve(strict=True)
    with (directory/"result.json").open("rb") as source:body=source.read(LINE+1)
    require(body.endswith(b"\n") and len(body)<=LINE,"bounded complete owned terminal result")
    end=decoded(body[:-1]);s=shape(end["spec"]);a=end["Admission"]
    require(integer(end["version"],1,1) and end["format"]==FORMAT and end["engine"]=="native-raft" and end["RetentionContract"]==CONTRACT and end["FramesCeiling"]==frames(s) and end["PageCeiling"]==64*s["facts"]+1024,"fresh Cost7/format/retention/finite-grant binding")
    require(integer(a["Version"],1,1) and a["Family"]==FAMILY and a["Spec"]==s and digest(encoded(a))==array(end["AdmissionSHA256"],32) and integer(a["BudgetSeconds"],1,MAX_SECONDS)*1000>integer(a["CausalMinimumMillis"]) and a["CausalMinimumMillis"]==5*a["Transport"]["targetRTTMillis"] and integer(a["DeadlineUnixNanos"],1,(1<<63)-1),"immutable original native admission/deadline, not recipe authority")
    require(type(a["Run"]) is str and len(a["Run"])==32 and bytes.fromhex(a["Run"]).hex()==a["Run"] and any(bytes.fromhex(a["Run"])),"nonzero exact original run")
    for expected,actual in ((binary_sha,a["BinarySHA256"]),(expected_spec,s),(expected_run,a["Run"]),(expected_deadline,a["DeadlineUnixNanos"]),(expected_seconds,a["BudgetSeconds"])):require(expected is None or expected==actual,"controller/source admission mismatch")
    certs={name:cut(end[name]) for name in ("Initial","Snapshot","BeforeGC","Checkpoint","Final","Reopened")}
    require(certs["Final"]==certs["Reopened"],"retention/replay controls invented graph progress")
    for group in range(2):
        first=certs["Initial"][group]
        require(all(c[group]["scope"]==first["scope"] and c[group]["configuration"]==first["configuration"] for c in certs.values()) and array(a["Graph"],16)==first["scope"][:16] and a["Topology"]==int.from_bytes(first["scope"][48:56],"big") and array(a["Declaration"],32)==first["scope"][56:88],"same actual graph/topology/configuration throughout GC/reopen")
    require(type(end["Completed"]) is list and len(end["Completed"])==32 and all(type(v) is int and v>=0 for v in end["Completed"]),"strict real caller census")
    require(Path(end["ArtifactDirectory"]).resolve(strict=True)==directory and Path(end["Directory"]).resolve(strict=True).parent==directory,"exact owned native artifact directory")
    require(all(type(end[k]) is int and end[k]>=0 for k in ("SetupNanos","BusinessNanos","DiagnosticNanos","TotalNanos","Pages","BusinessPages","ProgressPages","OriginalCaptures","ExtraInvocations","FeedExtraInvocations")) and end["BusinessNanos"]>0 and end["SetupNanos"]+end["BusinessNanos"]+end["DiagnosticNanos"]==end["TotalNanos"],"actual separate setup/business/diagnostic timing")
    selected=codec.owned_transport(end["Transport"],{"version":1,"family":codec.FAMILY,"facts":s["facts"],"crossPercent":s["crossPercent"],"writers":s["writers"],"layout":s["layout"]},maximum_frames=frames(s)*1024)
    require(selected==a["Transport"],"same real two-cohort packet/frame scheduler")
    for cohort in end["Transport"]:
        require(all(cohort[k]["Admitted"]<=frames(s)*1024 for k in ("Raft","SourceRequest","SourceReply")) and all(cohort[k]["AttemptedFrames"]<=frames(s)*1024 for k in ("PipeReads","PipeWrites")),"actual finite family source/packet grants")
    with (nullcontext(shared_spool) if shared_spool is not None else Spool(spool_directory,policy)) as spool:
        scalar._scalar_grants(directory,end,spool,AGREEMENT,cut);scalar._scalar_claims(directory,end,spool)
        scalar._scalar_attempts(directory,end,spool);sources(directory,end,spool);points(directory,end,spool)
        feeds=feed(directory,end,spool);collection=gc(directory,end,spool);columns(directory,end,spool);scalar.processes(directory,end,spool)
        timing=business(directory,end,spool);answer=hashlib.sha256()
        for value,stage in ((5,"old-before-gc"),(6,"final")):
            for fact in range(s["facts"]):
                spool.lookup(b"point",stage.encode()+u64(fact));spool.lookup(b"feed-update",u64(fact));spool.lookup(b"feed-presence",u64(fact))
                row=decoded(spool.lookup(b"row",u64(4+fact+(s["facts"] if value==6 else 0))+u64(7+2*fact)+b"\3"))
                answer.update(b"edge-i64-lifebound-v1\0"+b"".join(u64(v) for v in (fact,row["Owner"],row["Life"],row["Source"],row["Target"],row["SourceLife"],row["TargetLife"],row["After"]["Handle"]))+scalar.struct.pack(">q",row["After"]["Value"])+u64(row["After"]["Provenance"]))
        return {"spec":s,"run":a["Run"],"retentionContract":CONTRACT,"logicalAnswersSHA256":answer.hexdigest(),"transport":selected,"timing":timing,"feedCalls":feeds,"physicalCollection":collection,"spool":spool.report(),"scope":"actual native rolling-ACK retained-only snapshot-refusal/GC/unread-feed/checkpoint-column/source/voter/original/restart joins; installed-grant references are not Recipient.Next; conservative native history availability/cost differs from detached FDB, matching workload adapter and full V2 acceptance remain open"}
