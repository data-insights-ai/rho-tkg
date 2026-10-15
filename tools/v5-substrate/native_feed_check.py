#!/usr/bin/env python3
"""Bounded independent native snapshot-C/grouped-feed artifact joins.

Raw codecs and bounded native-control joins are shared; no family acceptance
wrapper or Stage reducer is an oracle. Fixed scalar operations define the footprint before observed effects.
All growing original/frame/row/caller/timing joins use the caller's finite spool.
"""
import base64
from contextlib import nullcontext
import hashlib
import json
import os
from pathlib import Path
import stat
import struct

import native_edge_unique_check as codec
from comparison_spool import Policy, Spool, u64

raw, column = codec.raw, codec.column
require, integer, fields, blob, encoded = raw.require, raw.integer, raw.fields, raw.raw_bytes, raw.go_json
array, checked, cursor, digest, decoded, lines = codec.array, codec.checked, codec.cursor, codec.digest, codec.decoded, codec.lines
FORMAT, FAMILY, LINE = "native-snapshot-feed-workload-v1", "snapshot-to-feed", 512 << 10
TRANSPORTS = {"local": 0, "rtt-1ms": 1, "rtt-20ms": 20, "rtt-100ms": 100}
MAX_SECONDS = 365*24*60*60
BASE = digest(b"rho-tkg:V2:durable-native-change-columns:v2\0exact-initial-final;retained-original-keeper;dependency-min;explicit-generation-retirement\0")
def semantic_agreement():
    descriptor=(b"rho-tkg:graphapply:partition-writes:v1\0genesis=default-schema-routing-v1\0range-publication=allocator-coinstalled-v1\0"
        b"routing=explicit-current-range-and-typed-buckets-v1\0axis-authority=AxisID-v1\0local-graph-writes=complete-footprint-v1\0global-value-identity=exact-v1\0"
        b"request-recovery=1\0cross-group-writes=unavailable\0certified-cuts=unavailable\0"
        b"cross-graph=ordered-coarse-prepare-kernel-v1\0recovery=protected-v2\0capacity-abort=global-TxID-owner-v1\0tx-id=sha256-128-graph-coordinator-request-v1\0")
    value=digest(descriptor)
    for extension in (
        b"certified-graph-reads=v1\0round-prefix=graph-and-routing-v1\0separate-fence=1\0",
        b"range-move=streamed-logical-and-protocol-preservation-v2\0admission=coarse-resolved-fence-v1\0placement=stable-range-epoch-v1\0replay=original-qualified-identity-v1\0retention=source-certified-lineage-v1\0recovery=protected-pages-and-precommit-abort-v1\0",
        b"conditional-graph=complete-native-prefix-v1\0original-register-exception=exact-v1\0",
        b"durable-cut-feed-pins=participant-ledger-v1\0terminal-bank=native-v2\0lease-policy=replicated-epoch-v1\0original-identity=issuer-request-v1\0",
        b"native-retention=authenticated-sparse-plan-v1\0source=current-plan-anchor-v1\0originals=immutable-controls-v1\0handoff=resolved-fence-retained-lineage-v1\0",
        b"durable-columns=exact-native-issuance-v1\0intent-cancel=ordered-tombstone-v1\0original-outcome-footprint=feed-min-v1\0",
        b"snapshot-feed=complete-native-envelopes-v1\0cursor=full-vector-pending-page-v1\0ack=same-original-all-keepers-v1\0payload=original-round-root-contributions-v1\0progress=bounded-native-prefix-census32-v1\0work=durable-admitted-operation-ceilings-v1\0"):
        value=digest(value+extension)
    return value
AGREEMENT=semantic_agreement()


def shape(s):
    fields(s, "version family facts crossPercent writers layout")
    require(integer(s["version"], 1, 1) and s["family"] == FAMILY and integer(s["facts"], 20, 1_000_000) % 20 == 0
            and integer(s["crossPercent"]) in (0, 10, 50, 100) and integer(s["writers"]) in (1, 8, 32)
            and s["facts"] > s["writers"] and s["layout"] in ("kv", "columns"), "explicit actual native feed dimensions")
    return s


def frames(s):
    n = shape(s)["facts"]
    return (16*n+3)*10+64*10+(32*n+256)*5+4*((n+15)//16)+4096


def cross(s, fact):
    return (fact+1)*s["crossPercent"]//100 > fact*s["crossPercent"]//100


def operation(s, q):
    fields(q, "Dataset Phase Fact Ordinal")
    phase, fact = q["Phase"], integer(q["Fact"])
    if phase == "setup":
        integer(fact, 0, 2); ordinal = fact+1
    else:
        require(phase in ("create", "update"), "fixed native feed operation phase")
        integer(fact, 0, s["facts"]-1)
        ordinal = 4+fact+(s["facts"] if phase == "update" else 0)
    require(array(q["Dataset"], 32) == digest(b"\1"+struct.pack(">QQ", s["facts"], s["crossPercent"])+b"graph-v2")
            and integer(q["Ordinal"], 1) == ordinal, "immutable native logical operation")
    return phase, fact, ordinal


def put(spool, ns, key, value):
    old = spool.find(ns, key)
    if old is None: spool.insert(ns, key, value)
    else: require(old == value, "immutable original/frame changed")


def cut(value):
    result = codec.cut(value)
    require(all(c["scope"][88:120] == AGREEMENT for c in result), "fresh exact feed semantic agreement")
    return result


def request(admission, domain, sequence):
    return digest(admission+domain.encode()+u64(sequence))[:16]


def attempts(directory, end, spool):
    return _scalar_attempts(directory, end, spool)


def _scalar_attempts(directory, end, spool):
    s, total, extra = end["spec"], 0, 0
    for a in lines(directory/"attempts.jsonl", 16*s["facts"]+4):
        fields(a, "Operation Engine ClientID AttemptOrdinal PreviousRequest Request Transaction BodySHA256 BusinessSHA256 DecisionDigest OriginalIndex Round Classification Error SubmitInvocations Proposal Manifest Outcome CDC")
        phase, fact, ordinal = operation(s, a["Operation"])
        client, number = integer(a["ClientID"], 0, s["writers"]-1), integer(a["AttemptOrdinal"], 1, 64)
        require(client == (ordinal-1) % s["writers"] and a["Engine"] == "native-raft" and a["Error"] == "", "actual independently assigned caller/terminal result")
        req, previous, tx = [array(a[k], 16) for k in ("Request", "PreviousRequest", "Transaction")]
        proposal, out, cdc = blob(a["Proposal"]), blob(a["Outcome"]), codec.optional(a["CDC"])
        require(any(req) and digest(proposal) == array(a["BodySHA256"], 32), "exact frozen business request/body")
        part = raw.native_parallel_proposal(proposal, req, ordinal)
        require(part == (8 if phase == "setup" and fact == 2 else 3), "actual proposal logical owner")
        prior = spool.find(b"attempt-last", u64(ordinal))
        if prior is None:
            require(number == 1 and not any(previous), "first immutable original")
        else:
            old = decoded(prior)
            require(old["Classification"] in ("authoritative-conflict", "authoritative-abort") and number == old["AttemptOrdinal"]+1
                    and previous == array(old["Request"],16) and req != previous and client == old["ClientID"], "only authenticated terminal refusal permits linked new original")
        remote = phase != "setup" and cross(s, fact)
        index = integer(a["OriginalIndex"], 1)
        if remote:
            m = codec.manifest(blob(a["Manifest"])); st = codec.status(out)
            require(m["tx"] == tx and m["digest"] == array(a["DecisionDigest"],32) and m["coordinator"] == 3
                    and st["phase"] == 3 and st["part"] == part and st["tx"] == tx.hex() and st["index"] == index and not cdc
                    and st["digest"] == m["digest"].hex() and st["round"] == integer(a["Round"])
                    and st["commit"] == (a["Classification"] == "commit") and a["Classification"] in ("commit","authoritative-abort"), "actual complete native COMMIT/ABORT decision")
            raw.native_parallel_manifest(blob(a["Manifest"]), req, tx, m["digest"], proposal)
            business = bytearray(proposal[:-32]); business[3] = 1
            require(array(a["BusinessSHA256"],32) == digest(bytes(business)+digest(business)), "frozen original business normalization")
            put(spool,b"terminal",tx,encoded(a))
        else:
            b = raw.fixed_body(out,b"GRO\4",120)
            require(b[:73] == b"\6"+bytes.fromhex(raw.GRAPH)+u64(part)+req+digest(proposal) and int.from_bytes(b[73:81],"big") == index
                    and b[81] == 1 and not any(tx) and not any(array(a["DecisionDigest"],32)) and a["Manifest"] is None,
                    "actual local Submit original, not an empty cross manifest")
            reason = int.from_bytes(b[82:],"big")
            require((reason == 0 and a["Classification"] == "commit" and cdc) or (reason in (8,11) and a["Classification"] == "authoritative-conflict" and not cdc), "exact local success/retained conflict")
            put(spool,b"terminal-local",u64(part)+req,encoded(a))
        calls = integer(a["SubmitInvocations"],1,8);extra += calls-1
        if a["Classification"] == "commit": spool.insert(b"useful",u64(ordinal),encoded(a))
        if prior is None: spool.insert(b"attempt-last",u64(ordinal),encoded(a))
        else: spool.replace(b"attempt-last",u64(ordinal),prior,encoded(a))
        total += 1
    require(2*s["facts"]+3 <= total <= integer(end["OriginalCaptures"],total,16*s["facts"]+3)
            and extra == integer(end["ExtraInvocations"],0,64), "finite actual original capture/SAME retry ledger")
    counts, completed = [0]*32, 0
    for record in lines(directory/"operations.jsonl",2*s["facts"]+4):
        fields(record,"Sample StartNanos StopNanos ElapsedNanos")
        sample = record["Sample"];phase,fact,ordinal = operation(s,sample["operation"])
        a = decoded(spool.lookup(b"useful",u64(ordinal)))
        client = integer(sample["clientID"],0,s["writers"]-1)
        require(client == (ordinal-1)%s["writers"] == a["ClientID"] and sample["clientAttempts"] == a["AttemptOrdinal"]
                and sample["phase"] == phase and sample["fact"] == fact and type(sample["cross"]) is bool
                and sample["cross"] == (phase != "setup" and cross(s,fact)), "completed useful operation joins full retained original and actual caller")
        start,stop,elapsed = [integer(record[k],0,(1<<63)-1) for k in ("StartNanos","StopNanos","ElapsedNanos")]
        require(stop-start == elapsed > 0 and stop <= end["TotalNanos"] and 0 <= integer(sample["admissionNanos"]) <= elapsed,
                "complete queued logical-operation latency includes all retries/waits")
        spool.insert(b"latency",u64(elapsed)+u64(ordinal),b"1")
        spool.insert(b"completed",u64(ordinal),encoded(sample));counts[client]+=1;completed+=1
    require(completed == 2*s["facts"]+3 and counts == end["Completed"] and all(counts[i] > 0 for i in range(s["writers"])),"exact admitted client census")
    expected = [0]*32
    for ordinal in range(1,2*s["facts"]+4):
        spool.lookup(b"completed",u64(ordinal));expected[(ordinal-1)%s["writers"]]+=1
    require(expected == counts, "wrong-client/omitted/duplicate useful operation")


def native_cell(c, component):
    present,kind = c.number(1),c.number(1)
    handle,payload,revision,provenance = [c.number() for _ in range(4)]
    require(present in (0,1) and (present == 0 and kind == handle == payload == revision == provenance == 0
            or present == 1 and kind == 2 and handle > 0 and payload == (9 if component == 3 else 0)), "canonical actual CC1 scalar/life cell")
    return dict(zip(column.CELL,(bool(present),False,handle,handle if component == 3 and present else 0,revision,provenance)))


def logical(frame, terminal, previous, spool):
    s,a = terminal["spec"],terminal["attempt"]
    phase,fact,ordinal = operation(s,a["Operation"])
    out,cdc = blob(frame["Outcome"]),blob(frame["CDC"])
    envelope=cursor(checked(cdc,b"GCE\1")); require(envelope.number(1)==6,"native logical command kind")
    tx,hash_,index=envelope.take(16),envelope.take(32),envelope.number()
    expect_tx=array(a["Transaction"],16) if a["Manifest"] is not None else array(a["Request"],16)
    expect_hash=array(a["DecisionDigest"],32) if a["Manifest"] is not None else array(a["BodySHA256"],32)
    require(tx==expect_tx and hash_==expect_hash and index==frame["Index"],"actual CDC original envelope")
    body=envelope.field(32768);envelope.done();c=cursor(checked(body,b"GCD\5"))
    image=array(frame["Root"],172)
    require(image[52:84]==digest(b"rho-tkg:graph-effects:v1\0"+previous[52:84]+u64(len(body))+body),"native effect-chain transition")
    require(c.take(16).hex()==raw.GRAPH and c.number()==frame["Partition"] and c.take(29)==bytes(29),"exact nonbootstrap native changes")
    axes=[]
    for _ in range(integer(c.number(4),0,1)):
        start=c.offset;c.take(16);require(c.number(1)==2 and c.number(2)==1,"rational axis profile")
        require(c.field(256).decode()==raw.SCOPE["reference"] and c.field(256).decode()==raw.SCOPE["canonicalUnit"],"actual declared axis meaning")
        axes.append(c.body[start:c.offset])
    entities=[(c.number(),c.number(1),c.number(4),c.field(256),c.number(),c.number(),c.number(1),c.number(1),c.number(1)) for _ in range(integer(c.number(4),0,1))]
    lives=[tuple(c.number() for _ in range(4)) for _ in range(integer(c.number(4),0,1))]
    values=[]
    for _ in range(integer(c.number(4),0,1)):
        value,scalar,axis=c.number(),c.field(64),c.number(4)
        require(value in (5,6) and scalar==b"\4"+u64(value) and axis==0,"actual native dictionary payload")
        values.append(value);put(spool,b"dictionary",u64(value),scalar)
    rows=[]
    for _ in range(integer(c.number(4),0,2)):
        owner,life,component,name,member,axis,scope=c.number(),c.number(),c.number(1),c.field(256).decode(),c.number(),c.number(4),c.field(256)
        require(member==0 and component in (1,3) and axis==1 and len(axes)==1,"actual component coordinate")
        axis=axes[0];require(scope==b"TS\1\2"+axis[:16]+digest(b"rho-tkg:axis:v1\0"+axis)+b"\6","full native all-scope")
        change=cursor(c.field(4096));require(change.take(4)==b"CC\1\2" and change.take(16)==axis[:16] and change.take(32)==digest(b"rho-tkg:axis:v1\0"+axis) and change.number(4)==1 and change.field(256)==scope,"native changed-region grammar")
        before,after=native_cell(change,component),native_cell(change,component);change.done()
        rows.append((owner,life,component,name,before,after,axis,scope))
    c.done()
    node=phase=="setup";part=frame["Partition"];remote=2*s["facts"]+7
    owner=(1,3,remote)[fact] if node else 7+2*fact
    life=owner+1;target=remote if cross(s,fact) else 3
    local=part==(8 if node and fact==2 else 3)
    expected_keys=([(owner,0,1,"")] if node else [(owner,0,1,""),(owner,life,3,"p")] if phase=="create" else [(owner,life,3,"p")]) if local else []
    expected_entities=[(owner,1 if node else 2,1,b"" if node else b"edge",0 if node else 1,0 if node else target,0 if node else 1,3 if node else 6,1 if node else 2)] if local and phase!="update" else []
    expected_lives=[(owner,life,0 if node else 2,0 if node else target+1)] if local and phase!="update" else []
    require(entities==expected_entities and lives==expected_lives and values==([5 if phase=="create" else 6] if local and not node and fact==0 else [])
            and [r[:4] for r in rows]==expected_keys,"fixed complete operation footprint: no phantom/collateral effects")
    require(len(frame["Rows"] or [])==len(rows),"typed row omission")
    for actual,(owner,life,component,name,before,after,axis,scope) in zip(frame["Rows"] or [],rows,strict=True):
        fields(actual," ".join(column.ROW));require((actual["Owner"],actual["Life"],actual["Component"],actual["Name"])==(owner,life,component,name)
            and actual["Before"]==before and actual["After"]==after and blob(actual["Axis"])==axis and blob(actual["Scope"])==scope,"typed raw cell/dictionary correspondence")
        require(after["Present"] and after["Revision"]>before["Revision"] and after["Provenance"]==ordinal and after["Handle"]==(owner+1 if component==1 else 5 if phase=="create" else 6),"exact intended native replacement")
        if phase=="update":require(before["Present"] and before["Handle"]==5 and before["Provenance"]==4+fact,"actual5->6 before-image")
        else:require(not before["Present"],"new row invented old state")
        require(actual["Kind"]==(1 if node else 2) and actual["Mode"]==(0 if node else 1) and actual["Interpretation"]==(3 if node else 6) and actual["Role"]==(1 if node else 2)
            and actual["Type"]==("" if node else "edge") and (actual["Source"],actual["Target"],actual["SourceLife"],actual["TargetLife"])==((0,0,0,0) if node else (1,target,2,target+1)),"actual immutable life/endpoint descriptor")
        key=u64(ordinal)+u64(owner)+bytes([component]);spool.insert(b"row",key,encoded(actual))


def result_index(out):
    if out.startswith(b"GCS\1"): return codec.status(out)["index"]
    if out.startswith(b"GRO\4") or out.startswith(b"GRO\2"): return int.from_bytes(checked(out,out[:4],120)[73:81],"big")
    if out.startswith(b"GPO\1"): return int.from_bytes(checked(out,b"GPO\1",256)[82:90],"big")
    if out.startswith(b"GFO\1"): return int.from_bytes(checked(out,b"GFO\1",256)[112:120],"big")
    return None


def authenticate_frame(frame, previous, s, spool, fresh):
    fields(frame,"Partition Index Root Outcome CDC Registration OriginalOutcome OriginalCDC Rows")
    part,index=integer(frame["Partition"]),integer(frame["Index"],1)
    require(part in (3,8) and frame["Outcome"] is not None and frame["CDC"] is not None,"missing native original is not empty")
    image=array(frame["Root"],172);column.root(raw,image,part)
    out,cdc,reg=[codec.optional(frame[n]) for n in ("Outcome","CDC","Registration")]
    original=result_index(out)
    key=u64(part)+u64(index)
    if original is not None and original<index:
        earlier=codec.optional(frame["OriginalOutcome"])
        same=earlier==out
        if out.startswith((b"GRO\4",b"GRO\2")):
            a,b=checked(out,out[:4],120),checked(earlier,out[:4],120)
            same=a[:81]==b[:81] and a[82:]==b[82:] and a[81]==2 and b[81]==1
        require(not cdc and not frame["Rows"] and image==previous and same
                and spool.lookup(b"original",u64(part)+u64(original))==digest(earlier)+digest(codec.optional(frame["OriginalCDC"])),"actual SAME-original replay/non-effects")
        return
    require(not codec.optional(frame["OriginalOutcome"]) and not codec.optional(frame["OriginalCDC"]),"fresh native record borrowed replay sidecars")
    terminal=None
    if out.startswith(b"GCS\1"):
        st,m=codec.status(out),codec.manifest(reg)
        require(st["part"]==part and st["index"]==index and bytes.fromhex(st["tx"])==m["tx"] and bytes.fromhex(st["digest"])==m["digest"],"actual native phase/registration")
        a=decoded(spool.lookup(b"terminal",m["tx"]))
        require(blob(a["Manifest"])==reg,"source Register roster equals genuine caller terminal roster")
        if fresh:spool.insert(b"phase",u64(part)+m["tx"]+bytes([st["phase"]]),out)
        if st["phase"]==3:require(blob(a["Outcome"])==out,"exact immutable Decide caller result")
        if st["phase"]==4 and st["commit"]:terminal=a
    elif out.startswith(b"GRO\4"):
        b=raw.fixed_body(out,b"GRO\4",120);a=decoded(spool.lookup(b"terminal-local",u64(part)+b[25:41]))
        require(out==blob(a["Outcome"]),"actual local original caller result")
        if a["Classification"]=="commit":terminal=a
    if terminal is not None:
        require(cdc and original==index,"business effects occur at first native installation only")
        if fresh:logical(frame,{"spec":s,"attempt":terminal},previous,spool)
        return
    require(not frame["Rows"] and image==previous,"protocol/refusal cannot alter business state")
    if out.startswith(b"GCS\1") or out.startswith(b"GRO\4"):
        require(not cdc,"ABORT/protocol produced business CDC");return
    if out.startswith(b"GPO\1") or out.startswith(b"GFO\1"):
        b=checked(out,out[:4],256)
        if out.startswith(b"GPO\1"):
            phase,status,pin,hash_,action=b[0],b[1],b[2:34],b[34:66],b[66:82]
            require(1<=phase<=8 and 1<=status<=5 and any(pin) and any(hash_) and any(action),"actual native pin result")
            pending=status==2;magic=b"GPX\1";tag=24
            require(not cdc or status==1 and cdc==out,"actual pin metadata CDC")
            if fresh:
                put(spool,b"pin-result",u64(part)+u64(index),encoded({"phase":phase,"status":status,"pin":pin.hex(),"digest":hash_.hex(),"publication":int.from_bytes(b[90:98],"big"),"epoch":int.from_bytes(b[98:106],"big"),"feed":int.from_bytes(b[106:114],"big")}))
                if phase in (3,4,5,8) and status in (1,4,5):put(spool,b"pin-terminal",u64(part)+pin+hash_,out)
        else:
            pin,hash_,action=b[:32],b[32:64],b[96:112];phase,status=b[128],b[129]
            require(1<=phase<=4 and 1<=status<=6 and any(pin) and any(hash_) and any(action) and not cdc,"actual native feed result")
            pending=status==2;magic=b"GFX\1";tag=35
            if fresh:put(spool,b"feed-result",u64(part)+hash_,out)
        if pending:require(not reg,"pending Stage observation is not a retained original")
        else:
            c=cursor(checked(reg,b"NCO1"));control,at,value=c.field(4096),c.number(),c.field(32768);c.done()
            x=checked(value,magic,512)
            require(len(control)==59 and control[:3]==b"ga"+bytes([tag]) and control[3:19].hex()==raw.GRAPH and control[19:27]==u64(part)
                    and control[-16:]==action and at==original==index and any(x[:32]) and x[32:]==out,"actual immutable metadata original/control coordinate")
        return
    require(not reg and not cdc,"unrecognized metadata cannot be called empty")
    if out.startswith(b"GSO\1"):
        phase,status,round_,original=struct.unpack(">BBQQ",raw.fixed_body(out,b"GSO\1",54))
        require(phase in (1,2) and status==1 and round_>0 and original==index,"genuine native certificate/read control")
    elif out.startswith(b"GRO\2"):
        require(original==index and len(checked(out,b"GRO\2",120))==84,"actual retained allocation control")
    else:require(not out,"unsupported nonempty native protocol outcome")


def sources(directory,end,spool):
    return _scalar_sources(directory,end,spool,cut,frames(end["spec"]))


def _scalar_sources(directory,end,spool,decode_cut,frame_ceiling):
    first=decode_cut(end["Initial"]);stages=("before-stop","reopened-replayed")
    stage,group,after,previous=0,0,first[0]["sourceIndex"],first[0]["image"]
    counts=[0,0]
    for row in lines(directory/"sources.jsonl",4*(frame_ceiling+1)):
        require(stage<2 and row["Stage"]==stages[stage] and row["Member"]==1+3*group,"physical source stage/owner order")
        p=row["Page"];part=3+5*group
        fields(p,"Partition After Next Through Root Complete Frames")
        require(p["Partition"]==part and p["After"]==after and integer(p["Through"],first[group]["index"])>=integer(p["Next"],after)
                and p["Next"]-after==len(p["Frames"])<=4 and p["Complete"] is (p["Next"]==p["Through"]),"complete bounded physical prefix")
        require(len(row["Voters"])==3*len(p["Frames"]),"all native voter raw records")
        for offset,f in enumerate(p["Frames"]):
            require(f["Partition"]==part and f["Index"]==after+offset+1,"source gap/reorder")
            for i in range(3):
                v=row["Voters"][3*offset+i]
                require(v=={"Member":1+3*group+i,"Index":f["Index"],"Outcome":f["Outcome"],"CDC":f["CDC"]},"actual voter original/CDC mismatch")
            key=u64(part)+u64(f["Index"]);value=encoded(f);old=spool.find(b"frame",key)
            require(old is None or old==value,"physical reopen changed exact retained source frame")
            authenticate_frame(f,previous,end["spec"],spool,old is None)
            if old is None:
                spool.insert(b"frame",key,value);spool.insert(b"original",key,digest(blob(f["Outcome"]))+digest(blob(f["CDC"])))
                counts[group]+=len(f["Rows"] or [])
            previous=array(f["Root"],172)
        after=p["Next"]
        if p["Complete"]:
            require(previous==blob(p["Root"]),"physical tip root/EOF")
            spool.insert(b"source-eof",bytes([stage,group]),u64(after));group+=1
            if group==2:stage,group=stage+1,0
            after,previous=first[group]["sourceIndex"],first[group]["image"]
        else:require(p["Frames"],"physical source made no progress")
    require(stage==2 and group==0 and counts==end["SourceRows"] and sum(counts)==3+3*end["spec"]["facts"],"complete raw/typed two-cohort prefix")
    for tx,wire in spool.walk(b"terminal"):
        a=decoded(wire);m=codec.manifest(blob(a["Manifest"]));d=codec.status(blob(a["Outcome"]))
        require(spool.lookup(b"phase",u64(3)+tx+b"\3")==blob(a["Outcome"]),"no orphan/missing retained decision")
        register=codec.status(spool.lookup(b"phase",u64(3)+tx+b"\1"));require(register["digest"]==d["digest"],"exact original Register")
        for part,floor in m["floors"].items():
            prep=spool.find(b"phase",u64(part)+tx+b"\2");resolve=spool.find(b"phase",u64(part)+tx+b"\4")
            if d["commit"]:
                require(prep is not None and resolve is not None,"committed complete participant roster")
                p,r=codec.status(prep),codec.status(resolve)
                require(p["yes"] and p["floor"]>=floor and d["round"]>p["floor"] and r["commit"] and r["round"]==d["round"] and r["digest"]==d["digest"],"real voted floor/complete commit")
            elif resolve is not None:require(not codec.status(resolve)["commit"],"durable ABORT changed to COMMIT")
    return counts


def point(s,p,value,spool):
    fields(p,"Fact Entity");fact=integer(p["Fact"],0,s["facts"]-1);r=p["Entity"]
    target=2*s["facts"]+7 if cross(s,fact) else 3
    require(r["id"]==7+2*fact and r["found"] is True and r["effective"] is True and r["kind"]==2 and r["type"]=="edge" and r["mode"]==1
            and r["source"]==1 and r["target"]==target and r["effectiveLife"]==8+2*fact and r["labels"]==[],"exact snapshot endpoint/life, no phantom entity")
    create=4+fact;ordinal=create+(s["facts"] if value==6 else 0);owner=7+2*fact
    for name,component,at,kind in (("presence",1,create,"life"),("property",3,ordinal,"i64")):
        cell=r[name];fields(cell,"present null handle revision provenance kind i64")
        expected=decoded(spool.lookup(b"row",u64(at)+u64(owner)+bytes([component])))["After"]
        actual={"Present":cell["present"],"Null":cell["null"],"Handle":integer(cell["handle"],1),
                "Value":integer(cell["i64"]),"Revision":integer(cell["revision"],1),"Provenance":integer(cell["provenance"],1)}
        require(cell["present"] is True and cell["null"] is False and cell["kind"]==kind and actual==expected,
                "point cell differs from authenticated original change, including historical revision/presence")
    require(r["presence"]["handle"]==r["effectiveLife"] and r["property"]["handle"]==r["property"]["i64"]==value,
            "exact old/current scalar and original presence life")
    return fact


def points(directory,end,spool):
    counts={name:0 for name in ("old-after-update","final","reopened-old","reopened-final")}
    for r in lines(directory/"points.jsonl",4*end["spec"]["facts"]+1):
        stage=r["Stage"];require(stage in counts,"actual old/current/reopened point phase")
        expected=end["Snapshot"] if stage.endswith("old") or stage=="old-after-update" else end["Final"]
        require(r["Cut"]==expected,"point substituted requested certificate")
        fact=point(end["spec"],r["Point"],5 if expected==end["Snapshot"] else 6,spool)
        spool.insert(b"point",stage.encode()+u64(fact),b"1");counts[stage]+=1
    require(all(n==end["spec"]["facts"] for n in counts.values()),"complete exact point census")


def feed_vector(c):
    n=integer(c.number(4),2,2);return [integer(c.number(),1) for _ in range(n)]


def feed_cut(c):
    require(c.number(4)==2,"full feed certified boundary")
    certs=[column.certificate(raw,base64.b64encode(c.field(512)).decode()) for _ in range(2)]
    require([x["partition"] for x in certs]==[3,8] and certs[0]["round"]==certs[1]["round"],"full complete feed cut")
    return certs


def page(wire):
    c=cursor(checked(wire,b"GFP\1",65536))
    p={"request":c.take(16),"sequence":c.number(),"pin":c.take(32),"previous":c.take(32)}
    p["from"],p["next"],p["boundary"],p["prior"]=feed_vector(c),feed_vector(c),feed_cut(c),feed_cut(c)
    p["tips"]=[(c.number(),c.take(172),c.number(1)) for _ in range(2)]
    p["kind"],p["tx"],p["round"],p["eof"]=c.number(1),c.take(16),c.number(),c.number(1)
    require(p["kind"] in (0,1,2,3) and p["eof"] in (0,1),"feed envelope enums")
    p["frames"]=[]
    for _ in range(integer(c.number(4),0,2)):
        f={"Partition":c.number(),"Index":c.number(),"Round":c.number(),"Root":c.take(172)}
        for name in ("Outcome","CDC","Registration","Intent","Decision","OriginalOutcome","OriginalCDC"):f[name]=c.field(65536)
        f["BeforeCut"],f["ProgressNext"],f["Census"]=c.number(1),c.number(),c.take(32)
        require(f["BeforeCut"] in (0,1),"feed original cutoff flag");p["frames"].append(f)
    require((p["kind"]==0 and not any(p["tx"]) and p["round"]==0) or (p["kind"]!=0 and any(p["tx"]) and p["round"]>0 and p["frames"]),"complete business identity cannot be empty/halfzero")
    p["payloads"]=[]
    for _ in range(integer(c.number(4),0,2)):
        part,index,root=c.number(),c.number(),c.take(172);lookups=[]
        for _ in range(integer(c.number(4),0,4096)):
            kind=c.number(1);key,upper,after=[c.field(65536) for _ in range(3)]
            rows,bytes_,visited,work=[c.number(4) for _ in range(4)];flags=c.number(1);next_=c.field(65536)
            values=[(c.field(65536),c.field(65536)) for _ in range(integer(c.number(4),0,4096))]
            require(kind in (1,2) and 1<=rows<=4096 and 1<=bytes_<=65536 and visited<=rows and work<=bytes_ and flags<=3,"bounded actual lookup transcript")
            lookups.append((kind,key,upper,after,flags,next_,values))
        p["payloads"].append((part,index,root,lookups))
    c.done();return p


def field(value):return struct.pack(">I",len(value))+value

def definition(end):
    wire=blob(end["Original"]);c=cursor(checked(wire,b"GFD\1",32768));manifest=c.field(32768)
    policy=(c.number(4),c.number(4),c.number(),c.number(),c.number(4),c.number(4),c.number(4),c.number());c.done()
    n=end["spec"]["facts"];pages=32*n+256
    require(policy==(65536,16,pages,4*pages+n//16+512,16384,16<<20,32<<20,end["Admission"]["DeadlineUnixNanos"]),"exact bounded feed policy/no allowance reset")
    c=cursor(checked(manifest,b"GPM\1",32768));scope,issuer,req=c.take(120),c.take(16),c.take(16)
    epoch,expires,count=c.number(),c.number(),c.number(4)
    require(count==2 and expires==100 and expires>epoch and req==request(array(end["AdmissionSHA256"],32),"feed-owner",0),"original native owner issuance identity/epoch")
    certs,starts=[],[]
    for group in range(2):
        cert=column.certificate(raw,base64.b64encode(c.field(512)).decode());start=c.number();lineage=c.take(32)
        require(start==cert["sourceIndex"]+1 and any(lineage),"exact initial snapshot/full feed start")
        certs.append(cert);starts.append(start)
    c.done();require(certs==cut(end["Snapshot"]),"actual captured C, not assumed scalar source")
    identity=digest(b"rho-tkg:retained-graph-pin:v1\0"+scope+issuer+req)
    return wire,manifest,issuer,identity,starts


def feed_command(scope,issuer,request_,phase,definition_,page_):
    action=digest(b"rho-tkg:retained-feed-action:v1\0"+request_+bytes([phase]))[:16]
    body=b"GFL\1"+scope+issuer+action+bytes([phase])+field(definition_)+field(page_)
    return body+digest(body),action


def payload_catalog(reply,s,fact):
    c=cursor(checked(blob(reply["Catalog"]),b"GCD\5",65536))
    require(c.take(16).hex()==raw.GRAPH and c.number()==3 and c.take(29)==bytes(29),"resolved original payload namespace")
    axes=[]
    for _ in range(integer(c.number(4),1,1)):
        start=c.offset;c.take(16);require(c.number(1)==2 and c.number(2)==1,"resolved axis profile")
        require(c.field(256).decode()==raw.SCOPE["reference"] and c.field(256).decode()==raw.SCOPE["canonicalUnit"],"resolved axis meaning")
        axes.append(c.body[start:c.offset])
    entities=[]
    for _ in range(integer(c.number(4),3,3)):
        entities.append((c.number(),c.number(1),c.number(4),c.field(256),c.number(),c.number(),c.number(1),c.number(1),c.number(1)))
    lives=[tuple(c.number() for _ in range(4)) for _ in range(integer(c.number(4),3,3))]
    values=[]
    for _ in range(integer(c.number(4),2,2)):
        identity,scalar,axis=c.number(),c.field(64),c.number(4)
        require(identity in (5,6) and scalar==b"\4"+u64(identity) and axis==0,"resolvable actual before/after i64 definitions")
        values.append(identity)
    require(c.number(4)==0,"catalog must not invent business effects");c.done()
    owner,life=7+2*fact,8+2*fact;target=2*s["facts"]+7 if cross(s,fact) else 3
    require(set(entities)=={(owner,2,1,b"edge",1,target,1,6,2),(1,1,1,b"",0,0,0,3,1),(target,1,1,b"",0,0,0,3,1)}
            and set(lives)=={(owner,life,2,target+1),(1,2,0,0),(target,target+1,0,0)} and set(values)=={5,6}
            and reply["Schemas"]==[{"Name":"p","Owner":2,"Type":4,"Cardinality":1,"Unique":0}],"complete exact original-root entity/life/schema/value payload, no current substitution")


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
    expected_boundary=cut(end["Final"]);snapshot=cut(end["Snapshot"])
    previous=bytes(32);sequence=0;newpages=business=progress=0;snapshot_next=0;replays=0;opened=0;released=False
    call=0;attempt=0;elapsed=0;timings={}
    for r in lines(directory/"feed.jsonl",16*(end["PageCeiling"]+end["spec"]["facts"]//16+64)):
        q,a=r["Input"],r["Reply"]
        require(q["Version"]==a["Version"]==1 and q["Action"]==a["Action"] and r["Member"]==2,"fresh actual feed RPC identity")
        if r["Stage"]!="actual-keeper-released-token":
            n=integer(r["Call"],1);i=integer(r["Attempt"],1,8)
            if n!=call:require(n==call+1 and i==1,"actual next bounded logical feed call");call,attempt,elapsed=n,0,0
            require(i==attempt+1 and 0<integer(r["AttemptNanos"])<=integer(r["OperationNanos"]) and r["OperationNanos"]>=elapsed,"SAME operation retry/wait duration cannot reset")
            attempt,elapsed=i,r["OperationNanos"]
        if r.get("Error"):
            if r["Stage"]=="actual-keeper-released-token":require(a["Failure"]==3 and q["Token"]==end["Token"],"genuine terminal refusal classification");released=True
            continue
        action=q["Action"]
        timings[action]=timings.get(action,0)+1
        spool.insert(b"feed-latency/"+action.encode(),u64(r["OperationNanos"])+u64(call),b"1")
        if action=="begin":require(a["Original"]==end["Original"] and a["Initial"]==end["Snapshot"],"original before first native publication")
        elif action=="publish":require(a["Token"]==end["Token"],"actual portable feed token")
        elif action=="open":require(q["Token"]==end["Token"],"same token, never journal metadata as authority");opened+=1
        elif action=="snapshot":
            require(q["From"]==snapshot_next and a["Next"]==min(snapshot_next+16,end["spec"]["facts"])
                    and len(a["Points"])==a["Next"]-snapshot_next and a["EOF"] is (a["Next"]==end["spec"]["facts"]),"exact held snapshot-C pagination")
            for offset,p in enumerate(a["Points"]):
                fact=point(end["spec"],p,5,spool);require(fact==snapshot_next+offset,"snapshot omission/duplication");spool.insert(b"snapshot-point",u64(fact),b"1")
            snapshot_next=a["Next"]
        elif action=="page":
            wire=blob(a["Page"]);hash_=digest(wire);p=page(wire)
            require(hash_==array(a["Digest"],32) and p["request"]==array(q["Request"],16) and p["pin"]==pin
                    and p["boundary"]==expected_boundary and q["Boundary"]==end["Final"] and a["EOF"] is bool(p["eof"]),"actual requested complete page/boundary")
            old=spool.find(b"feed-page",p["request"])
            if old is not None:
                require(a["Replay"] is True and old==wire,"SAME request after ACK/restart changed immutable result");replays+=1;continue
            sequence+=1;newpages+=1
            require(a["Replay"] is False and p["sequence"]==sequence and p["previous"]==previous and p["from"]==positions
                    and p["request"]==request(array(end["AdmissionSHA256"],32),"page",sequence),"full original page chain/vector")
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
                        fact=(row["Owner"]-7)//2;require(row["Component"]==3 and row["Name"]=="p" and row["Before"]["Value"]==5 and row["After"]["Value"]==6,"only authentic post-C5->6 update")
                        spool.insert(b"feed-update",u64(fact),encoded(row))
                        require(business_fact is None,"one complete fixed useful transaction per native business envelope");business_fact=fact
            for group in range(2):require(p["next"][group]==covered.get(group,positions[group]),"omitted participant advanced cursor")
            if p["kind"]==2:require(set(covered)=={0,1},"half transaction published")
            require(p["kind"] in (0,1,2),"this family has no manufactured move event")
            if p["kind"]:require(business_fact is not None,"empty useful family delta");payload_catalog(a,end["spec"],business_fact)
            else:require(a["Catalog"] is None and a["Schemas"] is None,"protocol progress invented semantic payload")
            business+=bool(p["kind"]);progress+=not p["kind"]
            for part,index,root,lookups in p["payloads"]:
                native=decoded(spool.lookup(b"frame",u64(part)+u64(index)));require(array(native["Root"],172)==root,"payload/dictionary lookup bound exact original root; configured but unused source may have no lookups")
            spool.insert(b"feed-page",p["request"],wire);spool.insert(b"feed-sequence",u64(sequence),p["request"])
            previous=hash_;positions=p["next"]
        elif action=="ack":
            wire=spool.lookup(b"feed-page",array(q["Request"],16));p=page(wire);hash_=digest(wire)
            require(array(q["Digest"],32)==hash_,"same complete ACK identity")
            for cert in expected_boundary:
                command,action_id=feed_command(cert["scope"],issuer,p["request"],3,original,wire)
                b=checked(spool.lookup(b"feed-result",u64(cert["partition"])+digest(command)),b"GFO\1",256)
                require(b[:32]==pin and b[64:96]==hash_ and b[96:112]==action_id and b[128:]==b"\3\1","actual all-participant durable ACK")
            complete=cut(a["Completed"])
            require(complete==expected_boundary if p["eof"] else complete in (snapshot,expected_boundary),"ACK complete cut never half transaction")
    require(newpages==end["Pages"] and business==end["BusinessPages"]==end["spec"]["facts"] and progress==end["ProgressPages"]
            and snapshot_next==end["spec"]["facts"] and replays>=3 and opened>=2 and released,"complete genuine snapshot/feed/loss/replay/terminal lifecycle")
    for fact in range(end["spec"]["facts"]):spool.lookup(b"snapshot-point",u64(fact));spool.lookup(b"feed-update",u64(fact))
    last=page(spool.lookup(b"feed-page",spool.lookup(b"feed-sequence",u64(sequence))))
    require(last["eof"]==1,"no completed final feed boundary")
    for part in (3,8):spool.lookup(b"pin-terminal",u64(part)+pin+digest(manifest))
    return timings


def columns(directory,end,spool):
    return _scalar_columns(directory,end,spool,cut,frames(end["spec"]),end["Snapshot"])


def _scalar_columns(directory,end,spool,decode_cut,frame_ceiling,old_cut):
    if end["spec"]["layout"]=="kv":
        require((directory/"columns.jsonl").stat().st_size==0,"KV invented derived generation work");return
    expected={"old-seal/native","old-seal/fill","old-seal/cached","final-seal/native","final-seal/fill","final-seal/cached","old-after-update",
              "reopened-0-false","reopened-0-true","reopened-1-false","reopened-1-true"}
    owners=[None,None];completed=set();sealed=set();retired=set();probed=set();cache=[None,None]
    for trace in lines(directory/"columns.jsonl",32*(frame_ceiling+end["Admission"]["BudgetSeconds"]//60+100)):
        if trace["Stage"]=="actual-keeper-retired-token":
            group=(integer(trace["Member"],1,4)-1)//3
            matches=[g for g in range(2) if end["ColumnManifests"][g][group]["Generation"]["ID"]==trace["Generation"]]
            require(len(matches)==1 and (matches[0],group) in retired and codec.response(trace["Response"])["code"]==8,"actual native keeper terminal refusal, distinct local journal state")
            probed.add((matches[0],group));continue
        stage,member,q=trace["Stage"],trace["Member"],trace["Input"]
        require(member in (1,4),"actual local derived owner")
        group=(member-1)//3;generation=int(stage.startswith(("final-seal","reopened-1","retire-1")))
        m,b=end["ColumnManifests"][generation][group],end["ColumnBindings"][generation][group]
        r=trace.get("Receipt");action=integer(q["Action"],1,6)
        if r is None:require(trace.get("Error"),"unknown derived response");continue
        p=b["Policy"]
        require(r["Binding"]==b and b["Version"]==2 and b["TerminalAttempts"]==4 and b["Coverage"]==1<<group and b["Layout"]==column.LAYOUT
                and array(b["Agreement"],32)==BASE and p["Version"]==1 and array(p["Admission"],32)==array(end["AdmissionSHA256"],32)
                and p["ChunkRecords"]==p["ChunkRows"]==16 and p["ChunkBytes"]==65536 and p["FrameBytes"]==32768
                and p["MaxChunks"]==2*frame_ceiling and p["MaxRetainedBytes"]==8*16384+2*frame_ceiling*(65536+128)
                and r["DeadlineUnixNanos"]==end["Admission"]["DeadlineUnixNanos"],"identical prepaid durable-column admission, no source TTL reset")
        require(array(m["Generation"]["Binding"],32)==digest(column.sealed_json(raw,b)),"immutable actual column binding")
        final=end["Final"] if generation else old_cut
        for g in range(2):
            boundary=m["Generation"]["Sources"][g]
            require(column.certificate(raw,boundary["Initial"])==decode_cut(end["Initial"])[g] and column.certificate(raw,boundary["Final"])==decode_cut(final)[g],"same exact generation prefix, no recreated current source")
        if action==1:
            require(owners[group] is None and r["Sequence"]==0 and not r["Closed"] and q["Initial"]==end["Initial"] and q["Final"]==final,"actual immutable constructor")
            owners[group]={"r":r,"sequence":0,"cursor":None}
        else:
            owner=owners[group];require(owner is not None and q["Cookie"]==r["Cookie"]==owner["r"]["Cookie"],"same real private query owner")
            for name,maximum in zip(column.WORK,("MaxWorkRecords","MaxWorkBytes","MaxDecodedRows","MaxOutputBytes","MaxRPCs","MaxWireBytes"),strict=True):
                require(integer(owner["r"]["Charged"][name])<=integer(r["Charged"][name])<=integer(p[maximum]),"cumulative actual source allowance cannot reset")
            if action not in (5,6):require(q["Sequence"]==r["Sequence"]==owner["sequence"]+1,"atomic private cursor/sequence");owner["sequence"]+=1
        owner=owners[group]
        previous=spool.find(b"durable-column-work",bytes([generation,group]))
        if previous is not None:require(all(integer(r["DurableWork"][n])>=decoded(previous)[n] for n in column.WORK),"reopen refunded durable generation work")
        put_work=encoded(r["DurableWork"])
        if previous is None:spool.insert(b"durable-column-work",bytes([generation,group]),put_work)
        else:spool.replace(b"durable-column-work",bytes([generation,group]),previous,put_work)
        require(r["RPCs"]<=r["Charged"]["RPCs"] and r["RequestBytes"]+r["ReplyBytes"]<=r["Charged"]["WireBytes"],"actual RPC/bytes, not nominal grant labels")
        if r["CacheLedger"]["Version"]:cache[group]=r["CacheLedger"]
        if action==2 and r["EOF"]:
            require(r["Manifest"]==m,"full source seal");codec.retention(r["Retention"],m,spool);sealed.add((generation,group))
        elif action==3:
            require(stage in expected and (stage,group) not in completed and r["Manifest"]==m and q["At"]==final,"exact cut projected read")
            owner["cursor"]={"ordinal":0,"chain":bytes(32),"position":decode_cut(end["Initial"])[group]["sourceIndex"],"frames":0,"rows":0,"bytes":0,"stage":stage,"cached":q["Cached"],"fill":q["Fill"]}
            if q["Cached"]:column.cache_manifest(raw,Path(end["Directory"]),group,m)
        elif action==4:
            c=owner["cursor"];require(c is not None and c["stage"]==stage and r["Ordinal"]==c["ordinal"] and r["Manifest"]==m and r["Cached"]==c["cached"] and r["Fill"]==c["fill"],"actual continuation/cached output")
            if r["EOF"]:
                require(not codec.optional(r["Body"]) and not r["Rows"] and c["ordinal"]==m["Chunks"] and c["chain"]==array(m["Chain"],32)
                        and c["position"]==m["Positions"][group] and c["frames"]==m["Frames"] and c["rows"]==m["Rows"] and c["bytes"]==m["Bytes"],"complete binary source/cache EOF")
                completed.add((stage,group));owner["cursor"]=None
            else:
                body=blob(r["Body"]);selected=[]
                for f in column.chunk(raw,body,m["Generation"],c["ordinal"],c["chain"],p):
                    require(f["Partition"]==3+5*group and f["Index"]==c["position"]+1,"binary frame gap")
                    actual=decoded(spool.lookup(b"frame",u64(f["Partition"])+u64(f["Index"])))
                    require(f["Root"]==array(actual["Root"],172) and all(f[n]==codec.optional(actual[n]) for n in ("Outcome","CDC","Registration","OriginalOutcome","OriginalCDC"))
                            and f["Rows"]==(actual["Rows"] or []),"binary rows equal independently authenticated native original/CDC prefix")
                    selected.extend(f["Rows"]);c["position"]+=1;c["frames"]+=1;c["rows"]+=len(f["Rows"])
                require(selected==(r["Rows"] or []),"actual projected typed output")
                column.cache_chunk(raw,Path(end["Directory"]),group,m,c["ordinal"],body,spool)
                key=b"generation/"+array(m["Generation"]["ID"],16).hex().encode()+b"/chunk/"+f'{c["ordinal"]:016x}'.encode()
                c["bytes"]+=len(key)+len(body);c["chain"]=digest(c["chain"]+digest(body));c["ordinal"]+=1
        elif action in (5,6):
            require(r["Closed"] and not r["EOF"] and owner["cursor"] is None,"local Close cannot invent EOF or release")
            if action==6:codec.retention(r["Retention"],m,spool,True);retired.add((generation,group))
            owners[group]=None
        if owners[group] is not None:owners[group]["r"]=r
    require(owners==[None,None] and completed=={(s,g) for s in expected for g in range(2)} and sealed==retired==probed=={(i,g) for i in range(2) for g in range(2)},"complete native/fill/cached/reopened/retired paths")
    for group in range(2):column.cache_terminal(raw,Path(end["Directory"]),group,cache[group],spool)


def processes(directory,end,spool):
    killed=restored=clean=0
    for r in lines(directory/"processes.jsonl",20):
        if r.get("Stage")=="pending-page-client-replacement":
            require(killed==0 and r["Member"]==2 and r["Signal"]=="SIGKILL" and r["Reaped"] is True and integer(r["PID"],1)!=integer(r["ReplacementPID"],1),"actual pending client kill/reap")
            before,after=codec.response(r["Before"]),codec.process_state(r["Restored"])
            require(before["incarnation"]!=after["incarnation"] and before["applied"]==after["applied"] and before["image"]==after["image"],"same-directory killed-client native prefix")
            killed+=1
        elif r.get("Stage")=="full-cohort-restore-before-election":
            before,after=codec.process_state(r["Before"]),codec.process_state(r["Restored"])
            require(r["Member"]==restored+1 and before["incarnation"]!=after["incarnation"] and before["applied"]==after["applied"] and before["image"]==after["image"]
                    and before["stats"][0]==after["stats"][0] and before["stats"][4]==after["stats"][4],"physical restore preserves bank/commit/full prefix before election")
            restored+=1
        else:
            cohort,member=divmod(clean,6);member+=1
            require(r["Cohort"]==cohort+1 and r["Member"]==member and r["Reaped"] is True and integer(r["PID"],1)
                    and r["WaitStatus"]==r["ExitCode"]==0 and Path(r["Volume"])==Path(end["Directory"])/str(member),"actual clean child exit")
            spool.insert(b"incarnation",array(r["Incarnation"],16),b"1");clean+=1
    require(killed==1 and restored==6 and clean==12 and end["Reaped"]==end["ProcessInstances"]==13,"no missing owned child cleanup")
    logical=allocated=0
    for member in range(1,7):
        for parent,dirs,files in os.walk(Path(end["Directory"])/str(member),followlinks=False):
            require(all(not (Path(parent)/n).is_symlink() for n in dirs),"foreign native directory link")
            for name in files:
                st=(Path(parent)/name).lstat();require(stat.S_ISREG(st.st_mode),"owned regular source/column/cache file")
                logical+=st.st_size;allocated+=st.st_blocks*512
    require((logical,allocated)==(end["PersistedFileBytes"],end["AllocatedFileBytes"]),"actual logical and allocated all-voter bytes")


def verify(directory,spool_directory,policy,*,binary_sha=None,expected_spec=None,expected_run=None,expected_deadline=None,expected_seconds=None,shared_spool=None):
    directory=Path(directory).resolve(strict=True)
    with (directory/"result.json").open("rb") as source:body=source.read(LINE+1)
    require(body.endswith(b"\n") and len(body)<=LINE,"bounded complete native terminal result")
    end=decoded(body[:-1]);s=shape(end["spec"]);a=end["Admission"]
    require(integer(end["version"],1,1) and end["format"]==FORMAT and end["engine"]=="native-raft" and end["RetentionContract"]=="snapshot-C-plus-feed-v1"
            and end["FramesCeiling"]==frames(s) and end["PageCeiling"]==32*s["facts"]+256,"exact source family/finite admission/retention profile")
    require(a["Version"]==1 and a["Family"]==FAMILY and a["Spec"]==s and digest(encoded(a))==array(end["AdmissionSHA256"],32)
            and integer(a["BudgetSeconds"],1,MAX_SECONDS)*1000>integer(a["CausalMinimumMillis"])
            and a["CausalMinimumMillis"]==5*a["Transport"]["targetRTTMillis"],"immutable actual graph/binary/run/budget/transport binding")
    require(type(a["Run"]) is str and len(a["Run"])==32 and bytes.fromhex(a["Run"]).hex()==a["Run"] and any(bytes.fromhex(a["Run"])),"nonzero run")
    for expected,actual in ((binary_sha,a["BinarySHA256"]),(expected_spec,s),(expected_run,a["Run"]),(expected_deadline,a["DeadlineUnixNanos"]),(expected_seconds,a["BudgetSeconds"])):require(expected is None or expected==actual,"controller/source admission mismatch")
    certs={k:cut(end[k]) for k in ("Initial","Snapshot","Final","Reopened")}
    require(certs["Final"]==certs["Reopened"],"protocol-only replay changed immutable graph certificate")
    for g in range(2):
        scope=certs["Initial"][g]["scope"]
        require(all(certs[k][g]["scope"]==scope and certs[k][g]["configuration"]==certs["Initial"][g]["configuration"] for k in certs)
                and array(a["Graph"],16)==scope[:16] and a["Topology"]==int.from_bytes(scope[48:56],"big") and array(a["Declaration"],32)==scope[56:88],"authentic graph/topology/configuration")
    require(type(end["Completed"]) is list and len(end["Completed"])==32 and all(type(v) is int and v>=0 for v in end["Completed"]),"strict actual caller counters")
    require(end["LogicalCross"]==2*(s["facts"]*s["crossPercent"]//100) and end["LogicalLocal"]+end["LogicalCross"]==2*s["facts"]+3,"true logical business participants, not physical protocol traffic")
    require(Path(end["ArtifactDirectory"]).resolve(strict=True)==directory and Path(end["Directory"]).resolve(strict=True).parent==directory,"owned artifact root")
    require(all(type(end[n]) is int and end[n]>=0 for n in ("SetupNanos","BusinessNanos","DiagnosticNanos","TotalNanos","Pages","BusinessPages","ProgressPages","OriginalCaptures","ExtraInvocations","FeedExtraInvocations"))
            and end["SetupNanos"]+end["BusinessNanos"]+end["DiagnosticNanos"]==end["TotalNanos"] and end["BusinessNanos"]>0,"business-only timing distinct diagnostics")
    # Reuse the scalar transport receipt grammar; its cap is overridden only by
    # an immutable source family shape, not nominal delay labels.
    selected=codec.owned_transport(end["Transport"],{"version":1,"family":codec.FAMILY,"facts":s["facts"],"crossPercent":s["crossPercent"],"writers":s["writers"],"layout":s["layout"]})
    require(selected==a["Transport"],"same actual two-cohort scheduler after restart")
    for cohort in end["Transport"]:
        require(all(cohort[name]["Admitted"]<=frames(s)*1024 for name in ("Raft","SourceRequest","SourceReply"))
                and all(cohort[name]["AttemptedFrames"]<=frames(s)*1024 for name in ("PipeReads","PipeWrites")),"exact feed family transport grant, not another family's upper bound")
    with (nullcontext(shared_spool) if shared_spool is not None else Spool(spool_directory,policy)) as spool:
        attempts(directory,end,spool);sources(directory,end,spool);points(directory,end,spool);feed_counts=feed(directory,end,spool);columns(directory,end,spool);processes(directory,end,spool)
        count=2*s["facts"]+3;ranks={"p50Nanos":(count*50+99)//100,"p95Nanos":(count*95+99)//100,"p99Nanos":(count*99+99)//100};latency={}
        for index,(key,_) in enumerate(spool.walk(b"latency"),1):
            for name,rank in ranks.items():
                if index==rank:latency[name]=int.from_bytes(key[:8],"big")
        require(len(latency)==3,"complete bounded on-disk logical-operation percentiles")
        feed_timing=[]
        for action,samples in sorted(feed_counts.items()):
            values={}
            for index,(key,_) in enumerate(spool.walk(b"feed-latency/"+action.encode()),1):
                for percentile in (50,95,99):
                    if index==(samples*percentile+99)//100:values[f"p{percentile}Nanos"]=int.from_bytes(key[:8],"big")
            feed_timing.append({"call":action,"samples":samples,**values})
        answer=hashlib.sha256()
        for value,stage in ((5,"old-after-update"),(6,"final")):
            for fact in range(s["facts"]):
                spool.lookup(b"point",stage.encode()+u64(fact));spool.lookup(b"feed-update",u64(fact))
                ordinal=4+fact+(s["facts"] if value==6 else 0)
                row=decoded(spool.lookup(b"row",u64(ordinal)+u64(7+2*fact)+b"\3"))
                coordinates=(fact,row["Owner"],row["Life"],row["Source"],row["Target"],row["SourceLife"],row["TargetLife"],row["After"]["Handle"])
                answer.update(b"edge-i64-lifebound-v1\0"+b"".join(u64(v) for v in coordinates)+struct.pack(">q",row["After"]["Value"])+u64(row["After"]["Provenance"]))
        return {"spec":s,"run":a["Run"],"logicalAnswersSHA256":answer.hexdigest(),"retentionContract":end["RetentionContract"],"transport":selected,
                "timing":{**latency,"logicalOperations":count,"usefulTransactionsPerSecond":count*1_000_000_000/end["BusinessNanos"],"feedCalls":feed_timing},"spool":spool.report(),
                "scope":"actual complete native snapshot-C/feed/original/voter/column/cache/reopen joins; source qualification, capacity, retention-equivalent GC and full V2 acceptance remain separate"}
