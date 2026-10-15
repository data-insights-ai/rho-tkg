#!/usr/bin/env python3
"""Private complete transfer-family verifier; scratch SQLite is only join state.

Stable source bucket IDs, current processing-owner epochs and physical FDB
replicas are separate. Snapshot-C retaining and later feed-only stages are
checked separately; this format grants no matched native retention verdict.
"""
import base64
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "fdb"))
import fdb_feed_check as feed
from comparison_spool import Policy, Spool, u64

raw, codec = feed.raw, feed.codec
require, fields, integer = feed.require, feed.fields, feed.integer
decoded, lines, blob, same, go_json = feed.decoded, feed.lines, feed.blob, feed.same, feed.go_json
key, prefix, raw_record = feed.key, feed.prefix, feed.raw_record
FAMILY, FORMAT = "transfer-retained-pins", "fdb-owner-transfer-workload-v1"
LINE = 512 << 10


def validate_grant(value, spec):
    """Same finite native admission; never silently enlarge a matrix profile."""
    fields(value, "MaxRecords MaxControlBytes MaxGrants MaxBlock Census")
    n = integer(spec["facts"], 20, 1_000_000)
    require(spec["family"] == FAMILY and spec["version"] == 2 and n > spec["writers"], "fresh transfer grant/dimensions")
    blocks = integer(value["MaxGrants"], 4, 64)
    block = integer(value["MaxBlock"], 8, 1 << 20)
    require(block % 2 == 0 and (2*n+6+block-1)//block+2 <= blocks, "actual HOME blocks, REMOTE and successor block admission")
    records = integer(value["MaxRecords"], 2*(3+2*n)+2*n+4*blocks+64, 1 << 32)
    total = integer(value["MaxControlBytes"], 512 << 10, 1 << 50)
    require(records <= total//128, "finite declared catalog byte grant")
    c = value["Census"]
    fields(c, "PageRows PageBytes HeadRows HeadBytes MaxRows MaxBytes MaxCalls MaxWorkRows MaxWorkBytes")
    page = integer(c["PageRows"], 1, 128)
    integer(c["PageBytes"], 64 << 10, 4 << 20)
    heads = integer(c["HeadRows"], 8, 512)
    integer(c["HeadBytes"], 4096, 48 << 10)
    rows = integer(c["MaxRows"], 33+21*n, 1 << 40)
    size = integer(c["MaxBytes"], 1 << 20, 1 << 55)
    minimum=33+21*n
    require(size>=minimum*(2*(len("graph-owned-v3-")+len(spec["layout"])+33)+6),"even useful native keys/values must fit declared census byte grant")
    calls = integer(c["MaxCalls"], 8, 1 << 32)
    work_rows = integer(c["MaxWorkRows"], rows, 1 << 48)
    work_bytes = integer(c["MaxWorkBytes"], size, 1 << 62)
    # Fund even the useful minimum's complete SDK delivery windows. This is
    # necessary admission, not a promise that ABORT/columns/control tails fit.
    pages = (33+21*n+page-1)//page+2
    window = (2*(16*(heads+1)+64)+page+12)*16
    require(calls >= pages and work_rows >= pages*window and work_bytes >= pages*window*(65536+1024), "explicit complete useful census callback/window allowance")
    return {**{k:value[k] for k in ("MaxRecords","MaxControlBytes","MaxGrants","MaxBlock")},
            "Census":{k:c[k] for k in "PageRows PageBytes HeadRows HeadBytes MaxRows MaxBytes MaxCalls MaxWorkRows MaxWorkBytes".split()}}


class Files:
    """Filename-only adapter to unchanged complete raw/column oracle APIs.

    No raw bytes, declared format, graph, cut or semantic result is relabelled.
    """
    def __init__(self, directory):
        self.directory = Path(directory)

    def __truediv__(self, name):
        if name in ("fdb-feed-history.jsonl", "fdb-feed-current.jsonl", "fdb-feed-cdc.jsonl", "fdb-feed-columns.jsonl"):
            name = name.replace("fdb-feed-", "fdb-transfer-", 1)
        return self.directory / name


def result(directory):
    body = raw.transport.admitted(Path(directory)/"fdb-transfer-result.json", LINE)
    end = decoded(body)
    fields(end, "Version Format Spec Config Binding Transport Initial Old Final Move Handoff Committed Aborted Attempts Issued Burned Clients Phases Sources AfterGC FeedOriginal FeedToken FeedPages FeedRows ReplayedPages Retired Processes EventSHA256 Storage"+(" Columns" if "Columns" in end else ""))
    require(end["Version"] == 1 and end["Format"] == FORMAT, "explicit transfer format; no old receipt promotion")
    p, c, b = end["Spec"], end["Config"], end["Binding"]
    policy = raw.transport.validate_policy(end["Transport"]["Policy"])
    raw.transport.family_record(end["Transport"], p, c)
    t = b["Transfer"]
    validate_grant({k:t[k] for k in ("MaxRecords","MaxControlBytes","MaxGrants","MaxBlock","Census")}, p)
    require(b["Version"] == t["Version"] == 2 and b["Config"] == t["Config"] == c and t["Admission"] == policy and t["Run"] == policy["Run"] and all(t[k] == policy[k] for k in ("BudgetSeconds","AdmittedUnixNanos","DeadlineUnixNanos")), "same persisted catalog/transport/absolute lifetime")
    require(b["Policy"] == {k:t[k] for k in ("MaxRecords","MaxControlBytes")} and all(t[k] == v for k,v in {"MaxProposalBytes":32768,"MaxAttempts":64,"MaxSubmissions":8,"MaxCallbacks":16,"CleanupMillis":30000,"MaxCleanupCalls":8}.items()), "finite original/recovery policy")
    n = p["facts"]
    originals = 3+2*n*64
    require(same(c, {"Format":2,"Graph":c["Graph"],"Topology":1,"Epochs":[7,11],"Limits":{"Bytes":(64<<20)+n*(64<<10)+originals*2048,"Transactions":originals,"Versions":3+6*n,"Certificates":1024},"Nodes":[1,7+2*n],"Cost":{"version":8,"facts":n,"crossPercent":p["crossPercent"],"writers":p["writers"]},"CostScope":codec.SCOPE}), "fresh Cost8 fixed scalar slice, source epochs unchanged")
    require(end["Committed"] == 3+2*n and 1 <= end["Aborted"] <= originals-end["Committed"] and end["Attempts"] == end["Committed"]+end["Aborted"] and end["Issued"] == 2*n+9 and end["Burned"] == 7, "exact useful/ABORT/actual issuance/burn census")
    require(len(end["Clients"]) == 32 and sum(end["Clients"]) == 2*n and all(type(v) is int and v >= 2 for v in end["Clients"][:p["writers"]]) and all(type(v) is int and v == 0 for v in end["Clients"][p["writers"]:]), "actual independent callers both phases")
    for name, groups in (("Initial",[0,0]),("Old",[2+n,1]),("Final",[2+2*n,1])):
        cut = end[name]
        fields(cut,"Graph Topology Epochs Scope Round Groups")
        require(cut["Graph"] == c["Graph"] and cut["Topology"] == 1 and cut["Epochs"] == c["Epochs"] and blob(cut["Scope"]) == b"\x00\x01" and cut["Groups"] == groups, "complete authentic source membership")
        integer(cut["Round"],1)
    require(end["Initial"]["Round"] < end["Old"]["Round"] < end["Final"]["Round"] and ("Columns" in end) == (p["layout"] == "columns"), "old5 after actual6; layout mandatory")
    return end


def intent(value, end):
    fields(value,"Body Digest Routes Allocated")
    body = blob(value["Body"])
    require(0 < len(body) <= 32768 and list(hashlib.sha256(body).digest()) == value["Digest"], "immutable native intent exact body/hash")
    tx = decoded(body)
    fields(tx,"Graph Topology ID Request Coordinator Dependency Participants GraphChange Lifecycle")
    require(tx["Graph"] == end["Config"]["Graph"] and tx["Topology"] == 1 and tx["Request"] == "request/"+tx["ID"] and tx["Dependency"] == 0, "native immutable transaction identity")
    require(value["Routes"] in ([{"Owner":"A","Epoch":1},{"Owner":"B","Epoch":1}], [{"Owner":"B","Epoch":2},{"Owner":"B","Epoch":1}]), "source bucket route versus processing epoch")
    return tx


def attempt(a, end, spool):
    fields(a,"Operation ClientID AttemptOrdinal PreviousRequest Intent Prepare Submissions Proof Classification Nanos")
    p,c=end["Spec"],end["Config"]
    ordinal,phase,fact=feed.operation(p,a["Operation"])
    client=integer(a["ClientID"],0,p["writers"]-1)
    number=integer(a["AttemptOrdinal"],1,64)
    tx=intent(a["Intent"],end)
    command,lifecycle,effects,reads=codec.fdb_contract(p,ordinal)
    require(tx["ID"] == f"transfer-{phase}-{fact}-c{client}-a{number}" and same(tx["GraphChange"],command) and same(tx["Lifecycle"],lifecycle) and [part["Group"] for part in tx["Participants"]] == sorted(effects), "actual factory operation/provenance/full business participants")
    for part in tx["Participants"]:
        g=integer(part["Group"],0,1)
        require(part["Epoch"] == c["Epochs"][g] and same(part["Effects"],effects[g]) and [r["Key"] for r in part["Reads"]] == reads[g], "immutable original source footprint")
    prior=spool.find(b"attempt-chain",u64(ordinal))
    previous=decoded(prior) if prior is not None else None
    require(previous is None and number==1 and a["PreviousRequest"]=="" or previous is not None and previous["Classification"]=="aborted" and previous["Proof"]["Outcome"]["Request"]==a["PreviousRequest"] and previous["AttemptOrdinal"]+1==number and previous["ClientID"]==client, "only authentic prior ABORT authorizes linked replan")
    proof=a["Proof"];fields(proof,"Pair Original Group Outcome")
    o=proof["Outcome"]
    fields(o,"Commit Round Digest ID Request Reason Coordinator Sequence")
    owner=1 if phase=="setup" and fact==2 else 0
    require(o["Digest"]==a["Intent"]["Digest"] and o["ID"]==tx["ID"] and o["Request"]==tx["Request"] and o["Coordinator"]==tx["Coordinator"]==owner and type(o["Commit"]) is bool, "actual original decision binds SAME digest/request/body")
    require(len(proof["Pair"])==2 and 1<=len(a["Submissions"])<=8, "paired originals/finite same-wire submissions")
    for record,kind,identity in zip(proof["Pair"],("id","request"),(o["ID"],o["Request"]),strict=True):
        native=key(c,owner,kind,identity)
        raw_record(record,native,go_json(o))
        spool.insert(b"attempt-"+kind.encode(),native,go_json(o))
        spool.insert(b"attempt-membership-"+kind.encode(),native,u64(ordinal))
    require(a["Prepare"]["Intent"]==a["Intent"] and a["Prepare"]["Error"]=="", "actual retained Prepare observation")
    for submission in a["Submissions"]:
        if submission["Error"]:
            require(submission["Refusal"]=="reply-unavailable" or submission["Retryable"] or submission["MaybeCommitted"] or submission["Refusal"] in ("owner-busy","limit"), "unknown/domain refusal is not ABORT authority")
        else:
            require(submission["Proof"]==proof and submission["Outcome"]==o, "actual successful same-original result")
    if o["Commit"]:
        require(a["Classification"]=="committed" and o["Reason"]=="" and o["Round"]>0 and o["Sequence"]>0 and proof["Group"]["Tx"]==tx and proof["Group"]["Outcome"]==o, "genuine useful COMMIT")
        body=go_json(proof["Group"])
        raw_record(proof["Original"],prefix(c,owner)+f'changes/{o["Sequence"]:016x}/'.encode()+o["Request"].encode().hex().encode(),body)
        spool.insert(b"fdb-operation",u64(ordinal),body)
        spool.insert(b"useful-attempt",u64(ordinal),go_json(a))
    else:
        require(a["Classification"]=="aborted" and o["Reason"] in ("generation","revision","owner-epoch") and o["Round"]==o["Sequence"]==0 and proof["Original"]=={"Key":None,"Value":None}, "authentic retained ABORT emits no CDC")
        if o["Reason"]=="owner-epoch":
            require(phase=="update" and fact==1 and number==1 and a["Intent"]["Routes"][0]=={"Owner":"A","Epoch":1}, "finite prior frozen fenced request")
    integer(a["Nanos"],1,(1<<63)-1)
    if prior is None:spool.insert(b"attempt-chain",u64(ordinal),go_json(a))
    else:spool.replace(b"attempt-chain",u64(ordinal),prior,go_json(a))
    return ordinal,o


def event_limit(end):
    return 128*end["Config"]["Limits"]["Transactions"]+2*end["Binding"]["Transfer"]["Census"]["MaxRows"]+10000


def first_pass(directory, end, spool):
    counts,clients=[0,0,0],[0]*32
    costs={"old":[[0,0],[0,0]],"final":[[0,0],[0,0]]}
    digest,footer=hashlib.sha256(),False
    pids,maximum,seen={}, {}, {}
    lost=[];refusals=set()
    for frame,line in lines(Path(directory)/"fdb-transfer-events.jsonl",event_limit(end)):
        require(not footer,"event after EOF")
        kind=frame.get("Type")
        if kind=="footer":
            fields(frame,"Type End")
            require(frame["End"]==end and digest.hexdigest()==end["EventSHA256"],"complete event EOF/hash/result")
            footer=True;continue
        digest.update(line)
        if kind=="header":
            fields(frame,"Type Binding Transport Initial")
            require(frame=={"Type":"header","Binding":end["Binding"],"Transport":end["Transport"],"Initial":end["Initial"]} and counts==[0,0,0] and not pids,"one exact native header")
            spool.insert(b"header",b"one",line)
        elif kind=="prepared":
            a=frame["Attempt"];tx=intent(a["Intent"],end)
            require(a["Submissions"] is None and a["Classification"]=="" and a["Nanos"]==0,"pre-submit durable original observation")
            spool.insert(b"prepared",tx["Request"].encode(),go_json(a["Intent"]))
        elif kind=="submit-issued":
            fields(frame,"Type Owner Call")
            call=frame["Call"]
            require(frame["Owner"] in ("A","B") and call["Method"]=="hold-submit" and call["ID"]>0,"actual issued/unobserved call, not a native decision")
            tx=intent(call["Intent"],end)
            require(go_json(call["Intent"])==spool.lookup(b"prepared",tx["Request"].encode()),"issued SAME immutable original")
            spool.insert(b"submit-issued",frame["Owner"].encode()+u64(call["ID"]),go_json(call))
        elif kind=="attempt":
            ordinal,o=attempt(frame["Attempt"],end,spool)
            a=frame["Attempt"]
            require(spool.lookup(b"prepared",o["Request"].encode())==go_json(a["Intent"]),"actual prepared body retained before any submission")
            counts[0]+=1;counts[1 if o["Commit"] else 2]+=1
            if o["Commit"] and ordinal>3:clients[a["ClientID"]]+=1
            size=sum(len(blob(r["Key"]))+len(blob(r["Value"])) for r in a["Proof"]["Pair"])
            for phase in ("old","final") if ordinal<=3+end["Spec"]["facts"] else ("final",):
                costs[phase][0][o["Coordinator"]]+=1;costs[phase][1][o["Coordinator"]]+=size
        elif kind=="rpc":
            fields(frame,"Type Owner Call Reply")
            owner,call,r=frame["Owner"],frame["Call"],frame["Reply"]
            require(owner in ("A","B") and call["ID"]==r["ID"] and call["Client"]==r["Client"],"actual admitted mux frame/paired response, not reconstructed ID")
            pid=integer(r["PID"],1,(1<<31)-1)
            if call["Method"]=="describe":
                require(pid not in pids.values() and (owner not in pids or any(x["Role"]==owner for x in lost)),"genuine new process incarnation after actual reap")
                pids[owner]=pid
            require(pids.get(owner)==pid and r["Nanos"]>0,"same actual current SDK process")
            identity=u64(pid)+u64(integer(call["ID"],1))
            spool.insert(b"rpc",identity,go_json(frame))
            maximum[pid]=max(maximum.get(pid,0),call["ID"]);seen[pid]=seen.get(pid,0)+1
            integer(call["Client"],0,end["Spec"]["writers"]-1)
            if "NativeCode" in r:
                integer(r["NativeCode"],1,(1<<31)-1)
                require(type(r["Retryable"]) is type(r["MaybeCommitted"]) is bool,"actual SDK predicates")
            else:require(not r["Retryable"] and not r["MaybeCommitted"],"domain refusal cannot invent SDK predicates")
            if r["Refusal"] in ("stale","mismatch","allocation-consumed"):
                tag=(owner,call["Method"],r["Refusal"])
                require(tag not in refusals and "NativeCode" not in r and r["Error"],"actual distinct stale/changed/session refusal")
                refusals.add(tag)
            if "Intent" in call:
                tx=intent(call["Intent"],end)
                # Changed-body refusal is deliberately not registered as a new
                # immutable attempt. All successful/replayed bodies stay exact.
                if not r["Error"]:require(spool.lookup(b"prepared",tx["Request"].encode())==go_json(call["Intent"]),"SAME request/body RPC")
            if "Intent" in r and not r["Error"]:
                tx=intent(r["Intent"],end)
                spool.insert(b"native-prepare",tx["Request"].encode(),go_json(r["Intent"]))
            if "Candidate" in r:
                captured=intent(r["Candidate"],end)
                require(call["Method"]=="prepare" and captured["ID"]==call["Prepare"]["Name"] and captured["GraphChange"]==call["Prepare"]["Command"] and captured["Lifecycle"]==call["Prepare"]["Metadata"] and r["Candidate"]["Allocated"]==call["Prepare"]["Claims"],"actual captured SDK submission input retained even on unknown/refusal; no durable authority inferred")
                if not r["Error"]:require(r["Candidate"]==r["Intent"],"successful native Prepare authenticates exact submitted candidate")
            if call["Method"] in ("next-node","next-value","next-edge") and not r["Error"]:
                for claim in r["Allocated"]:
                    spool.insert(b"actual-next",u64(integer(claim["ID"],1)),go_json(claim))
        elif kind=="committed-reply-lost":
            fields(frame,"Type Issued Observation Missing Process")
            call,r,missing,process=(frame[k] for k in ("Issued","Observation","Missing","Process"))
            require(process["Role"] in ("A","B") and process["PID"]==r["PID"]==pids[process["Role"]] and process["Signal"]=="SIGKILL" and process["Exit"]==-1 and process["Reaped"] is True and call["ID"]==r["ID"]==missing["ID"] and call["Client"]==r["Client"]==missing["Client"] and r["Error"]=="" and r["Outcome"]["Commit"] is True and missing["Refusal"]=="reply-unavailable" and "NativeCode" not in missing and "Outcome" not in missing,"actual post-native original observation, missing correlated reply, SIGKILL and observed reap")
            native=intent(call["Intent"],end)
            require(spool.lookup(b"submit-issued",process["Role"].encode()+u64(call["ID"]))==go_json(call),"actual pre-kill admitted mux request identity")
            require(go_json(r["Proof"]["Group"]["Tx"])==blob(call["Intent"]["Body"]) and r["Proof"]["Outcome"]==r["Outcome"] and native["Lifecycle"]["Provenance"]==4+end["Spec"]["facts"],"SAME genuine K across source and target losses")
            marker=Path(directory)/f'fdb-transfer-lost-{process["Role"]}-{call["ID"]:016x}.json'
            require(decoded(raw.transport.admitted(marker,LINE))==r,"actual closed exclusive child marker")
            spool.insert(b"rpc",u64(r["PID"])+u64(r["ID"]),go_json(frame))
            maximum[r["PID"]]=max(maximum.get(r["PID"],0),r["ID"]);seen[r["PID"]]=seen.get(r["PID"],0)+1
            lost.append(process)
    require(footer and counts==[end["Attempts"],end["Committed"],end["Aborted"]] and clients==end["Clients"] and len(lost)==2 and [p["Role"] for p in lost]==["A","B"],"complete useful/ABORT/client/lost-reply inventory")
    require(refusals=={("A","reserve","stale"),("A","stale-submit","stale"),("A","prepare","stale"),("A","install","stale"),("B","install","allocation-consumed"),("B","submit","mismatch")},"all actual old-owner/factory/installed-session/changed-body refusal doors")
    require(len(maximum)==4 and all(maximum[pid]==seen[pid] for pid in maximum),"consecutive genuine mux input IDs through both replacements")
    require(end["Processes"][:2]==lost and len(end["Processes"])==4 and all(p["Reaped"] and p["PID"] in maximum for p in end["Processes"]) and len({p["PID"] for p in end["Processes"]})==4 and all(p["Signal"]=="" and p["Exit"]==0 for p in end["Processes"][2:]),"four distinct SDK processes genuinely reaped")
    for ordinal in range(1,4+2*end["Spec"]["facts"]):
        require(decoded(spool.lookup(b"attempt-chain",u64(ordinal)))["Classification"]=="committed","exactly one useful COMMIT per logical operation")
    for phase,cost in costs.items():spool.insert(b"original-cost",phase.encode(),go_json(cost))
    for request,body in spool.walk(b"prepared"):
        tx=decoded(blob(decoded(body)["Body"]))
        require(request==tx["Request"].encode() and decoded(spool.lookup(b"attempt-request",key(end["Config"],tx["Coordinator"],"request",tx["Request"])))["Digest"]==decoded(body)["Digest"],"all admitted native intents have actual final COMMIT/ABORT; no missing/unknown attempt EOF")
    for owner in ("A","B"):
        child=decoded(raw.transport.admitted(Path(directory)/f"fdb-transfer-{owner}-input.json",LINE))
        fields(child,"Binding Authority Role DeadlineUnixNanos")
        require(child["Binding"]==end["Binding"] and child["Role"]==child["Authority"]["ID"]==owner and child["DeadlineUnixNanos"]==end["Binding"]["Transfer"]["DeadlineUnixNanos"],"original actual child input/lifetime unchanged on replacement")
        principal=next(p for p in end["Binding"]["Authorities"] if p["ID"]==owner)
        require(list(hashlib.sha256(bytes(child["Authority"]["Secret"])).digest())==principal["SecretHash"],"actual registered source/target principal")


def originals(directory,end,spool):
    state={"done":0,"stage":None}
    for frame,_ in lines(Path(directory)/"fdb-transfer-events.jsonl",event_limit(end)):
        if frame["Type"] in ("originals-header","original","originals-eof"):
            feed.original_inventory(frame,end,spool,state)
    require(state["done"]==2 and state["stage"] is None,"both actual complete ID AND request native barrier inventories")


def chain(previous,k,v):
    return hashlib.sha256(previous+u64(len(k))+k+u64(len(v))+v).digest()


def census_class(c,k):
    g=0 if k.startswith(prefix(c,0)) else 1
    require(k.startswith(prefix(c,g)),"foreign raw census namespace")
    suffix=k[len(prefix(c,g)):]
    kind,sep,tail=suffix.partition(b"/")
    require(sep and tail,"canonical census key")
    if kind in (b"current",b"history",b"changes",b"id",b"request",b"cut"):
        return kind==b"current"
    require(g==0 or kind==b"meta","foreign group control")
    if kind==b"meta":require(bytes.fromhex(tail.decode()) in (b"state",b"config"),"unknown native metadata");return True
    if kind==b"source-owner-v1":return True
    if kind in (b"source-retention-v1",b"workload-transport-v2",b"owner-census-v1",b"owner-catalog-v1"):
        name=bytes.fromhex(tail.decode()).decode()
        if kind==b"source-retention-v1":
            require(name in ("binding","ledger","state") or name.startswith(("intent/","seal/")),"unknown source control")
            return name in ("binding","ledger","state")
        if kind==b"workload-transport-v2":require(name=="binding","unknown transport control");return True
        if kind==b"owner-census-v1":require(name.rsplit("/",1)[-1] in ("original","progress","seal"),"unknown census control");return False
        which,sep,name=name.partition("/")
        require(sep and which in ("binding","state","ledger","allocation-state","move","move-progress","cleanup","intent-request","intent-id","allocation-grant","allocation-index","allocation-slot","allocation-issued"),"unknown catalog mutable class")
        if which in ("binding","state","ledger","allocation-state"):
            require(name=={"binding":"immutable","state":"current","ledger":"controls","allocation-state":"current"}[which],"unknown fixed catalog head")
        return which in ("binding","state","ledger","allocation-state","move-progress","cleanup","allocation-slot")
    if kind==b"feed-v1":
        which,sep,tail=tail.partition(b"/")
        require(sep and which in (b"meta",b"original",b"cursor",b"reader",b"cleanup",b"retained-phase",b"retained-reader",b"action",b"page",b"ack"),"unknown feed class")
        if which==b"meta":require(bytes.fromhex(tail.decode()) in (b"binding",b"ledger"),"unknown feed metadata head")
        return which in (b"meta",b"cursor",b"cleanup",b"retained-phase",b"retained-reader")
    require(kind==b"columns-v1","unknown raw census class")
    parts=tail.split(b"/")
    require(parts[0] in (b"binding",b"ledger",b"read-owner",b"retention-v1",b"generation"),"unknown columns class")
    if parts[0]==b"generation":
        require(len(parts)>=3 and parts[2] in (b"progress",b"owners",b"manifest",b"chunk"),"unknown generation class")
        return parts[2] in (b"progress",b"owners")
    if parts[0]==b"retention-v1":
        require(len(parts)>=2 and parts[1] in (b"binding",b"ledger",b"pin",b"reference",b"reference-request"),"unknown retention class")
        return parts[1]!=b"reference-request"
    return True


def census_events(frame,end,spool,states):
    kind=frame["Type"];c=end["Config"]
    if kind=="census-original":
        o=frame["Original"];request=o["Request"]
        require(request in ("handoff","terminal-storage") and request not in states and o["Binding"]==list(hashlib.sha256(go_json(end["Binding"])).digest()),"actual census original/native binding")
        heads=o["Heads"];policy=end["Binding"]["Transfer"]["Census"]
        require(len(heads)<=policy["HeadRows"] and len(go_json(heads))<=policy["HeadBytes"],"finite encoded native mutable heads")
        keys=[blob(r["Key"]) for r in heads]
        require(keys==sorted(set(keys)),"canonical unique mutable inventory")
        states[request]={"original":o,"ordinal":1,"position":{"Group":0,"Cursor":None},"hash":bytes(32),"rows":0,"bytes":0,"page":[],"previous":None,"done":False,"heads":{blob(r["Key"]):blob(r["Value"]) for r in heads},"found":set()}
    elif kind=="census-row":
        request=frame["Request"];s=states[request]
        require(not s["done"] and frame["Ordinal"]==s["ordinal"] and len(s["page"])<end["Binding"]["Transfer"]["Census"]["PageRows"],"bounded page rows in exact invocation")
        record=frame["Record"];fields(record,"Key Value")
        k,v=blob(record["Key"]),blob(record["Value"])
        require(0<len(k)<=1024 and 0<len(v)<=65536 and (s["previous"] is None or s["previous"]<k),"complete ordered native census, no duplicate")
        mutable=census_class(c,k)
        # Current graph rows stream independently and are not mutable CONTROL
        # heads; all remaining mutable classes must be in the frozen inventory.
        if mutable and b"/current/" not in k:
            require(s["heads"].get(k)==v,"every mutable class bound to same frozen native inventory")
            s["found"].add(k)
        if k in s["heads"]:require(s["heads"][k]==v,"exact frozen head observation");s["found"].add(k)
        spool.insert(b"census-"+request.encode(),k,v)
        s["previous"]=k;s["page"].append((k,v))
    else:
        request=frame["Request"];s=states[request];page=frame["Page"]
        total=sum(len(k)+len(v) for k,v in s["page"])
        digest=bytes(32)
        for k,v in s["page"]:digest=chain(digest,k,v);s["hash"]=chain(s["hash"],k,v)
        require(frame["Replay"] is False and page["Request"]==f'page-{s["ordinal"]:016x}' and page["Ordinal"]==s["ordinal"] and page["From"]==s["position"] and page["Rows"]==len(s["page"]) and page["Bytes"]==total and page["Hash"]==list(digest) and type(page["EOF"]) is bool and page["EOF"]==(page["Next"]["Group"]==2),"exact raw page/original/position/hash/EOF")
        if page["Next"]["Group"]<2:
            own={key(c,0,"owner-census-v1",request+"/"+name) for name in ("original","progress","seal")}
            cursor=blob(page["Next"]["Cursor"])
            require(s["page"] and (cursor==s["page"][-1][0] or cursor in own and cursor>=s["page"][-1][0]) and cursor.startswith(prefix(c,page["Next"]["Group"])),"native page cursor exact last key or explicit excluded own control")
        else:require(page["Next"]["Cursor"] is None,"complete both-prefix EOF cursor")
        s["rows"]+=page["Rows"];s["bytes"]+=total;s["position"]=page["Next"];s["ordinal"]+=1;s["page"]=[];s["done"]=page["EOF"]
        if s["done"]:require(s["found"]==set(s["heads"]),"missing mutable head class despite equal raw count")


def census_observation(observation,s,end,spool,sealed):
    require(s["done"] and observation["Original"]==s["original"],"same complete census original, not caller count/hash")
    o,p=observation["Original"],observation["Progress"]
    require(p["Original"]==list(hashlib.sha256(go_json(o)).digest()) and p["EOF"] is True and p["Pending"]=="" and p["Position"]==s["position"] and p["Rows"]==s["rows"] and p["Bytes"]==s["bytes"] and p["Hash"]==list(s["hash"]) and p["Pages"]==s["ordinal"]-1,"actual stored progress agrees with every raw native page")
    policy=end["Binding"]["Transfer"]["Census"]
    window=(2*(16*(policy["HeadRows"]+1)+64)+policy["PageRows"]+12)*16
    require(p["Pages"]<=p["Calls"]<=policy["MaxCalls"] and p["WorkRows"]==p["Calls"]*window and p["WorkBytes"]==p["WorkRows"]*(65536+1024) and p["WorkRows"]<=policy["MaxWorkRows"] and p["WorkBytes"]<=policy["MaxWorkBytes"],"all native delivered head/page windows charged, no phase reset")
    controls=observation["Controls"]
    require(len(controls)==3,"exact original/progress/seal native records")
    for record,name,value in zip(controls,("original","progress","seal"),(o,p,observation["Seal"]),strict=True):
        # Storage-only capture intentionally does not decode/report the pending
        # seal as an activated certificate. Its native raw bytes stay explicit.
        expected=go_json(value) if sealed or name!="seal" else blob(record["Value"])
        raw_record(record,key(end["Config"],0,"owner-census-v1",o["Request"]+"/"+name),expected)
        if not sealed and name=="seal":
            empty_state={"Version":0,"Routes":[{"Owner":"","Epoch":0},{"Owner":"","Epoch":0}],"Moving":""}
            empty_cut={"Graph":"","Topology":0,"Epochs":[0,0],"Scope":None,"Round":0,"Groups":[0,0]}
            require(decoded(expected)=={"Original":p["Original"],"Before":empty_state,"After":empty_state,"Cut":empty_cut,"Rows":0,"Bytes":0,"Hash":[0]*32,"Heads":[0]*32,"Complete":False},"storage-only pending seal is not a fabricated activation")
    if sealed:
        seal=observation["Seal"]
        after={"Version":2,"Routes":[{"Owner":"B","Epoch":2},{"Owner":"B","Epoch":1}],"Moving":""}
        require(seal=={"Original":p["Original"],"Before":o["Move"]["Before"],"After":after,"Cut":o["Frozen"]["Cut"],"Rows":p["Rows"],"Bytes":p["Bytes"],"Hash":p["Hash"],"Heads":list(hashlib.sha256(go_json(o["Heads"])).digest()),"Complete":True},"same-native-transaction complete census/activation certificate")
        require(observation["AllRows"]==p["Rows"]+3 and observation["AllBytes"]==p["Bytes"]+sum(len(blob(r["Key"]))+len(blob(r["Value"])) for r in controls),"all retained census coordination bytes counted")
    return o,p


def typed_read_policy(end):
    n=end["Spec"]["facts"]
    # Literal existing native selector policy, independently authenticated by
    # feed original and source binding. No synthetic selector observation.
    return {"PageRows":8,"PageBytes":4<<20,"InventoryBytes":4<<20,"WorkRows":1024*n+4096,"WorkBytes":(64<<20)+n*(2<<20),"ChainSteps":128,"Cleanup":{"Rows":1024,"Bytes":64<<20,"Millis":30000}}


def feed_identity(end):
    o,token=end["FeedOriginal"],end["FeedToken"]
    fields(o,"Version Request Secret Policy Selection Cut Pin Snapshot Opening DeadlineNanos Catalog")
    fields(token,"Request Secret Original DeadlineNanos")
    require(o["Version"]==3 and o["Request"]==token["Request"]=="transfer" and o["Secret"]==token["Secret"] and any(o["Secret"]) and token["Original"]==list(hashlib.sha256(go_json(o)).digest()) and o["Catalog"]==list(hashlib.sha256(go_json(end["Binding"])).digest()) and o["DeadlineNanos"]==token["DeadlineNanos"]==end["Binding"]["Transfer"]["DeadlineUnixNanos"] and o["Cut"]==end["Old"],"genuine feed3 reserved factory original/catalog/absolute lifetime")
    proxy=end|{"Binding":{"Read":typed_read_policy(end)}}
    require(o["Policy"]==feed.feed_policy(proxy) and all(v==0 for v in o["Opening"].values()),"actual unchanged finite feed/read work policy")
    zero={"Graph":"","Topology":0,"Epochs":[0,0],"Scope":None,"Round":0,"Groups":[0,0]}
    require(o["Selection"]=={"Mode":"Fresh","Scope":"AAE=","At":zero},"genuine snapshot selection, no caller-provided cut relabel")
    pin,reader=o["Pin"],o["Snapshot"]
    for value,kind in ((pin,"pin"),(reader,"read")):
        require(value["Kind"]==kind and value["Cut"]==end["Old"] and value["Required"]==end["Initial"] and value["State"]==1 and value["TerminalRequest"]=="" and not any(value["Lineage"]),"original held-owner complete physical-prefix requirement")
    require(pin["Request"]=="feed-transfer" and pin["Secret"]==list(hashlib.sha256(b"rho-fdb-feed-v1/pin"+bytes(o["Secret"])).digest()) and pin["PinRequest"]=="" and not any(pin["PinDigest"]),"factory pin identity")
    pin_token={"Request":pin["Request"],"Cut":pin["Cut"],"Secret":pin["Secret"]}
    require(reader["PinRequest"]==pin["Request"] and reader["PinDigest"]==list(hashlib.sha256(go_json(pin_token)).digest()) and reader["Request"]=="source-read-"+bytes(reader["Secret"])[:16].hex(),"actual held snapshot pin binding")


def retained_page(frame,end,spool,position,last):
    delivery,ack=frame["Delivery"],frame["Ack"]
    page=delivery["Page"];token=end["FeedToken"];c=end["Config"]
    fields(page,"Version Request Original From Next Boundary Last Rows EOF")
    require(page["Version"]==1 and page["Original"]==token["Original"] and page["Boundary"]==end["Final"] and page["From"]==position and len(page["Rows"])<=8 and len(go_json(page))<=65536 and delivery["Replay"] is False,"actual bounded pending-page original/boundary")
    raw_record(delivery["Record"],feed.feed_key(c,"page/transfer",page["Request"]),go_json(page))
    next_=list(position)
    for row in page["Rows"]:
        group=decoded(blob(row["Value"]));o=group["Outcome"];g=row["Group"]
        require(o["Commit"] is True and o["Coordinator"]==g and o["Sequence"]==next_[g]+1 and end["Old"]["Round"]<o["Round"]<=end["Final"]["Round"] and blob(row["Value"])==spool.lookup(b"native-cdc",blob(row["Key"])),"complete genuine native CDC group, no gap/foreign/missing original")
        order=(o["Round"],g,o["Request"])
        require(last is None or last<order,"deterministic enumeration is not provenance order")
        for other in range(2):
            if next_[other]<end["Final"]["Groups"][other]:
                head=decoded(spool.lookup(b"feed-sequence",bytes([other])+u64(next_[other]+1)))["Outcome"]
                require(order<=(head["Round"],other,head["Request"]),"bounded two-head merge cannot hide earlier group")
        for field,ns in (("ID",b"attempt-id"),("Request",b"attempt-request")):
            require(blob(row[field+"Key"])==key(c,g,field.lower(),o[field]) and blob(row[field+"Value"])==go_json(o)==spool.lookup(ns,blob(row[field+"Key"])),"every feed group paired originals")
        for write in group["Writes"]:
            native=key(c,write["Group"],"current",write["Key"])
            before=spool.lookup(b"feed-replica",native)
            require(decoded(before)==write["Before"],"snapshot plus feed exact predecessor, no invented normalized state")
            spool.replace(b"feed-replica",native,before,go_json(write["After"]))
        spool.insert(b"feed-consumed",blob(row["Key"]),blob(row["Value"]))
        last=order;next_[g]+=1
    require(page["Next"]==next_ and page["EOF"]==(next_==end["Final"]["Groups"]) and (page["Rows"] or page["EOF"]),"exact complete feed vectors/EOF")
    if page["Rows"]:require(page["Last"]==decoded(blob(page["Rows"][-1]["Value"]))["Outcome"],"page last original authority")
    a=ack["Ack"]
    require(a=={"Request":page["Request"],"Page":list(hashlib.sha256(go_json(page)).digest()),"From":position,"Next":next_,"Completed":end["Final"] if page["EOF"] else end["Old"]},"ACK exact original hash/complete boundary only")
    raw_record(ack["Record"],feed.feed_key(c,"ack/transfer",a["Request"]),go_json(a))
    cursor=ack["Cursor"]
    proxy=end|{"Binding":{"Read":typed_read_policy(end)},"Feed":{"Original":end["FeedOriginal"],"Token":token}}
    feed.feed_cursor(cursor,proxy)
    require(cursor["Positions"]==next_ and cursor["Completed"]==a["Completed"] and cursor["Pending"]=="","real native ACK cursor persisted before return")
    spool.insert(b"retained-page",page["Request"].encode(),go_json(page))
    return next_,last,len(page["Rows"])


def lifecycle(directory,end,spool):
    c=end["Config"];n=end["Spec"]["facts"]
    states={};mandatory=set();snapshot_counts={};snapshot_eofs=set();point_sets=set()
    position=list(end["Old"]["Groups"]);last=None;pages=rows=0;replayed=False;gc_done=False
    deleted=[0,0];deleted_bytes=[0,0]
    proxy=end|{"Binding":{"Read":typed_read_policy(end)}}
    def mark(name):
        require(name not in mandatory,"duplicate fixed lifecycle/control event")
        mandatory.update((name,))
    allowed={"header","prepared","attempt","rpc","submit-issued","committed-reply-lost","allocation-reservation-issued","allocation-session-issued","originals-header","original","originals-eof","owner-reaped","footer"}
    for frame,_ in lines(Path(directory)/"fdb-transfer-events.jsonl",event_limit(end)):
        kind=frame["Type"]
        if kind in allowed:continue
        if kind=="handoff-api":
            require(frame["Name"] in ("reserve","freeze","activate") and "reported separately" in frame["Access"],"actual catalog API timing distinct from census")
            integer(frame["Nanos"],1);mark(kind+"/"+frame["Name"]);continue
        if kind=="census-eof":
            require(states[frame["Request"]]["done"] and "chosen complete native diagnostic/certification" in frame["Access"],"census certification overhead not inherent FDB transfer")
            integer(frame["Nanos"],1);mark(kind+"/"+frame["Request"]);continue
        if kind in ("census-original","census-row","census-page"):
            census_events(frame,end,spool,states);continue
        if kind=="snapshot-row":
            name=frame["Name"]
            require(name in ("held-old-after-target-restart","current-before-gc") and name not in snapshot_eofs,"exact retained snapshot phase")
            feed.snapshot_row(frame["Row"],end["Old" if name.startswith("held") else "Final"],proxy,spool,name)
            snapshot_counts[name]=snapshot_counts.get(name,0)+1
        elif kind=="snapshot-eof":
            name=frame["Name"];cut=end["Old" if name.startswith("held") else "Final"]
            require(name in snapshot_counts and name not in snapshot_eofs and snapshot_counts[name]==3+3*n and frame["End"]["Cut"]==cut and sum(frame["End"]["Rows"])==3+3*n and frame["Selection"]["Selected"]==cut,"complete native snapshot exact set/EOF")
            require(frame["End"]["DirectoryRows"]==frame["End"]["Rows"] and frame["End"]["OriginalEffects"]==decoded(spool.lookup(b"source-count",b"old" if name.startswith("held") else b"final"))[1],"independent complete source directory AND original-effect census")
            for native,value in spool.walk(b"fdb-old" if name.startswith("held") else b"fdb-current"):
                require(spool.lookup(b"snapshot-seen-"+name.encode(),native)==value,"missing retained logical key despite equal row count")
            owner=decoded(blob(frame["Selection"]["AcquiredOwner"]["Value"]))
            require(owner["Cut"]==cut and owner["Required"]==end["Initial"] and owner["State"]==1 and owner["Kind"]=="read","real retained owner through source/index walks")
            raw_record(frame["Selection"]["AcquiredOwner"],key(c,0,"source-owner-v1",owner["Request"]),go_json(owner))
            if name.startswith("held"):require(owner==end["FeedOriginal"]["Snapshot"],"SAME original snapshot remains held through real target replacement")
            feed.snapshot_work(frame["Work"],proxy,name.startswith("held"))
            integer(frame["Nanos"],1,(1<<63)-1);snapshot_eofs.add(name)
        elif kind=="points":
            name=frame["Name"]
            require(name in snapshot_eofs and name not in point_sets and "scan-based" in frame["Access"] and len(frame["Points"])==3,"one complete scan-based fixed probe call")
            ns=b"fdb-old" if name.startswith("held") else b"fdb-current"
            for i,point in zip((0,n-1,n+16),frame["Points"],strict=True):
                logical=codec.fdb_contract(end["Spec"],4+i if i<n else 4)[0]["Edge"] if i<n else None
                if logical is None:
                    # Fixed phantom shares actual endpoint shape; its entity ID
                    # lies beyond all declared useful fact IDs.
                    actual=codec.fdb_contract(end["Spec"],4)[0]["Edge"]
                    logical={**actual,"ID":7+2*i}
                    remote=(i+1)*end["Spec"]["crossPercent"]//100>i*end["Spec"]["crossPercent"]//100
                    logical["Target"]={"Owner":1,"Node":2*n+7,"Life":2*n+8} if remote else {"Owner":0,"Node":3,"Life":4}
                suffix="/".join(f"{v:016x}" for v in (logical["ID"],logical["Source"]["Node"],logical["Source"]["Life"],logical["Target"]["Node"],logical["Target"]["Life"]))
                wanted="edge/"+suffix
                require(point["Key"]=={"Group":0,"Key":wanted} and point["Found"]==(i<n),"exact first/last/phantom key set")
                if i<n:require(go_json(point["Value"])==spool.lookup(ns,key(c,0,"current",wanted)),"point exact native retained revision")
            integer(frame["Nanos"],1,(1<<63)-1);point_sets.add(name)
        elif kind=="feed-begin":
            fields(frame,"Type Original Nanos");require(frame["Original"]==end["FeedOriginal"],"actual native Begin original");integer(frame["Nanos"],1);mark(kind)
        elif kind=="feed-publish":
            fields(frame,"Type Token Nanos");require("feed-begin" in mandatory and frame["Token"]==end["FeedToken"],"actual same original Publish");integer(frame["Nanos"],1);mark(kind)
        elif kind=="move-reserved":
            require(frame["Move"]==end["Move"],"actual reserved move identity");mark(kind)
        elif kind=="handoff-frozen":
            require("move-reserved" in mandatory and frame["Move"]==end["Move"] and frame["Frozen"]["Phase"]==1 and frame["View"]["State"]==end["Move"]["Before"],"actual original captured route and frozen native guard")
            mark(kind)
        elif kind=="handoff-activated":
            require("handoff-frozen" in mandatory and frame["Census"]==end["Handoff"],"actual observed sealed handoff")
            census_observation(frame["Census"],states["handoff"],end,spool,True)
            require(frame["State"]==end["Handoff"]["Seal"]["After"],"real A1 to B2 catalog authority, stable source epochs")
            mark(kind)
        elif kind=="held-source-gc-refused":
            fields(frame,"Type Error Records")
            require(frame["Error"]=="fdb-source: retained history has live owners" and len(frame["Records"])==3,"actual live-held GC refusal")
            values=feed.controls(frame["Records"])
            for name in ("Pin","Snapshot"):
                owner=end["FeedOriginal"][name]
                require(values[key(c,0,"source-owner-v1",owner["Request"])]==go_json(owner),"refusal retains genuine unchanged live owner")
            require(decoded(values[key(c,0,"source-retention-v1","state")])["Owners"]>=2,"native source guard remains live-held")
            mark(kind)
        elif kind=="feed-retained-only":
            require("held-source-gc-refused" in mandatory and not gc_done,"physical snapshot released before GC")
            phase=decoded(blob(frame["Record"]["Value"]))
            raw_record(frame["Record"],feed.feed_key(c,"retained-phase","transfer"),go_json(phase))
            require(phase["Version"]==1 and phase["Original"]==end["FeedToken"]["Original"] and phase["Request"]=="snapshot-released" and phase["OwnerRows"]>=2,"native irreversible feed-only transition")
            spool.insert(b"retained-phase",b"actual",go_json(phase));mark(kind)
        elif kind in ("retained-physical-refused","old-cut-refused","retained-new-refused"):
            require(frame["Error"]=="fdb-source: retained cut or token expired","actual expired refusal, no current/empty fallback")
            if kind=="retained-physical-refused":require("feed-retained-only" in mandatory and frame["Original"]==end["FeedOriginal"],"same old physical snapshot refused after durable release")
            elif kind=="old-cut-refused":require(gc_done and frame["Cut"]==end["Old"],"actual old physical cut refused only after source GC")
            else:require(position==end["Final"]["Groups"] and frame["Token"]==end["FeedToken"],"expired new retained reader refused after ACK EOF")
            mark(kind)
        elif kind=="source-gc-page":
            require("feed-retained-only" in mandatory and not gc_done and frame["Budget"]=={"Rows":32,"Bytes":4<<20,"OutputBytes":4096},"actual bounded source collector invocation")
            page=frame["Page"]
            require(page["Phase"] in (1,2,3) and 0<=page["WorkRows"]<=32 and 0<=page["WorkBytes"]<=4<<20,"authored finite native work admission")
            deleted=[a+b for a,b in zip(deleted,page["Deleted"],strict=True)];deleted_bytes=[a+b for a,b in zip(deleted_bytes,page["DeletedBytes"],strict=True)]
            if page["Done"]:
                require(page["Seal"]==end["Retired"] and page["Phase"]==3 and end["Retired"]["Deleted"]==deleted and end["Retired"]["DeletedBytes"]==deleted_bytes and sum(deleted)==3*n,"actual complete native GC seal/byte counters")
                gc_done=True
        elif kind in ("retained-open","retained-reopen"):
            identity=frame["Identity"]
            require(gc_done and identity["Original"]==end["FeedToken"]["Original"] and identity["Request"]==("retained-one" if kind=="retained-open" else "retained-replacement"),"real retained-only control identity, no physical snapshot authority")
            integer(frame["Nanos"],1);mark(kind)
        elif kind=="retained-page-replay":
            delivery=frame["Delivery"]
            require("retained-reopen" in mandatory and not replayed and delivery["Replay"] is True,"explicit pending page SAME recovery")
            raw_record(delivery["Record"],feed.feed_key(c,"page/transfer",delivery["Page"]["Request"]),go_json(delivery["Page"]))
            spool.insert(b"pending-replay",delivery["Page"]["Request"].encode(),go_json(delivery["Page"]));integer(frame["Nanos"],1);replayed=True
        elif kind=="retained-page":
            require(gc_done,"feed after genuine physical history deletion")
            position,last,used=retained_page(frame,end,spool,position,last)
            if pages==0:require(replayed and spool.lookup(b"pending-replay",frame["Delivery"]["Page"]["Request"].encode())==go_json(frame["Delivery"]["Page"]),"actual pending page survives reader Close/reopen unchanged")
            pages+=1;rows+=used;integer(frame["PageNanos"],1);integer(frame["AckNanos"],1)
        elif kind=="storage-eof":
            s=states["terminal-storage"];o,p=census_observation(frame["Captured"],s,end,spool,False)
            heads=frame["FinalHeads"]
            require(len(heads)==len(o["Heads"]) and [r["Key"] for r in heads]==[r["Key"] for r in o["Heads"]],"complete exact predecessor head replacement after authentic Cancel")
            native_bytes=p["Bytes"]+sum(len(blob(r["Key"]))+len(blob(r["Value"])) for r in frame["Captured"]["Controls"])
            for before,after in zip(o["Heads"],heads,strict=True):
                canceled_head(before,after,frame["Move"],end)
                native_bytes+=len(blob(after["Value"]))-len(blob(before["Value"]))
                spool.insert(b"terminal-head",blob(after["Key"]),blob(after["Value"]))
            require(end["Storage"]["Rows"]==p["Rows"]+3 and end["Storage"]["FrozenRows"]==p["Rows"] and end["Storage"]["FrozenBytes"]==p["Bytes"] and end["Storage"]["Bytes"]==native_bytes and "not physical" in frame["Access"],"all native retained/control/coordination logical costs, not physical/RSS")
            mark(kind)
        else:require(False,"unknown/mixed transfer event "+str(kind))
    required={"feed-begin","feed-publish","move-reserved","handoff-frozen","handoff-activated","held-source-gc-refused","feed-retained-only","retained-physical-refused","old-cut-refused","retained-new-refused","retained-open","retained-reopen","storage-eof","handoff-api/reserve","handoff-api/freeze","handoff-api/activate","census-eof/handoff","census-eof/terminal-storage"}
    require(mandatory==required and snapshot_eofs==point_sets=={"held-old-after-target-restart","current-before-gc"} and position==end["Final"]["Groups"] and rows==end["FeedRows"]==n and pages==end["FeedPages"] and end["ReplayedPages"]==1 and gc_done and all(s["done"] for s in states.values()) and set(states)=={"handoff","terminal-storage"},"complete authentic transfer/held read/release/GC/feed EOF lifecycle")


def canceled_head(before,after,move,end):
    c=end["Config"];native=blob(before["Key"])
    require(native==blob(after["Key"]),"same exact post-Cancel predecessor key")
    required={key(c,0,"owner-catalog-v1",name) for name in ("state/current","move-progress/"+move["Request"],"cleanup/move/"+move["Request"])}|{key(c,0,"source-owner-v1",move["Owner"]["Request"]),key(c,0,"source-retention-v1","state")}
    if before["Value"]==after["Value"]:
        require(native not in required,"missing mandatory actual Cancel transition")
        return
    old,next_=decoded(blob(before["Value"])),decoded(blob(after["Value"]))
    if native==key(c,0,"owner-catalog-v1","state/current"):
        require(old==move["Before"]|{"Moving":move["Request"]} and next_==move["Before"]|{"Version":move["Before"]["Version"]+1},"Cancel monotone catalog control version, no bucket epoch reset")
    elif native==key(c,0,"owner-catalog-v1","move-progress/"+move["Request"]):
        require(old["Phase"]==1 and next_==old|{"Phase":3},"same original native move canceled")
    elif native==key(c,0,"owner-catalog-v1","cleanup/move/"+move["Request"]):
        require(old["Request"]==next_["Request"]=="move/"+move["Request"] and next_["Calls"]==old["Calls"]+1<=8 and (next_["DeadlineUnixNanos"]==old["DeadlineUnixNanos"] if old["DeadlineUnixNanos"] else end["Binding"]["Transfer"]["AdmittedUnixNanos"]<next_["DeadlineUnixNanos"]<=end["Binding"]["Transfer"]["DeadlineUnixNanos"]+30_000_000_000),"same prepaid cleanup original/deadline, no reset")
    elif native==key(c,0,"source-owner-v1",move["Owner"]["Request"]):
        require(old==move["Owner"] and next_==old|{"State":2,"TerminalRequest":move["Request"]},"authenticated same held source owner release")
    elif native==key(c,0,"source-retention-v1","state"):
        require(old["Owners"]>0 and next_==old|{"Owners":old["Owners"]-1},"only original owner count released")
    else:require(False,"unrelated mutable head changed after frozen complete raw census")


def after_gc(directory,end,spool,header):
    class AfterFiles(Files):
        def __truediv__(self,name):
            return self.directory/"fdb-transfer-after-gc-history.jsonl" if name=="fdb-feed-history.jsonl" else super().__truediv__(name)
    proxy=end|{"Sources":[end["AfterGC"],*end["Sources"][1:]]}
    counts=[0,0];size=[0,0];after_header=None
    for kind,row in feed.raw_stream(AfterFiles(directory),"history",proxy):
        if kind=="header":after_header=row;continue
        g=row["Group"];v=decoded(row["Value"])
        lead=prefix(end["Config"],g)+b"history/"
        suffix=row["Key"].removeprefix(lead)
        coordinate,sep,encoded=suffix.partition(b"/")
        require(row["Key"].startswith(lead) and sep and len(coordinate)==16 and int(coordinate,16)==v["Round"],"canonical physical retained revision key")
        logical=bytes.fromhex(encoded.decode()).decode()
        require(row["Value"]==spool.lookup(b"fdb-current",key(end["Config"],g,"current",logical))==spool.lookup(b"fdb-history",row["Key"]),"every retained anchor is exact current6 original, not substituted equal-count old5")
        spool.insert(b"after-history",row["Key"],row["Value"]);counts[g]+=1;size[g]+=len(row["Key"])+len(row["Value"])
    require(sum(counts)==3+3*end["Spec"]["facts"] and after_header is not None,"all exact physical anchors after native GC")
    seal=end["Retired"]
    require(seal["Request"]=="source-gc" and seal["Cut"]==end["Final"] and seal["Before"]==header["Metadata"] and seal["After"]==after_header["Metadata"] and seal["Anchors"]==counts and seal["Originals"]==[m["Versions"] for m in header["Metadata"]] and seal["CumulativeDeleted"]==seal["Deleted"],"actual source-retirement native seal joins both physical EOFs")
    before_size=[0,0]
    for k,v in spool.walk(b"fdb-history"):
        g=0 if k.startswith(prefix(end["Config"],0)) else 1
        before_size[g]+=len(k)+len(v)
    for g in range(2):
        require(seal["Deleted"][g]==header["Metadata"][g]["Versions"]-counts[g] and seal["DeletedBytes"][g]==before_size[g]-size[g] and seal["After"][g]==seal["Before"][g]|{"Versions":counts[g],"Bytes":seal["Before"][g]["Bytes"]-seal["DeletedBytes"][g]},"exact atomic physical version/byte deletion accounting")
    for k,v in spool.walk(b"fdb-current"):
        g=0 if k.startswith(prefix(end["Config"],0)) else 1
        logical=bytes.fromhex(k.split(b"/current/",1)[1].decode())
        native=prefix(end["Config"],g)+f'history/{decoded(v)["Round"]:016x}/'.encode()+logical.hex().encode()
        require(spool.lookup(b"after-history",native)==v==spool.lookup(b"feed-replica",k),"no missing current anchor and snapshot-plus-complete-feed exact final set")


def native_census(end,spool):
    c=end["Config"];n=end["Spec"]["facts"]
    for request in ("handoff","terminal-storage"):
        ns=b"census-"+request.encode()
        history,current,changes,ids,requests,cuts=[0,0],[0,0],[0,0],[0,0],[0,0],[0,0]
        first_use=0;live_owners=0
        for native,value in spool.walk(ns):
            g=0 if native.startswith(prefix(c,0)) else 1
            suffix=native[len(prefix(c,g)):];kind,_,tail=suffix.partition(b"/")
            if kind==b"current":
                logical=bytes.fromhex(tail.decode()).decode()
                actual=spool.lookup(b"fdb-current" if request=="terminal-storage" else b"fdb-old",native)
                # The source K COMMIT precedes Freeze, while its reply is lost.
                if request=="handoff":
                    original=decoded(spool.lookup(b"fdb-operation",u64(4+n)))
                    match=[w for w in original["Writes"] if w["Group"]==g and w["Key"]==logical]
                    if match:actual=go_json(match[0]["After"])
                require(value==actual,"complete physical source head at exact frozen stage")
                current[g]+=1
            elif kind==b"history":
                require(value==spool.lookup(b"fdb-history" if request=="handoff" else b"after-history",native),"raw native census exact physical history/anchor")
                if request=="handoff":require(decoded(value)["Round"]<=end["Handoff"]["Original"]["Frozen"]["Cut"]["Round"],"future source revision in frozen prefix")
                history[g]+=1
            elif kind==b"changes":
                require(value==spool.lookup(b"native-cdc",native),"complete immutable original CDC tail remains retained")
                changes[g]+=1
            elif kind in (b"id",b"request"):
                require(value==spool.lookup(b"attempt-"+kind,native),"all actual COMMIT AND ABORT paired originals retained")
                (ids if kind==b"id" else requests)[g]+=1
            elif kind==b"cut":
                cut=decoded(value)
                raw_record({"Key":base64.b64encode(native).decode(),"Value":base64.b64encode(value).decode()},key(c,g,"cut",go_json(cut).hex()),value)
                require(cut["Graph"]==c["Graph"] and cut["Epochs"]==c["Epochs"] and cut["Topology"]==c["Topology"] and blob(cut["Scope"])==b"\x00\x01","retained exact-scope native certificate lineage")
                cuts[g]+=1
            elif kind==b"owner-catalog-v1":
                name=bytes.fromhex(tail.decode()).decode();which,_,identity=name.partition("/")
                if which in ("intent-id","intent-request"):
                    value_intent=decoded(value);tx=intent(value_intent,end)
                    require(identity==tx["ID" if which=="intent-id" else "Request"] and value==spool.lookup(b"prepared",tx["Request"].encode())==spool.lookup(b"native-prepare",tx["Request"].encode()),"native retained intent both identities/body/route capture")
                elif which=="allocation-issued":
                    issued=decoded(value);claim=issued["Claim"]
                    require(identity==f'{claim["ID"]:016x}',"native per-ID first-use provenance key")
                    outcome=decoded(spool.lookup(b"attempt-request",key(c,issued["Coordinator"],"request",issued["Request"])))
                    original=decoded(spool.lookup(b"prepared",issued["Request"].encode()))
                    require(outcome["Commit"] is True and outcome["Digest"]==issued["Digest"]==original["Digest"] and outcome["ID"]==issued["Transaction"] and claim in original["Allocated"],"durable first-use bytes bind genuine committed factory/claim, not reservation issuance")
                    require(spool.lookup(b"actual-next",u64(claim["ID"]))==go_json(claim),"every durable first-use ID came from an actual installed Cursor.Next result")
                    first_use+=1
            elif kind==b"columns-v1" and request=="terminal-storage":
                require(value==spool.lookup(b"column-storage",native),"whole native census includes every authentic derived chunk/control/tombstone byte")
            elif kind==b"source-owner-v1":
                owner=decoded(value);live_owners+=owner["State"]==1
                require(native==key(c,0,"source-owner-v1",owner["Request"]) and owner["State"] in (1,2,3) and owner["Kind"] in ("read","pin","column-pin"),"actual canonical retained owner key/state/kind")
                if request=="terminal-storage":
                    actual=decoded(spool.lookup(b"terminal-head",native))
                    require(actual["State"] in (2,3) and actual["TerminalRequest"],"no live physical source owner hidden by zero reported owner count")
        meta=[decoded(spool.lookup(ns,key(c,g,"meta","state"))) for g in range(2)]
        require(current[0]+current[1]==3+3*n and history==[m["Versions"] for m in meta] and changes==[m["Groups"] for m in meta] and ids==requests==[m["Transactions"] for m in meta] and cuts==[m["Certificates"] for m in meta] and first_use==2*n+8,"complete physical/control original EOF census, not reported attempt counts")
        require(decoded(spool.lookup(ns,key(c,0,"source-retention-v1","state")))["Owners"]==live_owners,"complete authentic native source owner inventory at each frozen census")
        if request=="terminal-storage":
            require(sum(ids)==end["Attempts"] and sum(history)==3+3*n and changes==end["Final"]["Groups"],"all original attempts survive target restart and real source GC")
            state=decoded(spool.lookup(b"terminal-head",key(c,0,"owner-catalog-v1","state/current")))
            require(state=={"Version":3,"Routes":[{"Owner":"B","Epoch":2},{"Owner":"B","Epoch":1}],"Moving":""},"storage-only reverse freeze Cancel advances catalog fence, preserving B2 bucket processing epoch")
            source=decoded(spool.lookup(b"terminal-head",key(c,0,"source-retention-v1","state")))
            require(source["Phase"]==3 and source["Completed"]==end["Retired"] and source["Owners"]==0,"all genuine source owners closed; physical floor remains authenticated")
            feed_binding=decoded(spool.lookup(ns,feed.feed_key(c,"meta","binding")))
            require(feed_binding["Version"]==3 and feed_binding["Retention"]==end["Binding"] and feed_binding["Source"]==end["Binding"]["Source"] and feed_binding["Policy"]==end["FeedOriginal"]["Policy"] and "Admission" not in feed_binding,"retained-only feed authentic catalog/transport lineage, distinct C retention contract")
            cursor=decoded(spool.lookup(b"terminal-head",feed.feed_key(c,"cursor","transfer")))
            require(cursor["State"]==3 and cursor["TerminalRequest"]=="feed-expired" and cursor["Positions"]==end["Final"]["Groups"] and cursor["Completed"]==end["Final"] and cursor["Pending"]=="","actual terminal retained cursor expired after complete ACK")
            snapshot=end["FeedOriginal"]["Snapshot"]|{"State":2,"TerminalRequest":"close"}
            pin=end["FeedOriginal"]["Pin"]|{"State":2,"TerminalRequest":"snapshot-released"}
            for owner in (snapshot,pin):require(spool.lookup(b"terminal-head",key(c,0,"source-owner-v1",owner["Request"]))==go_json(owner),"actual original source owner/pin authenticated release")
            phase=decoded(spool.lookup(b"terminal-head",feed.feed_key(c,"retained-phase","transfer")))
            require(go_json(phase)==spool.lookup(b"retained-phase",b"actual") and phase["SnapshotSHA256"]==list(hashlib.sha256(go_json(snapshot)).digest()) and phase["PinSHA256"]==list(hashlib.sha256(go_json(pin)).digest()),"irreversible feed-only transition binds same released original owners")
            for name in ("retained-one","retained-replacement"):
                reader=decoded(spool.lookup(b"terminal-head",feed.feed_key(c,"retained-reader/transfer",name)))
                require(reader["Identity"]["Request"]==name and reader["Identity"]["Original"]==end["FeedToken"]["Original"] and reader["State"]==2,"every genuine retained reader authentically closed")


def allocation(end,spool):
    c=end["Config"];ns=b"census-terminal-storage";n=end["Spec"]["facts"]
    next_,grants,burned=1,0,0
    for native,body in spool.walk(ns):
        start=key(c,0,"owner-catalog-v1","allocation-index/")
        if not native.startswith(start):continue
        g=decoded(body);name=g["Request"]
        require(g["Version"]==1 and g["Graph"]==c["Graph"] and g["First"]==next_ and g["Last"]==g["First"]+g["Count"]-1 and g["Count"]<=end["Binding"]["Transfer"]["MaxBlock"] and g["Home"] in ({"Owner":"A","Epoch":1},{"Owner":"B","Epoch":2}),"actual durable block continuity/source provenance, no per-ID counter emulation")
        require(body==spool.lookup(ns,key(c,0,"owner-catalog-v1","allocation-grant/"+name)) and native==key(c,0,"owner-catalog-v1",f'allocation-index/{g["Last"]:016x}'),"both native grant/index originals")
        slot=decoded(spool.lookup(b"terminal-head",key(c,0,"owner-catalog-v1","allocation-slot/"+name)))
        require(slot["State"]==2 and slot["Grant"]==name and slot["Session"]["Grant"]==name and slot["Session"]["Route"]==g["Recipient"] and 0<=slot["Consumed"]<=g["Count"],"genuine installed session closed without reset")
        require(slot["Consumed"]==(1 if name=="successor-block" else g["Count"]),"actual exhausted HOME/REMOTE; successor suffix burned")
        if name=="successor-block":require(g["First"]==2*n+9 and g["Count"]==8 and g["Recipient"]=={"Owner":"B","Epoch":2},"successor genuinely reserves under new epoch")
        next_=g["Last"]+1;grants+=1;burned+=g["Count"]-slot["Consumed"]
    actual=decoded(spool.lookup(b"terminal-head",key(c,0,"owner-catalog-v1","allocation-state/current")))
    require(actual=={"Next":next_,"Grants":grants,"Exhausted":False} and next_==2*n+17 and burned==end["Burned"]==7,"durable high-water/burnt reservation cost retained through restart")
    issued=0
    for _,body in spool.walk(b"rpc"):
        frame=decoded(body)
        if frame["Type"]!="rpc" or frame["Reply"]["Error"]:continue
        call,r=frame["Call"],frame["Reply"]
        if call["Method"] not in ("next-node","next-value","next-edge"):continue
        require(1<=len(r["Allocated"])<=2,"actual successful local Next result")
        for claim in r["Allocated"]:
            g=decoded(spool.lookup(ns,key(c,0,"owner-catalog-v1","allocation-grant/"+claim["Grant"])))
            slot=decoded(spool.lookup(b"terminal-head",key(c,0,"owner-catalog-v1","allocation-slot/"+claim["Grant"])))
            require(claim["ID"]==g["First"]+claim["Offset"] and claim["Offset"]<slot["Consumed"] and claim["Session"]==slot["Session"]["Request"],"Next exact installed grant/session/sequence")
            issued+=1
    require(issued==end["Issued"],"grant reservation is not an issuance claim")


def timing(directory,end,spool):
    counts={}
    def add(name,nanos,identity):
        integer(nanos,1,(1<<63)-1)
        spool.insert(b"transfer-time/"+name.encode(),u64(nanos)+identity,b"1")
        counts[name]=counts.get(name,0)+1
    for frame,_ in lines(Path(directory)/"fdb-transfer-events.jsonl",event_limit(end)):
        kind=frame["Type"]
        if kind=="attempt" and frame["Attempt"]["Classification"]=="committed":
            a=frame["Attempt"]
            add("business-"+a["Operation"]["Phase"],a["Nanos"],u64(a["Operation"]["Ordinal"]))
        elif kind in ("snapshot-eof","points"):
            add(("Scan-" if kind=="snapshot-eof" else "Points-scan-based-")+frame["Name"],frame["Nanos"],b"one")
        elif kind in ("feed-begin","feed-publish","retained-open","retained-reopen","retained-page-replay"):
            add(kind,frame["Nanos"],b"one")
        elif kind=="retained-page":
            identity=frame["Delivery"]["Page"]["Request"].encode()
            add("Page",frame["PageNanos"],identity);add("ACK",frame["AckNanos"],identity)
        elif kind=="handoff-api":add("catalog-"+frame["Name"],frame["Nanos"],b"one")
        elif kind=="census-eof":add("chosen-census-"+frame["Request"],frame["Nanos"],b"one")
    result=[]
    for name,count in sorted(counts.items()):
        values={}
        for index,(coordinate,_) in enumerate(spool.walk(b"transfer-time/"+name.encode()),1):
            for percentile in (50,95,99):
                if index==(percentile*count+99)//100:values[f"p{percentile}Nanos"]=int.from_bytes(coordinate[:8],"big")
        result.append({"call":name,"samples":count,**values})
    phases=[]
    for expected,phase in zip(("create","update"),end["Phases"],strict=True):
        cross=end["Spec"]["facts"]*end["Spec"]["crossPercent"]//100
        require(phase["Phase"]==expected and phase["Operations"]==end["Spec"]["facts"] and phase["BusinessCrossTransactions"]==cross and phase["BusinessLocalTransactions"]==end["Spec"]["facts"]-cross,"actual business read/write/claim footprint, distinct from owner collocation or control messages")
        wall=integer(phase["WallNanos"],1,(1<<63)-1)
        phases.append({**phase,"usefulOperationsPerSecond":phase["Operations"]*1_000_000_000/wall,"logicalParticipants":{"local":1,"cross":2},"currentProcessingOwners":2 if expected=="create" else 1,"physicalFDBRolePlacement":"four owned co-resident logical-zone processes; actual role map/resource evidence separate"})
    require(counts.get("business-setup")==3 and counts.get("business-create")==counts.get("business-update")==end["Spec"]["facts"],"complete useful-operation timing inventory")
    return {"nearestRank":result,"phases":phases,"scope":"complete caller retry/wait; throughput uses actual phase wall, never sum of parallel latencies. Update includes target loss/recovery and chosen mandatory catalog2 diagnostic census; the census/API durations are reported separately without subtraction or an inherent FDB copy-cost claim. Points are scan-based, not indexed latency."}


def verify(directory,spool_directory,policy=None):
    directory=Path(directory).resolve(strict=True)
    end=result(directory)
    feed_identity(end)
    require(policy is not None or end["Spec"]["facts"]<1_000_000 and end["Spec"]["writers"]!=32,"explicit finite verifier disk/cache grant for large cell")
    with Spool(spool_directory,policy or Policy(1<<30,8<<20,4096,LINE)) as spool:
        first_pass(directory,end,spool)
        originals(directory,end,spool)
        files=Files(directory)
        header=feed.source_fold(files,end,spool)
        feed.columns(files,end,spool)
        lifecycle(directory,end,spool)
        after_gc(directory,end,spool,header)
        native_census(end,spool)
        allocation(end,spool)
        durations=timing(directory,end,spool)
        transport=raw.transport.verify(directory,end["Transport"]["Policy"],spool)
        report=spool.report()
    return {"format":FORMAT,"family":FAMILY,"spec":end["Spec"],"runGraph":end["Config"]["Graph"],"attempts":end["Attempts"],"committed":end["Committed"],"aborted":end["Aborted"],"logicalNativeStorage":end["Storage"],"issuedIDs":end["Issued"],"burnedReservationIDs":end["Burned"],"timing":durations,"transport":transport,"spool":report,
            "retentionContract":"snapshot-C retained across A1-to-B2 and target SDK replacement; explicitly released before feed3 retained-only + physical source-history collection",
            "scope":"one actual fresh FDB transfer-family cell with stable logical source keys, authentic monotone processing-owner fence, immutable COMMIT/fenced-ABORT recovery and complete native cost census. Census is chosen conservative certification overhead, not physical replica copy. Feed-only post-GC differs from native C-retaining feed; no matched native/paired verdict, independent-host, full-matrix or capacity acceptance."}


if __name__=="__main__":
    try:
        if len(sys.argv) not in (3,5):raise ValueError("usage: fdb_transfer_check.py ARTIFACTS OWNED_SPOOL_DIR [DISK_BYTES CACHE_BYTES]")
        policy=None if len(sys.argv)==3 else Policy(int(sys.argv[3]),int(sys.argv[4]),4096,LINE)
        print(json.dumps(verify(sys.argv[1],sys.argv[2],policy),indent=2))
    except (ValueError,KeyError,TypeError,OSError) as error:
        print("transfer checker refused: "+str(error),file=sys.stderr);sys.exit(1)
