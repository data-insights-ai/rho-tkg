#!/usr/bin/env python3
"""Independent complete native selector answers; no execution/acceptance claim.

The scalar raw codec authenticates originals, native roots, typed CDC and all
voters. This oracle independently joins each mode's exact historical answers,
scope refusal, posting EOF and same-owner continuation against those raw cells.
Dataset-sized state belongs exclusively to the caller's finite scratch spool.
"""
from contextlib import nullcontext
import base64
import hashlib
import os
from pathlib import Path
import stat
import struct

import native_feed_check as scalar
from comparison_spool import Policy, Spool, u64

codec, raw, column = scalar.codec, scalar.raw, scalar.column
require, integer, fields, blob, encoded = scalar.require, scalar.integer, scalar.fields, scalar.blob, scalar.encoded
array, checked, cursor, digest, decoded, lines = scalar.array, scalar.checked, scalar.cursor, scalar.digest, scalar.decoded, scalar.lines
FORMAT, FAMILY, LINE = "native-snapshot-selectors-workload-v1", "snapshot-selectors", scalar.LINE
TRANSPORTS, MAX_SECONDS = scalar.TRANSPORTS, scalar.MAX_SECONDS


def agreement():
    value = digest(b"rho-tkg:graphapply:partition-writes:v1\0genesis=default-schema-routing-v1\0range-publication=allocator-coinstalled-v1\0"
        b"routing=explicit-current-range-and-typed-buckets-v1\0axis-authority=AxisID-v1\0local-graph-writes=complete-footprint-v1\0global-value-identity=exact-v1\0"
        b"request-recovery=1\0cross-group-writes=unavailable\0certified-cuts=unavailable\0"
        b"cross-graph=ordered-coarse-prepare-kernel-v1\0recovery=protected-v2\0capacity-abort=global-TxID-owner-v1\0tx-id=sha256-128-graph-coordinator-request-v1\0")
    for extension in (
        b"certified-graph-reads=v1\0round-prefix=graph-and-routing-v1\0separate-fence=1\0",
        b"range-move=streamed-logical-and-protocol-preservation-v2\0admission=coarse-resolved-fence-v1\0placement=stable-range-epoch-v1\0replay=original-qualified-identity-v1\0retention=source-certified-lineage-v1\0recovery=protected-pages-and-precommit-abort-v1\0",
        b"conditional-graph=complete-native-prefix-v1\0original-register-exception=exact-v1\0",
        b"durable-cut-feed-pins=participant-ledger-v1\0terminal-bank=native-v2\0lease-policy=replicated-epoch-v1\0original-identity=issuer-request-v1\0",
        b"native-retention=authenticated-sparse-plan-v1\0source=current-plan-anchor-v1\0originals=immutable-controls-v1\0handoff=resolved-fence-retained-lineage-v1\0",
        b"durable-columns=exact-native-issuance-v1\0intent-cancel=ordered-tombstone-v1\0original-outcome-footprint=feed-min-v1\0"):
        value = digest(value + extension)
    return value


AGREEMENT = agreement()
STAGES = ("initial-empty-fresh", "initial-empty-stable", "old-scope-fresh-3", "old-scope-fresh-8", "old-fresh",
          "stable-after-update", "stable-scope-after-update-3", "stable-scope-after-update-8", "same-pin-old-after-update",
          "at-old-after-update", "after-current", "fresh-current", "reopened-old", "reopened-final")
MODES = (1, 2, 1, 1, 1, 2, 2, 2, 0, 3, 4, 1, 0, 0)
EMPTY_CELL = {"present":False,"null":False,"handle":0,"revision":0,"provenance":0,"kind":"","i64":0}


def shape(s):
    fields(s, "version family facts crossPercent writers layout")
    require(integer(s["version"],1,1) and s["family"] == FAMILY and integer(s["facts"],20,1_000_000)%20 == 0
            and integer(s["crossPercent"]) in (0,10,50,100) and integer(s["writers"]) in (1,8,32)
            and s["facts"] > s["writers"] and s["layout"] in ("kv","columns"), "explicit native selectors dimensions")
    return s


def frames(s):
    return (16*shape(s)["facts"]+3)*10+64*10+4096


def grants(directory,end,spool):
    return scalar._scalar_grants(directory,end,spool,AGREEMENT,cut)


def claims(directory,end,spool):
    return scalar._scalar_claims(directory,end,spool)


def claim_binding(c,expected,home,business_index,spool):
    return scalar._scalar_claim_binding(c,expected,home,business_index,spool)


def cut(value):
    c=cursor(checked(blob(value),b"GEQ\1",16384))
    require(c.number(1)==3 and not any(c.take(16)) and c.number()==0,"actual selector At certificate encoding")
    count=integer(c.number(4),1,2)
    result=[column.certificate(raw,base64.b64encode(c.field(512)).decode()) for _ in range(count)]
    c.done()
    require([r["partition"] for r in result] in ([3],[8],[3,8]) and all(r["scope"][88:120]==AGREEMENT for r in result)
            and len({r["round"] for r in result})==1,"exact selector scope/semantic/round")
    return result


def pin(original, token, selected, spool, expected_request=None):
    wire=blob(original);c=cursor(checked(wire,b"GPM\1",32768))
    scope,issuer,request=c.take(120),c.take(16),c.take(16)
    epoch,expires,count=c.number(),c.number(),c.number(4)
    require(scope==selected[0]["scope"] and any(issuer) and any(request) and epoch<expires==100 and count==2,"full genuine selector publication recipe")
    require(expected_request is None or request==expected_request,"publication changed exact query original request")
    identity=digest(b"rho-tkg:retained-graph-pin:v1\0"+scope+issuer+request)
    for cert in selected:
        actual,feed,lineage=c.field(512),c.number(),c.take(32)
        require(column.certificate(raw,base64.b64encode(actual).decode())==cert and feed==cert["sourceIndex"]+1
                and lineage==digest(b"rho-tkg:retained-pin-source-lineage:v1\0"+actual+u64(feed)),"exact cut/root/feed lineage")
    c.done()
    if token is not None:
        t=cursor(checked(blob(token),b"GPT\1",33792))
        require(t.field(32768)==wire and t.number(4)==2,"same immutable original token")
        for cert in selected:
            index=t.number();actual=decoded(spool.lookup(b"pin-result",u64(cert["partition"])+u64(index)))
            require(actual["phase"]==actual["status"]==1 and actual["publication"]==index and actual["pin"]==identity.hex()
                    and actual["digest"]==digest(wire).hex() and actual["epoch"]==epoch,"actual participant installation before token")
        t.done()
    return identity, digest(wire)


def cell(actual, expected, kind):
    fields(actual,"present null handle revision provenance kind i64")
    require(type(actual["present"]) is type(actual["null"]) is bool and actual["kind"]==kind,"typed native cell shape")
    normalized={"Present":actual["present"],"Null":actual["null"],"Handle":integer(actual["handle"]),"Value":integer(actual["i64"]),
                "Revision":integer(actual["revision"]),"Provenance":integer(actual["provenance"])}
    require(normalized==expected,"historical cell differs from actual original revision/presence/value/provenance")


def point(s, entity, value, spool):
    fields(entity,"id found effective kind type mode source target effectiveLife labels presence property")
    identity=integer(entity["id"],1);require(type(entity["found"]) is type(entity["effective"]) is bool,"point existence booleans")
    for name in ("kind","mode","source","target","effectiveLife"):integer(entity[name])
    remote=2*s["facts"]+7
    if value==0 or identity==6+2*s["facts"]:
        require(entity=={"id":identity,"found":False,"effective":False,"kind":0,"type":"","mode":0,"source":0,"target":0,"effectiveLife":0,
                         "labels":[],"presence":EMPTY_CELL,"property":EMPTY_CELL},"phantom/nonempty initial answer")
        return
    if identity in (1,3,remote):
        ordinal={1:1,3:2,remote:3}[identity]
        require(entity["kind"]==1 and entity["type"]=="" and entity["mode"]==entity["source"]==entity["target"]==0
                and entity["property"]==EMPTY_CELL,"native immutable node descriptor")
        expected=decoded(spool.lookup(b"row",u64(ordinal)+u64(identity)+b"\1"))["After"]
        cell(entity["presence"],expected,"life")
        require(entity["found"] and entity["effective"] and entity["effectiveLife"]==identity+1 and entity["labels"]==[],"native node presence/life")
    else:
        fact=(identity-7)//2
        require(identity==7+2*fact and 0<=fact<s["facts"],"unexpected relationship identity")
        scalar.point(s,{"Fact":fact,"Entity":entity},value,spool)


def stage_value(stage):
    return 0 if stage.startswith("initial-empty") else 6 if stage in ("after-current","fresh-current","reopened-final") else 5


def authenticate_cut(selected,end,spool):
    initial=cut(end["Initial"])
    for cert in selected:
        group=0 if cert["partition"]==3 else 1;first=initial[group]
        require(cert["scope"]==first["scope"] and cert["configuration"]==first["configuration"] and cert["sourceIndex"]>=first["sourceIndex"],"source certificate scope/configuration")
        if cert["sourceIndex"]==first["sourceIndex"]:actual=first["image"]
        else:
            frame=decoded(spool.lookup(b"frame",u64(cert["partition"])+u64(cert["sourceIndex"])))
            actual=array(frame["Root"],172)
        require(cert["image"]==actual,"selector certificate does not name its actual retained native root")


def reads(directory,end,spool):
    s=end["spec"];stage_index=-1;current=None;owners={};last_call=0;previous_trace=None;extra=0
    completed=set();published={};timings=[];observations={};query_count=0
    bound=16*((s["facts"]+15)//16+end["Admission"]["BudgetSeconds"]//60+16)+4*((frames(s)+3)//4)+64
    for trace in lines(directory/"reads.jsonl",bound*8+256):
        if trace.get("Type")=="observation-timing":
            fields(trace,"Type Stage OpenNanos ScanNanos PointsNanos ScopeRefusalNanos Rows")
            require(current==trace["Stage"] and current not in completed and len(owners)==1 and observations[current]["points"]
                    and "timing" not in observations[current],"timing must join the genuine completed observation")
            o=observations[current];scoped="scope-" in current
            require(integer(trace["Rows"])==o["scan"] and integer(trace["OpenNanos"],1)>=o["openNanos"]
                    and integer(trace["ScanNanos"])>=o["scanNanos"] and integer(trace["PointsNanos"],1)>=o["pointNanos"]
                    and (integer(trace["ScopeRefusalNanos"],1)>=o["scopeNanos"] if scoped else trace["ScopeRefusalNanos"]==0)
                    and (trace["ScanNanos"]==0 if scoped else trace["ScanNanos"]>0),"full Open/Scan-through-EOF/Points timing includes retries and artifact callbacks")
            o["timing"]=trace
            continue
        fields(trace,"Stage Member Input Reply Call Attempt AttemptNanos OperationNanos"+(" Error" if "Error" in trace else ""))
        stage=trace["Stage"];require(stage in STAGES and trace["Member"]==2,"actual bounded selector owner/stage")
        if stage!=current:
            require(stage_index+1<len(STAGES) and stage==STAGES[stage_index+1],"omitted/reordered selector schedule")
            if current is not None:require(not owners,"previous selector source was not closed")
            stage_index+=1;current=stage
            observations[stage]={"scan":0,"points":False,"scope":False,"scanNanos":0,"openNanos":0,"pointNanos":0,"scopeNanos":0}
        q,r=trace["Input"],trace["Reply"];action=q["Action"];call=integer(trace["Call"],1);attempt=integer(trace["Attempt"],1,8)
        integer(trace["AttemptNanos"],1);integer(trace["OperationNanos"],trace["AttemptNanos"])
        if call==last_call:
            require(previous_trace is not None and previous_trace.get("Error") and q==previous_trace["Input"] and attempt==previous_trace["Attempt"]+1,"SAME retry changed original/query bytes")
            extra+=1
        else:require(call==last_call+1 and attempt==1,"query call/attempt omission")
        last_call=call;previous_trace=trace
        integer(q["Version"],1,1);integer(q["Mode"],0,4);integer(q["Handle"]);integer(q["Sequence"])
        integer(r["Version"],0,1);integer(r.get("Failure",0),0,1)
        require(type(r["EOF"]) is type(r["Replay"]) is bool,"fresh private reply booleans")
        scoped="scope-" in stage
        if trace.get("Error"):
            if r.get("Failure")==1:
                require(scoped and action=="points" and q["Sequence"]==1 and not observations[stage]["scope"] and not r["Points"] and not r["Candidates"] and not r["Token"],"scope error cannot become empty success")
                inside=1 if stage.endswith("-3") else 2*s["facts"]+7
                require(q["IDs"]==[2*s["facts"]+7 if inside==1 else 1],"actual outside-source negative probe")
                observations[stage]["scope"]=True
                observations[stage]["scopeNanos"]=trace["OperationNanos"]
            else:require(attempt<8 and not r.get("Failure"),"unresolved native source query")
            continue
        require(r["Version"]==1 and r["Action"]==action and not r.get("Failure"),"actual completed fresh response")
        if action in ("prepare","reopen"):
            query_count+=1;require(query_count<=16 and not owners and integer(r["Handle"],1),"finite genuine source ownership")
            selected=cut(r["Cut"]);value=stage_value(stage)
            authenticate_cut(selected,end,spool)
            expected=end["Initial"] if value==0 else end["Final"] if value==6 else end["Old"]
            if not scoped:require(r["Cut"]==expected,"mode substituted requested historical cut")
            else:require(len(selected)==1 and selected[0]["partition"]==(3 if stage.endswith("-3") else 8) and not r["Original"] and not r["Token"],"scoped capture invented full pin")
            require(q["Mode"]==MODES[stage_index] and (action=="reopen")==stage.startswith(("same-pin-","reopened-")),"actual mode/owner construction")
            domain="scoped-query" if scoped else "query"
            require(array(q["Request"],16)==scalar.request(array(end["AdmissionSHA256"],32),domain,stage_index+1),"exact predeclared query original identity")
            observations[stage]["cut"]=r["Cut"]
            if q["Mode"] in (1,4):require(q["Cut"] is None,"Fresh/After supplied a hidden At cut")
            if stage=="stable-after-update":require(q["Cut"] is None,"Stable was replaced by caller At")
            if stage.startswith("stable-scope-after-update-"):
                prior="old-scope-fresh-"+stage.rsplit("-",1)[1]
                require(q["Cut"]==r["Cut"]==observations[prior]["cut"],"scoped Stable changed original scope/certificate")
            if q["Mode"]==3:require(q["Cut"]==end["Old"],"At must bind genuine old certificate")
            if q["Mode"]==4:
                require(array(q["Local"],16)==array(end["AfterLocal"],16) and array(q["Transaction"],16)==array(end["AfterTransaction"],16) and q["Coordinator"]==3,"After receipt identity")
                if any(array(q["Transaction"],16)):
                    a=decoded(spool.lookup(b"terminal",array(q["Transaction"],16)));require(a["Classification"]=="commit","After unknown/ABORT original")
                else:
                    a=decoded(spool.lookup(b"terminal-local",u64(3)+array(q["Local"],16)));require(a["Classification"]=="commit","After local original")
                require(a["Operation"]["Phase"]=="update","After not bound to actual update")
            owner={"cut":selected,"wire":r["Cut"],"original":r["Original"],"token":r["Token"],"sequence":0,"last":None,"eof":False}
            owners[r["Handle"]]=owner
            if not scoped:
                identity,hash_=pin(r["Original"],r["Token"] if action=="reopen" else None,selected,spool,array(q["Request"],16) if action=="prepare" else None)
                owner["identity"],owner["hash"]=identity,hash_
                if action=="reopen":
                    require(r["Token"]==q["Token"]==end["FinalToken" if value==6 else "OldToken"],"reopen must authenticate SAME native token")
            observations[stage]["openNanos"]+=trace["OperationNanos"]
        else:
            require(q["Handle"]==r["Handle"] and q["Handle"] in owners,"reply belongs to actual current owner")
            owner=owners[q["Handle"]];o=observations[stage];value=stage_value(stage)
            if action=="publish":
                require(not scoped and owner["token"] is None and r["Token"],"publication before source release")
                pin(owner["original"],r["Token"],owner["cut"],spool);owner["token"]=r["Token"]
                published[owner["identity"]]=owner["hash"]
                if stage=="old-fresh":require(r["Token"]==end["OldToken"],"actual original old token")
                if stage=="after-current":require(r["Token"]==end["FinalToken"],"actual original final token")
                o["openNanos"]+=trace["OperationNanos"]
            elif action in ("scan","points"):
                seq=integer(q["Sequence"],1);require(r["Sequence"]==seq,"actual private sequence")
                if seq==owner["sequence"]:
                    require(owner["last"]==(q,r) and action=="points","SAME reply changed/advanced continuation")
                    continue
                require(seq==owner["sequence"]+1 and (scoped or owner["token"] is not None),"page before genuine publication or sequence gap")
                owner["sequence"],owner["last"]=seq,(q,r)
                starts=integer(r["Starts"],1,bound)
                require(len(r["Points"] or [])<=16 and integer(r["Calls"],1,bound*8),"bounded source owner charges")
                fields(r["Charged"]," ".join(column.WORK))
                for value_ in r["Charged"].values():integer(value_)
                require(r["Charged"]==dict(zip(column.WORK,(starts*16384,starts*(16<<20),0,starts*(32<<20),starts*1024,starts*(4<<30)))),"same finite retired source grants, not measured IO or refundable work")
                if action=="scan":
                    require(not scoped and not owner["eof"] and len(r["Points"] or [])==len(r["Candidates"] or []),"posting/hydration mismatch or scan after EOF")
                    for entity,candidate in zip(r["Points"] or [],r["Candidates"] or [],strict=True):
                        fact=o["scan"];require(value!=0 and candidate==entity["id"]==7+2*fact,"posting exact-set order/phantom/omission")
                        point(s,entity,value,spool);spool.insert(b"selector-row/"+stage.encode(),u64(fact),encoded(entity));o["scan"]+=1
                    require(type(r["EOF"]) is bool and (r["EOF"] or r["Points"]),"nonprogress native cursor")
                    owner["eof"]=r["EOF"];o["scanNanos"]+=trace["OperationNanos"]
                    if r["EOF"]:require(o["scan"]==(0 if value==0 else s["facts"]),"complete posting EOF exact cardinality")
                else:
                    ids=([1 if stage.endswith("-3") else 2*s["facts"]+7] if scoped else [1,3,2*s["facts"]+7,7,7+2*(s["facts"]-1),6+2*s["facts"]])
                    require(q["IDs"]==ids and len(r["Points"] or [])==len(ids) and (scoped and o["scope"] or not scoped and owner["eof"]),"fixed complete points/scope proof")
                    for identity,entity in zip(ids,r["Points"],strict=True):require(identity==entity["id"],"point reordered");point(s,entity,value,spool)
                    require(not o["points"],"duplicate point completion");o["points"]=True;o["pointNanos"]+=trace["OperationNanos"]
            elif action in ("close","release"):
                require(o["points"] and "timing" in o and (scoped or owner["eof"]),"source closed before complete observation/timing")
                if action=="release" and not scoped:
                    for cert in owner["cut"]:spool.lookup(b"pin-terminal",u64(cert["partition"])+owner["identity"]+owner["hash"])
                del owners[q["Handle"]];completed.add(stage)
            else:require(False,"unknown selector operation")
    require(not owners and completed==set(STAGES) and extra==integer(end["QueryExtraInvocations"],0,64),"missing actual selector lifecycle/SAME attempts")
    for identity,hash_ in published.items():
        for part in (3,8):spool.lookup(b"pin-terminal",u64(part)+identity+hash_)
    for stage,o in observations.items():
        timings.append({"name":stage,**{k:v for k,v in o["timing"].items() if k not in ("Type","Stage")}})
    return timings


def processes(directory,end,spool):
    clean=restored=0
    for r in lines(directory/"processes.jsonl",19):
        if r.get("Stage")=="full-cohort-restore-before-election":
            before,after=codec.process_state(r["Before"]),codec.process_state(r["Restored"])
            require(r["Member"]==restored+1 and before["incarnation"]!=after["incarnation"] and before["applied"]==after["applied"] and before["image"]==after["image"]
                    and before["stats"][0]==after["stats"][0] and before["stats"][4]==after["stats"][4],"actual same-directory bank/commit restore")
            restored+=1
        else:
            cohort,member=divmod(clean,6);member+=1
            require(r["Cohort"]==cohort+1 and r["Member"]==member and r["Reaped"] is True and integer(r["PID"],1) and r["WaitStatus"]==r["ExitCode"]==0
                    and Path(r["Volume"])==Path(end["Directory"])/str(member),"actual owned child reaping")
            spool.insert(b"incarnation",array(r["Incarnation"],16),b"1");clean+=1
    require(clean==12 and restored==6 and end["Reaped"]==end["ProcessInstances"]==12,"complete owned process inventory")
    logical=allocated=0
    for member in range(1,7):
        for parent,dirs,files in os.walk(Path(end["Directory"])/str(member),followlinks=False):
            require(all(not (Path(parent)/n).is_symlink() for n in dirs),"foreign native directory link")
            for name in files:
                st=(Path(parent)/name).lstat();require(stat.S_ISREG(st.st_mode),"regular owned native file")
                logical+=st.st_size;allocated+=st.st_blocks*512
    require((logical,allocated)==(end["PersistedFileBytes"],end["AllocatedFileBytes"]),"actual native logical/allocated file census")


def logical_answers(end,spool):
    answer=hashlib.sha256();s=end["spec"]
    for value,stage in ((5,"old-fresh"),(6,"fresh-current")):
        for fact in range(s["facts"]):
            point(s,decoded(spool.lookup(b"selector-row/"+stage.encode(),u64(fact))),value,spool)
            ordinal=4+fact+(s["facts"] if value==6 else 0)
            row=decoded(spool.lookup(b"row",u64(ordinal)+u64(7+2*fact)+b"\3"))
            coordinates=(fact,row["Owner"],row["Life"],row["Source"],row["Target"],row["SourceLife"],row["TargetLife"],row["After"]["Handle"])
            answer.update(b"edge-i64-lifebound-v1\0"+b"".join(u64(v) for v in coordinates)+struct.pack(">q",row["After"]["Value"])+u64(row["After"]["Provenance"]))
    return answer.hexdigest()


def selector_timings(samples):
    aggregate={};out=[]
    for index,sample in enumerate(samples):
        stage=sample["name"];mode={0:"At-reopen",1:"Fresh",2:"Stable",3:"At",4:"After"}[MODES[index]]
        scoped="scope-" in stage
        for call,key in (("Open","OpenNanos"),("Scan","ScanNanos"),("Points","PointsNanos"),("ScopeRefusal","ScopeRefusalNanos")):
            if call=="Scan" and scoped or call=="ScopeRefusal" and not scoped:continue
            value=integer(sample[key],1)
            aggregate.setdefault((call,mode,scoped),[]).append(value)
    for (call,mode,scoped),values in sorted(aggregate.items()):
        values.sort()
        out.append({"call":call,"mode":mode,"scoped":scoped,"samples":len(values),**{f"p{p}Nanos":values[(len(values)*p+99)//100-1] for p in (50,95,99)}})
    return {"samples":samples,"nearestRank":out,"scope":"whole Open including native pin publication; complete Scan through EOF with renewal/retry waits and artifact callbacks; fixed scan-based Points and separate scope refusals; no indexed/per-key latency claim"}


def verify(directory,spool_directory,policy,*,binary_sha=None,expected_spec=None,expected_run=None,expected_deadline=None,expected_seconds=None,shared_spool=None):
    directory=Path(directory).resolve(strict=True)
    with (directory/"result.json").open("rb") as source:body=source.read(LINE+1)
    require(body.endswith(b"\n") and len(body)<=LINE,"bounded complete native terminal result")
    end=decoded(body[:-1]);s=shape(end["spec"]);a=end["Admission"]
    integer(a["Version"],1,1);integer(a["Topology"],1);integer(a["DeadlineUnixNanos"],1,(1<<63)-1)
    require(integer(end["version"],1,1) and end["format"]==FORMAT and end["engine"]=="native-raft" and end["FramesCeiling"]==frames(s)
            and end["RetentionContract"]=="snapshot-C-retained-full-prefix-v1","exact source family/retention/finite stage admission")
    require(a["Version"]==1 and a["Family"]==FAMILY and a["Spec"]==s and digest(encoded(a))==array(end["AdmissionSHA256"],32)
            and integer(a["BudgetSeconds"],1,MAX_SECONDS)*1000>integer(a["CausalMinimumMillis"]) and a["CausalMinimumMillis"]==5*a["Transport"]["targetRTTMillis"],"immutable actual graph/binary/run/budget/transport")
    require(type(a["Run"]) is str and len(a["Run"])==32 and bytes.fromhex(a["Run"]).hex()==a["Run"] and any(bytes.fromhex(a["Run"])),"nonzero actual run")
    for expected,actual in ((binary_sha,a["BinarySHA256"]),(expected_spec,s),(expected_run,a["Run"]),(expected_deadline,a["DeadlineUnixNanos"]),(expected_seconds,a["BudgetSeconds"])):require(expected is None or expected==actual,"controller/source admission mismatch")
    certs={name:cut(end[name]) for name in ("Initial","Old","Final","Reopened")}
    require(certs["Final"]==certs["Reopened"] and all(len(v)==2 for v in certs.values()),"full old/current restored native cuts")
    for group in range(2):
        initial=certs["Initial"][group]
        require(all(v[group]["scope"]==initial["scope"] and v[group]["configuration"]==initial["configuration"] for v in certs.values())
                and array(a["Graph"],16)==initial["scope"][:16] and a["Topology"]==int.from_bytes(initial["scope"][48:56],"big")
                and array(a["Declaration"],32)==initial["scope"][56:88],"same actual graph/topology/configuration")
    require(Path(end["ArtifactDirectory"]).resolve(strict=True)==directory and Path(end["Directory"]).resolve(strict=True).parent==directory,"owned artifact root")
    require(type(end["Completed"]) is list and len(end["Completed"])==32 and all(type(v) is int and v>=0 for v in end["Completed"]),"strict actual writer counters")
    require(integer(end["LogicalCross"])==2*(s["facts"]*s["crossPercent"]//100) and integer(end["LogicalLocal"])+end["LogicalCross"]==2*s["facts"]+3,"true business participant fraction, not remote endpoints or protocol traffic")
    require(all(type(end[n]) is int and end[n]>=0 for n in ("SetupNanos","BusinessNanos","DiagnosticNanos","TotalNanos","OriginalCaptures","ExtraInvocations","QueryExtraInvocations"))
            and end["SetupNanos"]+end["BusinessNanos"]+end["DiagnosticNanos"]==end["TotalNanos"] and end["BusinessNanos"]>0,"business/diagnostic timing distinction")
    selected=codec.owned_transport(end["Transport"],{"version":1,"family":codec.FAMILY,"facts":s["facts"],"crossPercent":s["crossPercent"],"writers":s["writers"],"layout":s["layout"]})
    require(selected==a["Transport"],"same actual scheduler before/after restart")
    for cohort in end["Transport"]:
        require(all(cohort[n]["Admitted"]<=frames(s)*1024 for n in ("Raft","SourceRequest","SourceReply"))
                and all(cohort[n]["AttemptedFrames"]<=frames(s)*1024 for n in ("PipeReads","PipeWrites")),"exact selectors frame grants")
    with (nullcontext(shared_spool) if shared_spool is not None else Spool(spool_directory,policy)) as spool:
        grants(directory,end,spool);claims(directory,end,spool)
        scalar._scalar_attempts(directory,end,spool)
        scalar._scalar_sources(directory,end,spool,cut,frames(s))
        timing=reads(directory,end,spool)
        scalar._scalar_columns(directory,end,spool,cut,frames(s),end["Old"])
        processes(directory,end,spool)
        answer=logical_answers(end,spool)
        count=2*s["facts"]+3;latency={}
        for index,(key,_) in enumerate(spool.walk(b"latency"),1):
            for percentile in (50,95,99):
                if index==(count*percentile+99)//100:latency[f"p{percentile}Nanos"]=int.from_bytes(key[:8],"big")
        require(len(latency)==3,"complete logical-operation percentile census")
        return {"spec":s,"run":a["Run"],"logicalAnswersSHA256":answer,"retentionContract":end["RetentionContract"],"transport":selected,"selectorTiming":selector_timings(timing),
                "timing":{**latency,"logicalOperations":count,"usefulTransactionsPerSecond":count*1_000_000_000/end["BusinessNanos"]},"spool":spool.report(),
                "scope":"actual native complete source/original/voter/selector/scope/column/cache/reopen joins; planned IDs use genuine installed-grant references, not Recipient.Next; allocator API/cost matching, capacity/full matrix/physical retention costs remain unqualified"}
