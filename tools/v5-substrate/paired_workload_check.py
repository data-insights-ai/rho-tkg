#!/usr/bin/env python3
"""Complete owned local/RTT pair joins; source-authored, not measured acceptance.

Each full oracle authenticates its own native originals, observed state, clock,
transport and process lifetime. Only explicit logical roles cross the engines;
physical winning IDs, revisions, allocator doors and reclamation differ.
"""
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import struct

import native_workload_cell as common
import native_guarded_scalar_cell as guarded_cell
import fdb_selectors_check as scalar_raw
from comparison_spool import Spool,u64

require=common.require
ROOT=Path(__file__).resolve().parent
TRANSPORTS=common.TRANSPORTS
SCALARS=("partition-local-create-update","immutable-columns","late-correction-old-index")
FAMILIES=("contention-empty-range","cross-edge-endpoint-uniqueness","snapshot-to-feed","snapshot-selectors",*SCALARS)
FORMAT="owned-native-fdb-complete-pair-v1"


def registry(spec):
    family=spec["family"]
    require(family in FAMILIES,"actual native transfer Cost12 driver/schema is still required; no nominal pair")
    if family=="contention-empty-range":
        import native_contention_check as n
        import native_workload_cell as cell
        path=ROOT/"fdb/fdb_contention_check.py"
        module=importlib.util.spec_from_file_location("paired_fdb_contention",path)
        f=importlib.util.module_from_spec(module);module.loader.exec_module(f)
    elif family=="cross-edge-endpoint-uniqueness":
        import native_edge_unique_check as n
        import native_edge_unique_cell as cell
        import fdb_edge_unique_check as f
    elif family in ("snapshot-to-feed","snapshot-selectors") and spec["version"]==2:
        import native_guarded_scalar_check as n
        import native_guarded_scalar_cell as cell
        if family=="snapshot-to-feed":import fdb_feed_check as f
        else:import fdb_selectors_check as f
    elif family=="snapshot-to-feed" and spec["version"]==3:
        import native_feed_retained_check as n
        import native_feed_retained_cell as cell
        import fdb_feed_retained_check as f
    elif family=="partition-local-create-update":
        import native_create_update_check as n
        import native_create_update_cell as cell
        import fdb_create_update_check as f
    elif family=="immutable-columns":
        import native_immutable_columns_check as n
        import native_immutable_columns_cell as cell
        import fdb_columns_check as f
    elif family=="late-correction-old-index":
        import native_late_correction_check as n
        import native_late_correction_cell as cell
        import fdb_correction_check as f
    else:raise ValueError("bare-Set legacy feed/selectors are not the guarded business contract")
    n.shape(spec)
    return n,cell,f


def native_inventory(spec,cell,directory):
    if spec["family"] in ("snapshot-to-feed","snapshot-selectors") and spec["version"]==2:
        base=cell.feed_cell if spec["family"]=="snapshot-to-feed" else cell.selectors_cell
        return base.hash_artifacts(directory,spec["layout"])
    if spec["family"] in ("immutable-columns","late-correction-old-index"):
        return cell.common.hash_artifacts(directory,spec["layout"],family=spec["family"])
    return cell.hash_artifacts(directory,spec["layout"])


def fdb_spec(spec):
    registry(spec)
    return {**spec,"version":3 if spec["version"]==3 else 2}


def declared_transport(name):
    require(name in TRANSPORTS,"canonical actual added-delay selection")
    return {"kind":"local" if name=="local" else "injected-rtt","targetRTTMillis":TRANSPORTS[name],"jitterMillis":0,"seed":20261009}


def validate_transport(selected):
    require(type(selected) is dict and set(selected)=={"kind","targetRTTMillis","jitterMillis","seed"} and all(type(selected[k]) is int for k in ("targetRTTMillis","jitterMillis","seed")) and selected in [declared_transport(name) for name in TRANSPORTS],"strict canonical actual owned delay, booleans/nominal labels refused")
    return selected


TRANSPORT_ARTIFACTS={"transport-"+name for name in ("admission.json","input.json","commands.jsonl","baseline.json","armed.json","terminal.json","restored.json","samples.jsonl","rss.jsonl","complete.json","probes-before.json","probes-armed.json","probes-after.json","probe")} | {"transport-helper-"+role+".jsonl" for role in ("zone0","zone1","zone2","zone3","client")} | guarded_cell.OWNED_TOOLS


def fdb_artifacts(spec):
    family=spec["family"]
    if spec["version"]==3:return {"fdb-feed-retained-end.json","fdb-feed-retained-events.jsonl"}
    if family=="cross-edge-endpoint-uniqueness":
        return {"fdb-workload-"+s for s in ("result.json","attempts.jsonl","latency.jsonl","views.jsonl")} | {"fdb-cost-"+s+".jsonl" for s in ("history","current","cdc")} | ({"fdb-workload-columns.jsonl"} if spec["layout"]=="columns" else set())
    stem={"contention-empty-range":"workload","snapshot-to-feed":"feed","snapshot-selectors":"selectors","partition-local-create-update":"create-update","immutable-columns":"columns","late-correction-old-index":"correction"}[family]
    names={"result.json","attempts.jsonl","latency.jsonl","history.jsonl","current.jsonl","cdc.jsonl"}
    names.add("operations.jsonl" if family=="contention-empty-range" else "events.jsonl")
    if spec["layout"]=="columns":names.add("columns.jsonl")
    if family=="late-correction-old-index":names.add("indexes.jsonl")
    return {"fdb-"+stem+"-"+s for s in names}


def transport_join(spec,run,seconds,selected,native_proof,fdb_proof,end):
    validate_transport(selected)
    require(native_proof["transport"]==selected and native_proof["run"]==run,"native full oracle binds actual two-cohort scheduler/run")
    p=end["Transport"]["Policy"]
    require(p["Spec"]==fdb_spec(spec) and bytes(p["Run"]).hex()==run and p["Transport"]==selected and type(p["BudgetSeconds"]) is int and p["BudgetSeconds"]==seconds,"same admitted useful workload/run/chosen budget/configured delay, not observed latency equality")
    require(type(p["DeadlineUnixNanos"]) is type(p["AdmittedUnixNanos"]) is int and p["DeadlineUnixNanos"]-p["AdmittedUnixNanos"]==seconds*1_000_000_000,"FDB own original absolute lifetime; no restart renewal")
    canonical=json.dumps(p,separators=(",",":"),ensure_ascii=False).encode()
    actual=fdb_proof["transport"]
    require(actual["policySHA256"]==hashlib.sha256(canonical).hexdigest() and actual["configuredAddedOneWayMicros"]==selected["targetRTTMillis"]*500 and actual["drops"]==0,"actual native-stored FDB policy joined to full kernel/tool/probe/process/resource EOF verifier")
    return {"configured":selected,"FDBPolicySHA256":actual["policySHA256"],"FDBOriginalDeadlineUnixNanos":p["DeadlineUnixNanos"],"FDBPolicy":p,"foundationdb":actual,"scope":"native queue/frame scheduler and FDB receiver-ingress netem are different mechanisms; real baseline/armed/after probes remain separate, no equal observed RTT or physical-host-loss claim"}


def scalar_answers(spec,run,native,fdb,nspool,fspool):
    # Both full raw oracles have already authenticated these exact native rows,
    # immutable original bodies and complete old/current observations.
    answer=hashlib.sha256();n=spec["facts"];retained=spec["version"]==3
    for updated in (False,True):
        for fact in range(n):
            changed=updated and (spec["family"]!="late-correction-old-index" or fact<n//2)
            ordinal=4+fact+(n if changed else 0);owner=7+2*fact
            row=native.decoded(nspool.lookup(b"row",u64(ordinal)+u64(owner)+b"\3"))
            if retained:
                group=fdb.decoded(fspool.lookup(fdb.ns("group-ordinal"),u64(ordinal)))
            else:group=fdb.decoded(fspool.lookup(b"fdb-operation",u64(ordinal)))
            a,b=guarded_cell.normalized_native(row),guarded_cell.normalized_fdb(group)
            require(a==b,"complete actual typed scalar identity/life/endpoints/dictionary/value/provenance pair")
            presence=native.decoded(nspool.lookup(b"row",u64(ordinal)+u64(owner)+b"\1"))
            require(presence["After"]["Present"] is True and presence["After"]["Handle"]==a[1] and presence["After"]["Provenance"]==ordinal,"native exact same-life guarded presence revision is not silently discarded")
            if changed:
                require(row["Before"]["Present"] is True and row["Before"]["Value"]==5 and row["Before"]["Provenance"]==4+fact and presence["Before"]["Present"] is True and presence["Before"]["Handle"]==presence["After"]["Handle"] and presence["Before"]["Provenance"]==4+fact,"native complete actual5->6 and SAME-life guard before-images")
            for write in group["Writes"]:
                after=write["After"];key=scalar_raw.key({"Graph":run},write["Group"],"current",write["Key"])
                require(after["Deleted"] is False and after["Value"]==a[7],"actual FDB identity/out/in after-image equals typed value")
                if changed:require(write["Before"]["Deleted"] is False and write["Before"]["Value"]==row["Before"]["Value"]==5,"every actual acknowledged/update FDB predecessor equals native complete before-image")
                if retained:
                    stage="current" if updated else "snapshot-after-update"
                    require(fspool.lookup(fdb.ns("snapshot/"+stage),key)==fdb.go_json(after),"retained-only actual old/current observed snapshot EOF, not detached expected state")
                    if updated:require(fspool.lookup(fdb.ns("replica"),key)==fdb.go_json(after),"actual complete retained-only ACK after-image")
                else:
                    namespace=b"fdb-current" if updated else b"fdb-old"
                    observation=b"fresh-current" if updated else b"racing-old" if spec["family"]=="snapshot-to-feed" else b"old-fresh"
                    require(fspool.lookup(namespace,key)==fspool.lookup(b"snapshot-seen-"+observation,key)==scalar_raw.go_json(after),"actual complete historical/current physical cell plus observed EOF")
                    if updated and spec["family"]=="snapshot-to-feed":require(fspool.lookup(b"feed-replica",key)==scalar_raw.go_json(after),"FDB complete acknowledged retained-C after-image")
            if spec["family"]=="snapshot-to-feed" and updated:
                require(nspool.lookup(b"feed-update",u64(fact))==native.encoded(row) and nspool.lookup(b"feed-presence",u64(fact))==native.encoded(presence),"native complete acknowledged property+presence envelope")
                if retained:
                    o=group["Outcome"]
                    # The actual graph namespace is supplied in the original;
                    # run alone is never a native namespace or authority.
                    key=fdb.prefix({"Graph":group["Tx"]["Graph"]},o["Coordinator"])+f'changes/{o["Sequence"]:016x}/'.encode()+o["Request"].encode().hex().encode()
                    require(fspool.lookup(fdb.ns("feed-consumed"),key)==fdb.go_json(group),"FDB complete acknowledged detached original")
            answer.update(b"edge-i64-lifebound-v1\0"+b"".join(u64(v) for v in (fact,*a[:7]))+struct.pack(">q",a[7])+u64(a[8]))
    return answer.hexdigest(),2*n


def edge_answers(spec,native,fdb,nspool,fspool):
    answer=hashlib.sha256();n=spec["facts"]
    for updated in (False,True):
        nspace,fspace=(b"state",b"current") if updated else (b"old-state",b"old")
        for fact in range(n):
            nw=int.from_bytes(nspool.lookup(b"winner",u64(fact)),"big")
            fw=int.from_bytes(fspool.lookup(b"old-winner",u64(fact)),"big")
            nc,fc=(1-nw,1-fw) if updated else (nw,fw)
            owner=7+4*fact+2*nc;row=native.decoded(nspool.lookup(nspace,u64(owner)+u64(owner+1)+b"\3p"))
            presence=native.state_cell(nspool,nspace,owner)
            e=fdb.edge(spec,fact,fc,updated);actual=fdb.current(fspool,0,fdb.edge_key("edge",e),fspace)
            require(actual is not None and actual["Deleted"] is False and presence["Present"] is True and presence["Handle"]==owner+1,"both genuine effective unique winners, no descriptive role marker")
            require((row["Source"],row["SourceLife"],row["Target"],row["TargetLife"],row["After"]["Value"])==(e["Source"]["Node"],e["Source"]["Life"],e["Target"]["Node"],e["Target"]["Life"],actual["Value"]),"actual complete old/current endpoint-new-life ABA and scalar role pair")
            require(row["After"]["Present"] is True and row["After"]["Null"] is False,"no phantom/null paired scalar")
            # Unique winner physical IDs may differ; full independent views
            # already prove both candidates, index claims and old-incumbent close.
            answer.update(u64(fact)+u64(row["Target"])+u64(row["TargetLife"])+struct.pack(">q",actual["Value"]))
    return answer.hexdigest(),2*n


def contention_answers(spec,native,fdb,nspool,fspool):
    answer=hashlib.sha256();n=spec["facts"]
    for namespace in (b"old-state",b"state"):
        for fact in range(n):
            owner=7+2*fact;wire=nspool.find(namespace,u64(owner)+u64(owner+1)+u64(3)+b"p")
            cell=None if wire is None else native.decoded(wire)
            actual=fspool.find(namespace,u64(0)+fdb.fixture_edge_key(spec,fact))
            other=None if actual is None else fdb.decoded(actual)
            require((cell is None)==(other is None),"actual exact old absence/current claim coverage")
            answer.update(u64(fact)+bytes([native.cross(spec,fact),cell is not None]))
            if cell is not None:
                row=cell;after=row["After"]
                require(after["Present"] is True and after["Null"] is False and other["Deleted"] is False and after["Value"]==other["Value"],"actual complete hot RMW/claim scalar, no expected label")
                require((row["Owner"],row["Life"],row["Source"],row["SourceLife"])==(owner,owner+1,1,2) and row["Target"]==(5*n+65 if native.cross(spec,fact) else 3) and row["TargetLife"]==row["Target"]+1,"actual native life-bound endpoint descriptors")
                answer.update(struct.pack(">q",after["Value"]))
    return answer.hexdigest(),2*n


def verify(native_directory,fdb_directory,spool_directory,policy):
    native_directory,fdb_directory=Path(native_directory).resolve(strict=True),Path(fdb_directory).resolve(strict=True)
    request=common.bounded_json(native_directory/"request.json");receipt=common.bounded_json(native_directory/"receipt.json")
    spec=request["spec"];native,cell,fdb=registry(spec);selected=request["transport"]
    validate_transport(selected)
    require(receipt["completedCell"] is True and receipt["exit"]==0 and receipt["requestSHA256"]==common.digest(native_directory/"request.json") and all(receipt[k]==request[k] for k in ("format","spec","run","binarySHA256")),"actual original controller/native binary/spec/run receipt")
    meter=common.bounded_json(native_directory/"watchdog.json",65536)
    require(receipt["processTree"]==meter and receipt["processTreeSHA256"]==common.digest(native_directory/"watchdog.json") and meter["exit_code"]==0 and meter["reason"]=="completed" and meter["all_owned_exited"] is True and meter["deadline_unix_nanos"]==request["deadlineUnixNanos"] and meter["wall_limit_s"]==request["seconds"] and meter["rss_limit_mib"]==request["aggregateRSSMiB"] and meter["command"]==request["command"][request["command"].index("--")+1:],"actual all-parent/voter/helper process ownership and fixed original deadline")
    require(type(request["aggregateRSSMiB"]) is int and type(meter["peak_tree_rss_mib"]) in (int,float) and math.isfinite(meter["peak_tree_rss_mib"]) and 0<meter["peak_tree_rss_mib"]<=request["aggregateRSSMiB"],"actual finite aggregate native process RSS")
    require(receipt["artifacts"]==native_inventory(spec,cell,native_directory/"engine"),"every complete native artifact unchanged")
    fr=common.bounded_json(fdb_directory/"receipt.json")
    require(fr["success"] is True and fr["exit"]==fr["cleanup_exit"]==0 and fr["mode"]=="workload-cell","real owned FDB launcher/helper terminal")
    require((fdb_artifacts(spec)|TRANSPORT_ARTIFACTS)<=fr["sha256"].keys(),"every actual full-oracle FDB family input is launcher hash-bound")
    guarded_cell.fdb_inventory(fdb_directory,fr["sha256"])
    stem="fdb-workload-" if spec["family"] in ("contention-empty-range","cross-edge-endpoint-uniqueness") else "fdb-feed-retained-" if spec["version"]==3 else "fdb-feed-" if spec["family"]=="snapshot-to-feed" else "fdb-selectors-" if spec["family"]=="snapshot-selectors" else "fdb-create-update-" if spec["family"]=="partition-local-create-update" else "fdb-columns-" if spec["family"]=="immutable-columns" else "fdb-correction-"
    end=common.bounded_json(fdb_directory/("fdb-feed-retained-end.json" if spec["version"]==3 else stem+"result.json"));actual_spec=end.get("Spec",end.get("spec"))
    require(actual_spec==fdb_spec(spec),"exact fresh owned FDB native family profile; normalization never relabels wire")
    native_end=common.bounded_json(native_directory/"engine/result.json")
    with Spool(spool_directory,policy) as spool:
        left,right=common.NamespacedSpool(spool,b"native/"),common.NamespacedSpool(spool,b"fdb/")
        a=native.verify(native_directory/"engine",spool_directory,policy,binary_sha=request["binarySHA256"],expected_spec=spec,expected_run=request["run"],expected_deadline=request["deadlineUnixNanos"],expected_seconds=request["seconds"],shared_spool=left)
        b=fdb.verify(fdb_directory,spool_directory,policy,shared_spool=right)
        joined=transport_join(spec,request["run"],request["seconds"],selected,a,b,end)
        graph=end["Config"]["Graph"];require(graph.endswith("-"+request["run"]),"genuine FDB graph/run prefix authenticated by its full oracle")
        if spec["family"]=="cross-edge-endpoint-uniqueness":answers,count=edge_answers(spec,native,fdb,left,right)
        elif spec["family"]=="contention-empty-range":answers,count=contention_answers(spec,native,fdb,left,right)
        else:answers,count=scalar_answers(spec,graph,native,fdb,left,right)
        require(count==2*spec["facts"] and answers==a["logicalAnswersSHA256"] and (spec["family"]=="contention-empty-range" or answers==b["logicalAnswersSHA256"]),"complete actual typed answer enumeration before digest")
        if spec["version"]==3:
            require(a["minimumContract"]==b["minimumContract"]=={"completeOldCurrent":True,"snapshotTokenReopenRefused":True,"completeUpdates":spec["facts"],"unreadAfterRealGC":True,"samePageAckAfterClientReplacement":True},"same exact minimum retained-only outputs, not equal physical reclamation")
        elif spec["family"]!="contention-empty-range" and spec["family"]!="cross-edge-endpoint-uniqueness":require(a["retentionContract"]==b["retentionContract"],"same actual retained-C observable promise")
        return {"format":FORMAT,"spec":spec,"FDBSpec":actual_spec,"run":request["run"],"equal":True,"typedAnswers":count,"logicalAnswersSHA256":answers,"transport":joined,"nativeOriginalDeadlineUnixNanos":request["deadlineUnixNanos"],"nativeBinarySHA256":request["binarySHA256"],"nativeProcessTree":meter,"nativeTransportRecords":native_end["Transport"],"nativeStoredCounters":{k:native_end[k] for k in ("PersistedFileBytes","AllocatedFileBytes","SetupNanos","BusinessNanos","DiagnosticNanos","TotalNanos") if k in native_end},"nativeAllInControllerNanos":receipt["elapsedNanos"],"native":a,"foundationdb":b,"spool":spool.report(),"costBoundary":{"sameConfiguredAddedRTT":True,"equalObservedRTT":False,"equalPhysicalRetention":False,"allocator":"native installed-grant references versus FDB actual per-family issuance/fixed-ID APIs; each full source cost remains explicit","lifetime":"each engine retains its own immutable original absolute deadline and process/retry budgets under the SAME chosen seconds/run","physicalWinningIDs":"contention/unique winners may differ, both complete engine-specific original/index/absence proofs retained"},"scope":"complete source-implemented owned nonlocal pair; no measured efficiency, capacity, independent-host-loss or full V2 acceptance; native transfer pairing remains required after actual Cost12 driver integration"}
