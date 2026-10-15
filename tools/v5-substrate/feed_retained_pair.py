#!/usr/bin/env python3
"""Matched minimum observable feed contract; physical retention remains distinct.

No oracle success is inferred from controller labels or copied feed recipes.
Each engine's full raw verifier runs, then every actual typed old/current answer
and complete acknowledged update is joined within ONE bounded scratch spool.
"""
import argparse
import hashlib
import json
from pathlib import Path
import secrets
import sys

import fdb_feed_retained_check as fdb
import native_feed_retained_check as native
import native_feed_retained_cell as native_cell
from comparison_spool import Policy, Spool, u64

require=fdb.require
FORMAT="matched-retained-only-feed-minimum-v1"


def preflight(spec,transport,seconds,memory,policy,owned_options,owner_grant):
    native.shape(spec);fdb.shape(spec)
    require(transport=="local","paired nonlocal retained-only needs a separately complete compatible transport join")
    native_cell.preflight(spec,transport,seconds,memory)
    policy.validate();require(policy.max_key_bytes==4096 and policy.max_value_bytes==512<<10,"same finite verifier record admission")
    fdb.validate_grant(owner_grant,spec)
    shaped={"kind":"local","targetRTTMillis":0,"jitterMillis":0,"seed":20261009}
    fdb.transport.validate_input({"Version":3,"Spec":spec,"Run":"01"*16,"Transport":shaped,"BudgetSeconds":seconds,**owned_options,"ToolsDirectory":"@pinned-prebuild"})
    return shaped


def normalized_native(row):
    after=row["After"]
    require(after["Present"] is True and after["Null"] is False and type(after["Handle"]) is int and type(after["Value"]) is int,"actual native typed payload")
    return (row["Owner"],row["Life"],row["Source"],row["Target"],row["SourceLife"],row["TargetLife"],after["Handle"],after["Value"],after["Provenance"])


def normalized_fdb(group):
    x=group["Tx"];edge=x["GraphChange"]["Edge"];life=x["Lifecycle"];v=life["Definition"]
    require(v["Kind"]=="i64" and type(v["ID"]) is type(v["I64"]) is int,"actual FDB typed value definition")
    return (edge["ID"],life["EdgeLife"],edge["Source"]["Node"],edge["Target"]["Node"],edge["Source"]["Life"],edge["Target"]["Life"],v["ID"],v["I64"],life["Provenance"])


def compare(native_directory,fdb_directory,spool_directory,policy):
    native_directory=Path(native_directory).resolve(strict=True);fdb_directory=Path(fdb_directory).resolve(strict=True)
    with Spool(spool_directory,policy) as spool:
        left=native.verify(native_directory,spool_directory,policy,shared_spool=spool)
        right=fdb.verify(fdb_directory,spool_directory,policy,shared_spool=spool)
        s=left["spec"];require(s==right["spec"] and left["run"]==right["run"] and left["transport"]=={"kind":"local","targetRTTMillis":0,"jitterMillis":0,"seed":20261009},"same actual minimum-contract dimensions/run/local native scheduler")
        require(right["transport"]["configuredAddedOneWayMicros"]==0,"actual owned FDB local transport, no nonlocal relabel")
        expected={"completeOldCurrent":True,"snapshotTokenReopenRefused":True,"completeUpdates":s["facts"],"unreadAfterRealGC":True,"samePageAckAfterClientReplacement":True}
        require(left["minimumContract"]==right["minimumContract"]==expected,"both complete independently verified observable contracts")
        digest=hashlib.sha256();count=0
        for offset in (0,s["facts"]):
            for fact in range(s["facts"]):
                ordinal=4+offset+fact;owner=7+2*fact
                row=native.decoded(spool.lookup(b"row",u64(ordinal)+u64(owner)+b"\3"))
                group=fdb.decoded(spool.lookup(fdb.ns("group-ordinal"),u64(ordinal)))
                a,b=normalized_native(row),normalized_fdb(group);require(a==b,"complete actual typed old/current identity/life/endpoints/value/provenance parity")
                if offset:
                    spool.lookup(b"feed-update",u64(fact));spool.lookup(b"feed-presence",u64(fact))
                    o=group["Outcome"];ck=fdb.prefix({"Graph":right["runGraph"]},o["Coordinator"])+f'changes/{o["Sequence"]:016x}/'.encode()+o["Request"].encode().hex().encode()
                    require(spool.lookup(fdb.ns("feed-consumed"),ck)==fdb.go_json(group) and row["Before"]["Present"] is True and row["Before"]["Value"]==5 and all(w.get("Before") is not None and w["Before"]["Value"]==5 and w["After"]["Value"]==6 for w in group["Writes"]),"actual full acknowledged5->6 predecessor parity, no hash-only shortcut")
                digest.update(b"edge-i64-lifebound-v1\0"+b"".join(u64(v) for v in (fact,*a[:7]))+fdb.struct.pack(">q",a[7])+u64(a[8]));count+=1
        require(count==2*s["facts"] and digest.hexdigest()==left["logicalAnswersSHA256"]==right["logicalAnswersSHA256"],"all full exact-set typed answers, no partial digest approximation")
        return {"format":FORMAT,"spec":s,"run":left["run"],"minimumContract":expected,"logicalAnswersSHA256":digest.hexdigest(),"native":left,"foundationdb":right,"spool":spool.report(),"costBoundary":{"equalPhysicalGC":False,"nativeAvailability":"rolling ACK predecessor/unread prefix; generic At follows actual retained native floor","fdbAvailability":"detached CDC/paired originals/certificates; relinquished physical C refuses at native source floor","allocator":"native installed-grant-reference factory versus genuine FDB HOME/recipient session and local Next; not an allocator efficiency verdict","timing":"business-only whole operation retry/wait and phase walls; actual engine/helper monitor, initialization, cleanup, checker and spool costs reported separately"},"scope":"one completed local minimum-contract cell after both complete independent raw oracles; no nonlocal parity, equal storage mechanism, capacity or full V2 acceptance"}


def execute(spec,transport,output,binary,watchdog,seconds,memory,policy,*,profile,owned_options,owner_grant,run=None):
    preflight(spec,transport,seconds,memory,policy,owned_options,owner_grant)
    require(profile in ("small","large") and memory>=(3072 if profile=="small" else 10240),"complete FDB resource admission before either engine")
    binary,watchdog=native_cell.executables(binary,watchdog)
    run=secrets.token_hex(16) if run is None else run
    require(type(run) is str and len(run)==32 and bytes.fromhex(run).hex()==run and any(bytes.fromhex(run)),"one original paired run")
    requested=Path(output);require(requested.is_absolute() and not requested.is_symlink(),"fresh absolute owned pair directory")
    parent=requested.parent.resolve(strict=True);destination=parent/requested.name;destination.mkdir(mode=0o700,parents=False,exist_ok=False)
    native_cell.execute(spec,transport,destination/"native",binary,watchdog,seconds,memory,policy,run=run)
    # Import only at actual dispatch; this module grants neither build nor SDK
    # authority on import. The ordinary matrix path/default remains unchanged.
    import workload_matrix as matrix
    cell={"version":1,"engine":"foundationdb","workload":spec,"transport":{"kind":"local","targetRTTMillis":0,"jitterMillis":0,"seed":20261009},"repetition":1,"implemented":True,"scope":"explicit fresh retained-only source workload"}
    matrix.execute(cell,destination/"fdb",seconds,profile,memory,policy.max_disk_bytes,policy.cache_bytes,"explicit-caller",owned_options=owned_options,retained_policy=owner_grant,run_override=run)
    with (destination/"fdb/verified.json").open("rb") as source:body=source.read((512<<10)+1)
    require(len(body)<=512<<10,"bounded actual FDB controller result");verified=json.loads(body)
    result=compare(destination/"native/engine",verified["artifacts"],destination,policy)
    with (destination/"verified.json").open("x") as out:json.dump(result,out,indent=2);out.write("\n")
    return destination


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("native");p.add_argument("foundationdb");p.add_argument("owned_spool");p.add_argument("--disk-bytes",type=int,required=True);p.add_argument("--cache-bytes",type=int,required=True)
    a=p.parse_args()
    try:print(json.dumps(compare(a.native,a.foundationdb,a.owned_spool,Policy(a.disk_bytes,a.cache_bytes,4096,512<<10)),indent=2))
    except (ValueError,KeyError,TypeError,OSError) as error:raise SystemExit("retained-only pair refused: "+str(error)) from error
