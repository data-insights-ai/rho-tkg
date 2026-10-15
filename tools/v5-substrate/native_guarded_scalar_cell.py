#!/usr/bin/env python3
"""Explicit guarded native feed/selectors controller; no builds or emulation.

Old direct controllers keep their original format. Fresh Spec2 uses the SAME
owned native packet/frame scheduler and finite watchdog process-tree contract.
"""
import argparse
import hashlib
import json
from pathlib import Path
import struct
import sys

import native_guarded_scalar_check as native
import native_feed_cell as feed_cell
import native_selectors_cell as selectors_cell
import native_workload_cell as common
from comparison_spool import Policy,Spool,u64

require=native.require
executables,bounded_json,digest=feed_cell.executables,common.bounded_json,common.digest
LOCAL={"kind":"local","targetRTTMillis":0,"jitterMillis":0,"seed":20261009}


def preflight(spec,transport,seconds,memory_mib,engine="native-raft",*,owned_fdb=False):
    native.shape(spec)
    require(engine in ("native-raft","paired") and transport in native.TRANSPORTS and (engine!="paired" or transport=="local"),"nonlocal paired complete original/transport join remains separate")
    require(type(seconds) is int and 1<=seconds<=native.MAX_SECONDS and type(memory_mib) is int and 4096<=memory_mib<=1<<20,"finite original budget/aggregate RSS admission")
    require(seconds*1000>5*native.TRANSPORTS[transport],"necessary real anchor/value traffic, not a performance forecast")
    require(engine!="paired" or owned_fdb or seconds<=1800,"ordinary FDB pair budget unchanged; longer pair requires actual owned FDB policy")
    return {**LOCAL,"kind":"local" if transport=="local" else "injected-rtt","targetRTTMillis":native.TRANSPORTS[transport]}


def execute(spec,transport,output,binary,watchdog,seconds,memory_mib,policy,*,run=None):
    native.shape(spec)
    base=feed_cell if spec["family"]=="snapshot-to-feed" else selectors_cell
    test="TestNativeGraphGuardedSnapshotFeedWorkloadCell" if spec["family"]=="snapshot-to-feed" else "TestNativeGraphGuardedSnapshotSelectorsWorkloadCell"
    return base._execute(spec,transport,output,binary,watchdog,seconds,memory_mib,policy,run=run,_format=native.FORMATS[spec["family"]],_verify=native.verify,_preflight=preflight,_test=test)


def normalized_native(row):
    a=row["After"];v=(row["Owner"],row["Life"],row["Source"],row["Target"],row["SourceLife"],row["TargetLife"],a["Handle"],a["Value"],a["Provenance"])
    require(a["Present"] is True and a["Null"] is False and all(type(i) is int for i in v) and all(0<i<1<<64 for i in v[:7]) and -(1<<63)<=v[7]<1<<63 and 0<v[8]<1<<64,"full actual native typed payload/lives")
    return v


def normalized_fdb(group):
    tx=group["Tx"];e,m=tx["GraphChange"]["Edge"],tx["Lifecycle"];d=m["Definition"]
    require(d["Kind"]=="i64" and type(d["ID"]) is type(d["I64"]) is int,"actual immutable FDB dictionary definition")
    return (e["ID"],m["EdgeLife"],e["Source"]["Node"],e["Target"]["Node"],e["Source"]["Life"],e["Target"]["Life"],d["ID"],d["I64"],m["Provenance"])


OWNED_TOOLS=frozenset("owned-tools/"+name for name in ("tc","ip","manifest.json","build.txt","elf.txt"))


def fdb_inventory(directory,hashes):
    require(type(hashes) is dict and 0<len(hashes)<=256,"finite actual launcher inventory")
    directory=Path(directory).resolve(strict=True);nested=set()
    for name,checksum in hashes.items():
        require(type(name) is str and name not in ("",".","..","receipt.json") and ("/" not in name or name in OWNED_TOOLS),"foreign/traversing launcher artifact")
        require(type(checksum) is str and len(checksum)==64 and bytes.fromhex(checksum).hex()==checksum,"canonical actual artifact digest")
        path=directory/name
        require(not path.is_symlink() and path.is_file() and not path.parent.is_symlink() and path.resolve(strict=True)==path,"owned regular launcher artifact, no path escape")
        require(digest(path)==checksum,"changed actual FDB artifact")
        if "/" in name:nested.add(name)
    require(not nested or nested==OWNED_TOOLS,"complete fixed owned tooling inventory; full FDB oracle authenticates its policy/binaries/transport")


def compare(native_directory,fdb_directory,spool_directory,policy):
    native_directory,fdb_directory=Path(native_directory).resolve(strict=True),Path(fdb_directory).resolve(strict=True)
    receipt,request=bounded_json(native_directory/"receipt.json"),bounded_json(native_directory/"request.json");s=native.shape(request["spec"])
    base=feed_cell if s["family"]=="snapshot-to-feed" else selectors_cell
    if s["family"]=="snapshot-to-feed":import fdb_feed_check as fdb
    else:import fdb_selectors_check as fdb
    require(receipt["format"]==request["format"]==native.FORMATS[s["family"]] and receipt["completedCell"] is True and receipt["exit"]==0 and receipt["requestSHA256"]==digest(native_directory/"request.json") and receipt["spec"]==s and receipt["run"]==request["run"] and receipt["binarySHA256"]==request["binarySHA256"] and request["transport"]==LOCAL and receipt["processTreeSHA256"]==digest(native_directory/"watchdog.json") and receipt["processTree"]==bounded_json(native_directory/"watchdog.json",65536) and receipt["artifacts"]==base.hash_artifacts(native_directory/"engine",s["layout"]),"complete actual local native process/cleanup/source binding")
    r=bounded_json(fdb_directory/"receipt.json");require(r["success"] is True and r["exit"]==r["cleanup_exit"]==0 and r["mode"]=="workload-cell","actual FDB owned launcher cleanup")
    stem="fdb-feed-" if s["family"]=="snapshot-to-feed" else "fdb-selectors-"
    required={stem+n for n in ("result.json","attempts.jsonl","events.jsonl","latency.jsonl","history.jsonl","current.jsonl","cdc.jsonl")}
    if s["layout"]=="columns":required.add(stem+"columns.jsonl")
    require(required<=r["sha256"].keys(),"complete authentic FDB source inventory")
    fdb_inventory(fdb_directory,r["sha256"])
    with Spool(spool_directory,policy) as spool:
        left,right=common.NamespacedSpool(spool,b"native/"),common.NamespacedSpool(spool,b"fdb/")
        a=native.verify(native_directory/"engine",spool_directory,policy,binary_sha=request["binarySHA256"],expected_spec=s,expected_run=request["run"],expected_deadline=request["deadlineUnixNanos"],expected_seconds=request["seconds"],shared_spool=left)
        b=fdb.verify(fdb_directory,spool_directory,policy,shared_spool=right)
        require(b["spec"]["version"] in (1,2) and {**b["spec"],"version":2}==s and b["runGraph"].endswith("-"+request["run"]),"same exact dimensions/run; explicit nativeSpec2 vs genuine FDB profile1/2")
        answer=hashlib.sha256();count=0
        for offset in (0,s["facts"]):
            for fact in range(s["facts"]):
                ordinal=4+offset+fact;owner=7+2*fact
                row=native.decoded(left.lookup(b"row",u64(ordinal)+u64(owner)+b"\3"));original=fdb.decoded(right.lookup(b"fdb-operation",u64(ordinal)))
                x,y=normalized_native(row),normalized_fdb(original);require(x==y,"every actual old/current identity/life/endpoint/dictionary/value/provenance, not a label or hash approximation")
                _,_,effects,_=fdb.codec.fdb_contract(b["spec"],ordinal)
                for group in sorted(effects):
                    for effect in effects[group]:
                        key=fdb.key({"Graph":b["runGraph"]},group,"current",effect["Key"]);namespace=b"fdb-current" if offset else b"fdb-old"
                        actual=right.lookup(namespace,key);observation=b"fresh-current" if offset else b"racing-old" if s["family"]=="snapshot-to-feed" else b"old-fresh"
                        require(right.lookup(b"snapshot-seen-"+observation,key)==actual,"both full observed historical/current exact sets")
                        if offset and s["family"]=="snapshot-to-feed":require(right.lookup(b"feed-replica",key)==actual,"complete FDB feed after-image")
                if offset and s["family"]=="snapshot-to-feed":
                    require(left.lookup(b"feed-update",u64(fact))==native.encoded(row),"native acknowledged property row is actual original")
                    left.lookup(b"feed-presence",u64(fact))
                answer.update(b"edge-i64-lifebound-v1\0"+b"".join(u64(v) for v in (fact,*x[:7]))+struct.pack(">q",x[7])+u64(x[8]));count+=1
        require(count==2*s["facts"] and answer.hexdigest()==a["logicalAnswersSHA256"]==b["logicalAnswersSHA256"] and a["retentionContract"]==b["retentionContract"],"complete normalized exact-set parity after both independent raw oracles")
        return {"version":2,"family":s["family"],"spec":s,"run":request["run"],"equal":True,"logicalAnswersSHA256":answer.hexdigest(),"native":a,"foundationdb":b,"spool":spool.report(),"scope":"actual local guarded-endpoint retained-C complete answer/feed/original/column parity; native installed-grant references differ from FDB allocator APIs; no nonlocal paired completeness, equal physical costs, capacity or full V2 acceptance"}


def main():
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest="mode",required=True)
    run=sub.add_parser("run");run.add_argument("--spec",required=True);run.add_argument("--transport",choices=native.TRANSPORTS,required=True);run.add_argument("--output",required=True);run.add_argument("--binary",required=True);run.add_argument("--watchdog",default=str(Path(__file__).with_name("process_watchdog.py")));run.add_argument("--seconds",type=int,required=True);run.add_argument("--memory-mib",type=int,required=True)
    pair=sub.add_parser("compare");pair.add_argument("--native",required=True);pair.add_argument("--fdb",required=True);pair.add_argument("--output",required=True)
    for p in (run,pair):p.add_argument("--spool-disk-bytes",type=int,required=True);p.add_argument("--spool-cache-bytes",type=int,required=True)
    args=parser.parse_args();policy=Policy(args.spool_disk_bytes,args.spool_cache_bytes,4096,native.LINE)
    if args.mode=="run":print(execute(json.loads(args.spec),args.transport,args.output,args.binary,args.watchdog,args.seconds,args.memory_mib,policy))
    else:
        output=Path(args.output);output.mkdir(mode=0o700,parents=False,exist_ok=False)
        with (output/"paired.json").open("x") as out:json.dump(compare(args.native,args.fdb,output,policy),out,indent=2);out.write("\n")


if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,TypeError,OSError) as error:raise SystemExit("guarded native scalar refused: "+str(error)) from error
