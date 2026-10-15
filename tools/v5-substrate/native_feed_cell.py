#!/usr/bin/env python3
"""One supplied native snapshot/feed workload binary and owned process lifetime.

No builds or SDK substitutes. The native retained-C contract is explicit; paired
nonlocal and feed-only retention comparisons refuse before engine effects.
"""
import argparse
import json
import math
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time

import native_feed_check as native
import native_workload_cell as common
from comparison_spool import Policy, Spool

require, digest, bounded_json = native.require, common.digest, common.bounded_json
ARTIFACTS={"result.json","grants.jsonl","attempts.jsonl","operations.jsonl","feed.jsonl","points.jsonl","sources.jsonl","processes.jsonl","columns.jsonl"}


def executables(binary,watchdog):
    return common.executables(binary,Path(__file__).with_name("process_watchdog.py") if watchdog is None else watchdog)


def preflight(spec,transport,seconds,memory_mib,engine="native-raft"):
    native.shape(spec)
    require(engine in ("native-raft","paired") and transport in native.TRANSPORTS and (engine!="paired" or transport=="local"),"paired nonlocal needs compatible authenticated transport/original joins")
    require(type(seconds) is int and 1<=seconds<=native.MAX_SECONDS and type(memory_mib) is int and 4096<=memory_mib<=1<<20,"explicit finite original whole-run policy")
    require(seconds*1000>5*native.TRANSPORTS[transport],"caller budget cannot satisfy necessary sequential anchor/definition RTT delay")
    require(engine!="paired" or seconds<=1800,"ordinary paired FDB duration contract unchanged")
    return {"kind":"local" if transport=="local" else "injected-rtt","targetRTTMillis":native.TRANSPORTS[transport],"jitterMillis":0,"seed":20261009}


def hash_artifacts(directory,layout):
    require(layout in ("kv","columns"),"exact derived layout")
    found={};roots=0
    with os.scandir(directory) as entries:
        for e in entries:
            require(not e.is_symlink(),"foreign source artifact link")
            if e.is_dir(follow_symlinks=False):
                roots+=1;require(e.name.startswith("rho-native-feed-") and roots==1,"one actual six-voter directory")
            else:
                require(e.is_file(follow_symlinks=False) and e.name in ARTIFACTS,"foreign source artifact")
                found[e.name]=digest(e.path)
    require(found.keys()==ARTIFACTS and roots==1,"complete genuine native artifact inventory")
    return found


def execute(spec,transport,output,binary,watchdog,seconds,memory_mib,policy,*,run=None):
    return _execute(spec,transport,output,binary,watchdog,seconds,memory_mib,policy,run=run)


def _execute(spec,transport,output,binary,watchdog,seconds,memory_mib,policy,*,run=None,_format=native.FORMAT,_verify=native.verify,_preflight=preflight,_test="TestNativeGraphSnapshotFeedWorkloadCell"):

    shaped=_preflight(spec,transport,seconds,memory_mib);policy.validate()
    require(policy.max_key_bytes==4096 and policy.max_value_bytes==native.LINE,"bounded original/page verifier shape")
    binary,watchdog=executables(binary,watchdog)
    run=secrets.token_hex(16) if run is None else run
    require(type(run) is str and len(run)==32 and bytes.fromhex(run).hex()==run and any(bytes.fromhex(run)),"fresh original run identity")
    deadline=time.time_ns()+seconds*1_000_000_000
    require(0<deadline<(1<<63) and 32*seconds*1_000_000_000<(1<<63),"checked cumulative caller/time arithmetic")
    output=Path(output).resolve();output.mkdir(mode=0o700,parents=False,exist_ok=False);engine=output/"engine"
    env={**os.environ,"RHO_GRAPH_NATIVE_WORKLOAD":_format,"RHO_GRAPH_WORKLOAD_SPEC":json.dumps(spec,separators=(",",":")),"RHO_GRAPH_WORKLOAD_RUN":run,
         "RHO_GRAPH_WORKLOAD_SECONDS":str(seconds),"RHO_GRAPH_WORKLOAD_DEADLINE":str(deadline),"RHO_GRAPH_WORKLOAD_TRANSPORT":json.dumps(shaped,separators=(",",":")),
         "RHO_GRAPH_WORKLOAD_OUTPUT":str(engine),"GOMAXPROCS":"2","GOMEMLIMIT":"2GiB","GOWORK":"off","GOTOOLCHAIN":"local"}
    for name in ("RHO_GRAPH_COST_SPEC","RHO_GRAPH_COST_BLOCK_SIZE","RHO_GRAPH_EDGE_TRANSPORT_PROFILE","RHO_GRAPH_FEED_PROCESS","RHO_GRAPH_EDGE_UNIQUE_PROCESS","RHO_GRAPH_RANGE_PROCESS","RHO_GRAPH_CONDITIONAL_PROCESS","RHO_GRAPH_RETENTION_PROCESS","RHO_GRAPH_DURABLE_COLUMN_PROCESS","RHO_GRAPH_NATIVE_FAULT_PROCESS","RHO_GRAPH_FAULT_FAMILY_PROCESS","RHO_GRAPH_DURABLE_MODEL","RHO_GRAPH_DECISION_FAULT_PROCESS","RHO_GRAPH_LIFECYCLE_PROCESS","RHO_GRAPH_NATIVE_FAULT_FAMILY"):
        env[name]=""
    command=[sys.executable,str(watchdog),"--rss-mib",str(memory_mib),"--seconds",str(seconds),"--deadline-unix-nanos",str(deadline),"--report",str(output/"watchdog.json"),"--",str(binary),"-test.run=^"+_test+"$","-test.count=1","-test.v",f"-test.timeout={seconds+30}s"]
    request={"version":1,"format":_format,"spec":spec,"run":run,"transport":shaped,"seconds":seconds,"deadlineUnixNanos":deadline,"aggregateRSSMiB":memory_mib,
             "command":command,"binarySHA256":digest(binary),"watchdogSHA256":digest(watchdog),"retentionContract":"snapshot-C-plus-feed-v1",
             "spoolPolicy":{"max_disk_bytes":policy.max_disk_bytes,"cache_bytes":policy.cache_bytes,"max_key_bytes":policy.max_key_bytes,"max_value_bytes":policy.max_value_bytes}}
    with (output/"request.json").open("x") as out:json.dump(request,out,indent=2);out.write("\n")
    receipt={"version":1,"format":_format,"requestSHA256":digest(output/"request.json"),"binarySHA256":request["binarySHA256"],"spec":spec,"run":run,"exit":None,"completedCell":False,"artifacts":{},"proof":None}
    started=time.monotonic_ns()
    try:
        with (output/"stdout.txt").open("xb") as out,(output/"stderr.txt").open("xb") as err:
            done=subprocess.run(command,env=env,stdout=out,stderr=err,check=False)
        receipt["exit"]=done.returncode;meter=bounded_json(output/"watchdog.json",65536)
        require(done.returncode==meter["exit_code"]==0 and meter["reason"]=="completed" and meter["wall_limit_s"]==seconds and meter["rss_limit_mib"]==memory_mib
                and meter["command"]==command[command.index("--")+1:] and type(meter["peak_tree_rss_mib"]) in (int,float) and math.isfinite(meter["peak_tree_rss_mib"])
                and 0<meter["peak_tree_rss_mib"]<=memory_mib,"actual terminal bounded aggregate native process tree")
        receipt["processTree"],receipt["processTreeSHA256"]=meter,digest(output/"watchdog.json")
        receipt["artifacts"]=hash_artifacts(engine,spec["layout"])
        receipt["proof"]=_verify(engine,output,policy,binary_sha=request["binarySHA256"],expected_spec=spec,expected_run=run,expected_deadline=deadline,expected_seconds=seconds)
        require(receipt["proof"]["transport"]==shaped,"actual traffic counters match immutable admitted scheduler")
        receipt["completedCell"]=True
    finally:
        receipt["elapsedNanos"]=time.monotonic_ns()-started
        with (output/"receipt.json").open("x") as out:json.dump(receipt,out,indent=2);out.write("\n")
    return output


def compare(native_directory,fdb_directory,spool_directory,policy):
    import fdb_feed_check as fdb
    native_directory,fdb_directory=Path(native_directory).resolve(strict=True),Path(fdb_directory).resolve(strict=True)
    receipt,request=bounded_json(native_directory/"receipt.json"),bounded_json(native_directory/"request.json")
    require(receipt["completedCell"] is True and receipt["exit"]==0 and receipt["requestSHA256"]==digest(native_directory/"request.json")
            and receipt["spec"]==request["spec"] and receipt["run"]==request["run"] and receipt["binarySHA256"]==request["binarySHA256"]
            and request["transport"]=={"kind":"local","targetRTTMillis":0,"jitterMillis":0,"seed":20261009}
            and receipt["processTreeSHA256"]==digest(native_directory/"watchdog.json") and receipt["processTree"]==bounded_json(native_directory/"watchdog.json",65536)
            and receipt["artifacts"]==hash_artifacts(native_directory/"engine",request["spec"]["layout"]),"unchanged native local source and literal owned cleanup before paired join")
    fdb_receipt=bounded_json(fdb_directory/"receipt.json")
    require(fdb_receipt["success"] is True and fdb_receipt["exit"]==fdb_receipt["cleanup_exit"]==0 and fdb_receipt["mode"]=="workload-cell","actual FDB owned launcher/cleanup")
    required={"fdb-feed-result.json","fdb-feed-attempts.jsonl","fdb-feed-events.jsonl","fdb-feed-latency.jsonl","fdb-feed-history.jsonl","fdb-feed-current.jsonl","fdb-feed-cdc.jsonl"}
    if request["spec"]["layout"]=="columns":required.add("fdb-feed-columns.jsonl")
    require(required<=fdb_receipt["sha256"].keys(),"all actual FDB feed sources bound by launcher")
    for name,checksum in fdb_receipt["sha256"].items():require("/" not in name and name!="receipt.json" and digest(fdb_directory/name)==checksum,"changed/foreign FDB feed artifact")
    with Spool(spool_directory,policy) as spool:
        native_proof=native.verify(native_directory/"engine",spool_directory,policy,binary_sha=request["binarySHA256"],expected_spec=request["spec"],expected_run=request["run"],expected_deadline=request["deadlineUnixNanos"],expected_seconds=request["seconds"],shared_spool=common.NamespacedSpool(spool,b"native/"))
        fdb_proof=fdb.verify(fdb_directory,spool_directory,policy,shared_spool=common.NamespacedSpool(spool,b"fdb/"))
        require(native_proof["spec"]==fdb_proof["spec"] and native_proof["logicalAnswersSHA256"]==fdb_proof["logicalAnswersSHA256"]
                and native_proof["retentionContract"]==fdb_proof["retentionContract"]=="snapshot-C-plus-feed-v1","complete actual typed old/current/endpoint-life/definition/provenance and acknowledged feed parity")
        ledger=spool.report()
    return {"version":1,"family":native.FAMILY,"spec":request["spec"],"equal":True,"logicalAnswersSHA256":native_proof["logicalAnswersSHA256"],
            "nativeRun":request["run"],"FDBGraph":fdb_proof["runGraph"],"retentionContract":"snapshot-C-plus-feed-v1","nativeTiming":native_proof["timing"],"FDBTiming":fdb_proof["timing"],"spool":ledger,
            "scope":"actual local retained-C complete original/snapshot/feed typed parity; distinct replication/retention physical costs, no feed-only/nonlocal/capacity/full-matrix acceptance"}


def main():
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest="mode",required=True)
    run=sub.add_parser("run");run.add_argument("--spec",required=True);run.add_argument("--output",required=True)
    run.add_argument("--transport",choices=native.TRANSPORTS,default="local");run.add_argument("--binary",required=True);run.add_argument("--watchdog",default=str(Path(__file__).with_name("process_watchdog.py")))
    run.add_argument("--seconds",type=int,required=True);run.add_argument("--memory-mib",type=int,required=True)
    pair=sub.add_parser("compare");pair.add_argument("--native",required=True);pair.add_argument("--fdb",required=True);pair.add_argument("--output",required=True)
    for parser in (run,pair):parser.add_argument("--spool-disk-bytes",type=int,required=True);parser.add_argument("--spool-cache-bytes",type=int,required=True)
    a=p.parse_args();policy=Policy(a.spool_disk_bytes,a.spool_cache_bytes,4096,native.LINE)
    if a.mode=="run":print(execute(json.loads(a.spec),a.transport,a.output,a.binary,a.watchdog,a.seconds,a.memory_mib,policy))
    else:
        output=Path(a.output);output.mkdir(mode=0o700,parents=False,exist_ok=False)
        proof=compare(a.native,a.fdb,output,policy)
        with (output/"paired.json").open("x") as out:json.dump(proof,out,indent=2);out.write("\n")


if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,TypeError,OSError) as error:
        print("native feed cell refused: "+str(error),file=sys.stderr);sys.exit(1)
