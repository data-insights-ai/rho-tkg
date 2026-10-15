#!/usr/bin/env python3
"""Actual supplied native create/update binary, original deadline and owned tree.

No build/import result is execution evidence. Paired nonlocal remains a refusal
until compatible authentic transport bindings and complete joins exist.
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

import native_create_update_check as native
import native_workload_cell as common
from comparison_spool import Policy,Spool

require,digest,bounded_json=native.require,common.digest,common.bounded_json
ARTIFACTS={"result.json","grants.jsonl","attempts.jsonl","operations.jsonl","reads.jsonl","sources.jsonl","processes.jsonl","columns.jsonl"}


def executables(binary,watchdog):return common.executables(binary,Path(__file__).with_name("process_watchdog.py") if watchdog is None else watchdog)


def preflight(spec,transport,seconds,memory_mib,engine="native-raft"):
    native.shape(spec)
    require(engine in ("native-raft","paired") and transport in native.TRANSPORTS and (engine!="paired" or transport=="local"),"paired nonlocal requires actual compatible bindings/joins")
    require(type(seconds) is int and 1<=seconds<=native.MAX_SECONDS and type(memory_mib) is int and 4096<=memory_mib<=1<<20,"explicit finite original run policy")
    require(seconds*1000>5*native.TRANSPORTS[transport],"necessary native sequential anchor/definition delay floor only")
    require(engine!="paired" or seconds<=1800,"ordinary paired FDB duration unchanged")
    return {"kind":"local" if transport=="local" else "injected-rtt","targetRTTMillis":native.TRANSPORTS[transport],"jitterMillis":0,"seed":20261009}


def hash_artifacts(directory,layout):
    require(layout in ("kv","columns"),"explicit actual derived layout")
    found={};roots=0
    with os.scandir(directory) as entries:
        for entry in entries:
            require(not entry.is_symlink(),"foreign native artifact link")
            if entry.is_dir(follow_symlinks=False):
                roots+=1;require(entry.name.startswith("rho-native-create-update-") and roots==1,"one actual six-voter durable directory")
            else:
                require(entry.is_file(follow_symlinks=False) and entry.name in ARTIFACTS,"foreign source artifact")
                found[entry.name]=digest(entry.path)
    require(found.keys()==ARTIFACTS and roots==1,"complete actual artifact inventory")
    return found


def owned_output(output):
    requested=Path(output)
    require(requested.name not in ("",".","..") and not requested.is_symlink() and not requested.exists(),"fresh unlinked requested output leaf required")
    parent=requested.parent.resolve(strict=True)
    require(parent.is_dir(),"existing owned artifact parent required")
    destination=parent/requested.name
    destination.mkdir(mode=0o700,parents=False,exist_ok=False)
    # The Go parent is the sole owner of the fresh engine mkdir.
    return destination,destination/"engine"


def execute(spec,transport,output,binary,watchdog,seconds,memory_mib,policy,*,run=None):
    shaped=preflight(spec,transport,seconds,memory_mib);policy.validate()
    require(policy.max_key_bytes==4096 and policy.max_value_bytes==native.LINE,"bounded original/page verifier schema")
    binary,watchdog=executables(binary,watchdog)
    run=secrets.token_hex(16) if run is None else run
    require(type(run) is str and len(run)==32 and bytes.fromhex(run).hex()==run and any(bytes.fromhex(run)),"fresh immutable run identity")
    deadline=time.time_ns()+seconds*1_000_000_000
    require(0<deadline<(1<<63) and 32*seconds*1_000_000_000<(1<<63),"checked original lifetime and aggregate caller arithmetic")
    output,engine=owned_output(output)
    env={**os.environ,"RHO_GRAPH_NATIVE_WORKLOAD":native.FORMAT,"RHO_GRAPH_WORKLOAD_SPEC":json.dumps(spec,separators=(",",":")),"RHO_GRAPH_WORKLOAD_RUN":run,
         "RHO_GRAPH_WORKLOAD_SECONDS":str(seconds),"RHO_GRAPH_WORKLOAD_DEADLINE":str(deadline),"RHO_GRAPH_WORKLOAD_TRANSPORT":json.dumps(shaped,separators=(",",":")),"RHO_GRAPH_WORKLOAD_OUTPUT":str(engine),"GOMAXPROCS":"2","GOMEMLIMIT":"2GiB","GOWORK":"off","GOTOOLCHAIN":"local"}
    for name in ("RHO_GRAPH_COST_SPEC","RHO_GRAPH_COST_BLOCK_SIZE","RHO_GRAPH_EDGE_TRANSPORT_PROFILE","RHO_GRAPH_CREATE_UPDATE_PROCESS","RHO_GRAPH_SELECTORS_PROCESS","RHO_GRAPH_FEED_PROCESS","RHO_GRAPH_EDGE_UNIQUE_PROCESS","RHO_GRAPH_RANGE_PROCESS","RHO_GRAPH_CONDITIONAL_PROCESS","RHO_GRAPH_RETENTION_PROCESS","RHO_GRAPH_DURABLE_COLUMN_PROCESS","RHO_GRAPH_NATIVE_FAULT_PROCESS","RHO_GRAPH_FAULT_FAMILY_PROCESS","RHO_GRAPH_DURABLE_MODEL","RHO_GRAPH_DECISION_FAULT_PROCESS","RHO_GRAPH_LIFECYCLE_PROCESS","RHO_GRAPH_NATIVE_FAULT_FAMILY","RHO_GRAPH_STATE_RETENTION_PROCESS"):
        env[name]=""
    command=[sys.executable,str(watchdog),"--rss-mib",str(memory_mib),"--seconds",str(seconds),"--deadline-unix-nanos",str(deadline),"--report",str(output/"watchdog.json"),"--",str(binary),"-test.run=^TestNativeGraphCreateUpdateWorkloadCell$","-test.count=1","-test.v",f"-test.timeout={seconds+30}s"]
    request={"version":1,"format":native.FORMAT,"spec":spec,"run":run,"transport":shaped,"seconds":seconds,"deadlineUnixNanos":deadline,"aggregateRSSMiB":memory_mib,"command":command,"binarySHA256":digest(binary),"watchdogSHA256":digest(watchdog),"retentionContract":"snapshot-C-retained-full-prefix-v1","spoolPolicy":{"max_disk_bytes":policy.max_disk_bytes,"cache_bytes":policy.cache_bytes,"max_key_bytes":policy.max_key_bytes,"max_value_bytes":policy.max_value_bytes}}
    with (output/"request.json").open("x") as out:json.dump(request,out,indent=2);out.write("\n")
    receipt={"version":1,"format":native.FORMAT,"requestSHA256":digest(output/"request.json"),"binarySHA256":request["binarySHA256"],"spec":spec,"run":run,"exit":None,"completedCell":False,"artifacts":{},"proof":None}
    started=time.monotonic_ns()
    try:
        with (output/"stdout.txt").open("xb") as out,(output/"stderr.txt").open("xb") as err:done=subprocess.run(command,env=env,stdout=out,stderr=err,check=False)
        receipt["exit"]=done.returncode;meter=bounded_json(output/"watchdog.json",65536)
        require(done.returncode==meter["exit_code"]==0 and meter["reason"]=="completed" and meter["wall_limit_s"]==seconds and meter["rss_limit_mib"]==memory_mib and meter["command"]==command[command.index("--")+1:] and type(meter["peak_tree_rss_mib"]) in (int,float) and math.isfinite(meter["peak_tree_rss_mib"]) and 0<meter["peak_tree_rss_mib"]<=memory_mib,"actual terminal bounded aggregate native process tree")
        receipt["processTree"],receipt["processTreeSHA256"]=meter,digest(output/"watchdog.json")
        receipt["artifacts"]=hash_artifacts(engine,spec["layout"])
        receipt["proof"]=native.verify(engine,output,policy,binary_sha=request["binarySHA256"],expected_spec=spec,expected_run=run,expected_deadline=deadline,expected_seconds=seconds)
        require(receipt["proof"]["transport"]==shaped,"actual traffic counters bind the original owned delay scheduler")
        receipt["completedCell"]=True
    finally:
        receipt["elapsedNanos"]=time.monotonic_ns()-started
        with (output/"receipt.json").open("x") as out:json.dump(receipt,out,indent=2);out.write("\n")
    return output


def compare(native_directory,fdb_directory,spool_directory,policy):
    import fdb_create_update_check as fdb
    native_directory,fdb_directory=Path(native_directory).resolve(strict=True),Path(fdb_directory).resolve(strict=True)
    receipt,request=bounded_json(native_directory/"receipt.json"),bounded_json(native_directory/"request.json")
    require(receipt["completedCell"] is True and receipt["exit"]==0 and receipt["requestSHA256"]==digest(native_directory/"request.json") and receipt["spec"]==request["spec"] and receipt["run"]==request["run"] and receipt["binarySHA256"]==request["binarySHA256"] and request["transport"]=={"kind":"local","targetRTTMillis":0,"jitterMillis":0,"seed":20261009} and receipt["processTreeSHA256"]==digest(native_directory/"watchdog.json") and receipt["processTree"]==bounded_json(native_directory/"watchdog.json",65536) and receipt["artifacts"]==hash_artifacts(native_directory/"engine",request["spec"]["layout"]),"unchanged actual local native source and owned cleanup")
    fdb_receipt=bounded_json(fdb_directory/"receipt.json")
    require(fdb_receipt["success"] is True and fdb_receipt["exit"]==fdb_receipt["cleanup_exit"]==0 and fdb_receipt["mode"]=="workload-cell","actual FDB owned launcher/cleanup")
    required={"fdb-create-update-"+name for name in ("result.json","attempts.jsonl","events.jsonl","latency.jsonl","history.jsonl","current.jsonl","cdc.jsonl")}
    if request["spec"]["layout"]=="columns":required.add("fdb-create-update-columns.jsonl")
    require(required<=fdb_receipt["sha256"].keys(),"complete actual FDB family sources bound by launcher")
    for name,checksum in fdb_receipt["sha256"].items():require("/" not in name and name!="receipt.json" and digest(fdb_directory/name)==checksum,"changed/foreign FDB family artifact")
    with Spool(spool_directory,policy) as spool:
        native_proof=native.verify(native_directory/"engine",spool_directory,policy,binary_sha=request["binarySHA256"],expected_spec=request["spec"],expected_run=request["run"],expected_deadline=request["deadlineUnixNanos"],expected_seconds=request["seconds"],shared_spool=common.NamespacedSpool(spool,b"native/"))
        fdb_proof=fdb.verify(fdb_directory,spool_directory,policy,shared_spool=common.NamespacedSpool(spool,b"fdb/"))
        require(native_proof["spec"]==fdb_proof["spec"] and native_proof["logicalAnswersSHA256"]==fdb_proof["logicalAnswersSHA256"] and native_proof["retentionContract"]==fdb_proof["retentionContract"]=="snapshot-C-retained-full-prefix-v1" and native_proof["rowSemantics"]==fdb_proof["rowSemantics"],"complete actual typed old/current/endpoint-life/definition/provenance and business-locality parity")
        ledger=spool.report()
    return {"version":1,"family":native.FAMILY,"spec":request["spec"],"equal":True,"logicalAnswersSHA256":native_proof["logicalAnswersSHA256"],"retentionContract":"snapshot-C-retained-full-prefix-v1","nativeBusinessTiming":native_proof["businessTiming"],"FDBBusinessTiming":fdb_proof["businessPhases"],"spool":ledger,"scope":"actual local retained-C complete logical create/update parity after both independent native/raw EOF oracles; native planned installed-grant IDs and fixed FDB declarations are not equivalent allocator issuance; replication/retention/resource costs separate, no efficiency/nonlocal/capacity/full-matrix acceptance"}


def main():
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest="mode",required=True)
    run=sub.add_parser("run");run.add_argument("--spec",required=True);run.add_argument("--output",required=True);run.add_argument("--transport",choices=native.TRANSPORTS,default="local");run.add_argument("--binary",required=True);run.add_argument("--watchdog",default=str(Path(__file__).with_name("process_watchdog.py")));run.add_argument("--seconds",type=int,required=True);run.add_argument("--memory-mib",type=int,required=True)
    pair=sub.add_parser("compare");pair.add_argument("--native",required=True);pair.add_argument("--fdb",required=True);pair.add_argument("--output",required=True)
    for command in (run,pair):command.add_argument("--spool-disk-bytes",type=int,required=True);command.add_argument("--spool-cache-bytes",type=int,required=True)
    args=parser.parse_args();policy=Policy(args.spool_disk_bytes,args.spool_cache_bytes,4096,native.LINE)
    if args.mode=="run":print(execute(json.loads(args.spec),args.transport,args.output,args.binary,args.watchdog,args.seconds,args.memory_mib,policy))
    else:
        output=Path(args.output);output.mkdir(mode=0o700,parents=False,exist_ok=False)
        proof=compare(args.native,args.fdb,output,policy)
        with (output/"paired.json").open("x") as out:json.dump(proof,out,indent=2);out.write("\n")


if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,TypeError,OSError) as error:print("native create/update cell refused: "+str(error),file=sys.stderr);sys.exit(1)
