#!/usr/bin/env python3
"""One supplied native snapshot/feed workload binary and owned process lifetime.

No builds or SDK substitutes. Native rolling-ACK retained-only is explicit;
paired adapters with unlike minimum retention refuse before engine effects.
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

import native_feed_retained_check as native
import native_workload_cell as common
from comparison_spool import Policy

require, digest, bounded_json = native.require, common.digest, common.bounded_json
ARTIFACTS={"gc.jsonl","result.json","grants.jsonl","attempts.jsonl","operations.jsonl","feed.jsonl","points.jsonl","sources.jsonl","processes.jsonl","columns.jsonl"}


def executables(binary,watchdog):
    return common.executables(binary,Path(__file__).with_name("process_watchdog.py") if watchdog is None else watchdog)


def preflight(spec,transport,seconds,memory_mib,engine="native-raft"):
    native.shape(spec)
    require(engine=="native-raft" and transport in native.TRANSPORTS,"retained-only paired workload adapter is not yet implemented; old retained-C is a different contract")
    require(type(seconds) is int and 1<=seconds<=native.MAX_SECONDS and type(memory_mib) is int and 4096<=memory_mib<=1<<20,"explicit finite original whole-run policy")
    require(seconds*1000>5*native.TRANSPORTS[transport],"caller budget cannot satisfy necessary sequential anchor/definition RTT delay")
    return {"kind":"local" if transport=="local" else "injected-rtt","targetRTTMillis":native.TRANSPORTS[transport],"jitterMillis":0,"seed":20261009}


def hash_artifacts(directory,layout):
    require(layout in ("kv","columns"),"exact derived layout")
    found={};roots=0
    with os.scandir(directory) as entries:
        for e in entries:
            require(not e.is_symlink(),"foreign source artifact link")
            if e.is_dir(follow_symlinks=False):
                roots+=1;require(e.name.startswith("rho-native-feed-retained-") and roots==1,"one actual six-voter directory")
            else:
                require(e.is_file(follow_symlinks=False) and e.name in ARTIFACTS,"foreign source artifact")
                found[e.name]=digest(e.path)
    require(found.keys()==ARTIFACTS and roots==1,"complete genuine native artifact inventory")
    return found


def owned_output(output):
    requested=Path(output)
    require(requested.is_absolute() and requested.name not in ("",".","..") and not requested.is_symlink(),"fresh absolute owned output, never a foreign or symlink artifact")
    parent=requested.parent.resolve(strict=True)
    require(parent.is_dir(),"existing artifact parent required")
    destination=parent/requested.name
    destination.mkdir(mode=0o700,parents=False,exist_ok=False)
    # The Go parent owns O_EXCL-style Mkdir(engine). No directory is created or
    # reused here; this differs deliberately from the selector parent's API.
    return destination,destination/"engine"


def execute(spec,transport,output,binary,watchdog,seconds,memory_mib,policy,*,run=None):
    shaped=preflight(spec,transport,seconds,memory_mib);policy.validate()
    require(policy.max_key_bytes==4096 and policy.max_value_bytes==native.LINE,"bounded original/page verifier shape")
    binary,watchdog=executables(binary,watchdog)
    run=secrets.token_hex(16) if run is None else run
    require(type(run) is str and len(run)==32 and bytes.fromhex(run).hex()==run and any(bytes.fromhex(run)),"fresh original run identity")
    deadline=time.time_ns()+seconds*1_000_000_000
    require(0<deadline<(1<<63) and 32*seconds*1_000_000_000<(1<<63),"checked cumulative caller/time arithmetic")
    output,engine=owned_output(output)
    env={**os.environ,"RHO_GRAPH_NATIVE_WORKLOAD":native.FORMAT,"RHO_GRAPH_WORKLOAD_SPEC":json.dumps(spec,separators=(",",":")),"RHO_GRAPH_WORKLOAD_RUN":run,
         "RHO_GRAPH_WORKLOAD_SECONDS":str(seconds),"RHO_GRAPH_WORKLOAD_DEADLINE":str(deadline),"RHO_GRAPH_WORKLOAD_TRANSPORT":json.dumps(shaped,separators=(",",":")),
         "RHO_GRAPH_WORKLOAD_OUTPUT":str(engine),"GOMAXPROCS":"2","GOMEMLIMIT":"2GiB","GOWORK":"off","GOTOOLCHAIN":"local"}
    for name in ("RHO_GRAPH_FEED_RETAINED_PROCESS","RHO_GRAPH_CREATE_UPDATE_PROCESS","RHO_GRAPH_SELECTORS_PROCESS","RHO_GRAPH_STATE_RETENTION_PROCESS","RHO_GRAPH_COST_SPEC","RHO_GRAPH_COST_BLOCK_SIZE","RHO_GRAPH_EDGE_TRANSPORT_PROFILE","RHO_GRAPH_FEED_PROCESS","RHO_GRAPH_EDGE_UNIQUE_PROCESS","RHO_GRAPH_RANGE_PROCESS","RHO_GRAPH_CONDITIONAL_PROCESS","RHO_GRAPH_RETENTION_PROCESS","RHO_GRAPH_DURABLE_COLUMN_PROCESS","RHO_GRAPH_NATIVE_FAULT_PROCESS","RHO_GRAPH_FAULT_FAMILY_PROCESS","RHO_GRAPH_DURABLE_MODEL","RHO_GRAPH_DECISION_FAULT_PROCESS","RHO_GRAPH_LIFECYCLE_PROCESS","RHO_GRAPH_NATIVE_FAULT_FAMILY"):
        env[name]=""
    command=[sys.executable,str(watchdog),"--rss-mib",str(memory_mib),"--seconds",str(seconds),"--deadline-unix-nanos",str(deadline),"--report",str(output/"watchdog.json"),"--",str(binary),"-test.run=^TestNativeGraphSnapshotFeedRetainedOnlyWorkloadCell$","-test.count=1","-test.v",f"-test.timeout={seconds+30}s"]
    request={"version":1,"format":native.FORMAT,"spec":spec,"run":run,"transport":shaped,"seconds":seconds,"deadlineUnixNanos":deadline,"aggregateRSSMiB":memory_mib,
             "command":command,"binarySHA256":digest(binary),"watchdogSHA256":digest(watchdog),"retentionContract":native.CONTRACT,
             "spoolPolicy":{"max_disk_bytes":policy.max_disk_bytes,"cache_bytes":policy.cache_bytes,"max_key_bytes":policy.max_key_bytes,"max_value_bytes":policy.max_value_bytes}}
    with (output/"request.json").open("x") as out:json.dump(request,out,indent=2);out.write("\n")
    receipt={"version":1,"format":native.FORMAT,"requestSHA256":digest(output/"request.json"),"binarySHA256":request["binarySHA256"],"spec":spec,"run":run,"exit":None,"completedCell":False,"artifacts":{},"proof":None}
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
        receipt["proof"]=native.verify(engine,output,policy,binary_sha=request["binarySHA256"],expected_spec=spec,expected_run=run,expected_deadline=deadline,expected_seconds=seconds)
        require(receipt["proof"]["transport"]==shaped,"actual traffic counters match immutable admitted scheduler")
        receipt["completedCell"]=True
    finally:
        receipt["elapsedNanos"]=time.monotonic_ns()-started
        with (output/"receipt.json").open("x") as out:json.dump(receipt,out,indent=2);out.write("\n")
    return output


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--spec",required=True);p.add_argument("--output",required=True)
    p.add_argument("--transport",choices=native.TRANSPORTS,default="local")
    p.add_argument("--binary",required=True);p.add_argument("--watchdog",default=str(Path(__file__).with_name("process_watchdog.py")))
    p.add_argument("--seconds",type=int,required=True);p.add_argument("--memory-mib",type=int,required=True)
    p.add_argument("--spool-disk-bytes",type=int,required=True);p.add_argument("--spool-cache-bytes",type=int,required=True)
    a=p.parse_args();policy=Policy(a.spool_disk_bytes,a.spool_cache_bytes,4096,native.LINE)
    print(execute(json.loads(a.spec),a.transport,a.output,a.binary,a.watchdog,a.seconds,a.memory_mib,policy))


if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,TypeError,OSError) as error:
        print("native retained-only feed cell refused: "+str(error),file=sys.stderr);sys.exit(1)
