#!/usr/bin/env python3
"""Actual supplied BOTH-HOME transfer binary, immutable grants and owned lifetime."""
import argparse
import json
import math
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time

import native_transfer_check as native
import native_workload_cell as common
import native_create_update_cell as ownership
from comparison_spool import Policy

require,digest,bounded_json=native.require,common.digest,common.bounded_json
validate_grants=native.validate_grants
ARTIFACTS={"result.json","grants.jsonl","attempts.jsonl","operations.jsonl","controls.jsonl","moves.jsonl","sources.jsonl","processes.jsonl","columns.jsonl","gc.jsonl"}


def executables(binary,watchdog):
    return common.executables(binary,Path(__file__).with_name("process_watchdog.py") if watchdog is None else watchdog)


def preflight(spec,transport,seconds,memory_mib,engine="native-raft",*,native_grants=None):
    native.shape(spec);native.validate_grants(native_grants)
    require(engine in ("native-raft","paired") and transport in native.TRANSPORTS,"actual transfer engine/owned transport adapter")
    require(type(seconds) is int and 1<=seconds<=native.MAX_SECONDS and type(memory_mib) is int and 4096<=memory_mib<=1<<20,"finite original whole-cell duration/RSS grant")
    require(seconds*1000>8*native.TRANSPORTS[transport],"necessary genuine anchor/definition/K/ordered-handoff delay floor; not a capacity promise")
    return {"kind":"local" if transport=="local" else "injected-rtt","targetRTTMillis":native.TRANSPORTS[transport],"jitterMillis":0,"seed":20261009}


def hash_artifacts(directory,layout):
    require(layout in ("kv","columns"),"explicit actual layout")
    found={};roots=0
    with os.scandir(directory) as entries:
        for entry in entries:
            require(not entry.is_symlink(),"foreign native artifact link")
            if entry.is_dir(follow_symlinks=False):
                roots+=1;require(roots==1 and entry.name.startswith("rho-native-transfer-"),"one actual six-volume directory")
            else:
                require(entry.is_file(follow_symlinks=False) and entry.name in ARTIFACTS,"foreign transfer evidence")
                found[entry.name]=digest(entry.path)
    require(found.keys()==ARTIFACTS and roots==1,"complete raw transfer/source/original/control inventory")
    return found


def execute(spec,transport,output,binary,watchdog,seconds,memory_mib,policy,*,run=None,native_grants=None):
    selected=preflight(spec,transport,seconds,memory_mib,native_grants=native_grants)
    grants=native.validate_grants(native_grants);policy.validate()
    require(policy.max_key_bytes==4096 and policy.max_value_bytes==native.LINE,"finite exact raw/spool schema; no automatic scratch increase")
    binary,watchdog=executables(binary,watchdog)
    run=secrets.token_hex(16) if run is None else run
    require(type(run) is str and len(run)==32 and bytes.fromhex(run).hex()==run and any(bytes.fromhex(run)),"actual immutable nonzero run")
    deadline=time.time_ns()+seconds*1_000_000_000
    require(0<deadline<(1<<63) and deadline+30*1_000_000_000<(1<<63),"fixed normal + independent cleanup lifetime")
    output,engine=ownership.owned_output(output)
    env={**os.environ,"RHO_GRAPH_NATIVE_WORKLOAD":native.FORMAT,"RHO_GRAPH_WORKLOAD_SPEC":json.dumps(spec,separators=(",",":")),
         "RHO_GRAPH_WORKLOAD_RUN":run,"RHO_GRAPH_WORKLOAD_SECONDS":str(seconds),"RHO_GRAPH_WORKLOAD_DEADLINE":str(deadline),
         "RHO_GRAPH_WORKLOAD_TRANSPORT":json.dumps(selected,separators=(",",":")),"RHO_GRAPH_WORKLOAD_OUTPUT":str(engine),
         "RHO_GRAPH_TRANSFER_GRANTS":json.dumps(grants,separators=(",",":")),"GOMAXPROCS":"2","GOMEMLIMIT":"2GiB","GOWORK":"off","GOTOOLCHAIN":"local"}
    from native_transition_replay import FIXTURE_FLAGS
    for name in FIXTURE_FLAGS:env[name]=""
    env["RHO_GRAPH_NATIVE_WORKLOAD"]=native.FORMAT
    command=[sys.executable,str(watchdog),"--rss-mib",str(memory_mib),"--seconds",str(seconds),"--deadline-unix-nanos",str(deadline),"--report",str(output/"watchdog.json"),
             "--",str(binary),"-test.run=^TestNativeGraphTransferWorkloadCell$","-test.count=1","-test.v",f"-test.timeout={seconds+30}s"]
    request={"version":1,"format":native.FORMAT,"spec":spec,"run":run,"transport":selected,"seconds":seconds,"deadlineUnixNanos":deadline,
             "aggregateRSSMiB":memory_mib,"command":command,"binarySHA256":digest(binary),"watchdogSHA256":digest(watchdog),"nativeGrants":grants,
             "spoolPolicy":{"max_disk_bytes":policy.max_disk_bytes,"cache_bytes":policy.cache_bytes,"max_key_bytes":policy.max_key_bytes,"max_value_bytes":policy.max_value_bytes}}
    with (output/"request.json").open("x") as out:json.dump(request,out,indent=2);out.write("\n")
    receipt={"version":1,"format":native.FORMAT,"spec":spec,"run":run,"binarySHA256":request["binarySHA256"],"requestSHA256":digest(output/"request.json"),"exit":None,"completedCell":False,"proof":None,"artifacts":{}}
    began=time.monotonic_ns()
    try:
        with (output/"stdout.txt").open("xb") as out,(output/"stderr.txt").open("xb") as err:done=subprocess.run(command,env=env,stdout=out,stderr=err,check=False)
        receipt["exit"]=done.returncode;meter=bounded_json(output/"watchdog.json",65536)
        require(done.returncode==meter["exit_code"]==0 and meter["reason"]=="completed" and meter["all_owned_exited"] is True and meter.get("forced_cleanup") is False
                and meter["wall_limit_s"]==seconds and meter["deadline_unix_nanos"]==deadline and meter["rss_limit_mib"]==memory_mib
                and meter["command"]==command[command.index("--")+1:] and type(meter["peak_tree_rss_mib"]) in (int,float) and math.isfinite(meter["peak_tree_rss_mib"])
                and 0<meter["peak_tree_rss_mib"]<=memory_mib,"actual all-owned native process/replacement/cleanup terminal")
        receipt["processTree"],receipt["processTreeSHA256"]=meter,digest(output/"watchdog.json")
        receipt["artifacts"]=hash_artifacts(engine,spec["layout"])
        receipt["proof"]=native.verify(engine,output,policy,binary_sha=request["binarySHA256"],expected_spec=spec,expected_run=run,expected_deadline=deadline,expected_seconds=seconds)
        require(receipt["proof"]["transport"]==selected and receipt["proof"]["nativeGrants"]==grants,"both actual original transport/pools unchanged")
        receipt["completedCell"]=True
    finally:
        receipt["elapsedNanos"]=time.monotonic_ns()-began
        with (output/"receipt.json").open("x") as out:json.dump(receipt,out,indent=2);out.write("\n")
    return output


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ("spec","output","binary","native-grants"):p.add_argument("--"+name,required=True)
    p.add_argument("--watchdog");p.add_argument("--transport",choices=native.TRANSPORTS,required=True)
    for name in ("seconds","memory-mib","spool-disk-bytes","spool-cache-bytes"):p.add_argument("--"+name,type=int,required=True)
    a=p.parse_args();policy=Policy(a.spool_disk_bytes,a.spool_cache_bytes,4096,native.LINE)
    print(execute(json.loads(a.spec,object_pairs_hook=native.raw.pairs),a.transport,a.output,a.binary,a.watchdog,a.seconds,a.memory_mib,policy,
                  native_grants=bounded_json(a.native_grants,4096)))


if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,TypeError,OSError) as error:raise SystemExit("native transfer cell refused: "+str(error)) from error
