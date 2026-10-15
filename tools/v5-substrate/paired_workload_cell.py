#!/usr/bin/env python3
"""Explicit owned native/FDB pair: supplied native binary, real FDB launcher.

Both finite admissions precede engine effects. Runs use the same chosen seconds
and run identity; each real engine keeps its own original absolute deadline.
No probe distribution, replica topology or physical GC is declared equal.
"""
import argparse
import json
from pathlib import Path
import resource
import secrets
import sys
import time

import paired_workload_check as check
from comparison_spool import Policy

require,common=check.require,check.common
MAX_SECONDS=365*24*60*60


def preflight(spec,transport,seconds,memory,policy,owned_options,*,profile="small",retained_policy=None,transfer_grants=None):
    native,cell,fdb=check.registry(spec)
    transfer=spec["family"]=="transfer-retained-pins"
    require(transfer==(transfer_grants is not None),"transfer requires separate explicit native and FDB grants before either engine")
    if transfer:
        require(type(transfer_grants) is dict and set(transfer_grants)=={"native","foundationdb"} and type(transfer_grants["native"]) is dict and set(transfer_grants["native"])=={"MaxPages","DescribeBytes","ImportBytes","UndoBytes"},"exact independent pool and catalog/census grant policies")
        require(retained_policy is None,"transfer retention schedule is not a feed-only profile substitution")
        fdb.validate_grant(transfer_grants["foundationdb"],check.fdb_spec(spec))
    selected=cell.preflight(spec,transport,seconds,memory,"native-raft",**({"native_grants":transfer_grants["native"]} if transfer else {}))
    require(selected==check.declared_transport(transport),"actual immutable native scheduler selection")
    require(type(seconds) is int and 1<=seconds<=MAX_SECONDS and profile in ("small","large") and type(memory) is int and memory>=max(4096,3072 if profile=="small" else 10240),"both actual aggregate resource profiles before either engine")
    require(type(owned_options) is dict and set(owned_options)=={"QueuePackets","SampleMillis","MaxSamples","MaxArtifactBytes"},"all four finite actual FDB owned transport grants")
    policy.validate();require(policy.max_key_bytes==4096 and policy.max_value_bytes==512<<10,"ONE unchanged finite shared answer/original/transport spool")
    import workload_matrix as matrix
    candidate=matrix.owned_transport_input(check.fdb_spec(spec),"01"*16,selected,seconds,owned_options)
    reserve=((candidate["MaxSamples"]+4)*5+60)*((128<<10)+4096)+(64<<20)
    require(policy.max_disk_bytes>reserve,"before-effects scratch funds actual declared transport joins as well as family data")
    if spec["version"]==3:fdb.validate_grant(retained_policy,spec)
    else:require(retained_policy is None,"no retained-only owner policy relabel on a retained-C family")
    return selected


def usage():
    # These are real host observations, not summed with overlapping container
    # cgroup/native tree peaks. Each engine's full proof carries those layers.
    out={"platform":sys.platform}
    for name,who in (("controller",resource.RUSAGE_SELF),("reapedChildren",resource.RUSAGE_CHILDREN)):
        u=resource.getrusage(who)
        out[name]={"userSeconds":u.ru_utime,"systemSeconds":u.ru_stime,"maxRSS":u.ru_maxrss}
    out["rssUnits"]="bytes" if sys.platform=="darwin" else "KiB"
    return out


def execute(spec,transport,output,binary,watchdog,seconds,memory,policy,*,profile,owned_options,retained_policy=None,transfer_grants=None,run=None):
    began=time.monotonic_ns();before=usage()
    selected=preflight(spec,transport,seconds,memory,policy,owned_options,profile=profile,retained_policy=retained_policy,transfer_grants=transfer_grants)
    native,cell,fdb=check.registry(spec)
    if transfer_grants is not None:
        transfer_grants={"native":dict(transfer_grants["native"]),"foundationdb":fdb.validate_grant(transfer_grants["foundationdb"],check.fdb_spec(spec))}
    binary,watchdog=cell.executables(binary,watchdog)
    run=secrets.token_hex(16) if run is None else run
    require(type(run) is str and len(run)==32 and bytes.fromhex(run).hex()==run and any(bytes.fromhex(run)),"same fresh actual paired run")
    leaf=Path(output);require(leaf.is_absolute() and leaf.name not in ("",".","..") and not leaf.is_symlink(),"fresh absolute owned pair destination")
    destination=leaf.parent.resolve(strict=True)/leaf.name;destination.mkdir(mode=0o700,parents=False,exist_ok=False)
    request={"format":check.FORMAT,"spec":spec,"FDBSpec":check.fdb_spec(spec),"run":run,"transport":selected,"BudgetSeconds":seconds,"aggregateRSSMiB":memory,"deploymentProfile":profile,"FDBOwnedTransport":owned_options,"retainedPolicy":retained_policy,"nativeBinarySHA256":common.digest(binary),"watchdogSHA256":common.digest(watchdog),"spoolPolicy":{"max_disk_bytes":policy.max_disk_bytes,"cache_bytes":policy.cache_bytes,"max_key_bytes":policy.max_key_bytes,"max_value_bytes":policy.max_value_bytes}}
    if transfer_grants is not None:request["transferGrants"]=transfer_grants
    with (destination/"request.json").open("x") as out:json.dump(request,out,indent=2);out.write("\n")
    receipt={"format":check.FORMAT,"requestSHA256":common.digest(destination/"request.json"),"completed":False,"nativeCompleted":False,"FDBCompleted":False,"hostBefore":before,"stages":{}}
    try:
        start=time.monotonic_ns()
        options={"owned_transport":True} if spec["family"]=="cross-edge-endpoint-uniqueness" else {}
        if transfer_grants is not None:options["native_grants"]=transfer_grants["native"]
        cell.execute(spec,transport,destination/"native",binary,watchdog,seconds,memory,policy,run=run,**options)
        receipt["stages"]["nativeRunNanos"]=time.monotonic_ns()-start;receipt["nativeCompleted"]=True
        import workload_matrix as matrix
        fdbcell={"version":1,"engine":"foundationdb","workload":check.fdb_spec(spec),"transport":selected,"repetition":1,"implemented":True,"scope":"actual explicitly owned FDB counterpart; source scheduling is not measurement acceptance"}
        start=time.monotonic_ns()
        matrix.execute(fdbcell,destination/"fdb",seconds,profile,memory,policy.max_disk_bytes,policy.cache_bytes,"explicit-caller",owned_options=owned_options,retained_policy=retained_policy,run_override=run,
                       **({"transfer_policy":transfer_grants["foundationdb"]} if transfer_grants is not None else {}))
        receipt["stages"]["FDBRunNanos"]=time.monotonic_ns()-start;receipt["FDBCompleted"]=True
        actual=common.bounded_json(destination/"fdb/verified.json")
        start=time.monotonic_ns();proof=check.verify(destination/"native",actual["artifacts"],destination,policy)
        if transfer_grants is not None:
            require(proof["transfer"]["nativeGrants"]==transfer_grants["native"] and proof["transfer"]["FDBGrants"]==transfer_grants["foundationdb"],"both actual immutable engine admissions match the original independently funded request")
        receipt["stages"]["completePairVerificationNanos"]=time.monotonic_ns()-start
        with (destination/"verified.json").open("x") as out:json.dump(proof,out,indent=2);out.write("\n")
        receipt["proofSHA256"]=common.digest(destination/"verified.json");receipt["completed"]=True
    finally:
        receipt["allInControllerNanos"]=time.monotonic_ns()-began;receipt["hostAfter"]=usage()
        receipt["scope"]="all-in actual controller window includes engine initialization/build helpers, execution, raw artifact/oracle work and cleanup; business-only phase/whole-operation clocks remain in each full proof; resource layers/peaks are not summed"
        with (destination/"receipt.json").open("x") as out:json.dump(receipt,out,indent=2);out.write("\n")
    return destination


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("--spec",required=True);p.add_argument("--transport",choices=check.TRANSPORTS,required=True);p.add_argument("--output",required=True);p.add_argument("--binary",required=True);p.add_argument("--watchdog");p.add_argument("--seconds",type=int,required=True);p.add_argument("--memory-mib",type=int,required=True);p.add_argument("--profile",choices=("small","large"),required=True);p.add_argument("--transport-grants",required=True);p.add_argument("--retained-policy");p.add_argument("--transfer-grants");p.add_argument("--spool-disk-bytes",type=int,required=True);p.add_argument("--spool-cache-bytes",type=int,required=True)
    a=p.parse_args();policy=Policy(a.spool_disk_bytes,a.spool_cache_bytes,4096,512<<10)
    spec=json.loads(a.spec,object_pairs_hook=common.native.raw.pairs);grants=json.loads(a.transport_grants,object_pairs_hook=common.native.raw.pairs);retained=None
    if a.retained_policy is not None:retained=common.bounded_json(a.retained_policy,16384)
    transfer=None if a.transfer_grants is None else common.bounded_json(a.transfer_grants,16384)
    print(execute(spec,a.transport,a.output,a.binary,a.watchdog,a.seconds,a.memory_mib,policy,profile=a.profile,owned_options=grants,retained_policy=retained,transfer_grants=transfer))


if __name__=="__main__":
    try:main()
    except (ValueError,KeyError,TypeError,OSError) as error:raise SystemExit("owned paired workload refused: "+str(error)) from error
