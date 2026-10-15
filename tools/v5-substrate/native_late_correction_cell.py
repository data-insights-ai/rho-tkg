#!/usr/bin/env python3
"""Actual Cost11 native half-correction driver; supplied binary, owned lifetime."""
import argparse
import json
from pathlib import Path
import sys

import native_create_update_cell as common
import native_late_correction_check as native
from comparison_spool import Policy

executables,bounded_json=common.executables,common.bounded_json


def preflight(spec,transport,seconds,memory_mib,engine="native-raft"):
    return common._preflight(spec,transport,seconds,memory_mib,engine,family=native.FAMILY)


def execute(spec,transport,output,binary,watchdog,seconds,memory_mib,policy,*,run=None):
    return common._execute(spec,transport,output,binary,watchdog,seconds,memory_mib,policy,run=run,family=native.FAMILY)


def compare(native_directory,fdb_directory,spool_directory,policy):
    return common._compare(native_directory,fdb_directory,spool_directory,policy,family=native.FAMILY)


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
    except (ValueError,KeyError,TypeError,OSError) as error:print("native late-correction cell refused: "+str(error),file=sys.stderr);sys.exit(1)
