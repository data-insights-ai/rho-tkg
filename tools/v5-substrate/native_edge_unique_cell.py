#!/usr/bin/env python3
"""One supplied native edge workload binary, owned cleanup, exact local parity.

This controller never builds a binary or manufactures a native source/token.
Unsupported shaping refuses before directories or either engine is started.
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

import native_edge_unique_check as native
import native_workload_cell as common
import fdb_edge_unique_check as fdb
from comparison_spool import Policy, Spool

require, digest, bounded_json = native.require, common.digest, common.bounded_json
ARTIFACTS = {"result.json", "grants.jsonl", "grant-replays.jsonl", "operations.jsonl", "points.jsonl", "sources.jsonl", "processes.jsonl", "restore.jsonl", "duplicates.jsonl", "winners.jsonl", "frozen.jsonl", "close-originals.jsonl"}


def executables(binary, watchdog):
    return common.executables(binary, Path(__file__).with_name("process_watchdog.py") if watchdog is None else watchdog)


def preflight(spec, transport, seconds, memory_mib, engine="native-raft"):
    native.shape(spec)
    require(transport in native.TRANSPORTS and engine in ("native-raft", "paired") and (engine != "paired" or transport == "local"), "paired nonlocal requires a separately compatible actual FDB/native transport join")
    require(type(seconds) is int and 1 <= seconds <= native.MAX_SECONDS and type(memory_mib) is int and 4096 <= memory_mib <= 1 << 20, "explicit chosen finite whole-run/time/process-tree policy")
    require(engine != "paired" or seconds <= 1800, "paired FDB keeps its existing separate finite duration cap")
    require(seconds*1000 > native.causal_minimum(spec, native.TRANSPORTS[transport]), "chosen original budget cannot meet even necessary sequential native RTT delay")
    return {"kind": "local" if transport == "local" else "injected-rtt", "targetRTTMillis": native.TRANSPORTS[transport], "jitterMillis": 0, "seed": 20261009}


def hash_artifacts(directory, layout):
    expected = ARTIFACTS | ({"columns.jsonl"} if layout == "columns" else set())
    found, roots = {}, 0
    with os.scandir(directory) as entries:
        for entry in entries:
            require(not entry.is_symlink(), "owned artifact symlink")
            if entry.is_dir(follow_symlinks=False):
                roots += 1
                require(entry.name.startswith("rho-native-edge-unique-") and roots == 1, "one actual six-child durable directory")
            else:
                require(entry.is_file(follow_symlinks=False) and entry.name in expected, "foreign/missing native artifact")
                found[entry.name] = digest(entry.path)
    require(found.keys() == expected and roots == 1, "complete actual native source artifact set")
    return found


def execute(spec, transport, output, binary, watchdog, seconds, memory_mib, policy, *, run=None, owned_transport=False):
    shaped = preflight(spec, transport, seconds, memory_mib)
    require(type(owned_transport) is bool, "explicit immutable native transport profile")
    fresh = owned_transport or transport != "local"
    format_ = native.TRANSPORT_FORMAT if fresh else native.FORMAT
    policy.validate()
    require(policy.max_key_bytes == 4096 and policy.max_value_bytes == native.LINE, "unchanged shared verifier record bounds")
    binary, watchdog = executables(binary, watchdog)
    run = secrets.token_hex(16) if run is None else run
    require(type(run) is str and len(run) == 32 and bytes.fromhex(run).hex() == run and any(bytes.fromhex(run)), "explicit fresh run identity")
    deadline = time.time_ns()+seconds*1_000_000_000
    require(0 < deadline <= (1 << 63)-1 and 32*seconds*1_000_000_000 <= (1 << 63)-1, "checked immutable deadline/caller duration")
    destination = Path(output).resolve();destination.mkdir(mode=0o700, parents=False, exist_ok=False)
    engine = destination/"engine"
    env = {**os.environ, "RHO_GRAPH_NATIVE_WORKLOAD": format_, "RHO_GRAPH_WORKLOAD_SPEC": json.dumps(spec, separators=(",", ":")),
           "RHO_GRAPH_WORKLOAD_RUN": run, "RHO_GRAPH_WORKLOAD_TRANSPORT": json.dumps(shaped, separators=(",", ":")) if fresh else "local", "RHO_GRAPH_WORKLOAD_SECONDS": str(seconds),
           "RHO_GRAPH_EDGE_TRANSPORT_PROFILE": native.TRANSPORT_FORMAT if fresh else "",
           "RHO_GRAPH_WORKLOAD_DEADLINE": str(deadline), "RHO_GRAPH_WORKLOAD_OUTPUT": str(engine),
           "GOMAXPROCS": "2", "GOMEMLIMIT": "2GiB", "GOWORK": "off", "GOTOOLCHAIN": "local"}
    for name in ("RHO_GRAPH_COST_SPEC", "RHO_GRAPH_COST_BLOCK_SIZE", "RHO_GRAPH_EDGE_UNIQUE_PROCESS", "RHO_GRAPH_RANGE_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS", "RHO_GRAPH_DURABLE_COLUMN_PROCESS", "RHO_GRAPH_NATIVE_FAULT_PROCESS", "RHO_GRAPH_FAULT_FAMILY_PROCESS", "RHO_GRAPH_DURABLE_MODEL", "RHO_GRAPH_DECISION_FAULT_PROCESS", "RHO_GRAPH_LIFECYCLE_PROCESS", "RHO_GRAPH_NATIVE_FAULT_FAMILY"):
        env[name] = ""
    command = [sys.executable, str(watchdog), "--rss-mib", str(memory_mib), "--seconds", str(seconds), "--deadline-unix-nanos", str(deadline), "--report", str(destination/"watchdog.json"),
               "--", str(binary), "-test.run=^TestNativeGraphEdgeUniqueWorkloadCell$", "-test.count=1", "-test.v", f"-test.timeout={seconds+30}s"]
    request = {"version": 1, "format": format_, "spec": spec, "run": run, "transport": shaped if fresh else transport, "seconds": seconds,
               "deadlineUnixNanos": deadline, "aggregateRSSMiB": memory_mib, "command": command,
               "binarySHA256": digest(binary), "watchdogSHA256": digest(watchdog),
               "spoolPolicy": {"max_disk_bytes": policy.max_disk_bytes, "cache_bytes": policy.cache_bytes, "max_key_bytes": policy.max_key_bytes, "max_value_bytes": policy.max_value_bytes}}
    with (destination/"request.json").open("x") as out: json.dump(request, out, indent=2);out.write("\n")
    receipt = {"version": 1, "format": format_, "requestSHA256": digest(destination/"request.json"), "run": run, "spec": spec,
               "binarySHA256": request["binarySHA256"], "exit": None, "completedCell": False, "artifacts": {}, "proof": None,
               "scope": "one actual owned native cell; runtime acceptance/full matrix/capacity remain separate"}
    started = time.monotonic_ns()
    try:
        with (destination/"stdout.txt").open("xb") as out, (destination/"stderr.txt").open("xb") as error:
            completed = subprocess.run(command, env=env, stdout=out, stderr=error, check=False)
        receipt["exit"] = completed.returncode
        meter = bounded_json(destination/"watchdog.json", 65536)
        require(completed.returncode == meter["exit_code"] == 0 and meter["reason"] == "completed" and meter["wall_limit_s"] == seconds and meter["rss_limit_mib"] == memory_mib
                and meter["command"] == command[command.index("--")+1:] and type(meter["peak_tree_rss_mib"]) in (int, float)
                and math.isfinite(meter["peak_tree_rss_mib"]) and 0 < meter["peak_tree_rss_mib"] <= memory_mib, "actual completed parent/descendant process-tree watchdog receipt")
        receipt["processTree"], receipt["processTreeSHA256"] = meter, digest(destination/"watchdog.json")
        receipt["artifacts"] = hash_artifacts(engine, spec["layout"])
        receipt["proof"] = native.verify(engine, destination, policy, binary_sha=request["binarySHA256"], expected_spec=spec, expected_run=run, expected_deadline=deadline, expected_seconds=seconds)
        require(not fresh or receipt["proof"]["transport"] == shaped, "actual two-cohort transport differs from selected immutable cell")
        receipt["completedCell"] = True
    finally:
        receipt["elapsedNanos"] = time.monotonic_ns()-started
        with (destination/"receipt.json").open("x") as out: json.dump(receipt, out, indent=2);out.write("\n")
    return destination


def compare(native_directory, fdb_directory, spool_directory, policy):
    native_directory, fdb_directory = Path(native_directory).resolve(strict=True), Path(fdb_directory).resolve(strict=True)
    receipt, request = bounded_json(native_directory/"receipt.json"), bounded_json(native_directory/"request.json")
    require(receipt["completedCell"] is True and receipt["exit"] == 0 and receipt["requestSHA256"] == digest(native_directory/"request.json")
            and receipt["binarySHA256"] == request["binarySHA256"] and receipt["spec"] == request["spec"] and receipt["run"] == request["run"]
            and (request["transport"] == "local" or request["format"] == native.TRANSPORT_FORMAT and request["transport"] == {"kind": "local", "targetRTTMillis": 0, "jitterMillis": 0, "seed": 20261009})
            and receipt["processTreeSHA256"] == digest(native_directory/"watchdog.json")
            and receipt["processTree"] == bounded_json(native_directory/"watchdog.json", 65536)
            and hash_artifacts(native_directory/"engine", receipt["spec"]["layout"]) == receipt["artifacts"], "genuine unchanged native cell before paired join")
    fdb_receipt = bounded_json(fdb_directory/"receipt.json")
    require(fdb_receipt["success"] is True and fdb_receipt["exit"] == fdb_receipt["cleanup_exit"] == 0 and fdb_receipt["mode"] == "workload-cell", "actual FDB owned launcher cleanup")
    required = {"fdb-workload-result.json", "fdb-workload-attempts.jsonl", "fdb-workload-latency.jsonl", "fdb-workload-views.jsonl", "fdb-cost-history.jsonl", "fdb-cost-current.jsonl", "fdb-cost-cdc.jsonl"}
    if receipt["spec"]["layout"] == "columns": required.add("fdb-workload-columns.jsonl")
    require(required <= fdb_receipt["sha256"].keys(), "all actual FDB family artifacts bound by launcher receipt")
    for name, checksum in fdb_receipt["sha256"].items():
        require("/" not in name and name != "receipt.json" and digest(fdb_directory/name) == checksum, "changed/foreign FDB family/cache artifact")
    with Spool(spool_directory, policy) as spool:
        native_proof = native.verify(native_directory/"engine", spool_directory, policy, binary_sha=request["binarySHA256"], expected_spec=receipt["spec"], expected_run=receipt["run"], expected_deadline=request["deadlineUnixNanos"], expected_seconds=request["seconds"], shared_spool=common.NamespacedSpool(spool, b"native/"))
        require(request["format"] == native.FORMAT or request["format"] == native.TRANSPORT_FORMAT and native_proof["transport"] == request["transport"], "paired native transport profile matches its actual original receipt")
        fdb_proof = fdb.verify(fdb_directory, spool_directory, policy, shared_spool=common.NamespacedSpool(spool, b"fdb/"))
        require(native_proof["spec"] == fdb_proof["spec"] and native_proof["logicalAnswersSHA256"] == fdb_proof["logicalAnswersSHA256"], "full actual old/current endpoint-life/scalar logical answer parity")
        ledger = spool.report()
    return {"version": 1, "family": native.FAMILY, "equal": True, "spec": receipt["spec"], "nativeRun": receipt["run"], "FDBGraph": fdb_proof["runGraph"],
            "logicalAnswersSHA256": native_proof["logicalAnswersSHA256"], "nativeTiming": native_proof["timing"], "FDBTiming": fdb_proof["timing"], "spool": ledger,
            "scope": "complete actual local family histories/old-current answers; physical winning IDs/revisions/ABORT counts may differ, all costs retained; no RTT/matrix/capacity acceptance"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    run = sub.add_parser("run")
    run.add_argument("--spec", required=True);run.add_argument("--output", required=True)
    run.add_argument("--transport", choices=native.TRANSPORTS, default="local");run.add_argument("--owned-transport", action="store_true")
    run.add_argument("--binary", required=True);run.add_argument("--watchdog", default=str(Path(__file__).with_name("process_watchdog.py")))
    run.add_argument("--seconds", type=int, required=True);run.add_argument("--memory-mib", type=int, required=True)
    pair = sub.add_parser("compare")
    pair.add_argument("--native", required=True);pair.add_argument("--fdb", required=True);pair.add_argument("--output", required=True)
    for p in (run, pair): p.add_argument("--spool-disk-bytes", type=int, required=True);p.add_argument("--spool-cache-bytes", type=int, required=True)
    args = parser.parse_args();policy = Policy(args.spool_disk_bytes, args.spool_cache_bytes, 4096, native.LINE)
    if args.mode == "run": print(execute(json.loads(args.spec), args.transport, args.output, args.binary, args.watchdog, args.seconds, args.memory_mib, policy, owned_transport=args.owned_transport))
    else:
        directory = Path(args.output);directory.mkdir(mode=0o700, parents=False, exist_ok=False)
        proof = compare(args.native, args.fdb, directory, policy)
        with (directory/"paired.json").open("x") as out: json.dump(proof, out, indent=2);out.write("\n")


if __name__ == "__main__":
    try: main()
    except (ValueError, KeyError, TypeError, OSError) as error:
        print("native edge cell refused: "+str(error), file=sys.stderr);sys.exit(1)
