#!/usr/bin/env python3
"""Bounded real cost cells; native and FDB adapters must emit verified outcomes."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
FACTS = (10_000, 100_000, 1_000_000)
CROSS = (0, 10, 50, 100)
WRITERS = (1, 8, 32)
RUNS = 5


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as source:
        while chunk := source.read(1 << 20):
            value.update(chunk)
    return value.hexdigest()


def specification(facts, cross, writers):
    if facts not in FACTS or cross not in CROSS or writers not in WRITERS:
        raise ValueError("cell outside reviewed measurement matrix")
    return {"version": 1, "facts": facts, "crossPercent": cross, "writers": writers}


def plan():
    return {"minimumRuns": RUNS, "deployment": "single-machine distributed processes/containers",
            "cells": [specification(n, c, w) for n in FACTS for c in CROSS for w in WRITERS],
            "firstWorkload": "three setup nodes; N relationship creates(p=5), N updates(p=6); retained old/current graph and complete CDC",
            "remainingWorkloads": ["contention/empty-range guards", "Stable/After", "correction/old-cut index scans", "snapshot-feed", "columns", "range transfer/pins"],
            "implementationStatus": {"native": "writer1 test-support runner; single-grant ID limit explicitly enforced",
                                     "FDB": "writer1 real cost source/launcher present; qualification and same-dataset measurements pending",
                                     "writers8/32": "not yet available; refuse execution"}}


def parity(native, fdb, artifacts):
    module_spec = importlib.util.spec_from_file_location("actual_fault_cost_gate", ROOT / "compare-graph-fault.py")
    checker = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(checker)
    left, native_sha = checker.load(native)
    right, fdb_sha = checker.load(fdb)
    receipt = checker.compare(left, right, artifacts)
    if receipt.get("equal") is not True:
        raise ValueError("actual native/FDB fault parity required")
    return {"nativeSHA256": native_sha, "fdbSHA256": fdb_sha, "receipt": receipt}


def native_cell(args, spec, number, gate):
    root = args.output / f"native-{spec['facts']}-{spec['crossPercent']}-{spec['writers']}-run{number}"
    root.mkdir(parents=True, exist_ok=False)
    binary = args.native_binary.resolve(strict=True)
    watchdog = args.watchdog.resolve(strict=True)
    # Only compiler/build artifacts may be reused. Each database/client/server
    # starts anew with an exclusive fresh durable directory below this run.
    environment = os.environ | {"RHO_GRAPH_PROCESS_ARTIFACT": str(root), "RHO_GRAPH_COST_SPEC": json.dumps(spec),
                                "GOMAXPROCS": "2", "GOMEMLIMIT": "2GiB", "GOFLAGS": "-p=1",
                                "GOWORK": "off", "GOTOOLCHAIN": "local"}
    command = [sys.executable, str(watchdog), "--rss-mib", "4096", "--seconds", str(args.benchmark_seconds),
               "--report", str(root / "watchdog.json"), "--", str(binary),
               "-test.run=^TestNativeGraphCostCell$", "-test.v", f"-test.timeout={args.benchmark_seconds + 60}s"]
    started = time.monotonic_ns()
    with (root / "stdout.txt").open("wb") as stdout, (root / "stderr.txt").open("wb") as stderr:
        completed = subprocess.run(command, env=environment, stdout=stdout, stderr=stderr, check=False)
    measurement = json.loads((root / "watchdog.json").read_text())
    receipt = {"version": 1, "spec": spec, "run": number, "exit": completed.returncode,
               "command": command, "environment": {k: environment[k] for k in ("GOMAXPROCS", "GOMEMLIMIT", "GOFLAGS", "GOWORK", "GOTOOLCHAIN")},
               "host": {"platform": platform.platform(), "machine": platform.machine()}, "faultParity": gate,
               "binarySHA256": digest(binary), "elapsedNanos": time.monotonic_ns() - started,
               "processTree": measurement, "rssScope": "native client/test parent and every actual descendant voter process; sampler peak, not sum of separate role peaks",
               "deployment": "single-machine distributed; no independent physical-host-loss or LAN/WAN throughput claim",
               "completedCell": False, "probe": args.probe, "minimumFiveRunAcceptance": False}
    results = list(root.glob("rho-native-cost-*/native-cost-result.json"))
    if completed.returncode == 0:
        if measurement["exit_code"] != 0 or len(results) != 1:
            raise ValueError("successful command lacks exact one actual result")
        result = json.loads(results[0].read_text())
        if result["spec"] != spec or result["usefulFacts"] != spec["facts"] or result["Creates"] != spec["facts"] or result["Updates"] != spec["facts"] or result["setupNodes"] != 3 or result["reaped"] != 6 or result["CDCChanges"] != 3 + 3 * spec["facts"]:
            raise ValueError("actual native cardinality/result mismatch")
        receipt["result"] = result
        receipt["resultSHA256"] = digest(results[0])
        receipt["completedCell"] = True
        receipt["artifacts"] = {p.name: digest(p) for p in results[0].parent.iterdir() if p.is_file()}
    (root / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    if completed.returncode != 0:
        raise ValueError(f"native cell refused/failed; exact failed receipt retained: {root}")
    return root


def fdb_cell(args, spec, number, gate):
    root = args.output / f"fdb-{spec['facts']}-{spec['crossPercent']}-{spec['writers']}-run{number}"
    root.mkdir(parents=True, exist_ok=False)
    command = ["bash", str(ROOT / "fdb/run-local.sh"), "cost-cell", "small"]
    environment = os.environ | {"RHO_GRAPH_COST_SPEC": json.dumps(spec), "FDB_COST_SECONDS": str(args.benchmark_seconds),
                                "FDB_ADMISSION_MIB": "4096", "FDB_ADMISSION_CPUS": "2"}
    started = time.monotonic_ns()
    with (root / "stdout.txt").open("wb") as stdout, (root / "stderr.txt").open("wb") as stderr:
        completed = subprocess.run(command, env=environment, stdout=stdout, stderr=stderr, check=False)
    lines = (root / "stdout.txt").read_text().splitlines()
    if not lines:
        raise ValueError("FDB launcher did not retain an actual artifact root")
    artifacts = Path(lines[-1]).resolve(strict=True)
    if artifacts.parent != (ROOT / "fdb/artifacts").resolve() or not artifacts.name.startswith("rho-v5-fdb-"):
        raise ValueError("unexpected FDB artifact root")
    launch = json.loads((artifacts / "receipt.json").read_text())
    receipt = {"version": 1, "spec": spec, "run": number, "exit": completed.returncode,
               "command": command, "faultParity": gate, "probe": args.probe, "minimumFiveRunAcceptance": False,
               "controllerElapsedIncludingBuildNanos": time.monotonic_ns() - started,
               "FDBArtifacts": str(artifacts), "launchReceipt": launch, "completedCell": False,
               "setupContractDifference": "FDB accepts externally admitted fixed IDs in its actual durable configuration; native performs actual rho allocator/recipient issuance. No semantically equal allocator-overhead claim."}
    if completed.returncode == 0:
        if not launch["success"] or launch["cleanup_exit"] != 0:
            raise ValueError("FDB launch/cleanup did not qualify")
        for name, expected in launch["sha256"].items():
            if digest(artifacts / name) != expected:
                raise ValueError("FDB actual artifact hash changed: " + name)
        result = json.loads((artifacts / "fdb-cost-result.json").read_text())
        if result["spec"] != spec or result["CompletedCreates"] != spec["facts"] or result["CompletedUpdates"] != spec["facts"] or result["CDCGroups"] != 3 + 2 * spec["facts"] or result["LogicalComponentChanges"] != 3 + 3 * spec["facts"]:
            raise ValueError("actual FDB count/oracle mismatch")
        sampled, client_observed = 0, False
        with (artifacts / "cost-process-rss.jsonl").open() as source:
            for line in source:
                sample = json.loads(line)
                sampled = max(sampled, sample["rssKiB"])
                client_observed = client_observed or any(
                    c["container"] == artifacts.name + "-cost" and c["state"] == "live" and c["processes"]
                    for c in sample["containers"])
        marker = artifacts / "cost-instrumented.txt"
        mode = marker.read_text().strip() if marker.exists() else None
        if mode not in (None, "0", "1"):
            raise ValueError("invalid recorded cost instrumentation mode")
        instrumented = None if mode is None else mode == "1"
        if not client_observed or instrumented is not False:
            reason = ("actual client exited before any client PID RSS sample; aggregate cost unavailable" if not client_observed else
                      "qualification binary has race/coverage instrumentation" if instrumented else
                      "cost instrumentation mode is unavailable")
            receipt.update(functionalResult=result, costMeasurementQualified=False, refusal=reason,
                           actualClientPIDObserved=bool(client_observed), instrumentedQualification=instrumented,
                           sampledRSSKiB=sampled)
            (root / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
            raise ValueError("functional FDB cell passed, cost measurement refused: " + reason)
        allocated, lengths = 0, 0
        for member in range(4):
            rows = (artifacts / f"zone{member}-disk.txt").read_text().splitlines()
            if len(rows) != 2:
                raise ValueError("all data/log persisted allocation missing")
            allocated += sum(int(row.split()[0]) for row in rows)
            lengths += int((artifacts / f"zone{member}-file-length-bytes.txt").read_text())
        receipt.update(result=result, sampledAggregateProcessRSSKiB=sampled, persistedAllocatedBytes=allocated,
                       persistedFileBytes=lengths, physicalStorageScope="all four actual data and log directories, every replica/role; trace outputs excluded",
                       rssScope="all actual client and four server process PIDs; simultaneous sampled sum with captured sampling skew; not sum of role peaks",
                       completedCell=True)
    (root / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    if completed.returncode != 0:
        raise ValueError(f"FDB cell refused/failed; exact failed receipt retained: {root}")
    return root


def compare_cells(native_path, fdb_path):
    native, fdb = (json.loads(Path(p).read_text()) for p in (native_path, fdb_path))
    if not native["completedCell"] or not fdb["completedCell"] or native["spec"] != fdb["spec"]:
        raise ValueError("matching complete real cells required")
    spec = native["spec"]
    facts = spec["facts"]
    left_root = Path(native["result"]["directory"])
    right_root = Path(fdb["FDBArtifacts"])
    for name, expected in native["artifacts"].items():
        if digest(left_root / name) != expected:
            raise ValueError("native retained witness changed: " + name)
    for name, expected in fdb["launchReceipt"]["sha256"].items():
        if digest(right_root / name) != expected:
            raise ValueError("FDB retained witness changed: " + name)
    observed = hashlib.sha256()
    with (left_root / "observations.jsonl").open() as source:
        for value in (5, 6):
            # The independent FDB adapter verifies actual retained history and
            # both posting copies; consume its actual stored rows/projection.
            path = right_root / f"fdb-cost-view-{value}.json"
            if path.stat().st_size > 64 << 20:
                raise ValueError("first-cell comparison view exceeds64MiB; streaming larger layout pending")
            witness = json.loads(path.read_text())
            if sum(len(rows) for rows in witness["Snapshot"]["Values"]) != 3 + 3 * facts:
                raise ValueError("FDB complete retained row cardinality/phantom")
            edges = witness["View"]["VisibleEdges"]
            if len(edges) != facts or witness["View"]["IdentityEdges"] != edges or witness["View"]["Outgoing"] != edges or witness["View"]["Incoming"] != edges or len(witness["View"]["Nodes"]) != 3:
                raise ValueError("FDB exact complete retained projection/postings")
            for number, edge in enumerate(edges):
                line = source.readline(65537)
                if not line or len(line) > 65536:
                    raise ValueError("native retained point witness missing/oversized")
                entity = json.loads(line)
                prop = entity["property"]
                raw = edge["Source"], edge["Target"]
                key = "edge/" + "/".join(f"{v:016x}" for v in (edge["ID"], raw[0]["Node"], raw[0]["Life"], raw[1]["Node"], raw[1]["Life"]))
                row = witness["Snapshot"]["Values"][0].get(key)
                operation = "create" if value == 5 else "update"
                provenance = 4 + number + (facts if value == 6 else 0)
                if row is None or row["Deleted"] or row["Value"] != value or row["Version"] != (1 if value == 5 else 2) or row["Round"] <= 0 or row["TxID"] != f"cost-{operation}-{number}":
                    raise ValueError("FDB retained value not actual original operation")
                if not entity["found"] or not entity["effective"] or entity["id"] != edge["ID"] or entity["source"] != raw[0]["Node"] or entity["target"] != raw[1]["Node"] or entity["effectiveLife"] != edge["ID"] + 1 or not prop["present"] or prop["null"] or prop["kind"] != "i64" or prop["i64"] != value or prop["handle"] != value or prop["revision"] <= 0 or prop["provenance"] != provenance or edge["Value"] != value:
                    raise ValueError("actual native/FDB retained graph values differ")
                observed.update(json.dumps({"id": edge["ID"], "source": raw[0]["Node"], "target": raw[1]["Node"], "life": entity["effectiveLife"], "value": value, "operationProvenance": provenance}, sort_keys=True).encode() + b"\n")
        if source.readline():
            raise ValueError("extra native retained point witness")
    return {"equal": True, "spec": spec, "relationshipFacts": facts, "setupNodes": 3,
            "usefulCreateTransactions": facts, "usefulUpdateTransactions": facts,
            "retainedPointObservations": 2 * facts, "exactLogicalComponentChanges": 3 + 3 * facts,
            "canonicalRetainedOutputSHA256": observed.hexdigest(),
            "scope": "single-machine distributed common fixed graph/history/CDC workload; allocation setup contracts differ; no engine selection or fullmatrix acceptance"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", action="store_true")
    parser.add_argument("--compare", nargs=2, metavar=("NATIVE_RECEIPT", "FDB_RECEIPT"))
    parser.add_argument("--engine", choices=("native", "fdb"), default="native")
    parser.add_argument("--facts", type=int, choices=FACTS, default=10_000)
    parser.add_argument("--cross", type=int, choices=CROSS, default=0)
    parser.add_argument("--writers", type=int, choices=WRITERS, default=1)
    parser.add_argument("--runs", type=int, default=RUNS)
    parser.add_argument("--probe", action="store_true", help="one supervised first-cell feasibility run; never minimum-five matrix acceptance")
    parser.add_argument("--benchmark-seconds", type=int, default=1800)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--native-binary", type=Path)
    parser.add_argument("--watchdog", type=Path)
    parser.add_argument("--native-fault", type=Path)
    parser.add_argument("--fdb-fault", type=Path)
    parser.add_argument("--fdb-artifact-root", type=Path)
    args = parser.parse_args()
    if args.plan:
        print(json.dumps(plan(), indent=2)); return
    if args.compare:
        print(json.dumps(compare_cells(*args.compare), indent=2)); return
    spec = specification(args.facts, args.cross, args.writers)
    if not 1 <= args.benchmark_seconds <= 1800:
        parser.error("benchmark watchdog must be1..1800seconds; qualification/build gates remain180seconds")
    if args.probe:
        args.runs = 1
    if args.runs < RUNS and not args.probe:
        parser.error("at least five independent fresh runs required")
    if args.writers != 1:
        parser.error("requested concurrent writer mode is not implemented; no substitute cell")
    if not all((args.output, args.native_fault, args.fdb_fault, args.fdb_artifact_root)) or args.engine == "native" and not all((args.native_binary,args.watchdog)):
        parser.error("output, native binary/watchdog and actual fault parity inputs required")
    args.output = args.output.resolve()
    gate = parity(args.native_fault, args.fdb_fault, args.fdb_artifact_root)
    for number in range(1, args.runs + 1):
        print((native_cell if args.engine=="native" else fdb_cell)(args, spec, number, gate), flush=True)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, subprocess.SubprocessError) as error:
        raise SystemExit(f"cost cell refused: {error}") from error
