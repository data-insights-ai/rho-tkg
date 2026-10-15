#!/usr/bin/env python3
"""Fresh genuine native contention cells and explicit local FDB answer joins.

This controller executes only a supplied real native test binary. It never
builds during dispatch, reconstructs Hosts, manufactures source receipts, or
labels unimplemented FDB directed RTT as a paired result.
"""
import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import secrets
import struct
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from comparison_spool import Policy, Spool, u64
import native_contention_check as native

require = native.require
ARTIFACTS = frozenset(("result.json", "grants.jsonl", "grant-replays.jsonl", "operations.jsonl", "points.jsonl", "sources.jsonl", "processes.jsonl", "restore.jsonl"))
TRANSPORTS = {"local": 0, "rtt-1ms": 1, "rtt-20ms": 20, "rtt-100ms": 100}


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as source:
        while block := source.read(1 << 20): h.update(block)
    return h.hexdigest()


def bounded_json(path, maximum=native.LINE):
    with Path(path).open("rb") as source: body = source.read(maximum+1)
    require(0 < len(body) <= maximum, "bounded controller/terminal receipt")
    return json.loads(body, object_pairs_hook=native.raw.pairs,
                      parse_constant=lambda _: require(False, "nonfinite controller receipt"))


def preflight(spec, transport, seconds, memory_mib, engine="native-raft"):
    native.shape(spec)
    require(transport in TRANSPORTS and type(seconds) is int and 1 <= seconds <= native.MAX_SECONDS
            and type(memory_mib) is int and 4096 <= memory_mib <= 1 << 20, "explicit finite native run/time/process-tree resource policy")
    require(engine in ("native-raft", "paired") and (engine != "paired" or transport == "local"),
            "paired FDB directed RTT is absent; refuse before any engine effects")
    require(seconds*1000 > native.causal_minimum(spec, TRANSPORTS[transport]), "chosen budget cannot meet even the necessary no-error causal RTT delay")
    require(engine != "paired" or seconds <= 1800, "paired local FDB retains its separately valid existing 1800s budget boundary")
    return {"kind": "local" if transport == "local" else "injected-rtt", "targetRTTMillis": TRANSPORTS[transport], "jitterMillis": 0, "seed": 20261009}


def executables(binary, watchdog):
    require(binary is not None and watchdog is not None, "actual native binary/watchdog required")
    binary, watchdog = Path(binary).resolve(strict=True), Path(watchdog).resolve(strict=True)
    require(binary.is_file() and os.access(binary, os.X_OK) and watchdog.is_file(), "actual executable native binary and process-tree watchdog required")
    return binary, watchdog


def hash_artifacts(directory, layout):
    expected = ARTIFACTS | ({"columns.jsonl"} if layout == "columns" else set())
    found, roots = {}, 0
    with os.scandir(directory) as entries:
        for entry in entries:
            require(not entry.is_symlink(), "owned artifact root contains a symlink")
            if entry.is_dir(follow_symlinks=False):
                roots += 1
                require(entry.name.startswith("rho-native-contention-") and roots == 1, "one genuine six-child durable directory")
            else:
                require(entry.is_file(follow_symlinks=False) and entry.name in expected, "foreign artifact instead of genuine driver output")
                found[entry.name] = digest(entry.path)
    require(found.keys() == expected and roots == 1, "complete exact native source/original artifact set")
    return found


def execute(spec, transport, output, binary, watchdog, seconds, memory_mib, policy, *, run=None):
    shaped = preflight(spec, transport, seconds, memory_mib)
    policy.validate()
    require(policy.max_key_bytes == 4096 and policy.max_value_bytes == native.LINE, "unchanged bounded shared comparison record policy")
    binary, watchdog = executables(binary, watchdog)
    run = secrets.token_hex(16) if run is None else run
    require(type(run) is str and len(run) == 32 and bytes.fromhex(run).hex() == run and any(bytes.fromhex(run)), "explicit fresh run identity")
    deadline = time.time_ns()+seconds*1_000_000_000
    require(0 < deadline <= (1 << 63)-1 and 32*seconds*1_000_000_000 <= (1 << 63)-1, "checked fixed deadline and cumulative caller duration")
    floor = native.causal_minimum(spec, TRANSPORTS[transport])
    destination = Path(output).resolve()
    destination.mkdir(mode=0o700, parents=False, exist_ok=False)
    artifact_directory = destination/"engine"
    # Explicit selection overrides inherited opt-in profiles. Each child
    # independently authenticates its immutable profile before native Open.
    env = {**os.environ, "RHO_GRAPH_NATIVE_WORKLOAD": native.FORMAT,
           "RHO_GRAPH_WORKLOAD_SPEC": json.dumps(spec, separators=(",", ":")), "RHO_GRAPH_WORKLOAD_RUN": run,
           "RHO_GRAPH_WORKLOAD_TRANSPORT": json.dumps(shaped, separators=(",", ":")),
           "RHO_GRAPH_WORKLOAD_SECONDS": str(seconds),
           "RHO_GRAPH_WORKLOAD_DEADLINE": str(deadline), "RHO_GRAPH_WORKLOAD_OUTPUT": str(artifact_directory),
           "GOMAXPROCS": "2", "GOMEMLIMIT": "2GiB", "GOWORK": "off", "GOTOOLCHAIN": "local"}
    for name in ("RHO_GRAPH_COST_SPEC", "RHO_GRAPH_COST_BLOCK_SIZE", "RHO_GRAPH_RANGE_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS", "RHO_GRAPH_DURABLE_COLUMN_PROCESS", "RHO_GRAPH_NATIVE_FAULT_PROCESS", "RHO_GRAPH_FAULT_FAMILY_PROCESS", "RHO_GRAPH_DURABLE_MODEL", "RHO_GRAPH_DECISION_FAULT_PROCESS"):
        env[name] = ""
    command = [sys.executable, str(watchdog), "--rss-mib", str(memory_mib), "--seconds", str(seconds),
               "--report", str(destination/"watchdog.json"), "--", str(binary), "-test.run=^TestNativeContentionWorkloadCell$",
               "-test.count=1", "-test.v", f"-test.timeout={seconds+30}s"]
    request = {"version": 1, "format": native.FORMAT, "spec": spec, "run": run, "transport": shaped,
               "binarySHA256": digest(binary), "watchdogSHA256": digest(watchdog), "deadlineUnixNanos": deadline,
               "seconds": seconds, "BudgetSeconds": seconds, "CausalMinimumMillis": floor,
               "aggregateRSSMiB": memory_mib, "command": command,
               "spoolPolicy": {"max_disk_bytes": policy.max_disk_bytes, "cache_bytes": policy.cache_bytes,
                               "max_key_bytes": policy.max_key_bytes, "max_value_bytes": policy.max_value_bytes},
               "scope": "fresh native source cell; fixed actual deadline, no capacity/full-matrix acceptance"}
    with (destination/"request.json").open("x") as stream: json.dump(request, stream, indent=2); stream.write("\n")
    started = time.monotonic_ns()
    receipt = {"version": 1, "format": native.FORMAT, "requestSHA256": digest(destination/"request.json"),
               "binarySHA256": request["binarySHA256"], "run": run, "spec": spec, "transport": shaped,
               "elapsedNanos": 0, "exit": None, "processTree": None, "processTreeSHA256": None,
               "completedCell": False, "artifacts": {}, "proof": None,
               "scope": "all native test-parent/descendant voters; sampled peak process-tree RSS; framed IO layers overlap, not pure fsync; no independent-host or FDB RTT claim"}
    try:
        with (destination/"stdout.txt").open("xb") as out, (destination/"stderr.txt").open("xb") as error:
            completed = subprocess.run(command, env=env, stdout=out, stderr=error, check=False)
        receipt["exit"] = completed.returncode
        meter = bounded_json(destination/"watchdog.json", 65536)
        receipt["processTree"] = meter
        receipt["processTreeSHA256"] = digest(destination/"watchdog.json")
        require(completed.returncode == meter["exit_code"] == 0 and meter["reason"] == "completed"
                and meter["wall_limit_s"] == seconds and meter["rss_limit_mib"] == memory_mib
                and meter["command"] == command[command.index("--")+1:]
                and type(meter["peak_tree_rss_mib"]) in (int, float) and math.isfinite(meter["peak_tree_rss_mib"])
                and 0 < meter["peak_tree_rss_mib"] <= memory_mib, "genuine completed owned process-tree admission/cleanup receipt")
        hashes = hash_artifacts(artifact_directory, spec["layout"])
        proof = native.verify(artifact_directory, destination, policy, binary_sha=request["binarySHA256"], expected_spec=spec, expected_run=run, expected_deadline=deadline, expected_seconds=seconds)
        require(proof["transport"] == shaped, "actual configured/observed owned transport differs from selected cell")
        receipt.update(artifacts=hashes, proof=proof, completedCell=True)
    finally:
        receipt["elapsedNanos"] = time.monotonic_ns()-started
        with (destination/"receipt.json").open("x") as stream: json.dump(receipt, stream, indent=2); stream.write("\n")
    return destination


class NamespacedSpool:
    """Two engines share one finite database/cache/file ledger, disjoint keys."""
    def __init__(self, spool, namespace): self.spool, self.namespace = spool, namespace
    def find(self, namespace, key): return self.spool.find(self.namespace+namespace, key)
    def lookup(self, namespace, key): return self.spool.lookup(self.namespace+namespace, key)
    def insert(self, namespace, key, value): return self.spool.insert(self.namespace+namespace, key, value)
    def replace(self, namespace, key, expected, value): return self.spool.replace(self.namespace+namespace, key, expected, value)
    def walk(self, namespace): return self.spool.walk(self.namespace+namespace)
    def report(self): return self.spool.report()


def compare(native_directory, fdb_directory, spool_directory, policy):
    """Authenticate both actual runs again under ONE unchanged finite spool."""
    native_directory, fdb_directory = Path(native_directory).resolve(strict=True), Path(fdb_directory).resolve(strict=True)
    receipt, request = bounded_json(native_directory/"receipt.json"), bounded_json(native_directory/"request.json")
    require(receipt["completedCell"] is True and receipt["exit"] == 0 and receipt["requestSHA256"] == digest(native_directory/"request.json")
            and receipt["binarySHA256"] == request["binarySHA256"] and receipt["spec"] == request["spec"] and receipt["run"] == request["run"]
            and receipt["transport"] == request["transport"] and receipt["processTreeSHA256"] == digest(native_directory/"watchdog.json")
            and receipt["processTree"] == bounded_json(native_directory/"watchdog.json", 65536) and receipt["transport"]["kind"] == "local"
            and receipt["transport"]["targetRTTMillis"] == 0, "genuine local native receipt before paired join")
    require(hash_artifacts(native_directory/"engine", receipt["spec"]["layout"]) == receipt["artifacts"], "changed native source artifacts")
    fdb_receipt = bounded_json(fdb_directory/"receipt.json")
    require(fdb_receipt["success"] is True and fdb_receipt["exit"] == fdb_receipt["cleanup_exit"] == 0 and fdb_receipt["mode"] == "workload-cell", "actual FDB owned launcher cleanup receipt")
    required = {"fdb-workload-result.json", "fdb-workload-attempts.jsonl", "fdb-workload-operations.jsonl", "fdb-workload-latency.jsonl",
                "fdb-workload-history.jsonl", "fdb-workload-current.jsonl", "fdb-workload-cdc.jsonl"}
    if receipt["spec"]["layout"] == "columns": required.add("fdb-workload-columns.jsonl")
    require(required <= fdb_receipt["sha256"].keys(), "complete actual FDB raw/original artifacts must be hash-bound")
    for name in required:
        require(digest(fdb_directory/name) == fdb_receipt["sha256"][name], "changed actual FDB source artifact")
    module_spec = importlib.util.spec_from_file_location("paired_contention_fdb", ROOT/"fdb/fdb_contention_check.py")
    fdb = importlib.util.module_from_spec(module_spec); module_spec.loader.exec_module(fdb)
    end = bounded_json(fdb_directory/"fdb-workload-result.json")
    fdb.validate_result(end)
    require(end["spec"] == receipt["spec"], "exact family/useful dataset/writers/layout parity")
    with Spool(spool_directory, policy) as spool:
        native_spool, fdb_spool = NamespacedSpool(spool, b"native/"), NamespacedSpool(spool, b"fdb/")
        proof = native.verify(native_directory/"engine", spool_directory, policy, binary_sha=request["binarySHA256"], expected_spec=receipt["spec"], expected_run=receipt["run"], expected_deadline=request["deadlineUnixNanos"], expected_seconds=request["BudgetSeconds"], shared_spool=native_spool)
        fdb.attempts(fdb_directory, end, fdb_spool)
        fdb.verify_sources(fdb_directory, end, fdb_spool)
        fdb.columns(fdb_directory, end, fdb_spool)
        answers = hashlib.sha256()
        for namespace in (b"old-state", b"state"):
            for fact in range(end["spec"]["facts"]):
                cell = fdb_spool.find(namespace, u64(0)+fdb.fixture_edge_key(end["spec"], fact))
                answers.update(u64(fact)+bytes([fdb.cross(end["spec"], fact), cell is not None]))
                if cell is not None: answers.update(struct.pack(">q", fdb.decoded(cell)["Value"]))
        require(answers.hexdigest() == proof["logicalAnswersSHA256"], "actual old absence/final values/endpoint-life logical-role answers differ")
        report = spool.report()
    return {"version": 1, "equal": True, "family": native.FAMILY, "spec": receipt["spec"], "nativeRun": receipt["run"], "FDBGraph": end["Config"]["Graph"],
            "logicalAnswersSHA256": answers.hexdigest(), "nativeRegisteredOriginals": proof["source"]["uniqueOriginals"], "FDBOriginals": end["Attempts"],
            "spool": report, "scope": "actual local paired external answers and complete immutable original/CDC folds; internal ABORT/revision/physical ID allocations differ; no equal efficiency/full matrix/nonlocal FDB acceptance"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    run = sub.add_parser("run")
    run.add_argument("--spec", required=True); run.add_argument("--transport", choices=TRANSPORTS, required=True)
    run.add_argument("--output", required=True); run.add_argument("--binary", required=True); run.add_argument("--watchdog", required=True)
    run.add_argument("--seconds", type=int, required=True); run.add_argument("--memory-mib", type=int, required=True)
    paired = sub.add_parser("compare")
    paired.add_argument("--native", required=True); paired.add_argument("--fdb", required=True); paired.add_argument("--output", required=True)
    for p in (run, paired):
        p.add_argument("--spool-disk-bytes", type=int, required=True); p.add_argument("--spool-cache-bytes", type=int, required=True)
    args = parser.parse_args()
    policy = Policy(args.spool_disk_bytes, args.spool_cache_bytes, 4096, native.LINE)
    if args.mode == "run":
        print(execute(json.loads(args.spec, object_pairs_hook=native.raw.pairs), args.transport, args.output, args.binary, args.watchdog, args.seconds, args.memory_mib, policy))
    else:
        policy.validate()
        destination = Path(args.output); destination.mkdir(mode=0o700, parents=False, exist_ok=False)
        proof = compare(args.native, args.fdb, destination, policy)
        with (destination/"paired.json").open("x") as stream: json.dump(proof, stream, indent=2); stream.write("\n")


if __name__ == "__main__":
    try: main()
    except (ValueError, KeyError, TypeError, OSError) as error:
        print("native workload cell refused: "+str(error), file=sys.stderr)
        sys.exit(1)
