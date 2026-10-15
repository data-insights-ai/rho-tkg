#!/usr/bin/env python3
"""Explicit fresh workload scheduling; scheduling is not family/RTT acceptance.

The canonical eight-family matrix remains intact. Genuine FDB contention and
snapshot-selectors/local source drivers exist here; every unsupported
family/engine/transport refuses BEFORE directories, builds or cluster effects.
"""
import argparse
import hashlib
import itertools
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
FAMILIES = {
    "partition-local-create-update": "partition-local create/update",
    "cross-edge-endpoint-uniqueness": "cross-edge with endpoint-life and uniqueness checks",
    "contention-empty-range": "contended key and empty-range predicate",
    "snapshot-selectors": "Fresh/Stable/After/At reads",
    "late-correction-old-index": "late state correction and retained old-cut index scan",
    "snapshot-to-feed": "snapshot-to-feed replay",
    "immutable-columns": "immutable column publication/scan",
    "transfer-retained-pins": "transfer under write and retained pins",
}
FACTS, CROSS, WRITERS, LAYOUTS = (10_000, 100_000, 1_000_000), (0, 10, 50, 100), (1, 8, 32), ("kv", "columns")
TRANSPORTS = {"local": 0, "rtt-1ms": 1, "rtt-20ms": 20, "rtt-100ms": 100}
IMPLEMENTED = ("contention-empty-range", "snapshot-selectors", "snapshot-to-feed")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def matrix(families=tuple(FAMILIES), transports=tuple(TRANSPORTS)):
    """11520 cells/engine across all8 families and local+3 RTT profiles.

    Five repetitions are independent fresh databases, not repeat queries on one
    retained cache. A cell here is a proposed schedule, never a result/skip.
    """
    require(all(f in FAMILIES for f in families) and all(t in TRANSPORTS for t in transports), "unknown canonical workload dimension")
    for family, facts, cross, writers, layout, transport, repetition in itertools.product(families, FACTS, CROSS, WRITERS, LAYOUTS, transports, range(1, 6)):
        yield {"version": 1, "engine": "foundationdb", "workload": {"version": 1, "family": family, "facts": facts, "crossPercent": cross, "writers": writers, "layout": layout},
               "transport": {"kind": "local" if transport == "local" else "injected-rtt", "targetRTTMillis": TRANSPORTS[transport], "jitterMillis": 0, "seed": 20261009},
               "repetition": repetition, "implemented": family in IMPLEMENTED and transport == "local",
               "scope": "schedule only; missing family semantics/real directed shaping refuse execution, not accepted skips"}


def preflight(spec, run, engine="foundationdb", transport="local"):
    require(type(spec) is dict and set(spec) == {"version", "family", "facts", "crossPercent", "writers", "layout"}, "explicit fresh workload schema")
    require(type(spec["version"]) is int and spec["version"] == 1 and spec["family"] in IMPLEMENTED and engine == "foundationdb" and transport == "local", "family/engine/real transport driver not implemented")
    require(type(spec["facts"]) is int and 20 <= spec["facts"] <= 1_000_000 and spec["facts"] % 20 == 0 and type(spec["crossPercent"]) is int and spec["crossPercent"] in CROSS and type(spec["writers"]) is int and spec["writers"] in WRITERS and spec["layout"] in LAYOUTS, "fresh family shape")
    require(spec["family"] not in ("snapshot-selectors", "snapshot-to-feed") or spec["facts"] >= spec["writers"] + 1, "seed plus one genuine initial job per independent caller")
    require(type(run) is str and len(run) == 32 and bytes.fromhex(run).hex() == run and bytes.fromhex(run) != bytes(16), "explicit nonzero fresh run identity")
    return spec


def comparison_grant(facts, writers, disk, cache):
    explicit = disk is not None and cache is not None
    require((disk is None) == (cache is None), "declare disk and cache grants together")
    require(explicit or facts < 1_000_000 and writers != 32, "1M-fact/32-writer cells require explicit finite --spool-disk-bytes and --spool-cache-bytes; default policy is not capacity evidence")
    return (disk, cache, "explicit-caller") if explicit else (1 << 30, 8 << 20, "small-default")


def execute(cell, output, seconds, profile, memory, spool_disk, spool_cache, grant_origin="explicit-caller"):
    """One finite fresh run with existing launcher owned cleanup/accounting."""
    transport = "local" if cell["transport"] == {"kind": "local", "targetRTTMillis": 0, "jitterMillis": 0, "seed": 20261009} else "unsupported"
    run = secrets.token_hex(16)
    spec = preflight(cell["workload"], run, cell["engine"], transport)
    require(grant_origin in ("explicit-caller", "small-default") and (grant_origin == "explicit-caller" or spec["facts"] < 1_000_000 and spec["writers"] != 32), "large cells require explicit finite verifier grant")
    require(type(seconds) is int and 1 <= seconds <= 1800 and profile in ("small", "large"), "finite declared run/profile policy")
    require(memory >= (3072 if profile == "small" else 10240), "all four servers+client must fit declared aggregate allowance")
    # Private temporary verifier state only. No engine/backend replacement.
    from comparison_spool import Policy
    policy = Policy(spool_disk, spool_cache, 4096, 512 << 10)
    policy.validate()
    destination = Path(output)
    destination.mkdir(mode=0o700, parents=False, exist_ok=False)
    selector = spec["family"] in ("snapshot-selectors", "snapshot-to-feed")
    feed = spec["family"] == "snapshot-to-feed"
    originals = 3 + 2 * spec["facts"] * 64 if selector else 3 + spec["facts"] // 2 + 64 * spec["facts"] + spec["facts"] // 2 * max(2, spec["writers"])
    request = {"cell": cell, "run": run, "spoolPolicy": {"max_disk_bytes": spool_disk, "cache_bytes": spool_cache, "grantOrigin": grant_origin},
               "nativeAdmission": {"maxOriginals": originals, "logicalGraphBytes": (64 << 20) + spec["facts"] * ((64 if selector else 128) << 10) + originals * 2048,
                                   "retainedVersions": 3 + 6 * spec["facts"], "capacityQualified": False}, "deadlineSeconds": seconds,
               "deploymentProfile": profile, "admissionMiB": memory}
    if feed:
        pages = 3 + 2 * spec["facts"]
        calls = 8 * (2 * pages + 32)
        request["feedAdmission"] = {"maxPages": pages, "pageRows": 8, "pageBytes": 64 << 10, "maxCalls": calls,
                                    "maxControlBytes": (64 << 20) + pages * (2 * (64 << 10) + 8192),
                                    "maxPrepaidWorkRows": calls * (4096 + 128 * 8), "maxPrepaidWorkBytes": calls * (128 << 20),
                                    "cleanupTickets": 64, "cleanupInvocationsPerTicket": 8, "cleanupBytesPerInvocation": 64 << 20,
                                    "scope": "finite persisted logical admission, post-SDK page admission; no measured IO/RSS/capacity claim"}
    with (destination / "request.json").open("x") as out:
        json.dump(request, out, indent=2)
        out.write("\n")
    env = {**os.environ, "RHO_GRAPH_WORKLOAD_SPEC": json.dumps(spec, separators=(",", ":")), "RHO_GRAPH_WORKLOAD_RUN": run,
           "FDB_COST_SECONDS": str(seconds), "FDB_ADMISSION_MIB": str(memory)}
    with (destination / "launcher.stdout").open("x") as out, (destination / "launcher.stderr").open("x") as error:
        # run-local owns monitored containers/volumes and its EXIT cleanup. Its
        # own finite phases are authoritative; do not timeout only the Docker CLI.
        completed = subprocess.run(["bash", str(ROOT / "fdb/run-local.sh"), "workload-cell", profile], env=env, stdout=out, stderr=error, check=False)
    require(completed.returncode == 0, "fresh launcher failed; raw partial evidence retained, no completion receipt")
    with (destination / "launcher.stdout").open("rb") as source:
        source.seek(0, 2)
        require(source.tell() <= 1 << 20, "bounded launcher terminal response")
        source.seek(0)
        terminal = source.read().decode().splitlines()
    artifacts = Path(terminal[-1]).resolve(strict=True)
    require(artifacts.parent == (ROOT / "fdb/artifacts").resolve(strict=True), "foreign launcher artifact root")
    with (artifacts / "receipt.json").open() as source:
        receipt = json.load(source)
    require(receipt["success"] and receipt["exit"] == receipt["cleanup_exit"] == 0 and receipt["mode"] == "workload-cell", "actual terminal cleanup receipt required")
    # Import the private family oracle only after the actual run; it joins all
    # originals/raw EOFs/cache bytes under one finite spool policy.
    sys.path.insert(0, str(ROOT / "fdb"))
    if feed:
        import fdb_feed_check as checker
    elif selector:
        import fdb_selectors_check as checker
    else:
        import fdb_contention_check as checker
    proof = checker.verify(artifacts, destination, policy)
    prefix = "graph-workload-feed-v1-" if feed else "graph-workload-selectors-v1-" if selector else "graph-workload-contention-v1-"
    require(proof["spec"] == spec and proof["runGraph"] == prefix + spec["layout"] + "-" + run, "fresh run/native binding/result agreement")
    with (destination / "verified.json").open("x") as out:
        json.dump({"requestSHA256": hashlib.sha256((destination / "request.json").read_bytes()).hexdigest(), "artifacts": str(artifacts), "proof": proof,
                   "acceptance": "one verified family cell; full matrix/native parity/RTT/resources remain separate"}, out, indent=2)
        out.write("\n")


def execute_matrix(args):
    # Refuse absent semantics/shaping for the WHOLE selection before admitting
    # any database/build. Never silently skip unsupported cells from a matrix.
    require(args.family in IMPLEMENTED and args.transport == "local", "selected matrix family/transport implementation is absent")
    require(1 <= args.seconds <= 1800 and args.admission_mib >= (3072 if args.deployment_profile == "small" else 10240), "explicit matrix resource policy")
    disk, cache, origin = comparison_grant(1_000_000, 32, args.spool_disk_bytes, args.spool_cache_bytes)
    from comparison_spool import Policy
    Policy(disk, cache, 4096, 512 << 10).validate()
    root = Path(args.output)
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    count = 0
    with (root / "schedule.jsonl").open("x") as schedule:
        for cell in matrix((args.family,), (args.transport,)):
            schedule.write(json.dumps(cell, separators=(",", ":")) + "\n")
    with (root / "completed.jsonl").open("x") as completed:
        for cell in matrix((args.family,), (args.transport,)):
            destination = root / f"cell-{count:04d}"
            execute(cell, destination, args.seconds, args.deployment_profile, args.admission_mib, disk, cache, origin)
            completed.write(json.dumps({"cell": count, "repetition": cell["repetition"], "verified": str(destination / "verified.json")}) + "\n")
            completed.flush()
            count += 1
    require(count == 360, "all3 facts/4 cross/3 writers/2 layouts/5 fresh repetitions must complete")
    with (root / "matrix-result.json").open("x") as output:
        json.dump({"version": 1, "family": args.family, "transport": args.transport, "freshCells": count,
                   "scope": "selected implemented family/local schedule only; remaining families, directed RTT and native parity remain open"}, output, indent=2)
        output.write("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    sub.add_parser("plan")
    admission = sub.add_parser("preflight")
    admission.add_argument("spec")
    admission.add_argument("run")
    run = sub.add_parser("run")
    run.add_argument("--family", choices=FAMILIES, required=True)
    run.add_argument("--transport", choices=TRANSPORTS, required=True)
    run.add_argument("--facts", type=int, choices=FACTS, required=True)
    run.add_argument("--cross", type=int, choices=CROSS, required=True)
    run.add_argument("--writers", type=int, choices=WRITERS, required=True)
    run.add_argument("--layout", choices=LAYOUTS, required=True)
    run.add_argument("--repetition", type=int, choices=range(1, 6), required=True)
    run.add_argument("--output", required=True)
    run.add_argument("--seconds", type=int, default=180)
    run.add_argument("--deployment-profile", choices=("small", "large"), default="small")
    run.add_argument("--admission-mib", type=int, default=4096)
    run.add_argument("--spool-disk-bytes", type=int)
    run.add_argument("--spool-cache-bytes", type=int)
    campaign = sub.add_parser("run-matrix")
    campaign.add_argument("--family", choices=FAMILIES, required=True)
    campaign.add_argument("--transport", choices=TRANSPORTS, required=True)
    campaign.add_argument("--output", required=True)
    campaign.add_argument("--seconds", type=int, default=180)
    campaign.add_argument("--deployment-profile", choices=("small", "large"), default="small")
    campaign.add_argument("--admission-mib", type=int, default=4096)
    campaign.add_argument("--spool-disk-bytes", type=int)
    campaign.add_argument("--spool-cache-bytes", type=int)
    args = parser.parse_args()
    if args.mode == "plan":
        for cell in matrix():
            print(json.dumps(cell, separators=(",", ":")))
    elif args.mode == "preflight":
        preflight(json.loads(args.spec), args.run)
    elif args.mode == "run-matrix":
        execute_matrix(args)
    else:
        cell = next(cell for cell in matrix((args.family,), (args.transport,)) if
                    (cell["workload"]["facts"], cell["workload"]["crossPercent"], cell["workload"]["writers"], cell["workload"]["layout"], cell["repetition"]) ==
                    (args.facts, args.cross, args.writers, args.layout, args.repetition))
        disk, cache, origin = comparison_grant(args.facts, args.writers, args.spool_disk_bytes, args.spool_cache_bytes)
        execute(cell, args.output, args.seconds, args.deployment_profile, args.admission_mib, disk, cache, origin)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError) as error:
        print("workload dispatcher refused: " + str(error), file=sys.stderr)
        sys.exit(1)
