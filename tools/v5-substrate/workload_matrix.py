#!/usr/bin/env python3
"""Explicit fresh workload scheduling; scheduling is not family/RTT acceptance.

The canonical eight-family matrix remains intact. Genuine native contention
and owned native delays join the existing local FDB drivers; every unsupported
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
ENGINES = ("foundationdb", "native-raft", "paired")
IMPLEMENTED = ("contention-empty-range", "snapshot-selectors", "snapshot-to-feed", "cross-edge-endpoint-uniqueness", "immutable-columns", "late-correction-old-index", "transfer-retained-pins", "partition-local-create-update")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def implemented(family, engine, transport, owned=False):
    return (family in IMPLEMENTED and (family != "transfer-retained-pins" or owned) and (transport == "local" or owned) if engine == "foundationdb" else
            family == "contention-empty-range" and (engine == "native-raft" or engine == "paired" and transport == "local")
            or family in ("cross-edge-endpoint-uniqueness", "snapshot-to-feed", "snapshot-selectors", "partition-local-create-update", "immutable-columns", "late-correction-old-index") and (engine == "native-raft" or engine == "paired" and transport == "local"))


def matrix(families=tuple(FAMILIES), transports=tuple(TRANSPORTS), engines=("foundationdb",), owned=False, retained_only=False):
    """11520 cells/engine across all8 families and local+3 RTT profiles.

    Five repetitions are independent fresh databases, not repeat queries on one
    retained cache. A cell here is a proposed schedule, never a result/skip.
    """
    require(all(f in FAMILIES for f in families) and all(t in TRANSPORTS for t in transports) and all(e in ENGINES for e in engines), "unknown canonical workload dimension")
    require(not retained_only or tuple(families)==("snapshot-to-feed",) and all(e in ENGINES for e in engines) and (not any(e!="native-raft" for e in engines) or owned) and ("paired" not in engines or tuple(transports)==("local",)),"retained-only requires genuine owned FDB adapter and local-only complete pairing")
    for engine, family, facts, cross, writers, layout, transport, repetition in itertools.product(engines, families, FACTS, CROSS, WRITERS, LAYOUTS, transports, range(1, 6)):
        yield {"version": 1, "engine": engine, "workload": {"version": 3 if retained_only else 2 if owned and engine=="foundationdb" and family in IMPLEMENTED else 1, "family": family, "facts": facts, "crossPercent": cross, "writers": writers, "layout": layout},
               "transport": {"kind": "local" if transport == "local" else "injected-rtt", "targetRTTMillis": TRANSPORTS[transport], "jitterMillis": 0, "seed": 20261009},
               "repetition": repetition, "implemented": implemented(family, engine, transport, owned),
               "scope": "schedule only; missing family semantics/real directed shaping refuse execution, not accepted skips"}


def preflight(spec, run, engine="foundationdb", transport="local"):
    require(type(spec) is dict and set(spec) == {"version", "family", "facts", "crossPercent", "writers", "layout"}, "explicit fresh workload schema")
    retained = type(spec["version"]) is int and spec["version"]==3 and spec["family"]=="snapshot-to-feed" and engine in ENGINES and (engine!="paired" or transport=="local")
    fresh = type(spec["version"]) is int and spec["version"] == 2 and spec["family"] in IMPLEMENTED and engine == "foundationdb"
    require(type(spec["version"]) is int and (spec["version"] == 1 and (engine != "foundationdb" or transport == "local") or fresh or retained) and engine in ENGINES and transport in TRANSPORTS
            and implemented(spec["family"], engine, transport, fresh or retained and engine=="foundationdb"), "family/engine/real transport driver not implemented")
    require(type(spec["facts"]) is int and 20 <= spec["facts"] <= 1_000_000 and spec["facts"] % 20 == 0 and type(spec["crossPercent"]) is int and spec["crossPercent"] in CROSS and type(spec["writers"]) is int and spec["writers"] in WRITERS and spec["layout"] in LAYOUTS, "fresh family shape")
    require(spec["family"] not in ("snapshot-selectors", "snapshot-to-feed", "immutable-columns", "transfer-retained-pins", "partition-local-create-update") or spec["facts"] >= spec["writers"] + 1, "seed plus one genuine initial job per independent caller")
    require(spec["family"]!="late-correction-old-index" or spec["facts"]//2>spec["writers"],"half correction cohort funds seed plus a genuine job per caller")
    require(type(run) is str and len(run) == 32 and bytes.fromhex(run).hex() == run and bytes.fromhex(run) != bytes(16), "explicit nonzero fresh run identity")
    return spec


def comparison_grant(facts, writers, disk, cache):
    explicit = disk is not None and cache is not None
    require((disk is None) == (cache is None), "declare disk and cache grants together")
    require(explicit or facts < 1_000_000 and writers != 32, "1M-fact/32-writer cells require explicit finite --spool-disk-bytes and --spool-cache-bytes; default policy is not capacity evidence")
    return (disk, cache, "explicit-caller") if explicit else (1 << 30, 8 << 20, "small-default")


def run_seconds(seconds, engine):
    if engine == "foundationdb":
        seconds = 180 if seconds is None else seconds
        require(type(seconds) is int and 1 <= seconds <= 1800, "unchanged ordinary FDB default180/cap1800")
    else:
        require(type(seconds) is int and 1 <= seconds <= 365*24*60*60, "fresh native run requires explicit finite --seconds; no implicit completion budget")
        require(engine != "paired" or seconds <= 1800, "paired local requires a separately valid existing FDB run budget")
    return seconds


def owned_transport_input(spec, run, transport, seconds, options):
    sys.path.insert(0, str(ROOT / "fdb"))
    import owned_transport as owned
    return owned.validate_input({"Version":3 if spec["version"]==3 else 2,"Spec":spec,"Run":run,"Transport":transport,"BudgetSeconds":seconds,**options,"ToolsDirectory":"@pinned-prebuild"})


def transport_options(args):
    values = (args.transport_queue_packets,args.transport_sample_millis,args.transport_max_samples,args.transport_artifact_bytes)
    require(args.fdb_owned_transport or all(v is None for v in values), "transport grants require explicit fresh FDB adapter")
    if not args.fdb_owned_transport: return None
    require((args.engine == "foundationdb" or args.engine=="paired" and args.family=="snapshot-to-feed" and getattr(args,"feed_retention","snapshot-C")=="retained-only" and args.transport=="local") and args.family in IMPLEMENTED and all(v is not None for v in values), "fresh implemented-family transport requires all four finite output/packet grants")
    return dict(zip(("QueuePackets","SampleMillis","MaxSamples","MaxArtifactBytes"),values,strict=True))


def transfer_grant(path, spec):
    require(path is not None,"transfer requires explicit --transfer-grant native storage/census policy")
    from fdb_transfer_check import validate_grant
    with Path(path).open("rb") as source:body=source.read(16385)
    require(0<len(body)<=16384,"bounded caller-owned transfer policy file")
    return validate_grant(json.loads(body),spec)


def retained_feed_grant(path,spec):
    require(path is not None,"retained-only FDB requires explicit --retained-feed-grant")
    from fdb_feed_retained_check import validate_grant
    with Path(path).open("rb") as source:body=source.read(16385)
    require(0<len(body)<=16384,"bounded original owner/allocator/census grant")
    return validate_grant(json.loads(body),spec)


def execute(cell, output, seconds, profile, memory, spool_disk, spool_cache, grant_origin="explicit-caller", native_binary=None, watchdog=None, owned_options=None, transfer_policy=None, retained_policy=None, run_override=None):
    """One finite fresh run with existing launcher owned cleanup/accounting."""
    transport = next((name for name, delay in TRANSPORTS.items() if cell["transport"] ==
                      {"kind": "local" if name == "local" else "injected-rtt", "targetRTTMillis": delay, "jitterMillis": 0, "seed": 20261009}), "unsupported")
    run = secrets.token_hex(16) if run_override is None else run_override
    retained=cell["workload"]["version"]==3 and cell["workload"]["family"]=="snapshot-to-feed"
    require(owned_options is None or (cell["engine"] == "foundationdb" or retained and cell["engine"]=="paired" and transport=="local") and cell["workload"]["family"] in IMPLEMENTED, "no nominal paired/unsupported-family FDB shaping")
    actual_cell = cell if owned_options is None else {**cell,"workload":{**cell["workload"],"version":3 if retained else 2}}
    spec = preflight(actual_cell["workload"], run, cell["engine"], transport)
    transfer=spec["family"]=="transfer-retained-pins"
    require(transfer == (transfer_policy is not None),"transfer native grant is explicit and cannot silently affect another family")
    if transfer:
        from fdb_transfer_check import validate_grant
        transfer_policy=validate_grant(transfer_policy,spec)
    require(cell["engine"] != "foundationdb" or (spec["version"] in (2,3)) == (owned_options is not None), "fresh FDB spec requires the actual owned adapter, not a nominal transport label")
    require((retained and cell["engine"] in ("foundationdb","paired")) == (retained_policy is not None),"retained-only owner grant cannot change an ordinary/native-only cell")
    if retained_policy is not None:
        from fdb_feed_retained_check import validate_grant
        retained_policy=validate_grant(retained_policy,spec)
    if retained and cell["engine"]=="paired":require(transport=="local" and owned_options is not None,"complete paired retained-only is local with actual owned FDB adapter")
    require(grant_origin in ("explicit-caller", "small-default") and (grant_origin == "explicit-caller" or spec["facts"] < 1_000_000 and spec["writers"] != 32), "large cells require explicit finite verifier grant")
    owned_input = None
    if owned_options is None:
        seconds = run_seconds(seconds, cell["engine"])
    else:
        owned_input = owned_transport_input(spec,run,cell["transport"],seconds,owned_options)
    require(profile in ("small", "large"), "finite declared run/profile policy")
    require(memory >= (3072 if profile == "small" else 10240), "all four servers+client must fit declared aggregate allowance")
    # Private temporary verifier state only. No engine/backend replacement.
    from comparison_spool import Policy
    policy = Policy(spool_disk, spool_cache, 4096, 512 << 10)
    policy.validate()
    if owned_input is not None:
        # Same caller-owned spool, not a second renewable per-phase allowance.
        # Reserve worst admitted native-output joins plus fixed control pages;
        # remaining graph state is still subject to this ONE finite grant.
        reserve = ((owned_input["MaxSamples"]+4)*5+60)*((128<<10)+4096)+(64<<20)
        require(spool_disk>reserve,"explicit verifier disk grant cannot fund even declared transport joins; refuse before build/workload")
    if retained and cell["engine"]=="paired":
        import feed_retained_pair as paired
        paired.execute(spec,transport,Path(output).absolute(),native_binary,watchdog,seconds,memory,policy,profile=profile,owned_options=owned_options,owner_grant=retained_policy,run=run)
        return
    if cell["engine"] != "foundationdb":
        if spec["family"] == "cross-edge-endpoint-uniqueness":
            import native_edge_unique_cell as native
        elif spec["family"] == "snapshot-to-feed":
            if spec["version"]==3:import native_feed_retained_cell as native
            else:import native_feed_cell as native
        elif spec["family"] == "snapshot-selectors":
            import native_selectors_cell as native
        elif spec["family"] == "partition-local-create-update":
            import native_create_update_cell as native
        elif spec["family"] == "immutable-columns":
            import native_immutable_columns_cell as native
        elif spec["family"] == "late-correction-old-index":
            import native_late_correction_cell as native
        else:
            import native_workload_cell as native
        native.preflight(spec, transport, seconds, memory, cell["engine"])
        native_binary, watchdog = native.executables(native_binary, watchdog)
        # The fresh edge adapter supplies the actual bounded workload owner
        # also at0ms. Legacy direct local-v1 execution remains available and
        # its original admission bytes are never upgraded in place.
        native_options = {"owned_transport": True} if spec["family"] == "cross-edge-endpoint-uniqueness" else {}
        # Verify both local adapters before either engine starts. Unsupported
        # FDB shaping never turns into a nominally paired RTT result.
        if cell["engine"] == "paired": preflight(spec, run, "foundationdb", transport)
        destination = Path(output)
        if cell["engine"] == "native-raft":
            native.execute(spec, transport, destination, native_binary, watchdog, seconds, memory, policy, run=run, **native_options)
            receipt = native.bounded_json(destination / "receipt.json")
            proof, artifacts = receipt["proof"], str((destination / "engine").resolve(strict=True))
        else:
            destination.mkdir(mode=0o700, parents=False, exist_ok=False)
            native.execute(spec, transport, destination / "native", native_binary, watchdog, seconds, memory, policy, run=run, **native_options)
            execute({**cell, "engine": "foundationdb"}, destination / "fdb", seconds, profile, memory, spool_disk, spool_cache, grant_origin)
            fdb = native.bounded_json(destination / "fdb/verified.json")
            proof = native.compare(destination / "native", fdb["artifacts"], destination, policy)
            artifacts = {"native": str(destination / "native/engine"), "foundationdb": fdb["artifacts"]}
        with (destination / "verified.json").open("x") as out:
            json.dump({"cell": cell, "artifacts": artifacts, "proof": proof,
                       "acceptance": "one actual fresh cell; runtime capacity/full matrix and remaining D source are separate"}, out, indent=2)
            out.write("\n")
        return
    destination = Path(output)
    destination.mkdir(mode=0o700, parents=False, exist_ok=False)
    selector = spec["family"] in ("snapshot-selectors", "snapshot-to-feed", "immutable-columns", "late-correction-old-index", "transfer-retained-pins", "partition-local-create-update")
    create_update = spec["family"] == "partition-local-create-update"
    columns = spec["family"] == "immutable-columns"
    correction = spec["family"] == "late-correction-old-index"
    feed = spec["family"] == "snapshot-to-feed"
    edge_unique = spec["family"] == "cross-edge-endpoint-uniqueness"
    originals = (3+2*spec["facts"])*64 if retained else 7 + 258 * spec["facts"] if edge_unique else 3 + 2 * spec["facts"] * 64 if selector else 3 + spec["facts"] // 2 + 64 * spec["facts"] + spec["facts"] // 2 * max(2, spec["writers"])
    if correction: originals=3+(spec["facts"]+spec["facts"]//2)*64
    request = {"cell": actual_cell, "run": run, "spoolPolicy": {"max_disk_bytes": spool_disk, "cache_bytes": spool_cache, "grantOrigin": grant_origin},
               "nativeAdmission": {"maxOriginals": originals, "logicalGraphBytes": (64 << 20) + spec["facts"] * ((256 if edge_unique else 64 if selector else 128) << 10) + originals * 2048,
                                   "retainedVersions": 7 + 11 * spec["facts"] if edge_unique else 3+3*spec["facts"]+3*(spec["facts"]//2) if correction else 3 + 6 * spec["facts"], "capacityQualified": False}, "deadlineSeconds": seconds,
               "deploymentProfile": profile, "admissionMiB": memory}
    if owned_input is not None: request["ownedTransport"] = owned_input
    if create_update:
        request["businessSemantics"]={"RowSemantics":"strict-partition-local-create-update" if spec["crossPercent"]==0 else "mixed-or-cross-partition-create-update","setupExcluded":True,"crossPercentBasis":"complete actual business reads/effects, not metadata, client location or empty coordination","IDAdmission":"fixed declared fixture IDs; no allocator issuance claim"}
    if retained:request["retainedOnlyAdmission"]={"Policy":retained_policy,"MinimumContract":"snapshot-relinquished-complete-feed-v1","physicalRetention":"FDB detached CDC/originals versus conservative native rolling ACK predecessor; not equal physical GC"}
    if transfer:request["transferAdmission"]={"Policy":transfer_policy,"Scope":"finite native grant; stable logical buckets versus processing owners; explicit C-retaining then feed-only history-GC phases; no matched native retention verdict"}
    if feed or transfer:
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
    require(not os.environ.get("RHO_GRAPH_WORKLOAD_TRANSPORT") and not os.environ.get("RHO_FDB_TRANSPORT_INPUT"), "transport authority must come from this fresh explicit request")
    if owned_input is not None:
        env.update({"RHO_GRAPH_WORKLOAD_TRANSPORT":"fdb-owned-ingress-transport-v3" if retained else "fdb-owned-ingress-transport-v2","RHO_FDB_TRANSPORT_INPUT":json.dumps(owned_input,separators=(",",":")),"RHO_GRAPH_WORKLOAD_SECONDS":str(seconds)})
    if retained:env["RHO_FDB_FEED_RETAINED_GRANT"]=json.dumps(retained_policy,separators=(",",":"))
    if transfer:env["RHO_FDB_TRANSFER_GRANT"]=json.dumps(transfer_policy,separators=(",",":"))
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
    if create_update:
        import fdb_create_update_check as checker
    elif transfer:
        import fdb_transfer_check as checker
    elif correction:
        import fdb_correction_check as checker
    elif columns:
        import fdb_columns_check as checker
    elif edge_unique:
        import fdb_edge_unique_check as checker
    elif feed:
        if retained:import fdb_feed_retained_check as checker
        else:import fdb_feed_check as checker
    elif selector:
        import fdb_selectors_check as checker
    else:
        import fdb_contention_check as checker
    proof = checker.verify(artifacts, destination, policy)
    stem = "create-update" if create_update else "correction" if correction else "columns" if columns else "edge-unique" if edge_unique else "feed" if feed else "selectors" if selector else "contention"
    prefix = "graph-workload-feed-retained-v1-" if retained else "graph-owned-v3-" if transfer else f"graph-workload-{stem}-v{spec['version']}-"
    require(proof["spec"] == spec and proof["runGraph"] == prefix + spec["layout"] + "-" + run, "fresh run/native binding/result agreement")
    with (destination / "verified.json").open("x") as out:
        json.dump({"requestSHA256": hashlib.sha256((destination / "request.json").read_bytes()).hexdigest(), "artifacts": str(artifacts), "proof": proof,
                   "acceptance": "one verified family cell; full matrix/native parity/RTT/resources remain separate"}, out, indent=2)
        out.write("\n")


def execute_matrix(args):
    # Refuse absent semantics/shaping for the WHOLE selection before admitting
    # any database/build. Never silently skip unsupported cells from a matrix.
    retained=getattr(args,"feed_retention","snapshot-C")=="retained-only"
    require(not retained or args.family=="snapshot-to-feed" and (args.engine=="native-raft" and not args.fdb_owned_transport or args.engine=="foundationdb" and args.fdb_owned_transport or args.engine=="paired" and args.transport=="local" and args.fdb_owned_transport),"explicit real retained-only adapter required")
    options = transport_options(args)
    require(implemented(args.family, args.engine, args.transport, options is not None), "selected matrix family/engine/transport implementation is absent")
    require(args.engine != "foundationdb" or args.transport == "local" or options is not None, "nonlocal FDB matrix requires explicit fresh owned transport/grants before any directory/effect")
    seconds = run_seconds(args.seconds, args.engine) if options is None else args.seconds
    grant=None
    retained_grant=None
    require(args.retained_feed_grant is None or retained and args.engine!="native-raft","owner grant only for retained-only FDB/paired route")
    if retained and args.engine!="native-raft":retained_grant=retained_feed_grant(args.retained_feed_grant,{"version":3,"family":args.family,"facts":1_000_000,"crossPercent":100,"writers":32,"layout":"columns"})
    require(args.transfer_grant is None or args.family=="transfer-retained-pins","native transfer grant only for explicit transfer family")
    if args.family=="transfer-retained-pins":grant=transfer_grant(args.transfer_grant,{"version":2,"family":args.family,"facts":1_000_000,"crossPercent":100,"writers":32,"layout":"columns"})
    if options is not None:
        owned_transport_input({"version":3 if retained else 2,"family":args.family,"facts":1_000_000,"crossPercent":100,"writers":1,"layout":"columns"}, "01"*16,
                              {"kind":"local" if args.transport=="local" else "injected-rtt","targetRTTMillis":TRANSPORTS[args.transport],"jitterMillis":0,"seed":20261009},seconds,options)
    require(args.admission_mib >= (3072 if args.deployment_profile == "small" else 10240), "explicit matrix resource policy")
    disk, cache, origin = comparison_grant(1_000_000, 32, args.spool_disk_bytes, args.spool_cache_bytes)
    from comparison_spool import Policy
    Policy(disk, cache, 4096, 512 << 10).validate()
    if args.engine != "foundationdb":
        if args.family == "cross-edge-endpoint-uniqueness":
            import native_edge_unique_cell as native
        elif args.family == "snapshot-to-feed":
            if retained:import native_feed_retained_cell as native
            else:import native_feed_cell as native
        elif args.family == "snapshot-selectors":
            import native_selectors_cell as native
        elif args.family == "partition-local-create-update":
            import native_create_update_cell as native
        elif args.family == "immutable-columns":
            import native_immutable_columns_cell as native
        elif args.family == "late-correction-old-index":
            import native_late_correction_cell as native
        else:
            import native_workload_cell as native
        native.preflight({"version": 3 if retained else 1, "family": args.family, "facts": 1_000_000, "crossPercent": 100, "writers": 32, "layout": "columns"}, args.transport, seconds, args.admission_mib, "native-raft" if retained else args.engine)
        if retained and args.engine=="paired":
            import feed_retained_pair as paired
            paired.preflight({"version":3,"family":args.family,"facts":1_000_000,"crossPercent":100,"writers":32,"layout":"columns"},args.transport,seconds,args.admission_mib,Policy(disk,cache,4096,512<<10),options,retained_grant)
        native.executables(args.native_binary, args.watchdog)
    root = Path(args.output)
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    count = 0
    with (root / "schedule.jsonl").open("x") as schedule:
        for cell in matrix((args.family,), (args.transport,), (args.engine,), owned=options is not None,retained_only=retained):
            schedule.write(json.dumps(cell, separators=(",", ":")) + "\n")
    with (root / "completed.jsonl").open("x") as completed:
        for cell in matrix((args.family,), (args.transport,), (args.engine,), owned=options is not None,retained_only=retained):
            destination = root / f"cell-{count:04d}"
            execute(cell, destination, seconds, args.deployment_profile, args.admission_mib, disk, cache, origin, args.native_binary, args.watchdog, options, grant, retained_grant)
            completed.write(json.dumps({"cell": count, "repetition": cell["repetition"], "verified": str(destination / "verified.json")}) + "\n")
            completed.flush()
            count += 1
    require(count == 360, "all3 facts/4 cross/3 writers/2 layouts/5 fresh repetitions must complete")
    with (root / "matrix-result.json").open("x") as output:
        json.dump({"version": 1, "family": args.family, "engine": args.engine, "transport": args.transport, "freshCells": count,
                   "scope": "selected actual fresh family/engine/transport cells only; missing families, FDB directed RTT, capacity and remaining D composition stay open"}, output, indent=2)
        output.write("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    planned = sub.add_parser("plan")
    planned.add_argument("--engine", choices=ENGINES, default="foundationdb")
    admission = sub.add_parser("preflight")
    admission.add_argument("spec")
    admission.add_argument("run")
    run = sub.add_parser("run")
    run.add_argument("--family", choices=FAMILIES, required=True)
    run.add_argument("--engine", choices=ENGINES, default="foundationdb")
    run.add_argument("--transport", choices=TRANSPORTS, required=True)
    run.add_argument("--facts", type=int, choices=FACTS, required=True)
    run.add_argument("--cross", type=int, choices=CROSS, required=True)
    run.add_argument("--writers", type=int, choices=WRITERS, required=True)
    run.add_argument("--layout", choices=LAYOUTS, required=True)
    run.add_argument("--repetition", type=int, choices=range(1, 6), required=True)
    run.add_argument("--output", required=True)
    run.add_argument("--seconds", type=int)
    run.add_argument("--deployment-profile", choices=("small", "large"), default="small")
    run.add_argument("--admission-mib", type=int, default=4096)
    run.add_argument("--spool-disk-bytes", type=int)
    run.add_argument("--spool-cache-bytes", type=int)
    campaign = sub.add_parser("run-matrix")
    campaign.add_argument("--family", choices=FAMILIES, required=True)
    campaign.add_argument("--engine", choices=ENGINES, default="foundationdb")
    campaign.add_argument("--transport", choices=TRANSPORTS, required=True)
    campaign.add_argument("--output", required=True)
    campaign.add_argument("--seconds", type=int)
    campaign.add_argument("--deployment-profile", choices=("small", "large"), default="small")
    campaign.add_argument("--admission-mib", type=int, default=4096)
    campaign.add_argument("--spool-disk-bytes", type=int)
    campaign.add_argument("--spool-cache-bytes", type=int)
    for command in (run, campaign):
        command.add_argument("--feed-retention",choices=("snapshot-C","retained-only"),default="snapshot-C",help="fresh retained-only v3; owned FDB and local paired minimum contract are explicit, physical retention differs")
        command.add_argument("--native-binary")
        command.add_argument("--watchdog")
        command.add_argument("--fdb-owned-transport", action="store_true")
        command.add_argument("--transport-queue-packets", type=int)
        command.add_argument("--transport-sample-millis", type=int)
        command.add_argument("--transport-max-samples", type=int)
        command.add_argument("--transport-artifact-bytes", type=int)
        command.add_argument("--retained-feed-grant",help="explicit finite Cost10 owner/allocation/census JSON grant")
        command.add_argument("--transfer-grant", help="explicit finite native catalog/allocation/census JSON policy; transfer family only")
    args = parser.parse_args()
    if args.mode == "plan":
        for cell in matrix(engines=(args.engine,)):
            print(json.dumps(cell, separators=(",", ":")))
    elif args.mode == "preflight":
        spec=json.loads(args.spec)
        require(spec.get("version")==1,"fresh version2 requires owned_transport.py preflight and native admission; ordinary launcher cannot impersonate it")
        preflight(spec, args.run)
    elif args.mode == "run-matrix":
        execute_matrix(args)
    else:
        cell = next(cell for cell in matrix((args.family,), (args.transport,), (args.engine,)) if
                    (cell["workload"]["facts"], cell["workload"]["crossPercent"], cell["workload"]["writers"], cell["workload"]["layout"], cell["repetition"]) ==
                    (args.facts, args.cross, args.writers, args.layout, args.repetition))
        if getattr(args,"feed_retention","snapshot-C")=="retained-only":
            require(args.family=="snapshot-to-feed" and (args.engine=="native-raft" and not args.fdb_owned_transport or args.engine=="foundationdb" and args.fdb_owned_transport or args.engine=="paired" and args.transport=="local" and args.fdb_owned_transport),"retained-only requires exact fresh native/FDB adapter before effects")
            cell={**cell,"workload":{**cell["workload"],"version":3}}
        disk, cache, origin = comparison_grant(args.facts, args.writers, args.spool_disk_bytes, args.spool_cache_bytes)
        require(args.transfer_grant is None or args.family=="transfer-retained-pins","transfer grant cannot alter another family")
        transfer_spec={**cell["workload"],"version":2}
        grant=transfer_grant(args.transfer_grant,transfer_spec) if args.family=="transfer-retained-pins" else None
        require(args.retained_feed_grant is None or getattr(args,"feed_retention","snapshot-C")=="retained-only" and args.engine!="native-raft","retained owner grant only for its fresh route")
        retained_grant=retained_feed_grant(args.retained_feed_grant,cell["workload"]) if getattr(args,"feed_retention","snapshot-C")=="retained-only" and args.engine!="native-raft" else None
        execute(cell, args.output, args.seconds, args.deployment_profile, args.admission_mib, disk, cache, origin, args.native_binary, args.watchdog, transport_options(args), grant, retained_grant)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError) as error:
        print("workload dispatcher refused: " + str(error), file=sys.stderr)
        sys.exit(1)
