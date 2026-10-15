#!/usr/bin/env python3
"""Private fresh contention-family oracle. Source-only until coordinated gates.

All long-lived joins are disposable quota-bound BLOB spool state. This checks
actual native originals/physical effects; it is neither another graph backend
nor native parity/capacity acceptance. Native enumeration and invocation order
are distinct: complete CDC is folded in its genuine (Round,Coordinator,Request)
order, never in worker completion/provenance order.
"""
import base64
from contextlib import nullcontext
import hashlib
import importlib.util
import json
from pathlib import Path
import struct
import sys

PARENT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PARENT))
from comparison_spool import Policy, Spool, u64

# Reuse the existing raw-byte/column codec verification API, not its legacy
# profile dispatcher or create/update oracle. No shared checker source changes.
_spec = importlib.util.spec_from_file_location("contention_raw_codec", PARENT / "compare-cost-cdc.py")
_codec = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_codec)
_transport_spec = importlib.util.spec_from_file_location("family_owned_transport", Path(__file__).with_name("owned_transport.py"))
transport = importlib.util.module_from_spec(_transport_spec)
_transport_spec.loader.exec_module(transport)

require, fields, integer = _codec.require, _codec.fields, _codec.integer
same, go_json, blob = _codec.same, _codec.go_json, _codec.raw_bytes
FORMAT, FAMILY = "fdb-contention-empty-range-v1", "contention-empty-range"
LINE = 512 << 10
POLICY = Policy(1 << 30, 8 << 20, 4096, LINE)
SCOPE = _codec.SCOPE


def decoded(raw):
    require(0 < len(raw) <= LINE, "admit raw bytes before decode")
    value = json.loads(raw, object_pairs_hook=_codec.pairs,
                       parse_constant=lambda _: require(False, "nonfinite JSON"))
    require(go_json(value) == raw, "canonical native JSON bytes")
    return value


def lines(path, maximum):
    with Path(path).open("rb") as source:
        for _ in range(maximum):
            raw = source.readline(LINE + 1)
            if not raw:
                return
            require(len(raw) <= LINE and raw.endswith(b"\n"), "bounded complete JSONL row")
            yield decoded(raw[:-1]), raw
        require(not source.read(1), "declared artifact work limit")


def prefix(c, group=0):
    integer(group, 0, 1)
    return f'g{group}/{c["Graph"].encode().hex()}/'.encode()


def key(c, group, kind, name):
    return prefix(c, group) + kind.encode() + b"/" + name.encode().hex().encode()


def raw_record(record, expected_key, expected_body):
    fields(record, "Key Value")
    require(blob(record["Key"]) == expected_key and blob(record["Value"]) == expected_body,
            "actual native key/value observation")


def cross(p, fact):
    return (fact + 1) * p["crossPercent"] // 100 > fact * p["crossPercent"] // 100


def target(p, phase, fact):
    if phase == "seed":
        require(0 <= fact < p["facts"] // 2, "seed identity")
        return fact
    if phase == "claim":
        require(0 <= fact < p["facts"] // 2, "claim identity")
        return p["facts"] // 2 + fact
    require(phase == "hot" and 0 <= fact < p["facts"], "hot identity")
    return next(i for i in range(10) if cross(p, i) == cross(p, fact))


def operation(p, op, claimant):
    fields(op, "Dataset Phase Fact Ordinal")
    phase, fact = op["Phase"], integer(op["Fact"])
    competitors = max(2, p["writers"])
    if phase == "setup":
        require(fact < 3 and claimant == 0, "setup identity")
        ordinal = fact + 1
    elif phase == "seed":
        target(p, phase, fact)
        require(claimant == 0, "seed claimant")
        ordinal = 4 + fact
    elif phase == "hot":
        target(p, phase, fact)
        require(claimant == 0, "hot claimant")
        ordinal = 4 + p["facts"] // 2 + fact
    else:
        target(p, phase, fact)
        integer(claimant, 0, competitors - 1)
        ordinal = 4 + 3 * p["facts"] // 2 + fact * competitors + claimant
    dataset = hashlib.sha256(b"\x01" + struct.pack(">QQ", p["facts"], p["crossPercent"]) + b"graph-v2").digest()
    require(same(op, {"Dataset": list(dataset), "Phase": phase, "Fact": fact, "Ordinal": ordinal}),
            "family operation/dataset/ordinal provenance")
    return phase, fact, ordinal


def endpoint(owner, node, life):
    return {"Owner": owner, "Node": node, "Life": life}


def contract(p, phase, fact, value):
    nodes = [endpoint(0, 1, 2), endpoint(0, 3, 4), endpoint(1, 7 + 2 * p["facts"], 8 + 2 * p["facts"])]
    zero = endpoint(0, 0, 0)
    empty_edge = {"ID": 0, "Source": zero, "Target": zero, "Value": 0}
    if phase == "setup":
        node = nodes[fact]
        logical = f'node/{node["Node"]:016x}/life/{node["Life"]:016x}'
        return {"Kind": "node-create", "Node": node, "Edge": empty_edge}, {node["Owner"]: [{"Key": logical, "Value": 1, "Delete": False}]}, {node["Owner"]: [logical]}
    edge = target(p, phase, fact)
    source, destination = nodes[0], nodes[2 if cross(p, edge) else 1]
    suffix = f'{7 + 2 * edge:016x}/{source["Node"]:016x}/{source["Life"]:016x}/{destination["Node"]:016x}/{destination["Life"]:016x}'
    effects = {0: [], destination["Owner"]: []}
    for kind in ("edge", "out", "in"):
        owner = destination["Owner"] if kind == "in" else 0
        effects[owner].append({"Key": kind + "/" + suffix, "Value": value, "Delete": False})
    effects = {g: sorted(rows, key=lambda r: r["Key"]) for g, rows in effects.items()}
    reads = {0: ["edge/" + suffix, "node/0000000000000001/life/0000000000000002"]}
    reads.setdefault(destination["Owner"], []).append(f'node/{destination["Node"]:016x}/life/{destination["Life"]:016x}')
    if phase == "hot":
        reads[0].append("out/" + suffix)
        reads[destination["Owner"]].append("in/" + suffix)
    command = {"Kind": "edge-update" if phase == "hot" else "edge-create", "Node": zero,
               "Edge": {"ID": 7 + 2 * edge, "Source": source, "Target": destination, "Value": value}}
    return command, effects, {g: sorted(rows) for g, rows in reads.items()}


def fixture_edge_key(p, fact):
    destination = endpoint(1, 7 + 2 * p["facts"], 8 + 2 * p["facts"]) if cross(p, fact) else endpoint(0, 3, 4)
    return f'edge/{7 + 2 * fact:016x}/0000000000000001/0000000000000002/{destination["Node"]:016x}/{destination["Life"]:016x}'.encode()


def authenticate_attempt(row, p, c, spool):
    fields(row, "Operation Family Engine ClientID AttemptOrdinal PreviousRequest Request Transaction Target Claimant Proposal BodySHA256 Capture ReadCallbacks Submissions Classification Error Proof ElapsedNanos")
    phase, fact, ordinal = operation(p, row["Operation"], row["Claimant"])
    require(row["Family"] == FAMILY and row["Engine"] == "foundationdb", "family/engine binding")
    client = integer(row["ClientID"], 0, p["writers"] - 1)
    attempt = integer(row["AttemptOrdinal"], 1, 64 if phase == "hot" else 1)
    require((attempt == 1) == (row["PreviousRequest"] == ""), "first/replan request linkage")
    if phase == "claim":
        require(client == (0 if p["writers"] == 1 else row["Claimant"]), "real claim client assignment")
    opkey = u64(ordinal)
    previous = spool.find(b"attempt-chain", opkey)
    if attempt == 1:
        require(previous is None, "duplicate business operation first attempt")
    else:
        prior = decoded(previous) if previous is not None else None
        require(prior is not None and prior["AttemptOrdinal"] + 1 == attempt and prior["Request"] == row["PreviousRequest"] and
                prior["Classification"] == "aborted" and prior["Proof"]["Outcome"]["Reason"] in ("revision", "generation"), "new request only after exact authentic ABORT")
    body = blob(row["Proposal"])
    require(len(body) <= 64 << 10 and list(hashlib.sha256(body).digest()) == row["BodySHA256"], "actual immutable attempt body digest")
    tx = decoded(body)
    fields(tx, "Graph Topology ID Request Coordinator Dependency Participants GraphChange Lifecycle")
    for coordinate in (tx["Topology"], tx["Coordinator"], tx["Dependency"]):
        integer(coordinate)
    require(tx["ID"] == row["Transaction"] and tx["Request"] == row["Request"] == "request/" + tx["ID"] and
            tx["Graph"] == c["Graph"] and tx["Topology"] == c["Topology"] and tx["Dependency"] == 0, "native transaction/request/graph")
    value = tx["GraphChange"]["Edge"]["Value"]
    command, effects, reads = contract(p, phase, fact, value)
    require(same(tx["GraphChange"], command) and row["Target"] == (fact if phase == "setup" else target(p, phase, fact)), "actual graph operation")
    require(tx["ID"] == f'contention-{phase}-{fact}-c{client}-q{row["Claimant"]}-a{attempt}', "exact caller-established immutable attempt identity")
    metadata = {"Provenance": ordinal}
    if phase != "setup":
        require(value == 5 if phase != "hot" else 6 <= value <= 5 + p["facts"], "admitted scalar")
        metadata.update(EdgeLife=command["Edge"]["ID"] + 1, Definition={"ID": 4 * p["facts"] + 32 + value, "Kind": "i64", "I64": value})
    require(same(tx["Lifecycle"], metadata) and type(tx["Coordinator"]) is int and tx["Coordinator"] == (1 if phase == "setup" and fact == 2 else 0), "exact lifecycle definition/scope/provenance")
    capture = row["Capture"]
    fields(capture, "Metadata Records ReadVersion" + (" RangeBegin RangeEnd" if phase == "claim" else ""))
    require(0 < integer(capture["ReadVersion"]) and len(capture["Metadata"]) == len(capture["Records"]) == 2, "actual native capture")
    for g, m in enumerate(capture["Metadata"]):
        fields(m, "Floor Generation Versions Transactions Certificates Bytes Groups")
        for coordinate in m.values():
            integer(coordinate)
        raw_record(capture["Records"][g], key(c, g, "meta", "state"), go_json(m))
    require([part["Group"] for part in tx["Participants"]] == sorted(effects), "complete participant inventory")
    for part in tx["Participants"]:
        fields(part, "Group Epoch Generation Reads Effects")
        g = integer(part["Group"], 0, 1)
        integer(part["Epoch"], 1)
        integer(part["Generation"])
        require(part["Epoch"] == c["Epochs"][g] and part["Generation"] == capture["Metadata"][g]["Generation"] and
                same(part["Effects"], effects[g]) and [r["Key"] for r in part["Reads"]] == reads[g], "exact actual capture/effects/read footprint")
        for read in part["Reads"]:
            fields(read, "Key Version")
            integer(read["Version"])
    canonical_parts = [{"Group": part["Group"], "Epoch": part["Epoch"], "Generation": part["Generation"],
                        "Reads": [{"Key": r["Key"], "Version": r["Version"]} for r in part["Reads"]], "Effects": effects[part["Group"]]} for part in tx["Participants"]]
    canonical_tx = {"Graph": tx["Graph"], "Topology": tx["Topology"], "ID": tx["ID"], "Request": tx["Request"], "Coordinator": tx["Coordinator"], "Dependency": tx["Dependency"],
                    "Participants": canonical_parts, "GraphChange": command, "Lifecycle": metadata}
    require(body == go_json(canonical_tx), "canonical genuine Go transaction bytes, including nested struct order")
    if phase == "claim":
        begin = prefix(c) + b"current/" + f'edge/{command["Edge"]["ID"]:016x}/'.encode().hex().encode()
        require(blob(capture["RangeBegin"]) == begin and blob(capture["RangeEnd"]) == begin[:-1] + bytes([begin[-1] + 1]) and
                not capture.get("RangeRows"), "actual empty unique-ID range capture")
        wave = u64(fact)
        frozen = go_json(capture["Metadata"])
        prior = spool.find(b"claim-capture", wave)
        if prior is None:
            spool.insert(b"claim-capture", wave, frozen)
        else:
            require(prior == frozen, "claim wave frozen before any unrelated generation change")
    else:
        require(not capture.get("RangeBegin") and not capture.get("RangeEnd") and not capture.get("RangeRows"), "foreign range evidence")
    proof = row["Proof"]
    fields(proof, "Outcome Pair Original Group")
    o = proof["Outcome"]
    fields(o, "Commit Round Digest ID Request Reason Coordinator Sequence")
    for coordinate in (o["Round"], o["Coordinator"], o["Sequence"]):
        integer(coordinate)
    _codec.sha_array(o["Digest"])
    require(type(o["Commit"]) is bool and o["ID"] == tx["ID"] and o["Request"] == tx["Request"] and o["Coordinator"] == tx["Coordinator"] and o["Digest"] == row["BodySHA256"], "original outcome/body binding")
    require(len(proof["Pair"]) == 2, "both original authorities required")
    canonical_outcome = {name: o[name] for name in ("Commit", "Round", "Digest", "ID", "Request", "Reason", "Coordinator", "Sequence")}
    for record, kind, identity in zip(proof["Pair"], ("id", "request"), (tx["ID"], tx["Request"]), strict=True):
        raw_record(record, key(c, tx["Coordinator"], kind, identity), go_json(canonical_outcome))
    original_key = prefix(c, o["Coordinator"]) + f'changes/{o["Sequence"]:016x}/'.encode() + tx["Request"].encode().hex().encode()
    require(blob(proof["Original"]["Key"]) == original_key, "original CDC key")
    if o["Commit"]:
        require(row["Classification"] == "committed" and o["Reason"] == "" and o["Round"] > 0 and o["Sequence"] > 0 and
                proof["Group"]["Tx"] == tx and proof["Group"]["Outcome"] == o and blob(proof["Original"]["Value"]) == go_json(proof["Group"]), "complete COMMIT/CDC authority")
        spool.insert(b"committed-operation", opkey, go_json(proof["Group"]))
        spool.insert(b"fdb-operation", opkey, go_json(proof["Group"]))
    else:
        require(row["Classification"] == "aborted" and o["Round"] == o["Sequence"] == 0 and
                o["Reason"] in ("generation", "revision", "endpoint-life", "edge-identity", "graph-shape") and proof["Original"]["Value"] is None,
                "authentic retained ABORT has no committed CDC")
        zero_outcome = {"Commit": False, "Round": 0, "Digest": [0] * 32, "ID": "", "Request": "", "Reason": "", "Coordinator": 0, "Sequence": 0}
        zero_tx = {"Graph": "", "Topology": 0, "ID": "", "Request": "", "Coordinator": 0, "Dependency": 0, "Participants": None}
        require(same(proof["Group"], {"Tx": zero_tx, "Outcome": zero_outcome, "Writes": None}),
                "ABORT must not carry a fabricated committed group")
    require(row["Error"] == "" and 1 <= len(row["Submissions"]) <= 8, "complete bounded native submissions")
    integer(row["ReadCallbacks"], 1 + len(row["Submissions"]), 16 * (1 + len(row["Submissions"])))
    for invocation, submit in enumerate(row["Submissions"], 1):
        require(submit["Ordinal"] == invocation, "same-wire submission sequence")
        integer(submit["Callbacks"], 1, 16)
        require(type(submit["Retryable"]) is bool and type(submit["MaybeCommitted"]) is bool, "native SDK predicates")
        if submit["Error"]:
            require("NativeCode" not in submit or type(submit["NativeCode"]) is int, "preserve actual native code if present")
            if invocation < len(row["Submissions"]):
                require(type(submit.get("NativeCode")) is int and (submit["Retryable"] or submit["MaybeCommitted"]), "genuine ambiguity before SAME-body retry")
        else:
            require(submit["Outcome"] == o and "NativeCode" not in submit, "native successful submission exact original")
    identity = u64(o["Coordinator"]) + tx["ID"].encode()
    spool.insert(b"attempt-id", identity, go_json(proof))
    spool.insert(b"attempt-request", u64(o["Coordinator"]) + tx["Request"].encode(), body)
    if previous is None:
        spool.insert(b"attempt-chain", opkey, go_json(row))
    else:
        spool.replace(b"attempt-chain", opkey, previous, go_json(row))
    return phase, o


def fold_group(group, p, c, spool, generations, sequences, old):
    fields(group, "Tx Outcome Writes")
    tx, o = group["Tx"], group["Outcome"]
    identity = u64(o["Coordinator"]) + tx["ID"].encode()
    proof = decoded(spool.lookup(b"attempt-id", identity))
    require(proof["Group"] == group and o["Commit"], "raw CDC joins exact original attempt")
    require(go_json(group) == spool.lookup(b"operation-file", identity), "worker operation file complete group")
    ordinal = tx["Lifecycle"]["Provenance"]
    attempt = decoded(spool.lookup(b"attempt-chain", u64(ordinal)))
    phase, fact, _ = operation(p, attempt["Operation"], attempt["Claimant"])
    command, effects, reads = contract(p, phase, fact, tx["GraphChange"]["Edge"]["Value"])
    sequences[o["Coordinator"]] += 1
    require(o["Sequence"] == sequences[o["Coordinator"]], "complete coordinator CDC sequence")
    expected = [(g, effect) for g in sorted(effects) for effect in effects[g]]
    require(len(group["Writes"]) == len(expected), "all physical writes required")
    for part in tx["Participants"]:
        g = part["Group"]
        require(part["Generation"] == generations[g], "COMMIT authentic predecessor generation")
        for read in part["Reads"]:
            prior = spool.find(b"state", u64(g) + read["Key"].encode())
            require(read["Version"] == (0 if prior is None else decoded(prior)["Version"]), "COMMIT exact original read revision")
        generations[g] += 1
    canonical_writes = []
    for (g, effect), write in zip(expected, group["Writes"], strict=True):
        logical = u64(g) + effect["Key"].encode()
        prior_raw = spool.find(b"state", logical)
        prior = decoded(prior_raw) if prior_raw is not None else None
        require(write["Group"] == g and write["Key"] == effect["Key"] and same(write.get("Before"), prior), "exact physical predecessor")
        after = {"Value": effect["Value"], "Version": 1 if prior is None else prior["Version"] + 1,
                 "Round": o["Round"], "Deleted": False, "TxID": tx["ID"]}
        require(same(write["After"], after) and (prior is None or after["Round"] > prior["Round"]), "exact physical after/revision")
        canonical_write = {"Group": g, "Key": effect["Key"]}
        if prior is not None:
            canonical_write["Before"] = prior
        canonical_write["After"] = after
        canonical_writes.append(canonical_write)
        if phase == "hot":
            require(prior is not None and after["Value"] == prior["Value"] + 1, "genuine RMW increment; no lost update")
        body = go_json(after)
        if prior_raw is None:
            spool.insert(b"state", logical, body)
        else:
            spool.replace(b"state", logical, prior_raw, body)
        history = prefix(c, g) + f'history/{o["Round"]:016x}/'.encode() + effect["Key"].encode().hex().encode()
        spool.insert(b"history", u64(g) + history, body)
        if o["Sequence"] <= old["Groups"][o["Coordinator"]]:
            old_raw = spool.find(b"old-state", logical)
            if old_raw is None:
                spool.insert(b"old-state", logical, body)
            else:
                spool.replace(b"old-state", logical, old_raw, body)
    spool.insert(b"cdc-seen", identity, b"1")
    require(go_json(group) == go_json({"Tx": tx, "Outcome": {name: o[name] for name in ("Commit", "Round", "Digest", "ID", "Request", "Reason", "Coordinator", "Sequence")}, "Writes": canonical_writes}), "canonical full native CDC envelope")
    return phase




def validate_result(end):
    version = integer(end["version"], 1, 2)
    fields(end, "version format spec Config Binding BindingRecord Initial Old Final Attempts Aborted Committed HotAborted ClaimAborted Setup Seed Hot Claims Submissions SDKCallbacks ReadCallbacks ClientHot OldMetadata FinalMetadata Sources Columns Points ProbeWalks ProbeAccess AttemptPayloadSHA256" + (" Transport" if version == 2 else ""))
    require(end["format"] == FORMAT.replace("-v1", f"-v{version}"), "fresh family result only")
    p, c = end["spec"], end["Config"]
    fields(p, "version family facts crossPercent writers layout")
    n = integer(p["facts"], 20, 1_000_000)
    integer(p["version"], version, version)
    integer(p["crossPercent"], 0, 100)
    integer(p["writers"], 1, 32)
    require(n % 20 == 0 and p["version"] == version and p["family"] == FAMILY and p["crossPercent"] in (0, 10, 50, 100) and p["writers"] in (1, 8, 32) and p["layout"] in ("kv", "columns"), "supported fresh workload")
    graph_prefix = f"graph-workload-contention-v{version}-" + p["layout"] + "-"
    run = c["Graph"].removeprefix(graph_prefix)
    require(c["Graph"].startswith(graph_prefix) and len(run) == 32 and bytes.fromhex(run) != bytes(16) and bytes.fromhex(run).hex() == run, "fresh persisted per-run graph identity")
    originals = 3 + n // 2 + n * 64 + n // 2 * max(2, p["writers"])
    require(same(c, {"Format": 2, "Graph": c["Graph"], "Topology": 1, "Epochs": [7, 11],
                  "Limits": {"Bytes": (64 << 20) + n * (128 << 10) + originals * 2048,
                             "Transactions": originals, "Versions": 3 + 6 * n, "Certificates": 1024},
                  "Nodes": [1, 7 + 2 * n], "Cost": {"version": 2, "facts": n, "crossPercent": p["crossPercent"], "writers": p["writers"]}, "CostScope": SCOPE}), "exact immutable fresh config/quota; no widened v1")
    expected_binding = {"Version": 1, "Family": FAMILY, "Config": c, "Competitors": max(2, p["writers"]), "Window": 32,
                        "MaxAttempts": 64, "MaxSubmissions": 8, "MaxCallbacks": 16,
                        "Policy": "retained-all; hot native current-value+revision increment; each empty-ID wave isolated after hot drain; exact paired ABORT only authorizes replan"}
    if version == 2:
        expected_binding.update(Version=2, Admission=transport.family_record(end["Transport"], p, c))
    require(end["Binding"] == expected_binding, "immutable attempt policy")
    raw_record(end["BindingRecord"], key(c, 0, f"workload-contention-v{version}", "binding"), go_json(expected_binding))
    for name, groups in (("Initial", [0, 0]), ("Old", [2 + n // 2, 1]), ("Final", [2 + 2 * n, 1])):
        cut = end[name]
        fields(cut, "Graph Topology Epochs Scope Round Groups")
        require(cut["Graph"] == c["Graph"] and cut["Topology"] == c["Topology"] and cut["Epochs"] == c["Epochs"] and blob(cut["Scope"]) == b"\0\1" and cut["Groups"] == groups, "actual full-scope cut membership")
        integer(cut["Round"], 1)
        integer(cut["Topology"], 1)
        for coordinate in cut["Groups"]:
            integer(coordinate)
    for name in ("OldMetadata", "FinalMetadata"):
        require(len(end[name]) == 2, "both authentic partition metadata records")
        for meta in end[name]:
            fields(meta, "Floor Generation Versions Transactions Certificates Bytes Groups")
            for coordinate in meta.values():
                integer(coordinate)
    require(end["Initial"]["Round"] < end["Old"]["Round"] < end["Final"]["Round"], "old cut precedes hot mutations/claims")
    require(end["Setup"] == 3 and end["Seed"] == n // 2 and end["Hot"] == n and end["Claims"] == n // 2 and
            end["Committed"] == 3 + 2 * n and end["ClaimAborted"] == n // 2 * (max(2, p["writers"]) - 1) and
            end["Aborted"] == end["HotAborted"] + end["ClaimAborted"] and end["Attempts"] == end["Committed"] + end["Aborted"] <= originals,
            "useful/setup/rejected claim counts remain distinct")
    require(len(end["ClientHot"]) == 32 and sum(end["ClientHot"]) == n and all(v == 0 for v in end["ClientHot"][p["writers"]:]), "bounded actual client completions")
    require(all(end["ClientHot"][i] == len(range(i, n, p["writers"])) for i in range(p["writers"])), "actual per-client hot assignment")
    require(end["ProbeWalks"] == 2 and end["ProbeAccess"] == "four fixed probes share one retained-history walk per cut; final probes fold into required history export; not indexed point latency", "actual shared-scan sample work; no point-index performance claim")
    return p, c


def attempts(directory, end, spool):
    p, c = end["spec"], end["Config"]
    counts = {"attempts": 0, "committed": 0, "aborted": 0, "setup": 0, "seed": 0, "hot": 0, "claim": 0,
              "submissions": 0, "callbacks": 0, "reads": 0}
    payload, footer, header, barriers, inventory = hashlib.sha256(), False, False, [], [0, 0]
    stage, inventory_previous = "seed", None
    for row, raw in lines(directory / "fdb-workload-attempts.jsonl", 2 * end["Attempts"] + 32):
        require(not footer, "trailing attempt evidence")
        kind = row.get("Type")
        if kind == "footer":
            require(row["End"] == end and payload.hexdigest() == end["AttemptPayloadSHA256"], "sealed complete attempt payload/result")
            footer = True
            continue
        payload.update(raw)
        if not header:
            require(kind == "header" and row == {"Type": "header", "Format": end["format"], "Spec": p, "Binding": end["Binding"], "Record": end["BindingRecord"], "Initial": end["Initial"]}, "fresh native attempt header")
            header = True
        elif kind == "attempt":
            require(stage != "final", "attempt after final barrier")
            phase, outcome = authenticate_attempt(row["Attempt"], p, c, spool)
            require((phase in ("setup", "seed")) == (stage == "seed"), "old cut barrier before all useful updates/claims")
            if phase == "claim":
                stage = "claim"
                fact = row["Attempt"]["Operation"]["Fact"]
                count_key = u64(fact)
                previous = spool.find(b"claim-totals", count_key)
                totals = [0, 0] if previous is None else decoded(previous)
                totals[0] += 1
                totals[1] += int(outcome["Commit"])
                if previous is None:
                    spool.insert(b"claim-totals", count_key, go_json(totals))
                else:
                    spool.replace(b"claim-totals", count_key, previous, go_json(totals))
            else:
                require(stage != "claim", "hot workers must drain before first claim wave")
            counts["attempts"] += 1
            counts["committed" if outcome["Commit"] else "aborted"] += 1
            if outcome["Commit"]:
                counts[phase] += 1
            counts["submissions"] += len(row["Attempt"]["Submissions"])
            counts["callbacks"] += sum(s["Callbacks"] for s in row["Attempt"]["Submissions"])
            counts["reads"] += row["Attempt"]["ReadCallbacks"]
        elif kind == "barrier":
            name = row["Name"]
            require(name == ("old" if not barriers else "final") and len(barriers) < 2 and row["Cut"] == end[name.title()] and row["Metadata"] == end[name.title() + "Metadata"] and
                    [row["Attempts"], row["Committed"], row["Aborted"]] == [counts["attempts"], counts["committed"], counts["aborted"]], "actual barrier attempt/metadata membership")
            if name == "old":
                require(counts["attempts"] == 3 + p["facts"] // 2 and counts["aborted"] == 0, "all genuine seed originals precede old cut")
            else:
                require(counts["attempts"] == end["Attempts"], "all contenders finish before final cut")
            barriers.append(name)
            stage = "hot" if name == "old" else "final"
        elif kind == "original":
            require(stage == "final", "inventory only after final barrier")
            proof, g = row["Proof"], integer(row["Group"], 0, 1)
            o = proof["Outcome"]
            identity = u64(g) + o["ID"].encode()
            inventory_order = (g, blob(proof["Pair"][0]["Key"]))
            require(inventory_previous is None or inventory_previous < inventory_order, "genuine original-prefix enumeration")
            inventory_previous = inventory_order
            require(go_json(proof) == spool.lookup(b"attempt-id", identity), "complete actual retained original joins attempt")
            spool.insert(b"inventory-seen", identity, b"1")
            inventory[g] += 1
        elif kind == "originals-eof":
            require(row["Rows"] == inventory == [m["Transactions"] for m in end["FinalMetadata"]] and sum(inventory) == end["Attempts"] and row["Metadata"] == end["FinalMetadata"], "original inventory exact EOF")
            spool.insert(b"eof", b"originals", b"1")
        elif kind == "point-eof":
            require(stage == "final" and row["Point"] in end["Points"] and sum(row["Rows"]) <= c["Limits"]["Versions"], "actual held source point EOF")
            coordinate = row["Point"]["Name"].encode() + u64(row["Point"]["Fact"])
            spool.insert(b"point-eof", coordinate, go_json(row))
        elif kind == "probe-walk-eof":
            require(row["Name"] in ("old-held", "final") and row["Probes"] == 4 and row["Rows"] == [m["Versions"] for m in end["OldMetadata" if row["Name"] == "old-held" else "FinalMetadata"]], "one genuine shared probe/history scan reaches complete EOF")
            spool.insert(b"probe-walk-eof", row["Name"].encode(), go_json(row))
        else:
            require(False, "unknown attempt artifact frame")
    require(header and footer and barriers == ["old", "final"] and spool.lookup(b"eof", b"originals") == b"1", "missing complete attempt/barrier/inventory EOF")
    for name, actual in counts.items():
        expected = {"attempts": "Attempts", "committed": "Committed", "aborted": "Aborted", "setup": "Setup", "seed": "Seed", "hot": "Hot", "claim": "Claims", "submissions": "Submissions", "callbacks": "SDKCallbacks", "reads": "ReadCallbacks"}[name]
        require(actual == end[expected], "result actual attempt census: " + name)
    for fact in range(p["facts"] // 2):
        require(decoded(spool.lookup(b"claim-totals", u64(fact))) == [max(2, p["writers"]), 1], "one winner per genuinely empty ID; all loser originals retained")
    operations = 0
    for group, _ in lines(directory / "fdb-workload-operations.jsonl", end["Committed"]):
        identity = u64(group["Outcome"]["Coordinator"]) + group["Tx"]["ID"].encode()
        require(decoded(spool.lookup(b"attempt-id", identity))["Group"] == group, "worker group exact actual COMMIT")
        spool.insert(b"operation-file", identity, go_json(group))
        operations += 1
    require(operations == end["Committed"], "complete worker COMMIT file")
    samples = 0
    for sample, _ in lines(directory / "fdb-workload-latency.jsonl", end["Attempts"]):
        fields(sample, "Operation ClientID ElapsedNanos")
        op = sample["Operation"]
        ordinal = integer(op["Ordinal"], 1)
        attempt = decoded(spool.lookup(b"attempt-chain", u64(ordinal)))
        require(op == attempt["Operation"] and sample["ClientID"] == attempt["ClientID"], "actual operation latency identity")
        integer(sample["ElapsedNanos"], 1, (1 << 63) - 1)
        spool.insert(b"latency-seen", u64(ordinal), b"1")
        samples += 1
    require(samples == 3 + 3 * p["facts"] // 2 + p["facts"] // 2 * max(2, p["writers"]), "one latency per setup/seed/hot/claim operation, not per retry")


def raw_stream(directory, name, end):
    p, c = end["spec"], end["Config"]
    digest, counts, previous, footer = hashlib.sha256(), [0, 0], None, False
    header = None
    for frame, raw in lines(directory / ("fdb-workload-" + name + ".jsonl"), 6 * p["facts"] + 6):
        require(not footer, "trailing raw stream row")
        if header is None:
            header = frame
            fields(header, "Type Format Stream Config InitialCut OldCut FinalCut Policy Metadata ConfigRecords MetadataRecords CutRecords")
            require(header["Type"] == "header" and header["Format"] == end["format"] and header["Stream"] == name and header["Config"] == c and header["InitialCut"] == end["Initial"] and header["OldCut"] == end["Old"] and header["FinalCut"] == end["Final"] and header["Metadata"] == end["FinalMetadata"] and header["Policy"] == {"PageRows": 32, "PageBytes": 4 << 20}, "exact source config/cut/metadata/policy binding")
            for g in range(2):
                raw_record(header["ConfigRecords"][g], key(c, g, "meta", "config"), go_json(c))
                raw_record(header["MetadataRecords"][g], key(c, g, "meta", "state"), go_json(end["FinalMetadata"][g]))
            for record, cut in zip(header["CutRecords"], (end["Initial"], end["Old"], end["Final"]), strict=True):
                raw_record(record, key(c, 0, "cut", go_json(cut).hex()), go_json(cut))
            digest.update(raw)
        elif frame.get("Type") == "footer":
            require(frame == {"Type": "footer", "Rows": counts, "Groups": end["Final"]["Groups"], "Metadata": end["FinalMetadata"], "PayloadSHA256": digest.hexdigest()}, "complete source EOF/hash/counts")
            footer = True
        else:
            fields(frame, "Type Row")
            require(frame["Type"] == "row", "raw source frame type")
            row = frame["Row"]
            fields(row, "Group Key Value" + (" IDKey IDValue RequestKey RequestValue" if name == "cdc" else ""))
            g = integer(row["Group"], 0, 1)
            body = {k: blob(v) for k, v in row.items() if k != "Group"}
            require(0 < len(body["Key"]) <= 1024 and 0 < len(body["Value"]) <= 64 << 10 and sum(map(len, body.values())) <= 4 << 20, "admitted native raw record")
            if name == "cdc":
                group = decoded(body["Value"])
                order = (group["Outcome"]["Round"], g, group["Outcome"]["Request"])
            else:
                order = (g, body["Key"])
            require(previous is None or order > previous, "genuine deterministic native enumeration")
            previous = order
            counts[g] += 1
            digest.update(raw)
            yield g, body
    index = ("history", "current", "cdc").index(name)
    require(header is not None and footer and end["Sources"][index] == {"Rows": counts, "Groups": end["Final"]["Groups"], "Metadata": end["FinalMetadata"]}, "missing/mixed/truncated raw EOF result")


def verify_sources(directory, end, spool):
    p, c = end["spec"], end["Config"]
    generations, sequences, writes, cdc = [0, 0], [0, 0], [0, 0], 0
    old_generations, old_writes = [0, 0], [0, 0]
    charged = {"Old": [0, 0], "Final": [0, 0]}
    transactions = {"Old": [0, 0], "Final": [0, 0]}
    maximum_meta = dict.fromkeys(("Floor", "Generation", "Versions", "Transactions", "Certificates", "Bytes", "Groups"), (1 << 64) - 1)
    for name, cut_names in (("Old", ("Initial", "Old")), ("Final", ("Initial", "Old", "Final"))):
        for g in range(2):
            charged[name][g] = len(key(c, g, "meta", "config")) + len(go_json(c)) + len(key(c, g, "meta", "state")) + len(go_json(maximum_meta))
        charged[name][0] += len(blob(end["BindingRecord"]["Key"])) + len(blob(end["BindingRecord"]["Value"]))
        for cut_name in cut_names:
            body = go_json(end[cut_name])
            charged[name][0] += len(key(c, 0, "cut", body.hex())) + len(body)
    for _, body in spool.walk(b"attempt-id"):
        proof = decoded(body)
        o = proof["Outcome"]
        g = o["Coordinator"]
        size = sum(len(blob(r["Key"])) + len(blob(r["Value"])) for r in proof["Pair"])
        charged["Final"][g] += size
        transactions["Final"][g] += 1
        if o["Commit"] and o["Sequence"] <= end["Old"]["Groups"][g]:
            charged["Old"][g] += size
            transactions["Old"][g] += 1
    for g, row in raw_stream(directory, "cdc", end):
        group = decoded(row["Value"])
        o = group["Outcome"]
        require(g == o["Coordinator"] and row["Key"] == prefix(c, g) + f'changes/{o["Sequence"]:016x}/'.encode() + o["Request"].encode().hex().encode(), "CDC actual coordinator/sequence/request key")
        for kind, raw_name, identity in (("id", "ID", o["ID"]), ("request", "Request", o["Request"])):
            require(row[raw_name + "Key"] == key(c, g, kind, identity) and row[raw_name + "Value"] == go_json(o), "complete CDC paired original values")
        require(o["Round"] <= end["Final"]["Round"] and o["Sequence"] <= end["Final"]["Groups"][g], "CDC full-cut membership")
        fold_group(group, p, c, spool, generations, sequences, end["Old"])
        old_member = o["Sequence"] <= end["Old"]["Groups"][g]
        charged["Final"][g] += len(row["Key"]) + len(row["Value"])
        if old_member:
            charged["Old"][g] += len(row["Key"]) + len(row["Value"])
            for participant in group["Tx"]["Participants"]:
                old_generations[participant["Group"]] += 1
        for write in group["Writes"]:
            owner = write["Group"]
            writes[owner] += 1
            if old_member:
                old_writes[owner] += 1
                body = go_json(write["After"])
                history_key = prefix(c, owner) + f'history/{o["Round"]:016x}/'.encode() + write["Key"].encode().hex().encode()
                charged["Old"][owner] += len(history_key) + len(body)
        cdc += 1
    require(cdc == end["Committed"] and sequences == end["Final"]["Groups"] and generations == [m["Generation"] for m in end["FinalMetadata"]] and writes == [m["Versions"] for m in end["FinalMetadata"]], "complete CDC/effect/generation/metadata census")
    for name in ("history", "current"):
        counts = [0, 0]
        for g, row in raw_stream(directory, name, end):
            if name == "history":
                expected = spool.lookup(b"history", u64(g) + row["Key"])
            else:
                native_prefix = prefix(c, g) + b"current/"
                require(row["Key"].startswith(native_prefix), "actual current prefix")
                logical = bytes.fromhex(row["Key"][len(native_prefix):].decode())
                require(row["Key"] == key(c, g, "current", logical.decode()), "canonical current key")
                expected = spool.lookup(b"state", u64(g) + logical)
            require(row["Value"] == expected, "physical source differs from exact original fold")
            charged["Final"][g] += len(row["Key"]) + len(row["Value"])
            spool.insert(b"seen-" + name.encode(), u64(g) + row["Key"], b"1")
            counts[g] += 1
        require(sum(counts) == (3 + 6 * p["facts"] if name == "history" else 3 + 3 * p["facts"]), "complete physical source exact-set census")
    for logical, body in spool.walk(b"old-state"):
        owner = int.from_bytes(logical[:8], "big")
        charged["Old"][owner] += len(key(c, owner, "current", logical[8:].decode())) + len(body)
    for name, expected_generations, expected_versions, certificates in (("Old", old_generations, old_writes, [2, 0]), ("Final", generations, writes, [3, 0])):
        for g, meta in enumerate(end[name + "Metadata"]):
            require(meta == {"Floor": end[name]["Round"], "Generation": expected_generations[g], "Versions": expected_versions[g], "Transactions": transactions[name][g],
                             "Certificates": certificates[g], "Bytes": charged[name][g], "Groups": end[name]["Groups"][g]}, "exact native byte/version/attempt/certificate accounting at " + name)
    for fact in range(p["facts"]):
        _, effects, _ = contract(p, "seed" if fact < p["facts"] // 2 else "claim", fact if fact < p["facts"] // 2 else fact - p["facts"] // 2, 5)
        expected_value = 5
        for probe in (0, 9):
            if fact == target(p, "hot", probe):
                expected_value += p["facts"] * (p["crossPercent"] if cross(p, probe) else 100 - p["crossPercent"]) // 100
                break
        for g, rows in effects.items():
            for effect in rows:
                current = decoded(spool.lookup(b"state", u64(g) + effect["Key"].encode()))
                require(current["Value"] == expected_value and not current["Deleted"], "complete final identity/outgoing/incoming index state")
                old = spool.find(b"old-state", u64(g) + effect["Key"].encode())
                require((old is not None) == (fact < p["facts"] // 2) and (old is None or decoded(old)["Value"] == 5), "old-cut exact absence/state survives later updates")
    require(len(end["Points"]) == 8, "held-old/final source point inventory")
    require({(point["Name"], point["Fact"]) for point in end["Points"]} == {(name, fact) for name in ("old-held", "final") for fact in (0, 9, p["facts"] // 2, p["facts"] + 16)}, "exact fixed shared-scan probes")
    for point in end["Points"]:
        name = point["Name"]
        require(name in ("old-held", "final") and point["Cut"] == end["Old" if name == "old-held" else "Final"], "point exact retained cut")
        identity = u64(0) + blob(point["Key"])
        integer(point["Fact"])
        require(type(point["Found"]) is bool, "exact-empty/present is an explicit boolean")
        fact = point["Fact"]
        require(blob(point["Key"]) == fixture_edge_key(p, fact), "sample actual edge identity and endpoint-life key")
        expected = spool.find(b"old-state" if name == "old-held" else b"state", identity)
        require(point["Found"] == (expected is not None) and (expected is None or go_json(point["Value"]) == expected), "actual old/current/phantom point equals complete source fold")
        require(spool.lookup(b"point-eof", name.encode() + u64(point["Fact"])) is not None, "point requires actual complete held-reader EOF")
        spool.lookup(b"probe-walk-eof", name.encode())
        if expected is not None:
            cell = decoded(expected)
            proof = point["Original"]
            require(go_json(proof) == spool.lookup(b"attempt-id", u64(proof["Outcome"]["Coordinator"]) + cell["TxID"].encode()), "point authentic original lineage")


def column_key(c, generation, suffix):
    return prefix(c) + b"columns-v1/generation/" + generation.encode().hex().encode() + b"/" + suffix.encode()


def zero_manifest():
    cut = {"Graph": "", "Topology": 0, "Epochs": [0, 0], "Scope": None, "Round": 0, "Groups": [0, 0]}
    meta = dict.fromkeys(("Floor", "Generation", "Versions", "Transactions", "Certificates", "Bytes", "Groups"), 0)
    return {"Generation": {"Version": 0, "ID": "", "Binding": [0] * 32, "Initial": cut, "Cut": cut},
            "Metadata": [meta, meta], "Chunks": 0, "Rows": 0, "Bytes": 0, "History": [0, 0], "Chain": [0] * 32}


def column_refusal(row, end, manifests, held, token, spool, index, collected, deleted, stage_count):
    fields(row, "Type Name Generation Error Input Result Records")
    labels = ("expired-new-open", "held-old-collection", "retired-publication", "final-reference-collection")
    require(held is not None and token is not None and index < 4 and row["Name"] == labels[index] and
            row["Generation"] == ("final" if index == 3 else "old") and stage_count == (2 if index < 2 else 5), "refusal exact actual operation/lifetime position")
    c, generation = end["Config"], row["Generation"]
    colprefix = prefix(c) + b"columns-v1/"
    pin_key = colprefix + b"retention-v1/pin/" + b"hold-old".hex().encode()
    head_key = colprefix + b"retention-v1/reference/" + b"head".hex().encode()
    records = _codec.column_records(row["Records"])
    required = {column_key(c, generation, kind) for kind in ("manifest", "progress", "owners")}
    required.update((held, pin_key, head_key, colprefix + b"ledger", colprefix + b"retention-v1/ledger", key(c, 0, "meta", "state"), key(c, 1, "meta", "state")))
    require(set(records) == required and len(required) == 10, "every refusal binds complete actual native authority; labels alone never suffice")
    m = manifests[generation]
    for kind in ("manifest", "progress"):
        native = column_key(c, generation, kind)
        require(records[native] == spool.lookup(b"column-control", native), "refused generation retains exact original manifest/source EOF")
    guard = {"Manifest": list(hashlib.sha256(go_json(m)).digest()), "State": 3 if index == 2 else 1,
             "References": 1 if index == 3 else 0, "Pins": 0, "Readers": 1 if index < 2 else 0,
             "Cursor": m["Chunks"] if index == 2 else 0, "DeletedBytes": m["Bytes"] if index == 2 else 0,
             "Chain": m["Chain"] if index == 2 else [0] * 32}
    require(records[column_key(c, generation, "owners")] == go_json(guard), "actual expired/held/retired/referenced generation refuses the attempted door")
    pin = {"Token": token, "Intent": list(hashlib.sha256(go_json({"Columns": token["Columns"], "Generation": "old", "Cut": end["Old"]})).digest()),
           "State": 3, "TerminalRequest": "expire-old", "TerminalKind": 3}
    require(records[pin_key] == go_json(pin), "refusal exact authenticated expiry tombstone")
    reader = decoded(records[held])
    _codec.column_reader(reader, manifests, token, True)
    initial_reader = decoded(spool.lookup(b"column-held", held))
    require(same(reader | {"State": 1}, initial_reader) and reader["State"] == (1 if index < 2 else 2), "actual held reader identity survives expiry until authenticated Close")
    for g in range(2):
        require(records[key(c, g, "meta", "state")] == go_json(end["FinalMetadata"][g]), "refusal changes no source graph/CDC metadata")
    require(records[head_key] == spool.lookup(b"column-control", head_key) and records[colprefix + b"retention-v1/ledger"] == spool.lookup(b"column-control", colprefix + b"retention-v1/ledger"), "same actual reference/pin accounting during refusal")
    ledger = decoded(spool.lookup(b"column-control", colprefix + b"ledger"))
    ledger["Chunks"] -= collected
    ledger["Bytes"] -= deleted
    require(records[colprefix + b"ledger"] == go_json(ledger), "refusal preserves exact already-observed collector accounting")
    expired = "fdb-column: retained generation or token expired"
    in_use = "fdb-column: retained generation has live owners or references"
    require(row["Error"] == (expired if index in (0, 2) else in_use), "actual production sentinel classification, no SDK uncertainty substitution")
    if index == 0:
        require(same(row["Input"], token) and row["Result"] is None, "expired open returns no new owner")
    elif index == 2:
        require(same(row["Input"], {"Generation": "old", "Initial": end["Initial"], "Final": end["Old"]}) and same(row["Result"], zero_manifest()) and collected == m["Chunks"] and deleted == m["Bytes"], "same original publication explicitly refuses after complete derived GC")
    else:
        require(same(row["Input"], {"Generation": generation, "Budget": {"Chunks": 1, "Bytes": 128 << 10, "OutputBytes": 1024}}) and
                same(row["Result"], {"Generation": "", "Cursor": 0, "DeletedBytes": 0, "WorkChunks": 0, "WorkBytes": 0, "Done": False}), "actual refused collector returns zero page/no deletion authority")


def columns(directory, end, spool):
    path = directory / "fdb-workload-columns.jsonl"
    if end["spec"]["layout"] == "kv":
        require(not path.exists() and same(end["Columns"], [zero_manifest(), zero_manifest()]), "KV evidence cannot masquerade as columns")
        return
    c, p = end["Config"], end["spec"]
    manifests = dict(zip(("old", "final"), end["Columns"], strict=True))
    chunks, chunk_rows, retained, chains = {}, {}, {}, {}
    walks, stages, refusals = {}, [], []
    chunk_order, chunk_sequences, chunk_history, walk_order = {}, {}, {}, {}
    collected, deleted, storage_count, storage_bytes = 0, 0, 0, 0
    live_chunks, live_bytes, footer, header, point_count = 0, 0, False, False, 0
    digest, binding, policy = hashlib.sha256(), None, None
    held, token, pin_binding = None, None, None
    for row, raw in lines(path, 40 + 12 * end["Committed"]):
        require(not footer, "trailing column evidence")
        kind = row.get("Type")
        if kind == "footer":
            final = row["End"]
            require(final["Format"] == final["SourceFormat"] == end["format"] and final["ReferenceLayout"] == "native-logical-kv-reference" and
                    final["Old"] == manifests["old"] and final["Final"] == manifests["final"] and bytes(final["PayloadSHA256"]) == digest.digest(), "sealed actual derived column artifact")
            require(final["Counts"]["Stages"] == 6 and final["Counts"]["Chunks"] == sum(chunks.values()) and final["Counts"]["WalkRows"] == sum(walks.values()) and final["Counts"]["Points"] == point_count == 6 and final["Counts"]["Collections"] == collected and
                    final["Ledger"]["Chunks"] == live_chunks == manifests["final"]["Chunks"] and final["Storage"]["Records"] == storage_count and final["Storage"]["Bytes"] == storage_bytes and
                    final["Storage"]["LiveChunkBytes"] == live_bytes == manifests["final"]["Bytes"], "actual column live/tombstone/storage census")
            require(go_json(final["Ledger"]) == spool.lookup(b"column-control", prefix(c) + b"columns-v1/ledger") and
                    go_json(final["PinLedger"]) == spool.lookup(b"column-control", prefix(c) + b"columns-v1/retention-v1/ledger") and storage_count == live_chunks + 22, "complete strict terminal controls and quota ledgers")
            footer = True
            continue
        digest.update(raw)
        if not header:
            require(row["Type"] == "header" and row["Format"] == end["format"] and row["Config"] == c and len(row["Records"]) == 2, "fresh actual column binding header")
            binding = decoded(blob(row["Records"][0]["Value"]))
            require(binding["Version"] == 1 and binding["Config"] == c and binding["Scope"] == SCOPE and binding["Layout"] == _codec.COLUMN_LAYOUT and
                    binding["Schema"] == "node-presence;edge-life-bound;property-p:i64;canonical-original:v1", "typed column graph/schema/axis binding")
            raw_record(row["Records"][0], prefix(c) + b"columns-v1/binding", go_json(binding))
            policy = binding["Policy"]
            require(policy == {"ChunkRows": 32, "ChunkBytes": 64 << 10, "MaxChunks": 1_000_000, "MaxGenerations": 2,
                               "MaxRetainedBytes": (64 << 20) + (6 + 3 * p["facts"]) * (64 << 10), "MaxWorkRows": 3 + 2 * p["facts"],
                               "MaxWorkBytes": (64 << 20) + (3 + 2 * p["facts"]) * (512 << 10)}, "explicit unchanged column admission policy")
            pin_binding = decoded(blob(row["Records"][1]["Value"]))
            require(same(pin_binding, {"Version": 1, "Columns": list(hashlib.sha256(go_json(binding)).digest()), "Policy": {"MaxRecords": 32, "MaxBytes": 256 << 10, "PageChunks": 1, "PageBytes": 128 << 10,
                            "MaxWorkChunks": 1, "MaxWorkBytes": 128 << 10, "MaxOutputBytes": 1024}}), "actual pin namespace and finite quota binding")
            raw_record(row["Records"][1], prefix(c) + b"columns-v1/retention-v1/binding", go_json(pin_binding))
            for record in row["Records"]:
                spool.insert(b"column-control", blob(record["Key"]), blob(record["Value"]))
            for name, m in manifests.items():
                require(m["Generation"] == {"Version": 1, "ID": name, "Binding": list(hashlib.sha256(go_json(binding)).digest()), "Initial": end["Initial"], "Cut": end["Old" if name == "old" else "Final"]} and
                        m["Metadata"] == end["OldMetadata" if name == "old" else "FinalMetadata"] and m["History"] == [v["Versions"] for v in m["Metadata"]] and
                        m["Rows"] == (3 + p["facts"] // 2 if name == "old" else end["Committed"]), "complete source census at authentic generation cut")
            header = True
        elif kind == "stage":
            name = row["Name"]
            require(len(stages) < 6 and name == ("old-held", "final-head", "expired-held", "reads-complete", "old-closed", "terminal")[len(stages)] and 1 <= len(row["Records"]) <= 20, "actual bounded column lifetime stages")
            records = _codec.column_records(row["Records"])
            proxy = {"Config": c, "OldCut": end["Old"]}
            index = (0, 1, 2, 3, 4, 6)[len(stages)]
            if index == 6:
                proxy["Columns"] = {"Ledger": decoded(records.get(prefix(c) + b"columns-v1/ledger", b"")),
                                    "PinLedger": decoded(records.get(prefix(c) + b"columns-v1/retention-v1/ledger", b""))}
            held, token = _codec.column_stage(row, index, proxy, manifests, binding, pin_binding, held, token, spool)
            stages.append(name)
        elif kind == "chunk":
            name = row["Generation"]
            require(name in manifests, "foreign derived generation")
            m = manifests[name]
            ordinal = chunks.get(name, 0)
            body = blob(row["Record"]["Value"])
            native_key = column_key(c, name, f"chunk/{ordinal:016x}")
            raw_record(row["Record"], native_key, body)
            spool.insert(b"column-native-chunk", native_key, body)
            require(row["Ordinal"] == ordinal and row["Length"] == len(body) and row["SHA256"] == list(hashlib.sha256(body).digest()), "actual chunk ordinal/length/hash")
            cache_name = "column-cache-" + name
            filename = f'column-{hashlib.sha256(go_json(m["Generation"])).hexdigest()}-{ordinal:016x}.bin'
            require(row["CacheDirectory"] == cache_name and row["CacheFile"] == filename, "owned immutable cache path")
            cache_path = directory / cache_name / filename
            require(not cache_path.is_symlink() and cache_path.is_file(), "owned regular immutable cache file")
            with cache_path.open("rb") as cache:
                cached = cache.read(policy["ChunkBytes"] + 1)
            require(cached == body, "actual binary cache not synthesized typed output")
            count = 0
            for typed, original in _codec.column_chunk(body, m["Generation"], hashlib.sha256(go_json(binding)).digest(), ordinal, chains.get(name, bytes(32)), policy, spool):
                o = original["Outcome"]
                order = (o["Round"], o["Coordinator"], o["Request"])
                sequence = chunk_sequences.setdefault(name, [0, 0])
                census = chunk_history.setdefault(name, [0, 0])
                require((name not in chunk_order or chunk_order[name] < order) and o["Sequence"] == sequence[o["Coordinator"]] + 1, "column preserves actual complete CDC enumeration")
                chunk_order[name] = order
                sequence[o["Coordinator"]] += 1
                for write in original["Writes"]:
                    census[write["Group"]] += 1
                coordinate = u64(typed["Provenance"])
                spool.insert(b"column-chunk-" + name.encode(), coordinate, go_json(typed))
                count += 1
            require(count == row["Rows"], "chunk actual decoded row census")
            chains[name] = hashlib.sha256(chains.get(name, bytes(32)) + hashlib.sha256(body).digest()).digest()
            spool.insert(b"column-chain-" + name.encode(), u64(ordinal), chains[name])
            chunks[name] = ordinal + 1
            chunk_rows[name] = chunk_rows.get(name, 0) + count
            retained[name] = retained.get(name, 0) + len(native_key) + len(body)
            spool.insert(b"column-length-" + name.encode(), u64(ordinal), u64(len(native_key) + len(body)))
        elif kind == "chunks-eof":
            name = row["Generation"]
            m = manifests[name]
            require(row["Manifest"] == m and chunks.get(name) == m["Chunks"] and chunk_rows.get(name) == m["Rows"] and retained.get(name) == m["Bytes"] and chains.get(name) == bytes(m["Chain"]) and
                    chunk_sequences[name] == m["Generation"]["Cut"]["Groups"] and chunk_history[name] == m["History"], "complete actual binary chunk EOF")
            directory_path = directory / ("column-cache-" + name)
            require(sum(1 for _ in directory_path.iterdir()) == m["Chunks"], "no foreign/trailing cache files")
            spool.insert(b"column-eof", name.encode(), b"1")
        elif kind == "walk-row":
            name, generation, typed = row["Name"], row["Generation"], row["Row"]
            require(name in (generation, generation + "-cache") and generation in manifests, "actual native/cache walk mode")
            require(spool.lookup(b"column-chunk-" + generation.encode(), u64(typed["Provenance"])) == go_json(typed), "typed walk equals actual original binary chunk")
            original = decoded(blob(typed["Original"]))
            order = (typed["Round"], typed["Coordinator"], original["Tx"]["Request"])
            require(name not in walk_order or walk_order[name] < order, "deterministic complete typed walk order")
            walk_order[name] = order
            spool.insert(b"column-walk-" + name.encode(), u64(typed["Provenance"]), b"1")
            walks[name] = walks.get(name, 0) + 1
        elif kind == "walk-eof":
            name, generation = row["Name"], row["Generation"]
            require(row["Manifest"] == manifests[generation] and row["Cut"] == manifests[generation]["Generation"]["Cut"] and row["Rows"] == walks.get(name) == manifests[generation]["Rows"], "complete typed/cache EOF at exact cut")
            spool.insert(b"column-walk-eof", name.encode(), b"1")
        elif kind == "point":
            name, owner = row["Generation"], integer(row["Owner"], 1)
            found, typed = row["Found"], row["Row"]
            require(name in manifests and type(found) is bool and owner in (7, 7 + p["facts"], 39 + 2 * p["facts"]), "fixed actual column sample identity")
            if found:
                _codec.column_typed_row(typed, spool)
                require(typed["Owner"] == owner and spool.lookup(b"column-chunk-" + name.encode(), u64(typed["Provenance"])) == go_json(typed), "actual retained point lineage")
                expected = spool.find(b"old-state" if name == "old" else b"state", u64(0) + fixture_edge_key(p, (owner - 7) // 2))
                require(expected is not None and decoded(expected)["Value"] == typed["Value"] and decoded(expected)["Round"] == typed["Round"] and decoded(expected)["TxID"] == decoded(blob(typed["Original"]))["Tx"]["ID"], "column point equals exact complete source revision")
            else:
                fact = (owner - 7) // 2
                require(fact >= p["facts"] or name == "old" and fact >= p["facts"] // 2, "no false exact-empty point")
            spool.insert(b"column-point", name.encode() + u64(owner), b"1")
            point_count += 1
        elif kind == "refusal":
            require(len(refusals) < 4, "bounded fixed refusal controls")
            column_refusal(row, end, manifests, held, token, spool, len(refusals), collected, deleted, len(stages))
            refusals.append(row["Name"])
        elif kind == "collect":
            require(row["Budget"] == {"Chunks": 1, "Bytes": 128 << 10, "OutputBytes": 1024}, "separate bounded derived collector policy")
            page = row["Result"]
            size = int.from_bytes(spool.lookup(b"column-length-old", u64(collected)), "big")
            deleted += size
            collected += 1
            m = manifests["old"]
            require(page == {"Generation": "old", "Cursor": collected, "DeletedBytes": deleted, "WorkChunks": 1, "WorkBytes": size, "Done": collected == m["Chunks"]} and
                    blob(row["Absent"]) == column_key(c, "old", f"chunk/{collected - 1:016x}"), "actual atomic derived deletion/cursor/accounting")
            guard = decoded(blob(row["Guard"]["Value"]))
            raw_record(row["Guard"], column_key(c, "old", "owners"), go_json(guard))
            require(guard["Manifest"] == list(hashlib.sha256(go_json(m)).digest()) and bytes(guard["Chain"]) == spool.lookup(b"column-chain-old", u64(collected - 1)) and
                    guard["References"] == guard["Pins"] == guard["Readers"] == 0 and guard["Cursor"] == collected and guard["DeletedBytes"] == deleted and guard["State"] == (3 if page["Done"] else 2), "same guard authentic owner-free partial/resume state")
        elif kind == "storage-row":
            native_key, body = blob(row["Record"]["Key"]), blob(row["Record"]["Value"])
            require(native_key.startswith(prefix(c) + b"columns-v1/") and not native_key.startswith(column_key(c, "old", "chunk/")), "retired chunks absent; source retained separately")
            if native_key.startswith(column_key(c, "final", "chunk/")):
                require(body == spool.lookup(b"column-native-chunk", native_key), "terminal physical chunk equals original hash-bound bytes")
            elif native_key.startswith(prefix(c) + b"columns-v1/read-owner/") and native_key != held:
                reader = decoded(body)
                _codec.column_reader(reader, manifests, None, False)
                require(reader["State"] == 2 and native_key == prefix(c) + b"columns-v1/read-owner/" + reader["Owner"]["Request"].encode().hex().encode(), "all other read owners authentically closed")
            else:
                require(body == spool.lookup(b"column-control", native_key), "complete terminal physical control state")
            spool.insert(b"column-storage", native_key, body)
            storage_count += 1
            storage_bytes += len(native_key) + len(body)
            if native_key.startswith(column_key(c, "final", "chunk/")):
                live_chunks += 1
                live_bytes += len(native_key) + len(body)
        else:
            require(False, "unknown derived column frame")
    require(header and footer and collected == manifests["old"]["Chunks"] and deleted == manifests["old"]["Bytes"] and len(stages) == 6 and
            refusals == ["expired-new-open", "held-old-collection", "retired-publication", "final-reference-collection"], "missing complete production column lifetime")
    for name in ("old", "final"):
        spool.lookup(b"column-eof", name.encode())
        for mode in (name, name + "-cache"):
            spool.lookup(b"column-walk-eof", mode.encode())


def verify(directory, spool_directory, policy=None, *, shared_spool=None):
    directory = Path(directory).resolve(strict=True)
    with (directory / "fdb-workload-result.json").open("rb") as source:
        raw = source.read(LINE + 1)
    require(len(raw) <= LINE and raw.endswith(b"\n"), "bounded exclusive result")
    end = decoded(raw[:-1])
    validate_result(end)
    require(policy is not None or end["spec"]["facts"] < 1_000_000 and end["spec"]["writers"] != 32, "large cell requires explicit finite comparison grant, not the unqualified small default")
    policy = POLICY if policy is None else policy
    if shared_spool is not None: policy.validate()
    with (Spool(spool_directory, policy) if shared_spool is None else nullcontext(shared_spool)) as spool:
        attempts(directory, end, spool)
        verify_sources(directory, end, spool)
        columns(directory, end, spool)
        transport_proof = transport.verify(directory, end["Transport"]["Policy"], spool) if end["version"] == 2 else None
        report = spool.report()
    result = {"format": end["format"], "family": FAMILY, "runGraph": end["Config"]["Graph"], "spec": end["spec"],
            "attempts": end["Attempts"], "committed": end["Committed"], "aborted": end["Aborted"],
            "spool": report, "scope": "one fresh FDB family; exact native originals/history/current/CDC/columns, no native parity/RTT/matrix/capacity acceptance"}
    if transport_proof is not None:
        transport_proof["nativeRetainedBindingBytes"] = len(blob(end["Transport"]["Record"]["Key"])) + len(blob(end["Transport"]["Record"]["Value"]))
        result["transport"] = transport_proof
    return result


if __name__ == "__main__":
    try:
        if len(sys.argv) not in (3, 5):
            raise ValueError("usage: fdb_contention_check.py ARTIFACTS OWNED_SPOOL_DIR [DISK_BYTES CACHE_BYTES]")
        policy = None if len(sys.argv) == 3 else Policy(int(sys.argv[3]), int(sys.argv[4]), 4096, LINE)
        print(json.dumps(verify(sys.argv[1], sys.argv[2], policy), indent=2))
    except (ValueError, KeyError, TypeError, OSError) as error:
        print("contention evidence refused: " + str(error), file=sys.stderr)
        sys.exit(1)
