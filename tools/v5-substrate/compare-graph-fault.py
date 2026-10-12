#!/usr/bin/env python3
"""Raw-backed, fixed local graph-fault comparison; native codecs stay exporter-qualified."""
import argparse
import copy
import hashlib
import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SPEC = json.loads((ROOT / "graph-fault-local-v1.json").read_text())
_spec = importlib.util.spec_from_file_location("fault_lifecycle_primitives", ROOT / "fdb/compare-lifecycle.py")
lc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lc)
Refusal, require, blob, integer = lc.Refusal, lc.require, lc.blob, lc.integer
OPS = SPEC["operations"]
CASES = SPEC["faults"]
PAIRED_TRACE_BYTES = 8 << 20


def load(path):
    # A bounded read refuses the extra byte without retaining an arbitrary file.
    with Path(path).open("rb") as source:
        raw = source.read(PAIRED_TRACE_BYTES + 1)
    require(0 < len(raw) <= PAIRED_TRACE_BYTES, "paired trace byte bound (8 MiB)")
    return lc.parse(raw), hashlib.sha256(raw).hexdigest()


def artifact_files(trace, root):
    require(root is not None, "FDB artifact root required")
    root = Path(root).resolve()
    files, total = {}, 0
    declared = trace["controllerArtifacts"]
    require(type(declared) is dict and 0 < len(declared) <= 128, "controller artifact inventory")
    backend = trace["backendTraceArtifact"]
    hashes = dict(declared)
    require(backend["Artifact"] not in hashes, "backend/controller artifact collision")
    hashes[backend["Artifact"]] = backend["SHA256"]
    for name, digest in hashes.items():
        require(type(name) is str and Path(name).name == name and name not in ("", ".", ".."), "artifact basename")
        path = (root / name).resolve()
        require(path.parent == root, "artifact escaped root")
        with path.open("rb") as source:
            data = source.read(PAIRED_TRACE_BYTES + 1)
        require(0 < len(data) <= PAIRED_TRACE_BYTES, "artifact byte bound")
        total += len(data)
        require(total <= 64 << 20, "aggregate artifact byte bound")
        require(hashlib.sha256(data).hexdigest() == digest, "artifact SHA256 mismatch: " + name)
        files[name] = data
    ledger = trace["controllerCommandLedger"]
    require(ledger["Artifact"] in declared and ledger["IsPrefixOf"] == "commands.jsonl", "sealed controller ledger declaration")
    sealed = files[ledger["Artifact"]]
    integer(ledger["Bytes"], 1, PAIRED_TRACE_BYTES)
    require(len(sealed) == ledger["Bytes"] and sealed.endswith(b"\n"), "sealed ledger byte count/framing")
    with (root / "commands.jsonl").open("rb") as source:
        prefix = source.read(len(sealed))
    require(prefix == sealed, "sealed ledger is not exact final command prefix")
    commands = [lc.parse(line) for line in sealed.splitlines()]
    require(commands, "empty command ledger")
    return files, commands


def fdb_artifacts(trace, root):
    """Bind finalizer fields independently to the frozen backend and actual files."""
    files, ledger = artifact_files(trace, root)
    backend = lc.parse(files[trace["backendTraceArtifact"]["Artifact"]])
    projected = copy.deepcopy(trace)
    for key in ("controllerArtifacts", "backendTraceArtifact", "controllerCommandLedger"):
        del projected[key]
    for ordinal, fault in enumerate(projected["faults"]):
        for key in ("MarkerRaw", "CallerRaw", "ProcessEvidence"):
            require(backend["faults"][ordinal][key] is None, "backend already contained controller evidence")
            fault[key] = None
        for retry in fault.get("ReopenRetries", []):
            retry.pop("EventID", None)
    for old in projected["oldReads"]:
        old.pop("Checkpoint", None); old.pop("EventID", None)
    require(projected == backend, "final trace changed frozen backend content")

    def value(name):
        require(name in files, "unbound controller artifact: " + name)
        return lc.parse(files[name])

    def command(event, raw, field="Command"):
        index = integer(event["EventID"], 1, len(ledger)) - 1
        require(raw[field] == ledger[index], "process event not actual command ledger entry")
        require(ledger[index]["exit"] == 0 and ledger[index]["deadline_expired"] is False, "controller command did not succeed")
        return ledger[index]

    for ordinal, phase in enumerate(("before", "after")):
        fault = trace["faults"][ordinal]
        kill = next(e for e in fault["ProcessEvidence"] if e["Action"] == "kill")
        raw = lc.parse(blob(kill["Raw"]))
        require(raw["BeforeInspect"] == value(phase + "-child-live-inspect.json") and raw["AfterInspect"] == value(phase + "-child-killed.json"), "client process inspections changed")
        command(kill, raw)
        marker_bytes = files[phase + "-child-marker.json"]
        marker = lc.parse(marker_bytes)
        require(lc.parse(blob(fault["CallerRaw"])) == {"Marker": marker, "Kill": kill}, "caller classification provenance changed")
        if phase == "before":
            require(blob(fault["MarkerRaw"]) == marker_bytes and marker == {"ready": "before-native-commit"}, "actual precommit marker")
        else:
            receipt = files["after-child-pre-kill-committed.json"]
            envelope = lc.parse(blob(fault["MarkerRaw"]))
            require(blob(envelope["MarkerRaw"]) == marker_bytes and blob(envelope["PreKillReceiptRaw"]) == receipt, "pre-kill receipt artifact changed")
            require(marker == {"ready": "after-native-commit-before-reply", "receipt_sha256": hashlib.sha256(receipt).hexdigest()}, "actual pre-kill marker receipt hash")
            commit = next(e for e in fault["ProcessEvidence"] if e["Action"] == "commit")
            raw_commit = lc.parse(blob(commit["Raw"]))
            cmd = command(commit, raw_commit, "MarkerCommand")
            require(cmd["command"] == ["docker", "logs", raw["BeforeInspect"][0]["Name"].lstrip("/")], "pre-kill marker observed by another command")
            require(raw_commit["Marker"] == marker and raw_commit["PreKillReceipt"] == lc.parse(receipt) and raw_commit["OriginalRecords"] == lc.parse(receipt)["Records"], "pre-kill commit proof changed")

    quorum = trace["faults"][2]
    kill = next(e for e in quorum["ProcessEvidence"] if e["Action"] == "kill")
    raw = lc.parse(blob(kill["Raw"]))
    require(raw["BeforeInspect"] == value("zone-live-inspect.json") and raw["AfterInspect"] == value("zone-killed.json"), "while-down server evidence changed")
    kill_cmd = command(kill, raw)
    commit = next(e for e in quorum["ProcessEvidence"] if e["Action"] == "commit")
    raw_commit = lc.parse(blob(commit["Raw"]))
    progress_cmd = command(commit, raw_commit)
    require("paired-fault-progress" in progress_cmd["command"], "progress commit not actual client command")
    progress_name = progress_cmd["command"][progress_cmd["command"].index("--name") + 1]
    number = progress_name.rsplit("client", 1)[-1]
    progress = value(f"paired-fault-progress-{number}.json")
    inspect = value(f"paired-fault-progress-{number}-inspect.json")
    require(len(inspect) == 1 and inspect[0]["State"]["Running"] is False and inspect[0]["State"]["ExitCode"] == 0 and inspect[0]["State"]["OOMKilled"] is False, "actual progress caller completion")
    source_step = {k: v for k, v in trace["steps"][5].items() if k != "OriginalRequest"}
    require(lc.parse(blob(quorum["CallerRaw"])) == progress and progress["Steps"][5] == source_step and raw_commit["OriginalRecords"] == progress["Steps"][5]["RawRecords"], "progress caller/durable records changed")
    marker = lc.parse(blob(quorum["MarkerRaw"]))
    require(marker["KillCommand"] == kill_cmd and [commit["EventID"] - 1, progress_cmd] in marker["ProgressCommands"], "while-down marker command binding")
    server = raw["BeforeInspect"][0]["Name"].lstrip("/")
    starts = [(i, c) for i, c in enumerate(ledger) if c["command"] == ["docker", "start", server]]
    require(starts and marker["RestartCommand"] == starts[0][1] and commit["EventID"] - 1 < starts[0][0], "commit not while exact server remained down")

    cohort = trace["faults"][3]
    status = value("status-reopened.json")
    cluster = status["cluster"]
    require(status["client"]["database_status"]["healthy"] is True and cluster["configuration"]["redundancy_mode"] == "triple" and cluster["full_replication"] is True and cluster["data"]["state"]["name"] == "healthy" and cluster["data"]["state"]["min_replicas_remaining"] >= 3 and cluster["recovery_state"]["name"] == "fully_recovered", "actual FDB recovery health")
    require(len(cluster["processes"]) == len(cluster["machines"]) == 4 and len(status["client"]["coordinators"]["coordinators"]) == 3 and {m["locality"]["zoneid"] for m in cluster["machines"].values()} == {f"zone{i}" for i in range(4)}, "actual recovered FDB cohort topology")
    stop_indexes, restart_indexes = [], []
    for event in cohort["ProcessEvidence"]:
        raw = lc.parse(blob(event["Raw"]))
        name = raw["BeforeInspect"][0]["Name"].lstrip("/")
        require(event["Member"] == int(name.rsplit("zone", 1)[-1]), "controller storage member changed")
        require(raw["BeforeInspect"] == value(name + "-all-live.json"), "cohort predecessor artifact changed")
        cmd = command(event, raw)
        if event["Action"] == "kill":
            require(raw["AfterInspect"] == value(name + "-all-stopped.json"), "cohort stop artifact changed")
            stop_indexes.append(event["EventID"] - 1)
        else:
            require(event["Action"] == "reopen" and raw["StoppedInspect"] == value(name + "-all-stopped.json") and raw["AfterInspect"] == value(name + "-reopened.json") and raw["ReadyStatus"] == status, "cohort ready artifacts changed")
            require(cmd["command"] == ["docker", "inspect", name], "actual ready inspection command")
            start = [i for i, c in enumerate(ledger) if c["command"] == ["docker", "start", name] and i < event["EventID"] - 1]
            require(start, "ready observation lacks actual server start")
            restart_indexes.append(start[-1])
    require(max(stop_indexes) < min(restart_indexes), "actual server start preceded entire cohort stop")
    require(lc.parse(blob(cohort["MarkerRaw"])) == {"StopCommandIndexes": stop_indexes, "RestartCommandIndexes": restart_indexes, "ReadyStatus": status}, "cohort marker command order changed")
    require(lc.parse(blob(cohort["CallerRaw"])) == {"Classification": "observer-only", "Status": status}, "observer-only provenance changed")
    held = [i + 1 for i, c in enumerate(ledger) if "paired-fault-hold-old" in c["command"]]
    terminal = [i + 1 for i, c in enumerate(ledger) if "paired-fault-terminal" in c["command"]]
    reopened = [i + 1 for i, c in enumerate(ledger) if "paired-fault-reopened" in c["command"]]
    require(len(held) == len(terminal) == len(reopened) == 1, "actual observation/retry command inventory")
    for old in trace["oldReads"]:
        require(old["EventID"] == (held[0] if old["Mode"] == "held-old" else terminal[0]), "old read not actual observation command")
    require(all(r["EventID"] == reopened[0] for r in cohort["ReopenRetries"]), "post-reopen retries not actual executed client")
    return {"artifacts": len(files), "sealedCommandEvents": len(ledger), "backendSHA256": trace["backendTraceArtifact"]["SHA256"]}


def nonempty(value, label):
    require(type(value) is str and 0 < len(value) <= 1024, label)
    return value


def mapping(trace):
    require(trace["schemaVersion"] == 1 and trace["fixtureID"] == SPEC["fixtureID"], "paired fixture version")
    engine = trace["engine"]
    require(engine in SPEC["coordinators"], "paired engine")
    names, raw = {}, {}
    for item in trace["identityMapping"]:
        integer(item["Raw"], 1, 32)
        require(item["Logical"] not in names and item["Raw"] not in raw, "identity bijection")
        names[item["Logical"]] = item
        raw[item["Raw"]] = item
        nonempty(item["Graph"], "graph identity")
    roles = sorted((n, i["Role"], i["Owner"]) for n, i in names.items())
    require(roles == sorted(tuple(v) for v in SPEC["identityRoles"]), "exact identity roles/owners")
    require(len({v["Graph"] for v in names.values()}) == 1, "graph qualification")
    operations, rounds, requests, txids = {}, {}, set(), set()
    require(len(trace["operationMapping"]) == len(trace["steps"]) == 6, "six operation inventory")
    for declared, item, step in zip(OPS, trace["operationMapping"], trace["steps"]):
        name = declared["Logical"]
        require(item["Logical"] == step["Operation"] == name, "operation order")
        require(step["Coordinator"] == declared["Coordinator"] and item["Coordinator"] == SPEC["coordinators"][engine][declared["Coordinator"]], "coordinator binding")
        integer(item["Round"], 1)
        key = (item["Coordinator"], nonempty(item["Request"], "request key"))
        tx = (item["Coordinator"], nonempty(item["Transaction"], "transaction identity"))
        require(key not in requests and tx not in txids and item["Round"] not in rounds, "operation identity/round bijection")
        requests.add(key); txids.add(tx)
        operations[name] = item
        rounds[item["Round"]] = name
    return names, raw, operations, rounds


def cell(kind="", reference="", operation="", provenance=0, value=0):
    return {"Present": bool(kind), "Null": False, "Kind": kind, "Reference": reference,
            "I64": value, "RevisionOperation": operation, "Provenance": provenance}


def declaration(name):
    return {"ID": name, "Kind": "relationship" if name == "edge" else "node",
            "Mode": "life-bound" if name == "edge" else "", "Type": "edge" if name == "edge" else "",
            "Source": "node-a" if name == "edge" else "", "Target": "node-b" if name == "edge" else ""}


def fixture_view(view, count):
    """Check declared fixture inputs, never construct an observed trace."""
    expected = ["node-a", "node-b", "edge"][:min(count, 3)]
    require([e["ID"] for e in view["Entities"]] == expected, "exact entity set/order")
    lives = []
    for name in expected:
        lives.append({"Owner": name, "Life": {"node-a": "life-a1", "node-b": "life-b1", "edge": "life-edge"}[name],
                      "SourceLife": "life-a1" if name == "edge" else "", "TargetLife": "life-b1" if name == "edge" else ""})
    require(view["Lives"] == lives, "immutable lives; unused future life must remain absent")
    for entity in view["Entities"]:
        name = entity["ID"]
        require(all(entity[k] == v for k, v in declaration(name).items()), "immutable declaration")
        ordinal = {"node-a": 0, "node-b": 1, "edge": 2}[name]
        life = lives[ordinal]["Life"]
        require(entity["Presence"] == cell("life", life, OPS[ordinal]["Logical"], ordinal + 1), "exact presence/reference/revision")
        require(entity["Effective"] is True and entity["EffectiveLife"] == life and entity["Labels"] == [], "exact active lifecycle")
        property_cell = cell()
        if name == "edge":
            operation = OPS[count - 1]
            property_cell = cell("i64", operation["Reference"], operation["Logical"], operation["Provenance"], operation["I64"])
        require(entity["Property"] == property_cell, "typed property and ABA revision/provenance")
    if view["ObservationScope"] != "edge-only":
        require(view["CensusBounds"] == [1, 32] and view["CensusIDs"] == sorted(expected), "complete negative census")
        require(view["PhantomCount"] == 2 and view["PhantomFound"] == [False, False], "phantom absence")
        postings = []
        for endpoint, life in (("node-a", ""), ("node-a", "life-a1"), ("node-a", "life-a2"), ("node-b", ""), ("node-b", "life-b1")):
            edges = ["edge"] if count >= 3 and life != "life-a2" else []
            postings.append({"Endpoint": endpoint, "Life": life, "Declared": edges, "Effective": edges})
        require(view["Postings"] == postings, "exact both endpoint postings")


def current_rows(rows, physical, graph):
    actual = {}
    for row in rows:
        owner = integer(row["Group"], 0, 1)
        prefix = f"g{owner}/{graph.encode().hex()}/current/"
        key = blob(row["Key"]).decode()
        require(key.startswith(prefix), "current graph/owner prefix")
        logical = bytes.fromhex(key[len(prefix):]).decode()
        require((owner, logical) not in actual, "duplicate physical current row")
        actual[owner, logical] = lc.parse(blob(row["Value"]))
    require(actual == physical, "raw current differs from complete physical ledger")


def logical_group(step, previous, count):
    """Derive exact CDC from the two observed component states and fixture input."""
    name = step["Operation"]
    ordinal = count - 1
    declaration_names = ["node-a", "node-b", "edge"]
    absent = cell(); absent["Kind"] = "absent"
    entities = {e["ID"]: e for e in step["After"]["Entities"]}
    changes = []
    if count <= 3:
        owner = declaration_names[ordinal]
        changes.append({"Owner": owner, "Life": "", "Component": "presence", "Name": "", "Before": absent, "After": entities[owner]["Presence"], "AllScope": SPEC["scope"]})
    if count >= 3:
        before = absent if count == 3 else previous["Entities"][-1]["Property"]
        changes.append({"Owner": "edge", "Life": "life-edge", "Component": "property", "Name": "p", "Before": before, "After": entities["edge"]["Property"], "AllScope": SPEC["scope"]})
    actual = [{k: v for k, v in c.items() if k != "Scope"} for c in step["LogicalCDC"]]
    require(actual == changes, "exact complete logical component effects/before/after")
    require(len(step["NewEntities"]) == (1 if count <= 3 else 0), "new immutable entity inventory")
    if count <= 3:
        entity = step["NewEntities"][0]
        require(all(entity[k] == v for k, v in declaration(declaration_names[ordinal]).items()), "new entity definition")
        require(step["NewLives"] == [step["After"]["Lives"][-1]], "new immutable life inventory")
    else:
        require(not step["NewLives"], "update introduced a life")
    expected = [{"Reference": OPS[ordinal]["Reference"], "Kind": "i64", "I64": OPS[ordinal]["I64"]}] if count in (3, 4) else []
    require(step["NewValues"] == expected, "actual definition membership/reuse")


def native_decoded(raw, view, names, raw_ids):
    """Transparent JSON witnesses supplement the trusted native opaque-codec boundary."""
    observed = {raw_ids[e["id"]]["Logical"]: e for e in raw["entities"] if e["found"]}
    require(len(observed) == len(view["Entities"]), "native duplicate entity witness")
    for entity in view["Entities"]:
        item = observed[entity["ID"]]
        require(item["kind"] == (2 if entity["Kind"] == "relationship" else 1), "native declaration kind")
        require(item["effective"] == entity["Effective"], "native effective witness")
        require(raw_ids[item["effectiveLife"]]["Logical"] == entity["EffectiveLife"], "native life witness")
        if entity["ID"] == "edge":
            require(item["mode"] == 1 and item["type"] == "edge" and raw_ids[item["source"]]["Logical"] == "node-a" and raw_ids[item["target"]]["Logical"] == "node-b", "native immutable endpoints/mode")
    lives = [{"Owner": raw_ids[l["owner"]]["Logical"], "Life": raw_ids[l["life"]]["Logical"],
              "SourceLife": raw_ids[l["sourceLife"]]["Logical"] if l["sourceLife"] else "",
              "TargetLife": raw_ids[l["targetLife"]]["Logical"] if l["targetLife"] else ""} for l in raw["lives"] if l["found"]]
    require(lives == view["Lives"], "native immutable life witness")
    if view["ObservationScope"] != "edge-only":
        postings = [{"Endpoint": raw_ids[p["endpoint"]]["Logical"], "Life": raw_ids[p["life"]]["Logical"] if p["life"] else "",
                     "Declared": [raw_ids[n]["Logical"] for n in p["declared"]],
                     "Effective": [raw_ids[n]["Logical"] for n in p["effective"]]} for p in raw["postings"]]
        require(postings == view["Postings"], "native both-posting witness")
        require(len(raw["phantomEntities"]) == 2 and all(p["found"] is False for p in raw["phantomEntities"]), "native phantom witness")


def observation(trace, observed, count, context, compact=False, current=False):
    names, raw_ids, operations, rounds = context
    view = observed["View"]
    require(view["ObservationScope"] == ("edge-only" if compact else "graph-complete-in-issued-range" if trace["engine"] == "native-raft" else "graph-complete-in-fixture-domain"), "observation coverage")
    normalized = lc.view(view, names, operations)
    if compact:
        require(count == 3 and view["CensusBounds"] == [0, 0] and not view["CensusIDs"] and not view["Postings"] and view["PhantomCount"] == 0 and not view["PhantomFound"], "held edge-only scope")
        entity = view["Entities"][0]
        require(all(entity[k] == v for k, v in declaration("edge").items()) and entity["Presence"] == cell("life", "life-edge", "edge-create", 3) and entity["Effective"] is True and entity["EffectiveLife"] == "life-edge", "held immutable edge/presence")
        require(entity["ID"] == "edge" and entity["Property"] == cell("i64", "value5", "edge-create", 3, 5), "held old edge property/revision")
        require(view["Lives"] == [{"Owner": "edge", "Life": "life-edge", "SourceLife": "life-a1", "TargetLife": "life-b1"}], "held actual edge life")
    else:
        fixture_view(view, count)
    raw = observed["RawObservation"]
    if trace["engine"] == "native-raft":
        lc.native_raw(raw, view, raw_ids, rounds)
        native_decoded(raw, view, names, raw_ids)
        require(observed["Cut"]["Certificates"] and all(blob(c) for c in observed["Cut"]["Certificates"]), "native opaque cut retained")
    else:
        scope, physical = lc.fdb_raw(raw, view, operations, names, observed["Cut"])
        require(scope == SPEC["scope"], "stored fixed scope")
        require(len(raw["Groups"]) == count, "complete logical group prefix")
        require(not current or "RawCurrent" in observed, "actual physical current observation missing")
        if "RawCurrent" in observed:
            current_rows(observed["RawCurrent"], physical, next(iter(names.values()))["Graph"])
    return normalized


def raw_records(trace, records, count, context):
    require(records, "actual original records missing")
    if trace["engine"] == "native-raft":
        coordinates = [(r["Partition"], integer(r["Index"], 1)) for r in records]
        require(all(p in (3, 8) for p, _ in coordinates) and len(coordinates) == len(set(coordinates)), "native record source/index")
        require(all(blob(r["Outcome"]) for r in records), "native actual outcome bytes")
        return
    group = trace["steps"][count - 1]["RawObservation"]["Groups"][-1]
    outcome = group["Outcome"]
    graph = next(iter(context[0].values()))["Graph"]
    prefix = f"g{outcome['Coordinator']}/{graph.encode().hex()}/"
    keys = [prefix + f"changes/{outcome['Sequence']:016x}/" + outcome["Request"].encode().hex(), prefix + "request/" + outcome["Request"].encode().hex(), prefix + "id/" + outcome["ID"].encode().hex()]
    require(len(records) == 3, "FDB original group/request/id inventory")
    for row, key, expected in zip(records, keys, (group, outcome, outcome)):
        require(row["Group"] == outcome["Coordinator"] and blob(row["Key"]).decode() == key and blob(row["Value"]) == lc.go_json(expected), "FDB exact original witness")


def coordinates(result, original):
    require(all(result[k] == original[k] for k in ("Coordinator", "Request", "Transaction", "Round")), "original result coordinates")
    if "Logical" in result:
        require(result["Logical"] == original["Logical"], "original logical result")


def request(trace, value, original, context):
    require(value["Graph"] == next(iter(context[0].values()))["Graph"], "input graph")
    require(all(value[k] == original[k] for k in ("Coordinator", "Request", "Transaction")), "original request key/transaction")
    payload = blob(value["Payload"])
    require(payload, "original source-produced input bytes")
    if "InputSHA256" in value:
        require(value["InputSHA256"] == hashlib.sha256(payload).hexdigest(), "input hash")
    if trace["engine"] == "foundationdb":
        group = next(g for g in trace["steps"][-1]["RawObservation"]["Groups"] if g["Tx"]["ID"] == original["Transaction"])
        require(payload == lc.go_json(group["Tx"]), "FDB original input differs from persisted group")
    return payload


def new_records(trace, records):
    if trace["engine"] == "native-raft":
        keys = [(r["Partition"], integer(r["Index"], 1)) for r in records]
        require(all(p in (3, 8) for p, _ in keys) and len(keys) == len(set(keys)), "new native record source/index")
        require(all(blob(r["Outcome"]) and r["CDC"] == "" for r in records), "retry/refusal emitted native graph CDC")
    else:
        require(not records, "FDB retry/refusal added complete group")


def replay(trace, value, count, original, payload, context):
    require(blob(value["Payload"]) == payload and blob(value["RawResult"]), "retry input/actual result")
    coordinates(value["Result"], original)
    before = observation(trace, value["Before"], count, context, current=True)
    after = observation(trace, value["After"], count, context, current=True)
    require(before == after, "retry changed graph")
    if trace["engine"] == "foundationdb":
        require(value["Before"]["RawObservation"]["Groups"] == value["After"]["RawObservation"]["Groups"], "retry changed group ledger")
        group = next(g for g in value["After"]["RawObservation"]["Groups"] if g["Tx"]["ID"] == original["Transaction"])
        actual = lc.parse(blob(value["RawResult"]))
        require(actual == group["Outcome"] or actual == {"Recovered": group["Outcome"], "Retried": group["Outcome"]}, "FDB actual recovered/retry outcome")
    new_records(trace, value["NewRawRecords"])


def process_event(event, engine):
    integer(event["EventID"], 1)
    action = event["Action"]
    require(action in ("kill", "stop", "commit", "reopen") and event["Role"] in ("client", "replica", "storage"), "actual event kind/role")
    nonempty(event["Instance"], "actual instance")
    require(blob(event["Raw"]), "actual process event evidence")
    if action in ("kill", "stop"):
        require(event["Reaped"] is True and event["Signal"] in ("SIGKILL", "SIGTERM", "clean-exit"), "actual stop/reap")
        require(event["OOMKilled"] is None or event["OOMKilled"] is False, "OOM is not declared fault evidence")
    if action == "reopen":
        require(event["Ready"] is True and blob(event["RecoveryRaw"]), "actual ready/recovery observation")
        require(event["Instance"] != event["PreviousInstance"] and event["Volume"], "fresh same-volume process")
        require("Reaped" not in event, "live reopened process labelled reaped")
    if engine == "native-raft" and action in ("kill", "stop", "reopen"):
        raw = lc.parse(blob(event["Raw"]))
        member = integer(raw["Member"], 1, 6)
        pid = integer(raw["PID"], 1)
        require(event["Member"] == member and raw["Instance"] == event["Instance"] and raw["Volume"] == event["Volume"], "native actual member/instance/volume")
        parts = event["Instance"].split("/")
        require(len(parts) == 3 and parts[0] == f"voter{member}" and parts[1] == f"pid{pid}" and len(parts[2]) == 32 and bytes.fromhex(parts[2]), "native actual PID/incarnation")
        require(Path(event["Volume"]).is_absolute() and Path(event["Volume"]).name == str(member), "native durable directory")
        if action in ("kill", "stop"):
            require(raw["Reaped"] is True and raw["Signal"] == event["Signal"] and raw["OOMKilled"] is event["OOMKilled"], "native OS reap/signal/OOM binding")
            wait = integer(raw["WaitStatus"], 0, (1 << 32) - 1)
            require(type(raw["Signaled"]) is bool and type(raw["Success"]) is bool, "native OS wait flags")
            if action == "kill":
                require(event["Signal"] == "SIGKILL" and raw["Signaled"] is True and raw["SignalNumber"] == 9 and wait & 0x7f == 9 and raw["ExitCode"] == -1 and raw["Success"] is False, "native actual SIGKILL wait status")
                nonempty(raw["Error"], "native actual killed-process wait error")
            else:
                require(event["Signal"] == "clean-exit" and raw["Signaled"] is False and raw["Success"] is True and raw["ExitCode"] == wait == 0, "native actual graceful exit")
        else:
            require(raw["PreviousInstance"] == event["PreviousInstance"] and raw["Ready"] is True and raw["RecoveryRaw"] == event["RecoveryRaw"], "native actual startup/recovery binding")
    if engine == "foundationdb" and action in ("kill", "stop", "reopen"):
        raw = lc.parse(blob(event["Raw"]))
        before, after = raw["BeforeInspect"], raw["AfterInspect"]
        require(len(before) == len(after) == 1, "actual Docker instance inventory")
        b, a = before[0], after[0]
        require(b["Id"] == a["Id"] and b["State"]["Running"] is True and b["State"]["Pid"] > 0, "Docker actual predecessor")
        instance = lambda r: f"{r['Id']}:{r['State']['Pid']}:{r['State']['StartedAt']}"
        command = raw["Command"]
        require(command["exit"] == 0 and command["deadline_expired"] is False, "actual process command failed")
        if action in ("kill", "stop"):
            require(instance(b) == event["Instance"] and a["State"]["Running"] is False and a["State"]["OOMKilled"] is event["OOMKilled"], "Docker stop facts")
            require(command["command"] == ["docker", "kill", "--signal", "KILL", b["Name"].lstrip("/")] and a["State"]["ExitCode"] == 137 and event["Signal"] == "SIGKILL", "actual injected Docker kill")
        else:
            require(instance(b) == event["PreviousInstance"] and instance(a) == event["Instance"] and a["State"]["Running"] is True and a["State"]["Pid"] > 0 and a["State"]["OOMKilled"] is False, "Docker actual replacement")
            require(blob(event["RecoveryRaw"]) == json.dumps(raw["ReadyStatus"], separators=(",", ":")).encode(), "actual recovery bytes")
        if event["Role"] == "storage":
            volumes = lambda r: [m["Name"] for m in r["Mounts"] if m["Destination"] == "/var/fdb/data" and m["Type"] == "volume"]
            require(volumes(b) == volumes(a) == [event["Volume"]], "actual persisted volume")


def old_checkpoint(event, held, first_stop, ready, quorum_kill):
    integer(event, 1)
    if held:
        require(event < first_stop and event < quorum_kill, "held owner died before actual old read/close")
    else:
        require(event > ready, "old read crossed dead owner boundary")


def analyze(trace):
    trace = lc.normalize(trace)
    for fault in trace["faults"]:
        for retry in [fault.get("Retry"), *fault.get("ReopenRetries", [])]:
            if retry is not None and retry.get("NewRawRecords") is None:
                retry["NewRawRecords"] = []
        if fault.get("ChangedPayload") is not None and fault["ChangedPayload"].get("NewRawRecords") is None:
            fault["ChangedPayload"]["NewRawRecords"] = []
    context = mapping(trace)
    names, raw_ids, operations, rounds = context
    normalized, payloads = [], {}
    for count, step in enumerate(trace["steps"], 1):
        if count == 1:
            fixture_view(step["Before"], 0)
        else:
            require(step["Before"] == trace["steps"][count - 2]["After"], "step continuity")
        after = observation(trace, {"Cut": step["Cut"], "View": step["After"], "RawObservation": step["RawObservation"]}, count, context)
        raw_records(trace, step["RawRecords"], count, context)
        operation = OPS[count - 1]
        payloads[step["Operation"]] = request(trace, step["OriginalRequest"], operations[step["Operation"]], context)
        changes = []
        for change in step["LogicalCDC"]:
            lc.cell(change["Before"], names, operations); lc.cell(change["After"], names, operations)
            require(change["AllScope"] == SPEC["scope"] and blob(change["Scope"]), "actual AllScope")
            require(change["After"]["RevisionOperation"] == step["Operation"] and change["After"]["Provenance"] == operation["Provenance"], "actual CDC revision/provenance")
            item = dict(change); del item["Scope"]
            changes.append(item)
        require(len(changes) == (2 if count == 3 else 1), "complete logical CDC group")
        logical_group(step, step["Before"], count)
        normalized.append({"Operation": step["Operation"], "Coordinator": step["Coordinator"], "Before": lc.view(step["Before"], names, operations), "After": after, "LogicalCDC": changes, "NewEntities": step["NewEntities"], "NewLives": step["NewLives"], "NewValues": step["NewValues"]})
    require(len(trace["faults"]) == 4, "four fault cases")
    registry, cases = {}, {}
    for declaration_, fault in zip(CASES, trace["faults"]):
        case, operation = declaration_["Case"], declaration_["Operation"]
        require(fault["Case"] == case and fault["Operation"] == operation and fault["CallerClassification"] == declaration_["CallerClassification"], "caller fault classification")
        require(nonempty(fault["Marker"], "fault marker") and blob(fault["MarkerRaw"]) and blob(fault["CallerRaw"]), "actual marker/caller evidence")
        count = next(i + 1 for i, op in enumerate(OPS) if op["Logical"] == operation)
        require(request(trace, fault["OriginalRequest"], operations[operation], context) == payloads[operation], "fault input differs from original")
        observer = observation(trace, fault["Observer"], count, context, current=True)
        require(observer == normalized[count - 1]["After"], "durable observer differs from original result")
        raw_records(trace, fault["Observer"]["RawRecords"], count, context)
        events = fault["ProcessEvidence"]
        require(events and [e["EventID"] for e in events] == sorted({e["EventID"] for e in events}), "ordered actual events")
        for event in events:
            process_event(event, trace["engine"])
            prior = registry.setdefault(event["EventID"], event)
            require(prior == event, "one event ID changed evidence")
        if case == "precommit-uncertainty":
            kills = [e for e in events if e["Action"] == "kill" and e["Role"] == "client" and e["Signal"] == "SIGKILL"]
            require(len(kills) == 1, "actual precommit client loss")
        if declaration_["Retry"]:
            replay(trace, fault["Retry"], count, operations[operation], payloads[operation], context)
        if declaration_["Intermediate"]:
            intermediate = fault["Intermediate"]
            require(blob(intermediate["Raw"]), "actual intermediate response")
            if intermediate["Classification"] == "pending":
                require(trace["engine"] == "native-raft" and not any(k in intermediate for k in ("Cut", "View", "RawObservation")), "pending became complete/empty graph")
            else:
                require(intermediate["Classification"] == "old-complete", "intermediate classification")
                require(observation(trace, intermediate, 3, context, current=True) == normalized[2]["After"], "intermediate old cut differs")
        if declaration_["ChangedPayload"]:
            mismatch = fault["ChangedPayload"]
            require(mismatch["Classification"] == "mismatch" and blob(mismatch["Raw"]) and blob(mismatch["Payload"]) != payloads[operation], "valid changed-payload refusal")
            require(observation(trace, mismatch["Before"], count, context, current=True) == observation(trace, mismatch["After"], count, context, current=True), "mismatch changed graph")
            if trace["engine"] == "foundationdb":
                tx = lc.parse(blob(mismatch["Payload"]))
                require(tx["ID"] == operations[operation]["Transaction"] and tx["Request"] == operations[operation]["Request"] and tx["Coordinator"] == operations[operation]["Coordinator"], "changed payload changed request key")
                config = lc.parse(blob(mismatch["Before"]["RawObservation"]["Config"][0]["Value"]))
                effects, reads = lc.graph_contract(tx["GraphChange"], config)
                require(tx["Graph"] == config["Graph"] and tx["Topology"] == config["Topology"], "changed payload namespace")
                require([p["Group"] for p in tx["Participants"]] == sorted(effects), "changed payload participant inventory")
                require(all(p["Effects"] == effects[p["Group"]] and [r["Key"] for r in p["Reads"]] == reads[p["Group"]] for p in tx["Participants"]), "changed payload invalid shape")
                metadata = tx["Lifecycle"]
                integer(metadata["Provenance"], 1)
                require(metadata["EdgeLife"] == config["Lifecycle"]["EdgeLife"] and metadata["Definition"] in config["Lifecycle"]["Values"] and metadata["Definition"]["Kind"] == "i64" and metadata["Definition"]["I64"] == tx["GraphChange"]["Edge"]["Value"], "changed payload invalid typed definition/life")
                require(mismatch["Before"]["RawObservation"]["Groups"] == mismatch["After"]["RawObservation"]["Groups"], "mismatch changed complete groups")
            new_records(trace, mismatch["NewRawRecords"])
        cases[case] = fault
    lost = cases["lost-committed-reply"]["ProcessEvidence"]
    quorum = cases["quorum-preserving-failure"]["ProcessEvidence"]
    for events, label in ((lost, "lost reply"), (quorum, "quorum while down")):
        killed = [e for e in events if e["Action"] == "kill"]
        commits = [e for e in events if e["Action"] == "commit"]
        require(len(killed) == len(commits) == 1, label + " actual kill/commit")
        require(killed[0]["Role"] == "client" if events is lost else killed[0]["Role"] in ("replica", "storage"), label + " failure role")
        coordinates(commits[0]["Result"], operations["postcommit-update" if events is lost else "zone-loss-update"])
        require(commits[0]["EventID"] < killed[0]["EventID"] if events is lost else killed[0]["EventID"] < commits[0]["EventID"], label + " event order")
        require(not any(e["Action"] == "reopen" and e.get("PreviousInstance") == killed[0]["Instance"] and e["EventID"] < commits[0]["EventID"] for e in registry.values()), label + " stopped instance revived")
    if trace["engine"] == "foundationdb":
        lost_fault = cases["lost-committed-reply"]
        marker = lc.parse(blob(lost_fault["MarkerRaw"]))
        receipt_bytes = blob(marker["PreKillReceiptRaw"])
        receipt = lc.parse(receipt_bytes)
        original_marker = lc.parse(blob(marker["MarkerRaw"]))
        require(original_marker == {"ready": "after-native-commit-before-reply", "receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest()}, "pre-kill receipt marker binding")
        raw_records(trace, receipt["Records"], 5, context)
        require(blob(receipt["Original"]) == payloads["postcommit-update"] and receipt["Outcome"] == trace["steps"][4]["RawObservation"]["Groups"][4]["Outcome"], "pre-kill original decision")
        commit = next(e for e in lost if e["Action"] == "commit")
        event_raw = lc.parse(blob(commit["Raw"]))
        require(event_raw["PreKillReceipt"] == receipt and event_raw["OriginalRecords"] == receipt["Records"], "pre-kill event replaced by later observer")
        quorum_commit = next(e for e in quorum if e["Action"] == "commit")
        raw_records(trace, lc.parse(blob(quorum_commit["Raw"]))["OriginalRecords"], 6, context)
    cohort = cases["same-volume-reopen"]["ProcessEvidence"]
    stops, starts = [e for e in cohort if e["Action"] in ("kill", "stop")], [e for e in cohort if e["Action"] == "reopen"]
    members = set(range(1, 7)) if trace["engine"] == "native-raft" else set(range(4))
    require({e["Member"] for e in stops} == {e["Member"] for e in starts} == members and len(stops) == len(starts) == len(members), "entire storage cohort")
    require(max(e["EventID"] for e in stops) < min(e["EventID"] for e in starts), "whole cohort stopped before any reopen")
    require(len({e["Instance"] for e in stops}) == len(members) and len({e["Instance"] for e in starts}) == len(members), "unique process cohort")
    for start in starts:
        stop = next(e for e in stops if e["Member"] == start["Member"])
        require(stop["Role"] in ("replica", "storage") and start["Role"] in ("replica", "storage") and start["PreviousInstance"] == stop["Instance"] and stop["Volume"] == start["Volume"] != "", "same member/volume predecessor")
    ready = max(e["EventID"] for e in starts)
    retries = cases["same-volume-reopen"]["ReopenRetries"]
    require(len(retries) == 6 and [r["Operation"] for r in retries] == [o["Logical"] for o in OPS], "six executed post-reopen originals")
    require([integer(r["EventID"], ready + 1) for r in retries] == sorted(r["EventID"] for r in retries), "actual post-ready retry order")
    for retry in retries:
        operation = retry["Operation"]
        replay(trace, retry, 6, operations[operation], payloads[operation], context)
    expected_old = {tuple(v) for v in SPEC["oldReads"]}
    require(len(trace["oldReads"]) == len(expected_old) and {(r["Operation"], r["Mode"]) for r in trace["oldReads"]} == expected_old, "exact retained seed/ABA reads")
    old_results = []
    for old in trace["oldReads"]:
        count = next(i + 1 for i, op in enumerate(OPS) if op["Logical"] == old["Operation"])
        require(old["Cut"] == trace["steps"][count - 1]["Cut"], "old cut identity changed")
        held = old["Mode"] == "held-old"
        require(old["Checkpoint"] == ("before-cohort-stop" if held else "after-cohort-reopen"), "old owner checkpoint")
        old_checkpoint(old["EventID"], held, min(e["EventID"] for e in stops), ready, next(e["EventID"] for e in quorum if e["Action"] == "kill"))
        old_results.append({"Operation": old["Operation"], "Mode": old["Mode"], "View": observation(trace, old, count, context, held)})
    old_results.sort(key=lambda r: (r["Operation"], r["Mode"]))
    if trace["engine"] == "foundationdb":
        final = trace["steps"][-1]
        _, physical = lc.fdb_raw(final["RawObservation"], final["After"], operations, names, final["Cut"])
        current_rows(trace["finalRawCurrent"], physical, next(iter(names.values()))["Graph"])
    return {"steps": normalized, "oldReads": old_results}, {op: hashlib.sha256(value).hexdigest() for op, value in payloads.items()}


def validate(trace, artifact_root=None):
    try:
        evidence = fdb_artifacts(trace, artifact_root) if trace["engine"] == "foundationdb" else None
        normalized, inputs = analyze(trace)
        return normalized, inputs, evidence
    except Refusal:
        raise
    except (KeyError, TypeError, ValueError, IndexError, StopIteration, OverflowError, OSError) as error:
        raise Refusal(f"malformed paired trace/artifacts: {error}") from error


def compare(native, fdb, fdb_artifact_root=None):
    try:
        require(native["engine"] == "native-raft" and fdb["engine"] == "foundationdb", "engine order")
        left, native_inputs, _ = validate(native)
        right, fdb_inputs, evidence = validate(fdb, fdb_artifact_root)
        require(left == right, "actual paired fault graph/history/CDC differ")
        return {"equal": True, "fixtureID": SPEC["fixtureID"], "logicalOperations": 6,
                "completeLogicalCDCGroups": 6, "componentChanges": 7, "faultCases": 4,
                "originalInputSHA256": {"native-raft": native_inputs, "foundationdb": fdb_inputs},
                "FDBArtifactEvidence": evidence,
                "scope": SPEC["acceptanceLimit"], "nativeOpaqueCodecBoundary": "qualified actual exporter; not decoded into authority by Python"}
    except Refusal:
        raise
    except (KeyError, TypeError, ValueError, IndexError, StopIteration, OverflowError) as error:
        raise Refusal(f"malformed paired trace: {error}") from error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("native", nargs="?"); parser.add_argument("fdb", nargs="?")
    parser.add_argument("--fdb-only", metavar="TRACE", help="qualify actual FDB component; never pair acceptance")
    parser.add_argument("--fdb-artifact-root", type=Path)
    args = parser.parse_args()
    if args.fdb_only:
        if args.native or args.fdb:
            parser.error("--fdb-only does not take pair arguments")
        fdb, fdb_sha = load(args.fdb_only)
        require(fdb["engine"] == "foundationdb", "FDB-only engine")
        _, inputs, evidence = validate(fdb, args.fdb_artifact_root)
        print(json.dumps({"validatedEngine": "foundationdb", "fullPairAccepted": False, "fdbSHA256": fdb_sha,
                          "originalInputSHA256": inputs, "artifactEvidence": evidence, "scope": SPEC["acceptanceLimit"]}, indent=2))
        return
    if not args.native or not args.fdb:
        parser.error("supply native and FDB traces or --fdb-only")
    native, native_sha = load(args.native)
    fdb, fdb_sha = load(args.fdb)
    report = compare(native, fdb, args.fdb_artifact_root)
    report.update(nativeSHA256=native_sha, fdbSHA256=fdb_sha)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (Refusal, KeyError, TypeError, ValueError, IndexError, StopIteration, OSError) as error:
        raise SystemExit(f"comparison refused: {error}")
