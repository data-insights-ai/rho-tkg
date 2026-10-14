#!/usr/bin/env python3
"""P03 native trace bridge; reuse graph/cut checks and the bounded serial oracle.

The Go test qualifies authentic process extraction and opaque native codecs.
This offline bridge checks those retained observations and their correspondence;
it neither runs the old four-fault acceptance wrapper nor proves disk durability.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


FAULT = module("p03_graph_checks", Path(__file__).with_name("compare-graph-fault.py"))
ORACLE = ROOT / "docs/v5/reference/independent/protocol/oracle.py"
ORACLE_SHA = "a132b4e4981f3dcea485b7ee5dea0df48a906cdfcda4a75bec5ff9f1a5589d3a"
require, blob = FAULT.require, FAULT.blob


def mapping(trace):
    require(trace["schemaVersion"] == 1 and trace["fixtureID"] == FAULT.SPEC["fixtureID"] and trace["engine"] == "native-raft", "P03 native fixture")
    coordinator = trace["rawCounts"]["coordinatorPartition"]
    require(coordinator in (3, 8), "P03 A/B coordinator")
    names = {i["Logical"]: i for i in trace["identityMapping"]}
    raw = {i["Raw"]: i for i in trace["identityMapping"]}
    require(len(names) == len(raw) == len(trace["identityMapping"]) and
            sorted((n, i["Role"], i["Owner"]) for n, i in names.items()) == sorted(tuple(v) for v in FAULT.SPEC["identityRoles"]), "P03 identity bijection")
    require(len({i["Graph"] for i in names.values()}) == 1, "P03 graph identity")
    operations, rounds, requests, transactions = {}, {}, set(), set()
    require(len(trace["steps"]) == len(trace["operationMapping"]) == 6, "P03 six originals")
    for index, (declared, op, step) in enumerate(zip(FAULT.OPS, trace["operationMapping"], trace["steps"])):
        part = coordinator if index == 4 else 3 if index == 0 else 8
        require(op["Logical"] == step["Operation"] == declared["Logical"] and op["Coordinator"] == part and step["Coordinator"] == {3: "A", 8: "B"}[part], "P03 true coordinator mapping")
        FAULT.integer(op["Round"], 1)
        request, tx = (part, op["Request"]), (part, op["Transaction"])
        require(op["Request"] and op["Transaction"] and request not in requests and tx not in transactions and op["Round"] not in rounds, "P03 original identity/round bijection")
        requests.add(request); transactions.add(tx)
        operations[op["Logical"]], rounds[op["Round"]] = op, op["Logical"]
    return coordinator, (names, raw, operations, rounds)


def projected(view):
    """Project checked observed entities/lives/postings; no graph reducer."""
    owner = lambda name: "B" if name == "node-b" else "A"
    values = {owner(e["ID"]) + "/entity/" + e["ID"]: e for e in view["Entities"]}
    values.update({owner(l["Owner"]) + "/life/" + l["Life"]: l for l in view["Lives"]})
    values.update({owner(p["Endpoint"]) + "/posting/" + p["Endpoint"] + "/" + p["Life"]: p for p in view["Postings"]})
    return values


def cut_round(cut):
    parts, rounds = set(), set()
    for encoded in cut["Certificates"]:
        wire = blob(encoded)
        require(len(wire) == 392 and wire[:4] == b"GSC\1" and hashlib.sha256(wire[:-32]).digest() == wire[-32:], "P03 sealed original cut")
        body = wire[4:-32]
        part = int.from_bytes(body[16:24], "big")
        require(part in (3, 8) and part not in parts, "P03 cut participant inventory")
        parts.add(part)
        rounds.add(int.from_bytes(body[152:160], "big"))
    require(parts == {3, 8} and len(rounds) == 1, "P03 complete equal-round cut")
    return rounds.pop()


def check(data):
    trace = FAULT.lc.normalize(data)
    for fault in trace["faults"]:
        for retry in [fault.get("Retry"), *fault.get("ReopenRetries", [])]:
            if retry is not None and retry.get("NewRawRecords") is None:
                retry["NewRawRecords"] = []
    coordinator, context = mapping(trace)
    operations = context[2]
    require(hashlib.sha256(ORACLE.read_bytes()).hexdigest() == ORACLE_SHA, "P03 reviewed oracle hash")
    search = module("p03_existing_serial_oracle", ORACLE).serial_witness
    transactions, observations, inputs = {}, [], {}
    initial = projected(trace["steps"][0]["Before"])
    previous = initial
    footprints = [
        {"A/entity/node-a", "A/life/life-a1"},
        {"B/entity/node-b", "B/life/life-b1"},
        {"A/entity/edge", "A/life/life-edge", "A/posting/node-a/", "A/posting/node-a/life-a1", "B/posting/node-b/", "B/posting/node-b/life-b1"},
        {"A/entity/edge"}, {"A/entity/edge"}, {"A/entity/edge"},
    ]
    for count, step in enumerate(trace["steps"], 1):
        require(projected(step["Before"]) == previous, "P03 observed step continuity")
        FAULT.fixture_view(step["Before"], count - 1)
        FAULT.observation(trace, {"Cut": step["Cut"], "View": step["After"], "RawObservation": step["RawObservation"]}, count, context)
        FAULT.raw_records(trace, step["RawRecords"], count, context)
        FAULT.logical_group(step, step["Before"], count)
        payload = FAULT.request(trace, step["OriginalRequest"], operations[step["Operation"]], context)
        inputs[step["Operation"]] = payload
        after = projected(step["After"])
        changed = {k for k in previous.keys() | after.keys() if previous.get(k) != after.get(k)}
        require(changed == footprints[count - 1], "P03 observed original footprint")
        # Observed-prefix dependencies check serial/cut consistency. This is not
        # a reconstruction of the protocol's prepared read footprint.
        reads = [{"kind": "range", "lo": "A/", "hi": "B0", "values": previous}]
        transactions[step["Operation"]] = {"reads": reads, "writes": {k: {"op": "put", "value": after[k]} for k in changed}}
        observations.append({"event": step["Operation"], "scope": ["A", "B"], "round": cut_round(step["Cut"]), "values": after})
        previous = after
    faults = trace["faults"]
    require([f["Case"] for f in faults] == ["coordinator-leader-loss-after-commit", "same-volume-reopen"], "P03 distinct fault cases")
    fault, cohort = faults
    require(fault["CallerClassification"] == "pending" and blob(fault["CallerRaw"]), "P03 original uncertain caller")
    commit, killed, rejoined = fault["ProcessEvidence"]
    for event in [commit, killed, rejoined, *cohort["ProcessEvidence"]]:
        FAULT.process_event(event, "native-raft")
    require([e["Action"] for e in (commit, killed, rejoined)] == ["commit", "kill", "reopen"] and commit["EventID"] < killed["EventID"] < rejoined["EventID"], "P03 commit/kill/rejoin order")
    leader = 1 if coordinator == 3 else 4
    require(commit["Member"] == killed["Member"] == rejoined["Member"] == leader and killed["Role"] == "replica" and rejoined["PreviousInstance"] == killed["Instance"] and rejoined["Volume"] == killed["Volume"], "P03 exact coordinator leader and directory")
    op = operations["postcommit-update"]
    FAULT.coordinates(commit["Result"], op)
    captured = FAULT.lc.parse(blob(commit["Raw"]))
    require(blob(captured["Response"]) == blob(fault["MarkerRaw"]) and all(blob(captured[k]) for k in ("Request", "Manifest", "VoterRecords")), "P03 pre-kill capture binding")
    voters = FAULT.lc.parse(blob(captured["VoterRecords"]))
    require([r["Member"] for r in voters] == list(range(leader, leader + 3)) and all(blob(r["Record"]) for r in voters), "P03 all coordinator voter records")
    for retry in [fault["Retry"], *fault["ReopenRetries"]]:
        FAULT.replay(trace, retry, 5, op, inputs["postcommit-update"], context)
    require(killed["EventID"] < fault["Retry"]["EventID"] < rejoined["EventID"], "P03 original recovered while coordinator down")
    stops, starts = cohort["ProcessEvidence"][:6], cohort["ProcessEvidence"][6:]
    require(len(starts) == 6 and {e["Member"] for e in stops} == {e["Member"] for e in starts} == set(range(1, 7)) and all(e["Action"] == "stop" for e in stops) and all(e["Action"] == "reopen" for e in starts) and max(e["EventID"] for e in stops) < min(e["EventID"] for e in starts), "P03 entire stopped/reopened cohort")
    for start in starts:
        stop = next(e for e in stops if e["Member"] == start["Member"])
        require(start["PreviousInstance"] == stop["Instance"] and start["Volume"] == stop["Volume"], "P03 same cohort directories")
    require(len(cohort["ReopenRetries"]) == 6, "P03 complete duplicate original set")
    for retry, step in zip(cohort["ReopenRetries"], trace["steps"]):
        require(retry["Operation"] == step["Operation"], "P03 duplicate original order")
        FAULT.replay(trace, retry, 6, operations[step["Operation"]], inputs[step["Operation"]], context)
    expected = {(trace["steps"][i - 1]["Operation"], "while-coordinator-down") for i in (3, 4)} | {(trace["steps"][i - 1]["Operation"], "after-cohort-reopen") for i in range(1, 6)}
    require(len(trace["oldReads"]) == 7 and {(o["Operation"], o["Checkpoint"]) for o in trace["oldReads"]} == expected, "P03 complete old-cut inventory")
    for old in trace["oldReads"]:
        count = next(i + 1 for i, s in enumerate(trace["steps"]) if s["Operation"] == old["Operation"])
        step = trace["steps"][count - 1]
        require(old["Cut"] == step["Cut"] and old["View"] == step["After"], "P03 current substituted for old cut")
        require(killed["EventID"] < old["EventID"] < rejoined["EventID"] if old["Checkpoint"] == "while-coordinator-down" else old["EventID"] > max(e["EventID"] for e in starts), "P03 old-read checkpoint order")
        FAULT.observation(trace, old, count, context)
        observations.append({"event": old["Checkpoint"] + old["Operation"], "scope": ["A", "B"], "round": cut_round(old["Cut"]), "values": projected(old["View"])})
    rounds = {name: op["Round"] for name, op in operations.items()}
    witness, detail = search({"initial": initial, "transactions": transactions}, set(transactions), rounds, observations, [{"event": "final", "scope": ["A", "B"], "values": previous}])
    require(witness is not None, "P03 independent serial/cut witness: " + str(detail))
    return {"coordinator": coordinator, "serial_witness": witness, "operations": 6, "componentChanges": 7, "oldReads": 7, "fullFourFaultAccepted": False, "scope": "P03 native observed-prefix serial/cut bridge; authentic extraction, decision and CDC codec binding stay Go-qualified"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+", type=Path)
    reports = []
    for path in parser.parse_args().artifacts:
        trace, sha = FAULT.load(path)
        report = check(trace)
        report.update(path=str(path), sha256=sha)
        reports.append(report)
    print(json.dumps(reports, indent=2))


if __name__ == "__main__":
    main()
