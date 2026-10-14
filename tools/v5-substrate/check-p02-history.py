#!/usr/bin/env python3
"""Finite P02 capture bridge to existing codecs, graph checks and serial search.

Go qualifies the actual process/Stage extraction, kernel bases and opaque CDC.
This checks their retained correspondence and observed serial/cut consistency;
it supplies neither a graph reducer nor full P02/network/disk acceptance.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


P06 = module("p02_existing_wire_helpers", ROOT / "check-p06-history.py")
FAULT = module("p02_existing_graph_checks", ROOT / "compare-graph-fault.py")
require, raw, body, Cursor = P06.require, P06.raw, P06.body, P06.Cursor
SCHEDULES = {"delayed-A-prepare", "A-before-B-after", "persisted-fence-before-application"}


def cut(observed):
    return P06.cut({"Cut": observed})


def source_info(capture, scopes, voter):
    q = Cursor(body(raw(capture["Request"]), b"GSQ\1"))
    kind, sequence, nonce = q.number(1), q.number(), q.take(16)
    q.take(48)
    require(all(not q.field() for _ in range(4)) and q.take(17) == bytes(17), "P02 exact source-open input")
    q.end()
    r = Cursor(body(raw(capture["Reply"]), b"GSR\1"))
    scope = body(r.field(156), b"APS\1")
    member, issuer, reply_kind, reply_sequence, reply_nonce, mask = r.number(), r.take(16), r.number(1), r.number(), r.take(16), r.number(2)
    c = Cursor(body(r.field(4096), b"GSI\1"))
    r.end()
    cookie, read_id, info_nonce = c.take(40), c.take(24), c.take(16)
    observation = Cursor(body(c.field(), b"APO\1"))
    observation_kind, source_scope, source_index = observation.number(1), observation.take(120), observation.number()
    observation.take(32 + 8 + 32)
    observation.field()
    observation.end()
    generation, index, image_hash, image = c.number(), c.number(), c.take(32), c.field(172)
    floor = c.number()
    c.take(16)  # Two limits and the finite retained-source lifetime.
    c.end()
    part = capture["Partition"]
    require(kind == reply_kind == 1 and sequence == reply_sequence and nonce == reply_nonce == info_nonce and mask == 0 and member == voter, "P02 actual source correlation")
    require(scope == source_scope == scopes[part] and cookie[:16] == read_id[:16] == issuer and observation_kind == 1 and index <= source_index and len(image) == 172 and hashlib.sha256(image).digest() == image_hash, "P02 actual source scope/root binding")
    return {"floor": floor, "index": index, "source_index": source_index, "generation": generation}


def fence(record):
    c = body(raw(record["Outcome"]), b"GSO\1")
    require(len(c) == 18 and c[:2] == bytes([1, 1]) and int.from_bytes(c[10:18], "big") == record["Index"] and not raw(record["CDC"]), "P02 actual applied fence")
    return int.from_bytes(c[2:10], "big")


def decoded_observation(native, view, raw_ids):
    """Transparent witness correspondence, including an actual absent life.

    The four-fault helper assumes live endpoints; P02's delayed Close does not.
    This adapter compares observations and supplies no graph state transition.
    """
    observed = {raw_ids[e["id"]]["Logical"]: e for e in native["entities"] if e["found"]}
    require(len(observed) == len(view["Entities"]), "P02 native entity inventory")
    for entity in view["Entities"]:
        item = observed[entity["ID"]]
        life = raw_ids[item["effectiveLife"]]["Logical"] if item["effectiveLife"] else ""
        require(item["kind"] == (2 if entity["Kind"] == "relationship" else 1) and item["effective"] == entity["Effective"] and life == entity["EffectiveLife"], "P02 actual declaration/effective/life witness")
        if entity["ID"] == "edge":
            require(item["mode"] == 1 and item["type"] == "edge" and raw_ids[item["source"]]["Logical"] == "node-a" and raw_ids[item["target"]]["Logical"] == "node-b", "P02 actual edge endpoints/mode")
    lives = [{"Owner": raw_ids[l["owner"]]["Logical"], "Life": raw_ids[l["life"]]["Logical"],
              "SourceLife": raw_ids[l["sourceLife"]]["Logical"] if l["sourceLife"] else "",
              "TargetLife": raw_ids[l["targetLife"]]["Logical"] if l["targetLife"] else ""} for l in native["lives"] if l["found"]]
    require(lives == view["Lives"], "P02 actual immutable life inventory")
    postings = [{"Endpoint": raw_ids[p["endpoint"]]["Logical"], "Life": raw_ids[p["life"]]["Logical"] if p["life"] else "",
                 "Declared": [raw_ids[n]["Logical"] for n in p["declared"]],
                 "Effective": [raw_ids[n]["Logical"] for n in p["effective"]]} for p in native["postings"]]
    require(postings == view["Postings"] and len(native["phantomEntities"]) == 2 and all(not p["found"] for p in native["phantomEntities"]), "P02 actual postings/phantom inventory")


def check(data):
    data = FAULT.lc.normalize(data)
    trace, schedule = data["Trace"], data["Schedule"]
    require(schedule in SCHEDULES and trace["engine"] == "native-raft" and len(trace["steps"]) == 3, "P02 finite schedule")
    names = {i["Logical"]: i for i in trace["identityMapping"]}
    raw_ids = {i["Raw"]: i for i in trace["identityMapping"]}
    require(len(names) == len(raw_ids) == len(trace["identityMapping"]) and sorted((n, i["Role"], i["Owner"]) for n, i in names.items()) == sorted(tuple(v) for v in FAULT.SPEC["identityRoles"]), "P02 identity bijection")
    operations = {i["Logical"]: i for i in trace["operationMapping"]}
    rounds = {i["Round"]: i["Logical"] for i in operations.values()}
    require(len(rounds) == len(operations), "P02 actual revision bijection")
    context = names, raw_ids, operations, rounds
    for count, step in enumerate(trace["steps"], 1):
        require(step["Operation"] == ("node-a", "node-b", "edge-create")[count - 1], "P02 actual original order")
        FAULT.observation(trace, {"Cut": step["Cut"], "View": step["After"], "RawObservation": step["RawObservation"]}, count, context)
        FAULT.raw_records(trace, step["RawRecords"], count, context)
        FAULT.logical_group(step, step["Before"], count)
    require(data["Old"] == data["Retained"], "P02 current substituted for retained old cut")
    scopes, old_round = cut(data["Old"]["Cut"])
    FAULT.fixture_view(data["Old"]["View"], 2)
    for observed in (data["Old"], data["Current"], data["Retained"]):
        FAULT.lc.view(observed["View"], names, operations)
        FAULT.lc.native_raw(observed["Native"], observed["View"], raw_ids, rounds)
        decoded_observation(observed["Native"], observed["View"], raw_ids)
        cut(observed["Cut"])
    require(data["Fences"] and len({fence(r) for r in data["Fences"]}) == 1, "P02 genuine fence inventory")
    bound = fence(data["Fences"][0])
    require(bound > old_round, "P02 new fence follows genuine old published cut")
    coordinator = 3 if schedule == "persisted-fence-before-application" else 8
    peers = {3: 2 if coordinator == 3 else 1, 8: 4}
    recovery_peers = {3: 2, 8: 4} if schedule == "delayed-A-prepare" else {3: 1, 8: 5}
    require(6 <= len(data["Phases"]) <= 11 and len(data["SourceCaptures"]) == 2, "P02 genuine phase/source inventory")
    statuses, manifest, outcomes, seen = [], None, [], {}
    for phase in data["Phases"]:
        origin = phase["Origin"]
        require(origin == (3 if coordinator == 3 else 4) or coordinator == 8 and origin == 6, "P02 original/recovery same-coordinator route")
        voters = recovery_peers if origin == 6 else peers
        _, m, status, outcome = P06.peer_phase(raw(phase["Request"]), raw(phase["Reply"]), scopes, voters[phase["Partition"]])
        manifest = m if manifest is None else manifest
        require(m == manifest, "P02 original manifest changed")
        key = status["partition"], status["phase"]
        if key in seen:
            require(seen[key] == outcome, "P02 duplicate retained phase differs")
            continue
        index = len(statuses)
        require(index < 6 and status["phase"] == (1, 2, 2, 3, 4, 4)[index] and status["partition"] == (coordinator, 3, 8, coordinator, 3, 8)[index], "P02 retained original phase order")
        seen[key] = outcome
        statuses.append(status); outcomes.append(outcome)
    require(len(statuses) == 6, "P02 missing retained original phase")
    c = Cursor(body(manifest, b"GCM\1"))
    tx, actual_coordinator, original = c.take(16), c.number(), c.field()
    c.take(128)
    require(c.number(4) == 2, "P02 original two participants")
    floors = {}
    for _ in range(2):
        part, floor = c.number(), c.number()
        require(part in (3, 8) and part not in floors and c.field(), "P02 original kernel inventory")
        floors[part] = floor
    c.end()
    step = trace["steps"][2]
    request = P06.pgq(original)
    require(actual_coordinator == coordinator == request["partition"] and operations["edge-create"]["Coordinator"] == coordinator and step["Coordinator"] == {3: "A", 8: "B"}[coordinator], "P02 true coordinator mapping")
    require(P06.business_hash(raw(step["OriginalRequest"]["Payload"]), coordinator) == P06.business_hash(original) and all(s["tx"] == tx for s in statuses), "P02 exact original business/identity")
    require(all(s["digest"] == hashlib.sha256(manifest).digest() for s in statuses) and statuses[1]["yes"] and statuses[2]["yes"] and all(s["commit"] for s in statuses[3:]), "P02 immutable effect digest/YES/COMMIT")
    decision = statuses[3]
    require(decision["round"] > statuses[1]["floor"] and decision["round"] > statuses[2]["floor"] and decision["round"] == operations["edge-create"]["Round"] and all(s["round"] == decision["round"] for s in statuses[4:]), "P02 COMMIT strictly above actual voted floors")
    if coordinator == 8:
        P06.resumed(data["PendingCaller"], scopes, request["request"], tx, P06.business_hash(original), 1, b"")
    else:
        require(not raw(data["PendingCaller"]), "P02 Stage case unexpected pending caller")
    require(data["Encounters"] and all(e["Origin"] in (3, 4, 6) for e in data["Encounters"]), "P02 retained phase-attempt frames")
    sources = {s["Partition"]: source_info(s, scopes, peers[s["Partition"]]) for s in data["SourceCaptures"]}
    require(set(sources) == {3, 8} and all(sources[p]["floor"] == floors[p] for p in sources), "P02 genuine captured installed floors")
    if schedule == "A-before-B-after":
        require(statuses[1]["floor"] < bound <= statuses[2]["floor"], "P02 actual A-before/B-after votes")
    else:
        require(statuses[1]["floor"] >= bound, "P02 post-fence YES sampled stale floor")
    if coordinator == 3:
        marker = data["Stage"]
        require(marker["HardCommit"] >= marker["Index"] > marker["Checkpoint"] and marker["Member"] == 1 and sources[3]["index"] >= marker["Index"] and sources[3]["source_index"] >= marker["Index"], "P02 persistence/application/capture ordering")
        require(raw(marker["Command"]) == raw(data["Fences"][0]["Proposal"]) and marker["SHA256"] == hashlib.sha256(raw(marker["Command"])).hexdigest() and raw(marker["RetainedEntry"]), "P02 exact pre-application fence capture")
        killed, rejoined = data["Processes"]
        for event in (killed, rejoined):
            FAULT.process_event(event, "native-raft")
        require(killed["Member"] == rejoined["Member"] == 1 and killed["Signal"] == "SIGKILL" and rejoined["PreviousInstance"] == killed["Instance"] and rejoined["Volume"] == killed["Volume"], "P02 actual leader loss/rejoin")
    else:
        require(floors[3] < bound and data["Stage"] is None and data["DelayedLocal"] is None, "P02 genuine pre-fence planning")
    initial = P06.projected(data["Old"])
    recovered = P06.projected({"Native": step["RawObservation"]})
    transactions = {"edge-create": {"reads": [P06.read_key(initial, k) for k in ("A/presence/1", "B/presence/17", "A/entity/3")], "writes": P06.writes(initial, recovered, ("A/entity/3", "A/presence/3", "A/labels/3", "A/property/3/p", "A/life/3/4", "A/posting/1/0", "A/posting/1/2", "B/posting/17/0", "B/posting/17/18"))}}
    tx_rounds = {"edge-create": decision["round"]}
    current = P06.projected(data["Current"])
    if coordinator == 3:
        local = data["DelayedLocal"]
        outcome = body(raw(local["Outcome"]), b"GRO\4")
        proposal = P06.pgq(raw(local["Proposal"]))
        require(len(outcome) == 84 and outcome[25:41] == proposal["request"] and outcome[41:73] == hashlib.sha256(raw(local["Proposal"])).digest() and int.from_bytes(outcome[73:81], "big") == local["Index"] and outcome[82:84] == bytes(2) and raw(local["CDC"]), "P02 genuine original delayed local result")
        duplicate = data["DelayedLocalReplay"]
        replay = body(raw(duplicate["Outcome"]), b"GRO\4")
        require(raw(duplicate["Proposal"]) == raw(local["Proposal"]) and not raw(duplicate["CDC"]) and duplicate["Index"] > local["Index"] and replay[:81] == outcome[:81] and replay[81] == 2 and outcome[81] == 1 and replay[82:] == outcome[82:], "P02 local duplicate retained original identity/index and replay disposition")
        local_round = data["Current"]["Native"]["entities"][0]["presence"]["revision"]
        require(local_round > bound and local_round > decision["round"], "P02 delayed local committed below fence")
        transactions["node-close"] = {"reads": [P06.read_key(recovered, "A/presence/1")], "writes": P06.writes(recovered, current, ("A/presence/1",))}
        tx_rounds["node-close"] = local_round
    else:
        require(current == recovered, "P02 unexpected extra business install")
    records = {(r["Partition"], r["Index"]): r for r in data["Records"]}
    require(len(records) == len(data["Records"]), "P02 duplicate captured record")
    for status, wire in zip(statuses, outcomes):
        actual = raw(records[(status["partition"], status["index"])]["Outcome"])
        if status["phase"] == 1:
            # The native crossRegister query projects the original R record;
            # it deliberately omits the application record's installed floor.
            retained = P06.status(actual)
            expected = dict(status, floor=floors[coordinator])
            require(retained == expected and status["floor"] == status["round"] == 0 and not status["yes"] and not status["commit"] and body(actual, b"GCS\1")[-1] == body(wire, b"GCS\1")[-1] == 0, "P02 exact original Register projection/installed floor")
        else:
            require(actual == wire, "P02 phase not exact retained original")
    for retry in data["Retries"]:
        require(raw(retry["Payload"]) == raw(step["OriginalRequest"]["Payload"]), "P02 duplicate original input changed")
        FAULT.coordinates(retry["Result"], operations["edge-create"])
        require(retry["Before"]["View"] == retry["After"]["View"] and raw(retry["RawResult"]), "P02 duplicate changed complete graph")
        FAULT.new_records(trace, retry.get("NewRawRecords") or [])
    observations = [{"event": "old", "scope": ["A", "B"], "round": old_round, "values": initial}, {"event": "edge", "scope": ["A", "B"], "round": cut(step["Cut"])[1], "values": recovered}]
    observations += [{"event": key, "scope": ["A", "B"], "round": cut(data[key]["Cut"])[1], "values": P06.projected(data[key])} for key in ("Current", "Retained")]
    witness, detail = P06.oracle()({"initial": initial, "transactions": transactions}, set(transactions), tx_rounds, observations, [{"event": "final", "scope": ["A", "B"], "values": current}])
    require(witness is not None, "P02 independent serial/cut witness: " + str(detail))
    return {"schedule": schedule, "serial_witness": witness, "votedFloors": [s["floor"] for s in statuses[1:3]], "capturedInstalledFloors": floors, "fence": bound, "commitRound": decision["round"], "phaseFrames": len(data["Phases"]), "attemptFrames": len(data["Encounters"]), "fullP02Accepted": False, "scope": "Finite native capture correspondence and observed serial/cut consistency; authentic Stage, application/kernel/CDC codec and complete export remain Go-qualified"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+", type=Path)
    reports = []
    for path in parser.parse_args().artifacts:
        data, sha = FAULT.load(path)
        report = check(data)
        report.update(path=str(path), sha256=sha)
        reports.append(report)
    print(json.dumps(reports, indent=2))


if __name__ == "__main__":
    main()
