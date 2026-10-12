#!/usr/bin/env python3
"""Compare two actual bounded lifecycle traces, never literal expected answers."""
import argparse
import base64
import hashlib
import json
from pathlib import Path


class Refusal(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise Refusal(message)


def object_pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def parse(raw):
    return json.loads(raw, object_pairs_hook=object_pairs)


def load(path):
    raw = Path(path).read_bytes()
    require(0 < len(raw) <= 2 << 20, "trace byte bound")
    return parse(raw), hashlib.sha256(raw).hexdigest()


def integer(value, low=0, high=(1 << 64) - 1):
    require(type(value) is int and low <= value <= high, "integer type/range")
    return value


def blob(value):
    require(type(value) is str, "raw bytes missing")
    return base64.b64decode(value, validate=True)


ARRAYS = {"Entities", "Lives", "Postings", "Labels", "CensusIDs", "PhantomFound",
          "NewEntities", "NewLives", "NewValues", "LogicalCDC"}


def normalize(value, key=""):
    if value is None and key in ARRAYS:
        return []
    if isinstance(value, dict):
        return {k: normalize(v, k) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize(v) for v in value]
    return value


def index(trace):
    require(trace["schemaVersion"] == 1, "schema version")
    require(trace["fixtureID"] == "graph-lifecycle-seven-v1", "fixture contract")
    require(trace["engine"] in ("native-raft", "foundationdb"), "engine")
    names, raw = {}, {}
    for item in trace["identityMapping"]:
        role = item["Role"]
        require(role in ("entity", "life", "value"), "identity role")
        integer(item["Raw"], 1)
        require(item["Logical"] not in names and item["Raw"] not in raw, "identity bijection")
        require(bool(item["Graph"]), "graph identity")
        require(bool(item["Owner"]) == (role == "life"), "identity owner")
        names[item["Logical"]] = item
        raw[item["Raw"]] = item
    require(len(names) == 9 and len({v["Graph"] for v in names.values()}) == 1, "identity inventory")
    for item in names.values():
        if item["Role"] == "life":
            require(item["Owner"] in names and names[item["Owner"]]["Role"] == "entity", "foreign life owner")
    operations, rounds, requests, transactions = {}, {}, set(), set()
    for item in trace["operationMapping"]:
        integer(item["Round"], 1)
        require(item["Logical"] not in operations and item["Round"] not in rounds, "operation/round bijection")
        binding = (item["Coordinator"], item["Request"])
        tx = (item["Coordinator"], item["Transaction"])
        require(binding not in requests and tx not in transactions and bool(item["Request"]) and bool(item["Transaction"]), "request/transaction bijection")
        operations[item["Logical"]] = item
        rounds[item["Round"]] = item["Logical"]
        requests.add(binding)
        transactions.add(tx)
    require(len(operations) == len(trace["steps"]) == 7, "complete operation inventory")
    require(set(operations) == {s["Operation"] for s in trace["steps"]}, "operation records missing")
    return names, raw, operations, rounds


def cell(value, names, operations):
    require(set(value) == {"Present", "Null", "Kind", "Reference", "I64", "RevisionOperation", "Provenance"}, "cell fields")
    require(type(value["Present"]) is bool and type(value["Null"]) is bool, "cell flags")
    integer(value["I64"], -(1 << 63), (1 << 63) - 1)
    integer(value["Provenance"])
    operation = value["RevisionOperation"]
    require(not operation or operation in operations, "unknown revision operation")
    if value["Present"]:
        require(not value["Null"] and value["Kind"] in ("life", "i64"), "unsupported present type")
        role = "life" if value["Kind"] == "life" else "value"
        require(value["Reference"] in names and names[value["Reference"]]["Role"] == role, "typed reference")
    else:
        require(value["Reference"] == "" and not value["Null"], "absent reference")


def view(value, names, operations):
    scope = value["ObservationScope"]
    require(scope in ("graph-complete-in-issued-range", "graph-complete-in-fixture-domain", "edge-only"), "read completeness scope")
    for entity in value["Entities"]:
        require(entity["ID"] in names and names[entity["ID"]]["Role"] == "entity", "unknown entity")
        cell(entity["Presence"], names, operations)
        cell(entity["Property"], names, operations)
        require(not entity["Labels"], "nonempty labels outside comparison contract")
    require(len({e["ID"] for e in value["Entities"]}) == len(value["Entities"]), "duplicate entity")
    for life in value["Lives"]:
        require(life["Life"] in names and names[life["Life"]]["Owner"] == life["Owner"], "life binding")
        for endpoint in ("SourceLife", "TargetLife"):
            require(not life[endpoint] or life[endpoint] in names and names[life[endpoint]]["Role"] == "life", "endpoint life")
    if scope != "edge-only":
        require(value["CensusBounds"] == [1, 32], "census domain")
        require(sorted(value["CensusIDs"]) == sorted(e["ID"] for e in value["Entities"]), "census/entity mismatch")
    else:
        require(len(value["Entities"]) == 1 and value["Entities"][0]["Kind"] == "relationship", "edge subset")
        require(len(value["Lives"]) == 1 and value["Lives"][0]["Owner"] == value["Entities"][0]["ID"], "edge life subset")
    out = dict(value)
    out["ObservationScope"] = "edge-only" if scope == "edge-only" else "complete-fixture"
    return out


def native_raw(raw, canonical, raw_ids, rounds):
    require(raw["graph"] == next(iter(raw_ids.values()))["Graph"], "native graph binding")
    require(raw["observationScope"] == canonical["ObservationScope"], "native observation scope binding")
    found = [e for e in raw["entities"] if e["found"]]
    require(len(found) == len(canonical["Entities"]), "native raw entity inventory")
    by_name = {e["ID"]: e for e in canonical["Entities"]}
    for observed in found:
        require(observed["id"] in raw_ids, "native raw unmapped identity")
        name = raw_ids[observed["id"]]["Logical"]
        require(name in by_name, "native canonical omitted raw entity")
        target = by_name[name]
        for raw_key, canonical_key in (("presence", "Presence"), ("property", "Property")):
            source, result = observed[raw_key], target[canonical_key]
            require(source["present"] == result["Present"] and source["null"] == result["Null"] and source["kind"] == result["Kind"] and source["i64"] == result["I64"] and source["provenance"] == result["Provenance"], "native raw cell mismatch")
            require(rounds.get(source["revision"], "") == result["RevisionOperation"], "native raw revision")
            if source["present"]:
                require(source["handle"] in raw_ids and raw_ids[source["handle"]]["Logical"] == result["Reference"], "native raw reference")
    if raw["observationScope"] != "edge-only":
        census = raw["entityCensus"]
        require(len(census) == 32 and [e["id"] for e in census] == list(range(1, 33)), "native complete census")
        actual = [raw_ids[e["id"]]["Logical"] for e in census if e["found"]]
        require(sorted(actual) == sorted(canonical["CensusIDs"]), "native census membership")


def go_json(value):
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    for source, escaped in (("&", r"\u0026"), ("<", r"\u003c"), (">", r"\u003e"), ("\u2028", r"\u2028"), ("\u2029", r"\u2029")):
        raw = raw.replace(source, escaped)
    return raw.encode()


def graph_contract(command, config):
    kind, node, edge = command["Kind"], command["Node"], command["Edge"]
    effects, reads = {}, {}
    if kind in ("node-create", "node-close"):
        owner = integer(node["Owner"], 0, 1)
        require(node["Node"] == config["Nodes"][owner], "FDB endpoint identity owner")
        integer(node["Life"], 1)
        require(edge == {"ID": 0, "Source": {"Owner": 0, "Node": 0, "Life": 0}, "Target": {"Owner": 0, "Node": 0, "Life": 0}, "Value": 0}, "FDB node command has edge fields")
        key = f"node/{node['Node']:016x}/life/{node['Life']:016x}"
        effects[owner] = [{"Key": key, "Value": 1 if kind == "node-create" else 0, "Delete": kind == "node-close"}]
        reads[owner] = [key]
    else:
        require(kind in ("edge-create", "edge-update", "edge-close"), "FDB unsupported graph operation")
        require(node == {"Owner": 0, "Node": 0, "Life": 0}, "FDB edge command has node fields")
        source, target = edge["Source"], edge["Target"]
        require(source["Owner"] == 0 and target["Owner"] == 1 and source["Node"] == config["Nodes"][0] and target["Node"] == config["Nodes"][1], "FDB edge endpoint ownership")
        for value in (edge["ID"], source["Life"], target["Life"]):
            integer(value, 1)
        suffix = f"{edge['ID']:016x}/{source['Node']:016x}/{source['Life']:016x}/{target['Node']:016x}/{target['Life']:016x}"
        keys = {"edge": "edge/" + suffix, "out": "out/" + suffix, "in": "in/" + suffix}
        deleted = kind == "edge-close"
        require(not deleted or edge["Value"] == 0, "FDB close payload")
        effects[0] = [{"Key": keys[k], "Value": edge["Value"], "Delete": deleted} for k in ("edge", "out")]
        effects[1] = [{"Key": keys["in"], "Value": edge["Value"], "Delete": deleted}]
        reads = {0: [keys["edge"]], 1: []}
        if kind != "edge-close":
            reads[0].append(f"node/{source['Node']:016x}/life/{source['Life']:016x}")
            reads[1].append(f"node/{target['Node']:016x}/life/{target['Life']:016x}")
        if kind != "edge-create":
            reads[0].append(keys["out"])
            reads[1].append(keys["in"])
        reads = {owner: sorted(keys) for owner, keys in reads.items()}
    return effects, reads


def fdb_raw(raw, canonical, operations, names, cut):
    require(raw["ObservationScope"] == canonical["ObservationScope"], "FDB observation scope binding")
    configs = [parse(blob(row["Value"])) for row in raw["Config"]]
    require(len(configs) == 2 and configs[0] == configs[1], "FDB stored profile equality")
    config = configs[0]
    graph = next(iter(names.values()))["Graph"]
    prefix = lambda group: f"g{group}/{graph.encode().hex()}/"
    require(config["Graph"] == graph and cut["Graph"] == graph and cut["Topology"] == config["Topology"] and cut["Epochs"] == config["Epochs"] and blob(cut["Scope"]) == bytes((0, 1)), "FDB graph/config/cut binding")
    require([r["Group"] for r in raw["Config"]] == [0, 1], "FDB config ownership")
    for row in raw["Config"]:
        require(blob(row["Key"]).decode() == prefix(row["Group"]) + "meta/" + "config".encode().hex(), "FDB config key")
    profile = config["Lifecycle"]
    require(profile["Version"] == 1, "FDB comparison profile")
    histories = {}
    for row in raw["History"]:
        require(row["Group"] in (0, 1) and blob(row["Key"]).decode().startswith(prefix(row["Group"]) + "history/"), "FDB history graph/group prefix")
        key = blob(row["Key"]).decode().split("/history/", 1)
        require(len(key) == 2, "FDB history key")
        round_hex, logical_hex = key[1].split("/", 1)
        logical = bytes.fromhex(logical_hex).decode()
        identifier = (row["Group"], int(round_hex, 16), logical)
        require(identifier not in histories, "duplicate history row")
        histories[identifier] = parse(blob(row["Value"]))
    physical, metadata = {}, {}
    sequences, generations = [0, 0], [0, 0]
    for group in raw["Groups"]:
        tx, outcome = group["Tx"], group["Outcome"]
        require(tx["ID"] in operations, "unmapped FDB operation")
        op = operations[tx["ID"]]
        require(tx["Graph"] == graph and tx["Topology"] == config["Topology"], "FDB transaction namespace")
        coordinator = integer(tx["Coordinator"], 0, 1)
        sequences[coordinator] += 1
        require(outcome["Coordinator"] == coordinator and outcome["Sequence"] == sequences[coordinator] and outcome["Round"] <= cut["Round"], "FDB cut watermark")
        expected_effects, expected_reads = graph_contract(tx["GraphChange"], config)
        require([p["Group"] for p in tx["Participants"]] == sorted(expected_effects), "FDB participant owners")
        for participant in tx["Participants"]:
            owner = participant["Group"]
            require(participant["Epoch"] == config["Epochs"][owner] and participant["Generation"] == generations[owner], "FDB predecessor generation/epoch")
            require(participant["Effects"] == expected_effects[owner], "FDB graph physical shape")
            require([r["Key"] for r in participant["Reads"]] == expected_reads[owner], "FDB graph read footprint")
            for read in participant["Reads"]:
                require(read["Version"] == physical.get((owner, read["Key"]), {}).get("Version", 0), "FDB graph read revision")
            generations[owner] += 1
        require(op["Round"] == outcome["Round"] and op["Transaction"] == tx["ID"] and op["Request"] == tx["Request"] and op["Coordinator"] == tx["Coordinator"], "FDB actual operation mapping")
        require(outcome["Commit"] is True and outcome["ID"] == tx["ID"] and outcome["Request"] == tx["Request"], "FDB receipt binding")
        require(bytes(outcome["Digest"]) == hashlib.sha256(go_json(tx)).digest(), "FDB admitted metadata digest")
        metadata[tx["ID"]] = tx["Lifecycle"]
        expected = [(p["Group"], e) for p in tx["Participants"] for e in p["Effects"]]
        require(len(expected) == len(group["Writes"]), "FDB incomplete physical writes")
        for (owner, effect), write in zip(expected, group["Writes"]):
            require(write["Group"] == owner and write["Key"] == effect["Key"], "FDB physical ownership/key")
            row_key = (owner, write["Key"])
            require(write.get("Before") == physical.get(row_key), "FDB physical predecessor")
            after = write["After"]
            require(after["Value"] == effect["Value"] and after["Deleted"] == effect["Delete"] and after["TxID"] == tx["ID"] and after["Round"] == outcome["Round"], "FDB physical effect")
            require(histories.pop((owner, outcome["Round"], write["Key"]), None) == after, "FDB missing/mismatched history revision")
            physical[row_key] = after
        if tx["GraphChange"]["Kind"].startswith("edge-"):
            writes = group["Writes"]
            require(len(writes) == 3 and all(w["After"] == writes[0]["After"] and w.get("Before") == writes[0].get("Before") for w in writes), "FDB edge/out/in revision or tombstone mismatch")
    require(not histories, "FDB extra history rows")
    require(sequences == cut["Groups"], "FDB complete cut groups")
    require(len(raw["Census"]) == 32 and all(type(x) is bool for x in raw["Census"]), "FDB census proof")
    if canonical["ObservationScope"] != "edge-only":
        census = sorted(v["Logical"] for v in names.values() if v["Role"] == "entity" and raw["Census"][v["Raw"] - 1])
        require(census == sorted(canonical["CensusIDs"]), "FDB census membership")
    for entity in canonical["Entities"]:
        for key in ("Presence", "Property"):
            value = entity[key]
            operation = value["RevisionOperation"]
            if operation:
                require(operation in metadata and value["Provenance"] == metadata[operation]["Provenance"], "FDB invented caller provenance")
            if value["Kind"] == "i64" and value["Present"]:
                definition = metadata[operation]["Definition"]
                require(definition["I64"] == value["I64"] and names[value["Reference"]]["Raw"] == definition["ID"] and definition["Kind"] == "i64", "FDB invented value definition/reference")
    return profile["Scope"], physical


def analyze(trace):
    trace = normalize(trace)
    names, raw_ids, operations, rounds = index(trace)
    result = {"identityRoles": sorted((n, i["Role"], i["Owner"]) for n, i in names.items()), "steps": [], "oldReads": []}
    previous = None
    physical = None
    for step in trace["steps"]:
        op = operations[step["Operation"]]
        coordinator = ({3: "A", 8: "B"} if trace["engine"] == "native-raft" else {0: "A", 1: "B"})[op["Coordinator"]]
        require(coordinator == step["Coordinator"], "coordinator mapping")
        before = view(step["Before"], names, operations)
        after = view(step["After"], names, operations)
        if previous is not None:
            require(before == previous, "before/after continuity")
        previous = after
        scope = None
        if trace["engine"] == "native-raft":
            native_raw(step["RawObservation"], step["After"], raw_ids, rounds)
            require(step["RawRecords"] and all(blob(r["Outcome"]) for r in step["RawRecords"]), "native raw records")
            records = [(r["Partition"], integer(r["Index"], 1)) for r in step["RawRecords"]]
            require(all(p in (3, 8) for p, _ in records) and len(records) == len(set(records)), "native record membership/index uniqueness")
        else:
            scope, physical = fdb_raw(step["RawObservation"], step["After"], operations, names, step["Cut"])
            require(len(step["RawRecords"]) == 3, "FDB original receipt/group records")
            group = step["RawObservation"]["Groups"][-1]
            require(group["Tx"]["ID"] == step["Operation"], "FDB step original group")
            outcome = group["Outcome"]
            prefix = f"g{outcome['Coordinator']}/{next(iter(names.values()))['Graph'].encode().hex()}/"
            expected_keys = [prefix + f"changes/{outcome['Sequence']:016x}/" + outcome["Request"].encode().hex(), prefix + "request/" + outcome["Request"].encode().hex(), prefix + "id/" + outcome["ID"].encode().hex()]
            for row, key, expected_value in zip(step["RawRecords"], expected_keys, (group, outcome, outcome)):
                require(row["Group"] == outcome["Coordinator"] and blob(row["Key"]).decode() == key and blob(row["Value"]) == go_json(expected_value), "FDB retained original bytes/key/value binding")
        changes = []
        for change in step["LogicalCDC"]:
            cell(change["Before"], names, operations)
            cell(change["After"], names, operations)
            require(set(change["AllScope"]) == {"kind", "axisProfile", "axisVersion", "canonicalUnit", "reference"} and change["AllScope"]["kind"] == "all", "canonical AllScope missing")
            if scope is not None:
                require(change["AllScope"] == scope, "FDB scope not backed by config")
            integer(change["AllScope"]["axisVersion"], 1, 1)
            require(change["AllScope"]["axisProfile"] == "Q", "unsupported scope profile")
            item = dict(change)
            del item["Scope"]  # Preserve raw encoding in inputs; compare decoded AllScope.
            changes.append(item)
        result["steps"].append({"Operation": step["Operation"], "Coordinator": coordinator, "Before": before, "After": after, "LogicalCDC": changes, "NewEntities": step["NewEntities"], "NewLives": step["NewLives"], "NewValues": step["NewValues"]})
    expected_old = {(trace["steps"][2]["Operation"], "held-old"), *[(trace["steps"][i]["Operation"], "reopened-at") for i in (2, 3, 4)]}
    require(len(trace["oldReads"]) == 4 and {(o["Operation"], o["Mode"]) for o in trace["oldReads"]} == expected_old, "old-read exact operation/mode pairs")
    cuts = {s["Operation"]: s["Cut"] for s in trace["steps"]}
    for old in trace["oldReads"]:
        require(old["Cut"] == cuts[old["Operation"]], "old cut differs from original observed cut")
        v = view(old["View"], names, operations)
        if trace["engine"] == "native-raft":
            native_raw(old["RawObservation"], old["View"], raw_ids, rounds)
        else:
            fdb_raw(old["RawObservation"], old["View"], operations, names, old["Cut"])
        result["oldReads"].append({"Operation": old["Operation"], "Mode": old["Mode"], "View": v})
    require(len(result["oldReads"]) == 4, "old-read inventory")
    if trace["engine"] == "foundationdb":
        current = {}
        for row in trace["finalRawCurrent"]:
            prefix = f"g{row['Group']}/{next(iter(names.values()))['Graph'].encode().hex()}/"
            require(row["Group"] in (0, 1) and blob(row["Key"]).decode().startswith(prefix + "current/"), "FDB current graph/group prefix")
            key = blob(row["Key"]).decode().split("/current/", 1)
            require(len(key) == 2, "FDB current key")
            logical = bytes.fromhex(key[1]).decode()
            identifier = (row["Group"], logical)
            require(identifier not in current, "duplicate current row")
            current[identifier] = parse(blob(row["Value"]))
        require(current == physical, "FDB current state not matched to complete write ledger")
    return result


def compare(native, fdb):
    require(native["engine"] == "native-raft" and fdb["engine"] == "foundationdb", "engine order")
    left, right = analyze(native), analyze(fdb)
    require(left == right, "actual lifecycle answers/history/CDC differ")
    return {"equal": True, "steps": len(left["steps"]), "componentChanges": sum(len(s["LogicalCDC"]) for s in left["steps"]), "oldReads": len(left["oldReads"]), "scope": "fixed local functional comparison; raw issued-range/fixture-domain completeness and encoding differences remain visible in inputs"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("native")
    parser.add_argument("fdb")
    args = parser.parse_args()
    native, native_sha = load(args.native)
    fdb, fdb_sha = load(args.fdb)
    report = compare(native, fdb)
    report.update(nativeSHA256=native_sha, fdbSHA256=fdb_sha)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (Refusal, KeyError, TypeError, ValueError, OSError) as error:
        raise SystemExit(f"comparison refused: {error}")
