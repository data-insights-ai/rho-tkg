#!/usr/bin/env python3
"""Stream the fixed writer1 cost CDC contract; opaque native codecs stay qualified."""
import base64
import hashlib
import json
from pathlib import Path
import struct

from comparison_spool import Policy, Spool, u64

SCOPE = {"kind": "all", "axisProfile": "Q", "axisVersion": 1,
         "canonicalUnit": "millisecond", "reference": "posix-unix-epoch:1970-01-01T00:00:00Z:v1"}
GRAPH = "01000000000000000000000000000000"
PARTITIONS = {3: 0, 8: 1}
LINE_BYTES = 64 << 10
BATCH_OPERATIONS = 16
RAW_FORMAT = "fdb-graph-cost-raw-v2"
ARTIFACT_LINE_BYTES = 512 << 10
# Explicit verifier policy for this first-cell integration, not an approved
# million-fact capacity cap. Cache is a SQLite target, never a process RSS cap.
SMALL_POLICY = Policy(1 << 30, 8 << 20, 4096, ARTIFACT_LINE_BYTES)


class StoredState:
    """Sparse decoded predecessors stored as raw BLOBs, one lookup at a time."""
    def __init__(self, spool, namespace):
        self.spool, self.namespace = spool, namespace

    def key(self, key):
        require(all(type(v) in (int, str) for v in key), "sparse state coordinate/name type")
        return b"".join(u64(v) if type(v) is int else v.encode() for v in key)

    def get(self, key, default=None):
        raw = self.spool.find(self.namespace, self.key(key))
        return default if raw is None else json.loads(raw, object_pairs_hook=pairs)

    def __setitem__(self, key, value):
        key, body = self.key(key), go_json(value)
        prior = self.spool.find(self.namespace, key)
        if prior is None:
            self.spool.insert(self.namespace, key, body)
        else:
            self.spool.replace(self.namespace, key, prior, body)


def require(condition, message):
    if not condition:
        raise ValueError("cost CDC refused: " + message)


def fields(value, names):
    require(type(value) is dict and set(value) == set(names.split()), "fields: " + names)


def integer(value, low=0, high=(1 << 64) - 1):
    require(type(value) is int and low <= value <= high, "integer type/range")
    return value


def same(left, right):
    """JSON equality also preserves bool/int/float and every nested field."""
    if type(left) is not type(right):
        return False
    if type(left) is dict:
        return left.keys() == right.keys() and all(same(left[k], right[k]) for k in left)
    if type(left) in (list, tuple):
        return len(left) == len(right) and all(same(a, b) for a, b in zip(left, right))
    return left == right


def pairs(items):
    out = {}
    for key, value in items:
        require(key not in out, "duplicate JSON key")
        out[key] = value
    return out


def rows(path, maximum):
    with Path(path).open("rb") as source:
        number = 0
        while line := source.readline(LINE_BYTES + 1):
            number += 1
            require(number <= maximum and len(line) <= LINE_BYTES and line.endswith(b"\n"), "JSONL row/count bound")
            yield json.loads(line, object_pairs_hook=pairs, parse_constant=lambda _: require(False, "nonfinite JSON"))


def raw_bytes(value):
    require(type(value) is str, "raw original missing")
    raw = base64.b64decode(value, validate=True)
    require(base64.b64encode(raw).decode() == value, "noncanonical base64")
    return raw


def fixed_body(raw, magic, length):
    require(len(raw) == length and raw[:4] == magic, "original outcome kind/length")
    require(hashlib.sha256(raw[:-32]).digest() == raw[-32:], "original outcome seal")
    return raw[4:-32]


def go_json(value):
    text = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    for source, escaped in (("&", r"\u0026"), ("<", r"\u003c"), (">", r"\u003e"), ("\u2028", r"\u2028"), ("\u2029", r"\u2029")):
        text = text.replace(source, escaped)
    return text.encode()


def fixture(spec, ordinal):
    """Only identity/operation correspondence; observed effects come from streams."""
    count = spec["facts"]
    nodes = [{"Owner": 0, "Node": 1, "Life": 2}, {"Owner": 0, "Node": 3, "Life": 4},
             {"Owner": 1, "Node": 7 + 2 * count, "Life": 8 + 2 * count}]
    if ordinal <= 3:
        return "setup", ordinal - 1, nodes[ordinal - 1], None
    phase = "create" if ordinal <= 3 + count else "update"
    index = ordinal - 4 - (count if phase == "update" else 0)
    cross = (index + 1) * spec["crossPercent"] // 100 > index * spec["crossPercent"] // 100
    edge = {"ID": 7 + 2 * index, "Source": nodes[0], "Target": nodes[2 if cross else 1],
            "Value": 5 if phase == "create" else 6}
    return phase, index, None, edge


def operation(spec, ordinal):
    phase, index, node, edge = fixture(spec, ordinal)
    return "cost-" + phase + "-" + str(index), node["Owner"] if node else 0


def absent():
    return {"present": False, "null": False, "handle": 0, "revision": 0,
            "provenance": 0, "kind": "absent", "i64": 0}


def canonical_cell(cell):
    fields(cell, "present null handle revision provenance kind i64")
    require(type(cell["present"]) is bool and type(cell["null"]) is bool, "cell flags")
    for name in ("handle", "revision", "provenance"):
        integer(cell[name])
    integer(cell["i64"], -(1 << 63), (1 << 63) - 1)
    require(cell == absent() or cell["present"] and not cell["null"] and cell["kind"] in ("life", "i64")
            and cell["handle"] > 0 and cell["revision"] > 0 and cell["provenance"] > 0, "cell state")
    # Numeric revision is checked against the original and predecessor locally.
    return {k: v for k, v in cell.items() if k != "revision"}


def native_original(row):
    fields(row, "Partition Index Outcome CDC Changes Entities Lives Values Original CDCClass RawHashes Logical SourceBoundary")
    original = row["Original"]
    fields(original, "Kind Graph Request Transaction Digest Partition Index Coordinator Provenance Round Phase Commit Reason Disposition Participants")
    integer(row["Partition"], 1)
    require(original["Partition"] == row["Partition"] in PARTITIONS and original["Index"] == integer(row["Index"], 1), "native record coordinate")
    require(original["Graph"] == GRAPH and type(original["Commit"]) is bool, "native namespace/commit")
    for name in ("Partition", "Index", "Coordinator", "Provenance", "Round", "Phase", "Reason", "Disposition"):
        integer(original[name])
    boundary = row["SourceBoundary"]
    fields(boundary, "Index Generation ImageSHA256")
    require(boundary["Index"] == integer(row["Index"], 1) and type(boundary["Index"]) is int, "retained source boundary index")
    integer(boundary["Generation"], 1)
    require(type(boundary["ImageSHA256"]) is str and len(boundary["ImageSHA256"]) == 64 and bytes.fromhex(boundary["ImageSHA256"]).hex() == boundary["ImageSHA256"], "retained source image hash")
    outcome, cdc = raw_bytes(row["Outcome"]), raw_bytes(row["CDC"])
    fields(row["RawHashes"], "OutcomeSHA256 CDCSHA256")
    require(row["RawHashes"] == {"OutcomeSHA256": hashlib.sha256(outcome).hexdigest(), "CDCSHA256": hashlib.sha256(cdc).hexdigest()}, "native raw hash binding")
    logical = row["Logical"]
    fields(logical, "Entities Lives Values Components Schemas")
    for name in logical:
        integer(logical[name], 0, 2)
    require(logical["Schemas"] == 0, "unexpected native schema declaration")
    for name in ("Changes", "Entities", "Lives", "Values"):
        require(row[name] is None or type(row[name]) is list, "native array")
        key = "Components" if name == "Changes" else name
        require(logical[key] == len(row[name] or []), "native decoded inventory")
    kind, classification = original["Kind"], row["CDCClass"]
    if kind in ("read-control", "no-application-output"):
        require(classification == "read-control" and not cdc and not any(logical.values()), "unknown empty control")
        require(all(original[k] == "" for k in ("Request", "Transaction", "Digest")) and original["Coordinator"] == original["Provenance"] == original["Reason"] == 0 and original["Participants"] == [], "control union fields")
        if kind == "no-application-output":
            require(not outcome and original["Round"] == original["Phase"] == original["Disposition"] == 0 and not original["Commit"], "retained empty-output classification")
        else:
            body = fixed_body(outcome, b"GSO\1", 54)
            phase, status, round_, index = struct.unpack(">BBQQ", body)
            require(phase in (1, 2) and status == 1 and original["Commit"] and
                    (phase, status, round_, index) == (original["Phase"], original["Disposition"], original["Round"], row["Index"]), "read-control original")
        return original
    require(kind in ("local-graph", "cross") and original["Coordinator"] in PARTITIONS and original["Reason"] == 0, "unknown business original")
    for name, size in (("Request", 16), ("Transaction", 16), ("Digest", 32)):
        text = original[name]
        require(type(text) is str and len(text) == 2 * size and text == bytes.fromhex(text).hex() and int(text, 16) > 0, "original identity/digest")
    namespace = bytes.fromhex(GRAPH) + struct.pack(">Q", row["Partition"])
    if kind == "local-graph":
        body = fixed_body(outcome, b"GRO\4", 120)
        require(body == b"\6" + namespace + bytes.fromhex(original["Request"]) + bytes.fromhex(original["Digest"]) + struct.pack(">QBH", row["Index"], 1, 0), "local original fields")
        require(original["Transaction"] == original["Request"] and original["Participants"] == [row["Partition"]]
                and original["Coordinator"] == row["Partition"] and original["Commit"] and original["Disposition"] == 1 and original["Phase"] == 0 and original["Round"] > 0, "local original authority")
        require(classification == "logical" and cdc and logical["Components"] > 0, "local logical classification")
    else:
        body = fixed_body(outcome, b"GCS\1", 135)
        require(body[:72] == namespace + bytes.fromhex(original["Transaction"]) + bytes.fromhex(original["Digest"]), "cross original identity")
        phase, index, floor, round_, flags, reason = struct.unpack(">BQQQBB", body[72:])
        require(phase == original["Phase"] in (1, 2, 3, 4) and index == row["Index"] and round_ == original["Round"] and reason == original["Reason"] and flags <= 3 and bool(flags & 2) == original["Commit"] and original["Disposition"] == 0, "cross original fields")
        coordinator = bytes.fromhex(GRAPH) + struct.pack(">Q", original["Coordinator"])
        derived = hashlib.sha256(b"rho-tkg:cross-transaction-id:v1\0" + coordinator + bytes.fromhex(original["Request"])).digest()[:16].hex()
        require(original["Transaction"] == derived and same(original["Participants"], [3, 8]), "cross request/participants")
        if phase == 4:
            require(original["Commit"] and round_ > 0 and cdc and classification in ("logical", "participant-empty"), "cross resolve classification")
            require((classification == "logical") == bool(sum(logical.values())), "cross logical classification")
        else:
            require(not cdc and not any(logical.values()) and classification == "protocol-empty", "cross protocol classification")
            require(original["Commit"] == (phase == 3) and (round_ > 0) == (phase == 3), "cross decision phase")
    return original


def native_group(records, spec, ordinal, previous, definitions):
    name, owner = operation(spec, ordinal)
    phase, index, node, edge = fixture(spec, ordinal)
    expected_parts = [3, 8] if edge and edge["Target"]["Owner"] else [8 if owner else 3]
    first = records[0]["Original"]
    require(first["Participants"] == expected_parts and first["Coordinator"] == (8 if owner else 3), "native declared owner correspondence")
    inventory = [(r["Partition"], r["Original"]["Phase"]) for r in records]
    expected = [(p, s) for p in expected_parts for s in ((1, 2, 3, 4) if p == first["Coordinator"] else (2, 4))] if first["Kind"] == "cross" else [(expected_parts[0], 0)]
    require(sorted(inventory) == sorted(expected), "native complete original phase inventory")
    for row in records:
        original = row["Original"]
        require(all(original[k] == first[k] for k in ("Kind", "Coordinator", "Request", "Transaction", "Digest", "Provenance", "Participants")), "native original group binding")
    business = [r for r in records if r["CDCClass"] == "logical"]
    require(len(business) == 1 and business[0]["Partition"] == first["Coordinator"], "native logical group ownership")
    row = business[0]
    require(all(r["Original"]["Round"] in (0, row["Original"]["Round"]) for r in records), "native original decision/resolve revision")
    changes = []
    for change in row["Changes"] or []:
        fields(change, "Owner Life Component Name AllScope Scope Before After")
        require(same(change["AllScope"], SCOPE) and raw_bytes(change["Scope"]), "native temporal axis/scope")
        key = tuple(change[k] for k in ("Owner", "Life", "Component", "Name"))
        require(change["Before"] == previous.get(key, absent()), "native physical predecessor")
        require(change["After"]["revision"] == row["Original"]["Round"] and change["After"]["provenance"] == ordinal, "native original revision/provenance")
        before, after = canonical_cell(change["Before"]), canonical_cell(change["After"])
        changes.append({"key": key, "scope": change["AllScope"], "before": before, "after": after})
        previous[key] = change["After"]
        previous.spool.insert(b"native-cell-history", previous.key(key) + u64(ordinal), go_json(change["After"]))
    declarations = native_declarations(row, definitions)
    return {"operation": name, "coordinator": owner, "effects": changes, **declarations}


def native_declarations(row, definitions):
    entities, lives, values = [], [], []
    for entity in row["Entities"] or []:
        fields(entity, "id found kind source target mode type presence effective effectiveLife labels property")
        require(entity["found"] is True and entity["effective"] is False and entity["effectiveLife"] == 0 and not entity["labels"] and entity["presence"] == absent() | {"kind": ""} and entity["property"] == absent() | {"kind": ""}, "native declaration union")
        entities.append({k: entity[k] for k in ("id", "kind", "source", "target", "mode", "type")})
    for life in row["Lives"] or []:
        fields(life, "owner life found sourceLife targetLife")
        require(life["found"] is True, "native life declaration")
        lives.append({k: life[k] for k in ("owner", "life", "sourceLife", "targetLife")})
    for value in row["Values"] or []:
        fields(value, "ID Kind I64")
        require(value["ID"] not in definitions and value == {"ID": value["ID"], "Kind": "i64", "I64": value["ID"]} and value["ID"] in (5, 6), "native value declaration")
        definitions.add(value["ID"])
        values.append(value)
    return {"entities": entities, "lives": lives, "values": values}


def native_prefix(bounds):
    require(set(bounds) in ({"from", "through", "finalCut"}, {"from", "through", "finalCut", "oldCut"}), "native bound fields")
    require(type(bounds["from"]) is list and type(bounds["through"]) is list and len(bounds["from"]) == len(bounds["through"]) == 2, "native retained prefix bounds")
    for start, end in zip(bounds["from"], bounds["through"]):
        require(integer(start) <= integer(end), "native retained prefix order")
    fields(bounds["finalCut"], "Certificates")
    require(type(bounds["finalCut"]["Certificates"]) is list and len(bounds["finalCut"]["Certificates"]) == 2, "native complete certified boundary")
    images = {}
    for encoded, partition, end in zip(bounds["finalCut"]["Certificates"], PARTITIONS, bounds["through"]):
        body = fixed_body(raw_bytes(encoded), b"GSC\1", 392)
        require(body[:24] == bytes.fromhex(GRAPH) + struct.pack(">Q", partition), "native final boundary namespace")
        round_, index, source_round, source_index = struct.unpack(">QQQQ", body[-204:-172])
        require(0 < source_index == end < index and 0 < source_round <= round_, "native final certified prefix")
        images[partition] = hashlib.sha256(body[-172:]).hexdigest()
    return images


def native_old_prefix(bounds):
    fields(bounds["oldCut"], "Certificates")
    require(len(bounds["oldCut"]["Certificates"]) == 2, "native old full-scope cut")
    old = {}
    for n, partition in enumerate(PARTITIONS):
        body = fixed_body(raw_bytes(bounds["oldCut"]["Certificates"][n]), b"GSC\1", 392)
        final = fixed_body(raw_bytes(bounds["finalCut"]["Certificates"][n]), b"GSC\1", 392)
        require(body[:152] == final[:152], "native old/final immutable cut binding")
        round_, index, source_round, source_index = struct.unpack(">QQQQ", body[-204:-172])
        require(0 < source_round <= round_ and bounds["from"][n] < source_index < index and source_index <= bounds["through"][n], "native old certified prefix")
        old[partition] = source_index, hashlib.sha256(body[-172:]).hexdigest()
    return old


def native_groups(path, spec, bounds, stats, spool):
    pending, last_provenance = {}, {}
    certified_images = native_prefix(bounds)
    old = native_old_prefix(bounds) if "oldCut" in bounds else {}
    old_seen = set()
    boundaries = {}
    last_indexes = dict(zip(PARTITIONS, bounds["from"]))
    previous, definitions = StoredState(spool, b"native-before"), set()
    bindings = hashlib.sha256()
    next_ordinal, total = 1, 3 + 2 * spec["facts"]
    for row in rows(path, 128 * total + 16):
        original = native_original(row)
        partition, index = row["Partition"], row["Index"]
        require(index == last_indexes[partition] + 1 and index <= bounds["through"][PARTITIONS[partition]], "native missing/duplicate/reordered index")
        last_indexes[partition] = index
        stats["records"] += 1
        boundary = row["SourceBoundary"]
        if original["Kind"] == "no-application-output" and partition in boundaries:
            require(boundary["Generation"] == boundaries[partition]["Generation"] and boundary["ImageSHA256"] == boundaries[partition]["ImageSHA256"], "empty output changed retained root")
        boundaries[partition] = boundary
        if partition in old and index == old[partition][0]:
            require(boundary["ImageSHA256"] == old[partition][1], "native old retained root image")
            old_seen.add(partition)
        if original["Kind"] in ("read-control", "no-application-output"):
            stats["controls"] += 1
            continue
        ordinal = integer(original["Provenance"], next_ordinal, total)
        require(ordinal >= last_provenance.get(partition, 0), "native reordered operation")
        last_provenance[partition] = ordinal
        pending.setdefault(ordinal, []).append(row)
        require(len(pending) <= BATCH_OPERATIONS and len(pending[ordinal]) <= 6, "native finite batch join")
        while next_ordinal in pending:
            group = pending[next_ordinal]
            expected = 6 if group[0]["Original"]["Kind"] == "cross" else 1
            if len(group) != expected:
                break
            canonical = native_group(group, spec, next_ordinal, previous, definitions)
            original = group[0]["Original"]
            request = u64(original["Coordinator"]) + bytes.fromhex(original["Request"])
            transaction = u64(original["Coordinator"]) + bytes.fromhex(original["Transaction"])
            require(spool.find(b"native-request", request) is None and spool.find(b"native-transaction", transaction) is None, "native original request/transaction bijection")
            spool.insert(b"native-request", request, go_json(group))
            spool.insert(b"native-transaction", transaction, bytes.fromhex(original["Digest"]))
            binding = {k: original[k] for k in ("Graph", "Coordinator", "Request", "Transaction", "Digest", "Provenance")}
            bindings.update(json.dumps(binding, sort_keys=True, separators=(",", ":")).encode() + b"\n")
            del pending[next_ordinal]
            next_ordinal += 1
            yield canonical
    require(not pending and next_ordinal == total + 1, "native incomplete logical groups")
    require(list(last_indexes.values()) == bounds["through"], "native incomplete retained prefix")
    require(all(boundaries.get(p, {}).get("ImageSHA256") == expected for p, expected in certified_images.items()), "native final retained root image")
    require(old_seen == set(old), "native missing old certified prefix boundary")
    stats["originalBindingsSHA256"] = bindings.hexdigest()


def fdb_contract(spec, ordinal):
    phase, index, node, edge = fixture(spec, ordinal)
    zero_node = {"Owner": 0, "Node": 0, "Life": 0}
    zero_edge = {"ID": 0, "Source": zero_node, "Target": zero_node, "Value": 0}
    command = {"Kind": "node-create" if node else "edge-" + phase, "Node": node or zero_node, "Edge": edge or zero_edge}
    effects, reads = {}, {}
    if node:
        key = f"node/{node['Node']:016x}/life/{node['Life']:016x}"
        effects[node["Owner"]] = [{"Key": key, "Value": 1, "Delete": False}]
        reads[node["Owner"]] = [key]
    else:
        suffix = "/".join(f"{v:016x}" for v in (edge["ID"], edge["Source"]["Node"], edge["Source"]["Life"], edge["Target"]["Node"], edge["Target"]["Life"]))
        for kind in ("edge", "out", "in"):
            owner = edge["Target"]["Owner"] if kind == "in" else 0
            effects.setdefault(owner, []).append({"Key": kind + "/" + suffix, "Value": edge["Value"], "Delete": False})
        reads[0] = ["edge/" + suffix, "node/0000000000000001/life/0000000000000002"]
        owner = edge["Target"]["Owner"]
        reads.setdefault(owner, []).append(f"node/{edge['Target']['Node']:016x}/life/{edge['Target']['Life']:016x}")
        if phase == "update":
            reads[0].append("out/" + suffix)
            reads[owner].append("in/" + suffix)
        for owner in effects:
            effects[owner].sort(key=lambda e: e["Key"])
            reads[owner].sort()
    metadata = {"Provenance": ordinal}
    if edge:
        metadata.update(EdgeLife=edge["ID"] + 1, Definition={"ID": edge["Value"], "Kind": "i64", "I64": edge["Value"]})
    return command, metadata, effects, reads


def fdb_group(group, spec, ordinal, config, physical, generations, sequences, definitions):
    fields(group, "Tx Outcome Writes")
    tx, outcome = group["Tx"], group["Outcome"]
    fields(tx, "Graph Topology ID Request Coordinator Dependency Participants GraphChange Lifecycle")
    fields(outcome, "Commit Round Digest ID Request Reason Coordinator Sequence")
    for name in ("Topology", "Coordinator", "Dependency"):
        integer(tx[name])
    for name in ("Round", "Coordinator", "Sequence"):
        integer(outcome[name])
    name, owner = operation(spec, ordinal)
    require(tx["Graph"] == config["Graph"] and tx["Topology"] == config["Topology"] and tx["ID"] == name and tx["Request"] == "request/" + name and tx["Coordinator"] == owner and tx["Dependency"] == 0, "FDB original request/namespace/order")
    command, metadata, effects, reads = fdb_contract(spec, ordinal)
    require(same(tx["GraphChange"], command) and same(tx["Lifecycle"], metadata), "FDB declared identity/provenance/definition")
    require([p["Group"] for p in tx["Participants"]] == sorted(effects), "FDB participant inventory")
    for participant in tx["Participants"]:
        fields(participant, "Group Epoch Generation Reads Effects")
        group_id = integer(participant["Group"], 0, 1)
        integer(participant["Epoch"], 1)
        integer(participant["Generation"])
        require(participant["Epoch"] == config["Epochs"][group_id] and participant["Generation"] == generations[group_id], "FDB epoch/predecessor generation")
        require(same(participant["Effects"], effects[group_id]) and same(participant["Reads"], [{"Key": key, "Version": physical.get((group_id, key), {}).get("Version", 0)} for key in reads[group_id]]), "FDB every physical effect/read")
        generations[group_id] += 1
    sequences[owner] += 1
    integer(outcome["Round"], 1)
    require(outcome["Commit"] is True and outcome["Reason"] == "" and outcome["Sequence"] == sequences[owner] and outcome["Coordinator"] == owner and outcome["ID"] == tx["ID"] and outcome["Request"] == tx["Request"], "FDB original receipt binding")
    require(type(outcome["Digest"]) is list and len(outcome["Digest"]) == 32 and all(type(v) is int and 0 <= v <= 255 for v in outcome["Digest"]) and bytes(outcome["Digest"]) == hashlib.sha256(go_json(tx)).digest(), "FDB original request digest")
    expected = [(g, effect) for g in sorted(effects) for effect in effects[g]]
    require(len(group["Writes"]) == len(expected), "FDB complete physical writes")
    for (group_id, effect), write in zip(expected, group["Writes"]):
        require(set(write) in ({"Group", "Key", "After"}, {"Group", "Key", "Before", "After"}), "FDB write fields")
        key = group_id, effect["Key"]
        prior = physical.get(key)
        require(type(write["Group"]) is int and write["Group"] == group_id and write["Key"] == effect["Key"] and same(write.get("Before"), prior), "FDB physical predecessor/owner/key")
        if prior:
            require(outcome["Round"] > prior["Round"], "FDB original revision order")
        after = {"Value": effect["Value"], "Version": 1 if prior is None else prior["Version"] + 1, "Round": outcome["Round"], "Deleted": False, "TxID": tx["ID"]}
        require(same(write["After"], after), "FDB physical before/after revision/value")
        physical[key] = write["After"]
    return fdb_logical(group, command, spec, ordinal, definitions)


def fdb_logical(group, command, spec, ordinal, definitions):
    name, owner = operation(spec, ordinal)
    node, edge = command["Node"], command["Edge"]
    entities, lives, values, changes = [], [], [], []
    if command["Kind"] == "node-create":
        entity, life = node["Node"], node["Life"]
        entities = [{"id": entity, "kind": 1, "source": 0, "target": 0, "mode": 0, "type": ""}]
        lives = [{"owner": entity, "life": life, "sourceLife": 0, "targetLife": 0}]
        components = [(entity, 0, 1, "", life, "life", 0)]
    else:
        entity, life, value = edge["ID"], edge["ID"] + 1, edge["Value"]
        components = []
        if command["Kind"] == "edge-create":
            entities = [{"id": entity, "kind": 2, "source": edge["Source"]["Node"], "target": edge["Target"]["Node"], "mode": 1, "type": "edge"}]
            lives = [{"owner": entity, "life": life, "sourceLife": edge["Source"]["Life"], "targetLife": edge["Target"]["Life"]}]
            components.append((entity, 0, 1, "", life, "life", 0))
        components.append((entity, life, 3, "p", value, "i64", value))
        if value not in definitions:
            values.append(group["Tx"]["Lifecycle"]["Definition"])
            definitions.add(value)
    for entity, life, component, prop, handle, kind, value in components:
        before = absent()
        if command["Kind"] == "edge-update":
            # All three checked physical rows have the same actual predecessor.
            physical_before = group["Writes"][0]["Before"]
            require(all(w["Before"] == physical_before and w["After"] == group["Writes"][0]["After"] for w in group["Writes"]), "FDB posting revision correspondence")
            before = {"present": True, "null": False, "handle": physical_before["Value"], "revision": physical_before["Round"], "provenance": ordinal - spec["facts"], "kind": "i64", "i64": physical_before["Value"]}
        after = {"present": True, "null": False, "handle": handle, "revision": group["Outcome"]["Round"], "provenance": ordinal, "kind": kind, "i64": value}
        changes.append({"key": (entity, life, component, prop), "scope": SCOPE, "before": canonical_cell(before), "after": canonical_cell(after)})
    return {"operation": name, "coordinator": owner, "effects": changes, "entities": entities, "lives": lives, "values": values}


def fdb_groups(path, spec, config, final_cut, stats, spool):
    physical, definitions = StoredState(spool, b"fdb-before"), set()
    generations, sequences = [0, 0], [0, 0]
    total = 3 + 2 * spec["facts"]
    count = 0
    bindings = hashlib.sha256()
    for count, group in enumerate(rows(path, total), 1):
        canonical = fdb_group(group, spec, count, config, physical, generations, sequences, definitions)
        require(group["Outcome"]["Round"] <= final_cut["Round"], "FDB original outside final cut")
        stats["physicalWrites"] += len(group["Writes"])
        binding = {k: group["Tx"][k] for k in ("Graph", "Coordinator", "ID", "Request", "Lifecycle")}
        binding["Digest"] = group["Outcome"]["Digest"]
        bindings.update(json.dumps(binding, sort_keys=True, separators=(",", ":")).encode() + b"\n")
        yield canonical
    require(count == total and sequences == final_cut["Groups"], "FDB complete logical groups/cut watermark")
    stats["originalBindingsSHA256"] = bindings.hexdigest()


def fdb_key(config, group, kind, name):
    prefix = f"g{group}/{config['Graph'].encode().hex()}/"
    return (prefix + kind + "/" + name.encode().hex()).encode()


def decoded_body(raw):
    require(0 < len(raw) <= LINE_BYTES, "native raw body byte bound")
    value = json.loads(raw, object_pairs_hook=pairs, parse_constant=lambda _: require(False, "nonfinite JSON"))
    require(raw == go_json(value), "noncanonical native raw body")
    return value


def raw_binding(record, key, value):
    fields(record, "Key Value")
    require(raw_bytes(record["Key"]) == key and raw_bytes(record["Value"]) == go_json(value), "raw metadata/config/cut binding")


def artifact_header(header, name, result):
    fields(header, "Type Format Stream Config InitialCut OldCut FinalCut Policy Metadata ConfigRecords MetadataRecords CutRecords")
    require(header["Type"] == "header" and header["Format"] == RAW_FORMAT and header["Stream"] == name, "artifact header/format/stream")
    for key in ("Config", "InitialCut", "OldCut", "FinalCut"):
        require(same(header[key], result[key]), "mixed artifact/result " + key)
    fields(header["Policy"], "PageRows PageBytes")
    integer(header["Policy"]["PageRows"], 1, 128)
    integer(header["Policy"]["PageBytes"], 1, 16 << 20)
    config = header["Config"]
    require(len(header["Metadata"]) == len(header["ConfigRecords"]) == len(header["MetadataRecords"]) == 2 and len(header["CutRecords"]) == 3, "complete native header inventory")
    for g, metadata in enumerate(header["Metadata"]):
        fields(metadata, "Floor Generation Versions Transactions Certificates Bytes Groups")
        for value in metadata.values():
            integer(value)
        require(metadata["Floor"] == header["FinalCut"]["Round"] and metadata["Groups"] == header["FinalCut"]["Groups"][g] and metadata["Transactions"] == metadata["Groups"], "quiescent native metadata")
        for key in ("Bytes", "Transactions", "Versions", "Certificates"):
            require(metadata[key] <= config["Limits"][key], "native metadata quota")
        raw_binding(header["ConfigRecords"][g], fdb_key(config, g, "meta", "config"), config)
        raw_binding(header["MetadataRecords"][g], fdb_key(config, g, "meta", "state"), metadata)
    count = config["Cost"]["facts"]
    for n, key in enumerate(("InitialCut", "OldCut", "FinalCut")):
        cut = header[key]
        fields(cut, "Graph Topology Epochs Scope Round Groups")
        require(cut["Graph"] == config["Graph"] and cut["Topology"] == config["Topology"] and same(cut["Epochs"], config["Epochs"]) and raw_bytes(cut["Scope"]) == b"\0\1", "foreign cut namespace/full scope")
        integer(cut["Round"], 1)
        require(same(cut["Groups"], ([0, 0], [2 + count, 1], [2 + 2 * count, 1])[n]), "fixed writer1 cut membership")
        raw_binding(header["CutRecords"][n], fdb_key(config, 0, "cut", go_json(cut).hex()), cut)
    require(header["InitialCut"]["Round"] < header["OldCut"]["Round"] < header["FinalCut"]["Round"], "old/final cut order")


def artifact_rows(path, name, result, stats):
    """Raw order and original bytes survive; no footer or trailing-row shortcut."""
    maximum = 3 + 6 * result["spec"]["facts"]
    digest, counts, header, footer_seen = hashlib.sha256(), [0, 0], None, False
    with Path(path).open("rb") as source:
        for number in range(maximum + 3):
            line = source.readline(ARTIFACT_LINE_BYTES + 1)
            if not line:
                break
            require(len(line) <= ARTIFACT_LINE_BYTES and line.endswith(b"\n") and not footer_seen, "artifact line/EOF bound")
            value = json.loads(line, object_pairs_hook=pairs, parse_constant=lambda _: require(False, "nonfinite JSON"))
            require(type(value) is dict, "artifact JSON object required")
            require(line == go_json(value) + b"\n", "noncanonical artifact JSONL")
            if number == 0:
                artifact_header(value, name, result)
                header = value
                digest.update(line)
                yield header
            elif value.get("Type") == "footer":
                fields(value, "Type Rows Groups Metadata PayloadSHA256")
                require(same(value["Rows"], counts) and same(value["Groups"], header["FinalCut"]["Groups"]) and same(value["Metadata"], header["Metadata"]) and value["PayloadSHA256"] == digest.hexdigest(), "artifact incomplete/mixed footer")
                footer_seen = True
            else:
                fields(value, "Type Row")
                require(value["Type"] == "row" and sum(counts) < maximum, "artifact row type/count")
                row = value["Row"]
                fields(row, "Group Key Value" + (" IDKey IDValue RequestKey RequestValue" if name == "cdc" else ""))
                integer(row["Group"], 0, 1)
                raw = {key: raw_bytes(body) for key, body in row.items() if key != "Group"}
                require(0 < len(raw["Key"]) <= 1024 and 0 < len(raw["Value"]) <= LINE_BYTES and sum(map(len, raw.values())) <= header["Policy"]["PageBytes"], "raw row admission/key/value")
                counts[row["Group"]] += 1
                digest.update(line)
                yield {"Group": row["Group"], **raw}
        require(header is not None and footer_seen and not source.read(1), "missing/truncated/extra artifact EOF")
    index = ("history", "current", "cdc").index(name)
    end = result["StreamEnds"][index]
    fields(end, "Rows Groups Metadata")
    require(same(end, {"Rows": counts, "Groups": header["FinalCut"]["Groups"], "Metadata": header["Metadata"]}), "result/EOF binding")
    stats[name] = {"rows": counts, "payloadSHA256": digest.hexdigest()}


def put_raw(spool, namespace, key, body):
    prior = spool.find(namespace, key)
    if prior is None:
        spool.insert(namespace, key, body)
    else:
        spool.replace(namespace, key, prior, body)


def fdb_raw_groups(path, spec, config, final_cut, stats, spool, result):
    reader = artifact_rows(path, "cdc", result, stats)
    header = next(reader, None)
    require(header is not None, "missing CDC header")
    physical, definitions = StoredState(spool, b"fdb-before"), set()
    generations, sequences, versions, current_counts = [0, 0], [0, 0], [0, 0], [0, 0]
    native_bytes = [0, 0]
    maximum_metadata = {k: (1 << 64) - 1 for k in ("Floor", "Generation", "Versions", "Transactions", "Certificates", "Bytes", "Groups")}
    for g in range(2):
        native_bytes[g] = (len(fdb_key(config, g, "meta", "config")) + len(go_json(config)) +
                           len(fdb_key(config, g, "meta", "state")) + len(go_json(maximum_metadata)))
    for record in header["CutRecords"]:
        native_bytes[0] += len(raw_bytes(record["Key"])) + len(raw_bytes(record["Value"]))
    pending, ordinal_next, previous_order = {}, 1, None
    bindings = hashlib.sha256()
    total = 3 + 2 * spec["facts"]
    for raw in reader:
        group = decoded_body(raw["Value"])
        ordinal = integer(group["Tx"]["Lifecycle"]["Provenance"], 1, total)
        require(ordinal <= max(3, ordinal_next) and ordinal not in pending, "writer1 bounded setup/execution identity order")
        canonical = fdb_group(group, spec, ordinal, config, physical, generations, sequences, definitions)
        outcome = group["Outcome"]
        order = outcome["Round"], outcome["Coordinator"], outcome["Request"]
        require(previous_order is None or previous_order < order, "raw CDC deterministic enumeration order")
        previous_order = order
        owner = outcome["Coordinator"]
        prefix = f"g{owner}/{config['Graph'].encode().hex()}/"
        expected_key = (prefix + f"changes/{outcome['Sequence']:016x}/" + outcome["Request"].encode().hex()).encode()
        require(raw["Group"] == owner and raw["Key"] == expected_key and raw["IDKey"] == fdb_key(config, owner, "id", outcome["ID"]) and raw["RequestKey"] == fdb_key(config, owner, "request", outcome["Request"]) and raw["IDValue"] == raw["RequestValue"] == go_json(outcome), "raw original ID/request/sequence binding")
        require(header["InitialCut"]["Round"] < outcome["Round"] <= final_cut["Round"] and (outcome["Round"] <= header["OldCut"]["Round"]) == (ordinal <= 3 + spec["facts"]), "original old/final cut membership")
        spool.insert(b"fdb-id", raw["IDKey"], raw["IDValue"])
        spool.insert(b"fdb-request", raw["RequestKey"], raw["RequestValue"])
        spool.insert(b"fdb-operation", u64(ordinal), raw["Value"])
        native_bytes[owner] += sum(len(body) for body in raw.values() if type(body) is bytes)
        for write in group["Writes"]:
            g, key, after = write["Group"], write["Key"], write["After"]
            body = go_json(after)
            current_key = fdb_key(config, g, "current", key)
            history_key = (f"g{g}/{config['Graph'].encode().hex()}/history/{after['Round']:016x}/" + key.encode().hex()).encode()
            spool.insert(b"fdb-history", history_key, body)
            prior = spool.find(b"fdb-current", current_key)
            if prior is None:
                current_counts[g] += 1
            else:
                native_bytes[g] -= len(current_key) + len(prior)
            put_raw(spool, b"fdb-current", current_key, body)
            native_bytes[g] += len(history_key) + len(body) + len(current_key) + len(body)
            versions[g] += 1
        stats["physicalWrites"] += len(group["Writes"])
        for key in ("Key", "Value", "IDKey", "IDValue", "RequestKey", "RequestValue"):
            bindings.update(u64(len(raw[key])) + raw[key])
        pending[ordinal] = canonical
        require(len(pending) <= 3, "fixed writer1 setup buffer")
        while ordinal_next in pending:
            yield pending.pop(ordinal_next)
            ordinal_next += 1
    require(not pending and ordinal_next == total + 1 and sequences == final_cut["Groups"], "incomplete raw original operations/watermarks")
    for g, metadata in enumerate(header["Metadata"]):
        require(metadata["Versions"] == versions[g] and metadata["Generation"] == generations[g] and metadata["Certificates"] == (3 if g == 0 else 0) and metadata["Bytes"] == native_bytes[g], "exact retained metadata census")
    operation_count = 0
    for operation_count, group in enumerate(rows(Path(path).with_name("fdb-cost-operation-groups.jsonl"), total), 1):
        require(go_json(group) == spool.lookup(b"fdb-operation", u64(operation_count)), "per-operation original execution order/binding")
    require(operation_count == total, "incomplete per-operation original file")
    samples = 0
    for samples, sample in enumerate(rows(Path(path).with_name("fdb-cost-latency.jsonl"), total), 1):
        fields(sample, "phase fact cross ProposalNanos AckNanos VerificationNanos ProposalAttempts CommitAttempts Outcome")
        phase, fact, node, edge = fixture(spec, samples)
        require(sample["phase"] == phase and type(sample["fact"]) is int and sample["fact"] == fact and type(sample["cross"]) is bool and sample["cross"] == bool(edge and edge["Target"]["Owner"]), "per-request timing identity")
        for name in ("ProposalNanos", "AckNanos", "VerificationNanos"):
            integer(sample[name], 0, (1 << 63) - 1)
        for name in ("ProposalAttempts", "CommitAttempts"):
            integer(sample[name], 1, (1 << 31) - 1)
        name, owner = operation(spec, samples)
        require(go_json(sample["Outcome"]) == spool.lookup(b"fdb-id", fdb_key(config, owner, "id", name)), "per-request timing original receipt")
    require(samples == total, "incomplete per-request timings")
    for name, expected_counts in (("history", versions), ("current", current_counts)):
        reader = artifact_rows(Path(path).with_name("fdb-cost-" + name + ".jsonl"), name, result, stats)
        actual_header = next(reader, None)
        require(same(actual_header, header | {"Stream": name}), "mixed native stream headers")
        previous_key, counts = None, [0, 0]
        for row in reader:
            key = row["Group"], row["Key"]
            require(previous_key is None or previous_key < key, "raw history/current duplicate/reordered row")
            previous_key = key
            require(row["Value"] == spool.lookup(b"fdb-" + name.encode(), row["Key"]), "raw history/current differs from exact original physical write")
            counts[row["Group"]] += 1
            if name == "history":
                value = decoded_body(row["Value"])
                if value["Round"] <= header["OldCut"]["Round"]:
                    logical = row["Key"].split(b"/", 4)[-1]
                    current_key = fdb_key(config, row["Group"], "current", bytes.fromhex(logical.decode()).decode())
                    put_raw(spool, b"fdb-old", current_key, row["Value"])
        require(counts == expected_counts, "missing native retained history/current rows")
    stats["originalBindingsSHA256"] = bindings.hexdigest()
    stats["orderContract"] = "raw (Round,Coordinator,Request) unchanged; execution originals provenance1,2,3 bind independent setup rounds through fixed three-entry buffer"


def retained_observations(path, spec, config, spool):
    """Actual point cells match actual canonical CDC and every raw posting copy."""
    source, digest = iter(rows(path, 2 * spec["facts"])), hashlib.sha256()
    state = StoredState(spool, b"native-before")
    for value in (5, 6):
        namespace = b"fdb-old" if value == 5 else b"fdb-current"
        for fact in range(spec["facts"]):
            entity = next(source, None)
            require(entity is not None, "missing native retained point")
            fields(entity, "id found kind source target mode type presence effective effectiveLife labels property")
            ordinal = 4 + fact + (spec["facts"] if value == 6 else 0)
            _, _, _, edge = fixture(spec, ordinal)
            identity, life = edge["ID"], edge["ID"] + 1
            require(same({k:entity[k] for k in ("id", "found", "kind", "source", "target", "mode", "type", "effective", "effectiveLife", "labels")},
                         {"id": identity, "found": True, "kind": 2, "source": edge["Source"]["Node"], "target": edge["Target"]["Node"], "mode": 1, "type": "edge", "effective": True, "effectiveLife": life, "labels": None}), "native retained endpoint/life/declaration")
            for key, cell, provenance in (((identity, 0, 1, ""), entity["presence"], 4 + fact), ((identity, life, 3, "p"), entity["property"], ordinal)):
                canonical_cell(cell)
                require(go_json(cell) == spool.lookup(b"native-cell-history", state.key(key) + u64(provenance)), "native retained cell differs from its exact original revision")
            _, _, effects, _ = fdb_contract(spec, ordinal)
            physical = None
            for owner, writes in effects.items():
                for write in writes:
                    raw = spool.lookup(namespace, fdb_key(config, owner, "current", write["Key"]))
                    if physical is None:
                        physical = decoded_body(raw)
                    require(raw == go_json(physical) and physical["Value"] == value and physical["Version"] == (1 if value == 5 else 2) and physical["TxID"] == operation(spec, ordinal)[0], "retained raw edge/out/in correspondence")
            for node in (edge["Source"], edge["Target"]):
                logical = f"node/{node['Node']:016x}/life/{node['Life']:016x}"
                alive = decoded_body(spool.lookup(namespace, fdb_key(config, node["Owner"], "current", logical)))
                require(alive["Value"] == alive["Version"] == 1 and alive["Deleted"] is False, "retained endpoint life")
            digest.update(go_json({"id": identity, "source": edge["Source"]["Node"], "target": edge["Target"]["Node"], "life": life, "value": value, "operationProvenance": ordinal}) + b"\n")
    require(next(source, None) is None, "extra native retained point")
    return digest.hexdigest()


def compare_streams(native_path, fdb_path, spec, config, final_cut, native_bounds, *, spool_directory=None, spool_policy=SMALL_POLICY, raw_result=None, observations_path=None):
    owner = Path(native_path).parent if spool_directory is None else spool_directory
    with Spool(owner, spool_policy) as spool:
        report = _compare_streams(native_path, fdb_path, spec, config, final_cut, native_bounds, spool, raw_result, observations_path)
        report["comparisonState"] = spool.report()
        report["cleanup"] = "private spool removed on success/failure; raw inputs unchanged"
        return report


def _compare_streams(native_path, fdb_path, spec, config, final_cut, native_bounds, spool, raw_result, observations_path):
    fields(spec, "version facts crossPercent writers")
    integer(spec["facts"], 1, 1_000_000)
    require(type(spec["version"]) is type(spec["writers"]) is type(spec["crossPercent"]) is int and spec["version"] == 1 and spec["writers"] == 1 and spec["crossPercent"] in (0, 10, 50, 100), "fixed writer1 cost spec")
    count = spec["facts"]
    expected_config = {"Format": 2, "Graph": "graph-cost-v1", "Topology": 1, "Epochs": [7, 11],
                       "Limits": {"Bytes": (64 << 20) + count * (64 << 10), "Transactions": 3 + 2 * count, "Versions": 3 + 6 * count, "Certificates": 1024},
                       "Nodes": [1, 7 + 2 * count], "Cost": spec, "CostScope": SCOPE}
    require(same(config, expected_config), "FDB immutable cost contract")
    fields(final_cut, "Graph Topology Epochs Scope Round Groups")
    integer(final_cut["Topology"], 1)
    require(final_cut["Graph"] == config["Graph"] and final_cut["Topology"] == config["Topology"] and same(final_cut["Epochs"], config["Epochs"]) and raw_bytes(final_cut["Scope"]) == b"\0\1", "FDB final cut namespace/scope")
    require(type(final_cut["Groups"]) is list and len(final_cut["Groups"]) == 2, "FDB group watermark shape")
    for watermark in final_cut["Groups"]:
        integer(watermark)
    integer(final_cut["Round"], 1)
    native_stats, fdb_stats = {"records": 0, "controls": 0}, {"physicalWrites": 0}
    left = native_groups(native_path, spec, native_bounds, native_stats, spool)
    if raw_result is None:
        right = fdb_groups(fdb_path, spec, config, final_cut, fdb_stats, spool)
    else:
        fields(raw_result, "version engine spec deployment SetupNodes Creates Updates CompletedCreates CompletedUpdates SetupNanos WritesNanos VerificationNanos TotalNanos InitialCut OldCut FinalCut Config CDCGroups LogicalComponentChanges PhysicalGraphChanges IDAdmission oracleFormat StreamEnds")
        require(observations_path is not None and "oldCut" in native_bounds, "raw parity requires actual retained observations/old cut")
        require(type(raw_result["version"]) is int and raw_result["version"] == 2 and raw_result["oracleFormat"] == RAW_FORMAT and same(raw_result["Config"], config) and same(raw_result["FinalCut"], final_cut), "mixed raw result format/config/cut")
        require(same(raw_result["spec"], spec) and len(raw_result["StreamEnds"]) == 3, "raw result specification/complete EOF set")
        for name, expected in (("SetupNodes", 3), ("Creates", count), ("Updates", count), ("CompletedCreates", count), ("CompletedUpdates", count), ("CDCGroups", 3 + 2 * count), ("LogicalComponentChanges", 3 + 3 * count), ("PhysicalGraphChanges", 3 + 6 * count)):
            require(type(raw_result[name]) is int and raw_result[name] == expected, "raw result exact cardinality")
        require(raw_result["engine"] == "foundationdb" and raw_result["IDAdmission"] == "fixed declared fixture IDs/ranges; this adapter has no rho allocator protocol", "raw engine/admission identity")
        right = fdb_raw_groups(fdb_path, spec, config, final_cut, fdb_stats, spool, raw_result)
    digest, groups, components = hashlib.sha256(), 0, 0
    while True:
        native, fdb = next(left, None), next(right, None)
        require((native is None) == (fdb is None), "missing complete logical group")
        if native is None:
            break
        require(same(native, fdb), "ordered complete logical group/effects/declarations differ")
        groups += 1
        components += len(native["effects"])
        digest.update(json.dumps(native, sort_keys=True, separators=(",", ":")).encode() + b"\n")
    require(groups == 3 + 2 * spec["facts"] and components == 3 + 3 * spec["facts"], "complete logical cardinality")
    report = {"equal": True, "logicalGroups": groups, "componentChanges": components,
            "canonicalLogicalCDCSHA256": digest.hexdigest(), "native": native_stats, "fdb": fdb_stats,
            "nativeCodecBoundary": "cost-only qualified native decoder binds retained same-index views/originals/canonical CDC; Python checks outcome seals/fields, certified prefix/root hashes and complete decoded logical records",
            "scope": "fixed writer1 create/update contract; numeric rounds/indexes and physical protocol/replica layouts are validated per engine"}
    if raw_result is not None:
        report["canonicalRetainedOutputSHA256"] = retained_observations(observations_path, spec, config, spool)
        report["oracleFormat"] = RAW_FORMAT
    return report
