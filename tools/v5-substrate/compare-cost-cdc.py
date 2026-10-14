#!/usr/bin/env python3
"""Stream declared real cost cells; opaque native codec validation stays explicit."""
import base64
import hashlib
import json
import os
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
COLUMN_FORMAT = "fdb-graph-cost-columns-v1"
PARALLEL_FORMAT = "fdb-graph-cost-parallel-v1"
PARALLEL_COLUMN_FORMAT = "fdb-graph-cost-parallel-columns-v1"
COLUMN_LAYOUT = "scalar-cross-edge-i64-columns-v1"
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


def native_original(row, *, parallel=False):
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
    require(kind in ("local-graph", "cross") and original["Coordinator"] in PARTITIONS and (parallel or original["Reason"] == 0), "unknown business original")
    require(not parallel or original["Reason"] in (0,1,2,3,4,5,6,8,9,10,11), "unknown native retained refusal reason")
    for name, size in (("Request", 16), ("Transaction", 16), ("Digest", 32)):
        text = original[name]
        require(type(text) is str and len(text) == 2 * size and text == bytes.fromhex(text).hex() and int(text, 16) > 0, "original identity/digest")
    namespace = bytes.fromhex(GRAPH) + struct.pack(">Q", row["Partition"])
    require(not original["Commit"] or original["Reason"] == 0, "committed original refusal reason")
    if kind == "local-graph":
        body = fixed_body(outcome, b"GRO\4", 120)
        require(body == b"\6" + namespace + bytes.fromhex(original["Request"]) + bytes.fromhex(original["Digest"]) + struct.pack(">QBH", row["Index"], 1, original["Reason"]), "local original fields")
        if parallel and not original["Commit"]:
            require(original["Transaction"] == original["Request"] and original["Participants"] == [row["Partition"]] and original["Coordinator"] == row["Partition"] and original["Disposition"] == 1 and original["Phase"] == original["Round"] == original["Provenance"] == 0 and original["Reason"] > 0 and classification == "protocol-empty" and not cdc and not any(logical.values()), "local retained refusal classification")
            return original
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
            if parallel and not original["Commit"]:
                require(round_ == 0 and not cdc and not any(logical.values()) and classification == "protocol-empty", "cross abort resolve classification")
                return original
            require(original["Commit"] and round_ > 0 and cdc and classification in ("logical", "participant-empty"), "cross resolve classification")
            require((classification == "logical") == bool(sum(logical.values())), "cross logical classification")
        else:
            require(not cdc and not any(logical.values()) and classification == "protocol-empty", "cross protocol classification")
            require((parallel and phase == 3 and not original["Commit"] and round_ == 0) or original["Commit"] == (phase == 3) and (round_ > 0) == (phase == 3), "cross decision phase")
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
        require(change["After"]["revision"] > change["Before"]["revision"], "native actual component predecessor revision order")
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


def native_parallel_proposal(raw, request, ordinal):
    require(181 <= len(raw) <= LINE_BYTES and raw[:4] in (b"PGQ\1", b"PGQ\2") and hashlib.sha256(raw[:-32]).digest() == raw[-32:], "native immutable proposal envelope/seal")
    require(raw[4] == 6 and raw[5:21] == bytes.fromhex(GRAPH) and raw[29:45] == request and all(any(raw[n:n+32]) for n in (45,77,109)), "native actual proposal namespace/identity/configuration")
    partition = struct.unpack_from(">Q",raw,21)[0]
    require(partition in PARTITIONS, "native proposal partition")
    size = struct.unpack_from(">I",raw,141)[0]
    require(145+size == len(raw)-32, "native proposal exact inner span")
    inner = raw[145:-32]
    require(len(inner) >= 97 and inner[:5] == b"PGO\1\6" and inner[5:45] == raw[5:45] and hashlib.sha256(inner[:-32]).digest() == inner[-32:], "native original nested request seal/identity")
    revision, provenance = struct.unpack_from(">QQ",inner,45)
    require(revision > 0 and provenance == ordinal, "native proposal actual revision/provenance")
    return partition


def native_parallel_manifest(raw, request, transaction, digest, proposal):
    require(192 <= len(raw) <= LINE_BYTES and raw[:4] == b"GCM\1" and hashlib.sha256(raw[:-32]).digest() == raw[-32:] and hashlib.sha256(raw).digest() == digest, "native immutable actual manifest seal/digest")
    require(raw[4:20] == transaction, "native manifest original transaction")
    coordinator = struct.unpack_from(">Q",raw,20)[0]
    size = struct.unpack_from(">I",raw,28)[0]
    original = raw[32:32+size]
    require(size <= LINE_BYTES and len(original) == size, "native manifest original span")
    native_parallel_proposal(original,request,struct.unpack_from(">Q",proposal[145:-32],53)[0])
    # The coordinator recaptures guards/revision on the same original request;
    # authority/config/routing bytes and the source request identity stay fixed.
    require(original[4:141] == proposal[4:141] and original[5:21] == bytes.fromhex(GRAPH), "native manifest actual business/configuration binding")
    position = 32+size
    require(position+132 <= len(raw)-32 and raw[position:position+96] == original[45:141] and any(raw[position+96:position+128]), "native manifest immutable layout digests")
    count = struct.unpack_from(">I",raw,position+128)[0]
    require(count == 2 and coordinator == 3, "fixed cost actual manifest footprint")
    position += 132
    parts = []
    for _ in range(count):
        require(position+20 <= len(raw)-32, "native manifest truncated participant")
        partition,floor,size = struct.unpack_from(">QQI",raw,position)
        position += 20
        require(0 < size <= LINE_BYTES and position+size <= len(raw)-32 and floor < (1<<64)-1, "native manifest bounded kernel span")
        # Prepared kernel semantics remain the actual qualified Go decoder's
        # boundary; every raw byte is retained and manifest-hash bound here.
        parts.append(partition); position += size
    require(parts == [3,8] and position == len(raw)-32, "native actual participant inventory/trailing bytes")


def native_parallel_attempts(path, spec, spool):
    joined = StoredState(spool,b"native-attempt-join")
    attempts,aborts,submissions,commits = 0,0,0,0
    clients = [0]*32
    for a in rows(path,8*spec["facts"]):
        fields(a,"Operation Engine ClientID AttemptOrdinal PreviousRequest Request Transaction BodySHA256 BusinessSHA256 DecisionDigest OriginalIndex Round Classification Error SubmitInvocations Proposal Manifest Outcome CDC")
        ordinal = parallel_operation(spec,a["Operation"])
        phase,_,node,edge = fixture(spec,ordinal)
        require(node is None, "native business attempt stream contains setup")
        request = bytes(integer(v,0,255) for v in a["Request"])
        transaction = bytes(integer(v,0,255) for v in a["Transaction"])
        previous = bytes(integer(v,0,255) for v in a["PreviousRequest"])
        require(len(request) == len(transaction) == len(previous) == 16 and any(request), "native attempt fixed identity fields")
        client,number = integer(a["ClientID"],0,spec["writers"]-1),integer(a["AttemptOrdinal"],1,4)
        prior = joined.get((ordinal,))
        require((prior is None and number == 1 and not any(previous)) or prior is not None and prior["Classification"] in ("authoritative-abort","authoritative-conflict") and number == prior["AttemptOrdinal"]+1 and client == prior["ClientID"] and previous == bytes(prior["Request"]) and request != previous, "native actual fresh attempt lineage")
        proposal = raw_bytes(a["Proposal"])
        require(a["Engine"] == "native-raft" and a["Error"] == "" and hashlib.sha256(proposal).digest() == sha_array(a["BodySHA256"]), "native frozen actual body hash")
        partition = native_parallel_proposal(proposal,request,ordinal)
        require(partition == 3, "native actual business coordinator")
        outcome,cdc = raw_bytes(a["Outcome"]),raw_bytes(a["CDC"])
        cross = bool(edge["Target"]["Owner"])
        index,round_ = integer(a["OriginalIndex"],1),integer(a["Round"])
        classification = a["Classification"]
        if cross:
            digest = sha_array(a["DecisionDigest"])
            business = bytearray(proposal[:-32]); business[3] = 1
            business = bytes(business) + hashlib.sha256(business).digest()
            require(sha_array(a["BusinessSHA256"]) == hashlib.sha256(business).digest(), "native frozen business body independent of proposal frame version")
            manifest = raw_bytes(a["Manifest"])
            native_parallel_manifest(manifest,request,transaction,digest,proposal)
            derived = hashlib.sha256(b"rho-tkg:cross-transaction-id:v1\0" + bytes.fromhex(GRAPH)+u64(3)+request).digest()[:16]
            require(transaction == derived, "native attempt cross transaction/request derivation")
            body = fixed_body(outcome,b"GCS\1",135)
            require(body[:72] == bytes.fromhex(GRAPH)+u64(3)+transaction+digest and body[72] == 3 and struct.unpack_from(">Q",body,73)[0] == index and struct.unpack_from(">Q",body,89)[0] == round_ and not cdc and body[-1] == 0, "native genuine original Decide binding")
            committed = bool(body[-2]&2)
            require(body[-2] <= 3 and committed == (classification == "commit") and (round_ > 0) == committed and classification in ("commit","authoritative-abort"), "native terminal cross classification")
        else:
            require(not any(transaction) and not any(sha_array(a["DecisionDigest"])) and not any(sha_array(a["BusinessSHA256"])) and a["Manifest"] is None and round_ == 0, "native local attempt union")
            body = fixed_body(outcome,b"GRO\4",120)
            require(body[:73] == b"\6"+bytes.fromhex(GRAPH)+u64(3)+request+sha_array(a["BodySHA256"]) and struct.unpack_from(">Q",body,73)[0] == index and body[81] == 1, "native original local request/body/index")
            reason = struct.unpack_from(">H",body,82)[0]
            committed = reason == 0
            require((committed and classification == "commit" and cdc) or (not committed and classification == "authoritative-conflict" and reason in (8,11) and not cdc), "native authenticated local conflict classification")
        count = integer(a["SubmitInvocations"],1,128)
        attempts += 1; submissions += count; aborts += not committed; commits += committed
        spool.insert(b"native-attempt-request",u64(partition)+request,go_json(a))
        if committed:
            spool.insert(b"native-useful-attempt",u64(ordinal),go_json(a)); clients[client] += 1
        joined[(ordinal,)] = {k:a[k] for k in ("ClientID","AttemptOrdinal","Request","Classification")}
    require(commits == 2*spec["facts"], "native complete useful attempt census")
    for ordinal in range(4,4+2*spec["facts"]): require(joined.get((ordinal,),{}).get("Classification") == "commit", "native missing useful operation")
    samples = 0
    for sample in rows(Path(path).with_name("latency.jsonl"),2*spec["facts"]):
        fields(sample,"phase fact cross elapsedNanos proposalNanos ackNanos drainNanos verificationNanos clientAttempts localAckTiming operation clientID admissionNanos")
        ordinal = parallel_operation(spec,sample["operation"])
        phase,fact,node,edge = fixture(spec,ordinal)
        require(node is None and sample["phase"] == phase and type(sample["fact"]) is int and sample["fact"] == fact and type(sample["cross"]) is bool and sample["cross"] == bool(edge["Target"]["Owner"]), "native useful timing operation identity")
        a = json.loads(spool.lookup(b"native-useful-attempt",u64(ordinal)),object_pairs_hook=pairs)
        require(type(sample["clientID"]) is type(sample["clientAttempts"]) is int and sample["clientID"] == a["ClientID"] and sample["clientAttempts"] == a["AttemptOrdinal"], "native useful timing exact original attempt/client")
        for key in ("elapsedNanos","proposalNanos","ackNanos","drainNanos","verificationNanos","admissionNanos"): integer(sample[key],0,(1<<63)-1)
        fields(sample["localAckTiming"],"ProposeCount StateCount PacketWaitCount EntryRecordCount ProposeNanos StateNanos PacketWaitNanos EntryRecordNanos")
        for value in sample["localAckTiming"].values(): integer(value,0,(1<<63)-1)
        spool.insert(b"native-sample-output",u64(ordinal),go_json(sample)); samples += 1
    require(samples == 2*spec["facts"], "native complete useful timing stream")
    return {"Attempts":attempts,"AbortedAttempts":aborts,"SubmitInvocations":submissions,"ClientCompletions":clients}


def native_parallel_groups(path,spec,bounds,stats,spool):
    attempts = native_parallel_attempts(Path(path).with_name("attempts.jsonl"),spec,spool)
    actual_result = json.loads(Path(path).with_name("native-cost-result.json").read_bytes(),object_pairs_hook=pairs)
    require(actual_result.get("runner") == "independent-clients-request-bound-ack-v2" and actual_result.get("ClientWindow") == 32 and same(actual_result.get("spec"),spec) and same(actual_result.get("clientCompletions"),attempts["ClientCompletions"]) and actual_result.get("Attempts") == attempts["Attempts"] and actual_result.get("AbortedAttempts") == attempts["AbortedAttempts"] and actual_result.get("SameAttemptRetries") == attempts["SubmitInvocations"] - attempts["Attempts"], "native actual parallel result/attempt policy")
    images,old = native_prefix(bounds),native_old_prefix(bounds)
    last = dict(zip(PARTITIONS,bounds["from"]))
    boundaries,old_seen = {},set()
    total = 3+2*spec["facts"]
    source_requests = StoredState(spool,b"native-source-request")
    for row in rows(path,128*(3+8*spec["facts"])+16):
        original = native_original(row,parallel=True)
        partition,index = row["Partition"],row["Index"]
        require(index == last[partition]+1 and index <= bounds["through"][PARTITIONS[partition]], "native actual parallel prefix missing/duplicate/reorder")
        last[partition] = index; stats["records"] += 1
        boundary = row["SourceBoundary"]
        if original["Kind"] == "no-application-output" and partition in boundaries:
            require(boundary["Generation"] == boundaries[partition]["Generation"] and boundary["ImageSHA256"] == boundaries[partition]["ImageSHA256"], "native empty frame changed source root")
        boundaries[partition] = boundary
        if index == old[partition][0]:
            require(boundary["ImageSHA256"] == old[partition][1], "native old actual prefix image"); old_seen.add(partition)
        if original["Kind"] in ("read-control","no-application-output"):
            stats["controls"] += 1; continue
        request = u64(original["Coordinator"])+bytes.fromhex(original["Request"])
        attempt_body = spool.find(b"native-attempt-request",request)
        if attempt_body is None:
            ordinal = integer(original["Provenance"],1,3)
            require(original["Kind"] == "local-graph" and original["Commit"], "foreign/unrecorded native business attempt")
            spool.insert(b"native-setup-request",u64(ordinal),request)
        else:
            attempt = json.loads(attempt_body,object_pairs_hook=pairs)
            ordinal = parallel_operation(spec,attempt["Operation"])
            require(original["Provenance"] == ordinal or original["Kind"] == "local-graph" and not original["Commit"] and original["Provenance"] == 0, "native original/attempt provenance")
            require((index <= old[partition][0]) == (attempt["Operation"]["Phase"] == "create"), "native actual attempt cut membership")
            if original["Kind"] == "local-graph" or original["Phase"] == 3:
                require(raw_bytes(row["Outcome"]) == raw_bytes(attempt["Outcome"]) and raw_bytes(row["CDC"]) == raw_bytes(attempt["CDC"]) and original["Index"] == attempt["OriginalIndex"], "native actual retained attempt frames")
            digest = attempt["BodySHA256"] if original["Kind"] == "local-graph" else attempt["DecisionDigest"]
            require(bytes.fromhex(original["Digest"]) == sha_array(digest), "native original/attempt exact digest")
        key = request+u64(partition)+u64(original["Phase"])
        spool.insert(b"native-phase",key,go_json(row))
        info = source_requests.get((original["Coordinator"],original["Request"]),{"Ordinal":ordinal,"Inventory":[]})
        require(info["Ordinal"] == ordinal and len(info["Inventory"]) < 6, "native finite actual phase inventory")
        info["Inventory"].append([partition,original["Phase"]]); source_requests[(original["Coordinator"],original["Request"])] = info
    require(list(last.values()) == bounds["through"] and old_seen == set(PARTITIONS) and all(boundaries[p]["ImageSHA256"] == images[p] for p in PARTITIONS) and actual_result.get("CDCRecords") == stats["records"], "native complete parallel retained cut/root EOF")
    previous,definitions = StoredState(spool,b"native-before"),set()
    bindings = hashlib.sha256()
    seed_indexes = {}
    # Lookup, not an SQL sort or dataset-sized join. Raw record enumeration has
    # already been authenticated; canonical output follows logical identity.
    for ordinal in range(1,total+1):
        if ordinal <= 3: request = spool.lookup(b"native-setup-request",u64(ordinal))
        else:
            attempt = json.loads(spool.lookup(b"native-useful-attempt",u64(ordinal)),object_pairs_hook=pairs)
            request = u64(3)+bytes(attempt["Request"])
        info = source_requests.get((struct.unpack(">Q",request[:8])[0],request[8:].hex()))
        require(info is not None and info["Ordinal"] == ordinal, "missing native useful source original")
        records = [json.loads(spool.lookup(b"native-phase",request+u64(p)+u64(s)),object_pairs_hook=pairs) for p,s in info["Inventory"]]
        phase,fact,_,_ = fixture(spec,ordinal)
        if phase != "setup":
            if fact == 0:
                seed_indexes[phase] = max(r["Index"] for r in records if r["Partition"] == 3)
            else:
                require(all(r["Index"] > seed_indexes[phase] for r in records if r["Partition"] == 3), "native genuine value seed must precede dependents")
        canonical = native_group(records,spec,ordinal,previous,definitions)
        for row in records: require(row["Original"]["Commit"] or row["Original"]["Phase"] in (1,2), "ABORT original claimed useful completion")
        first = records[0]["Original"]
        spool.insert(b"native-request",request,go_json(records))
        spool.insert(b"native-transaction",u64(first["Coordinator"])+bytes.fromhex(first["Transaction"]),bytes.fromhex(first["Digest"]))
        bindings.update(go_json({k:first[k] for k in ("Graph","Coordinator","Request","Transaction","Digest","Provenance")})+b"\n")
        yield json.loads(go_json(canonical),object_pairs_hook=pairs)
    # Every retained refusal must correspond to its authentic recorded attempt.
    # A first NO permits a shorter actual prepare prefix; never invent missing
    # votes, empty CDC or a capacity-observation proof from reported counts.
    for _,body in spool.walk(b"native-attempt-request"):
        a = json.loads(body,object_pairs_hook=pairs)
        if a["Classification"] == "commit": continue
        request = u64(3)+bytes(a["Request"])
        info = source_requests.get((3,bytes(a["Request"]).hex()))
        require(info is not None, "missing native ABORT source original")
        records = [json.loads(spool.lookup(b"native-phase",request+u64(p)+u64(s)),object_pairs_hook=pairs) for p,s in info["Inventory"]]
        if a["Classification"] == "authoritative-conflict": require(info["Inventory"] == [[3,0]], "native local refusal phase inventory")
        else:
            inventory = sorted(info["Inventory"])
            prepares = sorted(p for p,s in inventory if s == 2)
            require(prepares in ([3],[3,8]) and inventory == sorted([[3,1],[3,3],[3,4],[8,4]]+[[p,2] for p in prepares]), "native ABORT complete actual prepare prefix/resolve footprint")
            for row in records:
                if row["Original"]["Phase"] == 2:
                    status = fixed_body(raw_bytes(row["Outcome"]),b"GCS\1",135)
                    require(bool(status[-2]&1) == (row["Partition"] != prepares[-1]), "native authoritative first-NO preparation")
        require(all(not raw_bytes(r["CDC"]) and not r["Original"]["Commit"] for r in records), "native ABORT carried committed effects")
    stats["originalBindingsSHA256"],stats["parallelAttempts"] = bindings.hexdigest(),attempts


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


def fdb_group(group, spec, ordinal, config, physical, generations, sequences, definitions, *, parallel=False):
    fields(group, "Tx Outcome Writes")
    tx, outcome = group["Tx"], group["Outcome"]
    fields(tx, "Graph Topology ID Request Coordinator Dependency Participants GraphChange Lifecycle")
    fields(outcome, "Commit Round Digest ID Request Reason Coordinator Sequence")
    for name in ("Topology", "Coordinator", "Dependency"):
        integer(tx[name])
    for name in ("Round", "Coordinator", "Sequence"):
        integer(outcome[name])
    name, owner = operation(spec, ordinal)
    require(tx["Graph"] == config["Graph"] and tx["Topology"] == config["Topology"] and (parallel or tx["ID"] == name) and tx["Request"] == "request/" + tx["ID"] and tx["Coordinator"] == owner and tx["Dependency"] == 0, "FDB original request/namespace/order")
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
        transactions = result["Parallel"]["Final"]["Metadata"][g]["Transactions"] if "Parallel" in result else metadata["Groups"]
        require(metadata["Floor"] == header["FinalCut"]["Round"] and metadata["Groups"] == header["FinalCut"]["Groups"][g] and metadata["Transactions"] == transactions, "quiescent native metadata")
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


def parallel_operation(spec, value):
    fields(value, "Dataset Phase Fact Ordinal")
    ordinal = integer(value["Ordinal"], 1, 3 + 2 * spec["facts"])
    phase, fact, _, _ = fixture(spec, ordinal)
    dataset = hashlib.sha256(bytes([spec["version"]]) + struct.pack(">QQ", spec["facts"], spec["crossPercent"]) + b"graph-v2").digest()
    require(same(value, {"Dataset": list(dataset), "Phase": phase, "Fact": fact, "Ordinal": ordinal}), "actual operation/dataset/provenance identity")
    return ordinal


def parallel_lines(path, maximum):
    with Path(path).open("rb") as source:
        for number in range(maximum):
            line = source.readline(ARTIFACT_LINE_BYTES + 1)
            if not line:
                return
            require(len(line) <= ARTIFACT_LINE_BYTES and line.endswith(b"\n"), "parallel line admission/termination")
            row = json.loads(line, object_pairs_hook=pairs, parse_constant=lambda _: require(False, "nonfinite parallel JSON"))
            require(type(row) is dict and line == go_json(row) + b"\n", "canonical parallel artifact")
            yield row, line
        require(not source.read(1), "parallel artifact row quota")


def fdb_parallel_attempts(path, result, spool):
    """All originals, including ABORT pairs, are raw native SDK observations."""
    spec, config, end = result["spec"], result["Config"], result["Parallel"]
    fields(end, "Format Binding BindingRecord Old Final Attempts AbortedAttempts SubmitInvocations SDKCallbacks ReadCallbacks ClientCompletions PayloadSHA256")
    for key in ("Attempts","AbortedAttempts","SubmitInvocations","SDKCallbacks","ReadCallbacks"): integer(end[key])
    expected = {"Version": 1, "Config": config, "Dataset": list(hashlib.sha256(bytes([spec["version"]]) + struct.pack(">QQ", spec["facts"], spec["crossPercent"]) + b"graph-v2").digest()),
                "Window": 32, "MaxAttempts": 64, "MaxSubmissions": 8, "MaxCallbacks": 16, "TransactionMillis": 3000, "OperationMillis": 30000}
    require(end["Format"] == PARALLEL_FORMAT and same(end["Binding"], expected), "fresh persisted parallel attempt policy")
    raw_binding(end["BindingRecord"], fdb_key(config, 0, "cost-parallel", "binding"), expected)
    counters = {n: 0 for n in ("Attempts", "AbortedAttempts", "SubmitInvocations", "SDKCallbacks", "ReadCallbacks")}
    clients, commits = [0] * 32, 0
    joined = StoredState(spool, b"fdb-attempt-join")
    abort_bytes, abort_counts = {"old": [0, 0], "final": [0, 0]}, {"old": [0, 0], "final": [0, 0]}
    payload, header_seen, footer_seen, barriers = hashlib.sha256(), False, False, 0
    total = 3 + 2 * spec["facts"]
    for row, line in parallel_lines(path, 7 + 3 + 2 * spec["facts"] * 64):
        kind = row.get("Type")
        require(not footer_seen, "extra parallel attempt after EOF")
        if kind == "footer":
            fields(row, "Type End")
            require(same(row["End"], end) and payload.hexdigest() == end["PayloadSHA256"] and barriers == 2, "parallel result/footer/EOF seal")
            footer_seen = True
            continue
        payload.update(line)
        if not header_seen:
            fields(row, "Type Format Binding Record")
            require(kind == "header" and row["Format"] == PARALLEL_FORMAT and same(row["Binding"], expected) and same(row["Record"], end["BindingRecord"]), "parallel attempt header/binding")
            header_seen = True
            continue
        if kind == "barrier":
            fields(row, "Type Phase Barrier")
            name = ("Old", "Final")[barriers] if barriers < 2 else ""
            require(name and row["Phase"] == ("create" if name == "Old" else "update") and same(row["Barrier"], end[name]), "ordered actual parallel barrier")
            b = row["Barrier"]
            fields(b, "Cut Metadata Records Attempts AbortedAttempts")
            integer(b["Attempts"]); integer(b["AbortedAttempts"])
            require(same(b["Cut"], result["OldCut" if name == "Old" else "FinalCut"]) and b["Attempts"] == counters["Attempts"] and b["AbortedAttempts"] == counters["AbortedAttempts"] and commits == (3 + spec["facts"] if name == "Old" else total), "barrier drained authentic attempts/useful commits")
            require(len(b["Metadata"]) == len(b["Records"]) == 2, "barrier metadata inventory")
            for g, m in enumerate(b["Metadata"]):
                fields(m, "Floor Generation Versions Transactions Certificates Bytes Groups")
                for value in m.values(): integer(value)
                raw_binding(b["Records"][g], fdb_key(config, g, "meta", "state"), m)
                require(m["Floor"] == b["Cut"]["Round"] and m["Groups"] == b["Cut"]["Groups"][g] and m["Transactions"] == m["Groups"] + abort_counts[name.lower()][g], "barrier authentic paired ABORT transaction census")
            barriers += 1
            continue
        fields(row, "Type Attempt")
        require(kind == "attempt", "unknown parallel attempt row")
        a = row["Attempt"]
        fields(a, "Operation Engine ClientID AttemptOrdinal PreviousRequest Request Transaction BodySHA256 Proposal Submissions ReadCallbacks Classification Error Originals Original")
        ordinal = parallel_operation(spec, a["Operation"])
        phase, fact, node, _ = fixture(spec, ordinal)
        require((barriers == 0 and phase in ("setup", "create")) or (barriers == 1 and phase == "update"), "attempt crossed drained old/final barrier")
        client, number = integer(a["ClientID"], 0, spec["writers"] - 1), integer(a["AttemptOrdinal"], 1, 1 if node else 64)
        prior = joined.get((ordinal,))
        require((prior is None and number == 1 and a["PreviousRequest"] == "") or prior is not None and prior["Classification"] == "aborted" and number == prior["AttemptOrdinal"] + 1 and client == prior["ClientID"] and a["PreviousRequest"] == prior["Request"], "fresh request requires exact original ABORT lineage")
        body = raw_bytes(a["Proposal"])
        tx = decoded_body(body)
        fields(tx, "Graph Topology ID Request Coordinator Dependency Participants GraphChange Lifecycle")
        for key in ("Topology","Coordinator","Dependency"): integer(tx[key])
        name, owner = operation(spec, ordinal)
        name += f"-c{client}-a{number}"
        command, lifecycle, effects, reads = fdb_contract(spec, ordinal)
        require(a["Engine"] == "foundationdb" and a["Error"] == "" and a["Transaction"] == tx["ID"] == name and a["Request"] == tx["Request"] == "request/" + name and sha_array(a["BodySHA256"]) == hashlib.sha256(body).digest(), "frozen actual parallel ID/request/body")
        require(tx["Graph"] == config["Graph"] and tx["Topology"] == config["Topology"] and tx["Coordinator"] == owner and tx["Dependency"] == 0 and same(tx["GraphChange"], command) and same(tx["Lifecycle"], lifecycle), "parallel actual command/config/provenance")
        require([p["Group"] for p in tx["Participants"]] == sorted(effects), "parallel attempt complete participants")
        for p in tx["Participants"]:
            fields(p, "Group Epoch Generation Reads Effects")
            g = integer(p["Group"], 0, 1)
            require(p["Epoch"] == config["Epochs"][g] and integer(p["Generation"]) <= end["Final"]["Metadata"][g]["Generation"] and same(p["Effects"], effects[g]) and [r["Key"] for r in p["Reads"]] == reads[g], "parallel immutable participant/read/effect shape")
            for r in p["Reads"]:
                fields(r, "Key Version"); integer(r["Version"])
        require(type(a["Originals"]) is list and len(a["Originals"]) == 2, "complete retained ID/request pair")
        o = decoded_body(raw_bytes(a["Originals"][0]["Value"]))
        fields(o, "Commit Round Digest ID Request Reason Coordinator Sequence")
        for key in ("Round","Coordinator","Sequence"): integer(o[key])
        require(type(o["Commit"]) is bool and o["ID"] == name and o["Request"] == a["Request"] and o["Coordinator"] == owner and sha_array(o["Digest"]) == hashlib.sha256(body).digest(), "retained attempt original identity/digest")
        for record, kind in zip(a["Originals"], ("id", "request")):
            raw_binding(record, fdb_key(config, owner, kind, o["ID"] if kind == "id" else o["Request"]), o)
        for kind, record in zip((b"fdb-attempt-id", b"fdb-attempt-request"), a["Originals"]):
            spool.insert(kind, raw_bytes(record["Key"]), raw_bytes(record["Value"]))
        require(type(a["Submissions"]) is list and 1 <= len(a["Submissions"]) <= 8, "finite SAME-wire submissions")
        for invocation, s in enumerate(a["Submissions"], 1):
            fields(s, "Ordinal Callbacks Outcome Error Retryable MaybeCommitted")
            require(type(s["Ordinal"]) is int and s["Ordinal"] == invocation and type(s["Error"]) is str and type(s["Retryable"]) is type(s["MaybeCommitted"]) is bool, "actual SAME-wire submission identity/classification")
            counters["SubmitInvocations"] += 1
            counters["SDKCallbacks"] += integer(s["Callbacks"], 1, 16)
            if s["Error"]:
                # A successful original may resolve ambiguity, but a later
                # outer resubmission may only follow a retryable/unknown call.
                if invocation < len(a["Submissions"]): require(s["Retryable"] or s["MaybeCommitted"], "definite SDK failure retried")
            else:
                require(same(s["Outcome"], o) and not s["Retryable"] and not s["MaybeCommitted"] and invocation == len(a["Submissions"]), "actual submission/original receipt")
        counters["ReadCallbacks"] += integer(a["ReadCallbacks"], 2, 16 * 9)
        counters["Attempts"] += 1
        if o["Commit"]:
            require(a["Classification"] == "committed" and o["Reason"] == "" and integer(o["Round"], 1) and integer(o["Sequence"], 1), "genuine useful COMMIT")
            fields(a["Original"], "Key Value")
            group = decoded_body(raw_bytes(a["Original"]["Value"]))
            require(same(group["Tx"], tx) and same(group["Outcome"], o), "complete committed original attempt")
            key = (f"g{owner}/{config['Graph'].encode().hex()}/changes/{o['Sequence']:016x}/" + o["Request"].encode().hex()).encode()
            raw_binding(a["Original"], key, group)
            spool.insert(b"fdb-useful-attempt", u64(ordinal), go_json(a))
            commits += 1
            if phase != "setup": clients[client] += 1
        else:
            require(a["Classification"] == "aborted" and o["Reason"] in ("generation", "revision") and o["Round"] == o["Sequence"] == 0 and same(a["Original"], {"Key": None, "Value": None}), "authenticated ABORT without committed CDC")
            counters["AbortedAttempts"] += 1
            size = sum(len(raw_bytes(r["Key"])) + len(raw_bytes(r["Value"])) for r in a["Originals"])
            abort_counts["final"][owner] += 1; abort_bytes["final"][owner] += size
            if phase != "update": abort_counts["old"][owner] += 1; abort_bytes["old"][owner] += size
        joined[(ordinal,)] = {k: a[k] for k in ("ClientID", "AttemptOrdinal", "Request", "Classification")}
    require(header_seen and footer_seen and all(end[k] == v for k, v in counters.items()) and same(end["ClientCompletions"], clients) and commits == total and end["Attempts"] == total + end["AbortedAttempts"], "complete authentic parallel attempt/result counts")
    for ordinal in range(1, total + 1): require(joined.get((ordinal,), {}).get("Classification") == "committed", "logical operation lacks exactly one useful COMMIT")
    for client in range(spec["writers"]): require(spec["facts"] <= spec["writers"] or clients[client] > 0, "declared independent client unused")
    return abort_bytes, abort_counts


def fdb_raw_groups(path, spec, config, final_cut, stats, spool, result):
    parallel = "Parallel" in result
    abort_bytes, abort_counts = fdb_parallel_attempts(Path(path).with_name("fdb-cost-attempts.jsonl"), result, spool) if parallel else ({"old": [0, 0], "final": [0, 0]}, {"old": [0, 0], "final": [0, 0]})
    if parallel:
        stats["parallelAttempts"] = {k: result["Parallel"][k] for k in ("Attempts", "AbortedAttempts", "SubmitInvocations", "SDKCallbacks", "ReadCallbacks", "ClientCompletions")}
        spool.insert(b"fdb-abort-metadata", b"old", go_json({"Bytes": abort_bytes["old"], "Transactions": abort_counts["old"]}))
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
    if parallel:
        record = result["Parallel"]["BindingRecord"]
        native_bytes[0] += len(raw_bytes(record["Key"])) + len(raw_bytes(record["Value"]))
        for g in range(2): native_bytes[g] += abort_bytes["final"][g]
    pending, ordinal_next, previous_order = {}, 1, None
    bindings = hashlib.sha256()
    total = 3 + 2 * spec["facts"]
    for raw in reader:
        group = decoded_body(raw["Value"])
        ordinal = integer(group["Tx"]["Lifecycle"]["Provenance"], 1, total)
        if not parallel: require(ordinal <= max(3, ordinal_next) and ordinal not in pending, "writer1 bounded setup/execution identity order")
        canonical = fdb_group(group, spec, ordinal, config, physical, generations, sequences, definitions, parallel=parallel)
        if parallel:
            actual_attempt = decoded_body(spool.lookup(b"fdb-useful-attempt", u64(ordinal)))
            require(raw["Value"] == raw_bytes(actual_attempt["Original"]["Value"]) and same(group["Tx"], decoded_body(raw_bytes(actual_attempt["Proposal"]))), "raw CDC is not genuine useful attempt original")
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
        if parallel:
            spool.insert(b"fdb-canonical", u64(ordinal), go_json(canonical))
        else:
            pending[ordinal] = canonical
            require(len(pending) <= 3, "fixed writer1 setup buffer")
            while ordinal_next in pending:
                yield pending.pop(ordinal_next)
                ordinal_next += 1
    require(not pending and (parallel or ordinal_next == total + 1) and sequences == final_cut["Groups"], "incomplete raw original operations/watermarks")
    for g, metadata in enumerate(header["Metadata"]):
        require(metadata["Versions"] == versions[g] and metadata["Generation"] == generations[g] and metadata["Transactions"] == sequences[g] + abort_counts["final"][g] and metadata["Certificates"] == (3 if g == 0 else 0) and metadata["Bytes"] == native_bytes[g], "exact retained metadata census including authentic ABORT pairs")
    if parallel: require(same(header["Metadata"], result["Parallel"]["Final"]["Metadata"]), "raw final metadata differs from drained authentic attempt barrier")
    operation_count = 0
    for operation_count, group in enumerate(rows(Path(path).with_name("fdb-cost-operation-groups.jsonl"), total), 1):
        ordinal = integer(group["Tx"]["Lifecycle"]["Provenance"], 1, total) if parallel else operation_count
        require(go_json(group) == spool.lookup(b"fdb-operation", u64(ordinal)), "per-operation original execution order/binding")
        if parallel: spool.insert(b"fdb-operation-output", u64(ordinal), go_json(group))
    require(operation_count == total, "incomplete per-operation original file")
    samples = 0
    for samples, sample in enumerate(rows(Path(path).with_name("fdb-cost-latency.jsonl"), total), 1):
        fields(sample, "phase fact cross ProposalNanos AckNanos VerificationNanos ProposalAttempts CommitAttempts Outcome" + (" Operation ClientID" if parallel else ""))
        ordinal = parallel_operation(spec, sample["Operation"]) if parallel else samples
        phase, fact, node, edge = fixture(spec, ordinal)
        require(sample["phase"] == phase and type(sample["fact"]) is int and sample["fact"] == fact and type(sample["cross"]) is bool and sample["cross"] == bool(edge and edge["Target"]["Owner"]), "per-request timing identity")
        for name in ("ProposalNanos", "AckNanos", "VerificationNanos"):
            integer(sample[name], 0, (1 << 63) - 1)
        for name in ("ProposalAttempts", "CommitAttempts"):
            integer(sample[name], 1, (1 << 31) - 1)
        name, owner = operation(spec, ordinal)
        if parallel:
            attempt = decoded_body(spool.lookup(b"fdb-useful-attempt", u64(ordinal)))
            require(sample["ClientID"] == attempt["ClientID"], "useful timing client binding")
            name = attempt["Transaction"]
            spool.insert(b"fdb-sample-output", u64(ordinal), go_json(sample))
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
    stats["orderContract"] = ("raw (Round,Coordinator,Request) and operation completion order unchanged; canonical parity joins exact operation identities in private spool; actual per-key predecessors and phase barriers validated" if parallel else "raw (Round,Coordinator,Request) unchanged; execution originals provenance1,2,3 bind independent setup rounds through fixed three-entry buffer")
    if parallel:
        for ordinal in range(1,total+1):
            spool.lookup(b"fdb-operation-output",u64(ordinal)); spool.lookup(b"fdb-sample-output",u64(ordinal))
            yield json.loads(spool.lookup(b"fdb-canonical",u64(ordinal)), object_pairs_hook=pairs)


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
                original_tx = decoded_body(spool.lookup(b"fdb-operation", u64(ordinal)))["Tx"]["ID"]
                require(raw == go_json(physical) and physical["Value"] == value and physical["Version"] == (1 if value == 5 else 2) and physical["TxID"] == original_tx, "retained raw edge/out/in correspondence")
            for node in (edge["Source"], edge["Target"]):
                logical = f"node/{node['Node']:016x}/life/{node['Life']:016x}"
                alive = decoded_body(spool.lookup(namespace, fdb_key(config, node["Owner"], "current", logical)))
                require(alive["Value"] == alive["Version"] == 1 and alive["Deleted"] is False, "retained endpoint life")
            digest.update(go_json({"id": identity, "source": edge["Source"]["Node"], "target": edge["Target"]["Node"], "life": life, "value": value, "operationProvenance": ordinal}) + b"\n")
    require(next(source, None) is None, "extra native retained point")
    return digest.hexdigest()


def sha_array(value):
    require(type(value) is list and len(value) == 32 and all(type(v) is int and 0 <= v <= 255 for v in value), "SHA256 byte array")
    return bytes(value)


def column_typed_row(row, spool):
    fields(row, "Kind Owner Life SourceNode SourceLife TargetNode TargetLife Value ValueID Provenance Round Sequence Coordinator Original")
    original = raw_bytes(row["Original"])
    require(0 < len(original) <= LINE_BYTES, "column original admitted before decode")
    group = decoded_body(original)
    ordinal = integer(group["Tx"]["Lifecycle"]["Provenance"], 1)
    require(original == spool.lookup(b"fdb-operation", u64(ordinal)), "column original is not exact source CDC/request/history lineage")
    command, lifecycle, outcome = group["Tx"]["GraphChange"], group["Tx"]["Lifecycle"], group["Outcome"]
    node, edge = command["Node"], command["Edge"]
    expected = {"Kind": 1 if command["Kind"] == "node-create" else 2 if command["Kind"] == "edge-create" else 3,
                "Owner": node["Node"] if command["Kind"] == "node-create" else edge["ID"],
                "Life": node["Life"] if command["Kind"] == "node-create" else lifecycle["EdgeLife"],
                "SourceNode": edge["Source"]["Node"], "SourceLife": edge["Source"]["Life"],
                "TargetNode": edge["Target"]["Node"], "TargetLife": edge["Target"]["Life"],
                "Value": 1 if command["Kind"] == "node-create" else edge["Value"],
                "ValueID": 0 if command["Kind"] == "node-create" else lifecycle["Definition"]["ID"],
                "Provenance": ordinal, "Round": outcome["Round"], "Sequence": outcome["Sequence"],
                "Coordinator": outcome["Coordinator"], "Original": row["Original"]}
    require(same(row, expected), "typed column differs from actual scalar/endpoint/life/definition/axis original")
    return group


def column_chunk(body, generation, binding, ordinal, chain, policy, spool):
    require(114 <= len(body) <= policy["ChunkBytes"], "binary chunk admitted byte bound")
    require(body[:8] == b"RHOCLM01" and body[8:40] == binding and body[40:72] == hashlib.sha256(go_json(generation)).digest()
            and body[72:80] == u64(ordinal) and body[80:112] == chain, "immutable chunk generation/ordinal/hash chain")
    count = struct.unpack_from(">H", body, 112)[0]
    require(1 <= count <= policy["ChunkRows"] <= 32, "binary typed chunk rows")
    arena = 114 + count * 98
    require(arena <= len(body), "binary typed chunk columns truncated")
    names = ("Owner", "Life", "SourceNode", "SourceLife", "TargetNode", "TargetLife", "Value", "ValueID", "Provenance", "Round", "Sequence")
    offset = 0
    for row in range(count):
        values = {name: struct.unpack_from(">Q", body, 114 + 2 * count + (column * count + row) * 8)[0]
                  for column, name in enumerate(names)}
        if values["Value"] >= 1 << 63:
            values["Value"] -= 1 << 64
        spans = 114 + 90 * count
        start = struct.unpack_from(">I", body, spans + 4 * row)[0]
        length = struct.unpack_from(">I", body, spans + 4 * count + 4 * row)[0]
        require(start == offset and 0 < length <= len(body) - arena - offset, "canonical original arena spans")
        original = body[arena + offset:arena + offset + length]
        decoded = {"Kind": body[114 + row], **values, "Coordinator": body[114 + count + row],
                   "Original": base64.b64encode(original).decode()}
        group = column_typed_row(decoded, spool)
        require(generation["Initial"]["Round"] < decoded["Round"] <= generation["Cut"]["Round"] and decoded["Sequence"] <= generation["Cut"]["Groups"][decoded["Coordinator"]], "column outside authenticated generation cut")
        yield decoded, group
        offset += length
    require(arena + offset == len(body), "binary typed chunk trailing/missing bytes")


def column_old_metadata(result, spool):
    config = result["Config"]
    maximum = {k: (1 << 64) - 1 for k in ("Floor", "Generation", "Versions", "Transactions", "Certificates", "Bytes", "Groups")}
    sizes = [len(fdb_key(config, g, "meta", "config")) + len(go_json(config)) + len(fdb_key(config, g, "meta", "state")) + len(go_json(maximum)) for g in range(2)]
    for name in ("InitialCut", "OldCut"):
        cut = result[name]
        sizes[0] += len(fdb_key(config, 0, "cut", go_json(cut).hex())) + len(go_json(cut))
    abort_counts = [0, 0]
    if "Parallel" in result:
        record = result["Parallel"]["BindingRecord"]
        sizes[0] += len(raw_bytes(record["Key"])) + len(raw_bytes(record["Value"]))
        aborts = decoded_body(spool.lookup(b"fdb-abort-metadata", b"old"))
        abort_counts = aborts["Transactions"]
        for g in range(2): sizes[g] += aborts["Bytes"][g]
    generations, versions = [0, 0], [0, 0]
    for ordinal in range(1, 4 + result["spec"]["facts"]):
        raw = spool.lookup(b"fdb-operation", u64(ordinal))
        group = decoded_body(raw)
        o = group["Outcome"]
        owner = o["Coordinator"]
        key = (f"g{owner}/{config['Graph'].encode().hex()}/changes/{o['Sequence']:016x}/" + o["Request"].encode().hex()).encode()
        sizes[owner] += len(key) + len(raw) + len(fdb_key(config, owner, "id", o["ID"])) + len(fdb_key(config, owner, "request", o["Request"])) + 2 * len(go_json(o))
        for participant in group["Tx"]["Participants"]:
            generations[participant["Group"]] += 1
        for write in group["Writes"]:
            g, value = write["Group"], write["After"]
            key = (f"g{g}/{config['Graph'].encode().hex()}/history/{value['Round']:016x}/" + write["Key"].encode().hex()).encode()
            versions[g] += 1
            sizes[g] += len(key) + len(go_json(value))
    for key, value in spool.walk(b"fdb-old"):
        g = int(chr(key[1]))
        sizes[g] += len(key) + len(value)
    return [{"Floor": result["OldCut"]["Round"], "Generation": generations[g], "Versions": versions[g],
             "Transactions": result["OldCut"]["Groups"][g] + abort_counts[g], "Certificates": 2 if g == 0 else 0,
             "Bytes": sizes[g], "Groups": result["OldCut"]["Groups"][g]} for g in range(2)]


def column_work(group, config):
    o = group["Outcome"]
    owner = o["Coordinator"]
    change_key = (f"g{owner}/{config['Graph'].encode().hex()}/changes/{o['Sequence']:016x}/" + o["Request"].encode().hex()).encode()
    work = len(change_key) + len(go_json(group)) + len(fdb_key(config, owner, "id", o["ID"])) + len(fdb_key(config, owner, "request", o["Request"])) + 2 * len(go_json(o))
    for write in group["Writes"]:
        for value in (write.get("Before"), write["After"]):
            if value is not None:
                key = (f"g{write['Group']}/{config['Graph'].encode().hex()}/history/{value['Round']:016x}/" + write["Key"].encode().hex()).encode()
                work += len(key) + len(go_json(value))
    return work


def column_artifacts(path, result, spool):
    """Validate actual sealed derived records; SQLite remains private verifier state."""
    end, config, spec = result["Columns"], result["Config"], result["spec"]
    fields(end, "Format SourceFormat ReferenceLayout PayloadSHA256 ArtifactSHA256 Old Final Counts Ledger PinLedger Storage")
    require(end["Format"] == COLUMN_FORMAT and end["SourceFormat"] == RAW_FORMAT and end["ReferenceLayout"] == "native-logical-kv-reference", "column format/reference identity")
    prefix = f"g0/{config['Graph'].encode().hex()}/columns-v1/".encode()
    key = lambda name: prefix + name.encode()
    generation_key = lambda name, kind: key("generation/" + name.encode().hex() + "/" + kind)
    retention = lambda kind, name: key("retention-v1/" + kind + "/" + name.encode().hex())
    policy = {"ChunkRows": 32, "ChunkBytes": 65536, "MaxChunks": 1_000_000, "MaxGenerations": 2,
              "MaxRetainedBytes": (64 << 20) + (6 + 3 * spec["facts"]) * (64 << 10),
              "MaxWorkRows": 3 + 2 * spec["facts"], "MaxWorkBytes": (64 << 20) + (3 + 2 * spec["facts"]) * (512 << 10)}
    binding = {"Version": 1, "Layout": COLUMN_LAYOUT, "Schema": "node-presence;edge-life-bound;property-p:i64;canonical-original:v1", "Config": config, "Scope": SCOPE, "Policy": policy}
    binding_sha = hashlib.sha256(go_json(binding)).digest()
    pin_policy = {"MaxRecords": 32, "MaxBytes": 256 << 10, "PageChunks": 1, "PageBytes": 128 << 10, "MaxWorkChunks": 1, "MaxWorkBytes": 128 << 10, "MaxOutputBytes": 1024}
    pin_binding = {"Version": 1, "Columns": list(binding_sha), "Policy": pin_policy}
    manifests = {"old": end["Old"], "final": end["Final"]}  # Exactly two, never dataset-sized.
    old_metadata = column_old_metadata(result, spool)
    if "Parallel" in result: require(same(old_metadata, result["Parallel"]["Old"]["Metadata"]), "column old metadata omits authentic retained attempt/binding bytes")
    for name, m in manifests.items():
        fields(m, "Generation Metadata Chunks Rows Bytes History Chain")
        g = m["Generation"]
        fields(g, "Version ID Binding Initial Cut")
        require(same(g, {"Version": 1, "ID": name, "Binding": list(binding_sha), "Initial": result["InitialCut"], "Cut": result["OldCut" if name == "old" else "FinalCut"]}), "immutable column generation/config/cut binding")
        metadata = old_metadata if name == "old" else result["StreamEnds"][0]["Metadata"]
        require(same(m["Metadata"], metadata) and same(m["History"], [v["Versions"] for v in metadata]), "complete actual source history census in manifest")
        require(integer(m["Rows"], 1, policy["MaxWorkRows"]) == sum(g["Cut"]["Groups"]) and 1 <= integer(m["Chunks"]) <= min(m["Rows"], policy["MaxChunks"]) and integer(m["Bytes"], 1) <= policy["MaxRetainedBytes"] and any(sha_array(m["Chain"])), "column manifest quotas/census")
    counters = {"Chunks": 0, "WalkRows": 0, "Points": 0, "Stages": 0, "Collections": 0}
    chunk_next, chunk_rows, chunk_bytes, works = ({"old": 0, "final": 0} for _ in range(4))
    chains = {"old": bytes(32), "final": bytes(32)}
    sequences, histories = {"old": [0, 0], "final": [0, 0]}, {"old": [0, 0], "final": [0, 0]}
    last_order, last_outcome = {"old": None, "final": None}, {}
    walk_counts = {"old": 0, "old-cache": 0, "final": 0, "final-cache": 0}
    chunk_eofs, walk_eofs, points, refusals = set(), set(), {"old": 0, "final": 0}, []
    stages = ("old-held", "final-head", "expired-held", "reads-complete", "old-closed", "old-collected", "terminal")
    held, token = None, None
    collected_bytes, collected_chain = 0, bytes(32)
    storage = {n: 0 for n in "Records Bytes LiveChunkRecords LiveChunkBytes TombstoneRecords TombstoneBytes RetiredLineageRecords RetiredLineageBytes".split()}
    storage_previous, reader_counts = None, {"old": 0, "final": 0}
    payload, whole, footer_seen = hashlib.sha256(), hashlib.sha256(), False
    maximum = 4 * (end["Old"]["Rows"] + end["Final"]["Rows"]) + 256
    with Path(path).open("rb") as source:
        for number in range(maximum):
            line = source.readline(ARTIFACT_LINE_BYTES + 1)
            if not line:
                break
            require(len(line) <= ARTIFACT_LINE_BYTES and line.endswith(b"\n") and not footer_seen, "column artifact line/EOF bound")
            row = json.loads(line, object_pairs_hook=pairs, parse_constant=lambda _: require(False, "nonfinite column JSON"))
            require(type(row) is dict and line == go_json(row) + b"\n", "canonical column artifact record")
            whole.update(line)
            kind = row.get("Type")
            if kind == "footer":
                fields(row, "Type End")
                expected = end | {"ArtifactSHA256": [0] * 32}
                require(same(row["End"], expected) and sha_array(end["PayloadSHA256"]) == payload.digest(), "column footer/result/payload seal")
                footer_seen = True
                continue
            payload.update(line)
            if number == 0:
                fields(row, "Type Format SourceFormat ReferenceLayout Config InitialCut StoragePagePolicy Records")
                require(kind == "header" and row["Format"] == COLUMN_FORMAT and row["SourceFormat"] == RAW_FORMAT and row["ReferenceLayout"] == end["ReferenceLayout"] and same(row["Config"], config) and same(row["InitialCut"], result["InitialCut"]), "column header namespace/layout")
                require(same(row["StoragePagePolicy"], {"PageRows": 32, "PageBytes": 32 * (65536 + 1024)}), "declared bounded post-SDK storage page policy")
                records = column_records(row["Records"])
                required = {key("binding"): go_json(binding), key("retention-v1/binding"): go_json(pin_binding),
                            fdb_key(config, 0, "meta", "config"): go_json(config), fdb_key(config, 1, "meta", "config"): go_json(config),
                            fdb_key(config, 0, "cut", go_json(result["InitialCut"]).hex()): go_json(result["InitialCut"])}
                require(records == required, "column raw persisted binding/config/cut header")
                for k, value in records.items():
                    if k.startswith(prefix):
                        spool.insert(b"column-control", k, value)
            elif kind == "stage":
                fields(row, "Type Name Records")
                index = counters["Stages"]
                require(index < len(stages) and row["Name"] == stages[index], "column lifecycle stage order")
                held, token = column_stage(row, index, result, manifests, binding, pin_binding, held, token, spool)
                counters["Stages"] += 1
            elif kind == "chunk":
                fields(row, "Type Generation CacheDirectory CacheFile Ordinal Length Rows SHA256 Record")
                name = row["Generation"]
                require(type(name) is str and name in manifests and name not in chunk_eofs and type(row["Ordinal"]) is int and row["Ordinal"] == chunk_next[name] and counters["Stages"] == 3, "column chunk scope/order")
                m, ordinal = manifests[name], chunk_next[name]
                records = column_records([row["Record"]])
                k = generation_key(name, f"chunk/{ordinal:016x}")
                require(set(records) == {k}, "raw chunk key/generation/ordinal")
                body = records[k]
                require(row["Length"] == integer(len(body)) and sha_array(row["SHA256"]) == hashlib.sha256(body).digest(), "actual binary chunk hash/length")
                count = 0
                for typed, original in column_chunk(body, m["Generation"], binding_sha, ordinal, chains[name], policy, spool):
                    o = original["Outcome"]
                    order = o["Round"], o["Coordinator"], o["Request"]
                    require((last_order[name] is None or last_order[name] < order) and typed["Sequence"] == sequences[name][typed["Coordinator"]] + 1, "column original sequence/enumeration")
                    last_order[name], last_outcome[name] = order, o
                    sequences[name][typed["Coordinator"]] += 1
                    for write in original["Writes"]:
                        histories[name][write["Group"]] += 1
                    spool.insert(b"column-original/" + name.encode(), u64(typed["Provenance"]), go_json(typed))
                    spool.insert(b"column-row/" + name.encode(), u64(chunk_rows[name] + count), go_json(typed))
                    works[name] += column_work(original, config)
                    count += 1
                require(type(row["Rows"]) is int and row["Rows"] == count, "binary chunk actual row count")
                cache_name = "column-" + hashlib.sha256(go_json(m["Generation"])).hexdigest() + f"-{ordinal:016x}.bin"
                require(row["CacheDirectory"] == "column-cache-" + name and row["CacheFile"] == cache_name, "cache immutable generation filename")
                cache_path = Path(path).parent / row["CacheDirectory"] / cache_name
                require(not cache_path.is_symlink() and not cache_path.parent.is_symlink(), "foreign cache link")
                with cache_path.open("rb") as cache:
                    require(cache.read(policy["ChunkBytes"] + 1) == body, "missing/foreign/partial actual chunk cache")
                spool.insert(b"column-cache/" + name.encode(), cache_name.encode(), u64(ordinal))
                spool.insert(b"column-chunk/" + name.encode(), u64(ordinal), body)
                chains[name] = hashlib.sha256(chains[name] + hashlib.sha256(body).digest()).digest()
                works[name] += len(body)
                chunk_rows[name] += count
                chunk_bytes[name] += len(k) + len(body)
                chunk_next[name] += 1
                counters["Chunks"] += 1
            elif kind == "chunks-eof":
                fields(row, "Type Generation Manifest")
                name = row["Generation"]
                require(type(name) is str and name in manifests and name not in chunk_eofs and same(row["Manifest"], manifests[name]), "column chunk EOF identity")
                m = manifests[name]
                require(chunk_next[name] == m["Chunks"] and chunk_rows[name] == m["Rows"] and chunk_bytes[name] == m["Bytes"] and chains[name] == sha_array(m["Chain"]) and sequences[name] == m["Generation"]["Cut"]["Groups"] and histories[name] == m["History"] and works[name] <= policy["MaxWorkBytes"], "complete binary generation original/history EOF")
                chunk_eofs.add(name)
            elif kind == "walk-row":
                fields(row, "Type Name Generation Row")
                name, generation = row["Name"], row["Generation"]
                require(type(name) is str and name in walk_counts and name not in walk_eofs and type(generation) is str and generation == name.removesuffix("-cache") and generation in chunk_eofs, "column actual walk identity/EOF")
                require(go_json(row["Row"]) == spool.lookup(b"column-row/" + generation.encode(), u64(walk_counts[name])), "typed walk/cache changed actual chunk rows/order")
                walk_counts[name] += 1
                counters["WalkRows"] += 1
            elif kind == "walk-eof":
                fields(row, "Type Name Generation Cut Rows Manifest")
                name, generation = row["Name"], row["Generation"]
                require(type(name) is str and name in walk_counts and name not in walk_eofs and type(generation) is str and generation == name.removesuffix("-cache") and same(row["Manifest"], manifests[generation]) and same(row["Cut"], manifests[generation]["Generation"]["Cut"]) and type(row["Rows"]) is int and row["Rows"] == walk_counts[name] == manifests[generation]["Rows"], "complete typed/cached walk EOF")
                walk_eofs.add(name)
            elif kind == "point":
                fields(row, "Type Generation Owner Found Row")
                name = row["Generation"]
                require(type(name) is str and name in points and name + "-cache" in walk_eofs and points[name] < 3, "column selected point scope")
                owner = (7, 7 + 2 * (spec["facts"] - 1), 5)[points[name]]
                require(type(row["Owner"]) is int and row["Owner"] == owner and type(row["Found"]) is bool and row["Found"] == (owner != 5), "column point/phantom identity")
                if owner == 5:
                    require(same(row["Row"], {n: None if n == "Original" else 0 for n in "Kind Owner Life SourceNode SourceLife TargetNode TargetLife Value ValueID Provenance Round Sequence Coordinator Original".split()}), "phantom column point not exact empty")
                else:
                    ordinal = 4 + (owner - 7) // 2 + (spec["facts"] if name == "final" else 0)
                    require(go_json(row["Row"]) == spool.lookup(b"column-original/" + name.encode(), u64(ordinal)), "column retained point substituted original/value/revision")
                points[name] += 1
                counters["Points"] += 1
            elif kind == "refusal":
                fields(row, "Type Name Error")
                expected = (("expired-new-open", "fdb-column: retained generation or token expired", 2),
                            ("held-old-collection", "fdb-column: retained generation has live owners or references", 2),
                            ("retired-publication", "fdb-column: retained generation or token expired", 5),
                            ("final-reference-collection", "fdb-column: retained generation has live owners or references", 6))
                require(len(refusals) < 4 and (row["Name"], row["Error"], counters["Stages"]) == expected[len(refusals)], "explicit expiry/held/reference/retired refusal")
                refusals.append(row["Name"])
            elif kind == "collect":
                fields(row, "Type Budget Result Absent Guard")
                require(counters["Stages"] == 5 and points == {"old": 3, "final": 3}, "collection precedes complete owned reads/close")
                budget, page, ordinal = row["Budget"], row["Result"], counters["Collections"]
                require(same(budget, {"Chunks": 1, "Bytes": 128 << 10, "OutputBytes": 1024}), "collector admitted policy")
                fields(page, "Generation Cursor DeletedBytes WorkChunks WorkBytes Done")
                body = spool.lookup(b"column-chunk/old", u64(ordinal))
                k = generation_key("old", f"chunk/{ordinal:016x}")
                require(raw_bytes(row["Absent"]) == k, "actual absent derived chunk identity")
                used = len(k) + len(body)
                collected_bytes += used
                collected_chain = hashlib.sha256(collected_chain + hashlib.sha256(body).digest()).digest()
                done = ordinal + 1 == end["Old"]["Chunks"]
                require(same(page, {"Generation": "old", "Cursor": ordinal + 1, "DeletedBytes": collected_bytes, "WorkChunks": 1, "WorkBytes": used, "Done": done}) and used <= budget["Bytes"] and len(go_json(page)) <= budget["OutputBytes"], "exact bounded collector cursor/accounting")
                expected_guard = {"Manifest": list(hashlib.sha256(go_json(end["Old"])).digest()), "State": 3 if done else 2, "References": 0, "Pins": 0, "Readers": 0, "Cursor": ordinal + 1, "DeletedBytes": collected_bytes, "Chain": list(collected_chain)}
                raw_binding(row["Guard"], generation_key("old", "owners"), expected_guard)
                counters["Collections"] += 1
            elif kind == "storage-row":
                fields(row, "Type Record")
                require(counters["Stages"] == 7, "terminal storage before closure")
                records = column_records([row["Record"]])
                k, value = next(iter(records.items()))
                require(k.startswith(prefix) and (storage_previous is None or storage_previous < k), "column storage duplicate/foreign/order")
                storage_previous = k
                expected = spool.find(b"column-control", k)
                if expected is not None:
                    require(value == expected, "terminal column control differs from original lineage/stage")
                elif k.startswith(generation_key("final", "chunk/")):
                    ordinal_text = k.rsplit(b"/", 1)[-1]
                    require(len(ordinal_text) == 16 and f"{int(ordinal_text, 16):016x}".encode() == ordinal_text, "terminal chunk coordinate")
                    require(value == spool.lookup(b"column-chunk/final", u64(int(ordinal_text, 16))), "terminal live binary chunk changed")
                else:
                    require(k.startswith(key("read-owner/")), "unexpected retained column key")
                    record = decoded_body(value)
                    column_reader(record, manifests, token, False)
                    require(k == key("read-owner/" + record["Owner"]["Request"].encode().hex()), "retained reader identity key")
                if k.startswith(key("read-owner/")):
                    record = decoded_body(value)
                    column_reader(record, manifests, token, k == held)
                    require(record["State"] == 2, "terminal reader is not a released tombstone")
                    reader_counts[record["Owner"]["Generation"]] += 1
                spool.insert(b"column-storage", k, value)
                size = len(k) + len(value)
                storage["Records"] += 1
                storage["Bytes"] += size
                if k.startswith(generation_key("final", "chunk/")):
                    storage["LiveChunkRecords"] += 1
                    storage["LiveChunkBytes"] += size
                elif k.startswith(generation_key("old", "")):
                    storage["RetiredLineageRecords"] += 1
                    storage["RetiredLineageBytes"] += size
                elif k.startswith(key("read-owner/")) or k == retention("pin", "hold-old"):
                    storage["TombstoneRecords"] += 1
                    storage["TombstoneBytes"] += size
            else:
                require(False, "unknown/misplaced column artifact kind")
        require(footer_seen and not source.read(1) and whole.digest() == sha_array(end["ArtifactSHA256"]), "missing/truncated/extra column artifact seal")
    require(same(counters, end["Counts"]) and counters == {"Chunks": end["Old"]["Chunks"] + end["Final"]["Chunks"], "WalkRows": 2 * (end["Old"]["Rows"] + end["Final"]["Rows"]), "Points": 6, "Stages": 7, "Collections": end["Old"]["Chunks"]} and len(refusals) == 4 and len(chunk_eofs) == 2 and len(walk_eofs) == 4, "complete column lifecycle counts/EOF")
    require(collected_bytes == end["Old"]["Bytes"] and collected_chain == sha_array(end["Old"]["Chain"]), "complete derived generation collection")
    require(same(storage, end["Storage"]) and storage["Records"] == end["Final"]["Chunks"] + 22 and storage["LiveChunkRecords"] == end["Final"]["Chunks"] and storage["LiveChunkBytes"] == end["Final"]["Bytes"] and storage["TombstoneRecords"] == 9 and storage["RetiredLineageRecords"] == 3 and reader_counts == {"old": 4, "final": 4}, "actual live/tombstone/retired namespace byte census")
    for k, value in spool.walk(b"column-control"):
        require(spool.lookup(b"column-storage", k) == value, "missing terminal control/lineage")
    for name in manifests:
        progress = decoded_body(spool.lookup(b"column-storage", generation_key(name, "progress")))
        require(progress["WorkBytes"] == works[name] and same(progress["Previous"], last_outcome[name]), "exact declared publication work/original EOF binding")
        count = 0
        with os.scandir(Path(path).parent / ("column-cache-" + name)) as entries:
            for entry in entries:
                require(not entry.is_symlink() and entry.is_file(follow_symlinks=False) and spool.find(b"column-cache/" + name.encode(), entry.name.encode()) is not None, "unexpected cache file/generation")
                count += 1
        require(count == manifests[name]["Chunks"], "complete cache directory census")
    return {"equal": True, "layout": COLUMN_LAYOUT, "artifactSHA256": whole.hexdigest(), "counts": counters, "storedLogicalBytes": storage,
            "admissionCharges": {"columns": end["Ledger"], "pins": end["PinLedger"]}, "sourceKVGC": "not performed; exact source EOF follows derived collection",
            "referenceLayout": end["ReferenceLayout"], "nativeColumnsCostParity": False, "pointScope": "three selected generation-scan witnesses per cut; complete typed/cached walks; no indexed-point cost claim"}


def column_records(records):
    require(type(records) is list and 1 <= len(records) <= 20, "bounded raw column control inventory")
    out = {}
    for record in records:
        fields(record, "Key Value")
        key, value = raw_bytes(record["Key"]), raw_bytes(record["Value"])
        require(0 < len(key) <= 1024 and 0 < len(value) <= 65536 and key not in out, "column raw record duplicate/admission")
        out[key] = value
    return out


def column_reader(record, manifests, token, pinned):
    fields(record, "Owner Manifest PinRequest PinToken State")
    fields(record["Owner"], "Request Generation Secret")
    owner = record["Owner"]
    secret = sha_array(owner["Secret"])
    require(any(secret) and owner["Request"] == "read-" + secret[:16].hex() and type(owner["Generation"]) is str and owner["Generation"] in manifests and type(record["State"]) is int and record["State"] in (1, 2), "authenticated retained read-owner union")
    require(sha_array(record["Manifest"]) == hashlib.sha256(go_json(manifests[owner["Generation"]])).digest(), "reader original generation/manifest")
    require(record["PinRequest"] == ("hold-old" if pinned else "") and sha_array(record["PinToken"]) == (hashlib.sha256(go_json(token)).digest() if pinned else bytes(32)), "reader exact issued pin binding")


def column_stage(row, index, result, manifests, binding, pin_binding, held, token, spool):
    config, prefix = result["Config"], f"g0/{result['Config']['Graph'].encode().hex()}/columns-v1/".encode()
    key = lambda name: prefix + name.encode()
    genkey = lambda name, kind: key("generation/" + name.encode().hex() + "/" + kind)
    retkey = lambda kind, name: key("retention-v1/" + kind + "/" + name.encode().hex())
    records = column_records(row["Records"])
    if index == 0:
        owners = [k for k in records if k.startswith(key("read-owner/"))]
        require(len(owners) == 1, "initial held reader inventory")
        held = owners[0]
        pin = decoded_body(records.get(retkey("pin", "hold-old"), b""))
        fields(pin, "Token Intent State TerminalRequest TerminalKind")
        token = pin["Token"]
        fields(token, "Version Request Generation Columns Manifest Secret Cut FeedStart")
        require(token["Version"] == 1 and type(token["Version"]) is int and token["Request"] == "hold-old" and token["Generation"] == "old" and sha_array(token["Columns"]) == hashlib.sha256(go_json(binding)).digest() and sha_array(token["Manifest"]) == hashlib.sha256(go_json(manifests["old"])).digest() and any(sha_array(token["Secret"])) and same(token["Cut"], result["OldCut"]) and same(token["FeedStart"], result["OldCut"]["Groups"]), "immutable actual issued pin token")
    names = ("old",) if index == 0 else ("old", "final")
    required = {key("ledger"), key("retention-v1/ledger"), fdb_key(config, 0, "meta", "state"), fdb_key(config, 1, "meta", "state"), held, retkey("pin", "hold-old"), retkey("reference", "head"), retkey("reference-request", "head-old")}
    for name in names:
        required.update(genkey(name, kind) for kind in ("manifest", "progress", "owners"))
        required.add(fdb_key(config, 0, "cut", go_json(manifests[name]["Generation"]["Cut"]).hex()))
    if index:
        required.add(retkey("reference-request", "head-final"))
    require(set(records) == required, "complete raw column lifecycle inventory")
    metadata = manifests["old" if index == 0 else "final"]["Metadata"]
    for g in range(2):
        require(records[fdb_key(config, g, "meta", "state")] == go_json(metadata[g]), "column stage authentic source metadata")
    for name in names:
        m = manifests[name]
        require(records[genkey(name, "manifest")] == go_json(m) and records[fdb_key(config, 0, "cut", go_json(m["Generation"]["Cut"]).hex())] == go_json(m["Generation"]["Cut"]), "original manifest/authenticated cut unchanged")
        progress = decoded_body(records[genkey(name, "progress")])
        fields(progress, "Generation Metadata Next Rows Bytes WorkBytes Sequences History Chain Previous EOF")
        expected = {"Generation": m["Generation"], "Metadata": m["Metadata"], "Next": m["Chunks"], "Rows": m["Rows"], "Bytes": m["Bytes"], "WorkBytes": progress["WorkBytes"], "Sequences": m["Generation"]["Cut"]["Groups"], "History": m["History"], "Chain": m["Chain"], "Previous": progress["Previous"], "EOF": True}
        require(same(progress, expected) and 0 < integer(progress["WorkBytes"]) <= binding["Policy"]["MaxWorkBytes"], "durable genuine source EOF/progress")
        prior = spool.find(b"column-control", genkey(name, "progress"))
        require(prior is None or prior == records[genkey(name, "progress")], "immutable original publication progress changed")
        collected = name == "old" and index >= 5
        guard = {"Manifest": list(hashlib.sha256(go_json(m)).digest()), "State": 3 if collected else 1,
                 "References": 1 if (name == "old" and index == 0 or name == "final") else 0,
                 "Pins": 1 if name == "old" and index <= 1 else 0, "Readers": 1 if name == "old" and index <= 3 else 0,
                 "Cursor": m["Chunks"] if collected else 0, "DeletedBytes": m["Bytes"] if collected else 0, "Chain": m["Chain"] if collected else [0] * 32}
        require(records[genkey(name, "owners")] == go_json(guard), "shared generation guard/reference/pin/read ownership stage")
    pin = {"Token": token, "Intent": list(hashlib.sha256(go_json({"Columns": token["Columns"], "Generation": "old", "Cut": result["OldCut"]})).digest()),
           "State": 1 if index <= 1 else 3, "TerminalRequest": "" if index <= 1 else "expire-old", "TerminalKind": 0 if index <= 1 else 3}
    require(records[retkey("pin", "hold-old")] == go_json(pin), "authenticated terminal pin identity/tombstone")
    reader = decoded_body(records[held])
    column_reader(reader, manifests, token, True)
    require(held == key("read-owner/" + reader["Owner"]["Request"].encode().hex()) and reader["Owner"]["Generation"] == "old" and reader["State"] == (1 if index <= 3 else 2), "held owner preserved across mutation/expiry until Close")
    initial_reader = spool.find(b"column-held", held)
    if initial_reader is None:
        spool.insert(b"column-held", held, go_json(reader))
    else:
        require(same(reader | {"State": 1}, decoded_body(initial_reader)), "held reader identity substituted")
    for name, request, version in (("old", "head-old", 1), ("final", "head-final", 2)):
        if name not in names:
            continue
        reference = {"Name": "head", "Generation": name, "Version": version, "Manifest": list(hashlib.sha256(go_json(manifests[name])).digest())}
        request_record = {"Intent": list(hashlib.sha256(go_json({"Name": "head", "Generation": name, "Expected": version - 1})).digest()), "Reference": reference}
        require(records[retkey("reference-request", request)] == go_json(request_record), "immutable named-reference request/ABA revision")
        if name == ("old" if index == 0 else "final"):
            require(records[retkey("reference", "head")] == go_json(reference), "current durable named reference")
    # Exact logical admission charges differ from actual stored/tombstone bytes.
    admitted = names
    charge = len(key("binding")) + len(go_json(binding)) + len(key("ledger")) + 256
    charge += sum(8192 + len(genkey(name, "progress")) + len(genkey(name, "manifest")) + len(genkey(name, "owners")) + 4096 + manifests[name]["Bytes"] for name in admitted)
    charge += (1 if index <= 2 else 8) * (len(held) + 4096)
    if index >= 5:
        charge -= manifests["old"]["Bytes"]
    ledger = {"Bytes": charge, "Generations": len(admitted), "Chunks": sum(manifests[name]["Chunks"] for name in admitted) - (manifests["old"]["Chunks"] if index >= 5 else 0)}
    require(records[key("ledger")] == go_json(ledger) and charge <= binding["Policy"]["MaxRetainedBytes"], "exact live/chunk/control reservation ledger")
    pin_charge = len(key("retention-v1/binding")) + len(go_json(pin_binding)) + len(key("retention-v1/ledger")) + 256
    pin_charge += sum(len(k) + 4096 for k in (retkey("reference", "head"), retkey("reference-request", "head-old"), retkey("pin", "hold-old")))
    if index:
        pin_charge += len(retkey("reference-request", "head-final")) + 4096
    pin_ledger = {"Records": 5 if index == 0 else 6, "Bytes": pin_charge, "ReservedRecords": 1, "ReservedBytes": len(prefix) + len(b"retention-v1/reference-request/") + 256 + 4096}
    require(records[key("retention-v1/ledger")] == go_json(pin_ledger), "pin/reference future-detach credit ledger")
    if index == 6:
        require(same(ledger, result["Columns"]["Ledger"]) and same(pin_ledger, result["Columns"]["PinLedger"]), "terminal quota ledgers bound to result")
    for k, value in records.items():
        if k.startswith(prefix):
            put_raw(spool, b"column-control", k, value)
    return held, token


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
    require(type(spec["version"]) is type(spec["writers"]) is type(spec["crossPercent"]) is int and spec["version"] == 1 and spec["writers"] in (1,8,32) and spec["crossPercent"] in (0, 10, 50, 100), "declared cost spec")
    count = spec["facts"]
    parallel = spec["writers"] != 1
    require(not parallel or raw_result is not None and raw_result.get("version") in (4,5) and "Parallel" in raw_result, "parallel evidence requires fresh complete attempt format")
    column_lane = raw_result is not None and (raw_result.get("version"),raw_result.get("oracleFormat")) in ((3,COLUMN_FORMAT),(5,PARALLEL_COLUMN_FORMAT))
    graph = "graph-cost-parallel-columns-v1" if parallel and column_lane else "graph-cost-parallel-v1" if parallel else "graph-cost-columns-v1" if column_lane else "graph-cost-v1"
    expected_config = {"Format": 2, "Graph": graph, "Topology": 1, "Epochs": [7, 11],
                       "Limits": {"Bytes": (64 << 20) + count * (64 << 10) + (2*count*63*2048 if parallel else 0), "Transactions": 3 + 2 * count * (64 if parallel else 1), "Versions": 3 + 6 * count, "Certificates": 1024},
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
    left = (native_parallel_groups if parallel else native_groups)(native_path, spec, native_bounds, native_stats, spool)
    if raw_result is None:
        right = fdb_groups(fdb_path, spec, config, final_cut, fdb_stats, spool)
    else:
        fields(raw_result, "version engine spec deployment SetupNodes Creates Updates CompletedCreates CompletedUpdates SetupNanos WritesNanos VerificationNanos TotalNanos InitialCut OldCut FinalCut Config CDCGroups LogicalComponentChanges PhysicalGraphChanges IDAdmission oracleFormat StreamEnds" + (" Columns" if column_lane else "") + (" Parallel" if parallel else ""))
        require(observations_path is not None and "oldCut" in native_bounds, "raw parity requires actual retained observations/old cut")
        expected_format = (5,PARALLEL_COLUMN_FORMAT) if parallel and column_lane else (4,PARALLEL_FORMAT) if parallel else (3,COLUMN_FORMAT) if column_lane else (2,RAW_FORMAT)
        require(type(raw_result["version"]) is int and (raw_result["version"], raw_result["oracleFormat"]) == expected_format and same(raw_result["Config"], config) and same(raw_result["FinalCut"], final_cut), "mixed raw result format/config/cut")
        require(same(raw_result["spec"], spec) and len(raw_result["StreamEnds"]) == 3, "raw result specification/complete EOF set")
        for name, expected in (("SetupNodes", 3), ("Creates", count), ("Updates", count), ("CompletedCreates", count), ("CompletedUpdates", count), ("CDCGroups", 3 + 2 * count), ("LogicalComponentChanges", 3 + 3 * count), ("PhysicalGraphChanges", 3 + 6 * count)):
            require(type(raw_result[name]) is int and raw_result[name] == expected, "raw result exact cardinality")
        admission = "fixed declared fixture IDs/ranges; independent SDK clients, no rho allocator protocol" if parallel else "fixed declared fixture IDs/ranges; this adapter has no rho allocator protocol"
        require(raw_result["engine"] == "foundationdb" and raw_result["IDAdmission"] == admission, "raw engine/admission identity")
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
            "scope": ("declared independent8/32 client create/update contract; complete attempts and per-key causality/cut membership; no common total round order, capacity, runtime or fullmatrix acceptance" if parallel else "fixed writer1 create/update contract; numeric rounds/indexes and physical protocol/replica layouts are validated per engine")}
    if raw_result is not None:
        report["canonicalRetainedOutputSHA256"] = retained_observations(observations_path, spec, config, spool)
        report["oracleFormat"] = raw_result["oracleFormat"]
        if column_lane:
            report["columns"] = column_artifacts(Path(fdb_path).with_name("fdb-cost-columns.jsonl"), raw_result, spool)
    return report
