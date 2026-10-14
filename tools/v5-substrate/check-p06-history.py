#!/usr/bin/env python3
"""Offline P06 raw-binding bridge to the existing bounded serial_witness search.

Go supplies the exact graph/index/CDC assertions and authentic pipe extraction.
This bridge validates sealed originals, correspondence and complete prefixes,
then projects observed cells into the existing oracle; it implements no reducer.
It proves neither fsync nor process authenticity from self-reported JSON alone.
"""
import argparse
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import struct

ROOT = Path(__file__).resolve().parents[2]
ORACLE = ROOT / "docs/v5/reference/independent/protocol/oracle.py"
ORACLE_SHA = "a132b4e4981f3dcea485b7ee5dea0df48a906cdfcda4a75bec5ff9f1a5589d3a"
GRAPH = bytes.fromhex("01000000000000000000000000000000")


def require(value, message):
    if not value:
        raise ValueError("P06 refused: " + message)


def raw(value):
    require(value is None or type(value) is str, "raw byte field")
    return b"" if value is None else base64.b64decode(value, validate=True)


def body(wire, magic):
    require(len(wire) >= 36 and wire[:4] == magic, "original codec")
    require(hashlib.sha256(wire[:-32]).digest() == wire[-32:], "original seal")
    return wire[4:-32]


class Cursor:
    def __init__(self, wire):
        self.wire = wire

    def take(self, size):
        require(0 <= size <= len(self.wire), "truncated original")
        result, self.wire = self.wire[:size], self.wire[size:]
        return result

    def number(self, size=8):
        return int.from_bytes(self.take(size), "big")

    def field(self, maximum=1 << 20):
        size = self.number(4)
        require(size <= maximum, "original field bound")
        return self.take(size)

    def end(self):
        require(not self.wire, "extra original bytes")


def pgq(wire):
    require(wire[:4] in (b"PGQ\1", b"PGQ\2"), "typed original proposal")
    c = Cursor(body(wire, wire[:4]))
    kind, graph, partition, request = c.number(1), c.take(16), c.number(), c.take(16)
    configuration, declaration, routing = c.take(32), c.take(32), c.take(32)
    inner = c.field()
    c.end()
    require(graph == GRAPH and partition in (3, 8), "original namespace")
    nested = body(inner, b"PGO\1")
    require(nested[:41] == bytes([kind]) + graph + struct.pack(">Q", partition) + request, "nested request binding")
    return dict(kind=kind, graph=graph, partition=partition, request=request,
                configuration=configuration, declaration=declaration, routing=routing, inner=inner)


def gct(wire):
    c = Cursor(body(wire, b"GCT\1"))
    phase, graph, partition = c.number(1), c.take(16), c.number()
    manifest = c.field()
    proofs = c.number(4)
    require(proofs <= 2, "finite original proof set")
    c.take(99 * proofs)
    c.end()
    require(graph == GRAPH and partition in (3, 8), "phase namespace")
    return phase, partition, manifest


def status(wire):
    b = body(wire, b"GCS\1")
    require(len(b) == 99 and b[:16] == GRAPH, "original status namespace/length")
    partition = int.from_bytes(b[16:24], "big")
    require(partition in (3, 8) and 1 <= b[72] <= 4 and b[97] <= 3, "status fields")
    return dict(partition=partition, tx=b[24:40], digest=b[40:72], phase=b[72],
                index=int.from_bytes(b[73:81], "big"), floor=int.from_bytes(b[81:89], "big"),
                round=int.from_bytes(b[89:97], "big"), yes=bool(b[97] & 1), commit=bool(b[97] & 2))


def peer_phase(request, reply, scopes, voter):
    q = Cursor(body(request, b"GSQ\1"))
    kind, sequence, nonce = q.number(1), q.number(), q.take(16)
    q.take(40 + 8)  # Source cookie and maximum/rows.
    q.field(); q.field(); q.field()
    proposal = Cursor(body(q.field(), b"GSC\1"))
    target = body(proposal.field(156), b"APS\1")
    command = proposal.field()
    proposal.end()
    q.take(17)
    q.end()
    phase, partition, manifest = gct(command)
    r = Cursor(body(reply, b"GSR\1"))
    scope = body(r.field(156), b"APS\1")
    actual_voter, issuer, reply_kind = r.number(), r.take(16), r.number(1)
    reply_sequence, reply_nonce, mask = r.number(), r.take(16), r.number(2)
    outcome = r.field()
    r.end()
    require(kind == reply_kind == 8 and sequence == reply_sequence and nonce == reply_nonce and any(nonce) and any(issuer) and mask == 0, "peer correlation")
    require(scope == target == scopes[partition] and actual_voter == voter, "configured source scope/voter correspondence")
    s = status(outcome)
    require(s["phase"] == phase and s["partition"] == partition and s["digest"] == hashlib.sha256(manifest).digest(), "phase original binding")
    return command, manifest, s, outcome


def cut(observation):
    scopes, rounds = {}, set()
    for encoded in observation["Cut"]["Certificates"]:
        b = body(raw(encoded), b"GSC\1")
        require(len(b) == 356 and b[:16] == GRAPH, "certified original cut")
        partition = int.from_bytes(b[16:24], "big")
        require(partition in (3, 8) and partition not in scopes, "cut participant set")
        scopes[partition] = b[:120]
        rounds.add(int.from_bytes(b[152:160], "big"))
    require(set(scopes) == {3, 8} and len(rounds) == 1, "complete equal-round cut")
    return scopes, rounds.pop()


def projected(observation):
    """Only an observed map projection; endpoint-effective answers stay in Go.

    Physical native revisions and provenance remain in cell values. Declared
    postings are retained candidates; they are not substituted for effectiveness.
    """
    native = observation["Native"]
    require(native["graph"] == GRAPH.hex() and len(native["entityCensus"]) == 32 and len(native["entities"]) == 3 and len(native["postings"]) == 5, "complete observed graph slice")
    result = {}
    prefix = lambda owner: "A" if owner < 17 else "B"
    for entity in native["entities"]:
        owner = entity["id"]
        if not entity["found"]:
            continue
        start = prefix(owner)
        result[f"{start}/entity/{owner}"] = {k: entity[k] for k in ("kind", "source", "target", "mode", "type")}
        result[f"{start}/presence/{owner}"] = entity["presence"]
        result[f"{start}/labels/{owner}"] = entity["labels"] or []
        if owner == 3:
            result["A/property/3/p"] = entity["property"]
    for life in native["lives"]:
        if life["found"]:
            result[f'{prefix(life["owner"])}/life/{life["owner"]}/{life["life"]}'] = {k: life[k] for k in ("sourceLife", "targetLife")}
    for posting in native["postings"]:
        result[f'{prefix(posting["endpoint"])}/posting/{posting["endpoint"]}/{posting["life"]}'] = posting["declared"]
    require(all(not p["found"] for p in native["phantomEntities"]), "phantom observed entity")
    return result


def mutations(before, after):
    return {k: {"op": "put", "value": after[k]} if k in after else {"op": "delete"}
            for k in before.keys() | after.keys() if k not in before or k not in after or before[k] != after[k]}


def writes(before, after, keys):
    # These are the fixed fixture's operation footprints, not reducer rules.
    # A changed observation must not invent additional transaction effects.
    result = mutations(before, after)
    require(set(result) == set(keys), "observed effects differ from original operation footprint")
    return result


def read_key(state, key):
    return {"kind": "key", "key": key, "exists": key in state, "value": state.get(key)}


def oracle():
    require(hashlib.sha256(ORACLE.read_bytes()).hexdigest() == ORACLE_SHA, "reviewed oracle hash changed")
    spec = importlib.util.spec_from_file_location("p06_existing_oracle", ORACLE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.serial_witness


def resumed(encoded, scopes, request, tx, business, pending, decision_wire):
    # Exact current processResponse fixed header: incarnation, 28 statistics,
    # cut/manifest hashes and read generation. No new wire format is introduced.
    process = Cursor(raw(encoded))
    require(any(process.take(16)), "process response incarnation")
    process.take(28 * 8 + 32 + 32 + 8)
    count = process.number(4)
    require(count <= 2, "process response handle bound")
    process.take(8 * count)
    result = Cursor(process.field())
    require(result.take(4) == b"GCR1", "actual pending/decision result")
    scope = body(result.field(156), b"APS\1")
    partition = int.from_bytes(scope[16:24], "big")
    require(scope == scopes[partition] and result.take(16) == request and result.take(16) == tx and result.take(32) == business and result.number(1) == pending and result.field(256) == decision_wire, "correlated original resume/decision")
    result.end()


def business_hash(wire, coordinator=None):
    request = pgq(wire)
    inner = bytearray(request["inner"][:-32])
    partition = request["partition"] if coordinator is None else coordinator
    inner[21:29] = struct.pack(">Q", partition)
    inner[45:53] = struct.pack(">Q", 1)
    inner = bytes(inner) + hashlib.sha256(inner).digest()
    outer = bytearray(wire[:-32])
    outer[3] = 1  # Coordinator.business uses the existing PGQ1 normalization.
    outer[21:29] = struct.pack(">Q", partition)
    outer[141:] = struct.pack(">I", len(inner)) + inner
    outer = bytes(outer) + hashlib.sha256(outer).digest()
    return hashlib.sha256(outer).digest()


def check(data):
    require(data["Version"] == 1 and data["Coordinator"] in (3, 8) and type(data["Commit"]) is bool, "finite cell identity")
    scopes, old_round = cut(data["Before"])
    first, manifest, yes, yes_wire = peer_phase(raw(data["FirstYES"]["Request"]), raw(data["FirstYES"]["Reply"]), scopes, 1)
    require(first == raw(data["Manifest"]) and yes["yes"] and yes["partition"] == 3, "first actual YES")
    m = Cursor(body(manifest, b"GCM\1"))
    tx, coordinator, original_wire = m.take(16), m.number(), m.field()
    original = pgq(original_wire)
    source = pgq(raw(data["Original"]))
    require(coordinator == data["Coordinator"] == original["partition"] and source["request"] == original["request"] and source["configuration"] == original["configuration"], "original business identity/configuration")
    require(business_hash(raw(data["Original"]), coordinator) == business_hash(original_wire), "complete original operations/claims/axes/provenance binding")
    expected_tx = hashlib.sha256(b"rho-tkg:cross-transaction-id:v1\0" + GRAPH + struct.pack(">Q", coordinator) + original["request"]).digest()[:16]
    require(tx == expected_tx == yes["tx"], "graph/coordinator/request-qualified TxID")
    require(len(data["Phases"]) == 5 and len(data["Duplicates"]) == 3, "complete recovery/replay set")
    decision, decision_wire = None, None
    for i, phase in enumerate(data["Phases"]):
        require(phase["Origin"] == (2 if coordinator == 3 else 4), "actual recovery origin correspondence")
        _, actual_manifest, s, outcome_wire = peer_phase(raw(phase["Request"]), raw(phase["Reply"]), scopes, 2 if phase["Partition"] == 3 else 4)
        require(actual_manifest == manifest and s["tx"] == tx and s["phase"] == (2, 2, 3, 4, 4)[i] and s["partition"] == (3, 8, coordinator, 3, 8)[i], "retained original phase/order")
        if i < 2:
            require(s["yes"] == (True if i == 0 else data["Commit"]), "actual prepared vote")
        else:
            require(s["commit"] == data["Commit"], "immutable original decision")
            decision = s if i == 2 else decision
            decision_wire = outcome_wire if i == 2 else decision_wire
            require(s["round"] == decision["round"], "original decision/resolve round")
    resumed(data["Pending"], scopes, original["request"], tx, business_hash(original_wire), 1, b"")
    resumed(data["Decision"], scopes, original["request"], tx, business_hash(original_wire), 0, decision_wire)
    originals = {(r["Partition"], r["Index"]): r for r in data["Records"]}
    require(len(originals) == len(data["Records"]), "duplicate physical record")
    for i, partition in enumerate((3, 8)):
        indices = [r["Index"] for r in data["Records"] if r["Partition"] == partition]
        require(indices == list(range(data["From"][i] + 1, data["Through"][i] + 1)), "complete ordered retained prefix")
    require(raw(originals[(3, yes["index"])]["Outcome"]) == yes_wire, "first YES stored original")
    for duplicate in data["Duplicates"]:
        s = status(raw(duplicate["Outcome"]))
        require(s["tx"] == tx and s["digest"] == yes["digest"] and s["phase"] == 4 and s["index"] < duplicate["Index"] and not raw(duplicate["CDC"]), "duplicate original/zero CDC")
    for local in [data["Refused"], data["Closed"]] + ([data["Changed"]] if data["Changed"] else []):
        proposal = pgq(raw(local["Proposal"]))
        o = body(raw(local["Outcome"]), b"GRO\4")
        require(len(o) == 84 and o[1:17] == GRAPH and o[25:41] == proposal["request"] and o[41:73] == hashlib.sha256(raw(local["Proposal"])).digest() and int.from_bytes(o[73:81], "big") == local["Index"], "local original outcome binding")
        stored = originals[(local["Partition"], local["Index"])]
        require(stored["Outcome"] == local["Outcome"] and stored["CDC"] == local["CDC"], "local original stored record")
        require((int.from_bytes(o[82:84], "big") == 11 and not raw(local["CDC"])) if local is data["Refused"] else (o[82:84] == b"\0\0" and raw(local["CDC"])), "prepared refusal or applied local effect")
    resolved, original_phases = set(), []
    known_local = {(r["Partition"], r["Index"]): r for r in [data["Refused"], data["Closed"]] + ([data["Changed"]] if data["Changed"] else [])}
    for record in data["Records"]:
        outcome, cdc = raw(record["Outcome"]), raw(record["CDC"])
        if not outcome:
            require(not cdc, "unknown empty output")
            continue
        if outcome[:4] == b"GCS\1":
            s = status(outcome)
            require(s["tx"] == tx and s["digest"] == yes["digest"] and s["partition"] == record["Partition"] and s["index"] <= record["Index"], "unknown cross original")
            identity, digest, index = tx, s["digest"], s["index"]
            require(not cdc or s["phase"] == 4 and s["commit"] and index == record["Index"], "nonbusiness cross CDC")
            if index == record["Index"]:
                original_phases.append((s["partition"], s["phase"]))
            if s["phase"] == 4 and index == record["Index"]:
                require(record["Partition"] not in resolved and s["commit"] == data["Commit"] and bool(cdc) == data["Commit"], "complete original Resolve CDC")
                resolved.add(record["Partition"])
        elif outcome[:4] == b"GRO\4":
            require((record["Partition"], record["Index"]) in known_local, "unknown local original")
            o = body(outcome, b"GRO\4")
            identity, digest, index = o[25:41], o[41:73], int.from_bytes(o[73:81], "big")
        else:
            b = body(outcome, b"GSO\1")
            require(len(b) == 18 and int.from_bytes(b[10:18], "big") == record["Index"] and not cdc, "unknown control output")
            continue
        if cdc:
            envelope = Cursor(body(cdc, b"GCE\1"))
            envelope.take(1)
            require(envelope.take(16) == identity and envelope.take(32) == digest and envelope.number() == index, "CDC/original identity binding")
            logical = body(envelope.field(), b"GCD\5")
            envelope.end()
            require(logical[:16] == GRAPH and int.from_bytes(logical[16:24], "big") == record["Partition"], "logical CDC namespace")
    require(resolved == {3, 8}, "missing original business participant")
    require(sorted(original_phases) == sorted([(coordinator, 1), (3, 2), (8, 2), (coordinator, 3), (3, 4), (8, 4)]), "complete original protocol inventory")
    require(data["Before"]["Cut"] == data["Retained"]["Cut"] and data["Before"]["Native"] == data["Retained"]["Native"], "old cut/current substitution")
    initial, recovered, current = (projected(data[k]) for k in ("Before", "Recovered", "Current"))
    transactions, rounds, committed = {}, {}, set()
    if data["Commit"]:
        transactions["edge"] = {"reads": [read_key(initial, k) for k in ("A/presence/1", "B/presence/17", "A/entity/3")], "writes": writes(initial, recovered, ("A/entity/3", "A/presence/3", "A/labels/3", "A/property/3/p", "A/life/3/4", "A/posting/1/0", "A/posting/1/2", "B/posting/17/0", "B/posting/17/18"))}
        rounds["edge"] = decision["round"]
        committed.add("edge")
    else:
        require(data["Changed"] is not None, "ABORT lacks genuine intervening mutation")
        transactions["close-b"] = {"reads": [read_key(initial, "B/presence/17")], "writes": writes(initial, recovered, ("B/presence/17",))}
        rounds["close-b"] = data["Recovered"]["Native"]["entities"][1]["presence"]["revision"]
        committed.add("close-b")
    transactions["close-a"] = {"reads": [read_key(recovered, "A/presence/1")], "writes": writes(recovered, current, ("A/presence/1",))}
    rounds["close-a"] = data["Current"]["Native"]["entities"][0]["presence"]["revision"]
    committed.add("close-a")
    history = {"name": "P06", "initial": initial, "transactions": transactions}
    observations = [{"event": k, "scope": ["A", "B"], "round": cut(data[k])[1], "values": projected(data[k])} for k in ("Before", "Recovered", "Current", "Retained")]
    witness, detail = oracle()(history, committed, rounds, observations, [{"event": "final", "scope": ["A", "B"], "values": current}])
    require(witness is not None, "independent serial/cut witness: " + str(detail))
    kill, reopen = data["Processes"]
    require(kill["Action"] == "kill" and kill["Member"] == 1 and kill["Signal"] == "SIGKILL" and kill["Reaped"] is True and reopen["PreviousInstance"] == kill["Instance"] and reopen["Volume"] == kill["Volume"] and reopen["Ready"] is True, "actual captured same-directory process boundary")
    kill_raw, reopen_raw = json.loads(raw(kill["Raw"])), json.loads(raw(reopen["Raw"]))
    require(kill_raw["SignalNumber"] == 9 and kill_raw["Signaled"] is True and kill_raw["Reaped"] is True and kill_raw["Success"] is False and kill_raw["Instance"] == kill["Instance"] and kill_raw["Volume"] == kill["Volume"], "OS kill/reap raw binding")
    require(reopen_raw["PreviousInstance"] == kill["Instance"] and reopen_raw["Volume"] == kill["Volume"] and reopen_raw["RecoveryRaw"] == reopen["RecoveryRaw"], "same-directory recovered raw binding")
    return {"coordinator": coordinator, "commit": data["Commit"], "serial_witness": witness, "records": len(originals), "scope": "P06 finite original-binding/serial bridge; authentic extraction and complete graph/CDC assertions are qualified by the Go test"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+", type=Path)
    args = parser.parse_args()
    result = []
    for path in args.artifacts:
        wire = path.read_bytes()
        require(len(wire) <= 4 << 20, "finite artifact bound")
        result.append(check(json.loads(wire)))
        result[-1].update(path=str(path), sha256=hashlib.sha256(wire).hexdigest())
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
