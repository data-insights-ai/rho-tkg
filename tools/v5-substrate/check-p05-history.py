#!/usr/bin/env python3
"""P05 raw-original and observed-history bridge to the pinned serial/cut oracle.

Only raw codec helpers are borrowed. No P06/fault acceptance wrapper or protocol
reducer runs. Native authenticity, complete typed CDC decoding, current authority
and durable process behavior require the separately qualified Go capture. Empty
raw outputs stay opaque; this bridge never supplies missing events or decisions.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import struct

ROOT = Path(__file__).resolve().parents[2]
ORACLE = ROOT / "docs/v5/reference/independent/protocol/oracle.py"
ORACLE_SHA = "a132b4e4981f3dcea485b7ee5dea0df48a906cdfcda4a75bec5ff9f1a5589d3a"
SCHEDULES = {"write-skew-A-first", "write-skew-B-first", "empty-range-and-absent-key-insertion", "temporal-life-value-ABA", "authority-fence-before-prepare"}


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


CODEC = module("p05_raw_codecs", Path(__file__).with_name("check-p06-history.py"))
raw, body, Cursor, pgq, gct, status = (getattr(CODEC, name) for name in ("raw", "body", "Cursor", "pgq", "gct", "status"))
GRAPH = CODEC.GRAPH


def require(value, message):
    if not value:
        raise ValueError("P05 refused: " + message)


def oracle():
    require(hashlib.sha256(ORACLE.read_bytes()).hexdigest() == ORACLE_SHA, "reviewed serial oracle hash changed")
    return module("p05_existing_serial_oracle", ORACLE).serial_witness


def seal(wire):
    return wire + hashlib.sha256(wire).digest()


def field(wire):
    return struct.pack(">I", len(wire)) + wire


def certificate(wire):
    value = body(wire, b"GSC\1")
    require(len(value) == 356 and value[:16] == GRAPH, "certificate namespace/length")
    partition = int.from_bytes(value[16:24], "big")
    require(partition in (3, 8), "certificate participant")
    return dict(partition=partition, scope=value[:120], configuration=value[120:152],
                round=int.from_bytes(value[152:160], "big"), index=int.from_bytes(value[160:168], "big"), image=value[184:356])


def cut(encoded):
    wires = [raw(v) for v in encoded["Certificates"]]
    certificates = [certificate(w) for w in wires]
    require([c["partition"] for c in certificates] == [3, 8] and len({c["round"] for c in certificates}) == 1, "complete equal-round cut")
    require(certificates[0]["round"] > 0 and certificates[0]["configuration"] == certificates[1]["configuration"], "cut configuration/round")
    return {c["partition"]: c["scope"] for c in certificates}, certificates[0]["round"], wires


def condition(wire):
    c = Cursor(body(wire, b"GUC\1"))
    require(c.number(4) == 2, "complete finite condition")
    parts = []
    for partition in (3, 8):
        encoded, prefix, authority = c.field(512), c.number(), c.take(32)
        cert = certificate(encoded)
        require(cert["partition"] == partition and cert["index"] <= prefix < 2**64 - 1 and any(authority), "condition original prefix/authority")
        parts.append((encoded, prefix, authority))
    c.end()
    require(certificate(parts[0][0])["round"] == certificate(parts[1][0])["round"], "condition common cut")
    return parts


def normalized_business(wire, coordinator):
    parsed = pgq(wire)
    inner = bytearray(parsed["inner"][:-32])
    inner[21:29], inner[45:53] = struct.pack(">Q", coordinator), struct.pack(">Q", 1)
    outer = bytearray(wire[:141])
    outer[3], outer[21:29] = 1, struct.pack(">Q", coordinator)
    return seal(bytes(outer) + field(seal(bytes(inner))))


def manifest(wire):
    conditional = wire[:4] == b"GCM\2"
    condition_wire = None
    if conditional:
        c = Cursor(body(wire, b"GCM\2"))
        inner, condition_wire = c.field(), c.field()
        c.end()
        parts = condition(condition_wire)
    else:
        inner, parts = wire, None
    c = Cursor(body(inner, b"GCM\1"))
    tx, coordinator, original = c.take(16), c.number(), c.field()
    parsed = pgq(original)
    hashes = [c.take(32) for _ in range(4)]
    require(coordinator == parsed["partition"] and hashes[:3] == [parsed[k] for k in ("configuration", "declaration", "routing")], "inner original binding")
    require(tx == hashlib.sha256(b"rho-tkg:cross-transaction-id:v1\0" + GRAPH + struct.pack(">Q", coordinator) + parsed["request"]).digest()[:16], "original TxID")
    require(c.number(4) == 2, "complete manifest participants")
    floors = {}
    for partition in (3, 8):
        actual, floor, kernel = c.number(), c.number(), c.field()
        require(actual == partition, "ordered manifest participants")
        k = Cursor(body(kernel, b"GPK\1"))
        require(k.take(16) == tx and k.take(16) == GRAPH and k.number() == partition and k.number() > 0, "opaque kernel identity")
        require([k.take(32) for _ in range(4)] == hashes, "opaque kernel request hashes")
        effect, revision, provenance = k.take(32), k.number(), k.number()
        require(revision > floor and provenance > 0, "captured actual kernel revision/floor")
        k.field(); effect_body = k.field(); k.end()
        require(hashlib.sha256(effect_body).digest() == effect, "opaque kernel effect seal")
        floors[partition] = floor
    c.end()
    if parts:
        for encoded, _, _ in parts:
            cert = certificate(encoded)
            require(cert["configuration"] == parsed["configuration"] and cert["scope"][56:88] == parsed["declaration"], "condition/inner original authority binding")
    business = normalized_business(original, coordinator)
    if conditional:
        business = seal(b"GUB\1" + field(business) + field(condition_wire))
    return dict(tx=tx, coordinator=coordinator, original=original, request=parsed["request"],
                condition=condition_wire, inner=inner, digest=hashlib.sha256(wire).digest(),
                business=hashlib.sha256(business).digest(), floors=floors)


def result(wire, scopes):
    c = Cursor(wire)
    require(c.take(4) == b"GCR1", "actual coordinator result")
    scope = body(c.field(156), b"APS\1")
    request, tx, business, pending, decision = c.take(16), c.take(16), c.take(32), c.number(1), c.field(256)
    c.end()
    partition = int.from_bytes(scope[16:24], "big")
    require(scope == scopes.get(partition) and any(request) and any(business) and pending in (0, 1), "result scope/identity")
    require(tx == hashlib.sha256(b"rho-tkg:cross-transaction-id:v1\0" + GRAPH + struct.pack(">Q", partition) + request).digest()[:16], "result TxID")
    decided = status(decision) if decision else None
    require(not decided or decided["tx"] == tx and decided["partition"] == partition and decided["phase"] == 3, "result actual decision")
    return dict(tx=tx, request=request, business=business, pending=pending, decision=decided)


def frames(encoded, scopes):
    manifests, replies, conflicts, captures = {}, [], 0, set()
    for frame in encoded:
        require(frame["Origin"] in range(1, 7) and frame["Partition"] in (3, 8), "frame process mapping")
        q = Cursor(body(raw(frame["Request"]), b"GSQ\1"))
        kind, sequence, nonce = q.number(1), q.number(), q.take(16)
        q.take(48)
        q.field(); q.field(); q.field()
        command = q.field(); q.take(17); q.end()
        r = Cursor(body(raw(frame["Reply"]), b"GSR\1"))
        scope, voter, issuer = body(r.field(156), b"APS\1"), r.number(), r.take(16)
        require(scope == scopes[frame["Partition"]] and voter == (1 if frame["Partition"] == 3 else 4) and any(issuer), "authenticated peer correspondence")
        require(r.number(1) == kind and r.number() == sequence and r.take(16) == nonce and any(nonce), "request/reply correlation")
        mask, reply = r.number(2), r.field(); r.end()
        require(kind in (8, 10) and (not mask or not reply), "captured frame kind/error representation")
        if mask:
            continue  # A literal error is not a vote, absence or decision.
        if kind == 8:
            p = Cursor(body(command, b"GSC\1"))
            require(body(p.field(156), b"APS\1") == scope, "submitted target scope")
            phase, partition, original = gct(p.field()); p.end()
            m, s = manifest(original), status(reply)
            require(partition == frame["Partition"] == s["partition"] and phase == s["phase"] and s["tx"] == m["tx"] and s["digest"] == m["digest"], "outer original/actual phase digest")
            require(m["tx"] not in manifests or manifests[m["tx"]][0] == original, "original manifest changed")
            manifests[m["tx"]] = (original, m)
            replies.append((s, reply))
        elif command[:4] == b"GUQ\1":
            q = Cursor(body(command, b"GUQ\1"))
            cert_wire = q.field(512)
            cert = certificate(cert_wire)
            grants = [q.number(4) for _ in range(3)]; q.end()
            require(cert["scope"] == scope and all(grants), "conditional capture original scope/grants")
            capture = Cursor(body(reply, b"GUI\1"))
            classification = capture.number(1)
            require(classification in (1, 2) and all(capture.number(4) <= grant for grant in grants), "capture classification/work")
            prefix, authority, ttl = capture.number(), capture.take(32), capture.number()
            require(0 < ttl <= 60_000_000_000 and capture.take(16) == issuer and capture.number() > 0 and capture.take(16) == nonce, "capture completed read identity/lifetime")
            observation = Cursor(body(capture.field(1024), b"APO\1"))
            require(observation.number(1) == 1 and observation.take(120) == scope, "capture native configuration observation")
            observed_index, configuration = observation.number(), observation.take(32)
            capture.number(); index, image_hash = capture.number(), capture.take(32)
            image = capture.field(172); capture.number(); capture.end()
            require(len(image) == 172 and hashlib.sha256(image).digest() == image_hash and observed_index == index and configuration == cert["configuration"], "capture actual root/observation binding")
            if classification == 1:
                require(prefix == index and prefix >= cert["index"] and any(authority) and image == cert["image"], "successful captured native prefix")
                captures.add((cert_wire, prefix, authority))
            else:
                conflicts += 1
        else:
            certificate(command)  # Ordinary certified-open body, not a condition.
    for _, m in manifests.values():
        if m["condition"]:
            require(set(condition(m["condition"])) <= captures, "condition lacks matching literal authenticated native capture")
    return manifests, replies, conflicts, captures


def records(encoded, manifests):
    retained, phases, decisions = {}, {}, {}
    for record in encoded:
        key = record["Partition"], record["Index"]
        require(key[0] in (3, 8) and key[1] > 0 and key not in retained, "retained record identity/duplicate")
        retained[key] = record
        outcome, cdc = raw(record["Outcome"]), raw(record["CDC"])
        if not outcome:
            require(not cdc, "opaque empty output has CDC")
            continue
        if outcome[:4] == b"GCS\1":
            s = status(outcome)
            require(s["partition"] == key[0] and s["index"] <= key[1] and s["tx"] in manifests and s["digest"] == manifests[s["tx"]][1]["digest"], "retained outer-original binding")
            original_key = (s["tx"], s["partition"], s["phase"])
            if s["index"] == key[1]:
                require(original_key not in phases, "different retained original phase")
                phases[original_key] = outcome
            else:
                require(phases.get(original_key) == outcome and not cdc, "duplicate differs from retained original or emitted CDC")
            if s["phase"] == 3:
                require(s["partition"] == manifests[s["tx"]][1]["coordinator"], "retained decision coordinator")
                decisions[s["tx"]] = s
            require(not cdc or s["phase"] == 4 and s["commit"] and s["index"] == key[1], "CDC before original committed Resolve")
            if cdc:
                envelope = Cursor(body(cdc, b"GCE\1")); envelope.number(1)
                require(envelope.take(16) == s["tx"] and envelope.take(32) == s["digest"] and envelope.number() == s["index"], "CDC outer original identity")
                logical = body(envelope.field(), b"GCD\5"); envelope.end()
                require(logical[:16] == GRAPH and int.from_bytes(logical[16:24], "big") == key[0], "native typed CDC namespace")
        elif outcome[:4] == b"GSO\1":
            b = body(outcome, b"GSO\1")
            require(len(b) == 18 and int.from_bytes(b[10:18], "big") == key[1] and not cdc, "literal cut control output")
        elif outcome[:4] == b"GMO\1":
            b = body(outcome, b"GMO\1")
            require(len(b) == 154 and b[:16] == GRAPH and int.from_bytes(b[16:24], "big") == key[0] and int.from_bytes(b[74:82], "big") == key[1] and not cdc, "literal range control output")
        else:
            raise ValueError("P05 refused: uncovered native output codec")
    for partition in (3, 8):
        indexes = [r["Index"] for r in encoded if r["Partition"] == partition]
        require(indexes and indexes == list(range(indexes[0], indexes[-1] + 1)), "gap/reordering in captured retained prefix")
    for tx, decision in decisions.items():
        m = manifests[tx][1]
        inventory = {(part, phase) for identity, part, phase in phases if identity == tx}
        require(inventory == {(m["coordinator"], 1), (3, 2), (8, 2), (m["coordinator"], 3), (3, 4), (8, 4)}, "incomplete original phase inventory")
        votes = [status(phases[(tx, p, 2)]) for p in (3, 8)]
        require(decision["commit"] == all(v["yes"] for v in votes), "actual decision/vote mismatch")
        require(not decision["commit"] or all(decision["round"] > v["floor"] for v in votes), "actual COMMIT below voted floor")
        for partition in (3, 8):
            resolved = status(phases[(tx, partition, 4)])
            require((resolved["commit"], resolved["round"]) == (decision["commit"], decision["round"]), "durable decision/Resolve changed")
    return retained, decisions


def projected(observation):
    native, census = (json.loads(raw(observation[k])) for k in ("Native", "Census"))
    require(native["graph"] == census["graph"] == GRAPH.hex() and [e["id"] for e in census["entityCensus"]] == list(range(1, 33)), "complete native observed census")
    result = {}
    def add(owner, name, value):
        key = ("A" if owner < 17 else "B") + "/" + name
        require(key not in result, "duplicate observed map key")
        result[key] = value
    for item in census["entityCensus"]:
        add(item["id"], "entity/" + str(item["id"]), item)
    require([e["id"] for e in native["entities"]] == [1, 17, 3] and len(native["lives"]) == 4 and len(native["postings"]) == 5, "complete native endpoint slice")
    for name, items, keys in (("native", native["entities"], ("id",)), ("life", native["lives"], ("owner", "life")), ("posting", native["postings"], ("endpoint", "life"))):
        for item in items:
            add(item[keys[0]], name + "/" + "/".join(str(item[k]) for k in keys), item)
    require(len(observation["Cells"]) == 16 and len(observation["Presence"]) == 8, "complete temporal observed cells")
    for cell in observation["Cells"]:
        add(cell["Owner"], f'cell/{cell["Owner"]}/{cell["Life"]}/{cell["Name"]}/{cell["Point"]}', cell)
    for presence in observation["Presence"]:
        add(presence["Owner"], f'presence/{presence["Owner"]}/{presence["Point"]}', presence)
    for name in ("Keys", "NodeUnique", "RelUnique", "AbsentEntity", "AbsentValue", "AbsentLife"):
        add(9, "footprint/" + name, observation[name])
    require(all(not e["found"] for e in native["phantomEntities"]), "phantom native entity")
    return result


def check(data):
    require(data["Schedule"] in SCHEDULES, "finite schedule")
    scopes, old_round, old_certificates = cut(data["Cut"])
    initial_condition = condition(raw(data["Condition"]))
    require([p[0] for p in initial_condition] == old_certificates, "captured condition/old cut identity")
    manifests, replies, conflicts, captures = frames(data["Phases"], scopes)
    require(set(initial_condition) <= captures, "old condition lacks exact native capture replies")
    retained, decisions = records(data["Records"], manifests)
    for s, reply in replies:
        require(raw(retained[(s["partition"], s["index"])]["Outcome"]) == reply, "reply differs from actual retained original")
    for control in data["Controls"] or []:
        require(retained[(control["Partition"], control["Index"])] == control, "range control/raw prefix differs")
    initial, current = projected(data["Old"]), projected(data["Current"])
    require(projected(data["Retained"]) == initial and data["Retained"] == data["Old"], "current substituted for retained old history")
    transactions, rounds, observations, step_txs = {}, {}, [], set()
    previous = initial
    for step in data["Steps"] or []:
        before, after = projected(step["Before"]), projected(step["After"])
        require(before == previous, "observed committed-step continuity")
        r = result(raw(step["Result"]), scopes)
        m = manifests[r["tx"]][1]
        require(r["decision"] == decisions[r["tx"]] and r["decision"]["commit"] and not r["pending"] and r["business"] == m["business"], "actual committed step result/original")
        require(normalized_business(raw(step["Original"]), m["coordinator"]) == normalized_business(m["original"], m["coordinator"]), "complete source business/retained inner original")
        require(r["tx"] not in step_txs and step["Name"] not in transactions, "duplicate observed business step")
        step_txs.add(r["tx"])
        changed = {k for k in before.keys() | after.keys() if k not in before or k not in after or before[k] != after[k]}
        require(changed, "committed step has no observed footprint")
        transactions[step["Name"]] = {"reads": [{"kind": "range", "lo": "A/", "hi": "B0", "values": before}], "writes": {k: {"op": "put", "value": after[k]} if k in after else {"op": "delete"} for k in changed}}
        rounds[step["Name"]] = r["decision"]["round"]
        _, round_, _ = cut(step["Cut"])
        observations.append({"event": step["Name"], "scope": ["A", "B"], "round": round_, "values": after})
        for record in step["Records"]:
            require(retained[(record["Partition"], record["Index"])] == record, "step/raw prefix correspondence")
        previous = after
    require(previous == current, "unknown observed business changes")
    require(step_txs == {tx for tx, d in decisions.items() if d["commit"] and d["round"] > old_round}, "missing/extra post-cut business step")
    for attempt in data["Attempts"]:
        r = result(raw(attempt["Result"]), scopes)
        if r["decision"]:
            require(r["decision"] == decisions[r["tx"]] and r["business"] == manifests[r["tx"]][1]["business"], "original recovery result/condition changed")
        elif attempt["Conflict"] or attempt["Closed"] or attempt["Expired"]:
            require(r["tx"] not in decisions and all(s["tx"] != r["tx"] for s, _ in replies), "preflight refusal registered/prepared")
        elif attempt["Invalid"]:
            require(r["tx"] in manifests and r["business"] != manifests[r["tx"]][1]["business"], "changed-condition original refusal lacks distinct business binding")
        else:
            require(attempt["Unavailable"] or r["pending"], "unclassified absent decision")
    if data["Schedule"] != "authority-fence-before-prepare":
        require(conflicts > 0, "preflight conflict lacks literal conditional capture reply")
    witness, detail = oracle()({"initial": initial, "transactions": transactions}, set(transactions), rounds, [{"event": "held-old", "scope": ["A", "B"], "round": old_round, "values": initial}] + observations, [{"event": "current", "scope": ["A", "B"], "values": current}])
    require(witness is not None, "independent observed serial/cut witness: " + str(detail))
    return {"schedule": data["Schedule"], "serial_witness": witness, "committed_steps": len(transactions), "records": len(retained), "scope": "P05 finite raw-binding and observed serial/cut bridge; native extraction/typed graph/CDC authority remain Go-qualified"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+", type=Path)
    reports = []
    for path in parser.parse_args().artifacts:
        wire = path.read_bytes()
        require(len(wire) <= 8 << 20, "finite artifact bound")
        report = check(json.loads(wire))
        report.update(path=str(path), sha256=hashlib.sha256(wire).hexdigest())
        reports.append(report)
    print(json.dumps(reports, indent=2))


if __name__ == "__main__":
    main()
