#!/usr/bin/env python3
"""P05 raw-original and observed-history bridge to the pinned serial/cut oracle.

Only raw codec helpers are borrowed. No P06/fault acceptance wrapper or protocol
reducer runs. Native authenticity, complete typed CDC decoding, current authority
and durable process behavior require the separately qualified Go capture. Empty
raw outputs stay opaque; this bridge never supplies missing events or decisions.
"""
import argparse
import copy
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
    owner_lives = ((1, 2), (17, 18), (3, 4), (9, 10))
    require(len(observation["Cells"]) == 16 and {(c["Owner"], c["Life"], c["Name"], c["Point"]) for c in observation["Cells"]} == {(o, l, n, p) for o, l in owner_lives for n in ("p", "u") for p in (0, 2)}, "complete fixed temporal cell inventory")
    require(len(observation["Presence"]) == 8 and {(c["Owner"], c["Point"]) for c in observation["Presence"]} == {(o, p) for o, _ in owner_lives for p in (0, 2)}, "complete fixed presence inventory")
    for cell in observation["Cells"]:
        add(cell["Owner"], f'cell/{cell["Owner"]}/{cell["Life"]}/{cell["Name"]}/{cell["Point"]}', cell["Cell"])
    for presence in observation["Presence"]:
        add(presence["Owner"], f'presence/{presence["Owner"]}/{presence["Point"]}', presence)
    for name in ("Keys", "NodeUnique", "RelUnique", "AbsentEntity", "AbsentValue", "AbsentLife"):
        add(9, "footprint/" + name, observation[name])
    require(all(not e["found"] for e in native["phantomEntities"]), "phantom native entity")
    return result


def operations(wire):
    """Bounded original codec inspection, not planning or predicate evaluation."""
    c = Cursor(body(pgq(wire)["inner"], b"PGO\1"))
    c.take(41); c.number(); provenance = c.number()
    require(c.number(4) == 1, "fixture has exactly one declared axis")
    axis, profile, version = c.take(16), c.number(1), c.number(2)
    c.field(); c.field()
    require(profile == 2 and version == 1, "fixture rational axis")
    count = c.number(4); require(1 <= count <= 4, "fixture operation count")
    ops = []
    for _ in range(count):
        kind, owner, life = c.number(1), c.number(), c.number()
        require(c.number(4) == 1, "operation axis index")
        scope = c.field()
        require(scope[:4] == b"TS\1\2" and scope[4:20] == axis, "original operation scope identity")
        record = (c.number(), c.number(1), c.number(4), c.field(), c.number(), c.number(), c.number(1), c.number(1), c.number(1))
        binding = tuple(c.number() for _ in range(4))
        name, scalar, scalar_axis, value, present = c.field(), c.field(), c.number(4), c.number(), c.number(1)
        require(scalar_axis == 0 and present == 0, "fixture scalar/operation flags")
        ops.append(dict(kind=kind, owner=owner, life=life, scope=scope, record=record, binding=binding, name=name, scalar=scalar, value=value))
    claims = c.number(4); require(claims <= 6, "fixture finite fresh claims")
    c.take(65 * claims); c.end()
    return provenance, ops


def native_cell(handle, revision, provenance, kind="i64", value=0, present=True):
    return dict(present=present, null=False, handle=handle, revision=revision, provenance=provenance, kind=kind, i64=value)


def scalar_keys(owner, life, name):
    group = "A" if owner < 17 else "B"
    return {f"{group}/cell/{owner}/{life}/{name}/{at}" for at in (0, 2)}


def checked_writes(before, after, expected):
    """Exact fixture literals: neither arbitrary changed keys nor After values."""
    require(before.keys() == after.keys(), "observed footprint changed shape")
    changed = {k for k in before if before[k] != after[k]}
    require(changed == expected.keys(), "collateral/missing original operation effects")
    require(all(after[k] == value for k, value in expected.items()), "original operation literal/result mismatch")
    return {k: {"op": "put", "value": copy.deepcopy(value)} for k, value in expected.items()}


def conditional_reads(condition_wire, own_cut, own_observation, before):
    """Use this original's captured read, not a later planning observation."""
    observed = projected(own_observation)
    require([p[0] for p in condition(condition_wire)] == cut(own_cut)[2] and before == observed, "conditional reads replaced by later planning observation")
    return [{"kind": "range", "lo": "A/", "hi": "B0", "values": observed}]


def scalar_states(step, op, round_, provenance, reference):
    """Check fixed All or [2,3] fixture SC1 bytes; never replay a reducer."""
    before = {x["Point"]: x for x in step["Before"]["Cells"] if (x["Owner"], x["Life"], x["Name"]) == (op["owner"], op["life"], op["name"].decode())}
    after = {x["Point"]: x for x in step["After"]["Cells"] if (x["Owner"], x["Life"], x["Name"]) == (op["owner"], op["life"], op["name"].decode())}
    require(set(before) == set(after) == {0, 2}, "complete scalar point observations")
    old, actual = raw(before[0]["State"]), raw(after[0]["State"])
    require(old == raw(before[2]["State"]) and actual == raw(after[2]["State"]) and old[:4] == actual[:4] == b"SC\1\2", "component state identity")
    require(old[4:52] == actual[4:52] == op["scope"][4:52], "component/original axis binding")
    updated = b"\1\2" + struct.pack(">QQQQ", reference, 9, round_, provenance)
    scope = op["scope"]
    if scope[52:] == b"\6":
        entries = [(scope, updated)]
    else:
        # Literal closed rational [2,3] and its two exclusive complements.
        two, three = b"\1\0\1\2\1\0\1\1", b"\1\0\1\3\1\0\1\1"
        require(scope[52:] == b"\4\x82" + two + b"\x82" + three, "fixture partial scope changed")
        cell = before[0]["Cell"]
        require(cell["present"] and cell["i64"] == 5 and cell["handle"] == 5 and cell["provenance"] == 3, "ABA fixed original outside cell")
        outside = b"\1\2" + struct.pack(">QQQQ", 5, 9, cell["revision"], 3)
        entries = [(scope[:52] + b"\4\1\2" + two, outside), (scope, updated), (scope[:52] + b"\4\2" + three + b"\3", outside)]
    expected = old[:52] + struct.pack(">I", len(entries)) + b"".join(field(s) + cell for s, cell in entries)
    require(actual == expected, "unobserved/extra scalar history or incorrect literal revision")


def fixture_writes(step, m, round_, schedule):
    before, after = projected(step["Before"]), projected(step["After"])
    provenance, ops = operations(raw(step["Original"]))
    written_components = {(o["owner"], o["life"], o["name"].decode()) for o in ops if o["kind"] == 8}
    old_states = {(c["Owner"], c["Life"], c["Name"], c["Point"]): raw(c["State"]) for c in step["Before"]["Cells"]}
    require(all(raw(c["State"]) == old_states[(c["Owner"], c["Life"], c["Name"], c["Point"])] for c in step["After"]["Cells"] if (c["Owner"], c["Life"], c["Name"]) not in written_components), "collateral history outside original Set footprint")
    expected = {}
    zero_record, zero_binding = (0, 0, 0, b"", 0, 0, 0, 0, 0), (0, 0, 0, 0)
    name = step["Name"]
    if name in ("conditional-winner", "conditional-re-evaluation", "native-ABA-4", "native-ABA-5"):
        require(len(ops) == 1, "single original Set")
        op = ops[0]
        owner = 17 if schedule == "write-skew-B-first" or name == "conditional-re-evaluation" else 1 if name == "conditional-winner" else 3
        life, value, requested = owner + 1, 6 if name == "native-ABA-4" else 5, 21 if schedule == "write-skew-B-first" else 6 if name == "native-ABA-4" else 5
        require((op["kind"], op["owner"], op["life"], op["name"], op["scalar"], op["value"], op["record"], op["binding"]) == (8, owner, life, b"p", b"\4" + struct.pack(">q", value), requested, zero_record, zero_binding), "Set original differs from fixed fixture")
        require(provenance == (104 if name == "native-ABA-4" else 105 if name == "native-ABA-5" else 101), "Set provenance")
        reference = 11 if name == "conditional-re-evaluation" else requested
        scalar_states(step, op, round_, provenance, reference)
        for key in scalar_keys(owner, life, "p"):
            if key.endswith("/0") and name.startswith("native-ABA"):
                continue
            expected[key] = native_cell(reference, round_, provenance, value=value)
    elif name == "unique-insertion":
        require(provenance == 102 and len(ops) == 4, "unique insertion original")
        signatures = [(o["kind"], o["owner"], o["life"], o["name"], o["scalar"], o["value"]) for o in ops]
        require(signatures == [(1, 9, 10, b"", b"", 0), (8, 9, 10, b"u", b"\4" + struct.pack(">q", 5), 11), (2, 3, 4, b"", b"", 0), (8, 3, 4, b"u", b"\4" + struct.pack(">q", 6), 12)], "insertion operation footprint")
        require(ops[0]["record"] == (0, 0, 0, b"", 0, 0, 0, 3, 1) and ops[2]["record"] == (0, 0, 0, b"edge", 1, 17, 1, 6, 2) and ops[2]["binding"] == (0, 0, 2, 18), "insertion immutable endpoint/declaration binding")
        for owner, life, reference, value in ((9, 10, 11, 5), (3, 4, 12, 6)):
            op = next(o for o in ops if o["owner"] == owner and o["kind"] == 8)
            require(op["record"] == zero_record and op["binding"] == zero_binding, "Set adds undeclared structure")
            scalar_states(step, op, round_, provenance, reference)
            for key in scalar_keys(owner, life, "u"):
                expected[key] = native_cell(reference, round_, provenance, value=value)
            for at in (0, 2):
                key = f"A/presence/{owner}/{at}"
                expected[key] = dict(Owner=owner, Point=at, Cell=native_cell(life, round_, provenance, "life"), Effective=True, Life=life)
            key = f"A/entity/{owner}"
            expected[key] = copy.deepcopy(before[key])
            expected[key].update(found=True, kind=1 if owner == 9 else 2, source=0 if owner == 9 else 1, target=0 if owner == 9 else 17, mode=0 if owner == 9 else 1, type="" if owner == 9 else "edge")
        expected["A/native/3"] = dict(id=3, found=True, kind=2, source=1, target=17, mode=1, type="edge", presence=native_cell(4, round_, provenance, "life"), effective=True, effectiveLife=4, labels=None, property=native_cell(0, 0, 0, "absent", present=False))
        expected["A/life/3/4"] = dict(owner=3, life=4, found=True, sourceLife=2, targetLife=18)
        for owner, life in ((1, 0), (1, 2), (17, 0), (17, 18)):
            prefix = "A" if owner == 1 else "B"
            expected[f"{prefix}/posting/{owner}/{life}"] = dict(endpoint=owner, life=life, candidates=[3], declared=[3], effective=[3])
        key = dict(Owner=9, Life=10, Kind=3, Name="u", Member=0)
        expected["A/footprint/Keys"] = [key]
        expected["A/footprint/NodeUnique"] = [dict(Owner=9, Life=10, Key=key)]
        expected["A/footprint/RelUnique"] = [dict(Owner=3, Life=4, Key=dict(Owner=3, Life=4, Kind=3, Name="u", Member=0))]
        for flag in ("AbsentEntity", "AbsentValue", "AbsentLife"):
            expected["A/footprint/" + flag] = False
    else:
        require(name in ("native-ABA-6", "native-ABA-7") and len(ops) == 1, "unknown fixed operation")
        op, closed = ops[0], name == "native-ABA-6"
        require((op["kind"], op["owner"], op["life"], op["name"], op["scalar"], op["value"], op["record"], op["binding"], provenance) == (4 if closed else 3, 1, 2 if closed else 7, b"", b"", 0, zero_record, zero_binding, 106 if closed else 107), "Close/Reopen original changed")
        require(op["scope"][52:] == b"\4\x82\1\0\1\2\1\0\1\1\x82\1\0\1\3\1\0\1\1", "Close/Reopen partial scope")
        expected["A/presence/1/2"] = dict(Owner=1, Point=2, Cell=native_cell(0 if closed else 7, round_, provenance, "absent" if closed else "life", present=not closed), Effective=not closed, Life=0 if closed else 7)
        if closed:
            expected["A/presence/3/2"] = copy.deepcopy(before["A/presence/3/2"])
            expected["A/presence/3/2"]["Effective"] = False
        else:
            expected["A/life/1/7"] = dict(owner=1, life=7, found=True, sourceLife=0, targetLife=0)
    return checked_writes(before, after, expected)


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
    steps = data["Steps"] or []
    expected_steps = (["conditional-winner"] if data["Schedule"].startswith("write-skew") else ["unique-insertion", "conditional-re-evaluation"] if data["Schedule"] == "empty-range-and-absent-key-insertion" else [f"native-ABA-{i}" for i in (4, 5, 6, 7)] if data["Schedule"] == "temporal-life-value-ABA" else [])
    require([s["Name"] for s in steps] == expected_steps, "fixed schedule operation inventory")
    for index, step in enumerate(steps):
        before, after = projected(step["Before"]), projected(step["After"])
        require(before == previous, "observed committed-step continuity")
        r = result(raw(step["Result"]), scopes)
        m = manifests[r["tx"]][1]
        require(r["decision"] == decisions[r["tx"]] and r["decision"]["commit"] and not r["pending"] and r["business"] == m["business"], "actual committed step result/original")
        require(normalized_business(raw(step["Original"]), m["coordinator"]) == normalized_business(m["original"], m["coordinator"]), "complete source business/retained inner original")
        require(r["tx"] not in step_txs and step["Name"] not in transactions, "duplicate observed business step")
        step_txs.add(r["tx"])
        conditional = step["Name"].startswith("conditional-")
        require(bool(m["condition"]) == conditional, "conditional source original lost its read condition")
        if conditional:
            own_cut = data["Cut"] if step["Name"] == "conditional-winner" else steps[index - 1]["Cut"]
            own_observation = data["Old"] if step["Name"] == "conditional-winner" else steps[index - 1]["After"]
            reads = conditional_reads(m["condition"], own_cut, own_observation, before)
        else:
            # Only fixed native intrinsic dependencies for these unconditional
            # originals; no earlier client predicate is inferred or revalidated.
            keys = (["A/entity/1", "B/entity/17", "A/life/1/2", "B/life/17/18", "A/entity/9", "A/entity/3", "A/footprint/NodeUnique", "A/footprint/RelUnique"] if step["Name"] == "unique-insertion" else ["A/native/3", "A/life/3/4", "A/cell/3/4/p/2"] if step["Name"] in ("native-ABA-4", "native-ABA-5") else ["A/entity/1", "A/presence/1/2", "A/life/1/2" if step["Name"] == "native-ABA-6" else "A/life/1/7"])
            reads = [{"kind": "key", "key": k, "exists": True, "value": before[k]} for k in keys]
        transactions[step["Name"]] = {"reads": reads, "writes": fixture_writes(step, m, r["decision"]["round"], data["Schedule"])}
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
