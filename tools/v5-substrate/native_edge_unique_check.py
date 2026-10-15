#!/usr/bin/env python3
"""Bounded raw-codec/fixed-footprint oracle for the actual native edge family.

No legacy family acceptance wrapper or second graph reducer is invoked. Raw
native originals, canonical operation bytes and CC1 predecessor cells are joined
in one caller-granted disposable spool. Actual native hydration remains the Go
Host/exporter boundary; checksums alone do not confer portable source authority.
"""
import base64
from contextlib import nullcontext
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import struct
import stat
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from comparison_spool import Policy, Spool, u64
import native_columns_check as column


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT/filename)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


raw = module("edge_native_raw", "compare-cost-cdc.py")
typed = module("edge_native_typed_codec", "check-p05-history.py")
require, integer, fields, blob, encoded = raw.require, raw.integer, raw.fields, raw.raw_bytes, raw.go_json
FORMAT, FAMILY, LINE, MAX_SECONDS = "native-edge-endpoint-unique-v1", "cross-edge-endpoint-uniqueness", 512 << 10, 365*24*60*60
TRANSPORT_FORMAT = "native-edge-endpoint-unique-transport-v2"
TRANSPORTS = {"local": 0, "rtt-1ms": 1, "rtt-20ms": 20, "rtt-100ms": 100}
ZERO = {"Present": False, "Null": False, "Handle": 0, "Value": 0, "Revision": 0, "Provenance": 0}


def digest(body): return hashlib.sha256(body).digest()
def array(value, size): return column.array(raw, value, size)
def checked(body, magic, maximum=32768): return column.checked_body(raw, body, magic, maximum)
def cursor(body): return column.Cursor(raw, body)
def optional(value): return b"" if value is None else blob(value)


def decoded(body):
    require(type(body) is bytes and 0 < len(body) <= LINE, "bounded raw JSON ownership")
    value = json.loads(body, object_pairs_hook=raw.pairs, parse_constant=lambda _: require(False, "nonfinite JSON"))
    require(encoded(value) == body, "canonical complete JSON")
    return value


def lines(filename, maximum):
    with Path(filename).open("rb") as source:
        for _ in range(integer(maximum, 1)):
            body = source.readline(LINE+1)
            if not body: return
            require(len(body) <= LINE and body.endswith(b"\n"), "complete bounded JSONL")
            yield decoded(body[:-1])
        require(not source.read(1), "finite family stream work grant")


def shape(s):
    fields(s, "version family facts crossPercent writers layout")
    require(integer(s["version"], 1, 1) and s["family"] == FAMILY and integer(s["facts"], 20, 1_000_000) % 20 == 0
            and integer(s["crossPercent"]) in (0, 10, 50, 100) and integer(s["writers"]) in (1, 8, 32)
            and s["layout"] in ("kv", "columns"), "complete fresh native spec")
    return s


def frames(s):
    n = shape(s)["facts"]
    return (9*n+7)*10+3*n+2*n+4*((n+15)//16+16)+256+4*32+64*10+128


def causal_minimum(s, rtt_millis):
    shape(s)
    require(type(rtt_millis) is int and rtt_millis in TRANSPORTS.values(), "actual native edge RTT profile")
    # The N FreezeOld source captures and seven setup/life Runs form actual
    # sequential chains. Each requires >=one remote authenticated source RTT.
    return (s["facts"]+7)*rtt_millis


def owned_transport(receipts, s):
    require(type(receipts) is list and len(receipts) == 2, "both actual transport process cohorts")
    selected, maximum = None, frames(s)*4096
    for r in receipts:
        fields(r, "Version Spec Raft SourceRequest SourceReply MaximumHeldFrames MaximumHeldBytes MetadataBytes PipeWrites PipeReads Scope")
        t = r["Spec"]
        fields(t, "kind targetRTTMillis jitterMillis seed")
        require(integer(r["Version"], 1, 1) and integer(t["seed"], 20261009, 20261009)
                and integer(t["jitterMillis"], 0, 0) == 0 and integer(t["targetRTTMillis"]) in TRANSPORTS.values()
                and t["kind"] == ("local" if t["targetRTTMillis"] == 0 else "injected-rtt"), "actual fixed native owned transport")
        require(selected is None or selected == t, "restart cannot change admitted transport")
        selected = t
        require(integer(r["MetadataBytes"], 32768, 32768) and integer(r["MaximumHeldFrames"], 1, 128)
                and integer(r["MaximumHeldBytes"], 32768, 8 << 20)
                and "not native delivery/ACK" in r["Scope"] and "no physical network claim" in r["Scope"], "bounded retained scheduler ownership and truthful delay scope")
        half = t["targetRTTMillis"]*500000
        for name in ("Raft", "SourceRequest", "SourceReply"):
            l = r[name]
            fields(l, "Admitted Completed Canceled Bytes ConfiguredNanos ObservedNanos DeviationNanos MinimumObservedNanos MaximumObservedNanos")
            for value in l.values(): integer(value)
            require(0 < l["Admitted"] <= maximum and l["Completed"]+l["Canceled"] == l["Admitted"]
                    and l["Bytes"] <= l["Admitted"]*(2 << 20) and l["ConfiguredNanos"] == l["Admitted"]*half
                    and l["MinimumObservedNanos"] <= l["MaximumObservedNanos"] <= l["ObservedNanos"]
                    and (not l["Completed"] or l["MinimumObservedNanos"] >= half), "actual settled half-RTT/cancellation/counter union")
        for name in ("PipeWrites", "PipeReads"):
            l = r[name]
            fields(l, "AttemptedFrames CompleteFrames FailedFrames PrepaidBytes ActualBytes")
            for value in l.values(): integer(value)
            require(0 < l["AttemptedFrames"] <= maximum and l["CompleteFrames"]+l["FailedFrames"] == l["AttemptedFrames"]
                    and 0 < l["ActualBytes"] <= l["PrepaidBytes"] <= l["AttemptedFrames"]*((2 << 20)+4), "actual bounded parent/child framed IO including failure/Close")
    return selected


def cut(value):
    c = cursor(checked(blob(value), b"GEQ\1", 16384))
    require(c.number(1) == 3 and not any(c.take(16)) and c.number() == 0 and c.number(4) == 2, "full actual At cut")
    result = [column.certificate(raw, base64.b64encode(c.field(512)).decode()) for _ in range(2)]
    c.done()
    require([v["partition"] for v in result] == [3, 8] and result[0]["round"] == result[1]["round"], "complete equal-round certified cut")
    return result


def operation(s, q):
    fields(q, "Version Family Phase Fact Candidate Ordinal Reopened")
    n, phase, fact, candidate = s["facts"], q["Phase"], integer(q["Fact"]), integer(q["Candidate"], 0, 1)
    require(type(q["Reopened"]) is bool and integer(q["Version"], 1, 1) and q["Family"] == FAMILY, "actual factory family")
    if phase == "setup":
        integer(fact, 0, 2); require(candidate == 0 and not q["Reopened"], "setup shape")
        ordinal = 1+fact
    elif phase == "life":
        integer(fact, 0, 3); require(candidate == 0 and q["Reopened"] == (fact % 2 == 1), "genuine close/new-life order")
        ordinal = 4+6*n+fact
    else:
        integer(fact, 0, n-1)
        require(phase in ("claim", "stale", "replace", "frozen-close", "close") and q["Reopened"] == (phase == "replace"), "edge phase")
        ordinal = 4+2*fact+candidate if phase == "claim" else 4+{"stale": 2, "replace": 3, "frozen-close": 4, "close": 5}[phase]*n+fact
    require(integer(q["Ordinal"], 1) == ordinal, "exact native operation ordinal")
    return phase, fact, candidate, ordinal


def endpoint(s, fact, reopened):
    remote = (fact+1)*s["crossPercent"]//100 > fact*s["crossPercent"]//100
    return (5*s["facts"]+8, 5*s["facts"]+9+int(reopened)) if remote else (3, 4*s["facts"]+7 if reopened else 4)


def original_proposal(body, q, s):
    parsed = typed.pgq(body)
    provenance, ops = typed.operations(body)
    phase, fact, candidate, ordinal = operation(s, q)
    require(parsed["kind"] == 6 and provenance == ordinal, "original native operation/provenance")
    if phase in ("setup", "life"):
        owner, life = ((1, 2), (3, 4), (5*s["facts"]+8, 5*s["facts"]+9))[fact] if phase == "setup" else ((3, 4*s["facts"]+7 if fact % 2 else 4) if fact < 2 else (5*s["facts"]+8, 5*s["facts"]+9+fact % 2))
        require(parsed["partition"] == (8 if owner == 5*s["facts"]+8 else 3) and len(ops) == 1
                and (ops[0]["kind"], ops[0]["owner"], ops[0]["life"]) == (1 if phase == "setup" else 3 if fact % 2 else 4, owner, life)
                and not ops[0]["name"] and not ops[0]["scalar"] and ops[0]["value"] == 0, "exact original node lifecycle")
    else:
        owner = 7+4*fact+2*candidate
        require(parsed["partition"] == 3, "relationship source coordinator")
        if phase in ("close", "frozen-close"):
            require(len(ops) == 1 and (ops[0]["kind"], ops[0]["owner"], ops[0]["life"]) == (4, owner, owner+1)
                    and not ops[0]["name"] and not ops[0]["scalar"] and ops[0]["value"] == 0, "exact old incumbent Close")
        else:
            target, life = endpoint(s, fact, phase == "replace")
            require(len(ops) == 2 and [(o["kind"], o["owner"], o["life"]) for o in ops] == [(2, owner, owner+1), (8, owner, owner+1)]
                    and ops[0]["record"][3:] == (b"edge", 1, target, 1, 6, 2)
                    and ops[0]["binding"] == (0, 0, 2, life) and not ops[0]["name"] and not ops[0]["scalar"] and ops[0]["value"] == 0
                    and ops[1]["name"] == b"p" and ops[1]["scalar"] == b"\4"+u64(fact+5)
                    and ops[1]["value"] == 4*s["facts"]+8+fact, "full original distinct-life/scalar footprint")
    return parsed


def status(body):
    c = cursor(checked(body, b"GCS\1", 135))
    require(c.take(16).hex() == raw.GRAPH, "native status namespace")
    part, tx, hash_ = c.number(), c.take(16), c.take(32)
    phase, index, floor, round_, flags, reason = c.number(1), c.number(), c.number(), c.number(), c.number(1), c.number(1)
    c.done()
    require(part in (3, 8) and any(tx) and any(hash_) and phase in (1, 2, 3, 4) and index > 0 and flags <= 3 and reason in (0, 1, 2, 3, 4, 5, 6, 8), "native phase/status union")
    return {"part": part, "tx": tx.hex(), "digest": hash_.hex(), "phase": phase, "index": index, "floor": floor, "round": round_, "yes": bool(flags & 1), "commit": bool(flags & 2), "reason": reason}


def manifest(body):
    m = typed.manifest(body)
    require(m["condition"] is None, "this family uses genuine frozen GCM1, not invented conditional reads")
    return m


def logical_key(role, q): return role.encode()+b"/"+u64(q["Fact"])+u64(q["Candidate"])


def caller(s, q, role, client):
    phase, fact, _, ordinal = operation(s, q)
    require(role == phase or role == "occupied" and phase == "claim", "explicit diagnostic or genuine business role")
    expected = fact % s["writers"] if role == "occupied" else (ordinal-1) % s["writers"]
    require(integer(client, 0, s["writers"]-1) == expected, "exact logical-ordinal independent caller assignment")
    return expected


def writer_census(s, expected, completed):
    total, writers, facts = 6*s["facts"]+7, s["writers"], s["facts"]
    require(type(completed) is list and len(completed) == 32
            and all(type(v) is int and v >= 0 for v in completed)
            and completed == expected and all(v > 0 for v in completed[:s["writers"]])
            and not any(completed[s["writers"]:]), "exact bounded actual caller census")
    for client in range(writers):
        base = (total+writers-1-client)//writers
        diagnostic = (facts+writers-1-client)//writers
        require(base+diagnostic <= completed[client] <= 2*base+diagnostic, "finite logical jobs, one occupied diagnostic and at most one linked priority per original")


def operations(directory, end, spool):
    counts, completed, extras = {"commit": 0, "abort": 0, "refusal": 0}, [0]*32, 0
    for a in lines(directory/"operations.jsonl", 10*end["spec"]["facts"]+8):
        q, s = a["Operation"], end["spec"]
        phase, fact, candidate, ordinal = operation(s, q)
        role = a["Role"]
        require(role == phase or role == "occupied" and phase == "claim", "explicit diagnostic or genuine business role")
        require(a["Family"] == FAMILY and a["Engine"] == "native-raft" and a["Error"] == "", "actual completed invocation")
        client, number = caller(s, q, role, a["Client"]), integer(a["Ordinal"], 1, 2)
        completed[client] += 1
        body = blob(a["Proposal"])
        require(digest(body) == array(a["BodySHA256"], 32), "entire original request digest")
        p = original_proposal(body, q, s)
        request = array(a["Request"], 16)
        require(p["request"] == request and any(request), "actual factory original request")
        key = logical_key(role, q)
        prior_wire = spool.find(b"logical", key)
        prior = None if prior_wire is None else decoded(prior_wire)
        require(prior is None and number == 1 and not any(array(a["Previous"], 16)) or prior is not None and number == 2 and prior["class"] == "abort" and array(a["Previous"], 16) == bytes.fromhex(prior["request"]), "only genuine settled ABORT permits linked priority")
        if prior is None:
            spool.insert(b"occupied-ordinal" if role == "occupied" else b"business-ordinal", u64(fact if role == "occupied" else ordinal), key)
        else:
            require(role != "occupied" and prior["client"] == client, "priority preserves actual original caller")
        for name in ("StartNanos", "StopNanos", "ElapsedNanos", "AdmissionNanos"): integer(a[name])
        require(0 < a["ElapsedNanos"] == a["StopNanos"]-a["StartNanos"] and a["AdmissionNanos"] <= a["ElapsedNanos"] and a["StopNanos"] <= end["TotalNanos"], "actual complete-operation/admission time")
        exchanges = a["Exchanges"]
        require(1 <= integer(a["Invocations"], 1, 8) == len(exchanges) <= 8 and all(e["Stage"] in ("register", "run") and blob(e["Reply"]) for e in exchanges), "all bounded same-original exchanges")
        extras += sum(max(0, sum(e["Stage"] == stage for e in exchanges)-1) for stage in ("register", "run"))
        classification = a["Classification"]
        if classification.startswith("pre-prepare-"):
            r = cursor(blob(a["Refusal"]))
            require(r.take(4) == b"GER1", "actual pre-registration refusal envelope")
            kind, scope, business, hash_, identity, tx, intent = r.number(1), checked(r.field(256), b"APS\1", 156), r.field(65536), r.take(32), r.take(16), r.take(16), r.take(32)
            r.done()
            require(kind in (1, 2) and scope == checked(blob(a["Target"]), b"APS\1", 156) and business == body and hash_ == digest(body) and identity == request
                    and tx == digest(b"rho-tkg:cross-transaction-id:v1\0"+p["graph"]+u64(p["partition"])+request)[:16]
                    and intent == digest(typed.normalized_business(body, p["partition"])), "exact refusal/body/intent is not a decision")
            require(classification == ("pre-prepare-unique" if kind == 1 else "pre-prepare-endpoint-life") and ((role == "occupied" and kind == 1) or phase == "stale"), "required authentic refusal semantics")
            counts["refusal"] += 1
        else:
            require(classification in ("commit", "abort"), "unknown is not an ABORT")
            proof = decoded(blob(a["Proof"]))
            fields(proof, "Manifest Outcome CDC")
            out, registered = blob(proof["Outcome"]), blob(proof["Manifest"])
            decision, m = status(out), manifest(registered)
            require(decision["phase"] == 3 and decision["tx"] == m["tx"].hex() and decision["digest"] == digest(registered).hex()
                    and decision["commit"] == (classification == "commit") and proof["CDC"] == ""
                    and typed.normalized_business(m["original"], p["partition"]) == typed.normalized_business(body, p["partition"]), "genuine first immutable decision/full business")
            if optional(a["Plan"]):
                plan = decoded(blob(a["Plan"]))
                require(blob(plan["Business"]) == body and blob(plan["Manifest"]) == registered and array(plan["ManifestSHA256"], 32) == digest(registered), "exact pre-ABA/empty-range frozen original")
            require(classification != "commit" or phase not in ("stale", "frozen-close") and role != "occupied", "stale/old-life original cannot install effects")
            txkey = m["tx"]
            spool.insert(b"terminal", txkey, encoded({"Operation": q, "Decision": proof["Outcome"], "Manifest": proof["Manifest"], "Proposal": a["Proposal"]}))
            counts[classification] += 1
        record = {"request": request.hex(), "class": "abort" if classification == "abort" else classification,
                  "start": a["StartNanos"] if prior is None else prior["start"], "stop": a["StopNanos"], "role": role, "client": client}
        raw.put_raw(spool, b"logical", key, encoded(record))
    require(counts == {"commit": end["Commits"], "abort": end["Aborts"], "refusal": end["Refusals"]}
            and sum(counts.values()) == end["Attempts"]
            and extras <= end["ExtraInvocations"] <= 64, "complete actual per-writer/original/extra-invocation census")
    writer_census(end["spec"], completed, end["Completed"])
    index = 0
    for index, (key, _) in enumerate(spool.walk(b"business-ordinal"), 1):
        require(int.from_bytes(key, "big") == index, "no missing or duplicated logical business ordinal")
    require(index == 6*end["spec"]["facts"]+7, "complete fixed logical business ordinal set")
    index = 0
    for index, (key, _) in enumerate(spool.walk(b"occupied-ordinal"), 1):
        require(int.from_bytes(key, "big") == index-1, "complete bounded occupied diagnostic fact set")
    require(index == end["spec"]["facts"], "one authentic occupied diagnostic per fact")
    count = 0
    for key, wire in spool.walk(b"logical"):
        r = decoded(wire)
        if r["role"] == "occupied": continue
        elapsed = r["stop"]-r["start"]
        require(elapsed > 0, "full linked logical latency")
        spool.insert(b"latency-order", u64(elapsed)+key, b"1")
        count += 1
    require(count == 6*end["spec"]["facts"]+7, "complete logical business stream")
    return counts


def row_key(row): return u64(row["Owner"])+u64(row["Life"])+bytes([row["Component"]])+row["Name"].encode()


def frame_digest(f):
    wire = u64(f["Partition"])+u64(f["Index"])+array(f["Root"], 172)
    for name in ("Outcome", "CDC", "Registration", "OriginalOutcome", "OriginalCDC"):
        value = optional(f[name]);require(len(value) <= 32768, "bounded raw field")
        wire += struct.pack(">I", len(value))+value
    rows = [{name: ({k: row[name][k] for k in column.CELL} if name in ("Before", "After") else row[name]) for name in column.ROW} for row in f["Rows"] or []]
    return digest(wire+encoded(rows))


def cell(c, component, s):
    present, kind = c.number(1), c.number(1)
    handle, payload, revision, provenance = [c.number() for _ in range(4)]
    require(present in (0, 1) and (present == 1 and kind == 2 and handle > 0 and payload == (9 if component == 3 else 0)
                                 or present == kind == handle == payload == 0), "canonical nonnull CC1 life/scalar cell")
    value = handle-(4*s["facts"]+8)+5 if present and component == 3 else 0
    require(not present or component != 3 or 5 <= value < s["facts"]+5, "native scalar identity, not caller DTO")
    return {"Present": bool(present), "Null": False, "Handle": handle, "Value": value, "Revision": revision, "Provenance": provenance}


def logical_rows(frame, decision, terminal, prior_root, s, spool):
    """Bind all declared rows to actual GCE/GCD/CC1 bytes before state folding."""
    outer = cursor(checked(blob(frame["CDC"]), b"GCE\1"))
    require(outer.number(1) == 6 and outer.take(16).hex() == decision["tx"] and outer.take(32).hex() == decision["digest"] and outer.number() == frame["Index"], "CDC immutable original correspondence")
    logical = outer.field(32768); outer.done()
    image = array(frame["Root"], 172)
    require(image[52:84] == digest(b"rho-tkg:graph-effects:v1\0"+prior_root[52:84]+u64(len(logical))+logical), "full native logical effect-chain transition")
    c = cursor(checked(logical, b"GCD\5"))
    require(c.take(16).hex() == raw.GRAPH and c.number() == frame["Partition"] and c.take(29) == bytes(29), "actual nonbootstrap semantic envelope")
    axes = []
    for _ in range(integer(c.number(4), 0, 1)):
        start = c.offset
        axis, profile, version = c.take(16), c.number(1), c.number(2)
        reference, unit = c.field(256), c.field(256)
        require(profile == 2 and version == 1 and reference.decode() == raw.SCOPE["reference"] and unit.decode() == raw.SCOPE["canonicalUnit"], "exact rational native axis")
        axes.append(c.body[start:c.offset])
    entities = []
    for _ in range(integer(c.number(4), 0, 2)):
        entity = (c.number(), c.number(1), c.number(4), c.field(256), c.number(), c.number(), c.number(1), c.number(1), c.number(1))
        entities.append(entity)
        require(entity[2] == 1 and axes, "native immutable entity axis")
    lives = [tuple(c.number() for _ in range(4)) for _ in range(integer(c.number(4), 0, 2))]
    values = []
    for _ in range(integer(c.number(4), 0, 2)):
        value_id, scalar, scalar_axis = c.number(), c.field(64), c.number(4)
        require(scalar_axis == 0 and len(scalar) == 9 and scalar[0] == 4, "canonical actual i64 dictionary definition")
        fact = value_id-(4*s["facts"]+8)
        require(0 <= fact < s["facts"] and scalar == b"\4"+u64(fact+5), "exact per-fact value definition")
        values.append(value_id)
        spool.insert(b"value-definition", u64(value_id), scalar)
    rows = []
    for _ in range(integer(c.number(4), 0, 2)):
        owner, life, component, name, member = c.number(), c.number(), c.number(1), c.field(256).decode(), c.number()
        axis_index, scope = c.number(4), c.field(256)
        require(component in (1, 3) and member == 0 and axis_index == 1 and len(axes) == 1, "exact native component/scope coordinate")
        axis = axes[0]
        all_scope = b"TS\1\2"+axis[:16]+digest(b"rho-tkg:axis:v1\0"+axis)+b"\6"
        require(scope == all_scope, "actual whole-axis native replacement")
        changes = cursor(c.field(4096))
        require(changes.take(4) == b"CC\1\2" and changes.take(16) == axis[:16] and changes.take(32) == digest(b"rho-tkg:axis:v1\0"+axis) and changes.number(4) == 1 and changes.field(256) == scope, "canonical complete changed-region encoding")
        before, after = cell(changes, component, s), cell(changes, component, s)
        changes.done()
        key = u64(owner)+u64(life)+bytes([component])+name.encode()
        rows.append((key, before, after, axis, scope))
    c.done()
    q = terminal["Operation"]
    phase, fact, candidate, ordinal = operation(s, q)
    part = frame["Partition"]
    node = phase in ("setup", "life")
    if node:
        owner, life = ((1, 2), (3, 4), (5*s["facts"]+8, 5*s["facts"]+9))[fact] if phase == "setup" else ((3, 4*s["facts"]+7 if fact % 2 else 4) if fact < 2 else (5*s["facts"]+8, 5*s["facts"]+9+fact % 2))
        local = part == (8 if owner == 5*s["facts"]+8 else 3)
        expected_keys = [u64(owner)+u64(0)+b"\1"] if local else []
        create, closing = phase == "setup", phase == "life" and fact % 2 == 0
        expected_entities = [(owner, 1, 1, b"", 0, 0, 0, 3, 1)] if local and create else []
        expected_lives = [(owner, life, 0, 0)] if local and not closing else []
        expected_values = []
    else:
        owner, life = 7+4*fact+2*candidate, 8+4*fact+2*candidate
        target, target_life = endpoint(s, fact, phase == "replace")
        closing = phase == "close"
        require(phase in ("claim", "replace", "close"), "refused stale/frozen originals have no effects")
        expected_keys = ([u64(owner)+u64(0)+b"\1"] if closing else [u64(owner)+u64(0)+b"\1", u64(owner)+u64(life)+b"\3p"]) if part == 3 else []
        expected_entities = [(owner, 2, 1, b"edge", 1, target, 1, 6, 2)] if part == 3 and not closing else []
        expected_lives = [(owner, life, 2, target_life)] if part == 3 and not closing else []
        expected_values = [4*s["facts"]+8+fact] if part == 3 and phase == "claim" else []
    require([r[0] for r in rows] == expected_keys and entities == expected_entities and lives == expected_lives and values == expected_values, "complete fixed operation footprint: no collateral catalog/component effects")
    actual_rows = frame["Rows"] or []
    require(len(actual_rows) == len(rows), "all decoded typed rows match actual raw CC1")
    for actual, (key, before, after, axis, scope) in zip(actual_rows, rows, strict=True):
        fields(actual, " ".join(column.ROW))
        require(row_key(actual) == key and actual["Before"] == before and actual["After"] == after and blob(actual["Axis"]) == axis and blob(actual["Scope"]) == scope, "typed cell values/coordinates differ from native CDC bytes")
        require(after["Revision"] > before["Revision"] and after["Provenance"] == ordinal and after["Present"] == (not closing) and after["Handle"] == (0 if closing else life if actual["Component"] == 1 else 4*s["facts"]+8+fact), "exact intended scalar/life/close replacement")
        require(actual["Kind"] == (1 if node else 2) and actual["Interpretation"] == (3 if node else 6) and actual["Role"] == (1 if node else 2)
                and actual["Name"] == ("p" if actual["Component"] == 3 else "") and actual["Type"] == ("" if node else "edge")
                and (actual["Source"], actual["Target"], actual["SourceLife"], actual["TargetLife"], actual["Mode"]) == ((0, 0, 0, 0, 0) if node else (1, target, 2, target_life, 1)), "full immutable entity/life decoration")
        spool.insert(b"ordered-row", u64(decision["round"])+u64(part)+key, encoded(actual))
        spool.insert(b"business-row", bytes.fromhex(decision["tx"])+key, digest(encoded(actual)))


def authenticate_frame(frame, prior_root, s, spool, fresh):
    fields(frame, "Partition Index Root Outcome CDC Registration OriginalOutcome OriginalCDC Rows")
    part, index, image = frame["Partition"], integer(frame["Index"], 1), array(frame["Root"], 172)
    column.root(raw, image, part)
    require(frame["Outcome"] is not None and frame["CDC"] is not None, "missing native frame is not present-empty")
    out, cdc, registered = optional(frame["Outcome"]), optional(frame["CDC"]), optional(frame["Registration"])
    if out.startswith(b"GCS\1"):
        st, m = status(out), manifest(registered)
        require(st["part"] == part and st["index"] <= index and bytes.fromhex(st["tx"]) == m["tx"] and bytes.fromhex(st["digest"]) == m["digest"], "native phase/registration coordinate")
        if st["index"] < index:
            require(not cdc and not frame["Rows"] and image == prior_root and optional(frame["OriginalOutcome"]) == out
                    and spool.lookup(b"original", u64(part)+u64(st["index"])) == digest(out)+digest(optional(frame["OriginalCDC"])), "exact replay original/non-effects")
        elif fresh:
            require(not optional(frame["OriginalOutcome"]) and not optional(frame["OriginalCDC"]), "new original cannot borrow a sidecar")
            if st["phase"] in (1, 3):
                terminal = decoded(spool.lookup(b"terminal", m["tx"]))
                require(m["coordinator"] == part and blob(terminal["Manifest"]) == registered, "every fresh Register/Decide belongs to the exact caller terminal roster; refusal cannot hide an installed original")
                if st["phase"] == 3: require(blob(terminal["Decision"]) == out, "actual immutable source decision equals terminal caller result")
            spool.insert(b"phase", u64(part)+m["tx"]+bytes([st["phase"]]), out)
            if st["phase"] == 4 and st["commit"]:
                terminal = decoded(spool.lookup(b"terminal", m["tx"]))
                require(blob(terminal["Manifest"]) == registered and status(blob(terminal["Decision"]))["commit"], "actual COMMIT Resolve joins original caller")
                logical_rows(frame, st, terminal, prior_root, s, spool)
            else:
                require(not cdc and not frame["Rows"] and image == prior_root, "protocol/ABORT cannot write business effects")
        return
    require(not frame["Rows"] and image == prior_root, "only joined native Resolve may change business state")
    if out.startswith(b"GPO\1"):
        p = cursor(checked(out, b"GPO\1", 256))
        phase, disposition, identity, hash_, action = p.number(1), p.number(1), p.take(32), p.take(32), p.take(16)
        original, publication, epoch, feed = [p.number() for _ in range(4)];p.done()
        require(1 <= phase <= 8 and 1 <= disposition <= 5 and any(identity) and any(hash_) and any(action) and 0 < original <= index, "actual native pin command result")
        if disposition == 2 and original == index:
            require(not cdc and not registered, "nonterminal pin pending is not an original")
        else:
            control = cursor(checked(registered, b"NCO1"))
            key, at, value = control.field(4096), control.number(), control.field(32768);control.done()
            x = checked(value, b"GPX\1", 512)
            require(len(key) == 59 and key[:3] == b"ga\x18" and key[3:19].hex() == raw.GRAPH and key[19:27] == u64(part)
                    and key[-16:] == action and at == original and any(x[:32]) and x[32:] == out,
                    "pin result joined to genuine retained immutable native control")
            require(not cdc or original == index and disposition == 1 and cdc == out, "pin CDC union")
        if fresh and original == index:
            spool.insert(b"pin-original", u64(part)+identity+action, out)
            spool.insert(b"pin-result", u64(part)+u64(index), encoded({"phase": phase, "status": disposition, "pin": identity.hex(), "digest": hash_.hex(), "publication": publication, "epoch": epoch, "feed": feed}))
            if phase in (3, 4, 5, 8) and disposition in (1, 4, 5): raw.put_raw(spool, b"pin-terminal", u64(part)+identity+hash_, out)
        if original < index:
            require(not cdc and optional(frame["OriginalOutcome"]) == out and spool.lookup(b"original", u64(part)+u64(original)) == digest(out)+digest(optional(frame["OriginalCDC"])), "native pin replay sidecar")
        return
    require(not registered and not optional(frame["OriginalOutcome"]) and not optional(frame["OriginalCDC"]) and not cdc, "unknown output cannot fabricate a metadata original")
    if out.startswith(b"GSO\1"):
        phase, disposition, round_, original = struct.unpack(">BBQQ", raw.fixed_body(out, b"GSO\1", 54))
        require(phase in (1, 2) and disposition == 1 and round_ > 0 and original == index, "genuine read-control tail")
    elif out.startswith(b"GRO\2"):
        b = checked(out, b"GRO\2", 120)
        require(b[1:17].hex() == raw.GRAPH and b[17:25] == u64(part) and b[81] == 2 and b[82:] == bytes(2)
                and spool.lookup(b"grant-frame", u64(part)+u64(index)) == digest(out+cdc+image), "actual immutable grant replay frame")
    else:
        require(not out, "unsupported native frame kind")


def grants(directory, end, spool):
    pending, count = None, 0
    for r in lines(directory/"grants.jsonl", 36):
        if r["Phase"] == "pending-original":
            require(pending is None and r["Index"] == 0 and r["Outcome"] is None and r["CDC"] is None, "original grant recipe persisted before submit")
            pending = r
            continue
        require(pending is not None and r["Phase"] == "complete-original" and all(r[k] == pending[k] for k in ("Action", "Partition", "Sequence", "Count", "Proposal")), "exact grant submission/retained original")
        proposal = cursor(blob(r["Proposal"]))
        scope, command = checked(proposal.field(256), b"APS\1", 156), proposal.field(32768);proposal.done()
        out = checked(blob(r["Outcome"]), b"GRO\2", 120)
        require(scope[:24] == out[1:25] and scope[:16].hex() == raw.GRAPH and out[41:73] == digest(command)
                and out[73:81] == u64(r["Index"]) and out[81] in (1, 4) and not any(out[82:]) and not blob(r["CDC"]), "genuine grant authority/request/index/hash")
        spool.insert(b"grant-original", digest(blob(r["Proposal"])), encoded(r))
        pending = None;count += 1
    require(pending is None and 4 <= count <= 18, "complete bounded multi-block grant roster")
    replays = 0
    for r in lines(directory/"grant-replays.jsonl", 18):
        original = decoded(spool.lookup(b"grant-original", digest(blob(r["Proposal"]))))
        expected = bytearray(blob(original["Outcome"]));expected[85] = 2;expected[-32:] = digest(expected[:-32])
        require(r["Phase"] == "actual-replay-entry" and blob(r["Outcome"]) == bytes(expected) and not blob(r["CDC"])
                and r["Index"] > original["Index"] and all(r[k] == original[k] for k in ("Action", "Partition", "Sequence", "Count", "Proposal")), "exact original grant replay, no fresh allocation")
        spool.insert(b"grant-frame", u64(r["Partition"])+u64(r["Index"]), digest(blob(r["Outcome"])+blob(r["CDC"])+blob(r["Root"])))
        replays += 1
    require(replays == count == end["GrantReplays"], "every genuine original grant replayed")


def sources(directory, end, spool):
    first, old = cut(end["Initial"]), cut(end["Old"])
    stages = (("before-stop", cut(end["BeforeStop"])), ("reopened-replayed", cut(end["Replayed"])))
    stage, group, voter, after, image = 0, 0, 1, first[0]["sourceIndex"], first[0]["image"]
    canonical, fresh_rows = None, 0
    for trace in lines(directory/"sources.jsonl", 12*(frames(end["spec"])+1)):
        fields(trace, "Stage Member Page")
        require(stage < 2 and trace["Stage"] == stages[stage][0] and trace["Member"] == group*3+voter, "exact complete all-voter/two-cohort source order")
        page, final = trace["Page"], stages[stage][1][group]
        fields(page, "Certificate After Next Complete Frames")
        cert = column.certificate(raw, page["Certificate"])
        require(cert == final and page["After"] == after and 0 < page["Next"]-after <= 4
                and page["Next"] <= final["sourceIndex"] and type(page["Complete"]) is bool
                and page["Complete"] == (page["Next"] == final["sourceIndex"]) and len(page["Frames"]) == page["Next"]-after,
                "authentic full prefix/cut/EOF without gaps")
        if voter == 1:
            canonical = page
            for offset, f in enumerate(page["Frames"], 1):
                require(f["Partition"] == final["partition"] and f["Index"] == after+offset, "exact source coordinate")
                key, checksum = u64(f["Partition"])+u64(f["Index"]), frame_digest(f)
                prior = spool.find(b"frame", key)
                require(prior is None or prior == checksum, "same-directory retained originals/typed data changed")
                authenticate_frame(f, image, end["spec"], spool, prior is None)
                if prior is None:
                    spool.insert(b"frame", key, checksum)
                    spool.insert(b"original", key, digest(optional(f["Outcome"]))+digest(optional(f["CDC"])))
                    fresh_rows += len(f["Rows"] or [])
                image = array(f["Root"], 172)
            if page["Complete"]: require(image == final["image"], "native source EOF root differs from actual certificate")
        else:
            require(raw.same(canonical, page), "native voters disagree on original prefix")
        voter += 1
        if voter == 4:
            voter, after = 1, page["Next"]
            if page["Complete"]:
                spool.insert(b"source-eof", bytes([stage, group]), b"1")
                group += 1
                if group == 2: stage, group = stage+1, 0
                after, image = first[group]["sourceIndex"], first[group]["image"]
    require(stage == 2 and group == 0 and voter == 1 and fresh_rows == sum(end["SourceRows"]) == 5*end["spec"]["facts"]+7, "complete raw business CDC/all-voter source census")
    count = 0
    for tx, terminal_wire in spool.walk(b"terminal"):
        terminal = decoded(terminal_wire)
        m, decided = manifest(blob(terminal["Manifest"])), status(blob(terminal["Decision"]))
        require(spool.lookup(b"phase", u64(m["coordinator"])+tx+b"\3") == blob(terminal["Decision"]), "actual retained first-D equals caller original")
        st = status(spool.lookup(b"phase", u64(m["coordinator"])+tx+b"\1"))
        require(st["digest"] == decided["digest"], "exact original Register")
        for part, floor in m["floors"].items():
            prepared, resolved = spool.find(b"phase", u64(part)+tx+b"\2"), spool.find(b"phase", u64(part)+tx+b"\4")
            p, r = None if prepared is None else status(prepared), None if resolved is None else status(resolved)
            if decided["commit"]:
                require(p is not None and p["yes"] and r is not None and decided["round"] > p["floor"], "COMMIT strictly above every real YES floor")
            else:
                require(decided["round"] == 0 and (r is not None) == (p is not None or part == m["coordinator"]), "ABORT preserves actual prepared-participant presence")
            if p is not None: require(p["floor"] >= floor and p["digest"] == decided["digest"], "actual vote/manifest floor")
            if r is not None: require(r["commit"] == decided["commit"] and r["round"] == decided["round"] and r["digest"] == decided["digest"], "actual Resolve cannot rewrite durable decision")
        count += 1
    require(count == end["Commits"]+end["Aborts"], "every installed original joins actual source phases")


def state_cell(spool, namespace, owner, life=0, component=1, name=""):
    value = spool.find(namespace, u64(owner)+u64(life)+bytes([component])+name.encode())
    return ZERO if value is None else decoded(value)["After"]


def fold(end, spool):
    old, n, rows = cut(end["Old"])[0]["round"], end["spec"]["facts"], 0
    for order_key, wire in spool.walk(b"ordered-row"):
        row = decoded(wire);key = row_key(row)
        previous = spool.find(b"state", key)
        require(row["Before"] == (ZERO if previous is None else decoded(previous)["After"]), "independent serial exact predecessor, no scalar/image ABA reset")
        after = row["After"]
        if row["Kind"] == 2 and row["Component"] == 1:
            fact, candidate = (row["Owner"]-7)//4, ((row["Owner"]-7)%4)//2
            head = spool.find(b"unique-owner", u64(fact))
            if after["Present"]:
                require(state_cell(spool, b"state", 1)["Handle"] == 2 and state_cell(spool, b"state", row["Target"])["Handle"] == row["TargetLife"], "exact endpoint-life support at actual COMMIT")
                if head is None:
                    spool.insert(b"winner", u64(fact), u64(candidate))
                else:
                    incumbent = decoded(head)
                    require(candidate == 1-incumbent["Candidate"] and state_cell(spool, b"state", incumbent["Target"])["Handle"] != incumbent["TargetLife"], "new-life same-value claim only after actual prior support ends")
                raw.put_raw(spool, b"unique-owner", u64(fact), encoded({"Owner": row["Owner"], "Candidate": candidate, "Target": row["Target"], "TargetLife": row["TargetLife"]}))
            else:
                require(head is not None and decoded(head)["Owner"] != row["Owner"], "old incumbent close cannot delete new owner's claim")
        raw.put_raw(spool, b"state", key, wire)
        # Immutable per-cell revisions enter old state only at their own
        # authenticated original round, not the latest Before of another read.
        if int.from_bytes(order_key[:8], "big") <= old: raw.put_raw(spool, b"old-state", key, wire)
        rows += 1
    require(rows == 5*n+7, "complete exact logical cell census")
    require(sum(1 for _ in spool.walk(b"value-definition")) == n and sum(1 for _ in spool.walk(b"winner")) == n, "all genuine per-fact definitions/old unique winners")


def observed_cell(expected, scalar=False):
    return {"present": expected["Present"], "null": expected["Null"], "handle": expected["Handle"], "revision": expected["Revision"],
            "provenance": expected["Provenance"], "kind": "absent" if not expected["Present"] else "i64" if scalar else "life", "i64": expected["Value"]}


def candidate_pair(entities):
    require(type(entities) is list and len(entities) == 2, "complete fixed two-candidate point set")
    return entities


def points(directory, end, spool):
    n, stages = end["spec"]["facts"], ("old-after-ABA", "final", "reopened-old", "reopened-final")
    count = 0
    for trace in lines(directory/"points.jsonl", 4*n+1):
        fields(trace, "Stage At Winner Observed")
        stage, fact = divmod(count, n)
        require(stage < 4 and trace["Stage"] == stages[stage] and trace["Observed"]["Fact"] == fact, "complete old/current/reopened point inventory")
        updated, o = stage % 2 == 1, trace["Observed"]
        require(trace["At"] == end["Final" if updated else "Old"] and o["Complete"] is True and o["PhantomFound"] is False
                and o["ValueFound"] is True and o["ValueID"] == 4*n+8+fact, "actual exact cut/index/value and phantom absence")
        winner = int.from_bytes(spool.lookup(b"winner", u64(fact)), "big")
        require(trace["Winner"] == winner, "saved winner equals independently folded actual first acquisition")
        active, namespace = 1-winner if updated else winner, b"state" if updated else b"old-state"
        seen = set()
        for claim in o["Claims"]:
            owner, life = claim["Owner"], claim["Life"]
            require(owner in (7+4*fact, 9+4*fact) and life == owner+1 and owner not in seen
                    and claim["Key"] == {"Owner": owner, "Life": life, "Kind": 3, "Name": "p", "Member": 0}, "complete unique candidate set has no phantom/duplicate/foreign property")
            seen.add(owner)
        require(7+4*fact+2*active in seen and 1 <= len(seen) <= 2, "actual new-life winner appears in complete candidate index")
        for candidate, x in enumerate(candidate_pair(o["Entities"])):
            entity, identity = x["Entity"], 7+4*fact+2*candidate
            found = updated or candidate == winner
            require(entity["id"] == identity and entity["found"] is found and entity["effective"] is (candidate == active), "exact historical/effective entity set")
            if not found:
                require(not entity["presence"]["present"] and not x["Life"]["found"] and not x["Scalar"]["present"], "losing old candidate must remain absent")
                continue
            target, target_life = endpoint(end["spec"], fact, candidate != winner)
            require((entity["kind"], entity["type"], entity["mode"], entity["source"], entity["target"]) == (2, "edge", 1, 1, target)
                    and entity["presence"] == observed_cell(state_cell(spool, namespace, identity))
                    and x["Scalar"] == observed_cell(state_cell(spool, namespace, identity, identity+1, 3, "p"), True)
                    and x["Life"] == {"owner": identity, "life": identity+1, "found": True, "sourceLife": 2, "targetLife": target_life}, "exact retained scalar/life revision and immutable endpoints")
            require(entity["effectiveLife"] == (identity+1 if candidate == active else 0), "endpoint ABA cannot rebind old relationship life")
        count += 1
    require(count == 4*n, "all useful facts compared at all four authentic cuts")


def retention(r, m, spool, terminal=False):
    require(r["Version"] == 1 and r["State"] == (5 if terminal else 3) and r["Generation"] == m["Generation"]["ID"] and r["Pin"] == m["Generation"]["Pin"]
            and integer(r["Attempts"], 0, 4) <= 4 and digest(blob(r["Original"])) == array(r["Pin"], 32), "actual retained generation original, not journal authority")
    wire = blob(r["Original"])
    c = cursor(checked(wire, b"GPM\1", 16384))
    scope, issuer, request = c.take(120), c.take(16), c.take(16)
    pin = digest(b"rho-tkg:retained-graph-pin:v1\0"+scope+issuer+request)
    epoch, expires = c.number(), c.number()
    require(any(issuer) and any(request) and epoch < expires == 20 and c.number(4) == 2, "actual replicated lease policy and full original keeper roster")
    for group in range(2):
        cert, feed, lineage = c.field(512), c.number(), c.take(32)
        lower = column.certificate(raw, m["Generation"]["Sources"][group]["Initial"])
        require(cert == blob(m["Generation"]["Sources"][group]["Initial"]) and 0 < feed <= lower["sourceIndex"]+1
                and lineage == digest(b"rho-tkg:retained-pin-source-lineage:v1\0"+cert+u64(feed)), "exact dependency-min feed/initial certificate/lineage")
        if terminal: spool.lookup(b"pin-terminal", u64(lower["partition"])+pin+digest(wire))
    c.done()
    token = cursor(checked(blob(r["Token"]), b"GPT\1", 17408))
    require(token.field(16384) == wire and token.number(4) == 2, "same original native token")
    for part in (3, 8):
        publication = token.number()
        actual = decoded(spool.lookup(b"pin-result", u64(part)+u64(publication)))
        require(actual["phase"] == actual["status"] == 1 and actual["publication"] == publication and actual["pin"] == pin.hex()
                and actual["digest"] == digest(wire).hex() and actual["epoch"] == epoch, "all-participant actual publication originals before native token exposure")
    token.done()


def columns(directory, end, spool):
    if end["spec"]["layout"] == "kv":
        require(not (directory/"columns.jsonl").exists(), "KV cannot invent column completion")
        return
    first = cut(end["Initial"])
    admission = digest(encoded(end["Admission"]))
    expected = {"old-seal/native", "old-seal/fill", "old-seal/cache", "final-seal/native", "final-seal/fill", "final-seal/cache", "old-after-ABA",
                "reopened-0-cached-false", "reopened-0-cached-true", "reopened-1-cached-false", "reopened-1-cached-true"}
    owners, completed, sealed, retired, probes = [None, None], set(), set(), set(), set()
    final_cache = [None, None]
    seconds = end["Admission"]["BudgetSeconds"]
    for trace in lines(directory/"columns.jsonl", 20*(2*frames(end["spec"])+seconds//60+100)):
        if trace["Stage"] == "actual-keeper-retired-token":
            fields(trace, "Stage Member Generation Token Response")
            group = (integer(trace["Member"], 1, 4)-1)//3
            require(trace["Member"] in (1, 4) and owners[group] is None, "actual released native keeper diagnostic")
            matches = [i for i in range(2) if end["ColumnManifests"][i][group]["Generation"]["ID"] == trace["Generation"]]
            require(len(matches) == 1 and (matches[0], group) in retired, "exact retired generation token probe")
            token = cursor(checked(blob(trace["Token"]), b"GPT\1", 17408))
            require(digest(token.field(16384)) == array(end["ColumnManifests"][matches[0]][group]["Generation"]["Pin"], 32), "same actual durable original token")
            require(response(trace["Response"])["code"] == 8, "native original-token refusal distinct from local journal closure")
            probes.add((matches[0], group));continue
        stage, member, q = trace["Stage"], trace["Member"], trace["Input"]
        require(member in (1, 4), "actual column member")
        group, action = (member-1)//3, integer(q["Action"], 1, 6)
        generation = int(stage.startswith(("final-seal", "reopened-1", "retire-1")))
        m, b = end["ColumnManifests"][generation][group], end["ColumnBindings"][generation][group]
        r = trace.get("Receipt")
        if r is None:
            require(trace.get("Error") and (action == 1 and owners[group] is None or owners[group] is not None and q["Cookie"] == owners[group]["receipt"]["Cookie"]), "failed request preserves exact original query")
            continue
        p = b["Policy"]
        require(r["Binding"] == b and p["Version"] == 1 and array(p["Admission"], 32) == admission and p["ChunkRecords"] == p["ChunkRows"] == 16
                and p["ChunkBytes"] == 65536 and p["FrameBytes"] == 32768 and p["MaxChunks"] == 2*frames(end["spec"])
                and p["MaxRetainedBytes"] == 8*16384+2*frames(end["spec"])*(65536+128)
                and r["DeadlineUnixNanos"] == end["Admission"]["DeadlineUnixNanos"], "same actual finite source/policy/absolute deadline")
        require(b["Version"] == 2 and b["Coverage"] == 1 << group and b["TerminalAttempts"] == 4 and b["Layout"] == column.LAYOUT
                and array(m["Generation"]["Binding"], 32) == digest(column.sealed_json(raw, b)), "exact native immutable binding/seal")
        for g in range(2):
            boundary = m["Generation"]["Sources"][g]
            require(column.certificate(raw, boundary["Initial"]) == first[g] and column.certificate(raw, boundary["Final"]) == cut(end["Final" if generation else "Old"])[g], "full same retained original source interval")
        if action == 1:
            require(owners[group] is None and r["Sequence"] == 0 and not r["Closed"] and q["Initial"] == end["Initial"] and q["Final"] == end["Final" if generation else "Old"]
                    and q["Generation"] == m["Generation"]["ID"] and q["RetainedExpires"] == 20, "genuine same-directory constructor")
            owners[group] = {"receipt": r, "sequence": 0, "cursor": None}
        else:
            owner = owners[group]
            require(owner is not None and q["Cookie"] == r["Cookie"] == owner["receipt"]["Cookie"], "same private native query cookie")
            for name, maximum in zip(column.WORK, ("MaxWorkRecords", "MaxWorkBytes", "MaxDecodedRows", "MaxOutputBytes", "MaxRPCs", "MaxWireBytes"), strict=True):
                require(integer(owner["receipt"]["Charged"][name]) <= integer(r["Charged"][name]) <= integer(p[maximum]), "no prepaid allowance refund/reset")
            if action not in (5, 6):
                require(q["Sequence"] == r["Sequence"] == owner["sequence"]+1, "actual private cursor/sequence commits together")
                owner["sequence"] += 1
        owner = owners[group]
        require(r["RPCs"] <= r["Charged"]["RPCs"] and r["RequestBytes"]+r["ReplyBytes"] <= r["Charged"]["WireBytes"], "actual RPC/wire usage separately funded")
        durable_key = bytes([generation, group])
        previous = spool.find(b"durable-column-work", durable_key)
        if previous is not None:
            for name in column.WORK: require(integer(r["DurableWork"][name]) >= decoded(previous)[name], "physical reopen refunded generation work")
        raw.put_raw(spool, b"durable-column-work", durable_key, encoded(r["DurableWork"]))
        if r["CacheLedger"]["Version"]: final_cache[group] = r["CacheLedger"]
        if action == 2 and r["EOF"]:
            require(r["Manifest"] == m, "actual complete native publisher seal")
            retention(r["Retention"], m, spool);sealed.add((generation, group))
        elif action == 3:
            require(stage in expected and (stage, group) not in completed and r["Manifest"] == m and not r["EOF"] and q["At"] == end["Final" if generation else "Old"], "exact source-qualified read start")
            owner["cursor"] = {"ordinal": 0, "chain": bytes(32), "position": first[group]["sourceIndex"], "frames": 0, "rows": 0, "bytes": 0, "cached": q["Cached"], "fill": q["Fill"], "stage": stage}
            if q["Cached"]: column.cache_manifest(raw, Path(end["Directory"]), group, m)
        elif action == 4:
            c = owner["cursor"]
            require(c is not None and c["stage"] == stage and r["Ordinal"] == c["ordinal"] and r["Manifest"] == m and r["Cached"] == c["cached"] and r["Fill"] == c["fill"], "same owned binary cursor")
            if r["EOF"]:
                require(not optional(r["Body"]) and not r["Rows"] and c["ordinal"] == m["Chunks"] and c["chain"] == array(m["Chain"], 32)
                        and c["position"] == m["Positions"][group] and c["frames"] == m["Frames"] and c["rows"] == m["Rows"] and c["bytes"] == m["Bytes"], "complete binary EOF/census, not a Close")
                completed.add((stage, group));owner["cursor"] = None
            else:
                body = blob(r["Body"])
                selected = []
                for f in column.chunk(raw, body, m["Generation"], c["ordinal"], c["chain"], p):
                    f = {**f, "Root": list(f["Root"]), **{name: base64.b64encode(f[name]).decode() for name in ("Outcome", "CDC", "Registration", "OriginalOutcome", "OriginalCDC")}}
                    require(f["Partition"] == first[group]["partition"] and f["Index"] == c["position"]+1
                            and frame_digest(f) == spool.lookup(b"frame", u64(f["Partition"])+u64(f["Index"])), "binary native/cache columns equal actual complete voter prefix")
                    c["position"] += 1;c["frames"] += 1;c["rows"] += len(f["Rows"]);selected.extend(f["Rows"])
                require(selected == (r["Rows"] or []), "exact binary projected typed rows")
                column.cache_chunk(raw, Path(end["Directory"]), group, m, c["ordinal"], body, spool)
                key = b"generation/"+array(m["Generation"]["ID"], 16).hex().encode()+b"/chunk/"+f'{c["ordinal"]:016x}'.encode()
                c["bytes"] += len(key)+len(body);c["chain"] = digest(c["chain"]+digest(body));c["ordinal"] += 1
        elif action in (5, 6):
            require(r["Closed"] and not r["EOF"] and owner["cursor"] is None, "actual terminal lifetime")
            if action == 6: retention(r["Retention"], m, spool, True);retired.add((generation, group))
            owners[group] = None
        else:
            require(action == 1 or action == 2 and not r["EOF"], "known actual column transition")
        if owners[group] is not None: owners[group]["receipt"] = r
    require(owners == [None, None] and completed == {(stage, g) for stage in expected for g in range(2)} and sealed == retired == probes == {(generation, g) for generation in range(2) for g in range(2)}, "all actual native/fill/cache/reopen/seal/retire/keeper-token paths")
    for group in range(2): column.cache_terminal(raw, Path(end["Directory"]), group, final_cache[group], spool)


def response(value):
    c = cursor(blob(value));incarnation = c.take(16)
    stats = [c.number() for _ in range(28)]
    c.take(64);c.number()
    for _ in range(integer(c.number(4), 0, 2)): c.number()
    aux = c.field(1 << 20)
    ready, sequence, applied, code, cause = c.number(1), c.number(), c.number(), c.number(1), c.number(2)
    detail = c.field(4096)
    packets = integer(c.number(4), 0, 4096)
    for _ in range(packets): c.take(17);c.field(1 << 20)
    reads = integer(c.number(4), 0, 16)
    for _ in range(reads): c.number();c.field(1 << 20)
    image = c.field(65536);c.field(65536);c.field(1 << 20)
    c.number(1);c.number()
    for _ in range(integer(c.number(4), 0, 16)): c.field(256)
    c.field(256);c.done()
    require(any(incarnation) and sequence > 0 and applied > 0 and 0 <= code <= 9 and ready in (0, 1), "actual framed native response")
    return {"incarnation": incarnation.hex(), "stats": stats, "applied": applied, "image": image.hex(), "code": code,
            "cause": cause, "detail": detail.hex(), "packets": packets, "reads": reads, "aux": aux.hex()}


def process_state(value):
    r = response(value)
    require(r["code"] == r["cause"] == r["packets"] == r["reads"] == 0 and not r["detail"] and r["stats"][7] == r["stats"][22] == 0, "genuine clean source-free restored state")
    return r


def processes(directory, end, spool):
    count = 0
    for r in lines(directory/"restore.jsonl", 13):
        stage, member = divmod(count, 6);member += 1
        require(stage < 2 and r["Member"] == member and r["Stage"] == ("before-stop" if stage == 0 else "restored-before-election"), "exact actual restored process inventory")
        actual = process_state(r["Raw"])
        if stage == 0: spool.insert(b"process-state", bytes([member]), encoded(actual))
        else:
            before = decoded(spool.lookup(b"process-state", bytes([member])))
            require(actual["incarnation"] != before["incarnation"] and actual["image"] == before["image"] and actual["applied"] == before["applied"]
                    and actual["stats"][0] == before["stats"][0] and actual["stats"][4] == before["stats"][4], "physical replacement preserves full raw durable state")
        count += 1
    require(count == end["ProcessInstances"] == 12, "both real six-child cohorts")
    count = 0
    for r in lines(directory/"processes.jsonl", 13):
        cohort, member = divmod(count, 6);member += 1
        require(r["Cohort"] == cohort+1 and r["Member"] == member and integer(r["PID"], 1) and r["Reaped"] is True and r["WaitStatus"] == r["ExitCode"] == 0
                and Path(r["Volume"]) == Path(end["Directory"])/str(member) and any(array(r["Incarnation"], 16)), "literal actual OS child reaping/owned directories")
        spool.insert(b"process-incarnation", array(r["Incarnation"], 16), b"1")
        count += 1
    require(count == 12 and end["Reaped"] == 6, "terminal complete owned cleanup")
    logical, allocated = 0, 0
    root = Path(end["Directory"])
    for member in range(1, 7):
        for parent, directories, files in os.walk(root/str(member), followlinks=False):
            require(all(not (Path(parent)/name).is_symlink() for name in directories), "no foreign symlink directory")
            for name in files:
                path = Path(parent)/name
                st = path.lstat()
                require(stat.S_ISREG(st.st_mode), "only actual owned regular native/cache files")
                logical += st.st_size;allocated += st.st_blocks*512
    require(logical == end["PersistedFileBytes"] and allocated == end["AllocatedFileBytes"], "actual all-voter/derived/cache filesystem accounting")


def terminal_original(a, proof, terminal):
    require(terminal == {"Operation": a["Operation"], "Decision": proof["Outcome"], "Manifest": proof["Manifest"], "Proposal": a["Proposal"]}, "duplicate operation/body/result is the actual retained original")


def duplicates(directory, end, spool):
    count = 0
    for r in lines(directory/"duplicates.jsonl", end["spec"]["facts"]+2):
        fields(r, "Original Replay")
        a, b = r["Original"], r["Replay"]
        require(a["Classification"] == b["Classification"] == "commit" and a["Operation"]["Phase"] == b["Operation"]["Phase"] == "close"
                and all(a[k] == b[k] for k in ("Operation", "Request", "Proposal", "BodySHA256", "Proof", "Reply")) and not b["Error"], "same original close/reopen recovery preserved full body and first decision")
        proof = decoded(blob(a["Proof"]))
        tx = manifest(blob(proof["Manifest"]))["tx"]
        terminal = decoded(spool.lookup(b"terminal", tx))
        terminal_original(a, proof, terminal)
        original_proposal(blob(a["Proposal"]), a["Operation"], end["spec"])
        key = u64(a["Operation"]["Fact"])
        if count < end["spec"]["facts"]: spool.insert(b"duplicate-seen", key, b"1")
        count += 1
    require(count == end["spec"]["facts"]+1, "every old incumbent close plus one physical-reopen original")


def verify(directory, spool_directory, policy, *, binary_sha=None, expected_spec=None, expected_run=None, expected_deadline=None, expected_seconds=None, shared_spool=None):
    directory = Path(directory).resolve(strict=True)
    with (directory/"result.json").open("rb") as source: body = source.read(LINE+1)
    require(body.endswith(b"\n") and len(body) <= LINE, "bounded complete terminal native result")
    end = decoded(body[:-1]);s = shape(end["spec"])
    require(integer(end["version"], 1, 1) and end["format"] in (FORMAT, TRANSPORT_FORMAT) and end["engine"] == "native-raft" and integer(end["FramesCeiling"], 1) == frames(s), "fresh family/result/complete issued ceiling")
    require(Path(end["ArtifactDirectory"]).resolve(strict=True) == directory and not Path(end["Directory"]).is_symlink()
            and Path(end["Directory"]).resolve(strict=True).parent == directory, "owned genuine durable/artifact roots")
    cuts = {name: cut(end[name]) for name in ("Initial", "Old", "Final", "BeforeStop", "Replayed")}
    for group in range(2):
        require(all(cuts[name][group]["scope"] == cuts["Initial"][group]["scope"] and cuts[name][group]["configuration"] == cuts["Initial"][group]["configuration"] for name in cuts)
                and [cuts[name][group]["sourceIndex"] for name in cuts] == sorted(cuts[name][group]["sourceIndex"] for name in cuts)
                and cuts["Replayed"][group]["sourceIndex"] <= frames(s), "actual original scopes/configuration, monotone cuts and finite frame ceiling")
    a = end["Admission"]
    shaped = end["format"] == TRANSPORT_FORMAT
    selected = owned_transport(end["Transport"], s) if shaped else None
    require(integer(a["Version"], 2 if shaped else 1, 2 if shaped else 1) and a["Family"] == FAMILY and a["Spec"] == {"version": 4, **{k: s[k] for k in ("facts", "crossPercent", "writers")}}
            and a["Transport"] == (TRANSPORT_FORMAT if shaped else "local") and integer(a["BudgetSeconds"], 1, MAX_SECONDS)
            and integer(a["DeadlineUnixNanos"], 1, (1 << 63)-1), "actual immutable local family/budget/deadline admission")
    if shaped:
        fields(a, "Version Family Run Spec Transport BudgetSeconds CausalMinimumMillis DeadlineUnixNanos OwnedTransport Graph Topology Declaration BinarySHA256")
        require(a["OwnedTransport"] == selected and integer(a["CausalMinimumMillis"]) == causal_minimum(s, selected["targetRTTMillis"])
                and a["BudgetSeconds"]*1000 > a["CausalMinimumMillis"] and a["BinarySHA256"] == end["BinarySHA256"]
                and type(a["BinarySHA256"]) is str and len(a["BinarySHA256"]) == 64 and bytes.fromhex(a["BinarySHA256"]).hex() == a["BinarySHA256"], "same actual binary, chosen original budget and necessary causal RTT floor")
        for group in range(2):
            scope = cuts["Initial"][group]["scope"]
            require(array(a["Graph"], 16) == scope[:16] and integer(a["Topology"], 1) == int.from_bytes(scope[48:56], "big")
                    and array(a["Declaration"], 32) == scope[56:88], "admitted graph/topology/declaration matches actual acquired native certificates")
    else:
        fields(a, "Version Family Run Spec Transport BudgetSeconds CausalMinimumMillis DeadlineUnixNanos")
        require(integer(a["CausalMinimumMillis"], 0, 0) == 0 and "Transport" not in end, "legacy local wire profile remains exact")
    require(type(a["Run"]) is str and len(a["Run"]) == 32 and bytes.fromhex(a["Run"]).hex() == a["Run"] and any(bytes.fromhex(a["Run"])), "fresh actual run identity")
    for expected, actual in ((binary_sha, end["BinarySHA256"]), (expected_spec, s), (expected_run, a["Run"]), (expected_deadline, a["DeadlineUnixNanos"]), (expected_seconds, a["BudgetSeconds"])):
        require(expected is None or expected == actual, "actual controller source/run/budget mismatch")
    for name in ("Attempts", "Commits", "Aborts", "Refusals", "ExtraInvocations", "FramesCeiling", "SetupNanos", "BusinessNanos", "DiagnosticNanos", "OccupiedDiagnosticNanos", "GrantReplays", "PersistedFileBytes", "AllocatedFileBytes", "TotalNanos", "Reaped", "ProcessInstances"):
        integer(end[name])
    require(type(end["SourceRows"]) is list and all(type(v) is int for v in end["SourceRows"])
            and end["SourceRows"] == [5*s["facts"]+4, 3], "exact per-participant native typed row census")
    require(end["SetupNanos"]+end["BusinessNanos"]+end["DiagnosticNanos"] == end["TotalNanos"] and end["BusinessNanos"] > 0
            and end["DiagnosticNanos"] >= end["OccupiedDiagnosticNanos"] and end["Commits"] == 3*s["facts"]+7, "full useful lifecycle and explicit business/diagnostic timing")
    with (nullcontext(shared_spool) if shared_spool is not None else Spool(spool_directory, policy)) as spool:
        counts = operations(directory, end, spool)
        grants(directory, end, spool)
        sources(directory, end, spool)
        fold(end, spool)
        points(directory, end, spool)
        duplicates(directory, end, spool)
        columns(directory, end, spool)
        processes(directory, end, spool)
        count, answers = 6*s["facts"]+7, hashlib.sha256()
        ranks = {"p50Nanos": (count*50+99)//100, "p95Nanos": (count*95+99)//100, "p99Nanos": (count*99+99)//100}
        latency = {}
        for index, (key, _) in enumerate(spool.walk(b"latency-order"), 1):
            for name, rank in ranks.items():
                if index == rank: latency[name] = int.from_bytes(key[:8], "big")
        for namespace in (b"old-state", b"state"):
            for fact in range(s["facts"]):
                winner = int.from_bytes(spool.lookup(b"winner", u64(fact)), "big")
                candidate = winner if namespace == b"old-state" else 1-winner
                target, target_life = endpoint(s, fact, namespace == b"state")
                owner = 7+4*fact+2*candidate
                require(state_cell(spool, namespace, owner)["Present"] and state_cell(spool, namespace, owner, owner+1, 3, "p")["Value"] == fact+5, "full old/current answer set")
                # Logical roles omit substrate-specific winning physical IDs;
                # both engines must retain old/final exact endpoint lives/value.
                answers.update(u64(fact)+u64(target)+u64(target_life)+struct.pack(">q", fact+5))
        require(len(latency) == 3, "all bounded spool percentile ranks")
        return {"spec": s, "run": a["Run"], "counts": counts, "logicalAnswersSHA256": answers.hexdigest(),
                **({"transport": selected, "CausalMinimumMillis": a["CausalMinimumMillis"], "BudgetSeconds": a["BudgetSeconds"]} if shaped else {}),
                "timing": {**latency, "logicalOperations": count, "usefulTransactionsPerSecond": end["Commits"]*1_000_000_000/end["BusinessNanos"]},
                "scope": "one actual native source cell; complete fixed original/cell/index/CDC/column/cache/reopen joins; runtime acceptance and full matrix remain separate"}
