#!/usr/bin/env python3
"""Finite P01 bridge for genuine OS-child A-only reads with coordinator B.

Checks literal old/current facts, actual immutable originals and complete native
prefixes before invoking the separate serial oracle. Captured bytes are inert
evidence; this bridge neither constructs a source nor grants cut authority.
"""
import argparse
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import struct

SPEC = importlib.util.spec_from_file_location("native_scope_components", Path(__file__).with_name("native_transition_certificate_check.py"))
Q = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(Q)
T, C, P = Q.T, Q.C, Q.P
require, integer = T.require, T.integer
PROFILE = "native-outside-data-scope-v1"
HEADER = set("Version Profile Mode Original Manifest PendingCaller Decision Lookup Failure FailureMask Before Current Old Held Recovered Retained Reopened Retry Prepared Processes From Through Records".split())


def load(path):
    digest, total, header, footer, rows = hashlib.sha256(), 0, None, None, []
    with path.open("rb") as source:
        while line := source.readline((512 << 10)+1):
            total += len(line)
            require(len(line) <= 512 << 10 and total <= 16 << 20 and line.endswith(b"\n"), "finite source frame")
            row = T.decoded(line)
            require(type(row) is dict and footer is None, "row after EOF")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and row["Type"] == "header", "header row")
                header = row["Artifact"]
                require(type(header) is dict and set(header) == HEADER and header["Records"] is None, "fresh scope grammar")
            elif row.get("Type") == "record":
                require(set(row) == {"Type", "Record"} and len(rows) < 512, "finite actual prefix")
                rows.append(row["Record"])
            else:
                require(set(row) == {"Type", "Rows", "Through", "PayloadSHA256"} and row["Type"] == "footer", "EOF grammar")
                footer = row
                require(integer(row["Rows"], 512) == len(rows) and row["PayloadSHA256"] == digest.hexdigest(), "exact prefix census/hash")
            if footer is None:
                digest.update(line)
    require(header is not None and footer is not None and footer["Through"] == header["Through"], "missing/foreign EOF")
    header["Records"] = rows
    return header


def request(wire, version=2):
    c = P.Cursor(P.body(wire, b"GEQ" + bytes([version])))
    mode, tx, coordinator, count = c.number(1), c.take(16), c.number(), c.number(4)
    require(mode in (1, 3, 4) and count <= 1, "finite requested mode/old cut")
    certificates = [c.field(512) for _ in range(count)]
    if version == 2:
        require(c.number(4) == 1 and c.number() == 3, "explicit DATA scope A only")
    c.end()
    require(mode == 3 if certificates else mode in (1, 4), "At needs its actual retained cut")
    require(any(tx) and coordinator == 8 if mode == 4 else tx == bytes(16) and coordinator == 0, "After must name the actual B original")
    return mode, tx, certificates


def facts(wire, current):
    c = P.Cursor(wire)
    require(c.take(4) == b"GSF1" and c.number(4) == 3, "complete scoped requested ID count")
    result = {}
    for owner in (1, 3, 7):
        require(c.number() == owner, "ordered exact A IDs, no outside-scope hydration")
        f = P.Cursor(c.field(8192))
        flag, life, source, target = f.number(1), f.number(), f.number(), f.number()
        require(f.number(4) == f.number(4) == 0, "no phantom labels or incidents")
        f.end()
        require((flag, life, source, target) == ((3, 2, 0, 0) if current and owner == 1 else (0, 0, 0, 0)), "literal old-empty/new-node answer")
        if flag:
            result["A/node/1"] = dict(life=life, labels=[], incident=[])
    c.end()
    return result


def observation(row, current, scopes, configuration):
    require(type(row) is dict and set(row) == {"Request", "Token", "Facts"}, "actual scope observation grammar")
    mode, tx, requested = request(P.raw(row["Request"]))
    c = P.Cursor(P.body(P.raw(row["Token"]), b"GET\1"))
    require(c.number() > 0, "actual owner handle")
    nested = P.Cursor(P.body(c.field(16 << 10), b"GEQ\1"))
    c.end()
    require(nested.number(1) == 3 and nested.take(16) == bytes(16) and nested.number() == 0 and nested.number(4) == 1, "returned token has exactly one actual certificate")
    encoded = nested.field(512)
    nested.end()
    cert = Q.certificate(base64.b64encode(encoded).decode())
    require(cert["scope"] == scopes[3] and cert["configuration"] == configuration and cert["partition"] == 3, "same full native scope/configuration")
    require(requested == [encoded] if mode == 3 else not requested, "At cannot substitute or widen the original certificate")
    return cert, facts(P.raw(row["Facts"]), current), mode, tx


def unavailable(response, mask):
    require(type(mask) is int and mask & (1 << 10) and not mask & ~((1 << 3) | (1 << 10))
            and response["code"] in (3, 6) and not response["mask"] & ~(1 << 7)
            and not any(response[k] for k in ("aux", "image", "outcome", "changes")),
            "only actual unavailable B, never invalid/limit/corrupt/empty-success")


def lookup(boundary, scopes, original, decision, *, source_voter=4):
    require(type(boundary) is dict and set(boundary) == {"Request", "Reply"}, "actual held lookup frames")
    q = P.Cursor(P.body(P.raw(boundary["Request"]), b"GSQ\1"))
    require(q.number(1) == 6, "genuine cross original read")
    sequence, nonce = q.number(), q.take(16)
    q.take(48)
    require(all(not q.field() for _ in range(4)) and q.take(16) == original["tx"] and q.number(1) == 3, "B immutable decision question")
    q.end()
    r = P.Cursor(P.body(P.raw(boundary["Reply"]), b"GSR\1"))
    scope, voter, issuer = P.body(r.field(156), b"APS\1"), r.number(), r.take(16)
    require(scope == scopes[8] and voter == source_voter and any(issuer) and r.number(1) == 6 and r.number() == sequence and sequence > 0
            and r.take(16) == nonce and any(nonce) and r.number(2) == 0, "actual configured B source/reply correlation")
    a = P.Cursor(P.body(r.field(), b"GSE\1"))
    r.end()
    require(P.body(a.field(156), b"APS\1") == scope and a.take(16) == issuer and a.number() > 0
            and a.take(16) == original["tx"] and a.take(16) == nonce and a.number(1) == 3, "actual completed native B read identity")
    index, outcome, manifest = a.number(), a.field(256), a.field(64 << 10)
    a.end()
    require(outcome == decision and index >= P.status(decision)["index"] and manifest == original["manifest"], "held reply is this same immutable durable decision")


def prefix(data, original, decision):
    records, statuses = {}, {}
    require(type(data["Records"]) is list and len(data["Records"]) <= 512, "finite native record inventory")
    for row in data["Records"]:
        require(type(row) is dict and set(row) == {"Partition", "Index", "Outcome", "CDC"}, "exact native record fields")
        key = integer(row["Partition"], 8, 3), integer(row["Index"], minimum=1)
        require(key[0] in (3, 8) and key not in records, "duplicate/foreign native record")
        records[key] = row
    require(type(data["From"]) is list and type(data["Through"]) is list and len(data["From"]) == len(data["Through"]) == 2, "full actual prefix vectors")
    for i, partition in enumerate((3, 8)):
        lo, hi = integer(data["From"][i], minimum=1), integer(data["Through"][i], minimum=1)
        require(lo <= hi and hi-lo <= 256 and [r["Index"] for r in data["Records"] if r["Partition"] == partition] == list(range(lo+1, hi+1)), "complete native prefix, including no-output/replay entries")
    for (partition, index), row in records.items():
        outcome, cdc = P.raw(row["Outcome"]), P.raw(row["CDC"])
        if not outcome:
            require(not cdc, "missing original with nonempty CDC")
            continue  # Both actual ApplicationRecord reads succeeded in Go.
        if outcome[:4] == b"GSO\1":
            require(Q.control(row["Outcome"])["index"] == index and not cdc, "actual cut control record")
            continue
        s = P.status(outcome)
        require(s["partition"] == partition and s["tx"] == original["tx"] and s["digest"] == original["digest"] and 0 < s["index"] <= index, "same qualified original/full manifest")
        if s["index"] < index:
            require(records.get((partition, s["index"]), {}).get("Outcome") == row["Outcome"] and not cdc, "authentic earlier original, actual later replay entry")
            continue
        key = partition, s["phase"]
        require(key not in statuses and (s["phase"] != 2 or s["yes"]) and (s["phase"] < 3 or s["commit"]), "immutable original phase")
        statuses[key] = s
        require(bool(cdc) == (s["phase"] == 4), "complete original participant CDC only on Resolve")
        if cdc:
            envelope = P.Cursor(P.body(cdc, b"GCE\1"))
            require(envelope.number(1) == 6 and envelope.take(16) == original["tx"] and envelope.take(32) == original["digest"] and envelope.number() == index, "complete same original CDC envelope")
            logical = envelope.field()
            envelope.end()
            creation(logical, partition, s["round"], original["axis"])
    require(set(statuses) == {(8, 1), (3, 2), (8, 2), (8, 3), (3, 4), (8, 4)}, "complete two-participant original census")
    d = P.status(decision)
    require(P.raw(records[8, d["index"]]["Outcome"]) == decision and d == statuses[8, 3]
            and d["round"] > max(statuses[p, 2]["floor"] for p in (3, 8))
            and all(statuses[p, 4]["round"] == d["round"] for p in (3, 8)), "actual first durable decision/both installations")
    require(type(data["Prepared"]) is list and len(data["Prepared"]) == 3, "actual three A-voter YES census")
    for member, row in zip((1, 2, 3), data["Prepared"]):
        require(type(row) is dict and set(row) == {"Member", "State", "Record"} and integer(row["Member"], 3, 1) == member, "actual distinct A voter identity")
        state, record = C.process_response(P.raw(row["State"])), C.process_response(P.raw(row["Record"]))
        require(state["code"] == state["mask"] == record["code"] == record["mask"] == 0
                and state["incarnation"] == record["incarnation"] and state["sequence"] < record["sequence"]
                and state["applied"] == record["applied"] == statuses[3, 2]["index"]
                and record["outcome"] == P.raw(records[3, statuses[3, 2]["index"]]["Outcome"]) and not record["changes"],
                "A remains prepared before inaccessible B authority")
    return records, statuses


def axis(c):
    axis_id, profile, version, reference, unit = c.take(16), c.number(1), c.number(2), c.field(256), c.field(256)
    require(any(axis_id) and profile == 2 and version == 1 and reference.decode().strip() and unit.decode().strip(), "actual axis descriptor")
    descriptor = axis_id + struct.pack(">BH", profile, version) + struct.pack(">I", len(reference)) + reference + struct.pack(">I", len(unit)) + unit
    definition = hashlib.sha256(b"rho-tkg:axis:v1\0" + descriptor).digest()
    return axis_id, profile, definition


def original_input(original):
    c = P.Cursor(P.body(original["source"]["inner"], b"PGO\1"))
    require(c.number(1) == 6 and c.take(16) == P.GRAPH and c.number() == 3 and c.take(16) == original["source"]["request"]
            and c.number() > 0 and c.number() == 1 and c.number(4) == 1, "genuine node-A factory identity/provenance/axis")
    expected = axis(c)
    scope = b"TS\1" + bytes([expected[1]]) + expected[0] + expected[2] + bytes([6])
    require(c.number(4) == 1 and [c.number(1), c.number(), c.number(), c.number(4)] == [1, 1, 2, 1]
            and c.field(53) == scope, "exact original create node1/life2/full-domain operation")
    # Original operation.Record is a declaration input, not its materialized row.
    require(c.number() == 0 and c.number(1) == 0 and c.number(4) == 0 and not c.field(256)
            and [c.number(), c.number(), c.number(1), c.number(1), c.number(1)] == [0, 0, 0, 3, 1]
            and c.take(32) == bytes(32) and not c.field(256) and not c.field() and c.number(4) == 0
            and c.number() == 0 and c.number(1) == 0 and c.number(4) == 2, "no substituted operation/record/life/name/value")
    grants = []
    for role, owner, id_ in ((1, 0, 1), (2, 1, 2)):
        require([c.number(1), c.number(), c.number()] == [role, owner, id_], "complete immutable fresh ID claim")
        session, incarnation, epoch, sequence = c.take(16), c.take(16), c.number(), c.number()
        require(any(session) and any(incarnation) and epoch > 0 and sequence == 1, "actual original recipient grant")
        grants.append((session, incarnation, epoch, sequence))
    c.end()
    require(grants[0] == grants[1], "same original grant for node/life")
    return expected


def creation(logical, partition, round_, expected_axis):
    c = P.Cursor(P.body(logical, b"GCD\5"))
    require(c.take(16) == P.GRAPH and c.number() == partition and c.number(1) == 0 and [c.number() for _ in range(3)] == [0, 0, 0]
            and c.number(4) == 0, "native original CDC namespace/no schema mutation")
    axes = c.number(4)
    if partition == 8:
        require(axes == 0 and [c.number(4) for _ in range(4)] == [0, 0, 0, 0], "B validates without inventing business rows")
        c.end()
        return
    require(axes == 1, "one genuine rational axis")
    axis_id, profile, definition = axis(c)
    require((axis_id, profile, definition) == expected_axis, "same original native axis/descriptor")
    require(c.number(4) == 1 and c.number() == 1 and c.number(1) == 1 and c.number(4) == 1 and not c.field(256)
            and [c.number(), c.number(), c.number(1), c.number(1), c.number(1)] == [0, 0, 0, 3, 1], "exact typed node1 declaration")
    require(c.number(4) == 1 and [c.number() for _ in range(4)] == [1, 2, 0, 0] and c.number(4) == 0 and c.number(4) == 1, "one exact life2, no value rows, one presence")
    require([c.number(), c.number(), c.number(1)] == [1, 0, 1] and not c.field(256) and c.number() == 0 and c.number(4) == 1, "exact presence component/axis")
    scope = b"TS\1" + bytes([profile]) + axis_id + definition + bytes([6])
    require(c.field(53) == scope, "whole native-domain creation")
    changes = P.Cursor(c.field())
    c.end()
    require(changes.take(4) == b"CC\1" + bytes([profile]) and changes.take(16) == axis_id and changes.take(32) == definition
            and changes.number(4) == 1 and changes.field(53) == scope and changes.take(34) == bytes(34)
            and changes.take(2) == bytes([1, 2]) and [changes.number() for _ in range(4)] == [2, 0, round_, 1], "literal absent→life2 original revision/provenance")
    changes.end()


def processes(rows):
    require(type(rows) is list and len(rows) == 12, "complete genuine native cohort replacement")
    stopped = {}
    for member, row in zip(range(1, 7), rows[:6]):
        raw = T.decoded(P.raw(row["Raw"]))
        require(row["Member"] == raw["Member"] == member and row["Action"] == "stop" and row["Reaped"] is True
                and row["Signal"] == raw["Signal"] == "clean-exit" and raw["Reaped"] is True and raw["Success"] is True
                and raw["ExitCode"] == 0 and raw["Signaled"] is False and raw["Instance"] == row["Instance"] and raw["Volume"] == row["Volume"], "actual graceful stop/reap, not crash/power-loss evidence")
        integer(raw["PID"], minimum=1)
        stopped[member] = row, raw
    for member, row in zip(range(1, 7), rows[6:]):
        raw = T.decoded(P.raw(row["Raw"]))
        old, prior = stopped[member]
        require(row["Member"] == raw["Member"] == member and row["Action"] == "reopen" and row["Ready"] is True and raw["Ready"] is True
                and row["Volume"] == raw["Volume"] == old["Volume"] and row["PreviousInstance"] == raw["PreviousInstance"] == old["Instance"]
                and raw["PID"] != prior["PID"] and raw["ReadyKind"] == "restored-and-caught-up-under-leader-read-index"
                and raw["RecoveryRaw"] == row["RecoveryRaw"], "actual replacement in the same durable directory")
        restored = C.process_response(P.raw(row["RecoveryRaw"]))
        require(restored["code"] == restored["mask"] == 0 and row["Instance"] == f'voter{member}/pid{raw["PID"]}/{restored["incarnation"].hex()}', "actual replacement native frame")
    return stopped


def program_scope_roles(context):
    # Shape alone is not authority. The fresh program caller authenticates the
    # actual elections/native owners/process roster before passing this value.
    require(type(context) is dict and set(context) == {"leaders", "target"}, "explicit authenticated program role context")
    leaders, target = context["leaders"], context["target"]
    require(type(leaders) is tuple and leaders in ((1, 4), (2, 6)) and all(type(v) is int for v in leaders), "actual supported elected leader layout")
    reader = 3 if leaders[0] == 2 else 2
    require(type(target) is int and target in range(1, 7) and target not in leaders and target not in (reader, 2, 5), "selected native target preserves caller and retained reader")
    return leaders, target


def scope_reader(*, selected_processes=None, program_roles=None):
    if program_roles is None:
        return 3 if selected_processes is None else 2
    require(selected_processes is not None, "fresh role context requires separately authenticated actual process loss")
    leaders, _ = program_scope_roles(program_roles)
    return 3 if leaders[0] == 2 else 2


def response_incarnation(wire, member, stopped):
    native = C.process_response(wire)
    require(type(member) is int and member in stopped and
            stopped[member][0]["Instance"].endswith("/" + native["incarnation"].hex()),
            "actual request/result source incarnation")


def check(data, *, failure_member=3, selected_processes=None, program_roles=None):
    # Fresh campaign callers first authenticate the native batch/kill roster.
    # Recheck its actual incarnation lineage here before allowing it to explain
    # a prepared reply from the selected process's earlier incarnation. Ordinary
    # check(data) retains its exact actor3/cohort-only contract.
    require(type(failure_member) is int and failure_member == scope_reader(selected_processes=selected_processes, program_roles=program_roles),
            "ordinary scope reader or explicit fresh campaign reader")
    leaders, selected_target = (1, 4), None
    if program_roles is not None:
        require(selected_processes is not None, "fresh role context requires separately authenticated actual process loss")
        leaders, selected_target = program_scope_roles(program_roles)
    require(type(data) is dict and set(data) == HEADER and data["Version"] == 1 and data["Profile"] == PROFILE and data["Mode"] in ("Fresh", "After"), "fresh finite P01 source profile")
    integer(data["Version"], 1, 1)
    scopes, old_round = P.cut(data["Before"])
    original = C.decision_manifest(P.raw(data["Original"]), P.raw(data["Manifest"]), 8, scopes, data["Before"])
    original["axis"] = original_input(original)
    _, manifest, decision, outcome = P.peer_phase(P.raw(data["Decision"]["Request"]), P.raw(data["Decision"]["Reply"]), scopes, leaders[1])
    require(manifest == original["manifest"] and decision["phase"] == 3 and decision["partition"] == 8 and decision["tx"] == original["tx"] and decision["commit"] and decision["round"] > old_round, "original B durable COMMIT before A resolution")
    records, statuses = prefix(data, original, outcome)
    lookup(data["Lookup"], scopes, original, outcome, source_voter=leaders[1])
    unavailable(C.process_response(P.raw(data["Failure"])), data["FailureMask"])
    P.resumed(data["PendingCaller"], scopes, original["source"]["request"], original["tx"], P.business_hash(original["wire"], 8), 1, b"")
    P.resumed(data["Retry"], scopes, original["source"]["request"], original["tx"], P.business_hash(original["wire"], 8), 0, outcome)
    parsed = {name: observation(data[name], name == "Recovered", scopes, original["configuration"]) for name in ("Old", "Held", "Recovered", "Retained", "Reopened")}
    old, current = parsed["Old"][0], parsed["Recovered"][0]
    require(all(parsed[name][0] == old and parsed[name][1] == {} for name in ("Held", "Retained", "Reopened"))
            and parsed["Old"][2] == 1 and parsed["Reopened"][2] == 3, "retained original A-only certificate/facts through reopen")
    require(parsed["Recovered"][2] == (1 if data["Mode"] == "Fresh" else 4)
            and parsed["Recovered"][3] == (bytes(16) if data["Mode"] == "Fresh" else original["tx"]), "same actual requested Fresh/After original")
    require(current["round"] >= decision["round"] and current["sourceRound"] == decision["round"] and current["sourceIndex"] == statuses[3, 4]["index"]
            and old["index"] <= data["From"][0] and current["index"] <= data["Through"][0], "old/current actual source cut coordinates")
    require(records.get((3, current["index"]), {}).get("Outcome") is not None and Q.control(records[3, current["index"]]["Outcome"])["round"] == current["round"], "new scoped certificate actual original publication")
    before, after = P.projected(data["Before"]), P.projected(data["Current"])
    require([e["id"] for e in data["Before"]["Native"]["entityCensus"] if e["found"]] == [17]
            and [e["id"] for e in data["Current"]["Native"]["entityCensus"] if e["found"]] == [1, 17], "literal full graph census, no phantom business rows")
    keys = ("A/entity/1", "A/presence/1", "A/labels/1", "A/life/1/2")
    changes = P.writes(before, after, keys)
    require(after["A/presence/1"] == dict(present=True, null=False, handle=2, revision=decision["round"], provenance=1, kind="life", i64=0)
            and after["A/labels/1"] == [], "complete literal new life/presence/labels")
    transactions = {"create-A": dict(reads=[P.read_key(before, "A/entity/1")], writes=changes)}
    observations = [dict(event="Before", scope=["A", "B"], round=old_round, values=before),
                    dict(event="Current", scope=["A", "B"], round=P.cut(data["Current"])[1], values=after)]
    for name, (cert, observed, _, _) in parsed.items():
        expected = {k: v for k, v in (after if observed else before).items() if k.startswith("A/")}
        observations.append(dict(event=name, scope=["A"], round=cert["round"], values=expected))
    witness, detail = P.oracle()({"initial": before, "transactions": transactions}, {"create-A"}, {"create-A": decision["round"]}, observations,
                                [dict(event="final", scope=["A", "B"], values=after)])
    require(witness is not None, "independent exact serial/scoped-cut witness: " + str(detail))
    stopped = processes(data["Processes"])
    selected = {}
    if selected_processes is not None:
        require(type(selected_processes) is list and len(selected_processes) == 2, "exact actual selected native kill/reopen")
        killed, reopened = selected_processes
        member = integer(killed["Member"], 6, 1)
        raw, restored = T.decoded(P.raw(killed["Raw"])), T.decoded(P.raw(reopened["Raw"]))
        require(set(raw) == set("Member PID ExitCode SignalNumber Signaled Reaped Volume Stderr Receipt".split()) and
                (member in (3, 6) if selected_target is None else member == selected_target) and killed["Action"] == "native-batch-transition-kill" and killed["Reaped"] is True and killed["Signal"] == "SIGKILL"
                and raw["Member"] == raw["Receipt"]["Member"] == member and integer(raw["PID"], minimum=1) == raw["Receipt"]["PID"] and raw["Reaped"] is True and raw["Signaled"] is True
                and raw["SignalNumber"] == 9 and raw["ExitCode"] == -1 and killed["Instance"].startswith(f'voter{member}/pid{raw["PID"]}/')
                and raw["Volume"] == killed["Volume"], "actual selected native process death")
        require(reopened["Member"] == restored["Member"] == member and reopened["Action"] == "reopen" and reopened["Ready"] is True
                and reopened["PreviousInstance"] == restored["PreviousInstance"] == killed["Instance"] and
                reopened["Volume"] == restored["Volume"] == killed["Volume"] and raw["PID"] != restored["PID"]
                and reopened["Instance"] == stopped[member][0]["Instance"] and restored["RecoveryRaw"] == reopened["RecoveryRaw"],
                "selected native same-directory replacement is the actual later stopped process")
        selected[member] = killed["Instance"]
    for row in data["Prepared"]:
        native = C.process_response(P.raw(row["State"]))
        actual = "/" + native["incarnation"].hex()
        require(stopped[row["Member"]][0]["Instance"].endswith(actual) or
                selected.get(row["Member"], "").endswith(actual), "prepared reply bound to its actual retired native process")
    for field, member in (("Failure", failure_member), ("PendingCaller", 5), ("Retry", 5)):
        response_incarnation(P.raw(data[field]), member, stopped)
    return dict(mode=data["Mode"], coordinator=8, dataScope=[3], serial_witness=witness, records=len(records),
                scope="finite genuine six-OS-child P01 source bridge; no full storage/network family or runtime acceptance")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path, nargs="+")
    args = parser.parse_args()
    require(len(args.artifact) <= 2, "finite Fresh/After source inventory")
    for path in args.artifact:
        print(json.dumps(check(load(path)), sort_keys=True))


if __name__ == "__main__":
    main()
