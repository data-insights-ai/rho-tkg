#!/usr/bin/env python3
"""Finite initial Register Sync interruption BEFORE genuine P06 first YES.

The surviving native coordinator retains its private original. This checker
joins actual source/IPC bytes, both crash/replacement orders and complete raw
originals to the existing independent serial oracle. Receipts are diagnostics,
not reconstructed authority, native ACKs or a whole-D/runtime acceptance claim.
"""
import argparse
import hashlib
import json
from pathlib import Path
import native_transition_conjunction_check as C

T, P, B, F = C.T, C.P, C.B, C.F
require, integer, fixed = C.require, C.integer, C.fixed
PROFILE = "native-p06-initial-register-sync-conjunction-v1"
HEADER = set("Version Profile Case Recipe Run DeadlineUnixNanos Actor Target Successor FirstVoter RecoveryVoter RecoveryBVoter P06 InitialCall InitialPhase Calls Arm Batch SelectedCommand TargetTransportLoss Processes InitialReopened FirstReopened ReceiptReads ReceiptGrantBytes GateCandidates GateDecodeWork InitialCapture RecoveryCapture".split())
CHARGE = {"Candidates", "Attempts", "Work", "Output", "Retained"}


def recipe(row):
    C.recipe(row)  # Exact existing four-coordinate semantic base; no extra phase.
    return f'p06-co{row["Coordinator"]}-commit{str(row["Commit"]).lower()}-initial-register-k{row["Kind"]}-{row["Side"]}'


def routing(row):
    recipe(row)
    return (3, 1, 2, 2, 1, 4) if row["Coordinator"] == 3 else (5, 4, 6, 1, 2, 6)


def load(path, *, program=False):
    digest, total, header, footer, rows = hashlib.sha256(), 0, None, None, []
    with path.open("rb") as source:
        while line := source.readline((512 << 10)+1):
            total += len(line)
            require(len(line) <= 512 << 10 and total <= 16 << 20 and line.endswith(b"\n"), "finite initial frame/artifact")
            row = T.decoded(line)
            require(type(row) is dict and footer is None, "initial row after EOF")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and row["Type"] == "header", "initial header")
                header = row["Artifact"]
                require(type(header) is dict and set(header) == HEADER | ({"Program"} if program else set()) and type(header["P06"]) is dict and set(header["P06"]) == C.P06 and header["P06"]["Records"] is None, "fresh initial grammar/streamed prefix")
            elif row.get("Type") == "record":
                require(set(row) == {"Type", "Record"} and len(rows) < 512, "finite initial prefix")
                rows.append(row["Record"])
            else:
                require(set(row) == {"Type", "Rows", "Through", "PayloadSHA256"} and row["Type"] == "footer" and type(row["Rows"]) is int and row["Rows"] == len(rows) and row["PayloadSHA256"] == digest.hexdigest(), "complete initial prefix hash/census")
                footer = row
            if footer is None:
                digest.update(line)
    require(header is not None and footer is not None and footer["Through"] == header["P06"]["Through"], "missing/foreign initial EOF")
    header["P06"]["Records"] = rows
    return header


def charge(value, rows):
    require(type(value) is dict and set(value) == CHARGE, "actual source charge grammar")
    candidates = integer(value["Candidates"], 256, 1)
    require(type(value["Attempts"]) is int and candidates >= value["Attempts"] == len(rows) and value["Work"] == candidates*(4096+32*(2 << 20)) and value["Output"] == len(rows)*((128 << 10)+8192), "cumulative capture prepay")
    require(integer(value["Retained"], 256 << 10) == sum(len(P.raw(r["Request"]))+len(P.raw(r["Reply"])) for r in rows), "actual owned source bytes")


def first_processes(rows, voter):
    require(type(rows) is list and len(rows) == 2, "actual later first-YES process pair")
    killed, restored = rows
    raw, reopened = T.decoded(P.raw(killed["Raw"])), T.decoded(P.raw(restored["Raw"]))
    require(killed["Action"] == "kill" and killed["Member"] == raw["Member"] == voter and killed["Reaped"] is True and killed["Signal"] == "SIGKILL" and raw["SignalNumber"] == 9 and raw["Signaled"] is True and raw["Reaped"] is True and raw["Success"] is False and raw["Instance"] == killed["Instance"] and raw["Volume"] == killed["Volume"], "genuine later prepared-leader death/reap")
    require(restored["Action"] == "reopen" and restored["Member"] == reopened["Member"] == voter and restored["Ready"] is True and restored["PreviousInstance"] == reopened["PreviousInstance"] == killed["Instance"] and restored["Volume"] == reopened["Volume"] == killed["Volume"] and restored["RecoveryRaw"] == reopened["RecoveryRaw"] and raw["PID"] != reopened["PID"], "later same-directory first-YES replacement")
    return F.process(P.raw(reopened["LocallyRestoredRaw"]))


def recovery(rows, scopes, actor, coordinator, voters, manifest, tx, records):
    require(type(rows) is list and len(rows) == 5, "five genuine recovery replies")
    order, seen = [(2, 3), (2, 8), (3, coordinator), (4, 3), (4, 8)], set()
    for row, expected in zip(rows, order):
        _, command, correlation = C.source_request(row, scopes, actor)
        require(correlation not in seen and (command["phase"], command["partition"]) == expected and command["manifest"] == manifest, "actual recovery order/original/correlation")
        seen.add(correlation)
        _, actual_manifest, status, outcome = P.peer_phase(P.raw(row["Request"]), P.raw(row["Reply"]), scopes, voters[row["Partition"]])
        require(actual_manifest == manifest and status["tx"] == tx and P.raw(records[status["partition"], status["index"]]["Outcome"]) == outcome, "recovery response missing actual stored original")


def check(a, *, program_roles=None):
    require(type(a) is dict and set(a) == HEADER and type(a["Version"]) is int and a["Version"] == 1 and a["Profile"] == PROFILE and a["Case"] == recipe(a["Recipe"]), "fresh initial Register profile")
    r, d = a["Recipe"], a["P06"]
    coordinator, commit = r["Coordinator"], r["Commit"]
    actor, target, successor, first, recovered, b_voter = routing(r)
    if program_roles is not None:
        roles=C.program_voters(program_roles);group=0 if coordinator==3 else 1
        target,successor=roles["initial"][group],roles["after"][group]
        first=roles["after"][0];recovered=3-first;b_voter=roles["after"][1]
    require(tuple(a[k] for k in ("Actor", "Target", "Successor", "FirstVoter", "RecoveryVoter", "RecoveryBVoter")) == (actor, target, successor, first, recovered, b_voter), "actual pre-YES leader/peer inventory")
    require(type(d) is dict and set(d) == C.P06 and type(d["Version"]) is int and d["Version"] == 1 and d["Coordinator"] == coordinator and d["Commit"] is commit, "same semantic P06 parent")
    scopes, _ = P.cut(d["Before"])
    prepare, manifest, yes, yes_wire = P.peer_phase(P.raw(d["FirstYES"]["Request"]), P.raw(d["FirstYES"]["Reply"]), scopes, first)
    require(prepare == P.raw(d["Manifest"]) and yes["yes"] and yes["partition"] == 3, "actual post-election first YES")
    original = C.C.decision_manifest(P.raw(d["Original"]), manifest, coordinator, scopes, d["Before"])
    tx, digest = original["tx"], original["digest"]
    records, decision = C.phase_census(d, tx, digest, coordinator, commit)
    require(yes["tx"] == tx and P.raw(records[3, yes["index"]]["Outcome"]) == yes_wire, "first YES absent from complete native prefix")
    selected, command, initial_correlation = C.source_request(a["InitialPhase"], scopes, actor)
    require(not P.raw(a["InitialPhase"]["Reply"]) and command["phase"] == 1 and command["partition"] == coordinator and command["manifest"] == manifest and not command["proofs"] and command["conflict"] is None and selected == P.raw(a["SelectedCommand"]), "unknown Register is not a genuine pre-YES original submission")
    run, deadline = fixed(a["Run"], 16, True), integer(a["DeadlineUnixNanos"], (1 << 63)-1, 1)
    event = C.batch_pair(a["Arm"], a["Batch"], run, deadline, target, r["Kind"], r["Side"], selected)
    B.processes(a["Processes"], None, a["Case"], a["Batch"])
    local = first_processes(d["Processes"], first)
    require(local["applied"] >= yes["index"], "first YES replacement lacks its own retained vote")
    process_order = a["Processes"]+d["Processes"]
    events = [integer(row["EventID"], minimum=1) for row in process_order]
    require(events == sorted(set(events)), "initial native loss/rejoin must PRECEDE genuine first-YES loss/rejoin")
    if r["Kind"] == 2:
        row = records.get((coordinator, event["Index"]))
        require(row is not None and P.status(P.raw(row["Outcome"]))["phase"] == 1 and P.status(P.raw(row["Outcome"]))["index"] == event["Index"] and not P.raw(row["CDC"]), "initial install differs from retained original registration")
    require(type(a["TargetTransportLoss"]) is str and 0 < len(a["TargetTransportLoss"]) <= 4096, "typed initial native pipe death")
    call = a["InitialCall"]
    require(type(call) is dict and set(call) == {"Reply", "Transport", "Pending"} and call["Pending"] is True, "genuine initial pending call")
    response = F.process(P.raw(call["Reply"]))
    C.unknown_invocation(response, call["Transport"])
    pending = F.process(P.raw(d["Pending"]))
    require(pending["incarnation"] == response["incarnation"] and pending["sequence"] > response["sequence"], "surviving SAME coordinator did not retry before first YES")
    decision_wire = P.raw(records[coordinator, decision["index"]]["Outcome"])
    for wire in (call["Reply"], d["Pending"]):
        P.resumed(wire, scopes, original["source"]["request"], tx, P.business_hash(original["wire"], coordinator), 1, b"")
    require(type(a["Calls"]) is list and len(a["Calls"]) == 2 and a["Calls"][0] == d["Decision"], "actual decision and same-directory original replay")
    for wire in a["Calls"]:
        returned = F.process(P.raw(wire))
        require(returned["incarnation"] == pending["incarnation"] and returned["sequence"] > pending["sequence"] and returned["code"] == returned["mask"] == 0 and not returned["outcome"] and not returned["changes"] and not returned["packets"] and not returned["reads"], "actual successful SAME original invocation")
        P.resumed(wire, scopes, original["source"]["request"], tx, P.business_hash(original["wire"], coordinator), 0, decision_wire)
        pending = returned
    recovery(d["Phases"], scopes, actor, coordinator, {3: recovered, 8: b_voter}, manifest, tx, records)
    require(initial_correlation not in [C.source_request(row, scopes, actor)[2] for row in d["Phases"]], "unknown Register borrowed recovery reply correlation")
    require(type(d["Duplicates"]) is list and len(d["Duplicates"]) == 3, "three actual late terminal duplicates")
    for i, row in enumerate(d["Duplicates"]):
        require(type(row) is dict and set(row) == C.LOCAL, "terminal duplicate grammar")
        command, status = C.C.decision_command(P.raw(row["Proposal"])), P.status(P.raw(row["Outcome"]))
        require(command["phase"] == (2 if i == 0 else 4) and command["partition"] == row["Partition"] == (3 if i < 2 else 8) and command["manifest"] == manifest and status["phase"] == 4 and status["tx"] == tx and status["digest"] == digest and status["index"] < row["Index"] and not P.raw(row["CDC"]) and records.get((row["Partition"], row["Index"])) == {k:row[k] for k in ("Partition", "Index", "Outcome", "CDC")}, "terminal original/current Resolve/empty CDC")
    for observed in (d["Retained"], a["InitialReopened"], a["FirstReopened"]):
        T.semantic(observed == d["Before"], "old-cut:exact-initial-register-history-and-index")
    charge(a["InitialCapture"], [a["InitialPhase"]]); charge(a["RecoveryCapture"], d["Phases"])
    require(integer(a["ReceiptReads"], 129, 1)*65536 == a["ReceiptGrantBytes"] and integer(a["GateCandidates"], 128, 1)*(4096+32*(2 << 20)) == a["GateDecodeWork"], "prepaid receipt/full envelope observations")
    witness = C.literal_and_serial(d, decision)
    return dict(case=a["Case"], records=len(records), first_voter=first, serial_witness=witness, scope="initial Register before later genuine first YES only; first-Prepare/leader/message/other P/state conjunction and runtime qualification remain open")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+", type=Path)
    args = parser.parse_args()
    require(len(args.artifacts) <= 16, "finite initial Register invocation")
    seen, results = set(), []
    for path in args.artifacts:
        result = check(load(path))
        require(result["case"] not in seen, "duplicate initial recipe")
        seen.add(result["case"]); results.append(result)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
