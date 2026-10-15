#!/usr/bin/env python3
"""Initial Prepare Sync loss, preserving absent replies and genuine stored YES."""
import argparse
import hashlib
import json
from pathlib import Path
import native_transition_initial_check as I

C, P, B, F, T = I.C, I.P, I.B, I.F, I.T
require, integer, fixed = I.require, I.integer, I.fixed
PROFILE = "native-p06-first-prepare-sync-conjunction-v1"
HEADER = (I.HEADER-{"InitialPhase", "InitialReopened"}) | {"InitialPhases", "EarlyReopened", "Survivor"}


def recipe(row):
    C.recipe(row)
    return f'p06-co{row["Coordinator"]}-commit{str(row["Commit"]).lower()}-first-prepare-k{row["Kind"]}-{row["Side"]}'


def lost_yes(row):
    recipe(row)
    return row["Kind"] == 2 and row["Side"] == "after"


def load(path, *, program=False):
    digest, total, header, footer, rows = hashlib.sha256(), 0, None, None, []
    with path.open("rb") as source:
        while line := source.readline((512 << 10)+1):
            total += len(line)
            require(len(line) <= 512 << 10 and total <= 16 << 20 and line.endswith(b"\n"), "finite first-Prepare frame/artifact")
            row = T.decoded(line)
            require(type(row) is dict and footer is None, "row after first-Prepare EOF")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and row["Type"] == "header", "first-Prepare header")
                header = row["Artifact"]
                require(type(header) is dict and set(header) == HEADER | ({"Program"} if program else set()) and type(header["P06"]) is dict and set(header["P06"]) == C.P06 and header["P06"]["Records"] is None, "fresh first-Prepare grammar/streamed prefix")
            elif row.get("Type") == "record":
                require(set(row) == {"Type", "Record"} and len(rows) < 512, "finite first-Prepare prefix")
                rows.append(row["Record"])
            else:
                require(set(row) == {"Type", "Rows", "Through", "PayloadSHA256"} and row["Type"] == "footer" and type(row["Rows"]) is int and row["Rows"] == len(rows) and row["PayloadSHA256"] == digest.hexdigest(), "complete first-Prepare prefix hash/census")
                footer = row
            if footer is None:
                digest.update(line)
    require(header is not None and footer is not None and footer["Through"] == header["P06"]["Through"], "missing/foreign first-Prepare EOF")
    header["P06"]["Records"] = rows
    return header


def stored_vote(outcome, changes, event, tx, digest, records):
    status = P.status(outcome)
    require(status["partition"] == 3 and status["phase"] == 2 and status["yes"] and status["tx"] == tx and status["digest"] == digest and status["index"] == event["Index"] and not changes, "actual survivor original differs from selected YES Sync")
    row = records.get((3, status["index"]))
    require(row is not None and P.raw(row["Outcome"]) == outcome and not P.raw(row["CDC"]), "survivor YES missing from full retained original census")
    return status


def unanswered_yes(boundary, initial, pending, first_call):
    require(type(boundary) is dict and set(boundary) == {"Request", "Reply"} and boundary["Request"] == initial["Request"] and not P.raw(boundary["Reply"]) and pending == first_call, "lost first YES cannot borrow a GSR or another invocation")


def check(a, *, program_roles=None):
    require(type(a) is dict and set(a) == HEADER and type(a["Version"]) is int and a["Version"] == 1 and a["Profile"] == PROFILE and a["Case"] == recipe(a["Recipe"]), "fresh first-Prepare profile")
    r, d, no_reply = a["Recipe"], a["P06"], lost_yes(a["Recipe"])
    co, commit = r["Coordinator"], r["Commit"]
    actor, first, recovered = (3 if co == 3 else 5), (1 if no_reply else 2), (2 if no_reply else 1)
    target,successor,b_voter,registered_voter=1,2,4,(1 if co==3 else 4)
    if program_roles is not None:
        roles=C.program_voters(program_roles);target,successor=roles["initial"][0],roles["after"][0]
        first,recovered=(target,successor) if no_reply else (successor,target)
        b_voter=roles["initial"][1];registered_voter=roles["initial"][0 if co==3 else 1]
    require(tuple(a[k] for k in ("Actor", "Target", "Successor", "FirstVoter", "RecoveryVoter", "RecoveryBVoter")) == (actor, target, successor, first, recovered, b_voter), "actual first-Prepare routing")
    require(type(d) is dict and set(d) == C.P06 and type(d["Version"]) is int and d["Version"] == 1 and d["Coordinator"] == co and d["Commit"] is commit, "same semantic P06 original")
    scopes, _ = P.cut(d["Before"])
    rows = a["InitialPhases"]
    require(type(rows) is list and len(rows) == 2, "Register success then actual unanswered first Prepare")
    _, registration, register_correlation = C.source_request(rows[0], scopes, actor)
    selected, prepare, unknown_correlation = C.source_request(rows[1], scopes, actor)
    require(registration["phase"] == 1 and registration["partition"] == co and not registration["proofs"] and registration["conflict"] is None and prepare["phase"] == 2 and prepare["partition"] == 3 and registration["manifest"] == prepare["manifest"] and prepare["conflict"] is None and register_correlation != unknown_correlation and not P.raw(rows[1]["Reply"]) and selected == P.raw(a["SelectedCommand"]) == P.raw(d["Manifest"]), "actual initial command/correlation/absent reply")
    manifest = prepare["manifest"]
    original = C.C.decision_manifest(P.raw(d["Original"]), manifest, co, scopes, d["Before"])
    tx, digest = original["tx"], original["digest"]
    records, decision = C.phase_census(d, tx, digest, co, commit)
    _, actual_manifest, registered, register_wire = P.peer_phase(P.raw(rows[0]["Request"]), P.raw(rows[0]["Reply"]), scopes, registered_voter)
    require(actual_manifest == manifest and registered["phase"] == 1 and registered["tx"] == tx and registered["digest"] == digest and P.raw(records[co, registered["index"]]["Outcome"]) == register_wire and prepare["proof_wires"] == [register_wire], "first Prepare lacks actual immutable Register proof")
    run, deadline = fixed(a["Run"], 16, True), integer(a["DeadlineUnixNanos"], (1 << 63)-1, 1)
    event = C.batch_pair(a["Arm"], a["Batch"], run, deadline, target, r["Kind"], r["Side"], selected)
    B.processes(a["Processes"], None, a["Case"], a["Batch"])
    if no_reply:
        survivor = a["Survivor"]
        require(type(survivor) is dict and set(survivor) == {"Member", "Reply", "Native", "InputGrantBytes", "OutputGrantBytes"} and type(survivor["Member"]) is int and survivor["Member"] == recovered and survivor["Native"] == a["Batch"] and type(survivor["InputGrantBytes"]) is int and survivor["InputGrantBytes"] == 512 and type(survivor["OutputGrantBytes"]) is int and survivor["OutputGrantBytes"] == 8*(2 << 20) and a["EarlyReopened"] is None and d["Processes"] is None and d["FirstYES"]["Request"] == rows[1]["Request"] and not P.raw(d["FirstYES"]["Reply"]) and d["Pending"] == a["InitialCall"]["Reply"], "lost installed YES must not invent source ACK or second death")
        actual = F.process(P.raw(survivor["Reply"]))
        unanswered_yes(d["FirstYES"], rows[1], d["Pending"], a["InitialCall"]["Reply"])
        require(actual["code"] == actual["mask"] == 0 and not actual["packets"] and not actual["reads"], "actual survivor ApplicationRecord read")
        yes = stored_vote(actual["outcome"], actual["changes"], event, tx, digest, records)
        process_order = a["Processes"]
        classification = "selected-installed-YES-without-GSR"
    else:
        require(a["Survivor"] is None and a["EarlyReopened"] is not None, "uninstalled/unknown boundary borrowed a stored-YES guarantee")
        command, actual_manifest, yes, outcome = P.peer_phase(P.raw(d["FirstYES"]["Request"]), P.raw(d["FirstYES"]["Reply"]), scopes, first)
        require(command == selected and actual_manifest == manifest and yes["tx"] == tx and yes["yes"] and P.raw(records[3, yes["index"]]["Outcome"]) == outcome, "actual later YES acknowledgement/current original")
        if r["Kind"] == 2:
            require(yes["index"] == event["Index"], "install-before retries SAME retained Prepare original")
        local = I.first_processes(d["Processes"], first)
        require(local["applied"] >= yes["index"], "later YES replacement lacks local original")
        T.semantic(a["EarlyReopened"] == d["Before"], "old-cut:first-Prepare-before-observed-YES")
        process_order = a["Processes"]+d["Processes"]
        classification = "repeated-stored-YES-GSR" if yes["index"] == event["Index"] else "later-original-YES-GSR"
    require(yes["partition"] == 3 and yes["digest"] == digest, "first YES native namespace/original")
    events = [integer(row["EventID"], minimum=1) for row in process_order]
    require(events == sorted(set(events)), "actual supported crash/rejoin causal order")
    call = a["InitialCall"]
    require(type(call) is dict and set(call) == {"Reply", "Transport", "Pending"} and call["Pending"] is True and type(a["TargetTransportLoss"]) is str and 0 < len(a["TargetTransportLoss"]) <= 4096, "actual pending first-Prepare/target loss")
    response, pending = F.process(P.raw(call["Reply"])), F.process(P.raw(d["Pending"]))
    C.unknown_invocation(response, call["Transport"])
    require(response["incarnation"] == pending["incarnation"] and (pending["sequence"] == response["sequence"] if no_reply else pending["sequence"] > response["sequence"]), "same retained coordinator/actual unknown or later cancellation")
    for wire in (call["Reply"], d["Pending"]):
        P.resumed(wire, scopes, original["source"]["request"], tx, P.business_hash(original["wire"], co), 1, b"")
    decision_wire = P.raw(records[co, decision["index"]]["Outcome"])
    require(type(a["Calls"]) is list and len(a["Calls"]) == 2 and a["Calls"][0] == d["Decision"], "actual recovery/reopen original calls")
    for wire in a["Calls"]:
        actual = F.process(P.raw(wire))
        require(actual["incarnation"] == pending["incarnation"] and actual["sequence"] > pending["sequence"] and actual["code"] == actual["mask"] == 0 and not actual["outcome"] and not actual["changes"] and not actual["packets"] and not actual["reads"], "actual successful same-original coordinator call")
        P.resumed(wire, scopes, original["source"]["request"], tx, P.business_hash(original["wire"], co), 0, decision_wire)
        pending = actual
    I.recovery(d["Phases"], scopes, actor, co, {3: recovered, 8: b_voter}, manifest, tx, records)
    require(unknown_correlation not in [C.source_request(row, scopes, actor)[2] for row in d["Phases"]], "missing first GSR borrowed a later successful reply")
    require(type(d["Duplicates"]) is list and len(d["Duplicates"]) == 3, "actual late Prepare/two Resolve duplicates")
    for i, row in enumerate(d["Duplicates"]):
        require(type(row) is dict and set(row) == C.LOCAL, "duplicate grammar")
        command, status = C.C.decision_command(P.raw(row["Proposal"])), P.status(P.raw(row["Outcome"]))
        require(command["phase"] == (2 if i == 0 else 4) and command["partition"] == row["Partition"] == (3 if i < 2 else 8) and command["manifest"] == manifest and status["phase"] == 4 and status["tx"] == tx and status["digest"] == digest and status["index"] < row["Index"] and not P.raw(row["CDC"]) and records.get((row["Partition"], row["Index"])) == {k:row[k] for k in ("Partition", "Index", "Outcome", "CDC")}, "terminal current Resolve/original/empty CDC")
    for observed in (d["Retained"], a["FirstReopened"]):
        T.semantic(observed == d["Before"], "old-cut:first-Prepare-actual-local-history-and-index")
    I.charge(a["InitialCapture"], rows); I.charge(a["RecoveryCapture"], d["Phases"])
    require(integer(a["ReceiptReads"], 129, 1)*65536 == a["ReceiptGrantBytes"] and integer(a["GateCandidates"], 128, 1)*(4096+32*(2 << 20)) == a["GateDecodeWork"], "prepaid receipt/full source envelope traversal")
    return dict(case=a["Case"], records=len(records), observed_yes=classification, serial_witness=C.literal_and_serial(d, decision), scope="first-Prepare actual native boundary only; other leader/message/P/state conjunction and runtime qualification remain open")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+", type=Path)
    args = parser.parse_args()
    require(len(args.artifacts) <= 16, "finite first-Prepare invocation")
    results, seen = [], set()
    for path in args.artifacts:
        result = check(load(path))
        require(result["case"] not in seen, "duplicate first-Prepare recipe")
        seen.add(result["case"]); results.append(result)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
