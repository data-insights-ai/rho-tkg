#!/usr/bin/env python3
"""Actual elected-role/packet programs over the complete pooled state oracle.

The role argument is passed only after real election, process and native
boundary receipts join. It never changes a wire identity or supplies authority.
"""
import argparse
import hashlib
import json
from pathlib import Path

import native_protocol_campaign_state_check as C
import native_protocol_campaign_program_check as K

P, F, T, R = C.P, C.F, C.T, C.R
require, integer, fixed = C.require, C.integer, C.fixed
EVIDENCE = set("Program Target Window Transport MessageCommand MessageRequest MessageReply BusinessOriginal BlockedCollectorResponse LocalRecord Cohort".split())


def stopped_cohort(rows):
    require(type(rows) is list and len(rows) == 6, "all six real owners are reaped")
    result = {}
    for member, row in enumerate(rows, 1):
        require(type(row) is dict and type(row["Member"]) is int and row["Member"] == member,
                "actual ordered native cohort, no renamed member")
        raw = T.decoded(P.raw(row["Raw"]))
        require(type(raw["Member"]) is int and raw["Member"] == member and type(raw["PID"]) is int and raw["PID"] > 0 and
                row["Action"] == "stop" and row["Reaped"] is raw["Reaped"] is raw["Success"] is True and
                row["Signal"] == raw["Signal"] == "clean-exit" and raw["Signaled"] is False and raw["ExitCode"] == 0 and
                row["Instance"] == raw["Instance"] and row["Volume"] == raw["Volume"] and
                row["Instance"].startswith(f"voter{member}/pid{raw['PID']}/"), "actual OS clean exit/identity/volume")
        result[member] = row, raw
    return result


def elections(program, run, deadline, cohort):
    expected = K.spec(program["Spec"])
    require(type(program["Elections"]) is list and len(program["Elections"]) == 2 and
            not (program["Successors"] or []), "both actual initial elections; selected followers do not replace a leader")
    result = []
    for group, row in enumerate(program["Elections"]):
        member = expected[group]
        require(type(row) is dict and set(row) == K.ELECTION and type(row["Prior"]) is type(row["Leader"]) is int and
                row["Prior"] == (1, 4)[group] and row["Leader"] == member, "actual configured elected layout")
        configuration, state = (F.process(P.raw(row[k])) for k in ("Configuration", "State"))
        stopped, raw = cohort[member]
        K.idle_owner(row["Owner"], run, deadline, member, raw["PID"])
        require(configuration["code"] == configuration["mask"] == state["code"] == state["mask"] == 0 and state["ready"] == 1 and
                configuration["sequence"] < state["sequence"] and configuration["incarnation"] == state["incarnation"] and
                stopped["Instance"].endswith("/"+state["incarnation"].hex()) and
                state["stats"][2] > 0 and state["stats"][3] == member, "same native elected process, actual term/vote and owner")
        result.append(state)
    return expected, result


def packet_program(program, scopes, part, target, leaders, states):
    group = 0 if part == 3 else 1
    sender, receiver = program["MessageFrom"], program["MessageTo"]
    require(type(sender) is type(receiver) is int and sender == leaders[group] and
            receiver in range(1+3*group, 4+3*group) and receiver not in (sender, target),
            "real directed native link excludes selected follower")
    events, frames = program["Packets"] or [], program["Frames"] or []
    action = program["Spec"]["Message"]
    if action == "literal":
        require(events == frames == [] and type(program["FrameCandidates"]) is int and program["FrameCandidates"] == 0 and
                not P.raw(program["TickResponse"]), "literal cannot claim simulated delivery")
        return
    require(type(events) is list and len(events) == 1 and type(frames) is list and 1 <= len(frames) <= 2 and
            integer(program["FrameCandidates"], 128, 1), "actual finite selected packet operation")
    event = events[0]
    require(type(event) is dict and set(event) == K.PACKET_EVENT and event["Action"] == action and
            type(event["From"]) is type(event["To"]) is type(event["Ordinal"]) is int and
            (event["From"], event["To"], event["Ordinal"]) == (sender, receiver, 1), "actual scheduler directed-link receipt")
    wire, message = K.frame(frames[0], scopes)
    require(frames[0]["From"] == sender and frames[0]["To"] == receiver and type(event["Bytes"]) is int and
            len(wire) == event["Bytes"] and hashlib.sha256(wire).digest() == fixed(event["SHA256"], 32, True) and
            message.get(4, [0])[0] >= states[group]["stats"][2], "same real packet/term/hash and scheduler operation")
    require(type(event["ConfiguredDelayMillis"]) is int and event["ConfiguredDelayMillis"] == (20 if action == "delay" else 0) and
            type(event["ObservedDelayNanos"]) is int and
            (event["ObservedDelayNanos"] >= 20_000_000 if action == "delay" else event["ObservedDelayNanos"] == 0), "actual configured delay elapsed")
    for row in frames[1:]:
        K.frame(row, scopes)
    if action == "reorder":
        tick = F.process(P.raw(program["TickResponse"]))
        require(len(frames) == 2 and tick["code"] == tick["mask"] == 0 and tick["incarnation"] == states[group]["incarnation"] and
                tick["sequence"] > states[group]["sequence"] and tick["packets"], "actual native successor Tick precedes release")
    else:
        require(len(frames) == 1 and not P.raw(program["TickResponse"]), "only reorder admits a successor Tick")


def collector_limit(response):
    require(type(response["code"]) is int and response["code"] == 2 and type(response["mask"]) is int and
            response["mask"] != 0 and response["mask"] & ~((1 << 2) | (1 << 5)) == 0,
            "actual known native/local limit, no unavailable/invalid/unknown substitution")


def blocked_collector(a, e, leaders, cohort):
    expected = a["Recipe"]["Family"] == "gc" or a["Recipe"]["Family"] == "move" and a["Recipe"]["Phase"] != 9
    wire = P.raw(e["BlockedCollectorResponse"])
    if not expected:
        require(not wire, "no invented held-reader collector refusal")
        return
    response = R.response(e["BlockedCollectorResponse"])
    collector_limit(response)
    receipt = T.decoded(response["aux"])
    member = leaders[0]
    require(type(receipt) is dict and set(receipt) == R.RECEIPT and receipt["Profile"] == C.PROFILE and
            receipt["Action"] == "collect" and receipt["Member"] == member and receipt["PID"] == cohort[member][1]["PID"] and
            receipt["Run"] == a["Run"] and receipt["DeadlineUnixNanos"] == a["DeadlineUnixNanos"] and
            receipt["Progress"]["Complete"] is False and cohort[member][0]["Instance"].endswith("/"+response["incarnation"].hex()),
            "same actual collector/held reader owner and original deadline")


def source_window(a, e, scopes, leaders, rows, states):
    family = a["Recipe"]["Family"]
    command = P.raw(e["MessageCommand"])
    require(0 < len(command) <= 64 << 10, "bounded actual predecessor or selected command")
    if family in ("move", "pin"):
        require((e["Window"], e["Transport"]) == ("selected-command", "native-batch") and command == P.raw(a["Command"]) and
                all(not P.raw(e[k]) for k in ("MessageRequest", "MessageReply", "BusinessOriginal", "LocalRecord")),
                "actual selected native command window, no extra predecessor claim")
    elif family == "gc":
        require((e["Window"], e["Transport"]) == ("committed-K-predecessor", "source-submit") and not P.raw(e["LocalRecord"]),
                "local collector has no Raft command emission; actual K predecessor is separate")
        actual, manifest, status, outcome = P.peer_phase(P.raw(e["MessageRequest"]), P.raw(e["MessageReply"]), scopes, leaders[0])
        _, lost_manifest, lost, _ = P.peer_phase(P.raw(a["LostRequest"]), P.raw(a["LostReply"]), scopes, leaders[1])
        require(actual == command and status["partition"] == 3 and status["phase"] == 2 and status["yes"] is True and
                manifest == lost_manifest and status["tx"] == lost["tx"] and
                P.raw(rows[3, status["index"]]["Outcome"]) == outcome and not P.raw(rows[3, status["index"]]["CDC"]),
                "genuine same K YES native original precedes local GC")
        business = P.pgq(P.raw(e["BusinessOriginal"]))
        c = P.Cursor(P.body(manifest, b"GCM\1")); c.take(16); coordinator = c.number(); original = c.field(32768)
        require(coordinator == business["partition"] and P.business_hash(original) == P.business_hash(P.raw(e["BusinessOriginal"])),
                "actual immutable K proposal belongs to complete captured manifest")
    else:
        require(family == "publication-race" and (e["Window"], e["Transport"]) == ("new-root-predecessor", "local-propose") and
                not P.raw(e["BusinessOriginal"]), "actual existing healthy-label root predecessor, no invented collector packet")
        c = P.Cursor(P.body(P.raw(e["MessageRequest"]), b"GSC\1"))
        require(P.body(c.field(156), b"APS\1") == scopes[3] and c.field() == command, "actual native Propose scope/command")
        c.end(); original = P.pgq(command)
        reply, record = R.response(e["MessageReply"]), R.response(e["LocalRecord"])
        require(reply["code"] == reply["mask"] == record["code"] == record["mask"] == 0 and
                reply["incarnation"] == record["incarnation"] == states[0]["incarnation"] and
                states[0]["sequence"] < reply["sequence"] < record["sequence"], "actual Propose reply followed by native retained record")
        outcome = P.body(record["outcome"], b"GRO\4")
        require(len(outcome) == 84 and outcome[0] == original["kind"] and outcome[1:17] == P.GRAPH and
                int.from_bytes(outcome[17:25], "big") == original["partition"] == 3 and outcome[25:41] == original["request"] and
                outcome[41:73] == hashlib.sha256(command).digest() and outcome[81:] == bytes((1, 0, 0)),
                "actual complete healthy-label original/outcome/index, not a reply label")
        index = int.from_bytes(outcome[73:81], "big")
        row = rows[3, index]
        require(P.raw(row["Outcome"]) == record["outcome"] and P.raw(row["CDC"]) == record["changes"] and record["changes"],
                "actual changing-answer root predecessor joins full native prefix")
    return command


def authenticate(a):
    require(type(a) is dict and type(a.get("Version")) is int and a["Version"] == 3 and
            a.get("Profile") == C.PROGRAM_PROFILE and set(a) == C.HEADER | {"Pooled", "Program"}, "fresh exact state program artifact")
    e = a["Program"]
    require(type(e) is dict and set(e) == EVIDENCE, "complete actual state program evidence")
    program = e["Program"]
    require(type(program) is dict and set(program) == K.PROGRAM and program["Profile"] == K.PROFILE, "same native program owner grammar")
    C.recipe(a["Recipe"])
    run, deadline = fixed(a["Run"], 16, True), integer(a["DeadlineUnixNanos"], (1 << 63)-1, 1)
    require(type(program["DeadlineUnixNanos"]) is int and program["DeadlineUnixNanos"] == deadline and
            type(program["OutputGrant"]) is int and program["OutputGrant"] == 16 << 20 and
            type(program["WorkGrant"]) is int and program["WorkGrant"] == (128+13)*32*(2 << 20), "original deadline and prepaid diagnostic ownership")
    cohort = stopped_cohort(e["Cohort"])
    leaders, states = elections(program, run, deadline, cohort)
    roles = dict(leaders=leaders, target=e["Target"])
    _, target = C.program_state_roles(roles, a["Recipe"])
    scopes, _, _ = C.cut(a["Before"], C.POOLED_SEMANTIC)
    family = a["Recipe"]["Family"]
    if family in ("move", "pin"):
        C.batch(a, scopes, roles); native_catchup = C.processes(a, a["Batch"], roles)
        receipt = a["Batch"]
    elif family == "gc":
        C.S.pair(a["StateArm"], a["State"], run, deadline, member=target)
        native_catchup = C.processes(a, a["State"], roles); receipt = a["State"]
    else:
        require(family == "publication-race" and not (a["Processes"] or []), "publication predecessor does not claim follower death")
        receipt = None
    if receipt is not None:
        failed = a["Processes"][0]
        replacement = next(row for row in a["Processes"][1:] if row["Member"] == target and row["Ready"] is True)
        restored = R.response(replacement["RecoveryRaw"])
        require(type(receipt["Member"]) is int and receipt["Member"] == target and
                failed["Instance"].startswith(f"voter{target}/pid{receipt['PID']}/") and
                replacement["Instance"] == cohort[target][0]["Instance"] and replacement["Volume"] == cohort[target][0]["Volume"] and
                cohort[target][1]["PID"] != receipt["PID"] and restored["code"] == restored["mask"] == 0 and (native_catchup or restored["ready"] == 1) and
                replacement["Instance"].endswith("/"+restored["incarnation"].hex()) and
                replacement["EventID"] < cohort[target][0]["EventID"], "selected actual follower replacement later reaped in SAME durable volume")
    rows = C.prefix(a)
    command = source_window(a, e, scopes, leaders, rows, states)
    require(fixed(program["CommandSHA256"], 32, True) == hashlib.sha256(command).digest(), "actual command owns packet window")
    packet_program(program, scopes, a["Recipe"]["Partition"], target, leaders, states)
    blocked_collector(a, e, leaders, cohort)
    return roles


def check(a):
    roles = authenticate(a)
    C.check(a, program_roles=roles)
    return dict(program=a["Program"]["Program"]["Spec"], recipe=a["Recipe"], verdict="complete-independent-state-oracle")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    args = parser.parse_args()
    print(json.dumps(check(C.load(args.artifact, program=True)), sort_keys=True))


if __name__ == "__main__":
    main()
