#!/usr/bin/env python3
"""Actual elected-role/message programs over the existing protocol oracles.

Role context is admitted only after native owner, process incarnation, packet
and selected storage receipts join. No wire member is rewritten for an oracle.
"""
import argparse
import hashlib
import json
from pathlib import Path

import native_protocol_campaign_check as H

P, B, F, Q, T = H.P, H.B, H.F, H.Q, H.T
require, integer, fixed = H.require, H.integer, H.fixed
PROFILE = "native-protocol-causal-program-v1"
PROGRAM = set("Profile Spec DeadlineUnixNanos Elections Successors CommandSHA256 MessageFrom MessageTo Packets Frames FrameCandidates TickResponse WorkGrant OutputGrant".split())
SPEC = {"Version", "Layout", "Message"}
ELECTION = {"Prior", "Leader", "Configuration", "State", "Owner"}
PACKET_EVENT = set("Action From To Ordinal Bytes SHA256 ConfiguredDelayMillis ObservedDelayNanos".split())


def spec(value):
    require(type(value) is dict and set(value) == SPEC and type(value["Version"]) is int and value["Version"] == 1 and
            value["Layout"] in ("original", "rotated") and value["Message"] in ("literal", "delay", "reorder", "duplicate"),
            "explicit finite actual leader/message program")
    return (1, 4) if value["Layout"] == "original" else (2, 6)


def idle_owner(owner, run, deadline, member, pid):
    require(type(owner) is dict and set(owner) == B.RECEIPT and type(owner["Version"]) is int and owner["Version"] == 1 and owner["Profile"] == B.PROFILE,
            "actual native election observer owner grammar")
    require(fixed(owner["Run"], 16, True) == run and fixed(owner["Cookie"], 16, True) and type(owner["Member"]) is int and owner["Member"] == member and
            type(owner["PID"]) is int and owner["PID"] == pid and type(owner["DeadlineUnixNanos"]) is int and owner["DeadlineUnixNanos"] == deadline and owner["State"] == "idle",
            "same original native process/run/deadline before selected Submit")
    require(all(type(owner[k]) is int and owner[k] == 0 for k in ("Sequence", "Attempts", "Work", "Entries", "Count")) and
            type(owner["Output"]) is int and 8192 <= owner["Output"] <= 16384 and
            type(owner["Events"]) is list and len(owner["Events"]) == 2,
            "idle inspection cannot claim an applied native boundary")
    control = owner["Control"]
    require(type(control) is dict and set(control) == B.CONTROL and control["Version"] == control["Sequence"] == control["Kind"] == 0 and
            control["Action"] == control["Side"] == "" and fixed(control["Cookie"], 16) == bytes(16) and
            fixed(control["CommandSHA256"], 32) == bytes(32), "no manufactured election-arm authority")
    for event in owner["Events"]:
        B.zero_event(event)


def frame(row, scopes):
    require(type(row) is dict and set(row) == {"From", "To", "Snapshot", "Payload"} and row["Snapshot"] is False,
            "actual ordinary native packet frame")
    sender, receiver = integer(row["From"], 6, 1), integer(row["To"], 6, 1)
    require(sender != receiver and (sender <= 3) == (receiver <= 3), "actual same-partition native routing")
    wire = P.raw(row["Payload"])
    require(0 < len(wire) <= 2 << 20, "finite real packet ownership")
    message = F.raft_packet(wire, scopes, 3 if sender <= 3 else 8)
    require(message.get(2) == [receiver] and message.get(3) == [sender], "native protobuf sender/recipient matches configured envelope")
    return wire, message


def conditional_restart(member, old, restored, terminal):
    raw = T.decoded(P.raw(old["Raw"]))
    require(old["Action"] == "stop" and old["Member"] == raw["Member"] == member and
            old["Reaped"] is raw["Reaped"] is raw["Success"] is True and raw["Signaled"] is False and raw["ExitCode"] == 0 and
            old["Instance"] == raw["Instance"] and old["Volume"] == raw["Volume"], "actual pre-reopen OS clean stop")
    state = F.process(P.raw(restored["RecoveryRaw"]))
    stopped, stop_raw = terminal
    require(restored["Action"] == "reopen" and restored["Member"] == member and restored["Ready"] is True and
            restored["PreviousInstance"] == old["Instance"] and restored["Volume"] == old["Volume"] == stopped["Volume"] and
            restored["Instance"] == stopped["Instance"] and stop_raw["PID"] != raw["PID"] and
            restored["Instance"].endswith("/"+state["incarnation"].hex()) and state["code"] == state["mask"] == 0 and
            old["EventID"] < restored["EventID"] < stopped["EventID"], "actual same-volume/new-incarnation source restart")
    # processStateOp returns certifiedProcessOwners.ready, not follower
    # catch-up readiness. Only the actual reopened source leaders observe
    # configuration and own that flag; all six still require native success,
    # exact process lineage and their later real clean reap above.
    if member in (1, 4):
        require(state["ready"] == 1 and state["stats"][2] > 0 and state["stats"][3] == member,
                "actual restored original source leader layout1/4")
    return raw


def authenticate(a):
    # The wrapper is a distinct artifact contract. The ordinary campaign
    # entry point continues to reject its extra evidence and profile.
    require(type(a) is dict and set(a) == {"Version", "Profile", "Case", "Evidence"} and
            type(a["Version"]) is int and a["Version"] == 2 and a["Profile"] == PROFILE,
            "explicit fresh program artifact boundary")
    e = a["Evidence"]
    require(type(e) is dict and ("P01" in e and set(e) == H.SCOPE_EVIDENCE | {"Program"} or
            "P02" in e and set(e) in (H.FENCE_EVIDENCE | {"Program"}, H.FENCE_EVIDENCE | {"Program", "FenceTransition"}) or
            "P03" in e and set(e) == H.COORDINATOR_EVIDENCE | {"Program"} or
            "Partial" in e and set(e) == H.PARTIAL_EVIDENCE | {"Program", "ProgramCohort"} or
            "P04" in e and set(e) == H.FROZEN_EVIDENCE | {"Program", "ProgramCohort"} or
            "P05" in e and set(e) == H.CONDITIONAL_EVIDENCE | {"Program", "ProgramCohort"} | ({"ProgramReopenAfter"} if e["Recipe"]["Schedule"] == "write-skew-A-first" else set()) or
            "Completion" in e and set(e) == H.EVIDENCE | {"Program", "ProgramCohort"}),
            "implemented semantic adapter and full native evidence")
    p01, live, coordinator, partial, frozen, conditional = "P01" in e, "Completion" in e, "P03" in e, "Partial" in e, "P04" in e, "P05" in e
    semantic = e["P01"] if p01 else e["Completion"]["P06"] if live else H.M.FAULT.lc.normalize(e["P03"]) if coordinator else e["Partial"] if partial else e["P04"] if frozen else e["P05"] if conditional else e["P02"]
    program = e["Program"]
    require(type(program) is dict and set(program) == PROGRAM and program["Profile"] == PROFILE, "fresh program owner grammar")
    expected = spec(program["Spec"])
    run, deadline = fixed(e["Run"], 16, True), integer(e["DeadlineUnixNanos"], (1 << 63)-1, 1)
    require(program["DeadlineUnixNanos"] == deadline and program["OutputGrant"] == 16 << 20 and
            program["WorkGrant"] == (128+13)*32*(2 << 20), "same fixed original deadline and separately prepaid full diagnostic work")
    selected = P.raw(e["SelectedCommand"])
    require(0 < len(selected) <= 64 << 10 and fixed(program["CommandSHA256"], 32, True) == hashlib.sha256(selected).digest(),
            "message window belongs to actual selected native command")
    # These checks precede any optional semantic-role context. Both native
    # receipts and the full stopped/reopened roster remain actual identities.
    H.C.batch_pair(e["Arm"], e["Batch"], run, deadline, e["Target"], e["Recipe"]["Kind"], e["Recipe"]["Side"], selected)
    B.processes(e["TargetProcesses"], None, a["Case"], e["Batch"])
    stage = not p01 and not live and not coordinator and not partial and not frozen and not conditional and e["Recipe"]["Schedule"] == "persisted-fence-before-application"
    if live or partial or frozen or conditional:
        if live:
            H.I.first_processes(semantic["Processes"], expected[0])
        elif not conditional:
            H.C.C.processes(semantic["Processes"], None, "partial-install-decision-loss", program_roles=dict(leaders=expected))
        stopped = {}
        require(type(e["ProgramCohort"]) is list and len(e["ProgramCohort"]) == 6, "complete actual terminal clean-stop cohort")
        for member, row in enumerate(e["ProgramCohort"], 1):
            raw = T.decoded(P.raw(row["Raw"]))
            require(row["Member"] == raw["Member"] == member and raw["PID"] > 0 and
                    row["Action"] == "stop" and row["Reaped"] is raw["Reaped"] is raw["Success"] is True and
                    row["Signal"] == raw["Signal"] == "clean-exit" and raw["Signaled"] is False and raw["ExitCode"] == 0 and
                    row["Instance"] == raw["Instance"] and row["Volume"] == raw["Volume"] and
                    row["Instance"].startswith(f"voter{member}/pid{raw['PID']}/"), "genuine terminal OS process wait/identity")
            stopped[member] = row, raw
    else:
        stopped = Q.processes(semantic["Processes"] if p01 else semantic["faults"][1]["ProcessEvidence"] if coordinator else e["Cohort"])
    rejoin_stopped = stopped
    native_instances = {(member, raw["PID"]): process["Instance"] for member, (process, raw) in stopped.items()}
    if conditional:
        require(not (program["Successors"] or []), "conditional fixture has no hidden selected-window successor")
        reopened = semantic["Processes"] or []
        if e["Recipe"]["Schedule"] == "write-skew-A-first":
            require(type(reopened) is list and len(reopened) == 12 and
                    type(e["ProgramReopenAfter"]) is int and 0 < e["ProgramReopenAfter"] < len(semantic["Phases"]),
                    "actual sole cohort barrier and exact cumulative selected-frame ordinal")
            rejoin_stopped = {}
            for member, (old, restored) in enumerate(zip(reopened[:6], reopened[6:]), 1):
                raw = conditional_restart(member, old, restored, stopped[member])
                rejoin_stopped[member] = old, raw
                native_instances[member, raw["PID"]] = old["Instance"]
        else:
            require(not reopened and "ProgramReopenAfter" not in e, "no invented conditional source switch")
    if stage:
        predecessor = e["FenceTransition"]
        H.C.batch_pair(predecessor["Arm"], predecessor["Batch"], run, deadline, predecessor["Target"],
                       predecessor["Kind"], predecessor["Side"], P.raw(predecessor["Proposal"]))
        B.processes(predecessor["TargetProcesses"], None, "actual-predecessor-fence", predecessor["Batch"])
    rosters = ([e["TargetProcesses"], e["FenceTransition"]["TargetProcesses"]] if stage else
               [e["TargetProcesses"], semantic["Processes"]] if live or partial or frozen else
               [e["TargetProcesses"], semantic["faults"][0]["ProcessEvidence"][1:]] if coordinator else [e["TargetProcesses"]])
    for roster in rosters:
        killed, reopened = roster
        raw = T.decoded(P.raw(killed["Raw"]))
        member, pid = raw["Member"], raw["PID"]
        require(killed["Volume"] == stopped[member][0]["Volume"] and killed["Instance"].startswith(f"voter{member}/pid{pid}/"),
                "native loss is in the SAME real semantic cohort directory")
        key = member, pid
        require(key not in native_instances or native_instances[key] == killed["Instance"], "ambiguous actual process incarnation")
        native_instances[key] = killed["Instance"]
        if live or partial or frozen or conditional:
            require(reopened["Instance"] == rejoin_stopped[member][0]["Instance"] and reopened["Volume"] == rejoin_stopped[member][0]["Volume"] and
                    reopened["EventID"] < rejoin_stopped[member][0]["EventID"], "same real replacement later cleanly reaped")
    elections = program["Elections"]
    require(type(elections) is list and len(elections) == 2, "both genuine initial leader groups")
    leaders = []
    def election(row, prior, member):
        require(type(row) is dict and set(row) == ELECTION and type(row["Prior"]) is type(row["Leader"]) is int and row["Prior"] == prior and row["Leader"] == member,
                "actual configured causal native election identity")
        configuration, state = (F.process(P.raw(row[k])) for k in ("Configuration", "State"))
        pid = integer(row["Owner"]["PID"], minimum=1)
        idle_owner(row["Owner"], run, deadline, member, pid)
        require(configuration["code"] == configuration["mask"] == state["code"] == state["mask"] == 0 and state["ready"] == 1 and
                configuration["sequence"] < state["sequence"] and configuration["incarnation"] == state["incarnation"] and
                native_instances.get((member, pid), "").endswith("/"+state["incarnation"].hex()) and
                state["stats"][2] > 0 and state["stats"][3] == member,
                "actual elected native term/vote/read-index and genuine process incarnation")
        return state

    for group, row in enumerate(elections):
        require(type(row) is dict and set(row) == ELECTION and row["Prior"] == (1, 4)[group] and
                type(row["Leader"]) is int and row["Leader"] == expected[group], "real old/new elected role, never renumbered DTO")
        member = row["Leader"]
        election(row, (1, 4)[group], member)
        leaders.append(member)
    successors = program["Successors"] or []
    require(type(successors) is list and len(successors) <= 2, "bounded real successor-election observations")
    current = list(leaders)
    if stage:
        successor = e["FenceTransition"]["SuccessorEvidence"]
        next_a = 3-leaders[0]
        election(successor, leaders[0], next_a)
        require(e["FenceTransition"]["Target"] == leaders[0] and e["FenceTransition"]["Successor"] == next_a and
                elections[0]["Owner"]["Cookie"] == e["FenceTransition"]["Arm"]["Cookie"],
                "actual initial leader is the native predecessor killed before the genuine successor")
        require(successors == ([] if e["Recipe"]["Phase"] == 0 else [successor]), "SAME actual post-fence source proof, no implicit current substitution")
        roles = dict(initial_leaders=tuple(leaders), leaders=(next_a, leaders[1]), target=e["Target"])
        H.program_fence_roles(roles, e["Recipe"])
        if e["Recipe"]["Phase"] != 0:
            current[0] = next_a
    elif conditional:
        require(not successors, "conditional selected follower does not replace its live authority peer")
        roles = dict(leaders=tuple(leaders), target=e["Target"], reopen_after=e.get("ProgramReopenAfter", 0))
        H.program_conditional_roles(roles, e["Recipe"])
    elif frozen:
        require(not successors, "native queued first decision precedes genuine later B owner kill")
        roles = dict(leaders=tuple(leaders), target=e["Target"])
        H.program_frozen_roles(roles, e["Recipe"])
        require(semantic["Queue"]["Member"] == leaders[1] and semantic["Queue"]["PID"] == elections[1]["Owner"]["PID"] and
                e["Frozen"]["Member"] == leaders[1] and e["Frozen"]["PID"] == semantic["Queue"]["PID"],
                "both genuine native candidates and frozen probes remain in actual current B process")
    elif partial:
        outage = e["Recipe"]["Phase"] == 4 and e["Recipe"]["Partition"] == 8
        if outage:
            require(len(successors) == 1, "actual B successor before selected partial recovery")
            election(successors[0], leaders[1], 5)
            current[1] = 5
        else:
            require(not successors, "actual selected early partial occurrence precedes B decision-source loss")
        roles = dict(initial_leaders=tuple(leaders), leaders=tuple(current), target=e["Target"])
        H.program_partial_roles(roles, e["Recipe"])
        require(not (outage and e["Recipe"]["Kind"] == 1 and program["Spec"]["Message"] != "literal"),
                "selected serving B Ready precedes its native messages")
    elif coordinator:
        current = list(leaders)
        co_group = 0 if e["Recipe"]["Coordinator"] == 3 else 1
        if e["Recipe"]["Phase"] == 4:
            require(len(successors) == 1, "actual durable-D successor before recovery selection")
            next_leader = 2 if co_group == 0 and leaders[0] == 1 else 3 if co_group == 0 else 5
            election(successors[0], leaders[co_group], next_leader)
            current[co_group] = next_leader
        else:
            require(not successors, "early selected occurrence precedes genuine D leader loss")
        roles = dict(initial_leaders=tuple(leaders), leaders=tuple(current), target=e["Target"])
        H.program_coordinator_roles(roles, e["Recipe"])
        require(not (e["Recipe"]["Phase"] == 4 and e["Recipe"]["Partition"] == e["Recipe"]["Coordinator"] and
                     e["Recipe"]["Kind"] == 1 and program["Spec"]["Message"] != "literal"),
                "serving recovery leader Ready precedes its selected emitted frames")
    elif live:
        require(len(successors) == 1, "actual A election while SAME original B caller remains live")
        election(successors[0], leaders[0], 3-leaders[0])
        current[0] = 3-leaders[0]
        roles = dict(initial_leaders=tuple(leaders), leaders=tuple(current), target=e["Target"], successor=e["Successor"])
        H.program_live_roles(roles)
        require(e["Recipe"]["Kind"] != 1 or program["Spec"]["Message"] == "literal",
                "selected B submitting-leader Ready precedes its own emitted Raft messages")
    else:
        require(not successors, "no unexplained causal election in live-owner scenario")
        roles = dict(leaders=tuple(leaders), target=e["Target"])
        Q.program_scope_roles(roles)
    group = 1 if live else 0 if e["Recipe"]["Partition"] == 3 else 1
    selected_election = elections[group]
    for successor in successors:
        if (successor["Leader"] <= 3) == (group == 0):
            selected_election = successor
    if e["Target"] == current[group]:
        require(selected_election["Owner"]["Cookie"] == e["Arm"]["Cookie"] and selected_election["Owner"]["PID"] == e["Arm"]["PID"],
                "selected local leader is the SAME actually observed native owner")
    to = next(member for member in range(1+3*group, 4+3*group) if member not in (current[group], e["Target"]))
    require(type(program["MessageFrom"]) is type(program["MessageTo"]) is int and program["MessageFrom"] == current[group] and
            1+3*group <= program["MessageTo"] < 4+3*group and program["MessageTo"] != current[group] and
            (program["MessageTo"] != e["Target"] or e["Target"] == current[group]) and
            (program["MessageTo"] == to if program["Spec"]["Message"] == "literal" else True),
            "actual program endpoints derived from native elections and selected member")
    rows, frames, action = program["Packets"] or [], program["Frames"] or [], program["Spec"]["Message"]
    if action == "literal":
        require(rows == frames == [] and program["FrameCandidates"] == 0 and not P.raw(program["TickResponse"]),
                "literal program cannot claim a simulated delivery")
    else:
        require(type(rows) is list and len(rows) == 1 and type(frames) is list and 1 <= len(frames) <= 2 and
                integer(program["FrameCandidates"], 128, 1), "genuinely observed bounded packet operation")
        event = rows[0]
        require(type(event) is dict and set(event) == PACKET_EVENT and event["Action"] == action and
                type(event["From"]) is type(event["To"]) is type(event["Ordinal"]) is int and
                event["From"] == current[group] and event["To"] == program["MessageTo"] and event["Ordinal"] == 1,
                "actual matching scheduler receipt and configured directed link")
        if conditional:
            scopes, _, _ = H.N.cut(semantic["Cut"])
        else:
            observation = dict(Cut=semantic["steps"][3]["Cut"]) if coordinator else semantic["Before"] if p01 or live or partial or frozen else semantic["Old"]
            scopes, _ = H.P.cut(observation)
        wire, message = frame(frames[0], scopes)
        initial = F.process(P.raw(selected_election["State"]))
        require(frames[0]["From"] == event["From"] and frames[0]["To"] == event["To"] and len(wire) == event["Bytes"] and
                hashlib.sha256(wire).digest() == fixed(event["SHA256"], 32, True) and message.get(4, [0])[0] >= initial["stats"][2],
                "retained exact native packet joins its actual scheduled hash")
        require(event["ConfiguredDelayMillis"] == (20 if action == "delay" else 0) and type(event["ObservedDelayNanos"]) is int and
                (event["ObservedDelayNanos"] >= 20_000_000 if action == "delay" else event["ObservedDelayNanos"] == 0),
                "actual delay elapsed; other programs cannot invent elapsed time")
        for successor in frames[1:]:
            frame(successor, scopes)
        if action == "reorder":
            require(len(frames) == 2, "real next queue frame required for reordered delivery")
            tick = F.process(P.raw(program["TickResponse"]))
            require(tick["code"] == tick["mask"] == 0 and tick["incarnation"] == initial["incarnation"] and
                    tick["sequence"] > initial["sequence"] and tick["packets"], "actual one-Tick successor on SAME native leader")
        else:
            require(not P.raw(program["TickResponse"]), "only reorder admits the explicit successor Tick")
    return roles


def check(a):
    roles = authenticate(a)
    result = H.check_scope(a, program_roles=roles) if "P01" in a["Evidence"] else H.check_live(a, program_roles=roles) if "Completion" in a["Evidence"] else H.check_coordinator(a, program_roles=roles) if "P03" in a["Evidence"] else H.check_partial(a, program_roles=roles) if "Partial" in a["Evidence"] else H.check_frozen(a, program_roles=roles) if "P04" in a["Evidence"] else H.check_conditional(a, program_roles=roles) if "P05" in a["Evidence"] else H.check_fence(a, program_roles=roles)
    return dict(program=a["Evidence"]["Program"]["Spec"], **result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    args = parser.parse_args()
    print(json.dumps(check(H.load(args.artifact, program=True)), sort_keys=True))


if __name__ == "__main__":
    main()
