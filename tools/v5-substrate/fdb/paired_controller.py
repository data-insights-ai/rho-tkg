#!/usr/bin/env python3
"""Attach actual owned Docker process evidence to the backend's paired trace."""
import base64
import hashlib
import json
from pathlib import Path
import sys

import launcher


def encode(value):
    return base64.b64encode(json.dumps(value, separators=(",", ":")).encode()).decode()


def committed_marker(marker_bytes, receipt_bytes):
    marker = json.loads(marker_bytes)
    assert marker == {"ready": "after-native-commit-before-reply",
                      "receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest()}


def inspected(value):
    assert len(value) == 1
    return value[0]


def instance(record):
    state = record["State"]
    assert state["Running"] and state["Pid"] > 0
    assert not state["OOMKilled"] and state["StartedAt"]
    return f'{record["Id"]}:{state["Pid"]}:{state["StartedAt"]}'


def volume(record):
    mounts = [m for m in record["Mounts"] if m["Destination"] == "/var/fdb/data"]
    assert len(mounts) == 1 and mounts[0]["Type"] == "volume"
    return mounts[0]["Name"]


def command(ledger, action, name):
    matches = [(i, c) for i, c in enumerate(ledger)
               if c["command"] == ["docker", *action, name]]
    assert len(matches) == 1
    i, c = matches[0]
    assert c["exit"] == 0 and not c["deadline_expired"]
    return i, c


def killed(before, after, cmd, role, member, event):
    b, a = inspected(before), inspected(after)
    assert b["Id"] == a["Id"] and b["Name"] == a["Name"]
    assert a["State"]["ExitCode"] == 137 and not a["State"]["Running"]
    assert not a["State"]["OOMKilled"]
    return {"EventID": event, "Action": "kill", "Role": role, "Member": member,
            "Instance": instance(b), "Reaped": True,
            "Signal": "SIGKILL", "OOMKilled": a["State"]["OOMKilled"],
            "Volume": volume(b) if role == "storage" else "",
            "Raw": encode({"BeforeInspect": before, "AfterInspect": after,
                           "Command": cmd})}


def reopened(before, stopped, after, cmd, status, member, event):
    b, s, a = map(inspected, (before, stopped, after))
    assert b["Id"] == s["Id"] == a["Id"]
    assert not s["State"]["Running"] and s["State"]["ExitCode"] == 137
    assert not s["State"]["OOMKilled"]
    launcher.healthy(status)
    assert instance(b) != instance(a) and volume(b) == volume(a)
    return {"EventID": event, "Action": "reopen", "Role": "storage", "Member": member,
            "PreviousInstance": instance(b), "Instance": instance(a), "Ready": True,
            "RecoveryRaw": encode(status), "Volume": volume(a),
            "Raw": encode({"BeforeInspect": before, "StoppedInspect": stopped,
                           "AfterInspect": after, "Command": cmd, "ReadyStatus": status})}


def finalize(trace, directory):
    artifacts = {}

    def raw(name):
        data = (directory / name).read_bytes()
        artifacts[name] = hashlib.sha256(data).hexdigest()
        return data

    def load(name):
        return json.loads(raw(name))

    # The launcher continues appending cleanup/status commands after this
    # finalizer. Seal the exact observed prefix without claiming its hash is
    # the later complete ledger hash.
    ledger_bytes = (directory / "commands.jsonl").read_bytes()
    (directory / "paired-controller-commands.jsonl").write_bytes(ledger_bytes)
    ledger = [json.loads(line) for line in raw("paired-controller-commands.jsonl").splitlines()]
    faults = trace["faults"]
    assert len(faults) == 4 and trace["fixtureID"] == "graph-fault-local-v1"
    for i, phase in enumerate(("before", "after")):
        marker = raw(f"{phase}-child-marker.json")
        if phase == "after":
            committed_marker(marker, raw("after-child-pre-kill-committed.json"))
        else:
            launcher.marker(json.loads(marker), phase)
        b, a = load(f"{phase}-child-live-inspect.json"), load(f"{phase}-child-killed.json")
        name = inspected(b)["Name"].lstrip("/")
        event_index, cmd = command(ledger, ["kill", "--signal", "KILL"], name)
        faults[i]["MarkerRaw"] = base64.b64encode(marker).decode()
        stop = killed(b, a, cmd, "client", phase, event_index + 1)
        faults[i]["CallerRaw"] = encode({"Marker": json.loads(marker), "Kill": stop})
        faults[i]["ProcessEvidence"] = [stop]
        if phase == "after":
            pre_kill = load("after-child-pre-kill-committed.json")
            assert pre_kill["Original"] == faults[i]["OriginalRequest"]["Payload"]
            assert pre_kill["Outcome"] == trace["steps"][4]["RawObservation"]["Groups"][4]["Outcome"]
            assert pre_kill["Records"] == trace["steps"][4]["RawRecords"]
            faults[i]["MarkerRaw"] = encode({"MarkerRaw": base64.b64encode(marker).decode(),
                                             "PreKillReceiptRaw": base64.b64encode(raw("after-child-pre-kill-committed.json")).decode()})
            marker_reads = [(j, c) for j, c in enumerate(ledger)
                            if c["command"] == ["docker", "logs", name] and j < event_index]
            assert marker_reads
            j, observed_marker = marker_reads[-1]
            result = trace["operationMapping"][4]
            faults[i]["ProcessEvidence"].insert(0, {
                "EventID": j + 1, "Action": "commit", "Role": "client", "Member": phase,
                "Instance": instance(inspected(b)), "Volume": "", "Result": result,
                "Raw": encode({"Marker": json.loads(marker), "MarkerCommand": observed_marker,
                               "PreKillReceipt": pre_kill,
                               "OriginalRecords": pre_kill["Records"]})})

    zone_before, zone_stopped = load("zone-live-inspect.json"), load("zone-killed.json")
    zone_name = inspected(zone_before)["Name"].lstrip("/")
    zone_kills = [(i, c) for i, c in enumerate(ledger)
                  if c["command"] == ["docker", "kill", "--signal", "KILL", zone_name]]
    assert len(zone_kills) == 2
    kill_index, kill_cmd = zone_kills[0]
    assert kill_cmd["exit"] == 0 and not kill_cmd["deadline_expired"]
    start_matches = [(i, c) for i, c in enumerate(ledger)
                     if c["command"] == ["docker", "start", zone_name]]
    # zone0 has a zone restart and the later full-cohort restart.
    assert len(start_matches) == 2
    restart_index = start_matches[0][0]
    progress_commands = [(i, c) for i, c in enumerate(ledger)
                         if "paired-fault-progress" in c["command"] and c["exit"] == 0]
    assert any(kill_index < i < restart_index for i, _ in progress_commands)
    progress_index, progress_command = next((i, c) for i, c in progress_commands
                                            if kill_index < i < restart_index)
    progress_name = progress_command["command"][progress_command["command"].index("--name") + 1]
    progress_number = progress_name.rsplit("client", 1)[-1]
    progress_result = load(f"paired-fault-progress-{progress_number}.json")
    assert len(progress_result["Steps"]) == 6
    # The first successful original progress response was captured before the
    # controller issued zone0's restart. Its complete group/outcome is immutable.
    result_step = progress_result["Steps"][5]
    assert all(trace["steps"][5][key] == value for key, value in result_step.items())
    faults[2]["MarkerRaw"] = encode({"KillCommand": kill_cmd,
                                    "ProgressCommands": progress_commands,
                                    "RestartCommand": start_matches[0][1]})
    progress_inspect = inspected(load(f"paired-fault-progress-{progress_number}-inspect.json"))
    assert not progress_inspect["State"]["Running"] and progress_inspect["State"]["ExitCode"] == 0
    assert not progress_inspect["State"]["OOMKilled"]
    faults[2]["CallerRaw"] = encode(progress_result)
    faults[2]["ProcessEvidence"] = [killed(zone_before, zone_stopped, kill_cmd, "storage", 0, kill_index + 1),
                                    {"EventID": progress_index + 1, "Action": "commit", "Role": "client",
                                     "Member": "progress", "Instance": f'{progress_inspect["Id"]}:{progress_inspect["State"]["StartedAt"]}',
                                     "Volume": "", "Result": trace["operationMapping"][5],
                                     "Raw": encode({"Command": progress_command, "OriginalRecords": result_step["RawRecords"]})}]

    servers = [zone_name[:-1] + str(i) for i in range(4)]
    status = load("status-reopened.json")
    launcher.healthy(status)
    stops, starts, evidence = [], [], []
    for member, name in enumerate(servers):
        before, stopped, after = (load(name + suffix) for suffix in
                                  ("-all-live.json", "-all-stopped.json", "-reopened.json"))
        stop_matches = [(i, c) for i, c in enumerate(ledger)
                        if c["command"] == ["docker", "kill", "--signal", "KILL", name]]
        start = [(i, c) for i, c in enumerate(ledger)
                 if c["command"] == ["docker", "start", name]][-1]
        stop = stop_matches[-1]
        assert stop[1]["exit"] == start[1]["exit"] == 0
        assert not stop[1]["deadline_expired"] and not start[1]["deadline_expired"]
        stops.append(stop[0]); starts.append(start[0])
        # Multiple server inspections are intentional; use the one captured
        # after the final start but before the next client command.
        ready_matches = [(i, c) for i, c in enumerate(ledger)
                         if c["command"] == ["docker", "inspect", name] and i > start[0]]
        assert ready_matches
        ready_index, ready_cmd = ready_matches[0]
        evidence.append(killed(before, stopped, stop[1], "storage", member, stop[0] + 1))
        evidence.append(reopened(before, stopped, after, ready_cmd, status, member, ready_index + 1))
    assert max(stops) < min(starts)
    # ProcessEvidence order follows actual command order, not per-server pairs.
    faults[3]["ProcessEvidence"] = evidence[::2] + evidence[1::2]
    faults[3]["MarkerRaw"] = encode({"StopCommandIndexes": stops,
                                    "RestartCommandIndexes": starts,
                                    "ReadyStatus": status})
    faults[3]["CallerRaw"] = encode({"Classification": "observer-only", "Status": status})
    held_commands = [(i, c) for i, c in enumerate(ledger)
                     if "paired-fault-hold-old" in c["command"]]
    terminal_commands = [(i, c) for i, c in enumerate(ledger)
                         if "paired-fault-terminal" in c["command"]]
    reopened_commands = [(i, c) for i,c in enumerate(ledger)
                         if "paired-fault-reopened" in c["command"]]
    assert len(reopened_commands) == 1 and reopened_commands[0][1]["exit"] == 0
    reopen_retry_event = reopened_commands[0][0] + 1
    assert reopen_retry_event > max(e["EventID"] for e in evidence)
    assert len(faults[3]["ReopenRetries"]) == 6
    for replay in faults[3]["ReopenRetries"]:
        replay["EventID"] = reopen_retry_event
    assert len(held_commands) == len(terminal_commands) == 1
    held_event = held_commands[0][0] + 1
    terminal_event = terminal_commands[0][0] + 1
    assert held_event < min(stops) + 1
    assert held_event < kill_index + 1
    assert terminal_event > max(e["EventID"] for e in evidence)
    for old in trace["oldReads"]:
        held = old["Mode"] == "held-old"
        old["Checkpoint"] = "before-cohort-stop" if held else "after-cohort-reopen"
        old["EventID"] = held_event if held else terminal_event
    trace["controllerArtifacts"] = artifacts
    trace["controllerCommandLedger"] = {"Artifact": "paired-controller-commands.jsonl",
                                        "IsPrefixOf": "commands.jsonl", "Bytes": len(ledger_bytes)}
    return trace


def main():
    if len(sys.argv) == 4 and sys.argv[1] == "marker":
        committed_marker(Path(sys.argv[2]).read_bytes(), Path(sys.argv[3]).read_bytes())
        return
    source, directory, destination = map(Path, sys.argv[1:])
    source_bytes = source.read_bytes()
    trace = finalize(json.loads(source_bytes), directory)
    trace["backendTraceArtifact"] = {"Artifact": source.name,
                                      "SHA256": hashlib.sha256(source_bytes).hexdigest()}
    Path(destination).write_text(json.dumps(trace, indent=2) + "\n")


if __name__ == "__main__":
    main()
