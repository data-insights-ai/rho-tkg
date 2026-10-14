#!/usr/bin/env python3
"""Strict decoder for the NEW real platform schedule, never a fault simulator.

A local four-zone container cluster is one physical host. Role selection uses
live status; a kill loses every listed co-resident role. Network detach is
symmetric domain isolation, not packet loss/duplication/reordering evidence.
"""
import base64
import hashlib
import json
import importlib.util
from pathlib import Path
import sys

import launcher

CASES = ("transaction", "log", "storage", "one-zone", "two-zones", "network", "commit-issued")
FORMAT = "fdb-platform-faults-v1"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def admitted_bytes(path, limit):
    path = Path(path)
    require(0 < path.stat().st_size <= limit, "raw file admission before read/decode")
    with path.open("rb") as source:
        data = source.read(limit + 1)
    require(0 < len(data) <= limit, "raw artifact admission")
    return data


def object_pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate raw JSON field")
        result[key] = value
    return result


def load(path, limit=2 << 20):
    return json.loads(admitted_bytes(path, limit), object_pairs_hook=object_pairs)


def records(path):
    data = admitted_bytes(path, 16 << 20)
    return [json.loads(line, object_pairs_hook=object_pairs) for line in data.splitlines()]


def live(record):
    state = record["State"]
    require(state["Running"] and state["Pid"] > 0 and state["StartedAt"] and
            not state["OOMKilled"], "actual live instance required")
    return (record["Id"], state["Pid"], state["StartedAt"])


def volume(record):
    mounts = [m for m in record["Mounts"] if m["Destination"] == "/var/fdb/data"]
    require(len(mounts) == 1 and mounts[0]["Type"] == "volume", "one owned retained data volume")
    return mounts[0]["Name"]


def inventory(status, inspected, network, directory):
    launcher.healthy(status)
    require(status["cluster"]["configuration"]["storage_engine"] == "ssd-2" and
            status["cluster"]["configuration"]["log_engine"] == "ssd-2", "actual native SSD engines")
    require(len(inspected) == 4, "four owned server containers")
    addresses, names, ids, zones, volumes = set(), set(), set(), set(), set()
    rows = []
    for record in inspected:
        identity = live(record)
        name = record["Name"].lstrip("/")
        links = record["NetworkSettings"]["Networks"]
        require(set(links) == {network}, "foreign/additional server network")
        address = links[network]["IPAddress"] + ":4500"
        top = admitted_bytes(Path(directory) / (name + ".top"), 64 << 10).decode()
        processes = [line.split(None, 1) for line in top.splitlines()[1:] if line.strip()]
        require(len(processes) == 1 and processes[0][0] == str(identity[1]) and
                processes[0][1].split()[0].rsplit("/", 1)[-1] == "fdbserver",
                "status-to-actual-exec-fdbserver PID binding")
        matches = [(key, p) for key, p in status["cluster"]["processes"].items()
                   if p["address"] == address]
        require(len(matches) == 1, "owned address must map to exactly one live FDB process")
        key, process = matches[0]
        zone = process["locality"]["zoneid"]
        require(process["locality"]["machineid"] == zone and
                zone == record["Config"]["Hostname"], "actual zone/machine/hostname binding")
        roles = sorted({r["role"] for r in process["roles"]})
        coordinators = status["client"]["coordinators"]["coordinators"]
        coordinator = any(c["address"] == address for c in coordinators)
        held_volume = volume(record)
        require(address not in addresses and name not in names and identity[0] not in ids and
                zone not in zones and held_volume not in volumes, "duplicate owned identity")
        addresses.add(address); names.add(name); ids.add(identity[0]); zones.add(zone); volumes.add(held_volume)
        rows.append({"Name": name, "Address": address, "Zone": zone, "FDBProcess": key,
                     "Roles": roles, "Coordinator": coordinator, "Instance": list(identity),
                     "Volume": held_volume, "Inspect": record, "FDBStatus": process})
    require(addresses == {p["address"] for p in status["cluster"]["processes"].values()},
            "foreign/unmapped FDB process")
    require(sum(row["Coordinator"] for row in rows) == 3, "actual three owned coordinators")
    return sorted(rows, key=lambda row: row["Zone"])


def targets(rows, case):
    require(case in CASES and len(rows) == 4, "unsupported explicit case")
    if case == "commit-issued":
        return []
    if case in ("two-zones", "network"):
        coordinators = [row for row in rows if row["Coordinator"]]
        require(len(coordinators) == 3, "two of three actual coordinator domains required")
        return coordinators[:2]
    if case == "one-zone":
        return rows[:1]
    accepted = {"transaction": {"commit_proxy", "grv_proxy", "proxy"},
                "log": {"log"}, "storage": {"storage"}}[case]
    selected = [row for row in rows if accepted.intersection(row["Roles"])]
    require(bool(selected), "requested live role not present; no role-loss acceptance")
    return selected[:1]


def unavailable(row):
    require(row["Classification"] == "native-error" and type(row.get("NativeCode")) is int and
            row["Error"] and (row["NativeCode"] == 1031 or row.get("Retryable") or row.get("MaybeCommitted")) and
            not row["Outcome"]["Commit"],
            "unavailable requires actual returned native SDK cause, never timeout-abort")


def native_marker(marker, state):
    original = state["Originals"][3]
    body = base64.b64decode(original["Bytes"], validate=True)
    proposal = json.loads(body)
    require(marker == {"Ready": "native-commit-issued-unobserved", "Graph": proposal["Graph"],
                       "Request": proposal["Request"], "Transaction": proposal["ID"],
                       "BodySHA256": list(hashlib.sha256(body).digest())}, "exact native invocation marker")
    # The marker is not success or an invented commit_unknown_result (1021).


def killed(before, stopped):
    require(before["Id"] == stopped["Id"] and before["Name"] == stopped["Name"], "kill identity")
    live(before)
    state = stopped["State"]
    require(not state["Running"] and state["ExitCode"] == 137 and not state["OOMKilled"],
            "genuine externally reaped SIGKILL, not OOM")
    return stopped


def commands(ledger, args):
    matches = [(i, row) for i, row in enumerate(ledger) if row["command"] == ["docker", *args]]
    require(len(matches) == 1, "exact unique recorded action")
    i, row = matches[0]
    require(row["exit"] == 0 and not row["deadline_expired"], "action must actually succeed")
    return i


def client_command(ledger, name):
    matches = [(i, row) for i, row in enumerate(ledger) if row["command"][:2] == ["docker", "run"]
               and "--name" in row["command"] and row["command"][row["command"].index("--name") + 1] == name]
    require(len(matches) == 1 and matches[0][1]["exit"] == 0 and not matches[0][1]["deadline_expired"],
            "actual successful client invocation")
    return matches[0][0]


def serial_binding(trace):
    """Feed observed complete entities/lives/postings to the existing oracle.

    This is an observed-prefix serial/cut consistency check, not a second graph
    reducer or a proof of production execution without the raw SDK/platform run.
    """
    path = Path(__file__).resolve().parents[3] / "docs/v5/reference/independent/protocol/oracle.py"
    require(hashlib.sha256(path.read_bytes()).hexdigest() ==
            "a132b4e4981f3dcea485b7ee5dea0df48a906cdfcda4a75bec5ff9f1a5589d3a", "pinned independent oracle")
    spec = importlib.util.spec_from_file_location("platform_existing_serial_oracle", path)
    oracle = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(oracle)

    def project(view):
        owner = lambda name: "B" if name == "node-b" else "A"
        pairs = [(owner(e["ID"]) + "/entity/" + e["ID"], e) for e in view["Entities"]]
        pairs += [(owner(l["Owner"]) + "/life/" + l["Life"], l) for l in view["Lives"]]
        pairs += [(owner(p["Endpoint"]) + "/posting/" + p["Endpoint"] + "/" + p["Life"], p) for p in view["Postings"]]
        require(len(dict(pairs)) == len(pairs), "duplicate observed semantic identity")
        return dict(pairs)

    transactions, rounds, observations = {}, {}, []
    previous = initial = project(trace["steps"][0]["Before"])
    for step, operation in zip(trace["steps"], trace["operationMapping"], strict=True):
        require(project(step["Before"]) == previous and step["Operation"] == operation["Logical"] and
                step["Cut"]["Scope"] == [0, 1] and step["Cut"]["Round"] == operation["Round"], "observed-prefix/cut/original continuity")
        after = project(step["After"])
        changed = {key for key in previous.keys() | after.keys() if previous.get(key) != after.get(key)}
        require(step["Operation"] not in transactions, "duplicate original identity")
        transactions[step["Operation"]] = {"reads": [{"kind": "range", "lo": "A/", "hi": "B0", "values": previous}],
                                            "writes": {key: {"op": "put", "value": after[key]} if key in after else {"op": "delete"} for key in changed}}
        rounds[step["Operation"]] = operation["Round"]
        observations.append({"event": step["Operation"], "scope": ["A", "B"], "round": operation["Round"], "values": after})
        previous = after
    for old in trace["oldReads"]:
        observations.append({"event": "old-" + old["Operation"], "scope": ["A", "B"], "round": old["Cut"]["Round"], "values": project(old["View"])})
    witness, detail = oracle.serial_witness({"initial": initial, "transactions": transactions}, set(transactions), rounds,
                                           observations, [{"event": "final", "scope": ["A", "B"], "values": previous}])
    require(witness is not None, "independent observed serial/cut witness: " + str(detail))
    return {"Witness": witness, "Search": detail, "OracleSHA256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "Scope": "observed-prefix serial/cut consistency; raw native SDK and real fault authority are separately bound"}


def finalize(directory, ledger_path):
    directory = Path(directory)
    plan, seed, terminal = (load(directory / name) for name in ("plan.json", "seed.json", "terminal.json"))
    require(plan["Inventory"] == inventory(load(directory / "healthy-before.json"), load(directory / "owned.before.json"),
                                           plan["Network"], directory), "raw status/owned process inventory binding")
    require(plan["Targets"] == targets(plan["Inventory"], plan["Case"]), "exact role/domain selection")
    require(plan["Format"] == terminal["Format"] == FORMAT and plan["Case"] == terminal["Case"] and
            terminal["Coordinator"] == seed["Coordinator"] == plan["Coordinator"], "fresh schema/case/orientation")
    require(terminal["SetupOperations"] == 3 and terminal["UsefulOperations"] == 1 and
            terminal["CompleteGroups"] == 4 and terminal["PhysicalHistoryRows"] == 8, "exact finite census")
    record = terminal["Record"]
    require(seed["Config"] == record["Config"] and seed["Initial"] == record["Initial"] and
            seed["Originals"] == record["Originals"] and seed["Steps"] == record["Steps"][:3],
            "original capture or held-old proof changed")
    require(len(record["Steps"]) == len(terminal["Replays"]) == 4 and
            len(record["Attempts"]) <= 8, "complete original/replay census and admission")
    require(terminal["Trace"]["steps"] == record["Steps"] and terminal["Trace"]["initialCut"] == record["Initial"] and
            len(terminal["Trace"]["steps"]) == 4 and len(terminal["Trace"]["oldReads"]) == 4 and
            len(terminal["Trace"]["finalRawCurrent"]) == 5, "complete graph/index/history/current observations")
    body = base64.b64decode(seed["Originals"][3]["Bytes"], validate=True)
    proposal = json.loads(body)
    for attempt in record["Attempts"]:
        require(attempt["SubmitInvocations"] == 1 and 1 <= attempt["SDKCallbacks"] <= 16, "finite actual native invocation/callback admission")
        require(attempt["BodySHA256"] == list(hashlib.sha256(body).digest()) and
                attempt["Request"] == proposal["Request"] and attempt["Transaction"] == proposal["ID"],
                "every submission retains exact same immutable request/body")
    useful = record["Steps"][3]["RawObservation"]["Groups"][3]["Outcome"]
    require(useful["Commit"], "useful original COMMIT")
    for i, replay in enumerate(terminal["Replays"]):
        original = base64.b64decode(seed["Originals"][i]["Bytes"], validate=True)
        tx = json.loads(original)
        expected = record["Steps"][i]["RawObservation"]["Groups"][i]["Outcome"]
        require(replay["Classification"] == "same-original-replay" and replay["Outcome"] == expected and
                replay["Request"] == tx["Request"] and replay["Transaction"] == tx["ID"] and
                replay["BodySHA256"] == list(hashlib.sha256(original).digest()) and
                expected["Digest"] == replay["BodySHA256"] and expected["Coordinator"] == tx["Coordinator"], "exact original replay binding")
        raw = record["Steps"][i]["RawRecords"]
        require(len(raw) == 3 and replay["Original"] == {key: raw[0][key] for key in ("Key", "Value")} and
                replay["Originals"] == [{key: row[key] for key in ("Key", "Value")} for row in (raw[2], raw[1])],
                "exact original ID/request/CDC raw correspondence")
        require(all(base64.b64decode(item["Value"], validate=True) for item in replay["Originals"]) and
                base64.b64decode(replay["Original"]["Value"], validate=True), "actual paired originals and complete CDC required")
    ledger_data = admitted_bytes(ledger_path, 16 << 20)
    full_ledger = records(ledger_path)
    require(type(plan["LedgerStart"]) is int and 0 <= plan["LedgerStart"] < len(full_ledger), "actual per-case command prefix")
    ledger = full_ledger[plan["LedgerStart"]:]
    faults, heals = [], []
    case = plan["Case"]
    for row in plan["Targets"]:
        name = row["Name"]
        before, after = load(directory / (name + ".before.json"))[0], load(directory / (name + ".after.json"))[0]
        require(live(before) == tuple(row["Instance"]) and before == row["Inspect"], "actual target changed before action")
        if case == "network":
            detached = load(directory / (name + ".detached.json"))[0]
            require(live(detached) == live(before) == live(after) and
                    plan["Network"] not in detached["NetworkSettings"]["Networks"] and
                    after["NetworkSettings"]["Networks"][plan["Network"]]["IPAddress"] + ":4500" == row["Address"],
                    "actual detach/heal unchanged process/IP")
            faults.append(commands(ledger, ["network", "disconnect", plan["Network"], name]))
            heals.append(commands(ledger, ["network", "connect", "--ip", row["Address"].split(":")[0], plan["Network"], name]))
        else:
            stopped = load(directory / (name + ".stopped.json"))[0]
            killed(before, stopped)
            require(live(before) != live(after) and before["Id"] == after["Id"] and
                    volume(before) == volume(after), "actual same-volume changed incarnation")
            faults.append(commands(ledger, ["kill", "--signal", "KILL", name]))
            heals.append(commands(ledger, ["start", name]))
    if case == "commit-issued":
        marker = load(directory / "commit-marker.json")
        native_marker(marker, seed)
        before, stopped = load(directory / "caller.before.json")[0], load(directory / "caller.stopped.json")[0]
        killed(before, stopped)
        require(before["Config"]["Cmd"] == ["fdb-spike", "platform-fault-commit-child"], "actual native caller executable")
        issued_index = client_command(ledger, before["Name"].lstrip("/"))
        faults.append(commands(ledger, ["kill", "--signal", "KILL", before["Name"].lstrip("/")]))
        marker_reads = [i for i, command in enumerate(ledger) if command["command"] == ["docker", "logs", before["Name"].lstrip("/")] and command["exit"] == 0 and not command["deadline_expired"]]
        require(any(issued_index < i < faults[0] for i in marker_reads), "marker not actually observed after native caller starts and before kill")
    else:
        require(max(faults) < min(heals), "all target failures precede any heal")
        submissions = records(directory / "submissions.jsonl")
        require(record["Attempts"][:len(submissions)] == [s["Response"] for s in submissions], "no dropped/reordered native attempt observation")
        if case in ("two-zones", "network"):
            require(len(submissions) == 1, "one finite unavailable diagnostic")
            unavailable(submissions[0]["Response"])
        else:
            require(1 <= len(submissions) <= 8 and submissions[-1]["Response"]["Classification"] == "committed" and
                    submissions[-1]["Response"]["Outcome"] == useful,
                    "one-zone/role-loss useful progress must precede heal")
        for submission in submissions:
            index = client_command(ledger, submission["Client"])
            require(max(faults) < index < min(heals), "diagnostic/progress not actually inside outage")
        if case in ("two-zones", "network"):
            require(len(record["Steps"][3]["RawObservation"]["Groups"]) == 4, "healed exact original missing")
    launcher.healthy(load(directory / "healthy-after.json"))
    terminal_index = client_command(ledger, load(directory / "clients.json")["Terminal"])
    require(terminal_index > max(heals or faults), "terminal original replay must follow heal/reap")
    # This immutable prefix is not claimed to include later cleanup/status actions.
    with (directory / "commands.prefix.jsonl").open("xb") as output:
        output.write(ledger_data)
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(directory.iterdir())
              if p.is_file() and p.name != "proof.json"}
    proof = {"Format": FORMAT, "Case": case, "Coordinator": plan["Coordinator"],
             "Scope": "one physical host; genuine co-resident-role process/domain and symmetric network faults; no matrix/power-loss claim",
             "RoleScope": "requested role present at selection; all co-resident listed roles lost together",
             "MarkerAuthority": "native Commit issued/unobserved only; original SDK lookup decides",
             "Plan": plan, "ArtifactsSHA256": hashes, "Commands": {"Artifact": "commands.prefix.jsonl", "IsPrefixOf": str(ledger_path), "Bytes": len(ledger_data), "CaseFromCommandOrdinal": plan["LedgerStart"] + 1},
             "SetupOperations": 3, "UsefulOperations": 1, "CompleteGroups": 4,
             "SerialBinding": serial_binding(terminal["Trace"])}
    with (directory / "proof.json").open("x") as output:
        json.dump(proof, output, indent=2); output.write("\n")


def main():
    mode, *args = sys.argv[1:]
    if mode == "select":
        status, inspected, network, directory, case, coordinator, destination, ledger = args
        require(coordinator in ("A", "B"), "explicit orientation")
        rows = inventory(load(status), load(inspected), network, directory)
        plan = {"Format": FORMAT, "Case": case, "Coordinator": "AB".index(coordinator),
                "Network": network, "Inventory": rows, "Targets": targets(rows, case), "LedgerStart": len(records(ledger))}
        with Path(destination).open("x") as output:
            json.dump(plan, output, indent=2); output.write("\n")
        for row in plan["Targets"]:
            print(row["Name"], row["Address"].split(":")[0])
    elif mode == "marker":
        native_marker(load(args[0]), load(args[1]))
    elif mode == "submission":
        source, client, destination = args
        response = load(source)
        with Path(destination).open("a") as output:
            output.write(json.dumps({"Client": client, "Response": response}) + "\n")
        print(response["Classification"])
    elif mode == "retry":
        row = load(args[0])
        require(row["Classification"] == "native-error" and
                (row["Retryable"] or row["MaybeCommitted"]),
                "definite error/ABORT cannot authorize original outer retry")
    elif mode == "finalize":
        finalize(*args)
    else:
        raise ValueError("unsupported explicit platform action")


if __name__ == "__main__":
    main()
