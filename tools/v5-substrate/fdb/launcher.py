#!/usr/bin/env python3
"""Bounded Docker CLI calls and fail-closed local functional receipts."""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def profile(name, memory, cpu):
    profiles = {
        "small": (512, 384, 64, 1024),
        "large": (2048, 1536, 128, 2048),
    }
    server, resident, cache, client = profiles[name]
    if memory < max(1024, 4 * server + client) or cpu < 2:
        raise ValueError("aggregate profile exceeds admission allowance")
    return {
        "name": name, "admission_mib": memory, "admission_cpus": cpu,
        "server_mib": server, "resident_mib": resident, "cache_mib": cache,
        "client_mib": client, "server_cpus": 0.375, "client_cpus": 0.5,
        "build_mib": 1024, "build_cpus": 2,
        "cluster_phase_mib": 4 * server + client, "cluster_phase_cpus": 2,
        "physical_hosts": 1, "independent_fault_domains": False,
        "scope": "local graph/scalar functional faults; no engine selection or V2 closure",
    }


def healthy(d):
    c = d["cluster"]
    assert d["client"]["database_status"]["healthy"]
    assert c["configuration"]["redundancy_mode"] == "triple"
    assert c["full_replication"] and c["data"]["state"]["name"] == "healthy"
    assert c["data"]["state"]["min_replicas_remaining"] >= 3
    assert c["recovery_state"]["name"] == "fully_recovered"
    assert len(c["processes"]) == 4 and len(c["machines"]) == 4
    assert len(d["client"]["coordinators"]["coordinators"]) == 3
    assert {p["locality"]["zoneid"] for p in c["machines"].values()} == {"zone0", "zone1", "zone2", "zone3"}


def marker(d, phase):
    expected = {"before": "before-native-commit", "after": "after-native-commit-before-reply"}
    assert d == {"ready": expected[phase]}


def resource(config, records, role):
    assert len(records) == 1
    for record in records:
        assert record["HostConfig"]["Memory"] == config[role + "_mib"] * (1 << 20)
        assert record["HostConfig"]["NanoCpus"] == int(config[role + "_cpus"] * 1_000_000_000)
        assert not record["State"]["OOMKilled"]


def bounded(seconds, ledger, command):
    start = time.monotonic()
    process = subprocess.Popen(command, start_new_session=True)
    expired = False
    try:
        result = process.wait(timeout=seconds)
    except subprocess.TimeoutExpired:
        expired = True
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=3)
        result = 124
    with Path(ledger).open("a") as output:
        output.write(json.dumps({"command": command, "deadline_seconds": seconds,
                                 "elapsed_seconds": time.monotonic() - start,
                                 "exit": result, "deadline_expired": expired}) + "\n")
    return result


def main():
    mode, *args = sys.argv[1:]
    if mode == "profile":
        name, memory, cpu, destination = args
        config = profile(name, int(memory), float(cpu))
        Path(destination).write_text(json.dumps(config, indent=2) + "\n")
        print(config["server_mib"], config["resident_mib"], config["cache_mib"], config["client_mib"])
    elif mode == "bounded":
        seconds, ledger, *command = args
        return bounded(float(seconds), ledger, command)
    elif mode == "healthy":
        healthy(json.loads(Path(args[0]).read_text()))
    elif mode == "marker":
        marker(json.loads(Path(args[0]).read_text()), args[1])
    elif mode == "killed":
        records = json.loads(Path(args[0]).read_text())
        assert len(records) == 1
        state = records[0]["State"]
        assert state["ExitCode"] == 137 and not state["Running"] and not state["OOMKilled"]
    elif mode == "stopped":
        records = json.loads(Path(args[0]).read_text())
        assert len(records) == 1 and not records[0]["State"]["Running"]
    elif mode == "resources":
        config, records = (json.loads(Path(p).read_text()) for p in args[:2])
        resource(config, records, args[2])
    elif mode == "receipt":
        directory, result, cleanup, mode = args
        out = Path(directory)
        hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in sorted(out.iterdir()) if p.is_file() and p.name != "receipt.json"}
        scopes = {
            "static": "pinned compilation/static tools; no cluster or fault schedule",
            "lifecycle-trace": "one physical host, four logical FDB zones; lifecycle trace, no external kill schedule",
            "graph-fault": "one physical host, four logical FDB zones; functional graph faults only",
            "test": "one physical host, four logical FDB zones; native tests and scalar faults",
            "smoke": "one physical host, four logical FDB zones; native scalar smoke only",
        }
        receipt = {"exit": int(result), "cleanup_exit": int(cleanup),
                   "success": result == "0" and cleanup == "0", "sha256": hashes,
                   "mode": mode, "scope": scopes[mode]}
        (out / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    else:
        raise ValueError("unsupported launcher action")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (AssertionError, KeyError, ValueError, OSError) as error:
        print(f"launcher refused: {error}", file=sys.stderr)
        sys.exit(1)
