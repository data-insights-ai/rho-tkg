#!/usr/bin/env python3
"""Sample actual RSS of every explicitly owned FDB server/client process."""
import argparse
import concurrent.futures
import json
from pathlib import Path
import subprocess
import time


def process_rows(text):
    rows = []
    for line in text.splitlines()[1:]:
        fields = line.split(None, 3)
        if len(fields) < 3:
            raise ValueError("incomplete docker top row")
        pid, parent, rss = (int(value) for value in fields[:3])
        if pid <= 0 or parent < 0 or rss < 0:
            raise ValueError("invalid actual process identity/RSS")
        rows.append({"pid": pid, "parent": parent, "rssKiB": rss, "command": fields[3] if len(fields) == 4 else ""})
    if len({row["pid"] for row in rows}) != len(rows):
        raise ValueError("duplicate process PID within container")
    return rows


def sample(container, required):
    start = time.monotonic_ns()
    top = subprocess.run(["docker", "top", container, "-eo", "pid,ppid,rss,args"], capture_output=True, text=True, timeout=5, check=False)
    if top.returncode == 0:
        rows = process_rows(top.stdout)
        if rows:
            return {"container": container, "startNanos": start, "endNanos": time.monotonic_ns(), "processes": rows, "state": "live"}
        # A successful top can race process exit and return just its header.
        # Only the actual terminal inspection below can classify that result.
    inspect = subprocess.run(["docker", "inspect", container], capture_output=True, text=True, timeout=5, check=True)
    records = json.loads(inspect.stdout)
    if len(records) != 1:
        raise ValueError("container identity changed")
    state = records[0]["State"]
    if required or state["Running"] or state["OOMKilled"] or state["ExitCode"] != 0:
        raise ValueError("missing/refused actual live server/client RSS: " + container)
    return {"container": container, "startNanos": start, "endNanos": time.monotonic_ns(), "processes": [], "state": "stopped", "inspection": state}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--servers", nargs=4, required=True)
    parser.add_argument("--client", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=180)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 1800 or len(set(args.servers + [args.client])) != 5:
        parser.error("four servers plus one distinct client; finite <=1800second watchdog required")
    started, peak = time.monotonic(), 0
    with args.output.open("x") as output, concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        while time.monotonic() - started < args.seconds:
            futures = [pool.submit(sample, name, True) for name in args.servers] + [pool.submit(sample, args.client, False)]
            observations = [future.result() for future in futures]
            pids = [p["pid"] for c in observations for p in c["processes"]]
            if len(set(pids)) != len(pids):
                raise ValueError("same actual host PID counted across containers")
            rss = sum(p["rssKiB"] for c in observations for p in c["processes"])
            peak = max(peak, rss)
            output.write(json.dumps({"elapsedSeconds": time.monotonic() - started, "rssKiB": rss,
                                     "sampledPeakRSSKiB": peak, "samplingSkewNanos": max(c["endNanos"] for c in observations) - min(c["startNanos"] for c in observations),
                                     "containers": observations}) + "\n")
            output.flush()
            if observations[-1]["state"] == "stopped":
                return
            time.sleep(0.5)
    raise ValueError("cost monitor reached finite deadline before actual client completion")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        raise SystemExit(f"cost RSS refused: {error}") from error
