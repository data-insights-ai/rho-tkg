#!/usr/bin/env python3
"""Finite POSIX process-group monitor for the repository's trusted native drivers.

This is sampled resource accounting, not an untrusted-process sandbox or an
exact OS high-water measurement. Native children must inherit the owned session.
No host-wide process or network settings are changed.
"""
import argparse
import json
import math
import os
from pathlib import Path
import selectors
import signal
import stat
import subprocess
import sys
import time

MAX_SECONDS = 31_536_000
MAX_PS_BYTES = 1 << 20
MAX_PS_ROWS = 8192
MAX_IDENTITIES = 128
MAX_OUTPUT = 64 << 20
MAX_FILES = 8192
MAX_DIRS = 1024
MAX_DISK = 1 << 50
SAMPLE_SECONDS = 0.1


def require(value, message):
    if not value:
        raise ValueError("owned process watchdog refused: " + message)


def admission(seconds, rss_mib, deadline, *, now=None):
    now = time.time_ns() if now is None else now
    require(type(seconds) is int and 1 <= seconds <= MAX_SECONDS
            and type(rss_mib) is int and 1 <= rss_mib <= 1 << 20,
            "finite explicit wall/RSS limits")
    if deadline is None:
        deadline = now + seconds * 1_000_000_000
    require(type(deadline) is int and now < deadline <= (1 << 63)-1
            and deadline-now <= seconds * 1_000_000_000,
            "same original absolute deadline within admitted wall limit")
    require(seconds * MAX_IDENTITIES * 1_000_000_000 <= (1 << 63)-1,
            "checked cumulative process-time ceiling")
    return deadline, (deadline-now)/1_000_000_000


def parse_processes(body):
    require(len(body) <= MAX_PS_BYTES, "bounded process metadata")
    rows = {}
    for line in body.decode("ascii", "strict").splitlines():
        fields = line.split()
        require(len(rows) < MAX_PS_ROWS and len(fields) == 10, "bounded exact ps metadata rows")
        pid, parent, group, rss = (int(value) for value in fields[:4])
        require(pid >= 0 and parent >= 0 and group >= 0 and rss >= 0 and pid not in rows,
                "unique process metadata")
        rows[pid] = dict(pid=pid, parent=parent, group=group, rssKiB=rss,
                         state=fields[4], birth=" ".join(fields[5:]))
    return rows


def process_snapshot():
    """Bounded stdout/stderr and lifetime for this one owned metadata reader."""
    child = subprocess.Popen(["ps", "-A", "-o", "pid=,ppid=,pgid=,rss=,stat=,lstart="],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             env={**os.environ, "LC_ALL": "C"})
    selector = selectors.DefaultSelector()
    bodies = {"out": bytearray(), "error": bytearray()}
    deadline = time.monotonic()+2
    try:
        for name, stream in (("out", child.stdout), ("error", child.stderr)):
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, name)
        while selector.get_map():
            require(time.monotonic() < deadline, "process metadata reader deadline")
            for key, _ in selector.select(min(0.1, max(0, deadline-time.monotonic()))):
                block = os.read(key.fileobj.fileno(), 8192)
                if not block:
                    selector.unregister(key.fileobj)
                else:
                    require(len(block) <= MAX_PS_BYTES-len(bodies[key.data]), "process metadata byte grant")
                    bodies[key.data].extend(block)
        require(child.wait(timeout=max(0.001, deadline-time.monotonic())) == 0 and not bodies["error"],
                "actual complete ps result")
        return parse_processes(bytes(bodies["out"]))
    finally:
        selector.close()
        if child.poll() is None:
            child.kill()
        child.wait()
        child.stdout.close()
        child.stderr.close()


def disk_snapshot(root, maximum):
    require(type(maximum) is int and 1 <= maximum <= MAX_DISK, "finite owned disk limit")
    stack, names, directories, logical, allocated = [root], 0, 1, 0, 0
    while stack:
        directory = stack.pop()
        # Refuse links at every observed level. Trusted native fixture processes
        # never replace directory names concurrently with outside symlinks.
        require(stat.S_ISDIR(os.lstat(directory).st_mode), "owned directory identity")
        with os.scandir(directory) as entries:
            for entry in entries:
                names += 1
                require(names <= MAX_FILES and not entry.is_symlink(), "finite owned file catalog, no symlinks")
                try:
                    info = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue  # Genuine concurrent native unlink; next sample observes the new name set.
                require(info.st_size >= 0, "nonnegative physical file size")
                if stat.S_ISDIR(info.st_mode):
                    directories += 1
                    require(directories <= MAX_DIRS, "finite owned directory catalog")
                    stack.append(Path(entry.path))
                else:
                    require(stat.S_ISREG(info.st_mode), "regular owned native/artifact file")
                # Count every observed name conservatively, including hard links
                # and directory metadata. No same-inode deduplication map.
                logical += info.st_size
                allocated += info.st_blocks * 512
                require(max(logical, allocated) <= maximum, "sampled owned disk limit")
    return dict(names=names, directories=directories, logicalBytes=logical, allocatedBytes=allocated)


def owned_rows(rows, group):
    return {pid: row for pid, row in rows.items() if row["group"] == group}


def live(rows):
    return any(not row["state"].startswith("Z") for row in rows.values())


def run(command, *, seconds, rss_mib, report, deadline=None, owned_dir=None, disk_bytes=None):
    require(os.name == "posix" and command and all(type(part) is str and part for part in command),
            "explicit POSIX native command")
    deadline, remaining = admission(seconds, rss_mib, deadline)
    stop_at = time.monotonic()+remaining
    report = Path(report).absolute()
    require(report.parent.resolve(strict=True) == report.parent and not report.exists() and not report.is_symlink(),
            "fresh report in owned existing parent")
    require((owned_dir is None) == (disk_bytes is None), "owned directory/disk policy pair")
    disk = None
    if owned_dir is not None:
        owned_dir = Path(owned_dir).absolute()
        require(owned_dir.resolve(strict=True) == owned_dir and not owned_dir.is_symlink(), "explicit owned native directory")
        disk = disk_snapshot(owned_dir, disk_bytes)  # Admission before command effects.
    started = time.monotonic_ns()
    receipt = dict(version=1, command=command, wall_limit_s=seconds, rss_limit_mib=rss_mib,
                   deadline_unix_nanos=deadline, reason="unavailable", exit_code=None,
                   peak_tree_rss_mib=0, peak_engine_rss_mib=0, peak_monitor_rss_mib=0,
                   all_owned_exited=False, observed_processes=[], samples=0,
                   output_bytes=0, received_output_bytes=0, disk=disk, forced_cleanup=False,
                   scope="sampled aggregate native session plus watchdog/metadata reader RSS; trusted inherited-session children; bounded output/catalog; no exact RSS/disk high-water or independent-host claim")
    interrupted = []
    old_handlers = {}
    child = None
    root_reaped = False
    selector = selectors.DefaultSelector()
    identities = {}
    reason = "completed"
    output_limit = False
    def signal_stop(number, _frame):
        if not interrupted:
            interrupted.append(number)
    def drain(timeout):
        nonlocal output_limit
        for key, _ in selector.select(timeout):
            block = os.read(key.fileobj.fileno(), 8192)
            if not block:
                selector.unregister(key.fileobj)
                continue
            receipt["received_output_bytes"] += len(block)
            if len(block) > MAX_OUTPUT-receipt["output_bytes"]:
                output_limit = True
                continue
            receipt["output_bytes"] += len(block)
            if not output_limit:
                view = memoryview(block)
                while view:
                    written = os.write(key.data, view)
                    require(written > 0, "forwarded owned output")
                    view = view[written:]
    try:
        for number in (signal.SIGINT, signal.SIGTERM):
            old_handlers[number] = signal.signal(number, signal_stop)
        require(time.monotonic() < stop_at, "whole-run deadline before process creation")
        child = subprocess.Popen(command, start_new_session=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for stream, target in ((child.stdout, sys.stdout.fileno()), (child.stderr, sys.stderr.fileno())):
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, target)
        while True:
            drain(SAMPLE_SECONDS)
            rows = process_snapshot()
            owned = owned_rows(rows, child.pid)
            require(child.pid in owned, "unreaped root anchors owned process-group identity")
            for pid, row in owned.items():
                identity = (pid, row["birth"])
                require(identity in identities or len(identities) < MAX_IDENTITIES, "finite owned process identities")
                identities.setdefault(identity, {key: row[key] for key in ("pid", "parent", "group", "birth")})
            # Detached descendants are unsupported rather than silently billed
            # as belonging to another run. Genuine repository drivers inherit.
            require(all(row["group"] == child.pid or row["parent"] not in set(owned)
                        for row in rows.values()), "native child escaped inherited session")
            engine = sum(row["rssKiB"] for row in owned.values())/1024
            monitor = sum(row["rssKiB"] for row in rows.values()
                          if row["pid"] == os.getpid() or row["parent"] == os.getpid() and row["pid"] != child.pid)/1024
            receipt["samples"] += 1
            require(receipt["samples"] <= seconds*20+1, "finite whole-run sampling work")
            receipt["peak_engine_rss_mib"] = max(receipt["peak_engine_rss_mib"], engine)
            receipt["peak_monitor_rss_mib"] = max(receipt["peak_monitor_rss_mib"], monitor)
            receipt["peak_tree_rss_mib"] = max(receipt["peak_tree_rss_mib"], engine+monitor)
            if owned_dir is not None:
                receipt["disk"] = disk_snapshot(owned_dir, disk_bytes)
            if engine+monitor > rss_mib:
                reason = "rss-limit"; break
            if output_limit:
                reason = "output-limit"; break
            if interrupted:
                reason = "interrupted"; break
            if time.monotonic() >= stop_at:
                reason = "deadline"; break
            if owned[child.pid]["state"].startswith("Z"):
                if live({pid: row for pid, row in owned.items() if pid != child.pid}):
                    reason = "descendants-after-parent"
                break
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        reason = "monitor-refused"
        receipt["diagnostic"] = str(error)[:4096]
    finally:
        if child is not None:
            # Root has NOT been polled/reaped: its PID still anchors our PGID,
            # so group cleanup cannot target a later unrelated reused group.
            try:
                for number, grace in ((signal.SIGTERM, 1.0), (signal.SIGKILL, 2.0)):
                    rows = owned_rows(process_snapshot(), child.pid)
                    if not live(rows):
                        break
                    receipt["forced_cleanup"] = True
                    if reason == "completed":
                        reason = "descendants-after-parent"
                    os.killpg(child.pid, number)
                    until = time.monotonic()+grace
                    while time.monotonic() < until:
                        drain(0.05)
                        if not live(owned_rows(process_snapshot(), child.pid)):
                            break
                receipt["all_owned_exited"] = not live(owned_rows(process_snapshot(), child.pid))
                receipt["exit_code"] = child.wait(timeout=2)
                root_reaped = True
                until = time.monotonic()+2
                while selector.get_map() and time.monotonic() < until:
                    drain(0.05)
                if selector.get_map() or not receipt["all_owned_exited"]:
                    reason = "cleanup-refused"
                elif reason == "completed" and receipt["exit_code"] != 0:
                    reason = "command-failed"
            except (ValueError, OSError, subprocess.SubprocessError) as error:
                reason = "cleanup-refused"
                receipt["diagnostic"] = str(error)[:4096]
                # Even if the metadata reader fails, this still-owned unreaped
                # group must be killed and its direct child reaped.
                if not root_reaped:
                    try:
                        os.killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    try:
                        receipt["exit_code"] = child.wait(timeout=2)
                        root_reaped = True
                    except subprocess.TimeoutExpired:
                        pass
            child.stdout.close()
            child.stderr.close()
        selector.close()
        for number, previous in old_handlers.items():
            signal.signal(number, previous)
        receipt["reason"] = reason
        receipt["observed_processes"] = list(identities.values())
        receipt["elapsed_nanos"] = time.monotonic_ns()-started
        wire = (json.dumps(receipt, separators=(",", ":"))+"\n").encode()
        require(len(wire) <= 65536 and math.isfinite(receipt["peak_tree_rss_mib"]), "finite terminal report")
        with report.open("xb") as stream:
            stream.write(wire)
    return 0 if reason == "completed" and receipt["exit_code"] == 0 and receipt["all_owned_exited"] else 124


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rss-mib", required=True, type=int)
    parser.add_argument("--seconds", required=True, type=int)
    parser.add_argument("--deadline-unix-nanos", type=int)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--owned-dir", type=Path)
    parser.add_argument("--disk-bytes", type=int)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    return run(command, seconds=args.seconds, rss_mib=args.rss_mib, report=args.report,
               deadline=args.deadline_unix_nanos, owned_dir=args.owned_dir, disk_bytes=args.disk_bytes)


if __name__ == "__main__":
    raise SystemExit(main())
