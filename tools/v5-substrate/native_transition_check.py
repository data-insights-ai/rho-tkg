#!/usr/bin/env python3
"""Bounded actual storage-boundary capture bridge, not phase or power-loss proof.

The native parent obtains real IO receipts and reaped OS children. This private
checker preserves those bytes through the existing complete-prefix and separate
serial oracle. It never manufactures a legacy fault receipt or graph source.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath

SPEC = importlib.util.spec_from_file_location("native_transition_history", Path(__file__).with_name("native_fault_schedule_check.py"))
C = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(C)
OPERATIONS = {"wal-write": "write", "wal-sync": "sync", "wal-sync-data": "sync-data",
              "wal-sync-to": "sync-to", "directory-sync": "sync",
              "options-rename": "rename", "manifest-create": "create", "manifest-close": "close"}
WAL_CASES = {"storage-" + operation + "-" + side
             for operation in ("wal-write", "wal-sync-data") for side in ("before", "after")}
HEADER = set("Version Profile Case Original Manifest PendingCaller InstalledA WithheldB PartialReads Before Retained Recovered Reopened Processes From Through Records PacketEvents IO Refused RefusedReplay".split())


def require(value, message):
    if not value:
        raise ValueError("native transition refused: " + message)


def integer(value, maximum=(1 << 64) - 1, minimum=0):
    require(type(value) is int and minimum <= value <= maximum, "finite integer")
    return value


def fixed(value, size, nonzero=False):
    require(type(value) is list and len(value) == size and all(type(n) is int and 0 <= n <= 255 for n in value), "fixed bytes")
    require(not nonzero or any(value), "zero identity")
    return bytes(value)


def pairs(items):
    require(len({key for key, _ in items}) == len(items), "duplicate JSON field")
    return dict(items)


def decoded(wire):
    return json.loads(wire, object_pairs_hook=pairs,
                      parse_constant=lambda _: require(False, "nonfinite JSON"))


def load(path):
    digest, total, header, footer, records = hashlib.sha256(), 0, None, None, []
    with path.open("rb") as source:
        while line := source.readline((512 << 10) + 1):
            total += len(line)
            require(len(line) <= 512 << 10 and total <= 16 << 20 and line.endswith(b"\n"), "artifact byte admission")
            row = decoded(line)
            require(type(row) is dict and footer is None, "row after EOF")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and row["Type"] == "header", "header row")
                header = row["Artifact"]
                require(type(header) is dict and set(header) == HEADER and header["Records"] is None, "fresh header grammar")
            elif row.get("Type") == "record":
                require(set(row) == {"Type", "Record"} and len(records) < 512, "prefix admission")
                records.append(row["Record"])
            else:
                require(set(row) == {"Type", "Rows", "Through", "PayloadSHA256"} and row["Type"] == "footer", "EOF row")
                footer = row
                require(row["Rows"] == len(records) and row["PayloadSHA256"] == digest.hexdigest(), "complete prefix hash/census")
            if footer is None:
                digest.update(line)
    require(header is not None and footer is not None and footer["Through"] == header["Through"], "missing/foreign EOF")
    header["Records"] = records
    return header


def io_receipt(receipt, case):
    require(type(receipt) is dict and set(receipt) == set("Version Profile Cookie Member PID Sequence Arms Kind Nth Seen Armed Matched IO".split()), "receipt grammar")
    require(receipt["Version"] == 1 and receipt["Profile"] == C.PROFILE, "actual native fault profile")
    integer(receipt["Version"], 1, 1)
    fixed(receipt["Cookie"], 16, True)
    integer(receipt["PID"], minimum=1)
    for field in ("Member", "Sequence", "Arms", "Nth", "Seen"):
        integer(receipt[field], 1, 1)
    require(receipt["Member"] == 1 and receipt["Sequence"] == receipt["Arms"] == receipt["Nth"] == receipt["Seen"] == 1 and receipt["Armed"] is False and receipt["Matched"] is True, "unmatched/foreign arm")
    require(case == "storage-" + receipt["Kind"], "case/selector binding")
    operation, side = receipt["Kind"].rsplit("-", 1)
    require(operation in OPERATIONS and side in ("before", "after"), "finite actual selector")
    io = receipt["IO"]
    require(type(io) is dict and set(io) == set("Kind Path Directory Requested Written InputSHA256 WrittenSHA256 Error Transition".split()), "WAL operation grammar")
    path = PurePosixPath(io["Path"])
    require(not path.is_absolute() and str(path) == io["Path"] and len(str(path)) <= 1024 and ".." not in path.parts, "owned relative native path")
    require(operation.startswith("wal-") and len(path.parts) == 1 and path.suffix == ".log" and io["Directory"] is False and io["Kind"] == OPERATIONS[operation], "actual WAL method/path")
    requested, written = integer(io["Requested"], 2 << 20), integer(io["Written"], 2 << 20)
    require(written <= requested, "native byte result")
    before_hash, after_hash = fixed(io["InputSHA256"], 32), fixed(io["WrittenSHA256"], 32)
    point = io["Transition"]
    require(type(point) is dict and set(point) == {"Operation", "Side", "Returned", "Guaranteed"}, "transition result grammar/error")
    require(point["Operation"] == operation and point["Side"] == side and point["Returned"] is (side == "after"), "before/returned distinction")
    guaranteed = side == "after" and operation != "wal-write"
    require(point["Guaranteed"] is guaranteed, "missing actual guaranteed synchronization (queued SyncTo is unavailable)")
    require(io["Error"] == "declared native storage transition SIGKILL", "actual declared stop")
    empty = hashlib.sha256(b"").digest()
    if operation == "wal-write":
        require(requested > 0 and written == (requested if side == "after" else 0), "full native before/after write")
        require(after_hash == (before_hash if side == "after" else empty), "actual written byte hash")
    else:
        require(requested == written == 0 and before_hash == after_hash == empty, "synchronization invented payload")
    return point


def processes(rows, receipt, case):
    io_receipt(receipt, case)
    require(type(rows) is list and len(rows) == 2, "one failed child/one replacement")
    failed, restored = rows
    actual, reopened = decoded(C.P.raw(failed["Raw"])), decoded(C.P.raw(restored["Raw"]))
    require(failed["Action"] == "native-storage-transition-kill" and failed["Reaped"] is True and failed["Member"] == actual["Member"] == receipt["Member"] and actual["PID"] == receipt["PID"] and actual["Signaled"] is True and actual["SignalNumber"] == 9 and actual["ExitCode"] == -1 and actual["Reaped"] is True and actual["Receipt"] == receipt and actual["Volume"] == failed["Volume"], "actual matched child self-kill/reap")
    require(restored["Action"] == "reopen" and restored["Ready"] is True and restored["Member"] == reopened["Member"] == failed["Member"] and reopened["PID"] != actual["PID"] and restored["PreviousInstance"] == reopened["PreviousInstance"] == failed["Instance"] and restored["Volume"] == reopened["Volume"] == failed["Volume"] and restored["RecoveryRaw"] == reopened["RecoveryRaw"], "genuine same-directory replacement")


def check(data):
    require(type(data) is dict and set(data) == HEADER and data["Version"] == 1 and data["Profile"] == C.PROFILE and data["Case"] in WAL_CASES, "finite implemented storage case")
    require(data["IO"] is not None and not any(data[key] for key in ("PendingCaller", "PartialReads", "PacketEvents", "Refused", "RefusedReplay")), "foreign case evidence")
    # Authenticate the new real-process grammar before shared history/serial
    # checks. This is not a permissive callback for an unknown artifact format.
    processes(data["Processes"], data["IO"], data["Case"])
    result = C.check_history(data, process_check=processes)
    result["scope"] = "actual matched VFS boundary during same-original recovery; no phase-specific IO, power-loss, whole-D or runtime qualification claim"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+", type=Path)
    args = parser.parse_args()
    require(len(args.artifacts) <= 16, "finite artifact invocation")
    print(json.dumps([check(load(path)) for path in args.artifacts], indent=2))


if __name__ == "__main__":
    main()
