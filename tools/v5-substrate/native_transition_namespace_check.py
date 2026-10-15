#!/usr/bin/env python3
"""Actual Pebble namespace publication sides, separate from graph commit IO.

These cells reopen an existing acknowledged directory. The matched operation
must self-kill before Ready, then a third actual process serves retained old and
current cuts before catch-up. Marker.Move attribution requires both the native
caller and its successfully created/closed marker identity; generic directory
Sync and rho atomic-KV snapshot activation retain their separate scopes.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath
import re

SPEC = importlib.util.spec_from_file_location("native_namespace_components", Path(__file__).with_name("native_transition_check.py"))
T = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(T)
C, P = T.C, T.C.P
require, integer, fixed = T.require, T.integer, T.fixed
PROFILE = "native-namespace-publication-transition-v1"
CALLER = "github.com/cockroachdb/pebble/v2/vfs/atomicfs.(*Marker).Move"
OPERATIONS = {"options-rename": "rename", "manifest-create": "create", "manifest-close": "close", "manifest-directory-sync": "sync"}
CASES = {"namespace-"+operation+"-"+side for operation in OPERATIONS for side in ("before", "after")}
HEADER = T.HEADER | set("DeadlineUnixNanos CurrentBeforeKill ReopenedCurrent Decision OriginalRetry".split())


def load(path):
    digest, total, header, footer, records = hashlib.sha256(), 0, None, None, []
    with path.open("rb") as source:
        while line := source.readline((512 << 10)+1):
            total += len(line)
            require(len(line) <= 512 << 10 and total <= 16 << 20 and line.endswith(b"\n"), "bounded actual namespace frame")
            row = T.decoded(line)
            require(type(row) is dict and footer is None, "row after EOF")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and row["Type"] == "header", "header row")
                header = row["Artifact"]
                require(type(header) is dict and set(header) == HEADER and header["Records"] is None, "fresh namespace artifact")
            elif row.get("Type") == "record":
                require(set(row) == {"Type", "Record"} and len(records) < 512, "finite native prefix")
                records.append(row["Record"])
            else:
                require(set(row) == {"Type", "Rows", "Through", "PayloadSHA256"} and row["Type"] == "footer", "EOF grammar")
                footer = row
                require(integer(row["Rows"], 512) == len(records) and row["PayloadSHA256"] == digest.hexdigest(), "actual prefix hash/census")
            if footer is None: digest.update(line)
    require(header is not None and footer is not None and header["Through"] == footer["Through"], "missing/foreign EOF")
    header["Records"] = records
    return header


def marker(name):
    require(type(name) is str and len(name) <= 1024 and re.fullmatch(r"marker\.manifest\.[0-9]{6,20}\.MANIFEST-[0-9]{6,20}", name), "actual manifest marker path")
    _, _, iteration, manifest = name.split(".")
    require(0 < int(iteration) < 1 << 64 and 0 < int(manifest[9:]) < 1 << 64, "finite marker/native file numbers")


def io_receipt(receipt, case):
    require(type(receipt) is dict and set(receipt) == set("Version Profile Cookie Member PID Sequence Arms Kind Nth Seen Armed Matched IO".split()), "actual IO receipt grammar")
    require(receipt["Profile"] == C.PROFILE and integer(receipt["Version"], 1, 1) and receipt["Armed"] is False and receipt["Matched"] is True, "matched native filesystem owner")
    fixed(receipt["Cookie"], 16, True)
    for field in ("Member", "Sequence", "Arms", "Nth", "Seen"): integer(receipt[field], 1, 1)
    integer(receipt["PID"], minimum=1)
    require(case in CASES and case == "namespace-"+receipt["Kind"], "finite exact native selector")
    operation, side = receipt["Kind"].rsplit("-", 1)
    io = receipt["IO"]
    expected = set("Kind Path Directory Requested Written InputSHA256 WrittenSHA256 Error Transition".split())
    if operation == "options-rename": expected.add("Destination")
    if operation == "manifest-directory-sync": expected |= {"Caller", "Publication", "PublicationClosed"}
    require(type(io) is dict and set(io) == expected and io["Kind"] == OPERATIONS[operation], "exact actual namespace method/lineage fields")
    path = PurePosixPath(io["Path"])
    require(type(io["Path"]) is str and 0 < len(io["Path"]) <= 1024 and str(path) == io["Path"] and not path.is_absolute() and len(path.parts) <= 1 and ".." not in path.parts, "owned relative native filename")
    if operation == "options-rename":
        require(io["Directory"] is False and io["Path"] != "." and type(io["Destination"]) is str and re.fullmatch(r"OPTIONS-[0-9]{6,20}", io["Destination"]), "real OPTIONS temp rename, not a manifest rename")
    elif operation == "manifest-directory-sync":
        require(io["Directory"] is True and io["Path"] == "." and io["Caller"] == CALLER and io["PublicationClosed"] is True, "actual Marker.Move native Sync caller plus successful marker Close")
        marker(io["Publication"])
    else:
        require(io["Directory"] is False, "marker file is not directory")
        marker(io["Path"])
    point = io["Transition"]
    require(type(point) is dict and set(point) == {"Operation", "Side", "Returned", "Guaranteed"} and point["Operation"] == operation and point["Side"] == side
            and point["Returned"] is (side == "after") and point["Guaranteed"] is (operation == "manifest-directory-sync" and side == "after"),
            "actual before/returned method and guaranteed synchronization, no invented durability")
    empty = hashlib.sha256(b"").digest()
    require(integer(io["Requested"], 0) == integer(io["Written"], 0) == 0 and fixed(io["InputSHA256"], 32) == fixed(io["WrittenSHA256"], 32) == empty
            and io["Error"] == "declared native storage transition SIGKILL", "actual method stop, not synthetic payload/error")


def processes(rows, receipt, case):
    io_receipt(receipt, case)
    require(type(rows) is list and len(rows) == 3, "original kill, actual pre-Ready failure, final replacement")
    killed, failed, restored = rows
    for index, row in enumerate(rows):
        require(row["Member"] == 1 and integer(row["EventID"], minimum=1) > (rows[index-1]["EventID"] if index else 0), "actual finite parent event order/member")
    before, actual, reopened = (T.decoded(P.raw(row["Raw"])) for row in rows)
    require(killed["Action"] == "kill" and killed["Signal"] == "SIGKILL" and killed["Reaped"] is True and before["Signaled"] is True and before["SignalNumber"] == 9 and before["Reaped"] is True, "genuine original process killed/reaped")
    require(failed["Action"] == "native-storage-transition-kill" and failed["Reaped"] is True and failed.get("Ready", False) is False and actual["Signaled"] is True
            and actual["SignalNumber"] == 9 and actual["ExitCode"] == -1 and actual["Reaped"] is True and actual["Receipt"] == receipt
            and actual["Member"] == before["Member"] == reopened["Member"] == 1 and actual["PID"] == receipt["PID"], "actual matched self-kill before Ready, no parent fallback")
    require(restored["Action"] == "reopen" and restored["Ready"] is True and restored["PreviousInstance"] == reopened["PreviousInstance"] == killed["Instance"]
            and restored["RecoveryRaw"] == reopened["RecoveryRaw"] and len({before["PID"], actual["PID"], reopened["PID"]}) == 3,
            "actual third process replacement, not re-labelled failed Open")
    require(killed["Volume"] == failed["Volume"] == restored["Volume"] == before["Volume"] == actual["Volume"] == reopened["Volume"], "same actual durable directory")
    startup, recovery = C.process_response(P.raw(reopened["LocallyRestoredRaw"])), C.process_response(P.raw(reopened["RecoveryRaw"]))
    require(startup["code"] == recovery["code"] == startup["mask"] == recovery["mask"] == 0 and startup["incarnation"] == recovery["incarnation"]
            and restored["Instance"] == reopened["Instance"] == f'voter1/pid{reopened["PID"]}/{startup["incarnation"].hex()}'
            and startup["applied"] <= recovery["applied"], "native Restore before real catch-up")
    return startup


def check(data):
    require(type(data) is dict and set(data) == HEADER and data["Version"] == 1 and data["Profile"] == PROFILE and data["Case"] in CASES, "fresh finite namespace source")
    integer(data["Version"], 1, 1); integer(data["DeadlineUnixNanos"], (1 << 63)-1, 1)
    require(data["IO"] is not None and not any(data[k] for k in ("PendingCaller", "PartialReads", "PacketEvents", "Refused", "RefusedReplay")), "namespace case cannot borrow unrelated fault evidence")
    startup = processes(data["Processes"], data["IO"], data["Case"])
    scopes, _ = P.cut(data["Before"])
    original = C.decision_manifest(P.raw(data["Original"]), P.raw(data["Manifest"]), 8, scopes, data["Before"])
    decision = P.status(P.raw(data["Decision"]))
    require(decision["tx"] == original["tx"] and decision["digest"] == original["digest"] and decision["partition"] == 8 and decision["phase"] == 3 and decision["commit"], "actual immutable original decision")
    P.resumed(data["OriginalRetry"], scopes, original["source"]["request"], original["tx"], P.business_hash(P.raw(data["Original"]), 8), 0, P.raw(data["Decision"]))
    result = C.check_history(data, process_check=processes, semantic_require=T.semantic)
    require(any(row["Partition"] == 8 and row["Index"] == decision["index"] and row["Outcome"] == data["Decision"] for row in data["Records"]), "original decision missing complete retained prefix")
    T.literal_edge_answers(data)
    T.semantic(data["CurrentBeforeKill"] == data["Recovered"] == data["ReopenedCurrent"], "same-current-cut-before-heal")
    P.cut(data["CurrentBeforeKill"])
    for encoded in data["CurrentBeforeKill"]["Cut"]["Certificates"]:
        cert = P.body(P.raw(encoded), b"GSC\1")
        if int.from_bytes(cert[16:24], "big") == 3:
            require(int.from_bytes(cert[160:168], "big") <= startup["applied"], "local new certificate absent before catch-up")
    before = data["Before"]["Native"]
    require([e["id"] for e in before["entityCensus"]] == list(range(1, 33)) and [e["id"] for e in before["entityCensus"] if e["found"]] == [1, 17]
            and all(not p["candidates"] and not p["declared"] and not p["effective"] for p in before["postings"]), "literal old nodes, no phantom/current edge")
    result["scope"] = "eight actual namespace method sides during same-directory native Open; precise marker Sync requires native caller plus created/closed-file lineage; distinct from graph phase IO, rho snapshot atomic-KV activation, power loss and runtime qualification"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    args = parser.parse_args()
    print(json.dumps(check(load(args.artifact)), indent=2))


if __name__ == "__main__":
    main()
