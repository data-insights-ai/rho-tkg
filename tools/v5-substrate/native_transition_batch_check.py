#!/usr/bin/env python3
"""Finite exact-entry Ready/install sync receipts and genuine graph recovery.

The native observer binds unique target membership to its complete ordered
Ready manifest and actual batch payload commitment. It is not an application
ACK, quorum proof, arbitrary VFS/namespace attribution or power-loss model.
"""
import argparse
import hashlib
import importlib.util
import json
import struct
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("native_exact_batch_common", Path(__file__).with_name("native_transition_check.py"))
T = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(T)
C, P = T.C, T.C.P
PROFILE = "native-exact-sync-batch-transition-v1"
HEADER = T.HEADER | set("Phase Partition Kind Side SelectedCommand Arm Batch Run DeadlineUnixNanos ReceiptReads ReceiptGrantBytes".split())
RECEIPT = set("Version Profile Cookie Run Member PID Sequence Control State DeadlineUnixNanos Attempts Work Entries Output Count Events".split())
CONTROL = set("Version Action Cookie Sequence Kind Side CommandSHA256".split())
EVENT = set("Selection Kind Stage Synced Matches EntryCount Position FirstIndex LastIndex Index Term EntryType CommandBytes CommandSHA256 EntryManifestSHA256 BatchBytes BatchPayloadSHA256 Work".split())
require, integer, fixed = T.require, T.integer, T.fixed


def load(path, *, program=False):
    digest, total, header, footer, records = hashlib.sha256(), 0, None, None, []
    with path.open("rb") as source:
        while line := source.readline((512 << 10) + 1):
            total += len(line)
            require(len(line) <= 512 << 10 and total <= 16 << 20 and line.endswith(b"\n"), "batch artifact admission")
            row = T.decoded(line)
            require(type(row) is dict and footer is None, "row after complete batch EOF")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and row["Type"] == "header", "batch header row")
                header = row["Artifact"]
                require(type(header) is dict and set(header) == HEADER | ({"Program"} if program else set()) and header["Records"] is None, "fresh batch grammar")
            elif row.get("Type") == "record":
                require(set(row) == {"Type", "Record"} and len(records) < 512, "batch prefix admission")
                records.append(row["Record"])
            else:
                require(set(row) == {"Type", "Rows", "Through", "PayloadSHA256"} and row["Type"] == "footer", "batch EOF row")
                footer = row
                require(type(row["Rows"]) is int and row["Rows"] == len(records) and row["PayloadSHA256"] == digest.hexdigest(), "complete batch prefix hash/census")
            if footer is None:
                digest.update(line)
    require(header is not None and footer is not None and footer["Through"] == header["Through"], "missing/foreign batch EOF")
    header["Records"] = records
    return header


def event(e, control):
    require(type(e) is dict and set(e) == EVENT, "actual native event grammar")
    s, work = e["Selection"], e["Work"]
    require(type(s) is dict and set(s) == {"Cookie", "Sequence", "CommandSHA256"} and s == {k: control[k] for k in s}, "exact live selection")
    require(e["Kind"] == control["Kind"] and type(e["Kind"]) is int and e["Kind"] in (1, 2), "Ready/install distinction")
    require(type(e["Synced"]) is bool and type(e["Matches"]) is int and e["Matches"] == 1, "unique exact membership")
    count = integer(e["EntryCount"], 4096, 1)
    position = integer(e["Position"], count, 1)
    first, last, index = (integer(e[k], minimum=1) for k in ("FirstIndex", "LastIndex", "Index"))
    require(last-first+1 == count and index == first+position-1, "ordered native Ready entry membership")
    integer(e["Term"], minimum=1)
    require(type(e["EntryType"]) is int and e["EntryType"] == 0, "selected native normal entry")
    integer(e["CommandBytes"], 64 << 10, 1)
    require(fixed(e["CommandSHA256"], 32, True) == fixed(control["CommandSHA256"], 32, True), "native command commitment")
    fixed(e["EntryManifestSHA256"], 32, True)
    integer(e["BatchBytes"], 1 << 40, 12)
    fixed(e["BatchPayloadSHA256"], 32, True)
    require(type(work) is dict and set(work) == {"Entries", "Bytes", "OutputBytes"}, "prepaid native traversal grammar")
    require(integer(work["Entries"], 4097, 2) == count+1 and integer(work["Bytes"], 1 << 40, 1) >= e["BatchBytes"] and integer(work["OutputBytes"], 1024, 1024), "prepaid complete manifest and batch")
    if e["Kind"] == 2:
        require(count == position == 1, "install must bind its actual one retained entry")
    if count == 1:
        manifest = b"rho-tkg:stable-storage-entry-manifest:v1\0" + struct.pack(">QQQQQ", 1, index, e["Term"], 0, e["CommandBytes"]) + fixed(e["CommandSHA256"], 32)
        require(hashlib.sha256(manifest).digest() == fixed(e["EntryManifestSHA256"], 32), "single actual native entry manifest hash")
    return e


def zero_event(e):
    require(type(e) is dict and set(e) == EVENT and type(e["Selection"]) is dict and set(e["Selection"]) == {"Cookie", "Sequence", "CommandSHA256"} and type(e["Work"]) is dict and set(e["Work"]) == {"Entries", "Bytes", "OutputBytes"}, "unused event grammar")
    require(fixed(e["Selection"]["Cookie"], 16) == bytes(16) and fixed(e["Selection"]["CommandSHA256"], 32) == bytes(32) and type(e["Selection"]["Sequence"]) is int and e["Selection"]["Sequence"] == 0 and e["Synced"] is False, "unused event identity")
    for k in ("CommandSHA256", "EntryManifestSHA256", "BatchPayloadSHA256"):
        require(fixed(e[k], 32) == bytes(32), "unused event digest")
    for k in EVENT - {"Selection", "Synced", "CommandSHA256", "EntryManifestSHA256", "BatchPayloadSHA256", "Work"}:
        require(type(e[k]) is int and e[k] == 0, "unused event value")
    require(all(type(v) is int and v == 0 for v in e["Work"].values()), "unused work")


def receipt_pair(arm, r, run, deadline, phase, partition, kind, side, command, *, member=None):
    selected_member=(1 if partition==3 else 4) if member is None else member
    require(type(selected_member) is int and selected_member in ((1,2) if partition==3 else (4,6)), "actual selected native batch voter")
    for value in (arm, r):
        require(type(value) is dict and set(value) == RECEIPT, "native owner receipt grammar")
        require(type(value["Version"]) is int and value["Version"] == 1 and value["Profile"] == PROFILE, "native owner profile")
        fixed(value["Cookie"], 16, True)
        require(fixed(value["Run"], 16, True) == run, "same original run owner")
        require(integer(value["Member"], 6, 1) == selected_member and integer(value["PID"], minimum=1) and integer(value["Sequence"], 1, 1), "actual native owner/member")
        require(integer(value["DeadlineUnixNanos"], (1 << 63)-1, 1) <= deadline, "fixed original deadline")
        c = value["Control"]
        require(type(c) is dict and set(c) == CONTROL and type(c["Version"]) is int and c["Version"] == 1 and c["Action"] == "arm" and c["Cookie"] == value["Cookie"] and type(c["Sequence"]) is int and c["Sequence"] == 1 and type(c["Kind"]) is int and c["Kind"] == kind and c["Side"] == side, "exact native selector/control")
        require(fixed(c["CommandSHA256"], 32, True) == hashlib.sha256(command).digest(), "actual source command selected by hash")
        integer(value["Attempts"], 16*128)
        integer(value["Work"], 1 << 40)
        integer(value["Entries"], 1 << 20)
        integer(value["Output"], 2 << 20, 2*8192)
        require(type(value["Events"]) is list and len(value["Events"]) == 2, "one fixed event pair")
    for field in ("Cookie", "Run", "Member", "PID", "Sequence", "Control", "DeadlineUnixNanos"):
        require(arm[field] == r[field], "receipt changed live owner identity")
    require(arm["State"] == "armed" and type(arm["Count"]) is int and arm["Count"] == 0 and arm["Attempts"] == arm["Work"] == arm["Entries"] == 0, "arm falsely claimed a native batch")
    for e in arm["Events"]:
        zero_event(e)
    require(r["State"] == "observing" and type(r["Count"]) is int and r["Count"] == (1 if side == "before" else 2), "unmatched/ambiguous/nonguaranteed boundary")
    e = event(r["Events"][0], r["Control"])
    require(type(e["Stage"]) is int and e["Stage"] == 1 and e["Synced"] is False, "before actual Sync")
    require(e["CommandBytes"] == len(command), "exact native command length")
    require(r["Attempts"] >= 1 and r["Entries"] >= e["Work"]["Entries"] and r["Work"] >= e["Work"]["Bytes"] and r["Output"] >= arm["Output"] + e["Work"]["OutputBytes"], "spent work/serialized reply ownership")
    if side == "after":
        after = event(r["Events"][1], r["Control"])
        require(after == {**e, "Stage": 2, "Synced": True}, "same exact batch guaranteed Sync result")
    else:
        zero_event(r["Events"][1])
    decoded = C.decision_command(command)
    require(decoded["phase"] == phase and decoded["partition"] == partition, "selected native phase/namespace")
    return e, decoded


def processes(rows, _io, _case, receipt):
    require(type(rows) is list and len(rows) == 2, "actual self-killed/replacement process pair")
    failed, restored = rows
    raw, reopened = T.decoded(P.raw(failed["Raw"])), T.decoded(P.raw(restored["Raw"]))
    require(set(raw) == set("Member PID ExitCode SignalNumber Signaled Reaped Volume Stderr Receipt".split()) and raw["Receipt"] == receipt, "persisted selected native receipt")
    require(failed["Action"] == "native-batch-transition-kill" and failed["Signal"] == "SIGKILL" and failed["Reaped"] is True and failed["Member"] == raw["Member"] == receipt["Member"] and raw["PID"] == receipt["PID"] and raw["ExitCode"] == -1 and raw["SignalNumber"] == 9 and raw["Signaled"] is True and raw["Reaped"] is True and failed["Volume"] == raw["Volume"], "no fallback kill or foreign process")
    require(restored["Action"] == "reopen" and restored["Ready"] is True and restored["Member"] == reopened["Member"] == receipt["Member"] and reopened["PID"] != receipt["PID"] and restored["PreviousInstance"] == reopened["PreviousInstance"] == failed["Instance"] and restored["Volume"] == reopened["Volume"] == failed["Volume"] and restored["RecoveryRaw"] == reopened["RecoveryRaw"], "genuine same-directory native replacement")


def check(data, *, program_roles=None):
    require(type(data) is dict and set(data) == HEADER and type(data["Version"]) is int and data["Version"] == 3 and data["Profile"] == PROFILE, "fresh exact-batch artifact")
    phase, part, kind, side = integer(data["Phase"], 4, 1), integer(data["Partition"], 8, 3), integer(data["Kind"], 2, 1), data["Side"]
    require(part in (3, 8) and (phase not in (1, 3) or part == 8) and side in ("before", "after") and data["Case"] == f"batch-cross-{phase}-p{part}-k{kind}-{side}", "finite applicable native transition inventory")
    require(data["IO"] is None and not any(data[k] for k in ("PartialReads", "PacketEvents", "Refused", "RefusedReplay")), "foreign VFS/refusal case")
    for key in ("InstalledA", "WithheldB"):
        require(type(data[key]) is dict and set(data[key]) == {"Request", "Reply"} and not any(P.raw(value) for value in data[key].values()), "foreign phase boundary")
    command = P.raw(data["SelectedCommand"])
    require(0 < len(command) <= 64 << 10, "selected source command bound")
    run, deadline = fixed(data["Run"], 16, True), integer(data["DeadlineUnixNanos"], (1 << 63)-1, 1)
    require(integer(data["ReceiptReads"], 129, 1)*65536 == integer(data["ReceiptGrantBytes"], 129*65536, 65536), "prepaid finite receipt authentication attempts")
    member=None
    if program_roles is not None:
        require(type(program_roles) is dict and set(program_roles)=={"initial","selected","after"}, "exact fresh batch program roles")
        initial=program_roles["initial"]
        require(type(initial) is tuple and len(initial)==2 and type(initial[0]) is type(initial[1]) is int and initial[0] in (1,2) and initial[1] in (4,6) and program_roles["selected"]==initial, "first occurrence uses actual initial native layout")
        member=initial[0 if part==3 else 1]
    e, decoded = receipt_pair(data["Arm"], data["Batch"], run, deadline, phase, part, kind, side, command, member=member)
    require(decoded["manifest"] == P.raw(data["Manifest"]), "same actual business manifest")
    processes(data["Processes"], None, data["Case"], data["Batch"])
    scopes, _ = P.cut(data["Before"])
    source = P.pgq(P.raw(data["Original"]))
    tx = P.body(decoded["manifest"], b"GCM\1")[:16]
    P.resumed(data["PendingCaller"], scopes, source["request"], tx, P.business_hash(P.raw(data["Original"]), 8), 1, b"")
    result = C.check_history(data, process_check=lambda rows, io, case: processes(rows, io, case, data["Batch"]), semantic_require=T.semantic)
    if kind == 2:
        rows = [row for row in data["Records"] if row["Partition"] == part and row["Index"] == e["Index"]]
        require(len(rows) == 1, "committed install entry missing from genuine complete prefix")
        actual = P.status(P.raw(rows[0]["Outcome"]))
        require(actual["partition"] == part and actual["phase"] == phase and actual["tx"] == tx and actual["digest"] == hashlib.sha256(decoded["manifest"]).digest() and actual["index"] <= e["Index"], "install bound to another retained entry")
    T.literal_edge_answers(data)
    result["scope"] = "exact target membership/ordered native batch commitment at Ready or install Sync; finite cross-command composition only, no ACK/power-loss/whole-D/runtime claim"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+", type=Path)
    args = parser.parse_args()
    require(len(args.artifacts) <= 24, "finite exact-batch artifact invocation")
    print(json.dumps([check(load(path)) for path in args.artifacts], indent=2))


if __name__ == "__main__":
    main()
