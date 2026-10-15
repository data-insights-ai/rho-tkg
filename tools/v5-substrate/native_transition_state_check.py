#!/usr/bin/env python3
"""Actual native state Commit(Sync) interruption and genuine snapshot recovery.

Receipts are diagnostic native batch observations, never import handles, ACKs,
portable cuts or power-loss evidence. A lost Begin supplies no guessed owner.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path


def module(name, file):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(file))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


B = module("native_state_batch", "native_transition_batch_check.py")
F = module("native_state_snapshot", "native_fault_family_check.py")
T, C, P = B.T, B.C, B.P
require, integer, fixed = T.require, T.integer, T.fixed
PROFILE = "native-actual-state-sync-process-v1"
HEADER = T.HEADER | set("Kind Side Run DeadlineUnixNanos Arm State Warm Snapshot SelectedSnapshot ReceiptReads ReceiptGrantBytes".split())
CONTROL = set("Version Action Cookie Sequence Kind Side Filter".split())
FILTER = set("Applied SourceIndex Sequence TargetGeneration SourceCutID ManifestID PlanID TargetBank MatchSequence MatchTarget".split())
IDENTITY = set("Applied SourceIndex ActiveGeneration TargetGeneration Sequence ActiveBank TargetBank Section Final Verified ImageSHA256 CurrentPublicationID ManifestID PlanID ProgressSHA256 SourceCutID SourceImageSHA256".split())
HASHES = set("ImageSHA256 CurrentPublicationID ManifestID PlanID ProgressSHA256 SourceCutID SourceImageSHA256".split())
EVENT = set("Selection Kind Stage Synced BeforeMetadataBytes MetadataBytes BeforeMetadataSHA256 MetadataSHA256 BatchBytes BatchPayloadSHA256 Work".split())
RECEIPT = set("Version Profile Cookie Run Member PID Sequence Control State DeadlineUnixNanos Attempts WorkBytes WorkEntries ObservedOutput SerializedOutput Count Events".split())
PHASE = {3: "before-begin", 4: "before-append", 5: "before-verify", 6: "before-activate", 8: "before-close", 9: "before-close"}


def load(path):
    digest, total, header, footer, records, snapshots = hashlib.sha256(), 0, None, None, [], []
    with path.open("rb") as source:
        while line := source.readline((512 << 10)+1):
            total += len(line)
            require(len(line) <= 512 << 10 and total <= 16 << 20 and line.endswith(b"\n"), "state artifact admission")
            row = T.decoded(line)
            require(type(row) is dict and footer is None, "state row after EOF")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and row["Type"] == "header", "state header")
                header = row["Artifact"]
                require(type(header) is dict and set(header) == HEADER and header["Records"] is None and header["Snapshot"] is None, "fresh state grammar")
            elif row.get("Type") == "record":
                require(set(row) == {"Type", "Record"} and len(records) < 512, "state actual prefix bound")
                records.append(row["Record"])
            elif row.get("Type") == "snapshot-boundary":
                require(set(row) == {"Type", "Sequence", "Boundary"} and type(row["Sequence"]) is int and row["Sequence"] == len(snapshots) < 16, "state actual notice bound/order")
                snapshots.append(row["Boundary"])
            else:
                require(set(row) == {"Type", "Rows", "SnapshotRows", "Through", "PayloadSHA256"} and row["Type"] == "footer", "state EOF")
                require(type(row["Rows"]) is int and row["Rows"] == len(records) and type(row["SnapshotRows"]) is int and row["SnapshotRows"] == len(snapshots) and row["PayloadSHA256"] == digest.hexdigest(), "state exact EOF census/hash")
                footer = row
            if footer is None:
                digest.update(line)
    require(header is not None and footer is not None and footer["Through"] == header["Through"], "missing/foreign state EOF")
    header["Records"], header["Snapshot"] = records, snapshots
    return header


def identity(value):
    require(type(value) is dict and set(value) == IDENTITY, "complete native state identity")
    for k in HASHES:
        fixed(value[k], 32)
    for k in IDENTITY - HASHES - {"Final", "Verified"}:
        integer(value[k], 1 if k in ("ActiveBank", "TargetBank") else 255 if k == "Section" else (1 << 64)-1)
    require(type(value["Final"]) is bool and type(value["Verified"]) is bool, "native cursor flags")
    return value


def selector(c, ident):
    require(type(c) is dict and set(c) == CONTROL and type(c["Version"]) is int and c["Version"] == 1 and c["Action"] == "arm" and c["Side"] in ("before", "after", "observe"), "state selector grammar")
    fixed(c["Cookie"], 16, True)
    integer(c["Sequence"], 16, 1)
    kind, f = integer(c["Kind"], 11, 1), c["Filter"]
    require(type(f) is dict and set(f) == FILTER and type(f["MatchTarget"]) is bool and type(f["MatchSequence"]) is bool, "finite actual filter")
    for k in ("Applied", "SourceIndex", "Sequence", "TargetGeneration", "TargetBank"):
        integer(f[k], 1 if k == "TargetBank" else (1 << 64)-1)
    for k in ("SourceCutID", "ManifestID", "PlanID"):
        fixed(f[k], 32)
    require(f["MatchSequence"] or f["Sequence"] == 0, "ignored sequence selector")
    require(f["MatchTarget"] or f["TargetBank"] == f["TargetGeneration"] == 0, "ignored target selector")
    if kind in (1, 2):
        valid = f["Applied"] > 0 and not any(f["ManifestID"]+f["PlanID"]) and not f["MatchTarget"] and not f["MatchSequence"]
    elif kind == 3:
        valid = f["SourceIndex"] > 0 and any(f["SourceCutID"]) and not any(f["PlanID"]) and not f["MatchTarget"] and not f["MatchSequence"]
    elif kind in (4, 5, 6, 8):
        valid = f["SourceIndex"] > 0 and any(f["SourceCutID"]) and any(f["ManifestID"]) and not any(f["PlanID"])
    elif kind == 7:
        valid = f["SourceIndex"] > 0 and any(f["PlanID"]) and not any(f["ManifestID"]+f["SourceCutID"])
    else:
        valid = kind in (9, 10) and f["MatchTarget"] and (kind != 9 or f["TargetGeneration"] > 0) and f["SourceIndex"] == 0 and not any(f["ManifestID"]+f["PlanID"]+f["SourceCutID"])
    require(valid, "unsupported/empty native state selector")
    for k in ("Applied", "SourceIndex", "SourceCutID", "ManifestID", "PlanID"):
        empty = not any(f[k]) if type(f[k]) is list else f[k] == 0
        require(empty or f[k] == ident[k], "selected native state field: " + k)
    require(not f["MatchSequence"] or f["Sequence"] == ident["Sequence"], "actual prospective cursor")
    require(not f["MatchTarget"] or (f["TargetBank"], f["TargetGeneration"]) == (ident["TargetBank"], ident["TargetGeneration"]), "actual generation target")


def event(e, c):
    require(type(e) is dict and set(e) == EVENT, "state event grammar")
    s, w = e["Selection"], e["Work"]
    require(type(s) is dict and set(s) == {"Cookie", "Sequence", "Identity"} and s["Cookie"] == c["Cookie"] and s["Sequence"] == c["Sequence"], "same live state selection")
    selector(c, identity(s["Identity"]))
    require(type(e["Kind"]) is int and e["Kind"] == c["Kind"] and type(e["Synced"]) is bool, "actual state batch kind")
    integer(e["BeforeMetadataBytes"], 65536, 1)
    integer(e["MetadataBytes"], 65536, 1)
    integer(e["BatchBytes"], 1 << 42, 12)
    for k in ("BeforeMetadataSHA256", "MetadataSHA256", "BatchPayloadSHA256"):
        fixed(e[k], 32, True)
    require(type(w) is dict and set(w) == {"Entries", "Bytes", "OutputBytes"}, "state prepaid work")
    require(integer(w["Entries"], 3, 3) and integer(w["Bytes"], 1 << 42, e["BatchBytes"]) and integer(w["OutputBytes"], 2048, 2048), "state work bounds")
    return e


def zero_event(e):
    require(type(e) is dict and set(e) == EVENT and type(e["Selection"]) is dict and set(e["Selection"]) == {"Cookie", "Sequence", "Identity"}, "zero state event grammar")
    require(fixed(e["Selection"]["Cookie"], 16) == bytes(16) and type(e["Selection"]["Sequence"]) is int and e["Selection"]["Sequence"] == 0, "zero selection")
    ident = identity(e["Selection"]["Identity"])
    require(all(not any(v) if type(v) is list else v is False if type(v) is bool else v == 0 for v in ident.values()), "unused identity nonzero")
    for k in ("BeforeMetadataSHA256", "MetadataSHA256", "BatchPayloadSHA256"):
        require(fixed(e[k], 32) == bytes(32), "unused event digest")
    require(e["Synced"] is False and type(e["Work"]) is dict and set(e["Work"]) == {"Entries", "Bytes", "OutputBytes"} and all(type(v) is int and v == 0 for v in e["Work"].values()), "unused state work")
    require(all(type(e[k]) is int and e[k] == 0 for k in EVENT - {"Selection", "Work", "Synced", "BeforeMetadataSHA256", "MetadataSHA256", "BatchPayloadSHA256"}), "unused state event")


def receipt(r, run, deadline):
    require(type(r) is dict and set(r) == RECEIPT and type(r["Version"]) is int and r["Version"] == 1 and r["Profile"] == PROFILE, "native state owner grammar")
    require(fixed(r["Run"], 16, True) == run and integer(r["Member"], 3, 3) and integer(r["PID"], minimum=1), "genuine state process/run")
    require(fixed(r["Cookie"], 16, True) == fixed(r["Control"]["Cookie"], 16, True) and integer(r["Sequence"], 16, 1) == r["Control"]["Sequence"], "state cookie/sequence")
    integer(r["DeadlineUnixNanos"], deadline, 1)
    for k, cap in (("Attempts", 2048), ("WorkBytes", 1 << 42), ("WorkEntries", 1 << 20), ("ObservedOutput", 8 << 20), ("SerializedOutput", 4 << 20)):
        integer(r[k], cap)
    require(r["SerializedOutput"] >= 2*16384, "separate serialized reply/receipt funding")
    require(type(r["Events"]) is list and len(r["Events"]) == 2, "fixed state event ownership")


def pair(arm, r, run, deadline):
    for value in (arm, r):
        receipt(value, run, deadline)
    for k in ("Cookie", "Run", "Member", "PID", "Sequence", "Control", "DeadlineUnixNanos"):
        require(arm[k] == r[k], "native state owner changed")
    require(arm["State"] == "armed" and type(arm["Count"]) is int and arm["Count"] == 0, "arm claimed an actual native event")
    for e in arm["Events"]:
        zero_event(e)
    side = r["Control"]["Side"]
    require(side in ("before", "after") and r["State"] == "observing" and type(r["Count"]) is int and r["Count"] == (1 if side == "before" else 2), "unobserved/failed state arm")
    e = event(r["Events"][0], r["Control"])
    require(type(e["Stage"]) is int and e["Stage"] == 1 and e["Synced"] is False, "before actual Commit Sync")
    require(r["Attempts"] == arm["Attempts"]+1 and r["WorkBytes"] == arm["WorkBytes"]+e["Work"]["Bytes"] and r["WorkEntries"] == arm["WorkEntries"]+3 and r["ObservedOutput"] == arm["ObservedOutput"]+2048 and r["SerializedOutput"] == arm["SerializedOutput"], "actual state attempted grant spent once")
    if side == "after":
        require(event(r["Events"][1], r["Control"]) == {**e, "Stage": 2, "Synced": True}, "same exact synchronized native state batch")
    else:
        zero_event(r["Events"][1])
    return e["Selection"]["Identity"]


def processes(rows, receipt_):
    require(type(rows) is list and len(rows) == 4, "actual lagged/failed/replacement census")
    first, failed, interrupted, restored = rows
    raw = T.decoded(P.raw(first["Raw"]))
    require(first["Action"] == "kill" and first["Member"] == raw["Member"] == 3 and first["Reaped"] is True and first["Signal"] == "SIGKILL" and raw["Signaled"] is True and raw["SignalNumber"] == 9 and raw["Reaped"] is True and raw["PID"] != receipt_["PID"] and first["Volume"] == raw["Volume"], "genuine lagged replica process loss")
    actual = T.decoded(P.raw(failed["Raw"]))
    require(set(actual) == set("Member PID ExitCode SignalNumber Signaled Reaped Volume Stderr Receipt".split()) and actual["Receipt"] == receipt_ and actual["Member"] == failed["Member"] == 3 and actual["PID"] == receipt_["PID"] and actual["Signaled"] is True and actual["SignalNumber"] == 9 and actual["ExitCode"] == -1 and actual["Reaped"] is True and failed["Action"] == "native-state-transition-kill" and failed["Reaped"] is True and failed["Signal"] == "SIGKILL" and failed["Volume"] == actual["Volume"] == first["Volume"], "actual matched native state self-SIGKILL")
    mid = T.decoded(P.raw(interrupted["Raw"]))
    require(set(mid) == {"Member", "PID", "Ready", "Startup"} and mid["Member"] == interrupted["Member"] == 3 and mid["PID"] == receipt_["PID"] and mid["Ready"] is False and interrupted["Action"] == "reopen-interrupted" and interrupted["Ready"] is False and interrupted["Instance"] == failed["Instance"] and interrupted["PreviousInstance"] == first["Instance"] and interrupted["Volume"] == first["Volume"], "interrupted replacement cannot claim Ready")
    startup = F.process(P.raw(mid["Startup"]))
    end = T.decoded(P.raw(restored["Raw"]))
    require(restored["Action"] == "reopen" and restored["Ready"] is True and restored["Member"] == end["Member"] == 3 and end["PID"] != receipt_["PID"] and restored["PreviousInstance"] == end["PreviousInstance"] == failed["Instance"] and restored["Volume"] == end["Volume"] == first["Volume"] and restored["RecoveryRaw"] == end["RecoveryRaw"], "same durable directory genuine replacement")
    local = F.process(P.raw(end["LocallyRestoredRaw"]))
    recovery = F.process(P.raw(end["RecoveryRaw"]))
    require(local["applied"] >= startup["applied"] and recovery["applied"] >= local["applied"] and local["incarnation"] == recovery["incarnation"] != startup["incarnation"], "native restored/caught-up process lineage")
    if receipt_["Control"]["Kind"] == 6 and receipt_["Control"]["Side"] == "after":
        ident = receipt_["Events"][0]["Selection"]["Identity"]
        require(local["stats"][1] >= ident["Applied"] and local["cut"] == fixed(ident["SourceCutID"],32) and local["manifest"] == fixed(ident["ManifestID"],32), "synced activation audit/checkpoint missing before catch-up")
    return startup["incarnation"]


def snapshots(data, scopes, ident, incarnation):
    rows, selected, kind = data["Snapshot"], data["SelectedSnapshot"], data["Kind"]
    require(type(rows) is list and 3 <= len(rows) <= 16, "snapshot notices bound")
    integer(selected, len(rows)-2)
    target = rows[selected]
    require(target["Phase"] == PHASE[kind], "selected actual operation boundary")
    current, attempts, final_activation = None, [], False
    selected_manifest = None
    for n, row in enumerate(rows):
        require(type(row) is dict and set(row) == {"Phase", "Donor", "Receiver", "Owner", "Manifest", "Chunk", "Packet", "ResponseWire"} and row["Donor"] == 1 and row["Receiver"] == 3, "actual state snapshot notice")
        owner = integer(row["Owner"])
        phase = row["Phase"]
        manifest = F.snapshot_manifest(P.raw(row["Manifest"]), scopes)
        if selected_manifest is not None:
            require(manifest == selected_manifest, "retry changed the actual published source cut/configuration/complete stream")
        packet = row["Packet"]
        require(type(packet) is dict and set(packet) == {"From", "To", "Snapshot", "Payload"} and packet["From"] == 1 and packet["To"] == 3 and packet["Snapshot"] is True, "actual owned native MsgSnap")
        message = F.raft_packet(P.raw(packet["Payload"]), scopes, 3)
        require(message.get(1) == [7] and message.get(2) == [3] and message.get(3) == [1] and len(message.get(9, [])) == 1, "actual snapshot packet fields")
        native = F.protobuf_fields(message[9][0])
        require(set(native) == {1, 2} and native[1] == [manifest["descriptor"]] and len(native[2]) == 1 and F.protobuf_fields(native[2][0]) == {1: [manifest["conf"]], 2: [manifest["index"]], 3: [manifest["term"]]}, "actual snapshot descriptor/config/index/term")
        if current is None or n == selected+1:
            require(not final_activation and len(attempts) < 2, "bounded genuine retry inventory")
            current = dict(sequence=0, after=b"", last=b"", final=False, prepared=False, rows=[0]*6, bytes=[0]*6, indexes={}, digest=manifest["seed"], manifest=manifest["manifest"], owner=owner, incarnation=None)
            attempts.append(current)
        require(current["manifest"] == manifest["manifest"], "same genuine publication changed during attempt")
        raw = P.raw(row["ResponseWire"])
        if phase in ("before-begin", "before-append", "before-verify"):
            # Exact all-zero process-response encoding, no correlated ACK/owner.
            require(len(raw) == 381 and not any(raw), "pre-call notice supplied fabricated native response")
            response = None
        else:
            response = F.process(raw)
            require(response["code"] == response["mask"] == 0 and not response["outcome"] and not response["changes"], "returned native refusal cannot qualify")
            if current["incarnation"] is None:
                current["incarnation"] = response["incarnation"]
            require(current["incarnation"] == response["incarnation"], "import capability changed process")
        if phase == "before-begin":
            require(owner == 0 and current["sequence"] == 0 and current["owner"] == 0 and not P.raw(row["Chunk"]), "unknown Begin invented import owner")
        else:
            require(owner > 0, "missing genuine live import")
            if current["owner"] == 0:
                require(phase == "after-begin", "unknown Begin became guessed import authority")
                current["owner"] = owner
            require(current["owner"] == owner, "actual same-live import handle changed")
            if phase == "after-begin":
                require(response["handles"] == [owner] and response["aux"] == manifest["manifest"] and current["sequence"] == 0, "genuine Begin reply/manifest")
            elif phase == "before-append":
                require(n == selected and not current["final"], "only selected pre-Append is emitted")
                prospective = {**current, "rows":current["rows"][:], "bytes":current["bytes"][:], "indexes":dict(current["indexes"])}
                F.snapshot_chunk(P.raw(row["Chunk"]), manifest, prospective)
                require(ident["Sequence"] == prospective["sequence"] and fixed(ident["ProgressSHA256"],32) == prospective["digest"], "actual prospective Append cursor/digest")
            elif phase == "after-append":
                require(not current["final"] and not current["prepared"], "Append after immutable EOF")
                F.snapshot_chunk(P.raw(row["Chunk"]), manifest, current)
            elif phase == "before-verify":
                require(n == selected and current["final"] and not current["prepared"], "seal lacks complete source EOF")
            elif phase in ("before-activate", "before-close"):
                require(current["final"] and not current["prepared"] and response["stats"][11] == response["stats"][23] == response["stats"][24] == 1, "actual verified unclaimed prepared owner")
                current["prepared"] = True
            elif phase == "after-activate":
                require(n == len(rows)-1 and current["final"] and current["prepared"] and response["stats"][1] == response["stats"][5] == manifest["index"] and response["cut"] == manifest["cut"] and response["manifest"] == manifest["manifest"], "genuine retry activation/ACK")
                final_activation = True
            else:
                require(False, "unsupported state snapshot phase")
        if n == selected:
            selected_manifest = manifest
            if kind != 9:
                require(ident["SourceIndex"] == manifest["index"] and fixed(ident["SourceCutID"],32) == manifest["cut"] and fixed(ident["SourceImageSHA256"],32) == hashlib.sha256(manifest["image"]).digest() and fixed(ident["ManifestID"],32) == manifest["manifest"], "actual native state identity/source join")
            if current["incarnation"] is not None:
                require(current["incarnation"] == incarnation, "selected import outside actual failed process")
            if kind == 3:
                require(ident["Sequence"] == 0 and ident["Final"] is False and ident["Verified"] is False, "Begin claimed imported EOF")
            elif kind in (5,6,8):
                require(ident["Final"] is True and ident["Verified"] is True and fixed(ident["ProgressSHA256"],32) == current["digest"] and ident["Sequence"] == current["sequence"], "seal/activation/retire lacks exact complete imported cursor")
            if kind == 6:
                require(ident["Applied"] == manifest["index"] and ident["ActiveBank"] == ident["TargetBank"] and ident["ActiveGeneration"] == ident["TargetGeneration"] and fixed(ident["ImageSHA256"],32) == hashlib.sha256(manifest["image"]).digest() and fixed(ident["CurrentPublicationID"],32) == manifest["cut"], "activation is its actual one native bank/checkpoint/publication batch")
            elif kind == 9:
                require(ident["ActiveBank"] != ident["TargetBank"] and not any(ident["ManifestID"]+ident["SourceCutID"]+ident["SourceImageSHA256"]+ident["PlanID"]) and ident["SourceIndex"] == 0, "physical clear claimed omitted source authority")
    require(len(attempts) == 2 and final_activation and selected_manifest is not None, "incomplete genuine snapshot retry")
    warm = data["Warm"]
    if kind in (8,9):
        receipt(warm, fixed(data["Run"],16,True), data["DeadlineUnixNanos"])
        require(warm["State"] == "observed" and warm["Control"]["Kind"] == 3 and warm["Control"]["Side"] == "observe" and warm["Count"] == 2 and warm["Cookie"] == data["Arm"]["Cookie"] and warm["PID"] == data["Arm"]["PID"] and warm["Sequence"]+1 == data["Arm"]["Sequence"], "same-live native Begin observation required")
        first = event(warm["Events"][0],warm["Control"])
        require(first["Stage"] == 1 and first["Synced"] is False and event(warm["Events"][1],warm["Control"]) == {**first,"Stage":2,"Synced":True}, "genuine Begin Sync pair")
        actual = first["Selection"]["Identity"]
        require(fixed(actual["ManifestID"],32) == selected_manifest["manifest"] and fixed(actual["SourceCutID"],32) == selected_manifest["cut"] and actual["SourceIndex"] == selected_manifest["index"] and (actual["TargetBank"],actual["TargetGeneration"]) == (ident["TargetBank"],ident["TargetGeneration"]), "clear/retire target derived from actual native Begin, never active+1")
    else:
        require(warm is None, "orphan same-live Begin diagnostic")


def check(data):
    require(type(data) is dict and set(data) == HEADER and type(data["Version"]) is int and data["Version"] == 4 and data["Profile"] == PROFILE, "fresh state artifact grammar")
    kind, side = integer(data["Kind"], 9, 3), data["Side"]
    require(kind in PHASE and side in ("before","after") and data["Case"] == f"state-import-k{kind}-{side}", "finite applicable state/process inventory")
    require(data["IO"] is None and not any(data[k] for k in ("PartialReads","PacketEvents","Refused","RefusedReplay")) and not P.raw(data["PendingCaller"]), "foreign refusal/network recipe")
    for k in ("InstalledA","WithheldB"):
        require(type(data[k]) is dict and set(data[k]) == {"Request","Reply"} and not any(P.raw(v) for v in data[k].values()), "foreign phase recipe")
    run,deadline = fixed(data["Run"],16,True),integer(data["DeadlineUnixNanos"],(1<<63)-1,1)
    require(data["Arm"]["Control"]["Kind"] == kind and data["Arm"]["Control"]["Side"] == side, "native selected kind/side")
    require(integer(data["ReceiptReads"],129,1)*131072 == integer(data["ReceiptGrantBytes"],129*131072,131072), "finite receipt authentication spend")
    ident = pair(data["Arm"],data["State"],run,deadline)
    incarnation = processes(data["Processes"],data["State"])
    scopes,_ = P.cut(data["Before"])
    snapshots(data,scopes,ident,incarnation)
    result = C.check_history(data,process_check=lambda rows,io,case:processes(rows,data["State"]),semantic_require=T.semantic)
    T.literal_edge_answers(data)
    result["scope"] = "native state before/after Commit(Sync), genuine source retry/EOF and local old-cut recovery; finite import cases only, no ACK/power-loss/whole-D qualification"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts",nargs="+",type=Path)
    args = parser.parse_args()
    require(len(args.artifacts)<=12,"finite state artifact invocation")
    print(json.dumps([check(load(path)) for path in args.artifacts],indent=2))


if __name__ == "__main__":
    main()
