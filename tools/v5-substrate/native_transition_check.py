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
LOGICAL_PROFILE = "native-live-submission-transition-v1"
LOGICAL_HEADER = HEADER | {"Recipe", "Gate", "Ticks", "Run", "DeadlineUnixNanos", "RecipeSHA256"}


class SemanticFailure(ValueError):
    """Only independently checked semantics, after authentic source framing."""
    def __init__(self, code, detail):
        self.code, self.detail = code, detail
        super().__init__(code + ": " + detail)


def semantic(value, label):
    if not value:
        code = label if label.startswith("old-cut:") else "serial-cut" if label.startswith("independent exact serial/cut witness:") else label
        raise SemanticFailure(code, label)


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
                require(type(header) is dict, "typed fresh header")
                expected = LOGICAL_HEADER if header.get("Profile") == LOGICAL_PROFILE else HEADER
                require(type(header) is dict and set(header) == expected and header["Records"] is None, "fresh header grammar")
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
    if type(data) is dict and data.get("Profile") == LOGICAL_PROFILE:
        return logical_check(data)
    require(type(data) is dict and set(data) == HEADER and data["Version"] == 1 and data["Profile"] == C.PROFILE and data["Case"] in WAL_CASES, "finite implemented storage case")
    require(data["IO"] is not None and not any(data[key] for key in ("PendingCaller", "PartialReads", "PacketEvents", "Refused", "RefusedReplay")), "foreign case evidence")
    # Authenticate the new real-process grammar before shared history/serial
    # checks. This is not a permissive callback for an unknown artifact format.
    processes(data["Processes"], data["IO"], data["Case"])
    result = C.check_history(data, process_check=processes)
    result["scope"] = "actual matched VFS boundary during same-original recovery; no phase-specific IO, power-loss, whole-D or runtime qualification claim"
    return result


def recipe(value):
    require(type(value) is dict and set(value) == {"Version", "Seed", "Phase", "Partition", "Side", "Actions"}, "recorded action recipe grammar")
    integer(value["Version"], 1, 1)
    integer(value["Seed"], minimum=1)
    phase, partition = integer(value["Phase"], 4, 1), integer(value["Partition"], 8, 3)
    require(partition in (3, 8) and (phase not in (1, 3) or partition == 8) and value["Side"] in ("before-submit", "after-applied"), "finite genuine cross-phase selector")
    require(value["Actions"] is None or type(value["Actions"]) is list, "recorded action container")
    actions = value["Actions"] or []
    require(type(actions) is list and len(actions) <= 8, "recorded action admission")
    total = 0
    for action in actions:
        require(type(action) is dict and set(action) == {"Member", "Ticks"}, "actual Tick action grammar")
        integer(action["Member"], 6, 1)
        total += integer(action["Ticks"], 3, 1)
    require(total <= 16, "whole-trial Tick work")
    return phase, partition, value["Side"], actions


def logical_gate(data):
    phase, partition, side, actions = recipe(data["Recipe"])
    g = data["Gate"]
    require(type(g) is dict and set(g) == set("Version Profile Cookie Run Member PID Sequence Control State DeadlineUnixNanos Proposal EntryData Target ProposalSHA256 Index Term Root Outcome CDC AttemptedWork PrepaidOutput".split()), "logical gate grammar")
    require(g["Version"] == 1 and g["Profile"] == LOGICAL_PROFILE and g["State"] == "reached", "matched live typed-call gate")
    fixed(g["Cookie"], 16, True)
    require(fixed(g["Run"], 16, True) == fixed(data["Run"], 16, True), "actual child/source run identity")
    member = 1 if partition == 3 else 4
    require(g["Member"] == member and integer(g["PID"], minimum=1) and integer(g["Sequence"], 1, 1), "actual source owner/member")
    require(integer(g["DeadlineUnixNanos"], (1 << 63)-1, 1) <= integer(data["DeadlineUnixNanos"], (1 << 63)-1, 1), "owner lifetime exceeds original caller deadline")
    integer(g["AttemptedWork"], 1 << 50, 1)
    require(g["PrepaidOutput"] == 512 << 10, "prepaid finite observation owner")
    control = g["Control"]
    require(type(control) is dict and set(control) == {"Version", "Action", "Cookie", "Sequence", "Family", "Phase", "Side", "Identity", "Partition", "Page"}, "live selection grammar")
    require(control["Version"] == 1 and control["Action"] == "arm" and control["Cookie"] == g["Cookie"] and control["Sequence"] == g["Sequence"] and control["Family"] == "cross" and control["Phase"] == phase and control["Partition"] == partition and control["Side"] == side and control["Page"] == 0, "exact admitted phase/side")
    proposal = C.P.raw(g["Proposal"])
    require(0 < len(proposal) <= 64 << 10 and fixed(g["ProposalSHA256"], 32) == hashlib.sha256(proposal).digest(), "owned actual command bytes/hash")
    command = C.decision_command(proposal)
    manifest = C.P.raw(data["Manifest"])
    require(command["phase"] == phase and command["partition"] == partition and command["manifest"] == manifest and fixed(control["Identity"], 16, True) == C.P.body(manifest, b"GCM\1")[:16], "actual selected command/original manifest")
    scopes, _ = C.P.cut(data["Before"])
    require(C.P.raw(g["Target"]) == scopes[partition], "complete configured source target")
    root = g["Root"]
    require(type(root) is dict and set(root) == {"Generation", "Index", "Image", "ImageHash"}, "captured native root grammar")
    if side == "before-submit":
        require(g["Index"] == g["Term"] == root["Index"] == root["Generation"] == 0 and not any(C.P.raw(g[field]) for field in ("EntryData", "Outcome", "CDC")) and not C.P.raw(root["Image"]) and fixed(root["ImageHash"], 32) == bytes(32), "before-submit claimed durable entry/source")
    else:
        index = integer(g["Index"], minimum=1)
        integer(g["Term"], minimum=1)
        integer(root["Generation"], minimum=1)
        image = C.P.raw(root["Image"])
        require(root["Index"] == index and C.P.raw(g["EntryData"]) == proposal and fixed(root["ImageHash"], 32) == hashlib.sha256(image).digest(), "exact native EntryData/root binding")
        require(len(image) == 172 and image[:3] == b"GR\3" and image[3] in (1, 2, 3) and image[4:20] == C.P.GRAPH and int.from_bytes(image[20:28], "big") == partition and hashlib.sha256(image[:-32]).digest() == image[-32:], "native root codec/namespace")
        record = [row for row in data["Records"] if (row["Partition"], row["Index"]) == (partition, index)]
        require(len(record) == 1 and record[0]["Outcome"] == g["Outcome"] and record[0]["CDC"] == g["CDC"], "applied result absent/different in complete authentic prefix")
        status = C.P.status(C.P.raw(g["Outcome"]))
        require(status["phase"] == phase and status["partition"] == partition and status["tx"] == fixed(control["Identity"], 16) and status["digest"] == hashlib.sha256(manifest).digest() and 0 < status["index"] <= index, "actual immutable phase outcome")
    ticks = data["Ticks"] or []
    expected = [(a["Member"], ordinal) for a in actions for ordinal in range(1, a["Ticks"]+1)]
    require(type(ticks) is list and len(ticks) == len(expected), "recorded actions actual Tick census")
    for tick, (voter, ordinal) in zip(ticks, expected):
        require(set(tick) == {"Member", "Ordinal", "Before", "After", "Response"} and tick["Member"] == voter and tick["Ordinal"] == ordinal and integer(tick["Before"], minimum=1) <= integer(tick["After"], minimum=1), "actual finite Tick transition")
        response = native_tick(C.P.raw(tick["Response"]))
        require(response["code"] == response["mask"] == 0 and response["applied"] >= tick["Before"], "Tick response invented success")
    return g


def native_tick(wire):
    # Tick may genuinely produce Raft packets. The older refusal-only response
    # parser intentionally disallows them and cannot be substituted here.
    require(len(wire) <= 2 << 20, "actual Tick frame admission")
    c = C.P.Cursor(wire)
    require(any(c.take(16)), "actual Tick incarnation")
    c.take(28*8 + 32 + 32 + 8)
    handles = c.number(4)
    require(handles <= 2, "Tick handles")
    c.take(8*handles)
    c.field(); ready, sequence, applied, code, mask = c.number(1), c.number(), c.number(), c.number(1), c.number(2)
    c.field(4096)
    packets = c.number(4)
    require(packets <= 128, "Tick packet admission")
    for _ in range(packets):
        source, target, snapshot = c.number(), c.number(), c.number(1)
        require(1 <= source <= 6 and 1 <= target <= 6 and source != target and snapshot <= 1, "actual Tick endpoints")
        c.field(2 << 20)
    reads = c.number(4)
    require(reads <= 16, "actual Tick reads")
    for _ in range(reads): c.number(); c.field(1024)
    c.field(64 << 10); c.field(64 << 10); c.field()
    flags, _, labels = c.number(1), c.number(), c.number(4)
    require(flags <= 3 and labels <= 16, "Tick fact shape")
    for _ in range(labels): c.field(256)
    c.field(256); c.end()
    require(ready <= 1 and sequence > 0 and code <= 6, "actual Tick identity/result")
    return dict(code=code, mask=mask, applied=applied)


def literal_edge_answers(data):
    """Independent fixture intent: one whole life-bound edge and scalar5.

    This model uses declared graph operations and literal IDs/values, never
    implementation locks/floors/fences or the observed final state as its write.
    """
    before, after = C.P.projected(data["Before"]), C.P.projected(data["Recovered"])
    decisions = [C.P.status(C.P.raw(row["Outcome"])) for row in data["Records"]
                 if C.P.raw(row["Outcome"])[:4] == b"GCS\1" and C.P.status(C.P.raw(row["Outcome"]))["phase"] == 3
                 and C.P.status(C.P.raw(row["Outcome"]))["index"] == row["Index"]]
    require(len(decisions) == 1 and decisions[0]["commit"], "genuine recovered original COMMIT")
    round_ = decisions[0]["round"]
    expected = {**before,
                "A/entity/3": dict(kind=2, source=1, target=17, mode=1, type="edge"),
                "A/presence/3": dict(present=True, null=False, handle=4, revision=round_, provenance=3, kind="life", i64=0),
                "A/labels/3": [],
                "A/property/3/p": dict(present=True, null=False, handle=5, revision=round_, provenance=3, kind="i64", i64=5),
                "A/life/3/4": dict(sourceLife=2, targetLife=18)}
    for key in ("A/posting/1/0", "A/posting/1/2", "B/posting/17/0", "B/posting/17/18"): expected[key] = [3]
    semantic(after == expected, "atomic-edge-state")
    native = data["Recovered"]["Native"]
    semantic([row["id"] for row in native["entityCensus"] if row["found"]] == [1, 3, 17], "exact-entity-census")
    edge = [row for row in native["entities"] if row["id"] == 3]
    semantic(len(edge) == 1 and edge[0]["effective"] is True and edge[0]["effectiveLife"] == 4, "endpoint-effective-life")
    for row in native["postings"]:
        wanted = [] if (row["endpoint"], row["life"]) == (1, 7) else [3]
        semantic(row["candidates"] == wanted and row["declared"] == wanted and row["effective"] == wanted, "exact-endpoint-postings")


def logical_processes(rows, _receipt, _case, gate):
    require(type(rows) is list and len(rows) == 2, "logical actual kill/reopen census")
    killed, restored = rows
    raw, recovery = decoded(C.P.raw(killed["Raw"])), decoded(C.P.raw(restored["Raw"]))
    require(killed["Action"] == "kill" and killed["Reaped"] is True and killed["Signal"] == "SIGKILL" and killed["Member"] == raw["Member"] == gate["Member"] and raw["PID"] == gate["PID"] and raw["SignalNumber"] == 9 and raw["Signaled"] is True and raw["Reaped"] is True, "selected originating child genuinely killed/reaped")
    require(restored["Action"] == "reopen" and restored["Ready"] is True and restored["Member"] == recovery["Member"] == gate["Member"] and recovery["PID"] != gate["PID"] and restored["PreviousInstance"] == recovery["PreviousInstance"] == killed["Instance"] and restored["Volume"] == recovery["Volume"] == killed["Volume"] and restored["RecoveryRaw"] == recovery["RecoveryRaw"], "same durable directory/native replacement")


def logical_check(data):
    require(type(data) is dict and set(data) == LOGICAL_HEADER and data["Version"] == 2 and data["Profile"] == LOGICAL_PROFILE, "fresh logical artifact grammar")
    phase, partition, side, _ = recipe(data["Recipe"])
    canonical = json.dumps(data["Recipe"], separators=(",", ":"), ensure_ascii=False).encode()
    require(fixed(data["RecipeSHA256"], 32) == hashlib.sha256(canonical).digest(), "exact recorded recipe bytes")
    require(data["Case"] == f"cross-{phase}-p{partition}-{side}" and data["IO"] is None and not any(data[field] for field in ("PartialReads", "PacketEvents", "Refused", "RefusedReplay")), "exact finite logical case/evidence")
    gate = logical_gate(data)
    logical_processes(data["Processes"], None, data["Case"], gate)
    scopes, _ = C.P.cut(data["Before"])
    source = C.P.pgq(C.P.raw(data["Original"]))
    tx = fixed(gate["Control"]["Identity"], 16)
    C.P.resumed(data["PendingCaller"], scopes, source["request"], tx, C.P.business_hash(C.P.raw(data["Original"]), 8), 1, b"")
    result = C.check_history(data, process_check=lambda rows, receipt, case: logical_processes(rows, receipt, case, gate), semantic_require=semantic)
    literal_edge_answers(data)
    result["scope"] = "exact live typed-call before-submit/after-applied result; separate from physical WAL/sync attribution; finite source-only family pending runtime qualification"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+", type=Path)
    args = parser.parse_args()
    require(len(args.artifacts) <= 16, "finite artifact invocation")
    print(json.dumps([check(load(path)) for path in args.artifacts], indent=2))


if __name__ == "__main__":
    main()
