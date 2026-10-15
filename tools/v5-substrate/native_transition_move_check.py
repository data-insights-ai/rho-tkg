#!/usr/bin/env python3
"""Actual owned transfer transitions, immutable originals and native old cuts.

This finite source bridge uses genuine child captures. Diagnostic cookies and
decoded intents supply no native authority; the Go parent retains the factory.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import struct

SPEC = importlib.util.spec_from_file_location("native_move_components", Path(__file__).with_name("native_fault_family_check.py"))
F = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(F)
P = F.P
PROFILE = "native-owned-transfer-transition-v1"
HEADER = set("Version Profile Recipe Gate Run DeadlineUnixNanos FactoryCookie PrepareRequest PrepareResponse Intent Pending Complete Replay Before Retained Recovered Reopened PlacementBefore PlacementAfter Processes From Through Records".split())
require, integer, fixed = F.require, F.integer, F.octets


def recipe(value):
    require(type(value) is dict and set(value) == {"Version", "Phase", "Partition", "Page", "Side"}, "exact move transition recipe")
    integer(value["Version"], 1, 1)
    phase, partition, page = integer(value["Phase"], 9, 1), integer(value["Partition"], 8, 3), integer(value["Page"], 1024)
    require(partition in (3, 8) and value["Side"] in ("before-submit", "after-applied"), "configured participant/side")
    require((phase in (1, 2, 7, 8, 9) and page == 0)
            or (phase == 3 and partition == 3 and page in (0, 1))
            or (phase == 4 and partition == 8 and page in (0, 1, 3))
            or (phase == 5 and partition == 8 and page == 0)
            or (phase == 6 and partition == 3 and page == 0), "implemented actual phase/page inventory")
    return phase, partition, page, value["Side"]


def load(path):
    digest, total, header, footer, records = hashlib.sha256(), 0, None, None, []
    with path.open("rb") as stream:
        while line := stream.readline((512 << 10)+1):
            total += len(line)
            require(total <= 16 << 20 and len(line) <= 512 << 10 and line.endswith(b"\n"), "bounded source frame")
            row = F.decoded(line)
            require(type(row) is dict and footer is None, "row after complete EOF")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and row["Type"] == "header", "header row")
                header = row["Artifact"]
                require(type(header) is dict and set(header) == HEADER and header["Records"] is None, "fresh move header")
            elif row.get("Type") == "record":
                require(set(row) == {"Type", "Record"} and len(records) < 512, "bounded original prefix")
                records.append(row["Record"])
            else:
                require(set(row) == {"Type", "Rows", "Through", "PayloadSHA256"} and row["Type"] == "footer", "EOF grammar")
                footer = row
                require(row["Rows"] == len(records) and row["PayloadSHA256"] == digest.hexdigest(), "exact prefix census/hash")
            if footer is None:
                digest.update(line)
    require(header is not None and footer is not None and footer["Through"] == header["Through"], "missing/foreign EOF")
    header["Records"] = records
    return header


def original(data, scopes, publication):
    prepared = F.move_result(data["PrepareResponse"])
    require(prepared["pending"] == 1 and prepared["committed"] == 0 and prepared["statuses"] == [None]*3
            and prepared["response"]["code"] == prepared["response"]["mask"] == 0, "actual pre-submit factory result, no fabricated decision")
    expected = bytearray(prepared["response"]["incarnation"])
    expected[8:] = struct.pack(">Q", prepared["response"]["sequence"])
    require(prepared["scope"] == scopes[3] and prepared["request"] == expected, "actual child incarnation/request correlation")
    raw = P.raw(data["PrepareRequest"])
    require(raw[:4] == b"GTM1" and len(raw) <= 4096, "bounded factory diagnostic input")
    input_ = F.decoded(raw[4:])
    require(type(input_) is dict and set(input_) == {"Version", "Cookie", "FamilyCookie", "RangeID", "Target", "Pages", "Rows", "Bytes"}
            and input_["Version"] == 1 and input_["RangeID"] == 1 and input_["Target"] == 8
            and input_["Rows"] == 8 and input_["Bytes"] == 16384
            and input_["Pages"] == (1 if data["Recipe"]["Phase"] == 9 else 128), "exact admitted move factory parameters")
    for field in ("Version", "RangeID", "Target", "Pages", "Rows", "Bytes"):
        integer(input_[field], minimum=1)
    require(fixed(input_["FamilyCookie"], 16, True) == fixed(data["FactoryCookie"], 16, True), "same live factory owner cookie")
    fixed(input_["Cookie"], 16, True)
    wire = P.raw(data["Intent"])
    c = P.Cursor(P.body(wire, b"GMI\1"))
    request, source, target, actual_publication = c.take(16), c.take(120), c.take(120), c.field(256)
    epoch, pages, rows, size = c.number(), c.number(4), c.number(4), c.number(4)
    c.end()
    require(request == prepared["request"] and source == scopes[3] and target == scopes[8]
            and P.body(actual_publication, b"RPB\1") == publication and epoch > 0
            and (pages, rows, size) == (input_["Pages"], 8, 16384)
            and hashlib.sha256(wire).digest() == prepared["intent"], "complete genuine original intent/configuration/reservation")
    return prepared, epoch


def gate(data, records, scopes, prepared):
    phase, partition, page, side = recipe(data["Recipe"])
    g = data["Gate"]
    require(type(g) is dict and set(g) == set("Version Profile Cookie Run Member PID Sequence Control State DeadlineUnixNanos Proposal EntryData Target ProposalSHA256 Index Term Root Outcome CDC AttemptedWork PrepaidOutput".split()), "actual move gate grammar")
    require(g["Version"] == 1 and g["Profile"] == "native-live-submission-transition-v1" and g["State"] == "reached"
            and g["Member"] == (1 if partition == 3 else 4) and integer(g["PID"], minimum=1)
            and g["Sequence"] == 1 and g["PrepaidOutput"] == 512 << 10, "matched originating live child")
    integer(g["Version"], 1, 1); integer(g["Sequence"], 1, 1); integer(g["Member"], 6, 1)
    fixed(g["Cookie"], 16, True)
    require(fixed(g["Run"], 16, True) == fixed(data["Run"], 16, True)
            and integer(g["DeadlineUnixNanos"], (1 << 63)-1, 1) <= integer(data["DeadlineUnixNanos"], (1 << 63)-1, 1), "fixed source run/deadline")
    integer(g["AttemptedWork"], 1 << 50, 1)
    require(g["Control"] == dict(Version=1, Action="arm", Cookie=g["Cookie"], Sequence=1, Family="move", Phase=phase,
                                 Side=side, Identity=list(prepared["move"]), Partition=partition, Page=page), "exact selected original/phase/page")
    for field in ("Version", "Sequence", "Phase", "Partition", "Page"):
        integer(g["Control"][field])
    fixed(g["Control"]["Identity"], 16, True)
    wire = P.raw(g["Proposal"])
    require(0 < len(wire) <= 64 << 10 and hashlib.sha256(wire).digest() == fixed(g["ProposalSHA256"], 32), "complete selected native proposal")
    c = P.Cursor(P.body(wire, b"GMQ\1"))
    target, actual_phase, actual_page = c.take(120), c.number(1), c.number()
    intent, _payload = c.field(1024), c.field(64 << 10)
    proofs = c.number(4)
    require(proofs <= 2, "bounded genuine native phase proofs")
    for _ in range(proofs):
        proof = F.move_status(c.field(256))
        require((proof["move"], proof["intent"]) == (prepared["move"], prepared["intent"]), "foreign native phase proof")
    c.end()
    require(target == P.raw(g["Target"]) == scopes[partition] and actual_phase == phase and actual_page == page
            and intent == P.raw(data["Intent"]), "full scope/intent phase selection")
    root = g["Root"]
    require(type(root) is dict and set(root) == {"Generation", "Index", "Image", "ImageHash"}, "native applied root grammar")
    if side == "before-submit":
        require(g["Index"] == g["Term"] == root["Index"] == root["Generation"] == 0
                and not any(P.raw(g[field]) for field in ("EntryData", "Outcome", "CDC"))
                and not P.raw(root["Image"]) and fixed(root["ImageHash"], 32) == bytes(32), "before-submit invented durability")
    else:
        index = integer(g["Index"], minimum=1)
        integer(g["Term"], minimum=1); integer(root["Generation"], minimum=1)
        image = P.raw(root["Image"])
        require(root["Index"] == index and P.raw(g["EntryData"]) == wire and len(image) == 172
                and image[:3] == b"GR\3" and image[3] in (1, 2, 3) and image[4:20] == P.GRAPH and int.from_bytes(image[20:28], "big") == partition
                and fixed(root["ImageHash"], 32) == hashlib.sha256(image).digest()
                and hashlib.sha256(image[:-32]).digest() == image[-32:], "exact native Entry/root image")
        require(records.get((partition, index)) == dict(Partition=partition, Index=index, Outcome=g["Outcome"], CDC=g["CDC"]), "applied result not in actual complete prefix")
        status = F.move_status(P.raw(g["Outcome"]))
        require((status["partition"], status["phase"], status["result"], status["move"], status["intent"])
                == (partition, phase, 1, prepared["move"], prepared["intent"])
                and status["index"] <= index, "selected actual phase outcome")
        if partition == 8 and phase in (5, 7):
            # Seal/Activate commands name page0 but their real GMO retains the
            # completed import cursor. Do not relabel that durable progress0.
            earlier = [F.move_status(P.raw(row["Outcome"])) for (p, i), row in records.items()
                       if p == partition and i < status["index"] and P.raw(row["Outcome"])[:4] == b"GMO\1"]
            earlier = [row for row in earlier if row["move"] == prepared["move"] and row["intent"] == prepared["intent"]]
            require(earlier, "missing genuine prior target import/seal")
            previous = max(earlier, key=lambda row: row["index"])
            require(previous["phase"] == (4 if phase == 5 else 5) and previous["sequence"] == status["sequence"] > 0,
                    "Seal/Activate lost actual imported cursor")
        else:
            require(status["sequence"] == page, "actual command/phase cursor")
    return g


def census(read):
    require(type(read) is dict and set(read) == {"Cut", "Details", "Census"}
            and type(read["Census"]) is list and len(read["Census"]) == 11, "complete bounded actual ID census")
    next_id, owned = 1, 0
    for encoded in read["Census"]:
        wire = P.raw(encoded)
        owned += len(wire)
        require(owned <= 64 << 10, "whole-census owned bytes")
        c = P.Cursor(wire)
        require(c.take(4) == b"GSF1", "native observation batch codec")
        count = c.number(4)
        require(count == min(3, 33-next_id), "ordinary three-ID batch/no skipped suffix")
        for _ in range(count):
            owner = c.number()
            row = F.fact(c.field(8 << 10))
            require(owner == next_id, "actual complete ordered IDs1..32")
            if owner in (1, 3, 17):
                require(row["flags"] == 3 and row["life"] == owner+1 and row["labels"] == []
                        and row["incident"] == ([] if owner == 3 else [3])
                        and (row["source"], row["target"]) == ((1, 17) if owner == 3 else (0, 0)), "literal whole graph identity/life/endpoint answer")
            else:
                require(row == dict(flags=0, life=0, source=0, target=0, labels=[], incident=[]), "phantom actual entity")
            next_id += 1
        c.end()
    require(next_id == 33, "incomplete native entity census")


def check(data):
    require(type(data) is dict and set(data) == HEADER and data["Version"] == 1 and data["Profile"] == PROFILE, "fresh actual move artifact")
    integer(data["Version"], 1, 1)
    phase, _, _, _ = recipe(data["Recipe"])
    records = F.prefix(data)
    for name in ("Before", "Retained", "Recovered", "Reopened"):
        census(data[name])
    parsed = {name: F.range_read(data[name]) for name in ("Before", "Retained", "Recovered", "Reopened")}
    scopes, _, initial = parsed["Before"]
    require(all(initial[key]["provenance"] == 23 and initial[key]["labels"] == [] for key in ("A/fact/1", "B/fact/17")),
            "literal original typed property provenance/empty labels")
    publication, owner = F.placement(data["PlacementBefore"])
    prepared, epoch = original(data, scopes, publication)
    require(epoch == owner["epoch"] and owner["partition"] == 3 and owner["fenced"] == 0, "actual initial source owner")
    g = gate(data, records, scopes, prepared)
    # Reuse the actual kill/reopen grammar after this fresh move admission.
    spec = importlib.util.spec_from_file_location("native_move_transition_process", Path(__file__).with_name("native_transition_check.py"))
    transition = importlib.util.module_from_spec(spec); spec.loader.exec_module(transition)
    transition.logical_processes(data["Processes"], None, "move", g)
    pending, complete, replay = [F.move_result(data[name]) for name in ("Pending", "Complete", "Replay")]
    identity = lambda value: (value["scope"], value["request"], value["move"], value["intent"])
    require(identity(prepared) == identity(pending) == identity(complete) == identity(replay)
            and pending["pending"] == 1 and pending["committed"] == 0
            and complete["pending"] == replay["pending"] == 0 and complete["wires"] == replay["wires"], "same original across actual loss/reassociation/replay")
    _, originals = F.originals(data, records, {}, complete)
    for status in complete["statuses"]:
        if status is not None:
            require(originals.get((status["partition"], status["index"])) == status, "terminal receipt absent from immutable native prefix")
    require(data["Retained"] == data["Reopened"] == data["Before"] and parsed["Recovered"][2] == initial,
            "transfer/refusal changed exact typed graph or substituted old cut")
    require(len(data["PlacementAfter"]) == 6 and len(set(data["PlacementAfter"])) == 1, "complete all-six physical placement agreement")
    after_publication, after_owner = F.placement(data["PlacementAfter"][0])
    activations = {}
    for (partition, index), status in originals.items():
        if P.raw(records[partition, index]["CDC"]):
            require(partition not in activations, "duplicate original placement activation")
            activations[partition] = F.move_changes(P.raw(records[partition, index]["CDC"]), status)
    if phase == 9:
        require(complete["committed"] == replay["committed"] == 0 and complete["mask"] & 1 << 1 and replay["mask"] == complete["mask"]
                and all(status is not None and status["phase"] == 9 and status["sequence"] == 0 for status in complete["statuses"][:2])
                and complete["statuses"][2] is None and not activations
                and after_publication == publication and after_owner == owner, "actual bound-induced ABORT/undo changed placement or invented COMMIT")
    else:
        source, target, decision = complete["statuses"]
        require(complete["committed"] == replay["committed"] == 1 and complete["mask"] == replay["mask"] == 0
                and all(status is not None and status["result"] == 1 for status in (source, target, decision))
                and (source["partition"], source["phase"], target["partition"], target["phase"], decision["partition"], decision["phase"]) == (3, 8, 8, 8, 3, 6)
                and source["round"] == target["round"] == decision["round"] > 0
                and source["rows"] == target["rows"] > 0 and source["size"] == target["size"] > 0
                and source["digest"] == target["digest"] == decision["digest"] and any(source["digest"]), "actual complete immutable move decision/digest")
        phases = {(status["partition"], status["phase"]) for status in originals.values()}
        require(phases == {(3, phase) for phase in (1, 2, 3, 6, 7, 8)} | {(8, phase) for phase in (1, 2, 4, 5, 7, 8)},
                "complete source/target native phase census, no omitted/foreign stage")
        require(after_publication == publication and after_owner == dict(owner, partition=8, epoch=epoch+1)
                and activations == {3: epoch, 8: epoch}, "both original GML activation lineages/physical target ownership")
    observations = [dict(event=name, scope=["A", "B"], round=round_, values=values)
                    for name, (_, round_, values) in parsed.items()]
    witness, detail = P.oracle()({"initial": initial, "transactions": {}}, set(), {}, observations,
                               [dict(event="final", scope=["A", "B"], values=initial)])
    require(witness is not None, "independent transfer preserves business graph/cut: " + str(detail))
    return dict(profile=PROFILE, recipe=data["Recipe"], records=len(records), serialWitness=witness,
                scope="finite actual typed-owner transfer transition/restart; physical IO attribution and full-D runtime qualification remain separate")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    args = parser.parse_args()
    print(json.dumps(check(load(args.artifact)), indent=2))


if __name__ == "__main__":
    main()
