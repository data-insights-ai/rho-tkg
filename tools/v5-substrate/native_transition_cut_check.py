#!/usr/bin/env python3
"""Finite actual Fresh cut gates with a concurrent whole transaction.

A held or interrupted GSQ is not a returned certificate. The real old read,
new read, immutable business original and complete native prefix are checked
independently; these diagnostic frames cannot supply cut authority.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("native_cut_transition", Path(__file__).with_name("native_transition_check.py"))
T = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(T)
C, P = T.C, T.C.P
require, integer, fixed = T.require, T.integer, T.fixed
PROFILE = "native-owned-cut-transition-v1"
HEADER = T.HEADER | set("Recipe Gate HeldAfter Run DeadlineUnixNanos ReadRequest ReadFailure Concurrent OriginalRetry".split())


def recipe(value):
    require(type(value) is dict and set(value) == {"Version", "Phase", "Partition", "Side"}, "exact cut recipe")
    integer(value["Version"], 1, 1)
    phase, partition = integer(value["Phase"], 2, 1), integer(value["Partition"], 8, 3)
    require(partition in (3, 8) and value["Side"] in ("before-submit", "after-applied"), "finite actual participant/side")
    return phase, partition, value["Side"]


def load(path):
    digest, total, header, footer, records = hashlib.sha256(), 0, None, None, []
    with path.open("rb") as source:
        while line := source.readline((512 << 10)+1):
            total += len(line)
            require(len(line) <= 512 << 10 and total <= 16 << 20 and line.endswith(b"\n"), "bounded actual source frame")
            row = T.decoded(line)
            require(type(row) is dict and footer is None, "row after EOF")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and row["Type"] == "header", "header row")
                header = row["Artifact"]
                require(type(header) is dict and set(header) == HEADER and header["Records"] is None, "fresh cut header")
            elif row.get("Type") == "record":
                require(set(row) == {"Type", "Record"} and len(records) < 512, "finite original prefix")
                records.append(row["Record"])
            else:
                require(set(row) == {"Type", "Rows", "Through", "PayloadSHA256"} and row["Type"] == "footer", "EOF grammar")
                footer = row
                require(row["Rows"] == len(records) and row["PayloadSHA256"] == digest.hexdigest(), "complete prefix hash/census")
            if footer is None:
                digest.update(line)
    require(header is not None and footer is not None and footer["Through"] == header["Through"], "missing/foreign EOF")
    header["Records"] = records
    return header


def gate(data):
    phase, partition, side = recipe(data["Recipe"])
    g = data["Gate"]
    require(type(g) is dict and set(g) == set("Version Profile Cookie Run Member PID Sequence Control State DeadlineUnixNanos Proposal EntryData Target ProposalSHA256 Index Term Root Outcome CDC AttemptedWork PrepaidOutput".split()), "actual cut gate grammar")
    integer(g["Version"], 1, 1); integer(g["Sequence"], 1, 1)
    require(g["Profile"] == T.LOGICAL_PROFILE and g["State"] == "reached" and g["Member"] == (1 if partition == 3 else 4), "matched actual originating source")
    integer(g["Member"], 6, 1); integer(g["PID"], minimum=1)
    fixed(g["Cookie"], 16, True)
    require(fixed(g["Run"], 16, True) == fixed(data["Run"], 16, True)
            and integer(g["DeadlineUnixNanos"], (1 << 63)-1, 1) <= integer(data["DeadlineUnixNanos"], (1 << 63)-1, 1), "same run/fixed owner deadline")
    integer(g["AttemptedWork"], 1 << 50, 1)
    require(g["PrepaidOutput"] == 512 << 10, "prepaid owned gate frame")
    require(g["Control"] == dict(Version=1, Action="arm", Cookie=g["Cookie"], Sequence=1, Family="cut", Phase=phase,
                                 Side=side, Identity=[0]*16, Partition=partition, Page=0), "exact selected cut phase")
    for field in ("Version", "Sequence", "Phase", "Partition", "Page"):
        integer(g["Control"][field])
    fixed(g["Control"]["Identity"], 16)
    proposal = P.raw(g["Proposal"])
    require(len(proposal) <= 512 and hashlib.sha256(proposal).digest() == fixed(g["ProposalSHA256"], 32), "actual complete cut command")
    c = P.Cursor(P.body(proposal, b"GSQ\1"))
    actual_phase, scope, configuration, round_ = c.number(1), c.take(120), c.take(32), c.number()
    c.end()
    scopes, old_round = P.cut(data["Before"])
    require(actual_phase == phase and scope == scopes[partition] == P.raw(g["Target"]) and old_round <= round_ < (1 << 64)-1,
            "selected actual full scope/source-observed round")
    for encoded in data["Before"]["Cut"]["Certificates"]:
        require(P.body(P.raw(encoded), b"GSC\1")[120:152] == configuration and any(configuration), "same genuine configuration")
    root = g["Root"]
    require(type(root) is dict and set(root) == {"Generation", "Index", "Image", "ImageHash"}, "native root grammar")
    if side == "before-submit":
        require(g["Index"] == g["Term"] == root["Index"] == root["Generation"] == 0
                and not any(P.raw(g[field]) for field in ("EntryData", "Outcome", "CDC"))
                and not P.raw(root["Image"]) and fixed(root["ImageHash"], 32) == bytes(32), "unsubmitted command invented durability")
    else:
        index = integer(g["Index"], minimum=1)
        integer(g["Term"], minimum=1); integer(root["Generation"], minimum=1)
        image = P.raw(root["Image"])
        require(root["Index"] == index and P.raw(g["EntryData"]) == proposal
                and len(image) == 172 and image[:3] == b"GR\3" and image[3] in (1, 2, 3) and image[4:20] == P.GRAPH
                and int.from_bytes(image[20:28], "big") == partition and hashlib.sha256(image[:-32]).digest() == image[-32:]
                and fixed(root["ImageHash"], 32) == hashlib.sha256(image).digest(), "exact actual native Entry/root")
        record = [r for r in data["Records"] if (r["Partition"], r["Index"]) == (partition, index)]
        require(len(record) == 1 and record[0]["Outcome"] == g["Outcome"] and record[0]["CDC"] == g["CDC"], "actual applied result absent from complete prefix")
        result = P.Cursor(P.body(P.raw(g["Outcome"]), b"GSO\1"))
        require((result.number(1), result.number(1), result.number(), result.number()) == (phase, 1, round_, index)
                and not P.raw(g["CDC"]), "actual cut control result, never business CDC")
        result.end()
    return g


def check(data):
    require(type(data) is dict and set(data) == HEADER and data["Version"] == 3 and data["Profile"] == PROFILE, "fresh actual cut artifact")
    integer(data["Version"], 3, 3)
    phase, partition, side = recipe(data["Recipe"])
    require(data["Case"] == f"cut-{phase}-p{partition}-{side}" and data["IO"] is None
            and not any(data[field] for field in ("PendingCaller", "PartialReads", "PacketEvents", "Refused", "RefusedReplay")), "exact finite cut case")
    g = gate(data)
    require(data["HeldAfter"] == g, "selected actual cut owner stopped holding before concurrent decision")
    T.logical_processes(data["Processes"], None, data["Case"], g)
    request = P.Cursor(P.body(P.raw(data["ReadRequest"]), b"GEQ\1"))
    require(request.number(1) == 1 and request.take(16) == bytes(16) and request.number() == request.number(4) == 0, "genuine Fresh request, no caller cut")
    request.end()
    failure = C.process_response(P.raw(data["ReadFailure"]))
    require(failure["code"] in (3, 6) and not any(failure[key] for key in ("aux", "image", "outcome", "changes")),
            "interrupted Fresh returned a certificate or decision")
    scopes, _ = P.cut(data["Before"])
    original = P.pgq(P.raw(data["Original"]))
    manifest = P.Cursor(P.body(P.raw(data["Manifest"]), b"GCM\1"))
    tx, coordinator = manifest.take(16), manifest.number()
    require(coordinator == 8, "actual concurrent coordinator")
    decisions = [P.raw(row["Outcome"]) for row in data["Records"] if P.raw(row["Outcome"])[:4] == b"GCS\1"
                 and P.status(P.raw(row["Outcome"]))["phase"] == 3 and P.status(P.raw(row["Outcome"]))["index"] == row["Index"]]
    require(len(decisions) == 1, "one immutable concurrent decision")
    for field in ("Concurrent", "OriginalRetry"):
        P.resumed(data[field], scopes, original["request"], tx, P.business_hash(P.raw(data["Original"]), 8), 0, decisions[0])
    result = C.check_history(data, process_check=lambda rows, receipt, case: T.logical_processes(rows, receipt, case, g), semantic_require=T.semantic)
    T.literal_edge_answers(data)
    result["scope"] = "eight finite genuine Fresh cut-call/side cells, including repeated-round controls, with concurrent whole transaction and retained old source; no new-certificate, physical IO or full-D/runtime qualification claim"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    args = parser.parse_args()
    print(json.dumps(check(load(args.artifact)), indent=2))


if __name__ == "__main__":
    main()
