#!/usr/bin/env python3
"""Authenticated recorded native two-conflict/fence/real-transfer captures.

Recipes contain bounded scheduling actions, never retained proposal authority.
Only independently classified semantics AFTER complete original/CDC/transfer
authentication may seed minimization; a failed/refused protocol cannot qualify.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

SPEC = importlib.util.spec_from_file_location("native_composed_family", Path(__file__).with_name("native_fault_family_check.py"))
F = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(F)
SPEC = importlib.util.spec_from_file_location("native_composed_semantics", Path(__file__).with_name("native_transition_check.py"))
T = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(T)
P, C = F.P, F.C
require, integer, fixed = T.require, T.integer, T.fixed
PROFILE = "native-recorded-conflict-fence-transfer-v1"
SemanticFailure = T.SemanticFailure
HEADER = F.HEADER_KEYS | {"Replay"}
STAGES = {"held": 1, "settled": 2, "transferred": 3}


def recipe(value):
    require(type(value) is dict and set(value) == set("Version Seed First Phase FenceFirst Actions".split()), "exact composed recipe")
    integer(value["Version"], 1, 1); integer(value["Seed"], minimum=1)
    require(value["First"] in ("A", "B") and value["Phase"] in ("registered", "prepared") and type(value["FenceFirst"]) is bool
            and (value["First"] == "A" or value["FenceFirst"]), "six genuine mandatory causal orders")
    require(value["Actions"] is None or type(value["Actions"]) is list, "recorded action container")
    actions, previous, ticks, duplicates = value["Actions"] or [], 0, 0, 0
    require(len(actions) <= 8, "finite optional action count")
    for a in actions:
        require(type(a) is dict and set(a) == set("At Kind Original Member Ticks".split()) and a["At"] in STAGES, "exact recorded action")
        require(STAGES[a["At"]] >= previous, "mandatory stage order cannot be rewritten")
        previous = STAGES[a["At"]]
        if a["Kind"] == "tick":
            integer(a["Member"], 6, 1); ticks += integer(a["Ticks"], 3, 1)
            require(a["Original"] == "", "Tick has no original payload")
        else:
            require(a["Kind"] == "duplicate" and a["At"] != "held" and a["Original"] in ("A", "B"), "duplicate only an actual completed immutable original")
            integer(a["Member"], 0); integer(a["Ticks"], 0); duplicates += 1
    require(ticks <= 16 and duplicates <= 4, "whole-replay Tick/duplicate grants")
    return actions


def load(path):
    digest, total, header, footer, records, actions, phase = hashlib.sha256(), 0, None, None, [], [], 0
    with path.open("rb") as source:
        while line := source.readline((512 << 10)+1):
            total += len(line)
            require(len(line) <= 512 << 10 and total <= 16 << 20 and line.endswith(b"\n"), "bounded actual replay frame")
            row = T.decoded(line)
            require(type(row) is dict and footer is None, "row after EOF")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and row["Type"] == "header", "header row")
                header = row["Artifact"]
                require(type(header) is dict and set(header) == HEADER and header["Records"] is None and header["Snapshot"] is None and header["Model"] is None,
                        "fresh composed source grammar, no model substitution")
                require(type(header["Replay"]) is dict and header["Replay"].get("Actions") is None, "streamed recorded actions")
            elif row.get("Type") == "replay-action":
                require(phase == 0 and set(row) == {"Type", "Sequence", "Action"} and integer(row["Sequence"], 7) == len(actions) and len(actions) < 8, "complete ordered replay action frames")
                actions.append(row["Action"])
            elif row.get("Type") == "record":
                phase = 1
                require(set(row) == {"Type", "Record"} and len(records) < 512, "finite actual prefix")
                records.append(row["Record"])
            else:
                require(set(row) == {"Type", "Rows", "Through", "PayloadSHA256"} and row["Type"] == "footer", "EOF grammar")
                footer = row
                require(integer(row["Rows"], 512) == len(records) and row["PayloadSHA256"] == digest.hexdigest(), "complete action/prefix hash")
            if footer is None: digest.update(line)
    require(header is not None and footer is not None and header["Through"] == footer["Through"], "missing/foreign EOF")
    header["Records"], header["Replay"]["Actions"] = records, actions
    return header


def actions(data):
    owner = data["Replay"]
    require(type(owner) is dict and set(owner) == set("Recipe RecipeSHA256 Run DeadlineUnixNanos Actions".split()), "closed recorded owner grammar")
    expected = recipe(owner["Recipe"])
    fixed(owner["Run"], 16, True); integer(owner["DeadlineUnixNanos"], (1 << 63)-1, 1)
    canonical = json.dumps(owner["Recipe"], separators=(",", ":"), ensure_ascii=False).encode()
    require(fixed(owner["RecipeSHA256"], 32) == hashlib.sha256(canonical).digest(), "exact actual canonical recipe")
    r = owner["Recipe"]
    require(data["Case"] == f'explore-{r["First"]}-{r["Phase"]}-fence-{str(r["FenceFirst"]).lower()}' and data["Case"] in F.EXPLORATIONS, "recorded mandatory causal selector")
    require(type(owner["Actions"]) is list and len(owner["Actions"]) == len(expected), "complete actual action census")
    scopes, _ = P.cut({"Cut": data["Exploration"]["Before"]["Cut"]})
    e, instances, sequences = data["Exploration"], {}, {}
    coordinators = (8, 3) if r["First"] == "B" else (3, 8)
    descriptors = [C.decision_manifest(P.raw(wire), P.raw(manifest), coordinator, scopes, {"Cut": e["Before"]["Cut"]})
                   for wire, manifest, coordinator in zip(e["Originals"], e["Manifests"], coordinators)]
    require(len(descriptors) == 2, "two actual original action targets")
    decisions = {p: P.status(P.raw(row["Outcome"]))["index"] for row in data["Records"] if P.raw(row["Outcome"])[:4] == b"GCS\1"
                 and (p := row["Partition"]) in (3, 8) and P.status(P.raw(row["Outcome"]))["phase"] == 3 and P.status(P.raw(row["Outcome"]))["index"] == row["Index"]}
    require(set(decisions) == {3, 8}, "actual decisions delimit optional causal stages")
    complete = F.move_result(e["MoveComplete"])
    finishes = {s["partition"]: s["index"] for s in complete["statuses"][:2] if s is not None}
    require(set(finishes) == {3, 8}, "both genuine finished transfer originals")
    previous = data["From"]
    for i, (row, action) in enumerate(zip(owner["Actions"], expected)):
        require(type(row) is dict and set(row) == {"Sequence", "Action", "Before", "After", "Ticks", "Response"} and integer(row["Sequence"], 8, 1) == i+1 and row["Action"] == action, "actual admitted action/sequence")
        require(type(row["Before"]) is list and type(row["After"]) is list and len(row["Before"]) == len(row["After"]) == 2, "two actual leader checkpoints per action")
        for j, p in enumerate((3, 8)):
            before, after = integer(row["Before"][j]), integer(row["After"][j])
            require(previous[j] <= before <= after <= data["Through"][j], "monotone actual checkpoint/native prefix")
            require(after < decisions[p] if action["At"] == "held" else before >= (finishes[p] if action["At"] == "transferred" else decisions[p]), "action phase must join actual durable causal boundaries")
        previous = row["After"]
        if action["Kind"] == "tick":
            require(not P.raw(row["Response"]) and type(row["Ticks"]) is list and len(row["Ticks"]) == action["Ticks"], "exact genuine Tick count")
            member = action["Member"]
            for ordinal, tick in enumerate(row["Ticks"], 1):
                require(type(tick) is dict and set(tick) == {"Member", "Ordinal", "Before", "After", "Response"} and tick["Member"] == member and integer(tick["Ordinal"], 3, 1) == ordinal, "actual selected Tick receipt")
                before, after = integer(tick["Before"]), integer(tick["After"])
                raw = P.raw(tick["Response"]); require(0 < len(raw) <= 64 << 10, "funded Tick diagnostic frame")
                response = F.process(raw)
                require(response["code"] == response["mask"] == 0 and before <= response["applied"] <= after <= data["Through"][0 if member <= 3 else 1], "actual monotone native Tick/source prefix")
                require(member not in instances or instances[member] == response["incarnation"], "Tick source silently replaced")
                require(response["sequence"] > sequences.get(member, 0), "repeated native Tick response is not another action")
                instances[member], sequences[member] = response["incarnation"], response["sequence"]
        else:
            require(not row["Ticks"] and 0 < len(P.raw(row["Response"])) <= 64 << 10, "bounded actual original duplicate")
            index = 0 if action["Original"] == "A" else 1
            descriptor = descriptors[index]
            member = 2 if descriptor["coordinator"] == 3 else 5
            original, repeated = F.process(P.raw(e["Results"][index])), F.process(P.raw(row["Response"]))
            require(repeated["code"] == repeated["mask"] == 0 and not repeated["outcome"] and not repeated["changes"] and original["incarnation"] == repeated["incarnation"]
                    and repeated["sequence"] > max(original["sequence"], sequences.get(member, 0)), "same actual original caller, no duplicate response substitution")
            require(member not in instances or instances[member] == repeated["incarnation"], "duplicate replaced recorded caller")
            instances[member], sequences[member] = repeated["incarnation"], repeated["sequence"]
            result = P.Cursor(original["aux"])
            require(result.take(4) == b"GCR1", "original actual decision response")
            result.field(156); result.take(16+16+32); require(result.number(1) == 0, "completed original action target")
            decision = result.field(256); result.end()
            P.resumed(row["Response"], scopes, descriptor["source"]["request"], descriptor["tx"], P.business_hash(descriptor["wire"], descriptor["coordinator"]), 0, decision)


def check(data):
    require(type(data) is dict and set(data) == HEADER and data["Version"] == 2 and data["Profile"] == PROFILE and data["Case"] in F.EXPLORATIONS, "fresh genuine composed replay profile")
    integer(data["Version"], 2, 2)
    actions(data)
    proof = F.check_source(data, semantic_require=T.semantic)
    proof["scope"] = "six mandatory genuine two-conflict/fence/real-transfer schedules plus bounded recorded native actions; semantic-only fresh-cohort deletion is conditional, no global minimum, infinite-state or runtime qualification claim"
    return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    args = parser.parse_args()
    try:
        print(json.dumps(dict(classification="passed", proof=check(load(args.artifact))), indent=2))
    except T.SemanticFailure as failure:
        print(json.dumps(dict(classification="semantic-failure", code=failure.code, detail=failure.detail[:4096])), file=sys.stderr)
        raise SystemExit(3) from failure
    except (ValueError, KeyError, TypeError, OSError) as failure:
        print(json.dumps(dict(classification="source-refused", diagnostic=str(failure)[:4096])), file=sys.stderr)
        raise SystemExit(2) from failure


if __name__ == "__main__":
    main()
