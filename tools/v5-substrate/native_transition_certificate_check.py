#!/usr/bin/env python3
"""First genuine GSC publication, distinct business cuts and complete prefixes.

The native parent reads the actual stored certificates through At. This bounded
offline bridge checks their original-index lineage, actual child replacement,
literal node answers and the independent serial witness. A held GSQ or one
participant's applied GSO never supplies a missing full GraphCut.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("native_certificate_components", Path(__file__).with_name("native_transition_cut_check.py"))
Q = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(Q)
T, C, P = Q.T, Q.C, Q.P
require, integer, fixed = T.require, T.integer, T.fixed
PROFILE = "native-first-certificate-transition-v1"
HEADER = set("Version Profile Case Recipe Run DeadlineUnixNanos Gate KillState ReadRequest ReadFailure ReadFailureMask Initial Before Current Replay Retained Reopened ReopenedCurrent RejoinedCurrent Originals Processes From Through Records".split())


def recipe(value):
    phase, partition, side = Q.recipe(value)
    require(phase == 2, "first certificate selector cannot be a fence")
    return partition, side


def load(path):
    digest, total, header, footer, rows = hashlib.sha256(), 0, None, None, []
    with path.open("rb") as source:
        while line := source.readline((512 << 10)+1):
            total += len(line)
            require(len(line) <= 512 << 10 and total <= 16 << 20 and line.endswith(b"\n"), "bounded actual source frame")
            row = T.decoded(line)
            require(type(row) is dict and footer is None, "row after EOF")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and row["Type"] == "header", "header row")
                header = row["Artifact"]
                require(type(header) is dict and set(header) == HEADER and header["Records"] is None, "fresh first-publication grammar")
            elif row.get("Type") == "record":
                require(set(row) == {"Type", "Record"} and len(rows) < 512, "finite native prefix")
                rows.append(row["Record"])
            else:
                require(set(row) == {"Type", "Rows", "Through", "PayloadSHA256"} and row["Type"] == "footer", "EOF grammar")
                footer = row
                require(integer(row["Rows"], 512) == len(rows) and row["PayloadSHA256"] == digest.hexdigest(), "prefix hash/census")
            if footer is None: digest.update(line)
    require(header is not None and footer is not None and footer["Through"] == header["Through"], "missing/foreign EOF")
    header["Records"] = rows
    return header


def control(encoded):
    c = P.Cursor(P.body(P.raw(encoded), b"GSO\1"))
    result = dict(phase=c.number(1), status=c.number(1), round=c.number(), index=c.number())
    c.end()
    require(result["phase"] in (1, 2) and result["status"] == 1 and 0 < result["round"] < (1 << 64)-1 and result["index"] > 0,
            "actual successful cut control, never missing or refused publication")
    return result


def certificate(encoded):
    c = P.Cursor(P.body(P.raw(encoded), b"GSC\1"))
    result = dict(scope=c.take(120), configuration=c.take(32), round=c.number(), index=c.number(), sourceRound=c.number(), sourceIndex=c.number(), image=c.take(172))
    c.end()
    b, image = result["scope"], result["image"]
    partition = int.from_bytes(b[16:24], "big")
    require(b[:16] == P.GRAPH and partition in (3, 8) and any(b[24:40]) and int.from_bytes(b[40:48], "big") > 0
            and int.from_bytes(b[48:56], "big") > 0 and any(b[56:88]) and any(b[88:120])
            and any(result["configuration"]) and 0 < result["round"] < (1 << 64)-1
            and result["sourceRound"] <= result["round"] and 0 < result["sourceIndex"] < result["index"], "complete certificate/source coordinates")
    # graphstore.DecodeRoot/Root.validate: GR3 is the declared-ownership layout,
    # with modes3 pending,4 published,5 initialized (not primitive GR1/single GR2).
    # Its final32 bytes are SHA256, not a physical/certified-cut authority.
    require(image[:3] == b"GR\3" and image[3] in (3, 4, 5) and image[4:20] == P.GRAPH and int.from_bytes(image[20:28], "big") == partition
            and hashlib.sha256(image[:-32]).digest() == image[-32:], "actual native root image codec/namespace")
    owner, epoch, next_ = (int.from_bytes(image[offset:offset+8], "big") for offset in (28, 36, 44))
    topology, schema, index = (int.from_bytes(image[offset:offset+8], "big") for offset in (84, 92, 100))
    require(owner > 0 and next_ > 0 and any(image[52:84]) and topology > 0 and schema == 1 and any(image[108:140])
            and index == (3 if image[3] == 5 else 0), "actual declared-root scalar/topology fields")
    require(owner == int.from_bytes(b[40:48], "big") and topology == int.from_bytes(b[48:56], "big")
            and image[108:140] == b[56:88], "native root exact certificate ownership/declaration binding")
    result["partition"] = partition
    return result


def first_publication(records, partition, round_):
    indexes = []
    for (actual, index), row in records.items():
        if actual != partition or P.raw(row["Outcome"])[:4] != b"GSO\1": continue
        s = control(row["Outcome"])
        require(s["index"] == index and not P.raw(row["CDC"]), "cut reply/native entry index")
        if (s["phase"], s["round"]) == (2, round_): indexes.append(index)
    require(indexes, "missing actual first certificate publication")
    return min(indexes), indexes


def stored_publication(cert, records):
    first, indexes = first_publication(records, cert["partition"], cert["round"])
    require(cert["index"] == first, "stored certificate must keep its first actual publication index")
    return indexes


def interrupted(response, mask):
    # Preserve the actual GPR1 native-cause mask captured by the parent. A
    # printable generic process error alone is not evidence of interruption.
    allowed = (1 << 3) | (1 << 10) | (1 << 11) | (1 << 12) | (1 << 13)
    require(type(mask) is int and 0 < mask <= 65535 and not mask & ~allowed and response["code"] in (3, 6)
            and not response["mask"] & ~(1 << 7) and not any(response[k] for k in ("aux", "image", "outcome", "changes")),
            "invalid/limit/corrupt/unexpected error cannot qualify interruption or return source authority")


def prefix(data, originals):
    records, phases = {}, {o["tx"]: {} for o in originals}
    require(type(data["Records"]) is list and len(data["Records"]) <= 512, "finite actual record inventory")
    for row in data["Records"]:
        require(type(row) is dict and set(row) == {"Partition", "Index", "Outcome", "CDC"}, "exact native entry fields")
        key = integer(row["Partition"], 8, 3), integer(row["Index"], minimum=1)
        require(key[0] in (3, 8) and key not in records, "duplicate/foreign native entry")
        records[key] = row
    require(type(data["From"]) is list and type(data["Through"]) is list and len(data["From"]) == len(data["Through"]) == 2, "full participant prefix vectors")
    for i, partition in enumerate((3, 8)):
        lo, hi = integer(data["From"][i], minimum=1), integer(data["Through"][i], minimum=1)
        require(lo <= hi and hi-lo <= 256 and [r["Index"] for r in data["Records"] if r["Partition"] == partition] == list(range(lo+1, hi+1)), "complete exact native prefix")
    for (partition, index), row in records.items():
        wire, cdc = P.raw(row["Outcome"]), P.raw(row["CDC"])
        if not wire:
            require(not cdc, "empty application record emitted changes")
            continue  # Go obtained both actual retained ApplicationRecord outputs.
        if wire[:4] == b"GSO\1":
            require(control(row["Outcome"])["index"] == index and not cdc, "actual control outcome binding")
            continue
        s = P.status(wire)
        found = [o for o in originals if o["tx"] == s["tx"]]
        require(len(found) == 1, "unknown business original in complete prefix")
        original = found[0]
        require(s["partition"] == partition and s["digest"] == original["digest"] and 0 < s["index"] <= index, "actual original request/manifest/entry binding")
        if s["index"] < index:
            require(not cdc and records.get((partition, s["index"]), {}).get("Outcome") == row["Outcome"], "later replay lacks retained earlier original")
            continue
        key, statuses = (partition, s["phase"]), phases[s["tx"]]
        require(key not in statuses and (s["phase"] != 2 or s["yes"]) and (s["phase"] < 3 or s["commit"]), "genuine applied original phase")
        require(bool(cdc) == (s["phase"] == 4), "complete business CDC only on original Resolve")
        if cdc:
            c = P.Cursor(P.body(cdc, b"GCE\1"))
            require(c.number(1) == 6 and c.take(16) == original["tx"] and c.take(32) == original["digest"] and c.number() == index, "actual original CDC identity")
            logical = P.body(c.field(), b"GCD\5"); c.end()
            require(logical[:16] == P.GRAPH and int.from_bytes(logical[16:24], "big") == partition, "complete CDC namespace")
        statuses[key] = s
    for original, receipt in zip(originals, data["Originals"]):
        statuses, coordinator = phases[original["tx"]], original["coordinator"]
        require(set(statuses) == {(coordinator, 1), (3, 2), (8, 2), (coordinator, 3), (3, 4), (8, 4)}, "complete original register/vote/decision/participant effects")
        decision = statuses[coordinator, 3]
        require(P.raw(receipt["Decision"]) == P.raw(records[coordinator, decision["index"]]["Outcome"]) and decision["commit"]
                and statuses[3, 4]["round"] == statuses[8, 4]["round"] == decision["round"], "immutable actual COMMIT and both effects")
        original["decision"], original["resolves"] = decision, {p: statuses[p, 4] for p in (3, 8)}
    return records


def literal(observation, stage, rounds):
    native = observation["Native"]
    expected_ids = [1, 17][:stage]
    require(native["observationScope"] == "graph-complete-in-issued-range" and all(type(e["found"]) is bool for e in native["entityCensus"])
            and [e["id"] for e in native["entityCensus"]] == list(range(1, 33))
            and [e["id"] for e in native["entityCensus"] if e["found"]] == expected_ids, "literal complete entity census, no current substitution")
    require([e["id"] for e in native["entities"]] == [1, 17, 3] and len(native["lives"]) == 4 and len(native["phantomEntities"]) == 2
            and all(not e["found"] for e in native["phantomEntities"]), "literal bounded entities/lives/phantoms")
    expected = {}
    for e in native["entities"]:
        owner, found = e["id"], e["id"] in expected_ids
        require(type(e["found"]) is bool and e["found"] == found, "literal entity existence")
        if not found: continue
        provenance, start = (1, "A") if owner == 1 else (2, "B")
        require(e["kind"] == 1 and (e["source"], e["target"], e["mode"], e["type"]) == (0, 0, 0, "") and e["effective"] is True
                and e["effectiveLife"] == owner+1 and not e["labels"], "literal node type/effectiveness/empty labels")
        presence = dict(present=True, null=False, handle=owner+1, revision=rounds[provenance-1], provenance=provenance, kind="life", i64=0)
        require(e["presence"] == presence and not e["property"]["present"], "literal original node presence/revision/provenance")
        expected.update({f"{start}/entity/{owner}": dict(kind=1, source=0, target=0, mode=0, type=""), f"{start}/presence/{owner}": presence,
                         f"{start}/labels/{owner}": [], f"{start}/life/{owner}/{owner+1}": dict(sourceLife=0, targetLife=0)})
    require([(l["owner"], l["life"]) for l in native["lives"]] == [(1, 2), (1, 7), (17, 18), (3, 4)], "exact life read domain")
    for life in native["lives"]:
        require(type(life["found"]) is bool and life["found"] == (life["owner"] in expected_ids and life["life"] == life["owner"]+1)
                and life["sourceLife"] == life["targetLife"] == 0, "literal life/phantom binding")
    require([(p["endpoint"], p["life"]) for p in native["postings"]] == [(1, 0), (1, 2), (1, 7), (17, 0), (17, 18)], "exact endpoint query domain")
    for posting in native["postings"]:
        require(posting["candidates"] == posting["declared"] == posting["effective"] == [], "no phantom relationship postings")
        expected[f'{"A" if posting["endpoint"] == 1 else "B"}/posting/{posting["endpoint"]}/{posting["life"]}'] = []
    T.semantic(P.projected(observation) == expected, "literal-node-state")
    return expected


def check(data):
    require(type(data) is dict and set(data) == HEADER and data["Version"] == 1 and data["Profile"] == PROFILE, "fresh first-certificate artifact")
    integer(data["Version"], 1, 1)
    partition, side = recipe(data["Recipe"])
    require(data["Case"] == f"first-certify-p{partition}-{side}", "exact selected first-publication case")
    scopes, old_round = P.cut(data["Before"])
    require(type(data["Originals"]) is list and len(data["Originals"]) == 2, "two genuine distinct business originals")
    originals = []
    for i, value in enumerate(data["Originals"]):
        require(type(value) is dict and set(value) == {"Coordinator", "Proposal", "Manifest", "Decision"} and value["Coordinator"] == (3 if i == 0 else 8), "exact original coordinator inventory")
        integer(value["Coordinator"], 8, 3)
        originals.append(C.decision_manifest(P.raw(value["Proposal"]), P.raw(value["Manifest"]), value["Coordinator"], scopes, data["Before"]))
    require(originals[0]["tx"] != originals[1]["tx"] and originals[0]["source"]["request"] != originals[1]["source"]["request"], "independent actual request identities")
    records = prefix(data, originals)
    rounds = [o["decision"]["round"] for o in originals]
    require(0 < P.cut(data["Initial"])[1] < rounds[0] <= old_round < rounds[1], "real earlier/later business state, no artificial round increment")
    g = Q.gate(data)
    T.logical_processes(data["Processes"], None, data["Case"], g)
    killed = T.decoded(P.raw(data["Processes"][0]["Raw"]))
    state = C.process_response(P.raw(data["KillState"]))
    require(state["code"] == state["mask"] == 0 and killed["Instance"] == f'voter{g["Member"]}/pid{g["PID"]}/{state["incarnation"].hex()}', "actual selected prekill state/incarnation")
    command = P.body(P.raw(g["Proposal"]), b"GSQ\1")
    require(int.from_bytes(command[-8:], "big") == rounds[1], "first Certify uses actual later installed floor")
    request = P.Cursor(P.body(P.raw(data["ReadRequest"]), b"GEQ\1"))
    require(request.number(1) == 1 and request.take(16) == bytes(16) and request.number() == request.number(4) == 0, "actual Fresh, no caller-created cut")
    request.end()
    failure = C.process_response(P.raw(data["ReadFailure"]))
    interrupted(failure, data["ReadFailureMask"])
    for name, business in (("Initial", None), ("Before", originals[0]), ("Current", originals[1])):
        actual_scopes, actual_round = P.cut(data[name])
        require(actual_scopes == scopes, "full configured cut participant scopes")
        for encoded in data[name]["Cut"]["Certificates"]:
            cert = certificate(encoded)
            p = cert["partition"]
            indexes = stored_publication(cert, records)
            first = cert["index"]
            require(cert["scope"] == scopes[p] and cert["configuration"] == originals[0]["configuration"] == originals[1]["configuration"]
                    and cert["index"] == first and data["From"][(3, 8).index(p)] < first <= data["Through"][(3, 8).index(p)], "actual first GSO index bound to stored GSC")
            if business is None:
                require(cert["sourceRound"] == 0, "empty initial source must precede real business")
            else:
                require(cert["sourceRound"] == business["decision"]["round"] and cert["sourceIndex"] == business["resolves"][p]["index"], "genuine complete installed source prefix, no current substitution")
            if name == "Current":
                require(actual_round == rounds[1] and len(indexes) >= 2, "real first publication followed by repeated native Certify replies")
                if p == partition:
                    require(first == g["Index"] <= state["applied"] if side == "after-applied" else first > state["applied"], "selected actual first publication before/after kill")
    require(data["Current"] == data["Replay"] == data["RejoinedCurrent"] and data["Before"] == data["Retained"] == data["Reopened"], "stored certificate stability and exact retained old source")
    require(data["ReopenedCurrent"] == data["Current"] if side == "after-applied" else data["ReopenedCurrent"] is None,
            "local preheal certificate proof must not synthesize missing participant authority")
    states = {"Initial": 0, "Before": 1, "Current": 2, "Replay": 2, "Retained": 1, "Reopened": 1, "RejoinedCurrent": 2}
    if data["ReopenedCurrent"] is not None: states["ReopenedCurrent"] = 2
    values = {name: literal(data[name], stage, rounds) for name, stage in states.items()}
    transactions = {}
    for name, start, owner, before, after in (("node-a", "A", 1, "Initial", "Before"), ("node-b", "B", 17, "Before", "Current")):
        keys = tuple(f"{start}/{kind}/{owner}" for kind in ("entity", "presence", "labels")) + (f"{start}/life/{owner}/{owner+1}",)
        transactions[name] = dict(reads=[P.read_key(values[before], f"{start}/entity/{owner}")], writes=P.writes(values[before], values[after], keys))
    observations = [dict(event=name, scope=["A", "B"], round=P.cut(data[name])[1], values=values[name]) for name in states]
    witness, detail = P.oracle()({"initial": values["Initial"], "transactions": transactions}, set(transactions), dict(zip(("node-a", "node-b"), rounds)), observations,
                                [dict(event="final", scope=["A", "B"], values=values["Current"])])
    T.semantic(witness is not None, "independent exact serial/cut witness: "+str(detail))
    return dict(case=data["Case"], records=len(records), serial_witness=witness,
                scope="four finite first-GSC logical publication sides with distinct genuine business states and native same-directory recovery; local preheal proof is not a synthetic full cut; physical namespace IO, composed seeds and runtime qualification remain separate")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    args = parser.parse_args()
    print(json.dumps(check(load(args.artifact)), indent=2))


if __name__ == "__main__":
    main()
