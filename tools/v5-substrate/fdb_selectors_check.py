#!/usr/bin/env python3
"""Private actual selector/source oracle; no runtime or capacity qualification.

All dataset-sized joins live in ONE caller-granted disposable BLOB spool. Native
round enumeration, business provenance and completion order remain independent.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys

# The older private raw codec is intentionally still in fdb/. Load its exact
# source location, without requiring callers to mutate their import path.
_spec = importlib.util.spec_from_file_location("selectors_raw_codec", Path(__file__).resolve().parent / "fdb/fdb_contention_check.py")
raw = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(raw)

codec = raw._codec
require, fields, integer = codec.require, codec.fields, codec.integer
same, go_json, blob = codec.same, codec.go_json, codec.raw_bytes
decoded, lines, key, prefix, raw_record = raw.decoded, raw.lines, raw.key, raw.prefix, raw.raw_record
Spool, Policy, u64 = raw.Spool, raw.Policy, raw.u64
FORMAT, FAMILY = "fdb-snapshot-selectors-v1", "snapshot-selectors"
POLICY = Policy(1 << 30, 8 << 20, 4096, 512 << 10)


def operation(p, value):
    fields(value, "Dataset Phase Fact Ordinal")
    ordinal = codec.parallel_operation({**p, "version": 1}, value)
    phase, fact, _, _ = codec.fixture(p, ordinal)
    return ordinal, phase, fact


def attempt(a, end, spool, request_prefix="selectors"):
    fields(a, "Operation Engine ClientID AttemptOrdinal PreviousRequest Request Transaction BodySHA256 Proposal Submissions ReadCallbacks Classification Error Originals Original")
    p, c = end["Spec"], end["Config"]
    ordinal, phase, fact = operation(p, a["Operation"])
    client, number = integer(a["ClientID"], 0, p["writers"] - 1), integer(a["AttemptOrdinal"], 1, 1 if phase == "setup" else 64)
    prior_raw = spool.find(b"attempt-chain", u64(ordinal))
    prior = decoded(prior_raw) if prior_raw is not None else None
    require((prior is None and number == 1 and a["PreviousRequest"] == "") or
            prior is not None and prior["Classification"] == "aborted" and number == prior["AttemptOrdinal"] + 1 and a["PreviousRequest"] == prior["Request"] and client == prior["ClientID"], "new request only after genuine previous ABORT")
    body = blob(a["Proposal"])
    require(0 < len(body) <= 64 << 10 and codec.sha_array(a["BodySHA256"]) == hashlib.sha256(body).digest(), "frozen immutable body digest")
    tx = decoded(body)
    fields(tx, "Graph Topology ID Request Coordinator Dependency Participants GraphChange Lifecycle")
    command, lifecycle, effects, reads = codec.fdb_contract(p, ordinal)
    name = f"{request_prefix}-{phase}-{fact}-c{client}-a{number}"
    owner = 1 if phase == "setup" and fact == 2 else 0
    require(a["Engine"] == "foundationdb" and a["Error"] == "" and tx["Graph"] == c["Graph"] and tx["Topology"] == c["Topology"] and tx["ID"] == a["Transaction"] == name and tx["Request"] == a["Request"] == "request/" + name and tx["Dependency"] == 0 and tx["Coordinator"] == owner and same(tx["GraphChange"], command) and same(tx["Lifecycle"], lifecycle), "actual family/namespace/command/provenance/request")
    require([part["Group"] for part in tx["Participants"]] == sorted(effects), "complete actual participants")
    for part in tx["Participants"]:
        fields(part, "Group Epoch Generation Reads Effects")
        g = integer(part["Group"], 0, 1)
        require(part["Epoch"] == c["Epochs"][g] and same(part["Effects"], effects[g]) and [r["Key"] for r in part["Reads"]] == reads[g], "immutable read/effect footprint")
        integer(part["Generation"])
        for read in part["Reads"]:
            fields(read, "Key Version")
            integer(read["Version"])
    canonical = {"Graph": c["Graph"], "Topology": c["Topology"], "ID": name, "Request": "request/" + name, "Coordinator": owner, "Dependency": 0,
                 "Participants": [{"Group": part["Group"], "Epoch": part["Epoch"], "Generation": part["Generation"], "Reads": [{"Key": r["Key"], "Version": r["Version"]} for r in part["Reads"]], "Effects": effects[part["Group"]]} for part in tx["Participants"]], "GraphChange": command, "Lifecycle": lifecycle}
    require(body == go_json(canonical), "canonical actual Go proposal bytes")
    require(len(a["Originals"]) == 2, "both genuine immutable originals")
    o = decoded(blob(a["Originals"][0]["Value"]))
    fields(o, "Commit Round Digest ID Request Reason Coordinator Sequence")
    require(type(o["Commit"]) is bool and o["ID"] == name and o["Request"] == a["Request"] and o["Coordinator"] == owner and o["Digest"] == a["BodySHA256"], "paired original authenticates exact body")
    for record, kind, identity in zip(a["Originals"], ("id", "request"), (name, a["Request"]), strict=True):
        raw_record(record, key(c, owner, kind, identity), go_json(o))
        spool.insert(b"attempt-" + kind.encode(), blob(record["Key"]), blob(record["Value"]))
        spool.insert(b"attempt-membership-" + kind.encode(), blob(record["Key"]), u64(ordinal))
    require(type(a["Submissions"]) is list and 1 <= len(a["Submissions"]) <= 8, "finite SAME-wire submissions")
    integer(a["ReadCallbacks"], 1 + len(a["Submissions"]), 16 * (1 + len(a["Submissions"])))
    for invocation, submit in enumerate(a["Submissions"], 1):
        fields(submit, "Ordinal Callbacks Outcome Error Retryable MaybeCommitted")
        require(type(submit["Ordinal"]) is int and submit["Ordinal"] == invocation and type(submit["Error"]) is str and type(submit["Retryable"]) is type(submit["MaybeCommitted"]) is bool, "native submission classifications")
        integer(submit["Callbacks"], 1, 16)
        if submit["Error"]:
            if invocation < len(a["Submissions"]):
                require(submit["Retryable"] or submit["MaybeCommitted"], "definite SDK error cannot authorize resubmission")
        else:
            require(submit["Outcome"] == o and invocation == len(a["Submissions"]) and not submit["Retryable"] and not submit["MaybeCommitted"], "successful native submission exact original")
    if o["Commit"]:
        require(a["Classification"] == "committed" and o["Reason"] == "" and integer(o["Round"], 1) and integer(o["Sequence"], 1), "genuine useful COMMIT")
        group = decoded(blob(a["Original"]["Value"]))
        require(same(group["Tx"], tx) and same(group["Outcome"], o), "actual full committed CDC original")
        raw_record(a["Original"], prefix(c, owner) + f'changes/{o["Sequence"]:016x}/'.encode() + o["Request"].encode().hex().encode(), go_json(group))
        spool.insert(b"fdb-operation", u64(ordinal), go_json(group))
        spool.insert(b"useful-attempt", u64(ordinal), go_json(a))
    else:
        require(a["Classification"] == "aborted" and o["Reason"] in ("generation", "revision") and o["Round"] == o["Sequence"] == 0 and same(a["Original"], {"Key": None, "Value": None}), "retained ABORT has no committed CDC")
    if prior_raw is None:
        spool.insert(b"attempt-chain", u64(ordinal), go_json(a))
    else:
        spool.replace(b"attempt-chain", u64(ordinal), prior_raw, go_json(a))
    return ordinal, o


def attempts(directory, end, spool, filename="fdb-selectors-attempts.jsonl", request_prefix="selectors", updates=None):
    updates = end["Spec"]["facts"] if updates is None else integer(updates, 1, end["Spec"]["facts"])
    digest, counts, footer = hashlib.sha256(), [0, 0, 0], False
    clients = [0] * 32
    original_cost = {"old": [[0, 0], [0, 0]], "final": [[0, 0], [0, 0]]}
    for frame, line in lines(directory / filename, end["Config"]["Limits"]["Transactions"] + 1):
        require(not footer, "attempt after EOF")
        if frame.get("Type") == "footer":
            require(frame == {"Type": "footer", "Attempts": counts[0], "Aborted": counts[1], "Committed": counts[2], "PayloadSHA256": digest.hexdigest()} and digest.hexdigest() == end["AttemptSHA256"], "attempt EOF/raw hash")
            footer = True
            continue
        fields(frame, "Type Attempt")
        require(frame["Type"] == "attempt", "unknown attempt frame")
        ordinal, outcome = attempt(frame["Attempt"], end, spool, request_prefix)
        require(ordinal <= 3 + end["Spec"]["facts"] + updates, "attempt outside explicitly admitted create/correction footprint")
        size = sum(len(blob(r["Key"])) + len(blob(r["Value"])) for r in frame["Attempt"]["Originals"])
        for phase in ("old", "final") if ordinal <= 3 + end["Spec"]["facts"] else ("final",):
            original_cost[phase][0][outcome["Coordinator"]] += 1
            original_cost[phase][1][outcome["Coordinator"]] += size
        counts[0] += 1
        counts[1 if not outcome["Commit"] else 2] += 1
        if outcome["Commit"] and ordinal > 3:
            clients[frame["Attempt"]["ClientID"]] += 1
        digest.update(line)
    require(footer and counts == [end["Attempts"], end["Aborted"], end["Committed"]] and clients == end["Clients"], "all durable useful/aborted attempts and real caller inventory")
    for ordinal in range(1, 4 + end["Spec"]["facts"] + updates):
        final = decoded(spool.lookup(b"attempt-chain", u64(ordinal)))
        require(final["Classification"] == "committed", "exactly one useful completion per operation")
    for phase, costs in original_cost.items():
        spool.insert(b"original-cost", phase.encode(), go_json(costs))


def raw_stream(directory, name, end, stem="fdb-selectors"):
    digest, counts, previous, footer, header = hashlib.sha256(), [0, 0], None, False, None
    for frame, line in lines(directory / (stem + "-" + name + ".jsonl"), 6 * end["Spec"]["facts"] + 6):
        require(not footer, "raw row after EOF")
        if header is None:
            header = frame
            fields(header, "Type Format Stream Config InitialCut OldCut FinalCut Policy Metadata ConfigRecords MetadataRecords CutRecords")
            require(header["Type"] == "header" and header["Format"] == end["Format"] and header["Stream"] == name and header["Config"] == end["Config"] and header["InitialCut"] == end["Initial"] and header["OldCut"] == end["Old"] and header["FinalCut"] == end["Final"] and header["Policy"] == {"PageRows": 32, "PageBytes": 4 << 20}, "fresh source header/scope/policy")
            for g in range(2):
                raw_record(header["ConfigRecords"][g], key(end["Config"], g, "meta", "config"), go_json(end["Config"]))
                raw_record(header["MetadataRecords"][g], key(end["Config"], g, "meta", "state"), go_json(header["Metadata"][g]))
            for record, cut in zip(header["CutRecords"], (end["Initial"], end["Old"], end["Final"]), strict=True):
                raw_record(record, key(end["Config"], 0, "cut", go_json(cut).hex()), go_json(cut))
            digest.update(line)
            yield "header", header
        elif frame.get("Type") == "footer":
            require(frame == {"Type": "footer", "Rows": counts, "Groups": end["Final"]["Groups"], "Metadata": header["Metadata"], "PayloadSHA256": digest.hexdigest()} and end["Sources"][("history", "current", "cdc").index(name)] == {"Rows": counts, "Groups": end["Final"]["Groups"], "Metadata": header["Metadata"]}, "complete raw EOF/hash/result")
            footer = True
        else:
            fields(frame, "Type Row")
            require(frame["Type"] == "row", "unknown raw frame")
            row = frame["Row"]
            fields(row, "Group Key Value" + (" IDKey IDValue RequestKey RequestValue" if name == "cdc" else ""))
            g = integer(row["Group"], 0, 1)
            row = {k: blob(v) if k != "Group" else v for k, v in row.items()}
            require(0 < len(row["Key"]) <= 1024 and 0 < len(row["Value"]) <= 64 << 10, "raw key/value admission")
            if name == "cdc":
                o = decoded(row["Value"])["Outcome"]
                order = o["Round"], g, o["Request"]
            else:
                order = g, row["Key"]
            require(previous is None or previous < order, "native raw order/duplicate")
            previous = order
            counts[g] += 1
            digest.update(line)
            yield "row", row
    require(header is not None and footer, "truncated raw source")


def source_fold(directory, end, spool, stem="fdb-selectors", updates=None):
    p, c = end["Spec"], end["Config"]
    updates = p["facts"] if updates is None else integer(updates, 1, p["facts"])
    physical, definitions = codec.StoredState(spool, b"fdb-before"), set()
    generations, sequences, versions = [0, 0], [0, 0], [0, 0]
    floors = [end["Old"]["Round"]] * 2
    old_generations, old_versions = [0, 0], [0, 0]
    header = None
    cdc_bytes = {"old": [0, 0], "final": [0, 0]}
    history_bytes = {"old": [0, 0], "final": [0, 0]}
    for kind, row in raw_stream(directory, "cdc", end, stem):
        if kind == "header":
            header = row
            continue
        group = decoded(row["Value"])
        ordinal = integer(group["Tx"]["Lifecycle"]["Provenance"], 1, 3 + p["facts"] + updates)
        require(row["Value"] == spool.lookup(b"fdb-operation", u64(ordinal)), "CDC exact useful attempt/body lineage")
        codec.fdb_group(group, p, ordinal, c, physical, generations, sequences, definitions, parallel=True)
        for participant in group["Tx"]["Participants"]:
            floors[participant["Group"]] = max(floors[participant["Group"]], group["Outcome"]["Round"])
        if ordinal <= 3 + p["facts"]:
            for participant in group["Tx"]["Participants"]:
                old_generations[participant["Group"]] += 1
        o = group["Outcome"]
        require(row["Group"] == o["Coordinator"] and row["Key"] == prefix(c, o["Coordinator"]) + f'changes/{o["Sequence"]:016x}/'.encode() + o["Request"].encode().hex().encode(), "CDC actual coordinator/sequence key")
        for field, namespace in (("ID", b"attempt-id"), ("Request", b"attempt-request")):
            require(row[field + "Key"] == key(c, o["Coordinator"], field.lower(), o[field]) and row[field + "Value"] == go_json(o) == spool.lookup(namespace, row[field + "Key"]), "raw CDC exact native paired originals")
        require(end["Initial"]["Round"] < o["Round"] <= end["Final"]["Round"] and (o["Round"] <= end["Old"]["Round"]) == (ordinal <= 3 + p["facts"]), "actual phase/cut membership")
        spool.insert(b"cdc-seen", u64(ordinal), b"1")
        spool.insert(b"native-cdc", row["Key"], row["Value"])
        for phase in ("old", "final") if ordinal <= 3 + p["facts"] else ("final",):
            cdc_bytes[phase][o["Coordinator"]] += len(row["Key"]) + len(row["Value"])
        for write in group["Writes"]:
            g, logical, after = write["Group"], write["Key"], write["After"]
            body = go_json(after)
            history_key = prefix(c, g) + f'history/{after["Round"]:016x}/'.encode() + logical.encode().hex().encode()
            spool.insert(b"fdb-history", history_key, body)
            for phase in ("old", "final") if ordinal <= 3 + p["facts"] else ("final",):
                history_bytes[phase][g] += len(history_key) + len(body)
            codec.put_raw(spool, b"fdb-current", key(c, g, "current", logical), body)
            if after["Round"] <= end["Old"]["Round"]:
                codec.put_raw(spool, b"fdb-old", key(c, g, "current", logical), body)
            versions[g] += 1
            if ordinal <= 3 + p["facts"]:
                old_versions[g] += 1
    require(header is not None and sequences == end["Final"]["Groups"], "complete native coordinator prefix")
    for ordinal in range(1, 4 + p["facts"] + updates):
        spool.lookup(b"cdc-seen", u64(ordinal))
    for name, expected_count in (("history", 3 + 3 * p["facts"] + 3 * updates), ("current", 3 + 3 * p["facts"])):
        count = 0
        for kind, row in raw_stream(directory, name, end, stem):
            if kind == "header":
                require(same(row, header | {"Stream": name}), "mixed raw stream authorities")
                continue
            require(row["Key"].startswith(prefix(c, row["Group"]) + name.encode() + b"/") and row["Value"] == spool.lookup(b"fdb-" + name.encode(), row["Key"]), "physical source exact authentic effects")
            spool.insert(b"raw-seen-" + name.encode(), row["Key"], b"1")
            count += 1
        require(count == expected_count, "complete raw physical exact set")
    for g, m in enumerate(header["Metadata"]):
        require(m["Generation"] == generations[g] and m["Versions"] == versions[g] and m["Groups"] == sequences[g], "actual graph generation/version/group census")
    for phase in ("old", "final"):
        current_bytes = [0, 0]
        for native_key, body in spool.walk(b"fdb-old" if phase == "old" else b"fdb-current"):
            group = 0 if native_key.startswith(prefix(c, 0)) else 1
            current_bytes[group] += len(native_key) + len(body)
        spool.insert(b"source-cost", phase.encode(), go_json([cdc_bytes[phase], history_bytes[phase], current_bytes]))
    spool.insert(b"source-count", b"old", go_json([old_generations, old_versions]))
    spool.insert(b"source-count", b"final", go_json([generations, versions]))
    spool.insert(b"source-floor", b"final", go_json(floors))
    return header


OBSERVATIONS = ("initial-empty", "old-fresh", "old-scope-0", "old-scope-1", "stable-after-update-4", "stable-after-update-2", "stable-after-update-3", "held-old-after-update", "at-old-after-update", "after-current", "fresh-current")


def records(values):
    require(type(values) is list and len(values) <= 16, "bounded native control inventory")
    out = {}
    for record in values:
        fields(record, "Key Value")
        k, v = blob(record["Key"]), blob(record["Value"])
        require(0 < len(k) <= 17 << 10 and 0 < len(v) <= 64 << 10 and k not in out, "raw control admission/uniqueness")
        out[k] = v
    return out


def certificate(c, record, scope=None):
    fields(record, "Key Value")
    body = blob(record["Value"])
    cert = decoded(body)
    fields(cert, "Graph Topology Epochs Scope Round Groups")
    coverage = blob(cert["Scope"])
    require(cert["Graph"] == c["Graph"] and cert["Topology"] == c["Topology"] and cert["Epochs"] == c["Epochs"] and coverage in (b"\0", b"\1", b"\0\1") and integer(cert["Round"], 1) and len(cert["Groups"]) == 2, "native certificate config/scope")
    for group in cert["Groups"]:
        integer(group)
    raw_record(record, key(c, coverage[0], "cut", body.hex()), body)
    if scope is not None:
        require(coverage == scope, "exact scope, never silent extension")
    return cert


def selection(value, name, end, spool):
    fields(value, "Request Selected Config Metadata Retention Inventory InventoryRows InventoryBytes ReceiptPair ReceiptCDC AcquiredOwner Acquisition")
    c, q, cut = end["Config"], value["Request"], value["Selected"]
    fields(q, "Mode Scope At" + (" After" if "After" in q else ""))
    expected_mode = "Stable" if name == "initial-empty" or name.startswith("stable-after") else "At" if name in ("at-old-after-update", "raw-source-held") else "After" if name == "after-current" else "Fresh"
    # The held read is the very same genuine old Fresh owner, not another open.
    if name == "held-old-after-update":
        expected_mode = "Fresh"
    require(q["Mode"] == expected_mode and q["Scope"] == cut["Scope"] and value["Inventory"] is None, "selector union/native exact scope")
    expected_scope = b"\0" if name in ("old-scope-0", "stable-after-update-2") else b"\1" if name in ("old-scope-1", "stable-after-update-3") else b"\0\1"
    require(blob(q["Scope"]) == expected_scope, "named requested scope")
    expected_cut = end["Initial"] if name == "initial-empty" else end["Final"] if name in ("after-current", "fresh-current", "raw-source-held") else end["Old"]
    require(cut == expected_cut | {"Scope": q["Scope"]}, "Stable/At cannot substitute current6 or invent a Fresh fence")
    if q["Mode"] == "At":
        require(q["At"] == cut and "After" not in q, "At exact caller certificate")
    else:
        require(q["At"] == {"Graph": "", "Topology": 0, "Epochs": [0, 0], "Scope": None, "Round": 0, "Groups": [0, 0]}, "no caller cut disguised as Stable/Fresh/After")
    source_key = lambda name: key(c, 0, "source-retention-v1", name)
    acquired = records(value["Acquisition"])
    mandatory = {source_key(n) for n in ("binding", "state", "ledger")}
    mandatory.add(key(c, expected_scope[0], "cut", go_json(cut).hex()))
    mandatory.update(key(c, g, "meta", n) for g in range(2) for n in ("config", "state"))
    require(set(acquired) == mandatory, "complete acquisition config/certificate/retention inventory")
    require(acquired[key(c, expected_scope[0], "cut", go_json(cut).hex())] == go_json(cut), "actual native acquired cut bytes")
    native_cut_key = key(c, expected_scope[0], "cut", go_json(cut).hex())
    prior_certificate = spool.find(b"native-certificate", native_cut_key)
    if prior_certificate is None:
        spool.insert(b"native-certificate", native_cut_key, go_json(cut))
    else:
        require(prior_certificate == go_json(cut), "immutable acquired certificate")
    for g in range(2):
        require(acquired[key(c, g, "meta", "config")] == go_json(c), "actual acquisition graph config")
    spool.insert(b"selection-metadata", name.encode(), go_json([decoded(acquired[key(c, g, "meta", "state")]) for g in range(2)]))
    binding = decoded(acquired[source_key("binding")])
    require(binding == {"Version": 1, "Config": c, "Initial": end["Initial"], "Policy": end["Binding"]["History"]}, "actual explicit retained-reader policy")
    state = decoded(acquired[source_key("state")])
    require(state["Owners"] > 0 and same(state, codec.fdb_retention_check.zero_state(state["Owners"])), "genuine retained owner acquired before data; family retains source")
    owner = decoded(blob(value["AcquiredOwner"]["Value"]))
    fields(owner, "Kind Request Cut Required Secret State PinRequest PinDigest Lineage TerminalRequest")
    secret = codec.sha_array(owner["Secret"])
    require(any(secret) and owner["Kind"] == "read" and owner["Request"] == "source-read-" + secret[:16].hex() and owner["Cut"] == cut and owner["Required"] == end["Initial"] and owner["PinRequest"] == "" and bytes(owner["PinDigest"]) == bytes(owner["Lineage"]) == bytes(32) and owner["State"] == 1 and owner["TerminalRequest"] == "", "actual factory/durable owner identity/physical prefix")
    raw_record(value["AcquiredOwner"], prefix(c) + b"source-owner-v1/" + owner["Request"].encode().hex().encode(), go_json(owner))
    if name == "old-fresh":
        spool.insert(b"held-snapshot", b"old", go_json(owner))
    if name == "held-old-after-update":
        require(go_json(owner) == spool.lookup(b"held-snapshot", b"old"), "held owner is the original acquired identity")
    if q["Mode"] == "After":
        fields(q["After"], "Tx Outcome")
        receipt = q["After"]
        require(receipt["Outcome"]["Commit"] is True and receipt["Tx"]["Lifecycle"]["Provenance"] == 4 + end["Spec"]["facts"], "After genuine update0 receipt")
        original = decoded(spool.lookup(b"fdb-operation", u64(receipt["Tx"]["Lifecycle"]["Provenance"])))
        require(receipt == {"Tx": original["Tx"], "Outcome": original["Outcome"]} and cut["Round"] >= receipt["Outcome"]["Round"], "After authentic original membership")
        for record, kind in zip(value["ReceiptPair"], ("id", "request"), strict=True):
            identity = receipt["Tx"]["ID" if kind == "id" else "Request"]
            raw_record(record, key(c, receipt["Outcome"]["Coordinator"], kind, identity), go_json(receipt["Outcome"]))
            require(blob(record["Value"]) == spool.lookup(b"attempt-" + kind.encode(), blob(record["Key"])), "After both native original authorities")
        o = receipt["Outcome"]
        raw_record(value["ReceiptCDC"], prefix(c, o["Coordinator"]) + f'changes/{o["Sequence"]:016x}/'.encode() + o["Request"].encode().hex().encode(), go_json(original))
    elif q["Mode"] != "Stable":
        require("After" not in q and value["InventoryRows"] == value["InventoryBytes"] == 0 and not value["Retention"], "no hidden selector inventory/receipt")
    return state


def work(value, policy, mode):
    fields(value, "Rows Bytes Callbacks OpeningRows OpeningBytes OpeningCallbacks CleanupRows CleanupBytes CleanupCallbacks")
    for n in value.values():
        integer(n)
    require(value["OpeningRows"] <= value["Rows"] <= policy["WorkRows"] and value["OpeningBytes"] <= value["Bytes"] <= policy["WorkBytes"] and value["OpeningCallbacks"] <= value["Callbacks"], "cumulative opening+scan admission/callback bounds")
    minimum = {"Stable": 5, "Fresh": 5, "At": 4, "After": 6}[mode]
    callbacks = integer(value["OpeningCallbacks"], minimum, 16 * minimum)
    extra = value["OpeningRows"] - 64 * callbacks
    require(extra >= 0 and (mode == "Stable" and extra % 1024 == 0 and 1 <= extra // 1024 <= 16 or mode != "Stable" and extra == 0), "conservative opening access reservations include actual retry callbacks")
    require(value["OpeningBytes"] == (4 << 20) * callbacks + (policy["InventoryBytes"] * (extra // 1024) if mode == "Stable" else 0), "opening reservation is declared, never measured I/O")
    require(value["CleanupRows"] <= policy["Cleanup"]["Rows"] and value["CleanupBytes"] <= policy["Cleanup"]["Bytes"] and value["CleanupBytes"] == value["CleanupRows"] * (4 << 20) // 64, "separately prepaid cumulative cleanup allowance")


def events(directory, end, spool, header):
    digest, footer, started = hashlib.sha256(), False, False
    active, current, count, cert_count, cert_bytes, best, previous_cert = None, None, [0, 0], 0, 0, None, None
    observations, points, source_held, source_closed = 0, 0, None, False
    inv_stage, inv_meta, inv_counts, inv_previous = None, None, [[0, 0], [0, 0]], {}
    committed, inventories = 0, 0
    opened = 0
    max_rows = 80 + 12 * (3 + 3 * end["Spec"]["facts"]) + 4 * end["Attempts"]
    for frame, line in lines(directory / "fdb-selectors-events.jsonl", max_rows):
        require(not footer, "event after EOF")
        kind = frame.get("Type")
        if kind == "footer":
            require(active is None and inv_stage is None and source_closed and frame == {"Type": "footer", "End": end} and digest.hexdigest() == end["EventSHA256"], "complete events/source-close/hash result")
            footer = True
            continue
        digest.update(line)
        if not started:
            fields(frame, "Type Format Spec Config Binding Record Initial")
            require(frame == {"Type": "header", "Format": end["Format"], "Spec": end["Spec"], "Config": end["Config"], "Binding": end["Binding"], "Record": end["BindingRecord"], "Initial": end["Initial"]}, "fresh actual binding header")
            started = True
            continue
        if kind == "selector-open":
            fields(frame, "Type Name Mode Access Cut Nanos")
            require(frame["Name"] in (*OBSERVATIONS, "raw-source-held") and frame["Name"] != "held-old-after-update" and active is None and opened < 11 and frame["Access"] == "actual Open through retained selection/acquisition including retries/wait; no data rows read", "actual independent selector opening timing")
            integer(frame["Nanos"], 1, (1 << 63) - 1)
            spool.insert(b"selector-open", frame["Name"].encode(), go_json(frame))
            opened += 1
        elif kind == "committed":
            fields(frame, "Type Operation Original Outcome")
            ordinal, _, _ = operation(end["Spec"], frame["Operation"])
            body = spool.lookup(b"fdb-operation", u64(ordinal))
            require(blob(frame["Original"]["Value"]) == body and frame["Outcome"] == decoded(body)["Outcome"], "actual per-operation completion original")
            spool.insert(b"completion-seen", u64(ordinal), b"1")
            committed += 1
        elif kind == "selection":
            fields(frame, "Type Name Selection Policy")
            require(active is None and observations < len(OBSERVATIONS) and frame["Name"] == OBSERVATIONS[observations] and frame["Policy"] == end["Binding"]["Read"], "actual selector lifecycle/order")
            active, current = frame["Name"], frame["Selection"]
            selection(current, active, end, spool)
            if active != "held-old-after-update":
                sample = decoded(spool.lookup(b"selector-open", active.encode()))
                require(sample["Cut"] == current["Selected"] and sample["Mode"] == current["Request"]["Mode"], "actual timed Open binds exact acquired cut/mode")
            count, cert_count, cert_bytes, best, previous_cert = [0, 0], 0, 0, None, None
            if current["Request"]["Mode"] == "Stable":
                c, coverage = end["Config"], blob(current["Selected"]["Scope"])
                require(len(current["Config"]) == len(current["Metadata"]) == 2 and len(current["Retention"]) == 3, "complete native inventory/config/retention snapshot")
                retained = records(current["Retention"])
                src = lambda name: key(c, 0, "source-retention-v1", name)
                require(set(retained) == {src(n) for n in ("binding", "state", "ledger")} and retained[src("binding")] == records(current["Acquisition"])[src("binding")], "same native retention binding")
                before = decoded(retained[src("state")])
                after = decoded(records(current["Acquisition"])[src("state")])
                require(same(before, codec.fdb_retention_check.zero_state(before["Owners"])) and after["Owners"] == before["Owners"] + 1, "read-only Stable inventory followed by genuine guarded acquisition")
                for g in range(2):
                    raw_record(current["Config"][g], key(c, g, "meta", "config"), go_json(c))
                    m = decoded(blob(current["Metadata"][g]["Value"]))
                    raw_record(current["Metadata"][g], key(c, g, "meta", "state"), go_json(m))
                    require(go_json(m) == records(current["Acquisition"])[key(c, g, "meta", "state")], "Stable made no fence/certificate/data metadata write")
        elif kind == "certificate":
            fields(frame, "Type Name Ordinal Record")
            require(active is not None and current["Request"]["Mode"] == "Stable" and frame["Name"] == active and frame["Ordinal"] == cert_count, "ordered complete actual native inventory")
            native = blob(frame["Record"]["Key"])
            require(previous_cert is None or previous_cert < native, "certificate native lex order/duplicate")
            previous_cert = native
            cert = certificate(end["Config"], frame["Record"])
            require(blob(cert["Scope"])[0] == blob(current["Request"]["Scope"])[0], "inventory exact actual prefix")
            cert_count += 1
            cert_bytes += len(native) + len(blob(frame["Record"]["Value"]))
            require(cert_count <= end["Config"]["Limits"]["Certificates"] and cert_bytes <= end["Binding"]["Read"]["InventoryBytes"], "finite inventory proof buffer")
            if cert["Scope"] == current["Request"]["Scope"]:
                order = cert["Round"], blob(frame["Record"]["Value"])
                if best is None or order > best[0]:
                    best = order, cert
            prior = spool.find(b"native-certificate", native)
            if prior is None:
                spool.insert(b"native-certificate", native, blob(frame["Record"]["Value"]))
            else:
                require(prior == blob(frame["Record"]["Value"]), "immutable native certificate changed")
        elif kind == "snapshot-row":
            fields(frame, "Type Name Row")
            require(active is not None and frame["Name"] == active and active != "initial-empty", "snapshot rows require actual owned source")
            snapshot_row(frame["Row"], current["Selected"], end, spool, active)
            count[frame["Row"]["Group"]] += 1
        elif kind == "snapshot-eof":
            fields(frame, "Type Name Access End Nanos")
            integer(frame["Nanos"], 1, (1 << 63) - 1)
            require(active == frame["Name"] and frame["End"]["Cut"] == current["Selected"] and frame["End"]["Rows"] == count and "not indexed" in frame["Access"], "actual semantic source EOF/scope/access")
            scope = blob(current["Selected"]["Scope"])
            expected = [0, 0]
            namespace = b"fdb-old" if observations < 9 else b"fdb-current"
            if active != "initial-empty":
                for native_key, body in spool.walk(namespace):
                    g = 0 if native_key.startswith(prefix(end["Config"], 0)) else 1
                    if g in scope:
                        require(body == spool.lookup(b"snapshot-seen-" + active.encode(), native_key), "complete exact semantic source set")
                        expected[g] += 1
            fields(frame["End"], "Cut Rows DirectoryRows OriginalEffects Work")
            effects = [0, 0] if active == "initial-empty" else decoded(spool.lookup(b"source-count", b"old" if observations < 9 else b"final"))[1]
            effects = [n if g in scope else 0 for g, n in enumerate(effects)]
            require(count == expected and frame["End"]["DirectoryRows"] == expected and frame["End"]["OriginalEffects"] == effects, "no omitted/extra/phantom source key or original census")
            if current["Request"]["Mode"] == "Stable":
                metadata = decoded(blob(current["Metadata"][scope[0]]["Value"]))
                require(best is not None and best[1] == current["Selected"] and cert_count == current["InventoryRows"] == metadata["Certificates"] and cert_bytes == current["InventoryBytes"], "Stable actual complete numeric-round selection, not lexical last or caller At")
            work(frame["End"]["Work"], end["Binding"]["Read"], current["Request"]["Mode"])
            spool.insert(b"selector-scan", active.encode(), go_json({"Mode": current["Request"]["Mode"], "Cut": current["Selected"], "Rows": count, "Nanos": frame["Nanos"]}))
            observations += 1
            active = None
        elif kind == "points-eof":
            fields(frame, "Type Name Access Points Work Nanos")
            integer(frame["Nanos"], 1, (1 << 63) - 1)
            require(observations == 11 and points == 0 and frame["Name"] == "fresh-current" and "not an index" in frame["Access"] and len(frame["Points"]) == 3, "one genuine batched scan-based point call")
            for index, point in enumerate(frame["Points"]):
                fields(point, "Key Found Value")
                fact = (0, end["Spec"]["facts"] - 1, end["Spec"]["facts"] + 16)[index]
                if index < 2:
                    command = codec.fdb_contract(end["Spec"], 4 + fact)[0]
                    edge = command["Edge"]
                    logical = "edge/" + "/".join(f"{n:016x}" for n in (edge["ID"], edge["Source"]["Node"], edge["Source"]["Life"], edge["Target"]["Node"], edge["Target"]["Life"]))
                    require(point["Key"] == {"Group": 0, "Key": logical} and point["Found"] is True and go_json(point["Value"]) == spool.lookup(b"fdb-current", key(end["Config"], 0, "current", logical)), "actual present point equals complete final source")
                else:
                    # Out-of-range fixture ID still uses the real endpoint
                    # routing formula; it is not an arbitrary absent key.
                    target = end["Config"]["Nodes"][1] if (fact + 1) * end["Spec"]["crossPercent"] // 100 > fact * end["Spec"]["crossPercent"] // 100 else 3
                    logical = "edge/" + "/".join(f"{n:016x}" for n in (7 + 2 * fact, 1, 2, target, target + 1))
                    require(point["Key"] == {"Group": 0, "Key": logical} and point["Found"] is False and point["Value"] == {"Value": 0, "Version": 0, "Round": 0, "Deleted": False, "TxID": ""}, "actual phantom point is exact empty")
            work(frame["Work"], end["Binding"]["Read"], "Fresh")
            spool.insert(b"selector-points", b"fresh-current", go_json({"Mode": "Fresh", "Cut": end["Final"], "Probes": 3, "Found": 2, "Nanos": frame["Nanos"]}))
            points += 1
        elif kind == "originals-header":
            fields(frame, "Type Stage Cut Metadata Records")
            require(inv_stage is None and frame["Stage"] == ("old", "final")[inventories] and frame["Cut"] == end["Old" if inventories == 0 else "Final"], "actual drained native inventory barrier")
            inv_stage, inv_meta, inv_counts, inv_previous = frame["Stage"], frame["Metadata"], [[0, 0], [0, 0]], {}
            for g in range(2):
                raw_record(frame["Records"][g], key(end["Config"], g, "meta", "state"), go_json(inv_meta[g]))
                require(inv_meta[g]["Groups"] == frame["Cut"]["Groups"][g] and inv_meta[g]["Floor"] == frame["Cut"]["Round"], "actual barrier watermark")
            spool.insert(b"barrier-metadata", inv_stage.encode(), go_json(inv_meta))
        elif kind == "original":
            fields(frame, "Type Stage Kind Group Records")
            require(inv_stage == frame["Stage"] and frame["Kind"] in ("id", "request"), "native original inventory stage/kind")
            kind_index, g = ("id", "request").index(frame["Kind"]), integer(frame["Group"], 0, 1)
            native = blob(frame["Records"][0]["Key"])
            order_key = kind_index, g
            require(order_key not in inv_previous or inv_previous[order_key] < native, "native original prefix duplicate/reorder")
            inv_previous[order_key] = native
            for record, kind in zip(frame["Records"], (frame["Kind"], "request" if kind_index == 0 else "id"), strict=True):
                require(blob(record["Value"]) == spool.lookup(b"attempt-" + kind.encode(), blob(record["Key"])), "actual paired original present at the declared barrier")
                if inv_stage == "old":
                    require(int.from_bytes(spool.lookup(b"attempt-membership-" + kind.encode(), blob(record["Key"])), "big") <= 3 + end["Spec"]["facts"], "old native inventory excludes post-barrier attempts")
                    outcome = decoded(blob(record["Value"]))
                    # Authentic ABORT has no round: membership comes from actual
                    # barrier inventory, never a reported phase/count alone.
                    body = spool.lookup(b"attempt-id", key(end["Config"], g, "id", outcome["ID"]))
                    require(body == blob(record["Value"]), "old actual paired authority")
            spool.insert(b"inventory-" + inv_stage.encode() + b"-" + frame["Kind"].encode(), native, blob(frame["Records"][0]["Value"]))
            inv_counts[kind_index][g] += 1
        elif kind == "originals-eof":
            fields(frame, "Type Stage Rows Metadata")
            require(inv_stage == frame["Stage"] and inv_counts == frame["Rows"] and inv_meta == frame["Metadata"] and inv_counts[0] == inv_counts[1] == [m["Transactions"] for m in inv_meta], "complete ID AND request native prefix census")
            expected_transactions = decoded(spool.lookup(b"original-cost", inv_stage.encode()))[0]
            require(inv_counts[0] == expected_transactions, "authentic attempt/barrier membership, including ABORTs")
            inv_stage = None
            inventories += 1
        elif kind == "source-held":
            fields(frame, "Type Selection Policy")
            require(observations == 11 and points == 1 and inventories == 2 and source_held is None and frame["Policy"] == end["Binding"]["Read"], "genuine final raw-source owner before exports")
            selection(frame["Selection"], "raw-source-held", end, spool)
            sample = decoded(spool.lookup(b"selector-open", b"raw-source-held"))
            require(sample["Mode"] == "At" and sample["Cut"] == end["Final"], "raw source timed opening actual final owner")
            source_held = decoded(blob(frame["Selection"]["AcquiredOwner"]["Value"]))
        elif kind == "source-closed":
            fields(frame, "Type Access Owner Work")
            require(frame["Access"] == "Work covers selector opening/cleanup only; complete raw history uses the separately bound History policy", "raw history work cannot be mislabeled as selector debit")
            require(source_held is not None and not source_closed, "raw source EOF before authenticated Close")
            expected_owner = source_held | {"State": 2, "TerminalRequest": "close"}
            raw_record(frame["Owner"], prefix(end["Config"]) + b"source-owner-v1/" + source_held["Request"].encode().hex().encode(), go_json(expected_owner))
            work(frame["Work"], end["Binding"]["Read"], "At")
            require(frame["Work"]["CleanupCallbacks"] > 0, "actual separately bounded source cleanup")
            source_closed = True
        else:
            require(False, "unknown selector event")
    require(started and footer and opened == 11 and committed == end["Committed"] and observations == len(OBSERVATIONS) and inventories == 2 and points == 1 and source_closed, "missing actual selectors/original inventories/timed opens/EOF")


def snapshot_row(row, cut, end, spool, name):
    fields(row, "Group Key Value Current History Chain")
    g, logical = integer(row["Group"], 0, 1), row["Key"]
    require(g in blob(cut["Scope"]) and type(logical) is str and 0 < len(logical.encode()) <= 128, "source row scope/key")
    native_key = key(end["Config"], g, "current", logical)
    namespace = b"fdb-old" if cut["Round"] == end["Old"]["Round"] else b"fdb-current"
    require(go_json(row["Value"]) == spool.lookup(namespace, native_key), "exact old/current logical snapshot answer")
    require(type(row["Chain"]) is list and 1 <= len(row["Chain"]) <= end["Binding"]["Read"]["ChainSteps"], "finite source predecessor chain")
    head = decoded(blob(row["Current"]["Value"]))
    raw_record(row["Current"], native_key, go_json(head))
    actual_head = spool.lookup(b"fdb-old" if name.startswith("old-") else b"fdb-current", native_key)
    require(go_json(head) == actual_head, "genuine current directory observation at actual phase")
    v = head
    for index, raw in enumerate(row["Chain"]):
        fields(raw, "Group Key Value IDKey IDValue RequestKey RequestValue")
        body = blob(raw["Value"])
        require(body == spool.lookup(b"native-cdc", blob(raw["Key"])), "actual original predecessor authority")
        original = decoded(body)
        o = original["Outcome"]
        require(raw["Group"] == o["Coordinator"], "source chain actual coordinator")
        for kind in ("ID", "Request"):
            require(blob(raw[kind + "Key"]) == key(end["Config"], o["Coordinator"], kind.lower(), o[kind]) and blob(raw[kind + "Value"]) == go_json(o) == spool.lookup(b"attempt-" + kind.lower().encode(), blob(raw[kind + "Key"])), "source paired original authority")
        matches = [w for w in original["Writes"] if w["Group"] == g and w["Key"] == logical and w["After"] == v]
        require(len(matches) == 1, "source chain exact key/revision correspondence")
        if index + 1 == len(row["Chain"]):
            require(v == row["Value"] and v["Round"] <= cut["Round"] and o["Sequence"] <= cut["Groups"][o["Coordinator"]] and not v["Deleted"], "latest live revision at selected cut")
        else:
            require(v["Round"] > cut["Round"] and matches[0].get("Before") is not None, "chain cannot skip already-selected revision")
            v = matches[0]["Before"]
    history_key = prefix(end["Config"], g) + f'history/{v["Round"]:016x}/'.encode() + logical.encode().hex().encode()
    raw_record(row["History"], history_key, spool.lookup(b"fdb-history", history_key))
    spool.insert(b"snapshot-seen-" + name.encode(), native_key, go_json(v))


def columns(directory, end, spool, filename="fdb-selectors-columns.jsonl"):
    # Explicit caller filename only; native graph/config/cut/format and every
    # source/original/chunk/owner join below remain independently authenticated.
    path = directory / filename
    if end["Spec"]["layout"] == "kv":
        require(not path.exists() and "Columns" not in end, "KV evidence cannot masquerade as columns")
        return
    c, p = end["Config"], end["Spec"]
    column_end = end["Columns"]
    manifests = {"old": column_end["Old"], "final": column_end["Final"]}
    # These are actual independently enumerated barriers, including ABORT pairs.
    end = end | {"OldMetadata": decoded(spool.lookup(b"barrier-metadata", b"old")),
                 "FinalMetadata": decoded(spool.lookup(b"barrier-metadata", b"final"))}
    chunks, chunk_rows, retained, chains = {}, {}, {}, {}
    walks, stages, refusals = {}, [], []
    chunk_order, chunk_sequences, chunk_history, walk_order = {}, {}, {}, {}
    publication_work, last_outcome = {"old": 0, "final": 0}, {}
    storage = dict.fromkeys("Records Bytes LiveChunkRecords LiveChunkBytes TombstoneRecords TombstoneBytes RetiredLineageRecords RetiredLineageBytes".split(), 0)
    readers, storage_previous = {"old": 0, "final": 0}, None
    collected, deleted, storage_count, storage_bytes = 0, 0, 0, 0
    live_chunks, live_bytes, footer, header, point_count = 0, 0, False, False, 0
    digest, whole, binding, policy = hashlib.sha256(), hashlib.sha256(), None, None
    held, token, pin_binding = None, None, None
    for row, line in lines(path, 40 + 12 * end["Committed"]):
        require(not footer, "trailing column evidence")
        whole.update(line)
        kind = row.get("Type")
        if kind == "footer":
            final = row["End"]
            require(final == column_end | {"ArtifactSHA256": [0] * 32} and final["Format"] == final["SourceFormat"] == end["Format"] and final["ReferenceLayout"] == "native-logical-kv-reference" and
                    final["Old"] == manifests["old"] and final["Final"] == manifests["final"] and bytes(final["PayloadSHA256"]) == digest.digest(), "sealed actual derived column artifact")
            require(final["Counts"]["Stages"] == 6 and final["Counts"]["Chunks"] == sum(chunks.values()) and final["Counts"]["WalkRows"] == sum(walks.values()) and final["Counts"]["Points"] == point_count == 6 and final["Counts"]["Collections"] == collected and
                    final["Ledger"]["Chunks"] == live_chunks == manifests["final"]["Chunks"] and final["Storage"]["Records"] == storage_count and final["Storage"]["Bytes"] == storage_bytes and
                    final["Storage"]["LiveChunkBytes"] == live_bytes == manifests["final"]["Bytes"], "actual column live/tombstone/storage census")
            require(go_json(final["Ledger"]) == spool.lookup(b"column-control", prefix(c) + b"columns-v1/ledger") and
                    go_json(final["PinLedger"]) == spool.lookup(b"column-control", prefix(c) + b"columns-v1/retention-v1/ledger") and storage_count == live_chunks + 22, "complete strict terminal controls and quota ledgers")
            footer = True
            continue
        digest.update(line)
        if not header:
            require(row["Type"] == "header" and row["Format"] == end["Format"] and row["Config"] == c and len(row["Records"]) == 2, "fresh actual column binding header")
            binding = decoded(blob(row["Records"][0]["Value"]))
            require(binding["Version"] == 1 and binding["Config"] == c and binding["Scope"] == codec.SCOPE and binding["Layout"] == codec.COLUMN_LAYOUT and
                    binding["Schema"] == "node-presence;edge-life-bound;property-p:i64;canonical-original:v1", "typed column graph/schema/axis binding")
            raw_record(row["Records"][0], prefix(c) + b"columns-v1/binding", go_json(binding))
            policy = binding["Policy"]
            require(policy == {"ChunkRows": 32, "ChunkBytes": 64 << 10, "MaxChunks": 1_000_000, "MaxGenerations": 2,
                               "MaxRetainedBytes": (64 << 20) + (6 + 3 * p["facts"]) * (64 << 10), "MaxWorkRows": 3 + 2 * p["facts"],
                               "MaxWorkBytes": (64 << 20) + (3 + 2 * p["facts"]) * (512 << 10)}, "explicit unchanged column admission policy")
            pin_binding = decoded(blob(row["Records"][1]["Value"]))
            require(same(pin_binding, {"Version": 1, "Columns": list(hashlib.sha256(go_json(binding)).digest()), "Policy": {"MaxRecords": 32, "MaxBytes": 256 << 10, "PageChunks": 1, "PageBytes": 128 << 10,
                            "MaxWorkChunks": 1, "MaxWorkBytes": 128 << 10, "MaxOutputBytes": 1024}}), "actual pin namespace and finite quota binding")
            raw_record(row["Records"][1], prefix(c) + b"columns-v1/retention-v1/binding", go_json(pin_binding))
            for record in row["Records"]:
                spool.insert(b"column-control", blob(record["Key"]), blob(record["Value"]))
            for name, m in manifests.items():
                fields(m, "Generation Metadata Chunks Rows Bytes History Chain")
                require(m["Generation"] == {"Version": 1, "ID": name, "Binding": list(hashlib.sha256(go_json(binding)).digest()), "Initial": end["Initial"], "Cut": end["Old" if name == "old" else "Final"]} and
                        m["Metadata"] == end["OldMetadata" if name == "old" else "FinalMetadata"] and m["History"] == [v["Versions"] for v in m["Metadata"]] and
                        m["Rows"] == (3 + p["facts"] if name == "old" else end["Committed"]), "complete source census at authentic generation cut")
                require(1 <= integer(m["Chunks"]) <= min(m["Rows"], policy["MaxChunks"]) and integer(m["Bytes"], 1) <= policy["MaxRetainedBytes"], "column manifest finite admitted work")
            header = True
        elif kind == "stage":
            name = row["Name"]
            require(len(stages) < 6 and name == ("old-held", "final-head", "expired-held", "reads-complete", "old-closed", "terminal")[len(stages)] and 1 <= len(row["Records"]) <= 20, "actual bounded column lifetime stages")
            records = codec.column_records(row["Records"])
            proxy = {"Config": c, "OldCut": end["Old"]}
            index = (0, 1, 2, 3, 4, 6)[len(stages)]
            if index == 6:
                proxy["Columns"] = {"Ledger": decoded(records.get(prefix(c) + b"columns-v1/ledger", b"")),
                                    "PinLedger": decoded(records.get(prefix(c) + b"columns-v1/retention-v1/ledger", b""))}
            held, token = codec.column_stage(row, index, proxy, manifests, binding, pin_binding, held, token, spool)
            stages.append(name)
        elif kind == "chunk":
            name = row["Generation"]
            require(name in manifests and len(stages) == 3 and spool.find(b"column-eof", name.encode()) is None, "foreign/late derived generation")
            m = manifests[name]
            ordinal = chunks.get(name, 0)
            body = blob(row["Record"]["Value"])
            native_key = raw.column_key(c, name, f"chunk/{ordinal:016x}")
            raw_record(row["Record"], native_key, body)
            spool.insert(b"column-native-chunk", native_key, body)
            require(row["Ordinal"] == ordinal and row["Length"] == len(body) and row["SHA256"] == list(hashlib.sha256(body).digest()), "actual chunk ordinal/length/hash")
            cache_name = "column-cache-" + name
            filename = f'column-{hashlib.sha256(go_json(m["Generation"])).hexdigest()}-{ordinal:016x}.bin'
            require(row["CacheDirectory"] == cache_name and row["CacheFile"] == filename, "owned immutable cache path")
            cache_path = directory / cache_name / filename
            require(not cache_path.is_symlink() and not cache_path.parent.is_symlink() and cache_path.is_file(), "owned regular immutable cache file")
            with cache_path.open("rb") as cache:
                cached = cache.read(policy["ChunkBytes"] + 1)
            require(cached == body, "actual binary cache not synthesized typed output")
            spool.insert(b"column-cache-name-" + name.encode(), filename.encode(), u64(ordinal))
            count = 0
            for typed, original in codec.column_chunk(body, m["Generation"], hashlib.sha256(go_json(binding)).digest(), ordinal, chains.get(name, bytes(32)), policy, spool):
                o = original["Outcome"]
                order = (o["Round"], o["Coordinator"], o["Request"])
                sequence = chunk_sequences.setdefault(name, [0, 0])
                census = chunk_history.setdefault(name, [0, 0])
                require((name not in chunk_order or chunk_order[name] < order) and o["Sequence"] == sequence[o["Coordinator"]] + 1, "column preserves actual complete CDC enumeration")
                chunk_order[name] = order
                last_outcome[name] = o
                publication_work[name] += codec.column_work(original, c)
                sequence[o["Coordinator"]] += 1
                for write in original["Writes"]:
                    census[write["Group"]] += 1
                coordinate = u64(typed["Provenance"])
                spool.insert(b"column-chunk-" + name.encode(), coordinate, go_json(typed))
                count += 1
            require(count == row["Rows"], "chunk actual decoded row census")
            chains[name] = hashlib.sha256(chains.get(name, bytes(32)) + hashlib.sha256(body).digest()).digest()
            spool.insert(b"column-chain-" + name.encode(), u64(ordinal), chains[name])
            chunks[name] = ordinal + 1
            chunk_rows[name] = chunk_rows.get(name, 0) + count
            retained[name] = retained.get(name, 0) + len(native_key) + len(body)
            publication_work[name] += len(body)
            spool.insert(b"column-length-" + name.encode(), u64(ordinal), u64(len(native_key) + len(body)))
        elif kind == "chunks-eof":
            name = row["Generation"]
            m = manifests[name]
            require(row["Manifest"] == m and chunks.get(name) == m["Chunks"] and chunk_rows.get(name) == m["Rows"] and retained.get(name) == m["Bytes"] and chains.get(name) == bytes(m["Chain"]) and
                    chunk_sequences[name] == m["Generation"]["Cut"]["Groups"] and chunk_history[name] == m["History"] and publication_work[name] <= policy["MaxWorkBytes"], "complete actual binary chunk EOF/work")
            directory_path = directory / ("column-cache-" + name)
            count = 0
            with os.scandir(directory_path) as entries:
                for entry in entries:
                    require(not entry.is_symlink() and entry.is_file(follow_symlinks=False) and spool.find(b"column-cache-name-" + name.encode(), entry.name.encode()) is not None, "no foreign/trailing cache files")
                    count += 1
            require(count == m["Chunks"], "complete streamed cache directory census")
            spool.insert(b"column-eof", name.encode(), b"1")
        elif kind == "walk-row":
            name, generation, typed = row["Name"], row["Generation"], row["Row"]
            require(name in (generation, generation + "-cache") and generation in manifests and spool.find(b"column-walk-eof", name.encode()) is None and spool.lookup(b"column-eof", generation.encode()) == b"1", "actual native/cache walk mode/EOF")
            require(spool.lookup(b"column-chunk-" + generation.encode(), u64(typed["Provenance"])) == go_json(typed), "typed walk equals actual original binary chunk")
            original = decoded(blob(typed["Original"]))
            order = (typed["Round"], typed["Coordinator"], original["Tx"]["Request"])
            require(name not in walk_order or walk_order[name] < order, "deterministic complete typed walk order")
            walk_order[name] = order
            spool.insert(b"column-walk-" + name.encode(), u64(typed["Provenance"]), b"1")
            walks[name] = walks.get(name, 0) + 1
        elif kind == "walk-eof":
            name, generation = row["Name"], row["Generation"]
            require(row["Manifest"] == manifests[generation] and row["Cut"] == manifests[generation]["Generation"]["Cut"] and row["Rows"] == walks.get(name) == manifests[generation]["Rows"], "complete typed/cache EOF at exact cut")
            spool.insert(b"column-walk-eof", name.encode(), b"1")
        elif kind == "point":
            name, owner = row["Generation"], integer(row["Owner"], 1)
            found, typed = row["Found"], row["Row"]
            require(name in manifests and type(found) is bool and owner in (7, 7 + 2 * (p["facts"] - 1), 5), "fixed actual column sample identity")
            if found:
                require(owner != 5, "phantom column point must be exact empty")
                codec.column_typed_row(typed, spool)
                require(typed["Owner"] == owner and spool.lookup(b"column-chunk-" + name.encode(), u64(typed["Provenance"])) == go_json(typed), "actual retained point lineage")
                command = codec.fdb_contract(p, 4 + (owner - 7) // 2)[0]
                edge = command["Edge"]
                logical = "edge/" + "/".join(f"{v:016x}" for v in (edge["ID"], edge["Source"]["Node"], edge["Source"]["Life"], edge["Target"]["Node"], edge["Target"]["Life"]))
                expected = spool.find(b"fdb-old" if name == "old" else b"fdb-current", key(c, 0, "current", logical))
                require(expected is not None and decoded(expected)["Value"] == typed["Value"] and decoded(expected)["Round"] == typed["Round"] and decoded(expected)["TxID"] == decoded(blob(typed["Original"]))["Tx"]["ID"], "column point equals exact complete source revision")
            else:
                require(owner == 5, "no false exact-empty point")
                require(typed == dict.fromkeys("Kind Owner Life SourceNode SourceLife TargetNode TargetLife Value ValueID Provenance Round Sequence Coordinator".split(), 0) | {"Original": None}, "exact-empty point has no invented typed row")
            spool.insert(b"column-point", name.encode() + u64(owner), b"1")
            point_count += 1
        elif kind == "refusal":
            require(len(refusals) < 4, "bounded fixed refusal controls")
            raw.column_refusal(row, end, manifests, held, token, spool, len(refusals), collected, deleted, len(stages))
            refusals.append(row["Name"])
        elif kind == "collect":
            require(row["Budget"] == {"Chunks": 1, "Bytes": 128 << 10, "OutputBytes": 1024}, "separate bounded derived collector policy")
            page = row["Result"]
            size = int.from_bytes(spool.lookup(b"column-length-old", u64(collected)), "big")
            deleted += size
            collected += 1
            m = manifests["old"]
            require(page == {"Generation": "old", "Cursor": collected, "DeletedBytes": deleted, "WorkChunks": 1, "WorkBytes": size, "Done": collected == m["Chunks"]} and
                    blob(row["Absent"]) == raw.column_key(c, "old", f"chunk/{collected - 1:016x}"), "actual atomic derived deletion/cursor/accounting")
            guard = decoded(blob(row["Guard"]["Value"]))
            raw_record(row["Guard"], raw.column_key(c, "old", "owners"), go_json(guard))
            require(guard["Manifest"] == list(hashlib.sha256(go_json(m)).digest()) and bytes(guard["Chain"]) == spool.lookup(b"column-chain-old", u64(collected - 1)) and
                    guard["References"] == guard["Pins"] == guard["Readers"] == 0 and guard["Cursor"] == collected and guard["DeletedBytes"] == deleted and guard["State"] == (3 if page["Done"] else 2), "same guard authentic owner-free partial/resume state")
        elif kind == "storage-row":
            native_key, body = blob(row["Record"]["Key"]), blob(row["Record"]["Value"])
            require(len(stages) == 6 and (storage_previous is None or storage_previous < native_key) and native_key.startswith(prefix(c) + b"columns-v1/") and not native_key.startswith(raw.column_key(c, "old", "chunk/")), "retired chunks absent; source retained separately; ordered complete terminal census")
            storage_previous = native_key
            if native_key.startswith(raw.column_key(c, "final", "chunk/")):
                require(body == spool.lookup(b"column-native-chunk", native_key), "terminal physical chunk equals original hash-bound bytes")
            elif native_key.startswith(prefix(c) + b"columns-v1/read-owner/") and native_key != held:
                reader = decoded(body)
                codec.column_reader(reader, manifests, None, False)
                require(reader["State"] == 2 and native_key == prefix(c) + b"columns-v1/read-owner/" + reader["Owner"]["Request"].encode().hex().encode(), "all other read owners authentically closed")
            else:
                require(body == spool.lookup(b"column-control", native_key), "complete terminal physical control state")
            spool.insert(b"column-storage", native_key, body)
            storage_count += 1
            storage_bytes += len(native_key) + len(body)
            size = len(native_key) + len(body)
            storage["Records"] += 1
            storage["Bytes"] += size
            if native_key.startswith(raw.column_key(c, "final", "chunk/")):
                live_chunks += 1
                live_bytes += size
                storage["LiveChunkRecords"] += 1
                storage["LiveChunkBytes"] += size
            elif native_key.startswith(raw.column_key(c, "old", "")):
                storage["RetiredLineageRecords"] += 1
                storage["RetiredLineageBytes"] += size
            elif native_key.startswith(prefix(c) + b"columns-v1/read-owner/") or native_key == prefix(c) + b"columns-v1/retention-v1/pin/" + b"hold-old".hex().encode():
                storage["TombstoneRecords"] += 1
                storage["TombstoneBytes"] += size
            if native_key.startswith(prefix(c) + b"columns-v1/read-owner/"):
                reader = decoded(body)
                codec.column_reader(reader, manifests, token, native_key == held)
                require(reader["State"] == 2, "all terminal actual owners are closed")
                readers[reader["Owner"]["Generation"]] += 1
        else:
            require(False, "unknown derived column frame")
    require(header and footer and collected == manifests["old"]["Chunks"] and deleted == manifests["old"]["Bytes"] and len(stages) == 6 and
            refusals == ["expired-new-open", "held-old-collection", "retired-publication", "final-reference-collection"], "missing complete production column lifetime")
    require(codec.sha_array(column_end["ArtifactSHA256"]) == whole.digest(), "complete column artifact raw hash")
    require(column_end["Storage"] == storage and readers == {"old": 4, "final": 4} and storage["TombstoneRecords"] == 9 and storage["RetiredLineageRecords"] == 3 and sum(walks.values()) == 2 * sum(m["Rows"] for m in manifests.values()), "exact live/tombstone/source-lineage bytes and complete walks")
    for native_key, body in spool.walk(b"column-control"):
        require(spool.lookup(b"column-storage", native_key) == body, "missing terminal native control")
    for name in ("old", "final"):
        progress = decoded(spool.lookup(b"column-storage", raw.column_key(c, name, "progress")))
        require(progress["WorkBytes"] == publication_work[name] and progress["Previous"] == last_outcome[name], "actual publication work and original EOF")
        spool.lookup(b"column-eof", name.encode())
        for mode in (name, name + "-cache"):
            spool.lookup(b"column-walk-eof", mode.encode())


def validate_result(end):
    version = integer(end["Version"], 1, 2)
    fields(end, "Version Format Spec Config Binding BindingRecord Initial Old Final Attempts Aborted Committed Setup Creates Updates Observations Clients Sources AttemptSHA256 EventSHA256" + (" Columns" if "Columns" in end else "") + (" Transport" if version == 2 else ""))
    require(end["Format"] == FORMAT.replace("-v1", f"-v{version}"), "fresh selector result only")
    p, c = end["Spec"], end["Config"]
    fields(p, "version family facts crossPercent writers layout")
    n = integer(p["facts"], 20, 1_000_000)
    integer(p["version"], version, version)
    integer(p["writers"], 1, 32)
    require(p["version"] == version and p["family"] == FAMILY and n % 20 == 0 and p["writers"] in (1, 8, 32) and n >= p["writers"] + 1 and p["crossPercent"] in (0, 10, 50, 100) and type(p["crossPercent"]) is int and p["layout"] in ("kv", "columns"), "actual implemented selector dimensions")
    graph_prefix = f"graph-workload-selectors-v{version}-" + p["layout"] + "-"
    run = c["Graph"].removeprefix(graph_prefix)
    require(c["Graph"].startswith(graph_prefix) and len(run) == 32 and bytes.fromhex(run).hex() == run and bytes.fromhex(run) != bytes(16), "explicit fresh persisted run identity")
    originals = 3 + 2 * n * 64
    require(same(c, {"Format": 2, "Graph": c["Graph"], "Topology": 1, "Epochs": [7, 11],
                    "Limits": {"Bytes": (64 << 20) + n * (64 << 10) + originals * 2048, "Transactions": originals, "Versions": 3 + 6 * n, "Certificates": 1024},
                    "Nodes": [1, 7 + 2 * n], "Cost": {"version": 3, "facts": n, "crossPercent": p["crossPercent"], "writers": p["writers"]}, "CostScope": codec.SCOPE}), "fresh v3 binding cannot widen older profiles")
    binding = {"Version": 1, "Config": c, "Dataset": list(hashlib.sha256(b"\x01" + u64(n) + u64(p["crossPercent"]) + b"graph-v2").digest()),
               "Window": 32, "Attempts": 64, "Submissions": 8, "Callbacks": 16,
               "Read": {"PageRows": 8, "PageBytes": 4 << 20, "InventoryBytes": 4 << 20, "WorkRows": 1024 * n + 4096, "WorkBytes": (64 << 20) + n * (2 << 20), "ChainSteps": 128,
                        "Cleanup": {"Rows": 1024, "Bytes": 64 << 20, "Millis": 30000}},
               "History": {"MaxOwners": 64, "MaxControlBytes": 4 << 20, "PageRows": 32, "PageBytes": 4 << 20, "MaxWorkRows": 32 * n + 4096,
                           "MaxWorkBytes": (64 << 20) + n * (1 << 20), "MaxChainSteps": 128, "MaxOutputBytes": 4096}}
    if version == 2:
        binding.update(Version=2, Admission=raw.transport.family_record(end["Transport"], p, c))
    require(end["Binding"] == binding, "actual finite opening/scan/cleanup/history admission")
    raw_record(end["BindingRecord"], key(c, 0, f"workload-selectors-v{version}", "binding"), go_json(binding))
    for name, groups in (("Initial", [0, 0]), ("Old", [2 + n, 1]), ("Final", [2 + 2 * n, 1])):
        cut = end[name]
        fields(cut, "Graph Topology Epochs Scope Round Groups")
        require(cut["Graph"] == c["Graph"] and cut["Topology"] == 1 and cut["Epochs"] == c["Epochs"] and blob(cut["Scope"]) == b"\0\1" and cut["Groups"] == groups, "native complete phase/cut membership")
        integer(cut["Round"], 1)
    require(end["Initial"]["Round"] < end["Old"]["Round"] < end["Final"]["Round"], "mutation-then-old-query fixture")
    require(end["Setup"] == 3 and end["Creates"] == end["Updates"] == n and end["Committed"] == 3 + 2 * n and end["Observations"] == 11 and
            integer(end["Aborted"]) + end["Committed"] == end["Attempts"] <= originals, "authentic attempt/useful/observation census")
    require(len(end["Clients"]) == 32 and all(type(v) is int and v >= 2 for v in end["Clients"][:p["writers"]]) and all(type(v) is int and v == 0 for v in end["Clients"][p["writers"]:]) and sum(end["Clients"]) == 2 * n, "real independent callers in both phases")
    require(("Columns" in end) == (p["layout"] == "columns"), "actual layout artifact required")
    for digest in (end["AttemptSHA256"], end["EventSHA256"]):
        require(type(digest) is str and len(digest) == 64 and bytes.fromhex(digest).hex() == digest, "sealed raw digest")


def metadata(end, spool, header):
    c = end["Config"]
    maximum = dict.fromkeys(("Floor", "Generation", "Versions", "Transactions", "Certificates", "Bytes", "Groups"), (1 << 64) - 1)
    for phase, cert_counts in (("old", [3, 1]), ("final", [4, 1])):
        cut = end[phase.title()]
        transactions, pair_bytes = decoded(spool.lookup(b"original-cost", phase.encode()))
        generation, versions = decoded(spool.lookup(b"source-count", phase.encode()))
        source_bytes = decoded(spool.lookup(b"source-cost", phase.encode()))
        charged = [len(key(c, g, "meta", "config")) + len(go_json(c)) + len(key(c, g, "meta", "state")) + len(go_json(maximum)) for g in range(2)]
        charged[0] += len(blob(end["BindingRecord"]["Key"])) + len(blob(end["BindingRecord"]["Value"]))
        counts = [0, 0]
        for native_key, body in spool.walk(b"native-certificate"):
            cert = decoded(body)
            if cert["Round"] <= cut["Round"]:
                g = blob(cert["Scope"])[0]
                counts[g] += 1
                charged[g] += len(native_key) + len(body)
        require(counts == cert_counts, "complete native certificate inventory at real phase barrier")
        actual = decoded(spool.lookup(b"barrier-metadata", phase.encode()))
        for g in range(2):
            charged[g] += pair_bytes[g] + sum(part[g] for part in source_bytes)
            expected = {"Floor": cut["Round"], "Generation": generation[g], "Versions": versions[g], "Transactions": transactions[g], "Certificates": cert_counts[g], "Bytes": charged[g], "Groups": cut["Groups"][g]}
            require(actual[g] == expected, "exact source bytes/versions/certificates including authentic ABORT pairs")
        if phase == "final":
            require(actual == header["Metadata"], "raw final EOF metadata equals authentic native barrier")
    # Every selection exposes an actual post-acquisition metadata observation.
    # The held-old event retains its ORIGINAL opening proof, not a later read.
    for name in (*OBSERVATIONS, "raw-source-held"):
        initial = name == "initial-empty"
        old = name.startswith("old-") or name == "held-old-after-update"
        phase = "old" if old else "final"
        expected = [dict(m) for m in decoded(spool.lookup(b"barrier-metadata", phase.encode()))]
        certificates = [end["Initial"]]
        if not initial:
            certificates.append(end["Old"])
        if not initial and name not in ("old-fresh", "held-old-after-update"):
            certificates.append(end["Old"] | {"Scope": "AA=="})
        if not initial and name not in ("old-fresh", "held-old-after-update", "old-scope-0"):
            certificates.append(end["Old"] | {"Scope": "AQ=="})
        if name in ("after-current", "fresh-current", "raw-source-held"):
            certificates.append(end["Final"])
        for g in range(2):
            current = expected[g]
            # Remove the phase barrier's complete certificate charge, then add
            # only certificates genuinely published at this opening position.
            barrier_cut = end[phase.title()]
            for _, body in spool.walk(b"native-certificate"):
                cert = decoded(body)
                if cert["Round"] <= barrier_cut["Round"] and blob(cert["Scope"])[0] == g:
                    current["Bytes"] -= len(key(c, g, "cut", body.hex())) + len(body)
            current["Certificates"] = 0
            for cert in certificates:
                if blob(cert["Scope"])[0] == g:
                    body = go_json(cert)
                    current["Bytes"] += len(key(c, g, "cut", body.hex())) + len(body)
                    current["Certificates"] += 1
            if initial:
                current.update(Floor=end["Initial"]["Round"], Generation=0, Versions=0, Transactions=0, Groups=0)
                current["Bytes"] = len(key(c, g, "meta", "config")) + len(go_json(c)) + len(key(c, g, "meta", "state")) + len(go_json(maximum))
                if g == 0:
                    body = go_json(end["Initial"])
                    current["Bytes"] += len(key(c, 0, "cut", body.hex())) + len(body) + len(blob(end["BindingRecord"]["Key"])) + len(blob(end["BindingRecord"]["Value"]))
            elif not old and name not in ("after-current", "fresh-current", "raw-source-held"):
                current["Floor"] = decoded(spool.lookup(b"source-floor", b"final"))[g]
        require(decoded(spool.lookup(b"selection-metadata", name.encode())) == expected, "exact actual selection metadata at its native lifecycle position")


def latencies(directory, end, spool, filename="fdb-selectors-latency.jsonl"):
    count = 0
    for row, _ in lines(directory / filename, end["Committed"]):
        fields(row, "Phase Fact Client Nanos Access")
        integer(row["Nanos"], 1, (1 << 63) - 1)
        phase, fact = row["Phase"], integer(row["Fact"])
        ordinal = 1 + fact if phase == "setup" else 4 + fact + (end["Spec"]["facts"] if phase == "update" else 0)
        require(phase in ("setup", "create", "update") and fact < (3 if phase == "setup" else end["Spec"]["facts"]), "actual operation latency identity")
        original = decoded(spool.lookup(b"useful-attempt", u64(ordinal)))
        require(original["Operation"]["Phase"] == phase and original["Operation"]["Fact"] == fact and original["ClientID"] == row["Client"] and row["Access"] == "actual admission through business attempt/replan/verification; not selector point latency", "actual admission/attempt latency, no indexed read claim")
        spool.insert(b"latency-seen", u64(ordinal), b"1")
        count += 1
    require(count == end["Committed"], "one actual latency per useful operation including setup/seeds")


def selector_timings(spool):
    # At most eleven family observations: this is fixed control proof state,
    # independent of graph size/writers. Five fresh repetitions aggregate later.
    samples, aggregate = [], {}
    for name in OBSERVATIONS:
        scan = decoded(spool.lookup(b"selector-scan", name.encode()))
        samples.append({"name": name, "call": "Scan", **scan})
        aggregate.setdefault(("Scan", scan["Mode"]), []).append(scan["Nanos"])
        if name != "held-old-after-update":
            opening = decoded(spool.lookup(b"selector-open", name.encode()))
            require(opening["Cut"] == scan["Cut"] and opening["Mode"] == scan["Mode"], "timed open/complete scan cardinality join")
            samples.append({"name": name, "call": "Open", "Mode": scan["Mode"], "Cut": scan["Cut"], "Rows": scan["Rows"], "Nanos": opening["Nanos"]})
            aggregate.setdefault(("Open", scan["Mode"]), []).append(opening["Nanos"])
    points = decoded(spool.lookup(b"selector-points", b"fresh-current"))
    samples.append({"name": "fresh-current", "call": "Points", **points})
    aggregate[("Points", "Fresh")] = [points["Nanos"]]
    percentiles = []
    for (call, mode), values in sorted(aggregate.items()):
        values.sort()
        percentiles.append({"call": call, "mode": mode, "samples": len(values),
                            **{f"p{p}Nanos": values[(p * len(values) + 99) // 100 - 1] for p in (50, 95, 99)}})
    return {"samples": samples, "nearestRank": percentiles,
            "scope": "actual Open retry/wait, Scan through EOF including artifact callbacks, and separate fixed scan-based Points; finite per-cell samples, no indexed/per-key latency claim"}


def verify(directory, spool_directory, policy=None):
    directory = Path(directory).resolve(strict=True)
    with (directory / "fdb-selectors-result.json").open("rb") as source:
        body = source.read(raw.LINE + 1)
    require(len(body) <= raw.LINE and body.endswith(b"\n"), "bounded exclusive result")
    end = decoded(body[:-1])
    validate_result(end)
    require(policy is not None or end["Spec"]["facts"] < 1_000_000 and end["Spec"]["writers"] != 32, "large cell needs explicit finite comparison grant")
    policy = POLICY if policy is None else policy
    with Spool(spool_directory, policy) as spool:
        attempts(directory, end, spool)
        header = source_fold(directory, end, spool)
        events(directory, end, spool, header)
        metadata(end, spool, header)
        columns(directory, end, spool)
        latencies(directory, end, spool)
        timing = selector_timings(spool)
        transport_proof = raw.transport.verify(directory, end["Transport"]["Policy"], spool) if end["Version"] == 2 else None
        report = spool.report()
    result = {"format": end["Format"], "family": FAMILY, "runGraph": end["Config"]["Graph"], "spec": end["Spec"], "attempts": end["Attempts"],
            "committed": end["Committed"], "aborted": end["Aborted"], "spool": report, "selectorTiming": timing,
            "scope": "one actual FDB selectors/source cell; opening reservations and successful-page logical debit; scan-based points; source retain-all and derived GC; no native parity/RTT/matrix/capacity acceptance"}
    if transport_proof is not None:
        transport_proof["nativeRetainedBindingBytes"] = len(blob(end["Transport"]["Record"]["Key"])) + len(blob(end["Transport"]["Record"]["Value"]))
        result["transport"] = transport_proof
    return result


if __name__ == "__main__":
    try:
        if len(sys.argv) not in (3, 5):
            raise ValueError("usage: fdb_selectors_check.py ARTIFACTS OWNED_SPOOL_DIR [DISK_BYTES CACHE_BYTES]")
        policy = None if len(sys.argv) == 3 else Policy(int(sys.argv[3]), int(sys.argv[4]), 4096, raw.LINE)
        print(json.dumps(verify(sys.argv[1], sys.argv[2], policy), indent=2))
    except (ValueError, KeyError, TypeError, OSError) as error:
        print("selector evidence refused: " + str(error), file=sys.stderr)
        sys.exit(1)
