#!/usr/bin/env python3
"""Private actual snapshot/feed oracle; no runtime or capacity qualification.

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
_spec = importlib.util.spec_from_file_location("feed_raw_codec", Path(__file__).resolve().parent / "fdb/fdb_contention_check.py")
raw = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(raw)

codec = raw._codec
require, fields, integer = codec.require, codec.fields, codec.integer
same, go_json, blob = codec.same, codec.go_json, codec.raw_bytes
decoded, lines, key, prefix, raw_record = raw.decoded, raw.lines, raw.key, raw.prefix, raw.raw_record
Spool, Policy, u64 = raw.Spool, raw.Policy, raw.u64
FORMAT, FAMILY = "fdb-snapshot-feed-workload-v1", "snapshot-to-feed"
POLICY = Policy(1 << 30, 8 << 20, 4096, 512 << 10)


def operation(p, value):
    fields(value, "Dataset Phase Fact Ordinal")
    ordinal = codec.parallel_operation({**p, "version": 1}, value)
    phase, fact, _, _ = codec.fixture(p, ordinal)
    return ordinal, phase, fact


def attempt(a, end, spool):
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
    name = f"feed-{phase}-{fact}-c{client}-a{number}"
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


def attempts(directory, end, spool):
    digest, counts, footer = hashlib.sha256(), [0, 0, 0], False
    clients = [0] * 32
    original_cost = {"old": [[0, 0], [0, 0]], "final": [[0, 0], [0, 0]]}
    for frame, line in lines(directory / "fdb-feed-attempts.jsonl", end["Config"]["Limits"]["Transactions"] + 1):
        require(not footer, "attempt after EOF")
        if frame.get("Type") == "footer":
            require(frame == {"Type": "footer", "Attempts": counts[0], "Aborted": counts[1], "Committed": counts[2], "PayloadSHA256": digest.hexdigest()} and digest.hexdigest() == end["AttemptSHA256"], "attempt EOF/raw hash")
            footer = True
            continue
        fields(frame, "Type Attempt")
        require(frame["Type"] == "attempt", "unknown attempt frame")
        ordinal, outcome = attempt(frame["Attempt"], end, spool)
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
    for ordinal in range(1, 4 + 2 * end["Spec"]["facts"]):
        final = decoded(spool.lookup(b"attempt-chain", u64(ordinal)))
        require(final["Classification"] == "committed", "exactly one useful completion per operation")
    for phase, costs in original_cost.items():
        spool.insert(b"original-cost", phase.encode(), go_json(costs))


def raw_stream(directory, name, end):
    digest, counts, previous, footer, header = hashlib.sha256(), [0, 0], None, False, None
    for frame, line in lines(directory / ("fdb-feed-" + name + ".jsonl"), 6 * end["Spec"]["facts"] + 6):
        require(not footer, "raw row after EOF")
        if header is None:
            header = frame
            fields(header, "Type Format Stream Config InitialCut OldCut FinalCut Policy Metadata ConfigRecords MetadataRecords CutRecords")
            require(header["Type"] == "header" and header["Format"] == FORMAT and header["Stream"] == name and header["Config"] == end["Config"] and header["InitialCut"] == end["Initial"] and header["OldCut"] == end["Old"] and header["FinalCut"] == end["Final"] and header["Policy"] == {"PageRows": 32, "PageBytes": 4 << 20}, "fresh source header/scope/policy")
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


def source_fold(directory, end, spool):
    p, c = end["Spec"], end["Config"]
    physical, definitions = codec.StoredState(spool, b"fdb-before"), set()
    generations, sequences, versions = [0, 0], [0, 0], [0, 0]
    floors = [end["Old"]["Round"]] * 2
    old_generations, old_versions = [0, 0], [0, 0]
    header = None
    cdc_bytes = {"old": [0, 0], "final": [0, 0]}
    history_bytes = {"old": [0, 0], "final": [0, 0]}
    for kind, row in raw_stream(directory, "cdc", end):
        if kind == "header":
            header = row
            continue
        group = decoded(row["Value"])
        ordinal = integer(group["Tx"]["Lifecycle"]["Provenance"], 1, 3 + 2 * p["facts"])
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
        spool.insert(b"feed-sequence",bytes([o["Coordinator"]])+u64(o["Sequence"]),row["Value"])
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
                codec.put_raw(spool,b"feed-replica",key(c,g,"current",logical),body)
            versions[g] += 1
            if ordinal <= 3 + p["facts"]:
                old_versions[g] += 1
    require(header is not None and sequences == end["Final"]["Groups"], "complete native coordinator prefix")
    for ordinal in range(1, 4 + 2 * p["facts"]):
        spool.lookup(b"cdc-seen", u64(ordinal))
    for name, expected_count in (("history", 3 + 6 * p["facts"]), ("current", 3 + 3 * p["facts"])):
        count = 0
        for kind, row in raw_stream(directory, name, end):
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


def columns(directory, end, spool):
    path = directory / "fdb-feed-columns.jsonl"
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
            require(final == column_end | {"ArtifactSHA256": [0] * 32} and final["Format"] == final["SourceFormat"] == FORMAT and final["ReferenceLayout"] == "native-logical-kv-reference" and
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
            require(row["Type"] == "header" and row["Format"] == FORMAT and row["Config"] == c and len(row["Records"]) == 2, "fresh actual column binding header")
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
    fields(end, "Version Format Spec Config Binding BindingRecord Initial Old Final Attempts Aborted Committed Setup Creates Updates Observations Clients Sources Feed AttemptSHA256 EventSHA256" + (" Columns" if "Columns" in end else ""))
    require(integer(end["Version"], 1, 1) == 1 and end["Format"] == FORMAT, "fresh selector result only")
    p, c = end["Spec"], end["Config"]
    fields(p, "version family facts crossPercent writers layout")
    n = integer(p["facts"], 20, 1_000_000)
    integer(p["version"], 1, 1)
    integer(p["writers"], 1, 32)
    require(p["version"] == 1 and p["family"] == FAMILY and n % 20 == 0 and p["writers"] in (1, 8, 32) and n >= p["writers"] + 1 and p["crossPercent"] in (0, 10, 50, 100) and type(p["crossPercent"]) is int and p["layout"] in ("kv", "columns"), "actual implemented selector dimensions")
    graph_prefix = "graph-workload-feed-v1-" + p["layout"] + "-"
    run = c["Graph"].removeprefix(graph_prefix)
    require(c["Graph"].startswith(graph_prefix) and len(run) == 32 and bytes.fromhex(run).hex() == run and bytes.fromhex(run) != bytes(16), "explicit fresh persisted run identity")
    originals = 3 + 2 * n * 64
    require(same(c, {"Format": 2, "Graph": c["Graph"], "Topology": 1, "Epochs": [7, 11],
                    "Limits": {"Bytes": (64 << 20) + n * (64 << 10) + originals * 2048, "Transactions": originals, "Versions": 3 + 6 * n, "Certificates": 1024},
                    "Nodes": [1, 7 + 2 * n], "Cost": {"version": 5, "facts": n, "crossPercent": p["crossPercent"], "writers": p["writers"]}, "CostScope": codec.SCOPE}), "fresh v5 binding cannot widen older profiles")
    binding = {"Version": 1, "Config": c, "Dataset": list(hashlib.sha256(b"\x01" + u64(n) + u64(p["crossPercent"]) + b"graph-v2").digest()),
               "Window": 32, "Attempts": 64, "Submissions": 8, "Callbacks": 16,
               "Read": {"PageRows": 8, "PageBytes": 4 << 20, "InventoryBytes": 4 << 20, "WorkRows": 1024 * n + 4096, "WorkBytes": (64 << 20) + n * (2 << 20), "ChainSteps": 128,
                        "Cleanup": {"Rows": 1024, "Bytes": 64 << 20, "Millis": 30000}},
               "History": {"MaxOwners": 64, "MaxControlBytes": 4 << 20, "PageRows": 32, "PageBytes": 4 << 20, "MaxWorkRows": 32 * n + 4096,
                           "MaxWorkBytes": (64 << 20) + n * (1 << 20), "MaxChainSteps": 128, "MaxOutputBytes": 4096}}
    require(end["Binding"] == binding, "actual finite opening/scan/cleanup/history admission")
    raw_record(end["BindingRecord"], key(c, 0, "workload-feed-v1", "binding"), go_json(binding))
    for name, groups in (("Initial", [0, 0]), ("Old", [2 + n, 1]), ("Final", [2 + 2 * n, 1])):
        cut = end[name]
        fields(cut, "Graph Topology Epochs Scope Round Groups")
        require(cut["Graph"] == c["Graph"] and cut["Topology"] == 1 and cut["Epochs"] == c["Epochs"] and blob(cut["Scope"]) == b"\0\1" and cut["Groups"] == groups, "native complete phase/cut membership")
        integer(cut["Round"], 1)
    require(end["Initial"]["Round"] < end["Old"]["Round"] < end["Final"]["Round"], "mutation-then-old-query fixture")
    require(end["Setup"] == 3 and end["Creates"] == end["Updates"] == n and end["Committed"] == 3 + 2 * n and end["Observations"] == 3 and
            integer(end["Aborted"]) + end["Committed"] == end["Attempts"] <= originals, "authentic attempt/useful/observation census")
    require(len(end["Clients"]) == 32 and all(type(v) is int and v >= 2 for v in end["Clients"][:p["writers"]]) and all(type(v) is int and v == 0 for v in end["Clients"][p["writers"]:]) and sum(end["Clients"]) == 2 * n, "real independent callers in both phases")
    require(("Columns" in end) == (p["layout"] == "columns"), "actual layout artifact required")
    for digest in (end["AttemptSHA256"], end["EventSHA256"]):
        require(type(digest) is str and len(digest) == 64 and bytes.fromhex(digest).hex() == digest, "sealed raw digest")


def latencies(directory, end, spool):
    count = 0
    for row, _ in lines(directory / "fdb-feed-latency.jsonl", end["Committed"]):
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
    candidates = (spool.lookup(b"fdb-old", native_key), spool.lookup(b"fdb-current", native_key)) if name == "racing-old" else (spool.lookup(b"fdb-current", native_key),)
    require(go_json(head) in candidates, "actual bounded scan page observed genuine pre/post-write current head")
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


def controls(values, allow_absent=False):
    require(type(values) is list and len(values) <= 24, "fixed native control inventory")
    result = {}
    for record in values:
        fields(record, "Key Value")
        k, v = blob(record["Key"]), (b"" if allow_absent and record["Value"] is None else blob(record["Value"]))
        require(0 < len(k) <= 1024 and len(v) <= 64 << 10 and (v or allow_absent) and k not in result, "bounded unique native control record")
        result[k] = v
    return result


def feed_key(c, kind, name):
    return key(c, 0, "feed-v1/" + kind, name)


def feed_policy(end):
    n = end["Spec"]["facts"]
    pages = 3 + 2 * n
    calls = 8 * (2 * pages + 32)
    return {"PageRows": 8, "PageBytes": 64 << 10, "MaxPages": pages, "MaxCalls": calls,
            "MaxControlBytes": (64 << 20) + pages * (2 * (64 << 10) + 8192), "MaxWorkRows": calls * (4096 + 128 * 8),
            "MaxWorkBytes": calls * (128 << 20), "Read": end["Binding"]["Read"]}


def feed_original(end):
    summary, c = end["Feed"], end["Config"]
    fields(summary, "Original Token Pages AcknowledgedRows ReplayedPages Completed Processes KilledReaped ReplacementReaped Storage")
    o, token = summary["Original"], summary["Token"]
    fields(o, "Version Request Secret Policy Selection Cut Pin Snapshot Opening DeadlineNanos")
    fields(token, "Request Secret Original DeadlineNanos")
    require(o["Version"] == 1 and o["Request"] == token["Request"] == "workload" and codec.sha_array(o["Secret"]) == codec.sha_array(token["Secret"]) and any(o["Secret"]) and
            codec.sha_array(token["Original"]) == hashlib.sha256(go_json(o)).digest() and token["DeadlineNanos"]==o["DeadlineNanos"] and o["Policy"] == feed_policy(end) and o["Cut"] == end["Old"], "native immutable feed original/policy/cut/digest")
    integer(o["DeadlineNanos"], 1, (1 << 63) - 1)
    zero = {"Graph": "", "Topology": 0, "Epochs": [0, 0], "Scope": None, "Round": 0, "Groups": [0, 0]}
    require(o["Selection"] == {"Mode": "Fresh", "Scope": "AAE=", "At": zero} and all(v == 0 for v in o["Opening"].values()), "Begin is genuine native Fresh intent; feed control debit separate from snapshot work")
    for name, kind in (("Pin", "pin"), ("Snapshot", "read")):
        owner = o[name]
        fields(owner, "Kind Request Cut Required Secret State PinRequest PinDigest Lineage TerminalRequest")
        require(owner["Kind"] == kind and owner["State"] == 1 and owner["Cut"] == end["Old"] and owner["Required"] == end["Initial"] and owner["TerminalRequest"] == "" and any(codec.sha_array(owner["Secret"])) and not any(codec.sha_array(owner["Lineage"])), "factory-issued original owner and complete physical prefix")
    pin = o["Pin"]
    require(pin["Request"] == "feed-workload" and pin["PinRequest"] == "" and not any(pin["PinDigest"]) and codec.sha_array(pin["Secret"]) == hashlib.sha256(b"rho-fdb-feed-v1/pin" + bytes(o["Secret"])).digest(), "pin original exact derived capability")
    reader = o["Snapshot"]
    pin_token = {"Request": pin["Request"], "Cut": pin["Cut"], "Secret": pin["Secret"]}
    require(reader["PinRequest"] == pin["Request"] and bytes(reader["PinDigest"]) == hashlib.sha256(go_json(pin_token)).digest() and reader["Request"] == "source-read-" + bytes(reader["Secret"])[:16].hex(), "snapshot pin and native factory identity")
    require(summary["AcknowledgedRows"] == end["Spec"]["facts"] and 1 <= summary["Pages"] <= o["Policy"]["MaxPages"] and summary["ReplayedPages"] == 2 and summary["Completed"] == end["Final"] and summary["KilledReaped"] is summary["ReplacementReaped"] is True and len(summary["Processes"]) == 2 and summary["Processes"][0] != summary["Processes"][1], "complete finite process and replay census")
    for pid in summary["Processes"]:
        integer(pid, 1, (1 << 31) - 1)


def feed_cursor(c, end):
    fields(c, "Version Original State Positions Completed Last Pages Calls WorkRows WorkBytes Pending TerminalRequest")
    p = feed_policy(end)
    require(c["Version"] == 1 and c["Original"] == end["Feed"]["Token"]["Original"] and integer(c["State"], 0, 3) >= 0 and type(c["Pending"]) is type(c["TerminalRequest"]) is str, "exact cursor authority")
    integer(c["Pages"], 0, p["MaxPages"])
    calls = integer(c["Calls"], 1, p["MaxCalls"])
    require(c["WorkRows"] == calls * (4096 + 128 * p["PageRows"]) and c["WorkBytes"] == calls * (128 << 20), "durable prepaid control work cannot reset on reopen/replay")
    require(len(c["Positions"]) == 2 and all(end["Old"]["Groups"][g] <= integer(c["Positions"][g]) <= end["Final"]["Groups"][g] for g in range(2)) and c["Completed"] in (end["Old"], end["Final"]), "complete exact native boundary vectors")


def feed_page(delivery, end, spool, expected_from=None):
    fields(delivery, "Page Record Replay")
    p, c, o = delivery["Page"], end["Config"], end["Feed"]["Original"]
    fields(p, "Version Request Original From Next Boundary Last Rows EOF")
    body = blob(delivery["Record"]["Value"])
    require(type(delivery["Replay"]) is bool and p["Version"] == 1 and p["Original"] == end["Feed"]["Token"]["Original"] and p["Boundary"] == end["Final"] and type(p["EOF"]) is bool and
            0 < len(body) <= o["Policy"]["PageBytes"] and body == go_json(p) and type(p["Rows"]) is list and len(p["Rows"]) <= o["Policy"]["PageRows"], "genuine bounded immutable page")
    raw_record(delivery["Record"], feed_key(c, "page/workload", p["Request"]), body)
    prior = spool.find(b"feed-page", p["Request"].encode())
    if prior is not None:
        require(prior == body and delivery["Replay"], "duplicate page must be byte-identical explicit replay")
        return p, False
    require(expected_from is not None and p["From"] == expected_from, "no skipped pending/ACK vector")
    next_ = list(expected_from)
    last = decoded(spool.lookup(b"feed-control-state", b"last"))
    for row in p["Rows"]:
        fields(row, "Group Key Value IDKey IDValue RequestKey RequestValue")
        group = decoded(blob(row["Value"]))
        original = group["Outcome"]
        g = integer(row["Group"], 0, 1)
        require(original["Coordinator"] == g and original["Sequence"] == next_[g] + 1 and original["Commit"] is True and end["Old"]["Round"] < original["Round"] <= end["Final"]["Round"], "feed exact per-coordinator original sequence after snapshot C")
        require(blob(row["Value"]) == spool.lookup(b"native-cdc", blob(row["Key"])), "feed complete original including actual before/after and request")
        order = original["Round"], g, original["Request"]
        if last["Round"]:
            require((last["Round"], last["Coordinator"], last["Request"]) < order, "feed total deterministic enumeration, independent of provenance")
        for field, ns in (("ID", b"attempt-id"), ("Request", b"attempt-request")):
            require(blob(row[field + "Key"]) == key(c, g, field.lower(), original[field]) and blob(row[field + "Value"]) == go_json(original) == spool.lookup(ns, blob(row[field + "Key"])), "feed exact paired originals")
        # Bounded two-head enumeration independently checks that another
        # coordinator's earlier group was not hidden behind an equal count.
        for other in range(2):
            if next_[other] >= end["Final"]["Groups"][other]:
                continue
            wanted = spool.lookup(b"feed-sequence", bytes([other]) + u64(next_[other] + 1))
            head = decoded(wanted)["Outcome"]
            require(order <= (head["Round"], other, head["Request"]), "earlier independent native head skipped")
        for write in group["Writes"]:
            native = key(c, write["Group"], "current", write["Key"])
            before = spool.find(b"feed-replica", native)
            require(before is not None and decoded(before) == write["Before"], "snapshot-plus-feed exact predecessor, no invented normalized value")
            spool.replace(b"feed-replica", native, before, go_json(write["After"]))
        spool.insert(b"feed-consumed", blob(row["Key"]), blob(row["Value"]))
        next_[g] += 1
        last = original
    require(p["Next"] == next_ and p["Last"] == last and p["EOF"] == (next_ == end["Final"]["Groups"]) and (p["Rows"] or p["EOF"]), "exact sealed page EOF/vector; no partial empty success")
    spool.replace(b"feed-control-state", b"last", spool.lookup(b"feed-control-state", b"last"), go_json(last))
    spool.insert(b"feed-page", p["Request"].encode(), body)
    return p, True


def feed_ack(value, page, end, spool, fresh):
    fields(value, "Ack Record Cursor")
    ack = value["Ack"]
    fields(ack, "Request Page From Next Completed")
    require(ack["Request"] == page["Request"] and bytes(ack["Page"]) == hashlib.sha256(go_json(page)).digest() and ack["From"] == page["From"] and ack["Next"] == page["Next"] and ack["Completed"] == end["Final" if page["EOF"] else "Old"], "ACK original hash/exact complete vectors only")
    raw_record(value["Record"], feed_key(end["Config"], "ack/workload", ack["Request"]), go_json(ack))
    feed_cursor(value["Cursor"], end)
    prior = spool.find(b"feed-ack", ack["Request"].encode())
    if fresh:
        require(prior is None and value["Cursor"]["Positions"] == ack["Next"] and value["Cursor"]["Completed"] == ack["Completed"] and value["Cursor"]["Pending"] == "", "ACK persisted native cursor before return")
        spool.insert(b"feed-ack", ack["Request"].encode(), go_json(ack))
    else:
        require(prior == go_json(ack), "duplicate ACK must retain the original, not later cursor as original")


def snapshot_selection(value, name, end, spool):
    fields(value, "Request Selected Config Metadata Retention Inventory InventoryRows InventoryBytes ReceiptPair ReceiptCDC AcquiredOwner Acquisition")
    cut = end["Old" if name in ("racing-old", "held-old-after-update") else "Final"]
    require(value["Selected"] == cut and blob(value["Request"]["Scope"]) == b"\0\1" and value["Request"]["Mode"] == ("At" if name == "raw-source-held" else "Fresh") and value["InventoryRows"] == value["InventoryBytes"] == 0 and not value["Inventory"] and not value["Retention"], "actual family retained selection, no synthetic Stable/feed export")
    fields(value["Request"],"Mode Scope At")
    zero_cut={"Graph":"","Topology":0,"Epochs":[0,0],"Scope":None,"Round":0,"Groups":[0,0]}
    require(value["Request"]["At"]==(cut if name=="raw-source-held" else zero_cut),"exact native At authority or genuine Fresh; no caller-cut relabel")
    empty_record={"Key":None,"Value":None}
    require(value["Config"]==value["Metadata"]==value["ReceiptPair"]==[empty_record,empty_record] and value["ReceiptCDC"]==empty_record,"unused selector inventory/receipt fields cannot carry foreign evidence")
    c = end["Config"]
    acquired = controls(value["Acquisition"])
    source = lambda n: key(c, 0, "source-retention-v1", n)
    mandatory = {source(n) for n in ("binding", "state", "ledger")} | {key(c, 0, "cut", go_json(cut).hex())} | {key(c, g, "meta", n) for g in range(2) for n in ("config", "state")}
    require(set(acquired) == mandatory and acquired[key(c, 0, "cut", go_json(cut).hex())] == go_json(cut), "complete native acquisition evidence")
    for g in range(2):
        require(acquired[key(c, g, "meta", "config")] == go_json(c), "actual persisted configuration")
    require(decoded(acquired[source("binding")]) == {"Version": 1, "Config": c, "Initial": end["Initial"], "Policy": end["Binding"]["History"]}, "source lifetime policy binding")
    state = decoded(acquired[source("state")])
    require(state == codec.fdb_retention_check.zero_state(integer(state["Owners"], 1, 64)), "actual acquired source ownership, no source GC relabel")
    owner = decoded(blob(value["AcquiredOwner"]["Value"]))
    fields(owner,"Kind Request Cut Required Secret State PinRequest PinDigest Lineage TerminalRequest")
    require(owner["Kind"] == "read" and owner["State"] == 1 and owner["Cut"] == cut and owner["Required"] == end["Initial"] and owner["TerminalRequest"] == "", "exact native retained owner")
    raw_record(value["AcquiredOwner"], key(c, 0, "source-owner-v1", owner["Request"]), go_json(owner))
    if name in ("racing-old", "held-old-after-update"):
        require(owner == end["Feed"]["Original"]["Snapshot"], "same original pin-bound snapshot across mutation")
    else:
        require(owner["PinRequest"] == "" and not any(owner["PinDigest"]) and owner["Request"] == "source-read-" + bytes(owner["Secret"])[:16].hex(), "genuine separate current/source factory")
    spool.insert(b"selection-metadata", name.encode(), go_json([decoded(acquired[key(c, g, "meta", "state")]) for g in range(2)]))
    return owner


def snapshot_work(value, end, feed_owned=False):
    fields(value, "Rows Bytes Callbacks OpeningRows OpeningBytes OpeningCallbacks CleanupRows CleanupBytes CleanupCallbacks")
    for v in value.values():
        integer(v)
    p = end["Binding"]["Read"]
    require(0 < value["OpeningRows"] <= value["Rows"] <= p["WorkRows"] and 0 < value["OpeningBytes"] <= value["Bytes"] <= p["WorkBytes"] and value["OpeningCallbacks"] <= value["Callbacks"] and value["OpeningRows"] == 64 * value["OpeningCallbacks"] and value["OpeningBytes"] == (4 << 20) * value["OpeningCallbacks"], "actual opening retry callbacks + cumulative logical Scan/Points admission")
    require(value["CleanupRows"] <= (8192 if feed_owned else p["Cleanup"]["Rows"]) and value["CleanupBytes"] <= (512 << 20 if feed_owned else p["Cleanup"]["Bytes"]), "separate finite durable cleanup credit")


def original_inventory(frame, end, spool, inventory):
    kind = frame["Type"]
    if kind == "originals-header":
        fields(frame, "Type Stage Cut Metadata Records")
        stage = ("old", "final")[inventory["done"]]
        require(inventory["stage"] is None and frame["Stage"] == stage and frame["Cut"] == end[stage.title()], "drained actual attempt barrier")
        inventory.update(stage=stage, metadata=frame["Metadata"], counts=[[0, 0], [0, 0]], previous={})
        for g in range(2):
            raw_record(frame["Records"][g], key(end["Config"], g, "meta", "state"), go_json(frame["Metadata"][g]))
            require(frame["Metadata"][g]["Groups"] == frame["Cut"]["Groups"][g] and frame["Metadata"][g]["Floor"] == frame["Cut"]["Round"], "actual complete barrier cut/metadata")
        spool.insert(b"barrier-metadata", stage.encode(), go_json(frame["Metadata"]))
    elif kind == "original":
        fields(frame, "Type Stage Kind Group Records")
        stage, name, g = frame["Stage"], frame["Kind"], integer(frame["Group"], 0, 1)
        require(stage == inventory["stage"] and name in ("id", "request") and len(frame["Records"]) == 2, "actual two-door original enumeration")
        index = ("id", "request").index(name)
        native = blob(frame["Records"][0]["Key"])
        order = index, g
        require(order not in inventory["previous"] or inventory["previous"][order] < native, "original native order/duplicate")
        inventory["previous"][order] = native
        for record, namespace in zip(frame["Records"], (name, "request" if index == 0 else "id"), strict=True):
            k, v = blob(record["Key"]), blob(record["Value"])
            require(v == spool.lookup(b"attempt-" + namespace.encode(), k), "actual stored originals, not reported attempt counts")
            ordinal = int.from_bytes(spool.lookup(b"attempt-membership-" + namespace.encode(), k), "big")
            require(stage == "final" or ordinal <= 3 + end["Spec"]["facts"], "old barrier excludes all update/ABORT attempts")
        spool.insert(b"inventory-" + stage.encode() + b"-" + name.encode(), native, blob(frame["Records"][0]["Value"]))
        inventory["counts"][index][g] += 1
    else:
        fields(frame, "Type Stage Rows Metadata")
        stage = inventory["stage"]
        require(stage == frame["Stage"] and frame["Rows"] == inventory["counts"] and frame["Metadata"] == inventory["metadata"] and frame["Rows"][0] == frame["Rows"][1] == [m["Transactions"] for m in frame["Metadata"]] == decoded(spool.lookup(b"original-cost", stage.encode()))[0], "independent native ID AND request EOF including all authentic ABORTs")
        inventory["stage"] = None
        inventory["done"] += 1


CONTROL_NAMES = ("begin", "publish", "local-open", "local-reopen", "expired-new-refused", "terminal")
OBSERVATIONS = ("racing-old", "held-old-after-update", "fresh-current")


def feed_control(frame, end, spool, ordinal):
    fields(frame, "Type Name Access Input Records Nanos")
    require(ordinal < len(CONTROL_NAMES) and frame["Name"] == CONTROL_NAMES[ordinal] and "actual native API" in frame["Access"], "fixed genuine feed lifecycle control")
    integer(frame["Nanos"], 0 if ordinal >= 4 else 1, (1 << 63) - 1)
    c, o, token = end["Config"], end["Feed"]["Original"], end["Feed"]["Token"]
    values = controls(frame["Records"], True)
    mandatory = {feed_key(c, "meta", n) for n in ("binding", "ledger")} | {feed_key(c, n, "workload") for n in ("original", "cursor")} | {key(c, 0, "source-retention-v1", n) for n in ("binding", "state")} | {key(c, 0, "source-owner-v1", o[n]["Request"]) for n in ("Pin", "Snapshot")} | {feed_key(c, "cleanup/workload", o[n]["Request"]) for n in ("Pin", "Snapshot")}
    for request in ("process-reader", "local-one", "local-two"):
        k = feed_key(c, "reader/workload", request)
        mandatory.add(k)
        if values.get(k):
            owner = decoded(values[k])
            mandatory.update((key(c, 0, "source-owner-v1", owner["Request"]), feed_key(c, "cleanup/workload", owner["Request"])))
    require(set(values) == mandatory and values[feed_key(c, "original", "workload")] == go_json(o), "exact native original/control/owner inventory")
    binding = {"Version": 1, "Config": c, "Source": {"Version": 1, "Config": c, "Initial": end["Initial"], "Policy": end["Binding"]["History"]}, "Policy": feed_policy(end),
               "Selector": "full-scope;fixed-scalar-cross-edge;unfiltered-complete-before-after:v1", "Order": "Round,Coordinator,Request;independent-coordinator-sequences:v1"}
    require(decoded(values[feed_key(c, "meta", "binding")]) == binding, "actual fresh persisted feed binding")
    cursor = decoded(values[feed_key(c, "cursor", "workload")])
    feed_cursor(cursor, end)
    require(cursor["State"] == (0 if ordinal == 0 else 3 if ordinal >= 4 else 1), "durable prepared/acquired/expired distinction")
    if ordinal == 0:
        require(frame["Input"] == o and cursor["Positions"] == end["Old"]["Groups"] and cursor["Completed"] == end["Old"] and all(not values[key(c, 0, "source-owner-v1", o[n]["Request"])] for n in ("Pin", "Snapshot")), "Begin returned authentic stored factory reservation before acquisition")
    elif ordinal == 1:
        require(frame["Input"] == token and all(values[key(c, 0, "source-owner-v1", o[n]["Request"])] == go_json(o[n]) for n in ("Pin", "Snapshot")), "Publish atomically acquired both genuine source owners")
    elif ordinal in (2, 3):
        request = "local-one" if ordinal == 2 else "local-two"
        require(values[feed_key(c, "reader/workload", request)] == go_json(frame["Input"]) == values[key(c, 0, "source-owner-v1", frame["Input"]["Request"])], "local handle original before data")
    elif ordinal == 4:
        require(frame["Input"]["Request"] == "expired-new" and type(frame["Input"]["Error"]) is str and frame["Input"]["Error"], "explicit expired new-open refusal")
        require(values[key(c, 0, "source-owner-v1", o["Pin"]["Request"])] == go_json(o["Pin"] | {"State": 3, "TerminalRequest": "expire-workload"}) and values[key(c, 0, "source-owner-v1", o["Snapshot"]["Request"])] == go_json(o["Snapshot"]), "expiry refuses new authority while held original remains active")
    else:
        require(frame["Input"] == token and cursor["Completed"] == end["Final"] and cursor["Positions"] == end["Final"]["Groups"] and cursor["Pending"] == "" and cursor["TerminalRequest"] == "expire-workload", "complete terminal exact cursor")
        for request in ("process-reader", "local-one", "local-two"):
            owner = decoded(values[feed_key(c, "reader/workload", request)])
            require(values[key(c, 0, "source-owner-v1", owner["Request"])] == go_json(owner | {"State": 2, "TerminalRequest": "close"}), "all actual process/local owners authenticated Close")
        require(values[key(c, 0, "source-owner-v1", o["Snapshot"]["Request"])] == go_json(o["Snapshot"] | {"State": 2, "TerminalRequest": "close"}), "expired held snapshot eventually closed via original ticket")
    spool.insert(b"feed-control-observation", frame["Name"].encode(), go_json(frame))


def events(directory, end, spool, header):
    digest, footer, started = hashlib.sha256(), False, False
    active, selection_, counts = None, None, [0, 0]
    observations = control_count = committed = page_count = replay_count = acknowledged = 0
    positions = list(end["Old"]["Groups"])
    race_started = race_finished = replaced = killed = points = storage_eof = source_closed = False
    source_owner = None
    inventory = {"stage": None, "done": 0}
    storage_previous, storage_count, storage_bytes = None, 0, 0
    zero_outcome = {"Commit": False, "Round": 0, "Digest": [0] * 32, "ID": "", "Request": "", "Reason": "", "Coordinator": 0, "Sequence": 0}
    spool.insert(b"feed-control-state", b"last", go_json(zero_outcome))
    limit = 256 + 16 * (3 + 3 * end["Spec"]["facts"]) + 4 * end["Attempts"] + 8 * feed_policy(end)["MaxPages"]
    for frame, line in lines(directory / "fdb-feed-events.jsonl", limit):
        require(not footer, "event after EOF")
        kind = frame.get("Type")
        if kind == "footer":
            require(frame == {"Type": "footer", "End": end} and digest.hexdigest() == end["EventSHA256"] and source_closed and storage_eof and active is None and inventory["stage"] is None, "complete raw feed/source event EOF")
            footer = True
            continue
        digest.update(line)
        if not started:
            require(frame == {"Type": "header", "Format": FORMAT, "Spec": end["Spec"], "Config": end["Config"], "Binding": end["Binding"], "Record": end["BindingRecord"], "Initial": end["Initial"]}, "actual workload header/binding")
            started = True
        elif kind == "committed":
            fields(frame, "Type Operation Original Outcome")
            ordinal, phase, _ = operation(end["Spec"], frame["Operation"])
            body = spool.lookup(b"fdb-operation", u64(ordinal))
            require(blob(frame["Original"]["Value"]) == body and frame["Outcome"] == decoded(body)["Outcome"] and (phase != "update" or race_started and not race_finished), "real useful completion strictly inside snapshot/write race")
            spool.insert(b"completion-seen", u64(ordinal), b"1")
            committed += 1
        elif kind in ("originals-header", "original", "originals-eof"):
            original_inventory(frame, end, spool, inventory)
        elif kind == "feed-control":
            feed_control(frame, end, spool, control_count)
            control_count += 1
        elif kind == "selector-open":
            fields(frame, "Type Name Mode Access Cut Nanos")
            require(frame["Name"] in ("fresh-current", "raw-source-held") and frame["Cut"] == end["Final"] and frame["Mode"] == ("Fresh" if frame["Name"] == "fresh-current" else "At") and "actual Open" in frame["Access"], "actual separate selector opening")
            integer(frame["Nanos"], 1, (1 << 63) - 1)
            spool.insert(b"timing-open", frame["Name"].encode(), go_json(frame))
        elif kind == "selection":
            fields(frame, "Type Name Selection Policy")
            require(active is None and observations < 3 and frame["Name"] == OBSERVATIONS[observations] and frame["Policy"] == end["Binding"]["Read"], "actual complete racing/held/final source lifecycle")
            active, selection_, counts = frame["Name"], frame["Selection"], [0, 0]
            snapshot_selection(selection_, active, end, spool)
        elif kind == "snapshot-row":
            fields(frame, "Type Name Row")
            require(active == frame["Name"], "row without acquired source")
            snapshot_row(frame["Row"], selection_["Selected"], end, spool, active)
            counts[frame["Row"]["Group"]] += 1
        elif kind == "race-first-row":
            fields(frame, "Type Cut")
            require(active == "racing-old" and sum(counts) == 1 and not race_started and frame["Cut"] == end["Old"], "first actual source row gates writes")
            race_started = True
        elif kind == "race-writes-complete":
            fields(frame, "Type Updates")
            require(race_started and not race_finished and frame["Updates"] == end["Spec"]["facts"] and committed == end["Committed"], "actual useful update ACKs before racing source resumes")
            race_finished = True
        elif kind == "snapshot-eof":
            fields(frame, "Type Name Access End Nanos")
            require(frame["Name"] == active and race_finished and "not indexed" in frame["Access"] and frame["End"]["Cut"] == selection_["Selected"] and frame["End"]["Rows"] == frame["End"]["DirectoryRows"] == counts, "genuine semantic complete EOF")
            namespace = b"fdb-current" if active == "fresh-current" else b"fdb-old"
            expected = [0, 0]
            for native, value in spool.walk(namespace):
                require(value == spool.lookup(b"snapshot-seen-" + active.encode(), native), "missing/extra exact snapshot row")
                expected[0 if native.startswith(prefix(end["Config"], 0)) else 1] += 1
            require(expected == counts and frame["End"]["OriginalEffects"] == decoded(spool.lookup(b"source-count", b"final" if active == "fresh-current" else b"old"))[1], "independent complete physical/original source census")
            snapshot_work(frame["End"]["Work"], end, active != "fresh-current")
            integer(frame["Nanos"], 1, (1 << 63) - 1)
            spool.insert(b"timing-scan", active.encode(), go_json(frame))
            observations += 1
            active = None
        elif kind in ("points-eof", "expired-held-points"):
            require((points is False if kind == "points-eof" else control_count == 5), "fixed actual point observations")
            fields(frame,"Type Name Access Points Work Nanos" if kind=="points-eof" else "Type Points Work Nanos")
            integer(frame["Nanos"],1,(1<<63)-1)
            wanted_namespace = b"fdb-current" if kind == "points-eof" else b"fdb-old"
            require(len(frame["Points"]) == 3, "fixed three probes")
            for i, point in enumerate(frame["Points"]):
                fields(point, "Key Found Value")
                fact=(0,end["Spec"]["facts"]-1,end["Spec"]["facts"]+16)[i]
                target=end["Config"]["Nodes"][1] if (fact+1)*end["Spec"]["crossPercent"]//100>fact*end["Spec"]["crossPercent"]//100 else 3
                logical="edge/"+"/".join(f"{n:016x}" for n in (7+2*fact,1,2,target,target+1))
                require(point["Key"]=={"Group":0,"Key":logical},"exact declared fixed probe identities, including genuine phantom")
                expected = spool.find(wanted_namespace, key(end["Config"], point["Key"]["Group"], "current", point["Key"]["Key"]))
                require(point["Found"] is (expected is not None) and (go_json(point["Value"]) == expected if expected is not None else point["Value"] == {"Value": 0, "Version": 0, "Round": 0, "Deleted": False, "TxID": ""}) and point["Found"] is (i < 2), "actual old/current and phantom exact point results")
            snapshot_work(frame["Work"], end, kind != "points-eof")
            if kind == "points-eof":
                integer(frame["Nanos"], 1, (1 << 63) - 1)
                spool.insert(b"timing-points", b"final", go_json(frame))
                points = True
            else:
                spool.insert(b"expired-held-proof", b"old", go_json(frame))
        elif kind == "client-killed":
            fields(frame, "Type Input Result Signal Reaped")
            require(not killed and race_finished and frame["Signal"] == "SIGKILL" and frame["Reaped"] is True and frame["Result"]["PID"] == end["Feed"]["Processes"][0] and "Ack" not in frame["Result"] and not frame["Result"]["Delivery"]["Replay"], "actual first SDK process killed/reaped only after pending native page")
            child_input(frame["Input"], end)
            page, fresh = feed_page(frame["Result"]["Delivery"], end, spool, positions)
            require(fresh, "first native page original")
            spool.insert(b"process-proof", b"first", go_json(frame))
            killed = True
        elif kind == "client-replaced":
            fields(frame, "Type Input Result Exit Reaped")
            require(killed and not replaced and frame["Exit"] == 0 and frame["Reaped"] is True and frame["Result"]["PID"] == end["Feed"]["Processes"][1], "genuine replacement SDK process terminal/reap")
            child_input(frame["Input"], end)
            first = decoded(spool.lookup(b"process-proof", b"first"))
            require(frame["Input"] == first["Input"] and frame["Result"]["Owner"] == first["Result"]["Owner"] and frame["Result"]["Delivery"]["Record"] == first["Result"]["Delivery"]["Record"], "same persisted factory reader and byte-identical pending page across processes")
            page, fresh = feed_page(frame["Result"]["Delivery"], end, spool)
            require(not fresh, "replacement must replay original pending page")
            feed_ack(frame["Result"]["Ack"], page, end, spool, True)
            positions = list(page["Next"])
            page_count, replay_count, acknowledged = 1, 1, len(page["Rows"])
            for field in ("OpenNanos", "PageNanos", "AckNanos"):
                integer(frame["Result"][field], 1, (1 << 63) - 1)
            spool.insert(b"process-proof", b"second", go_json(frame))
            replaced = True
        elif kind == "feed-page":
            fields(frame, "Type Delivery Ack PageNanos AckNanos")
            require(replaced, "pages require actual replacement/reopen first")
            page, fresh = feed_page(frame["Delivery"], end, spool, positions)
            require(fresh and not frame["Delivery"]["Replay"] and page["Request"] == f"page-{page_count}", "actual one pending page original at a time")
            feed_ack(frame["Ack"], page, end, spool, True)
            positions = list(page["Next"])
            page_count += 1
            acknowledged += len(page["Rows"])
            for field in ("PageNanos", "AckNanos"):
                integer(frame[field], 1, (1 << 63) - 1)
            for call in ("Page","Ack"):
                spool.insert(b"feed-duration/"+call.encode(),u64(frame[call+"Nanos"])+u64(page_count),b"1")
        elif kind == "feed-replay":
            fields(frame, "Type Delivery Ack PageNanos AckNanos")
            require(positions == end["Final"]["Groups"] and replay_count == 1, "explicit replay after full EOF only")
            page, fresh = feed_page(frame["Delivery"], end, spool)
            require(not fresh and page["Request"] == "page-0", "same original replay, no second business fold")
            feed_ack(frame["Ack"], page, end, spool, False)
            for field in ("PageNanos","AckNanos"):
                integer(frame[field],1,(1<<63)-1)
            spool.insert(b"feed-replay-timing",b"final",go_json(frame))
            replay_count += 1
        elif kind == "feed-storage-row":
            fields(frame, "Type Record")
            require(control_count == 6 and not storage_eof, "complete terminal native control enumeration")
            native, body = blob(frame["Record"]["Key"]), blob(frame["Record"]["Value"])
            require(native.startswith(prefix(end["Config"]) + b"feed-v1/") and (storage_previous is None or storage_previous < native) and 0 < len(body) <= 64 << 10, "exact ordered terminal native controls")
            storage_previous = native
            spool.insert(b"feed-native-control", native, body)
            storage_count += 1
            storage_bytes += len(native) + len(body)
        elif kind == "feed-storage-eof":
            fields(frame, "Type Storage Ledger")
            require(not storage_eof and frame["Storage"] == end["Feed"]["Storage"] and frame["Storage"]["Records"] == storage_count == frame["Ledger"]["Records"] + 1 and frame["Storage"]["Bytes"] == storage_bytes and frame["Storage"]["ReservedBytes"] == frame["Ledger"]["Bytes"] and frame["Storage"]["CleanupTickets"] == frame["Ledger"]["CleanupTickets"], "actual physical native feed bytes vs separately funded reservation ledger")
            storage_eof = True
        elif kind == "source-held":
            fields(frame, "Type Selection Policy")
            require(source_owner is None and observations == 3 and inventory["done"] == 2 and frame["Policy"] == end["Binding"]["Read"], "genuine separate final raw-source owner")
            source_owner = snapshot_selection(frame["Selection"], "raw-source-held", end, spool)
        elif kind == "source-closed":
            fields(frame, "Type Access Owner Work")
            require(source_owner is not None and not source_closed and "separately bound History policy" in frame["Access"], "raw history work scope distinct from source opening")
            raw_record(frame["Owner"], key(end["Config"], 0, "source-owner-v1", source_owner["Request"]), go_json(source_owner | {"State": 2, "TerminalRequest": "close"}))
            snapshot_work(frame["Work"], end)
            require(frame["Work"]["CleanupCallbacks"] > 0, "actual source Close through bounded SDK")
            source_closed = True
        else:
            require(False, "unknown feed/source event")
    require(started and footer and observations == 3 and control_count == 6 and committed == end["Committed"] and points and race_finished and killed and replaced and inventory["done"] == 2 and
            page_count == end["Feed"]["Pages"] and replay_count == end["Feed"]["ReplayedPages"] and acknowledged == end["Feed"]["AcknowledgedRows"] and positions == end["Final"]["Groups"], "missing complete feed race/process/page/ACK/ownership/source proof")
    spool.lookup(b"expired-held-proof", b"old")
    spool.insert(b"feed-page-timing-count",b"count",u64(page_count-1))
    for native, body in spool.walk(b"fdb-current"):
        require(spool.lookup(b"feed-replica", native) == body, "snapshot plus acknowledged complete feed differs from final graph/index")


def child_input(value, end):
    fields(value, "Config Source Policy Token Boundary ReaderRequest PageRequest DeadlineNanos")
    require(value == {"Config": end["Config"], "Source": {"Version": 1, "Config": end["Config"], "Initial": end["Initial"], "Policy": end["Binding"]["History"]}, "Policy": feed_policy(end), "Token": end["Feed"]["Token"], "Boundary": end["Final"], "ReaderRequest": "process-reader", "PageRequest": "page-0", "DeadlineNanos": end["Feed"]["Original"]["DeadlineNanos"]}, "actual client replacement uses same original, finite outer deadline and immutable page request")


def metadata(end, spool, header):
    c = end["Config"]
    maximum = dict.fromkeys(("Floor", "Generation", "Versions", "Transactions", "Certificates", "Bytes", "Groups"), (1 << 64) - 1)
    for phase, certificates in (("old", (end["Initial"], end["Old"])), ("final", (end["Initial"], end["Old"], end["Final"]))):
        cut = end[phase.title()]
        transactions, pair_bytes = decoded(spool.lookup(b"original-cost", phase.encode()))
        generations, versions = decoded(spool.lookup(b"source-count", phase.encode()))
        source_bytes = decoded(spool.lookup(b"source-cost", phase.encode()))
        charged = [len(key(c, g, "meta", "config")) + len(go_json(c)) + len(key(c, g, "meta", "state")) + len(go_json(maximum)) for g in range(2)]
        charged[0] += len(blob(end["BindingRecord"]["Key"])) + len(blob(end["BindingRecord"]["Value"]))
        for cert in certificates:
            charged[0] += len(key(c, 0, "cut", go_json(cert).hex())) + len(go_json(cert))
        expected = []
        for g in range(2):
            expected.append({"Floor": cut["Round"], "Generation": generations[g], "Versions": versions[g], "Transactions": transactions[g], "Certificates": len(certificates) if g == 0 else 0,
                             "Bytes": charged[g] + pair_bytes[g] + sum(part[g] for part in source_bytes), "Groups": cut["Groups"][g]})
        require(decoded(spool.lookup(b"barrier-metadata", phase.encode())) == expected, "exact native graph bytes/originals/history/CDC at both phase barriers")
        if phase == "final":
            require(expected == header["Metadata"], "source EOF agrees with actual final barrier")
    for name in (*OBSERVATIONS, "raw-source-held"):
        expected = spool.lookup(b"barrier-metadata", b"old" if name != "fresh-current" and name != "raw-source-held" else b"final")
        require(spool.lookup(b"selection-metadata", name.encode()) == expected, "actual immutable acquisition metadata at declared cut")


def native_storage(end, spool):
    c, o = end["Config"], end["Feed"]["Original"]
    policy = feed_policy(end)
    expected_keys = set()  # Fixed control paths only; page/action paths stream.
    maximum = {"Records": (1 << 64) - 1, "Bytes": (1 << 64) - 1, "CleanupTickets": (1 << 64) - 1}
    reserved = len(feed_key(c, "meta", "ledger")) + len(go_json(maximum))
    records = tickets = pages = acks = actions = invocations = 0
    def expected(k, v):
        require(spool.lookup(b"feed-native-control", k) == go_json(v), "missing/foreign terminal native original")
        expected_keys.add(k)
    begin = decoded(spool.lookup(b"feed-control-observation", b"begin"))
    binding = controls(begin["Records"], True)[feed_key(c, "meta", "binding")]
    expected(feed_key(c, "meta", "binding"), decoded(binding))
    expected(feed_key(c, "original", "workload"), o)
    cursor = decoded(spool.lookup(b"feed-native-control", feed_key(c, "cursor", "workload")))
    feed_cursor(cursor, end)
    require(cursor["State"] == 3 and cursor["Positions"] == end["Final"]["Groups"] and cursor["Completed"] == end["Final"] and cursor["Pages"] == end["Feed"]["Pages"] and cursor["Pending"] == "" and cursor["TerminalRequest"] == "expire-workload", "complete expired terminal cursor")
    expected_keys.add(feed_key(c, "cursor", "workload"))
    owners = {o["Pin"]["Request"]: o["Pin"], o["Snapshot"]["Request"]: o["Snapshot"]}
    for request in ("process-reader", "local-one", "local-two"):
        k = feed_key(c, "reader/workload", request)
        owner = decoded(spool.lookup(b"feed-native-control", k))
        require(owner["Kind"] == "read" and owner["State"] == 1 and owner["Cut"] == end["Old"] and owner["Required"] == end["Initial"] and owner["PinRequest"] == o["Pin"]["Request"] and owner["PinDigest"] == o["Snapshot"]["PinDigest"] and owner["Request"] == "source-read-" + bytes(owner["Secret"])[:16].hex() and owner["Request"] not in owners, "unique persisted actual reader factory intent")
        owners[owner["Request"]] = owner
        if request=="process-reader":
            require(owner==decoded(spool.lookup(b"process-proof",b"second"))["Result"]["Owner"],"actual SDK process replacement uses the stored native factory original")
        expected_keys.add(k)
    expected_keys.add(feed_key(c, "meta", "ledger"))
    for native, body in spool.walk(b"feed-native-control"):
        records += 1
        relative = native.removeprefix(prefix(c) + b"feed-v1/")
        if native == feed_key(c, "meta", "ledger"):
            continue
        if relative.startswith(b"meta/"):
            require(native == feed_key(c, "meta", "binding"), "unknown feed meta")
            reserved += len(native) + len(body)
        elif relative.startswith(b"original/"):
            require(native == feed_key(c, "original", "workload"), "foreign feed intent")
            reserved += len(native) + (64 << 10)
        elif relative.startswith(b"cursor/"):
            require(native in expected_keys, "foreign cursor")
            reserved += len(native) + 8192
        elif relative.startswith(b"reader/workload/"):
            require(native in expected_keys, "unobserved/foreign reader intent")
            reserved += len(native) + 4096
        elif relative.startswith(b"cleanup/workload/"):
            ticket = decoded(body)
            fields(ticket, "Original Owner OuterDeadlineNanos DeadlineNanos Calls Rows Bytes TerminalRequest TerminalState Closed")
            request = ticket["Owner"]["Request"]
            require(request in owners and ticket["Owner"] == owners[request] and native == feed_key(c, "cleanup/workload", request) and ticket["Original"] == end["Feed"]["Token"]["Original"] and ticket["OuterDeadlineNanos"] == o["DeadlineNanos"] and
                    0 < integer(ticket["DeadlineNanos"]) <= o["DeadlineNanos"] and ticket["Closed"] is True and 1 <= integer(ticket["Calls"]) <= 8 and ticket["Rows"] == ticket["Calls"] * 1024 and ticket["Bytes"] == ticket["Calls"] * (64 << 20), "durable SAME-owner cleanup credit/deadline/spent work survives process replacement")
            require(ticket["TerminalRequest"] == ("expire-workload" if ticket["Owner"]["Kind"] == "pin" else "close") and ticket["TerminalState"] == (3 if ticket["Owner"]["Kind"] == "pin" else 2), "explicit authenticated release/expiry, never timeout authority")
            expected_keys.add(native)
            tickets += 1
            reserved += len(native) + 4096
        elif relative.startswith(b"page/workload/"):
            page = decoded(body)
            require(body == spool.lookup(b"feed-page", page["Request"].encode()) and native == feed_key(c, "page/workload", page["Request"]), "physical native page equals consumed immutable original")
            pages += 1
            reserved += len(native) + (64 << 10)
        elif relative.startswith(b"ack/workload/"):
            ack = decoded(body)
            require(body == spool.lookup(b"feed-ack", ack["Request"].encode()) and native == feed_key(c, "ack/workload", ack["Request"]), "physical immutable native ACK")
            acks += 1
            reserved += len(native) + 8192
        elif relative.startswith(b"action/workload/"):
            action = decoded(body)
            fields(action, "Request Kind Input Invocations")
            request, kind = action["Request"], action["Kind"]
            invocations+=integer(action["Invocations"], 1, 8)
            if kind in ("page", "ack"):
                number = request.removeprefix(kind + "/page-")
                require(number.isdecimal() and str(int(number)) == number and int(number) < end["Feed"]["Pages"], "finite known page/ACK action")
                page = decoded(spool.lookup(b"feed-page", ("page-" + number).encode()))
                original_input = {"Boundary": end["Final"]} if kind == "page" else list(hashlib.sha256(go_json(page)).digest())
            elif kind == "publish":
                require(request == "publish", "exact publish action")
                original_input = o
            else:
                require(kind == "open" and request in ("process-reader", "local-one", "local-two", "expired-new"), "unknown action cannot become authority")
                original_input = end["Feed"]["Token"]
            require(native == feed_key(c, "action/workload", request) and bytes(action["Input"]) == hashlib.sha256(go_json(original_input)).digest(), "immutable native action digest/retry identity")
            actions+=1
            reserved += len(native) + 1024
        else:
            require(False, "unknown native control record")
    ledger = decoded(spool.lookup(b"feed-native-control", feed_key(c, "meta", "ledger")))
    require(records == len(expected_keys)+pages+acks+actions == ledger["Records"] + 1 and reserved == ledger["Bytes"] <= policy["MaxControlBytes"] and tickets == ledger["CleanupTickets"] == 5 and pages == acks == end["Feed"]["Pages"] and actions==2*pages+5 and cursor["Calls"]==invocations+1, "complete native controls/funded bytes including independent terminal tickets")


def timings(end, spool):
    # Dataset-sized timing order is also BLOB-spooled: no in-memory sample list
    # or SQL temporary sort. Per-call nearest-rank totals remain fixed control.
    page_samples=int.from_bytes(spool.lookup(b"feed-page-timing-count",b"count"),"big")
    counts = {"Page":page_samples,"Ack":page_samples}
    def add(call, nanos, identity):
        integer(nanos, 1, (1 << 63) - 1)
        counts[call] = counts.get(call, 0) + 1
        spool.insert(b"feed-duration/" + call.encode(), u64(nanos) + identity, b"1")
    for name in ("begin", "publish", "local-open", "local-reopen"):
        value = decoded(spool.lookup(b"feed-control-observation", name.encode()))
        add(name, value["Nanos"], b"control")
    for name in OBSERVATIONS:
        value = decoded(spool.lookup(b"timing-scan", name.encode()))
        add("Scan-old" if name != "fresh-current" else "Scan-final", value["Nanos"], name.encode())
    value = decoded(spool.lookup(b"timing-points", b"final"))
    add("Points-scan-based", value["Nanos"], b"final")
    value=decoded(spool.lookup(b"expired-held-proof",b"old"))
    add("Points-scan-based-old-held",value["Nanos"],b"old")
    for name in (b"first", b"second"):
        result = decoded(spool.lookup(b"process-proof", name))["Result"]
        add("Open-process", result["OpenNanos"], name)
        add("Page-replay" if name == b"second" else "Page", result["PageNanos"], name)
        if name == b"second":
            add("Ack", result["AckNanos"], name)
    replay=decoded(spool.lookup(b"feed-replay-timing",b"final"))
    add("Page-replay",replay["PageNanos"],b"final")
    add("Ack-replay",replay["AckNanos"],b"final")
    result = []
    for call, count in sorted(counts.items()):
        values = {}
        for index, (coordinate, _) in enumerate(spool.walk(b"feed-duration/" + call.encode()), 1):
            for percentile in (50, 95, 99):
                if index == (percentile * count + 99) // 100:
                    values[f"p{percentile}Nanos"] = int.from_bytes(coordinate[:8], "big")
        result.append({"call": call, "samples": count, **values})
    return {"nearestRank": result, "scope": "actual API retry/wait, complete retained Scan through EOF with callback IO, separate scan-based Points and Page/ACK/replay; no indexed/per-key or external exactly-once claim"}


def verify(directory, spool_directory, policy=None):
    directory = Path(directory).resolve(strict=True)
    with (directory / "fdb-feed-result.json").open("rb") as source:
        body = source.read(raw.LINE + 1)
    require(len(body) <= raw.LINE and body.endswith(b"\n"), "bounded exclusive result")
    end = decoded(body[:-1])
    validate_result(end)
    feed_original(end)
    require(policy is not None or end["Spec"]["facts"] < 1_000_000 and end["Spec"]["writers"] != 32, "large cell requires explicit finite comparison grant")
    with Spool(spool_directory, POLICY if policy is None else policy) as spool:
        attempts(directory, end, spool)
        header = source_fold(directory, end, spool)
        events(directory, end, spool, header)
        metadata(end, spool, header)
        native_storage(end, spool)
        columns(directory, end, spool)
        latencies(directory, end, spool)
        timing = timings(end, spool)
        report = spool.report()
    return {"format": FORMAT, "family": FAMILY, "runGraph": end["Config"]["Graph"], "spec": end["Spec"], "attempts": end["Attempts"], "committed": end["Committed"], "aborted": end["Aborted"],
            "feedPages": end["Feed"]["Pages"], "replayedPages": end["Feed"]["ReplayedPages"], "spool": report, "timing": timing,
            "scope": "one source-implemented FDB snapshot/write-race/feed/actual-client-replacement cell, source retain-all plus derived-column lifetime; explicit replay, no external exactly-once/native parity/RTT/full-matrix/capacity acceptance"}


if __name__ == "__main__":
    try:
        if len(sys.argv) not in (3, 5):
            raise ValueError("usage: fdb_feed_check.py ARTIFACTS OWNED_SPOOL_DIR [DISK_BYTES CACHE_BYTES]")
        policy = None if len(sys.argv) == 3 else Policy(int(sys.argv[3]), int(sys.argv[4]), 4096, raw.LINE)
        print(json.dumps(verify(sys.argv[1], sys.argv[2], policy), indent=2))
    except (ValueError, KeyError, TypeError, OSError) as error:
        print("feed evidence refused: " + str(error), file=sys.stderr)
        sys.exit(1)
