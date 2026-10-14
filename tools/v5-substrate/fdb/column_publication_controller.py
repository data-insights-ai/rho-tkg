#!/usr/bin/env python3
"""Bind authentic publication/derived-GC process interruptions to native rows.

Source history is retained. This is not source-history restart, a graph model,
a packet simulator, capacity acceptance, or proof without a real SDK campaign.
"""
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import launcher
import platform_fault_controller as platform

FORMAT = "fdb-column-publication-faults-v1"
require, load = platform.require, platform.load


def blob(value):
    require(isinstance(value, str), "actual binary record required")
    return base64.b64decode(value, validate=True)


def go_json(value):
    # This finite wire uses ASCII launcher IDs, integers and ordered struct
    # fields. Canonical raw originals always retain their delivered bytes.
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value):
    return list(hashlib.sha256(go_json(value)).digest())


def prefix(state, group=0):
    return f'g{group}/{state["Config"]["Graph"].encode().hex()}/'.encode()


def generation_key(state, generation, suffix):
    return prefix(state) + b"columns-v1/generation/" + generation.encode().hex().encode() + b"/" + suffix.encode()


def row_map(rows):
    result = {}
    for row in rows:
        key = blob(row["Key"])
        require(key not in result, "duplicate observed native key")
        result[key] = None if row["Value"] is None else blob(row["Value"])
    return result


def native(rows, key):
    require(key in rows and rows[key] is not None, "missing native authority")
    return json.loads(rows[key], object_pairs_hook=platform.object_pairs)


def marker(value, state, raw_state):
    expected = {"chunk": "chunk", "source-eof": "source-eof", "manifest-issued": "native-manifest-commit-issued"}[state["Case"]]
    require(value == {"Ready": expected, "Format": FORMAT, "Case": state["Case"], "Coordinator": state["Coordinator"],
                      "Generation": {"Version": 1, "ID": "final", "Binding": state["OldManifest"]["Generation"]["Binding"],
                                     "Initial": state["Initial"], "Cut": state["Final"]},
                      "StateSHA256": list(hashlib.sha256(raw_state).digest())}, "exact genuine publication seam/state binding")
    # This marker proves invocation/observer readiness only. A native stored
    # manifest and complete source joins below decide, never an invented1021.


def gc_marker(value, state):
    require(value == {"Ready": "derived-gc-page-committed", "Format": FORMAT, "Case": state["Case"], "Coordinator": state["Coordinator"], "Page": state["Pages"][0]} and
            len(state["Pages"]) == 1 and state["Pages"][0]["Result"]["Cursor"] == 1 and not state["Pages"][0]["Result"]["Done"], "actual native partial derived GC marker")


def chunk(raw, manifest, ordinal, previous, typed, originals):
    require(114 <= len(raw) <= 32 << 10 and raw[:8] == b"RHOCLM01", "bounded complete cache chunk")
    g = manifest["Generation"]
    require(raw[8:40] == bytes(g["Binding"]) and raw[40:72] == hashlib.sha256(go_json(g)).digest() and
            int.from_bytes(raw[72:80], "big") == ordinal and raw[80:112] == previous and
            int.from_bytes(raw[112:114], "big") == 1, "exact generation/ordinal/predecessor and one-row policy")
    require(len(raw) >= 212 and int.from_bytes(raw[204:208], "big") == 0 and
            int.from_bytes(raw[208:212], "big") == len(raw) - 212, "exact original arena, no truncation/extra bytes")
    numbers = [int.from_bytes(raw[116 + i * 8:124 + i * 8], "big") for i in range(11)]
    fields = ("Owner", "Life", "SourceNode", "SourceLife", "TargetNode", "TargetLife", "Value", "ValueID", "Provenance", "Round", "Sequence")
    require(raw[114] == typed["Kind"] and raw[115] == typed["Coordinator"] and
            numbers == [typed[k] & ((1 << 64) - 1) for k in fields] and raw[212:] == blob(typed["Original"]),
            "typed columns changed complete original or unsigned coordinates")
    group = json.loads(raw[212:], object_pairs_hook=platform.object_pairs)
    key = (group["Tx"]["Coordinator"], group["Tx"]["ID"])
    require(key in originals and raw[212:] == blob(originals[key]["CDC"]["Value"]), "column row must be genuine complete retained CDC")
    return hashlib.sha256(previous + hashlib.sha256(raw).digest()).digest()


def caches(directory, state, originals):
    for name, count in (("old", 4), ("final", 5)):
        m = state["OldManifest" if name == "old" else "FinalManifest"]
        require(m["Rows"] == m["Chunks"] == count, "actual fixed one-row chunk/source census")
        stage = next(s for s in state["Stages"] if s["Name"] == "cached-" + name)
        require(stage["Manifest"] == m and len(stage["Rows"]) == count, "cache walk must reach native authenticated EOF")
        previous, retained = bytes(32), 0
        generation = hashlib.sha256(go_json(m["Generation"])).hexdigest()
        files = sorted((directory / ("cache-" + name)).iterdir())
        require(len(files) == count and all(p.is_file() for p in files), "missing/mixed/extra cache file")
        for ordinal, typed in enumerate(stage["Rows"]):
            path = directory / ("cache-" + name) / f"column-{generation}-{ordinal:016x}.bin"
            raw = platform.admitted_bytes(path, 32 << 10)
            previous = chunk(raw, m, ordinal, previous, typed, originals)
            retained += len(generation_key(state, name, f"chunk/{ordinal:016x}")) + len(raw)
        require(previous == bytes(m["Chain"]) and retained == m["Bytes"], "complete immutable cache chain/length EOF")


def original_joins(state):
    require(len(state["Originals"]) == 5 and state.get("Pending") is None and state.get("Failure") is None,
            "complete committed immutable five-original inventory")
    result, histories = {}, {}
    for original in state["Originals"]:
        body = blob(original["Proposal"])
        tx = json.loads(body, object_pairs_hook=platform.object_pairs)
        o = original["Outcome"]
        require(o["Commit"] and o["ID"] == tx["ID"] and o["Request"] == tx["Request"] and
                o["Coordinator"] == tx["Coordinator"] and o["Digest"] == list(hashlib.sha256(body).digest()), "authentic original transaction/request/body")
        key = (tx["Coordinator"], tx["ID"])
        require(key not in result and tx["Graph"] == state["Config"]["Graph"], "duplicate/foreign original identity")
        pair = original["Originals"]
        require(len(pair) == 2 and blob(pair[0]["Value"]) == blob(pair[1]["Value"]), "both actual original authorities")
        for row, kind, identity in zip(pair, ("id", "request"), (tx["ID"], tx["Request"]), strict=True):
            require(blob(row["Key"]) == prefix(state, tx["Coordinator"]) + kind.encode() + b"/" + identity.encode().hex().encode() and
                    json.loads(blob(row["Value"])) == o, "native original key/scope/outcome")
        cdc = original["CDC"]
        require(blob(cdc["Key"]) == prefix(state, tx["Coordinator"]) + f'changes/{o["Sequence"]:016x}/'.encode() + tx["Request"].encode().hex().encode(), "native CDC key/sequence/request")
        group = json.loads(blob(cdc["Value"]), object_pairs_hook=platform.object_pairs)
        require(group["Tx"] == tx and group["Outcome"] == o, "complete CDC original body correspondence")
        for write in group["Writes"]:
            cell = write["After"]
            identity = (write["Group"], write["Key"], cell["Version"])
            require(identity not in histories and cell["TxID"] == tx["ID"] and cell["Round"] == o["Round"], "exact physical source revision identity")
            histories[identity] = cell
        result[key] = original
    require(len(histories) == 9 and state["Final"]["Groups"][0] + state["Final"]["Groups"][1] == 5, "complete actual history/original census")
    return result, histories


def accept_source_row(actual, expected, key, value):
    require(len(actual) < 9 and key not in actual and key in expected, "duplicate/foreign/missing-replacement physical source row")
    require(value == expected[key], "exact raw source key/value differs from authentic original/view")
    actual[key] = value


def source_streams(directory, state, originals, histories):
    """Finite nine-row source verifier, not a reconstructed source artifact."""
    require(state["Config"]["Cost"] == {"version": 1, "facts": 1, "crossPercent": 100, "writers": 1}, "only the explicit one-fact fault profile")
    history_expected = {(g, prefix(state, g) + f'history/{cell["Round"]:016x}/'.encode() + key.encode().hex().encode()): go_json(cell)
                        for (g, key, _), cell in histories.items()}
    current_expected = {(g, prefix(state, g) + b"current/" + key.encode().hex().encode()): go_json(cell)
                        for g, rows in enumerate(state["Views"][2]["Values"]) for key, cell in rows.items()}
    cdc_expected = {(g, blob(original["CDC"]["Key"])): blob(original["CDC"]["Value"])
                    for (g, _), original in originals.items()}
    by_cdc = {(g, blob(original["CDC"]["Key"])): original for (g, _), original in originals.items()}
    shared = None
    for i, (name, expected) in enumerate((("history", history_expected), ("current", current_expected), ("cdc", cdc_expected))):
        data = platform.admitted_bytes(directory / ("fdb-cost-" + name + ".jsonl"), 2 << 20)
        lines = data.splitlines(keepends=True)
        require(2 <= len(lines) <= 11 and all(line.endswith(b"\n") and len(line) <= 512 << 10 for line in lines), "complete admitted raw source framing")
        frames = [json.loads(line, object_pairs_hook=platform.object_pairs) for line in lines]
        header, footer = frames[0], frames[-1]
        require(header["Type"] == "header" and header["Format"] == "fdb-graph-cost-raw-v2" and header["Stream"] == name and
                header["Config"] == state["Config"] and header["InitialCut"] == state["Initial"] and header["OldCut"] == state["Old"] and
                header["FinalCut"] == state["Final"] and header["Policy"] == {"PageRows": 8, "PageBytes": 256 << 10}, "authentic raw stream config/full cuts/policy")
        require(footer == {"Type": "footer", "Rows": state["Sources"][i]["Rows"], "Groups": state["Final"]["Groups"],
                           "Metadata": state["Sources"][i]["Metadata"], "PayloadSHA256": hashlib.sha256(b"".join(lines[:-1])).hexdigest()} and
                header["Metadata"] == footer["Metadata"] and state["Sources"][i]["Groups"] == footer["Groups"], "verified EOF payload/metadata/watermark binding")
        common = {key: value for key, value in header.items() if key != "Stream"}
        require(shared is None or common == shared, "mixed source headers")
        shared = common
        for g in (0, 1):
            for field, kind, identity, value in (("ConfigRecords", "meta", "config", state["Config"]),
                                                  ("MetadataRecords", "meta", "state", header["Metadata"][g])):
                record = header[field][g]
                require(blob(record["Key"]) == prefix(state, g) + kind.encode() + b"/" + identity.encode().hex().encode() and
                        blob(record["Value"]) == go_json(value), "actual source config/metadata record bytes")
        for record, cut in zip(header["CutRecords"], (state["Initial"], state["Old"], state["Final"]), strict=True):
            require(blob(cut["Scope"]) == b"\x00\x01" and blob(record["Key"]) == prefix(state) + b"cut/" + go_json(cut).hex().encode().hex().encode() and
                    blob(record["Value"]) == go_json(cut), "actual complete source cut record identity")
        actual, order, counts = {}, [], [0, 0]
        for frame in frames[1:-1]:
            require(set(frame) == {"Type", "Row"} and frame["Type"] == "row", "foreign/partial raw source frame")
            row = frame["Row"]
            fields = {"Group", "Key", "Value"} | ({"IDKey", "IDValue", "RequestKey", "RequestValue"} if name == "cdc" else set())
            require(set(row) == fields and type(row["Group"]) is int and row["Group"] in (0, 1), "exact raw row fields/scope")
            key = row["Group"], blob(row["Key"])
            accept_source_row(actual, expected, key, blob(row["Value"]))
            counts[row["Group"]] += 1
            if name == "cdc":
                original = by_cdc[key]
                require((blob(row["IDKey"]), blob(row["IDValue"]), blob(row["RequestKey"]), blob(row["RequestValue"])) ==
                        (blob(original["Originals"][0]["Key"]), blob(original["Originals"][0]["Value"]),
                         blob(original["Originals"][1]["Key"]), blob(original["Originals"][1]["Value"])), "every raw CDC paired original authority")
                o = original["Outcome"]
                order.append((o["Round"], o["Coordinator"], o["Request"]))
            else:
                order.append(key)
        require(actual == expected and counts == footer["Rows"] and order == sorted(order), "complete exact raw source set/sequence/EOF census")


def serial_binding(state, originals, histories):
    path = Path(__file__).resolve().parents[3] / "docs/v5/reference/independent/protocol/oracle.py"
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    require(sha == "a132b4e4981f3dcea485b7ee5dea0df48a906cdfcda4a75bec5ff9f1a5589d3a", "pinned independent serial oracle")
    spec = importlib.util.spec_from_file_location("column_existing_serial_oracle", path)
    oracle = importlib.util.module_from_spec(spec); spec.loader.exec_module(oracle)
    transactions, rounds = {}, {}
    for original in originals.values():
        tx = json.loads(blob(original["Proposal"]))
        group = json.loads(blob(original["CDC"]["Value"]))
        reads = []
        for participant in tx["Participants"]:
            for read in participant["Reads"]:
                key = f'{"AB"[participant["Group"]]}/{read["Key"]}'
                if read["Version"] == 0:
                    reads.append({"kind": "key", "key": key, "exists": False, "value": None})
                else:
                    cell = histories[(participant["Group"], read["Key"], read["Version"])]
                    require(not cell["Deleted"], "unsupported retired/deleted input")
                    reads.append({"kind": "key", "key": key, "exists": True, "value": cell["Value"], "revision": cell["TxID"]})
        writes = {f'{"AB"[w["Group"]]}/{w["Key"]}': {"op": "delete"} if w["After"]["Deleted"] else {"op": "put", "value": w["After"]["Value"]} for w in group["Writes"]}
        transactions[tx["ID"]] = {"reads": reads, "writes": writes}; rounds[tx["ID"]] = original["Outcome"]["Round"]
    require(len(state["Views"]) == 3, "actual initial/old/current complete SDK observations")
    observations = []
    for index, view in enumerate(state["Views"]):
        values = {f'{"AB"[g]}/{key}': cell["Value"] for g, rows in enumerate(view["Values"]) for key, cell in rows.items() if not cell["Deleted"]}
        require(view["Cut"] == (state["Initial"], state["Old"], state["Final"])[index], "exact observed cut identity")
        observations.append({"event": str(index), "scope": ["A", "B"], "round": view["Cut"]["Round"], "values": values})
    require(not observations[0]["values"] and len(observations[1]["values"]) == len(observations[2]["values"]) == 6, "native complete source observation cardinality")
    witness, detail = oracle.serial_witness({"initial": {}, "transactions": transactions}, set(transactions), rounds, observations,
                                           [{"event": "final", "scope": ["A", "B"], "values": observations[2]["values"]}])
    require(witness is not None, "independent serial/cut witness: " + str(detail))
    return {"Witness": witness, "OracleSHA256": sha, "Scope": "actual raw physical source observations/original reads; no graph reducer/protocol simulation"}


def finalize(directory, ledger_path):
    directory = Path(directory)
    terminal, seed, interrupted, resumed = (load(directory / name) for name in ("terminal.json", "seed.json", "publication-state.json", "recovery.json"))
    state = terminal["Record"]
    require(terminal["Format"] == state["Format"] == FORMAT and terminal["SetupOperations"] == 3 and terminal["UsefulOperations"] == 2, "distinct finite publication source scope")
    require(state["Case"] == seed["Case"] == interrupted["Case"] and state["Coordinator"] == seed["Coordinator"] == interrupted["Coordinator"] and
            state["Config"] == seed["Config"] and state["Owner"] == seed["Owner"] == interrupted["Owner"] == resumed["Owner"] and
            state["Token"] == seed["Token"] == interrupted["Token"] == resumed["Token"], "exact established durable owner/token/config across real process replacement")
    require(seed["Originals"] == state["Originals"][:4] and interrupted["Originals"] == resumed["Originals"] == state["Originals"] and
            interrupted["Stages"] == state["Stages"][:3] and resumed["Stages"] == state["Stages"][:9], "actual caller original/stage prefix preserved")
    marker(load(directory / "publication-marker.json"), interrupted, platform.admitted_bytes(directory / "publication-state.json", 2 << 20))
    stages = state["Stages"]
    require([s["Name"] for s in stages] == ["owner-active-before-update", "held-old-after-update",
            {"chunk": "chunk", "source-eof": "source-eof", "manifest-issued": "native-manifest-commit-issued"}[state["Case"]],
            "expired-new-open-refused-held-owner-live", "cached-old", "original-owner-authenticated-close", "final-generation", "cached-final", "collection-ready", "terminal-lineage-preserved"], "complete actual lifetime/checkpoint inventory")
    if state["Case"] != "manifest-issued":
        require(stages[2]["Refusal"] == "manifest-unavailable-no-eof" and
                blob(stages[2]["Missing"]) == generation_key(state, "final", "manifest"), "incomplete generation never returned current/empty/partial EOF")
    gc_state = load(directory / "gc-state.json")
    require(gc_state == {**resumed, "Pages": state["Pages"][:1]}, "exact state prefix across collector child replacement")
    gc_marker(load(directory / "gc-marker.json"), gc_state)
    originals, histories = original_joins(state)
    source_streams(directory, state, originals, histories)
    caches(directory, state, originals)
    binding = native(row_map(stages[0]["Records"]), prefix(state) + b"columns-v1/binding")
    require(binding["Config"] == state["Config"] and binding["Scope"] == state["Config"]["CostScope"] and binding["Policy"] == state["Columns"] and
            digest(binding) == state["OldManifest"]["Generation"]["Binding"] == state["FinalManifest"]["Generation"]["Binding"], "full actual layout/config/axis/policy binding")
    for index, active in ((0, True), (1, True), (3, True), (5, False)):
        rows = row_map(stages[index]["Records"])
        owner_key = prefix(state) + b"columns-v1/read-owner/" + state["Owner"]["Request"].encode().hex().encode()
        owner = native(rows, owner_key)
        guard = native(rows, generation_key(state, "old", "owners"))
        require(owner["Owner"] == state["Owner"] and owner["State"] == (1 if active else 2) and
                guard["Readers"] == (1 if active else 0), "original physical ownership must survive until authenticated Close")
        if index == 3:
            pin_key = prefix(state) + b"columns-v1/retention-v1/pin/" + state["Token"]["Request"].encode().hex().encode()
            require(native(rows, pin_key)["State"] == 3 and guard["Pins"] == guard["References"] == 0, "token expiry/named head move did not protect held owner")
    before = native(row_map(stages[8]["Records"]), prefix(state) + b"columns-v1/ledger")
    require(len(state["Pages"]) == 4 and state["Pages"][0] == load(directory / "gc-marker.json")["Page"], "actual committed page before collector process loss")
    for i, page in enumerate(state["Pages"]):
        result, rows = page["Result"], row_map(page["Records"])
        key = generation_key(state, "old", f"chunk/{i:016x}")
        generation = hashlib.sha256(go_json(state["OldManifest"]["Generation"])).hexdigest()
        body = platform.admitted_bytes(directory / "cache-old" / f"column-{generation}-{i:016x}.bin", 32 << 10)
        debit = len(key) + len(body)
        after = native(rows, prefix(state) + b"columns-v1/ledger")
        guard = native(rows, generation_key(state, "old", "owners"))
        require(blob(page["Missing"]["Key"]) == key and page["Missing"]["Value"] is None and result["Cursor"] == i + 1 and
                result["WorkChunks"] == 1 and result["WorkBytes"] == debit and result["Done"] == (i == 3) and
                guard["Cursor"] == i + 1 and guard["DeletedBytes"] == result["DeletedBytes"] and
                after == {**before, "Bytes": before["Bytes"] - debit, "Chunks": before["Chunks"] - 1}, "exact atomic native page/cursor/byte accounting")
        before = after
    require(guard["State"] == 3 and guard["DeletedBytes"] == state["OldManifest"]["Bytes"] and guard["Chain"] == state["OldManifest"]["Chain"], "complete derived-only collection and retained tombstone chain")
    ledger_data = platform.admitted_bytes(ledger_path, 16 << 20)
    ledger = platform.records(ledger_path)
    plan = load(directory / "clients.json")
    ledger = ledger[plan["LedgerStart"]:]
    for phase in ("publication", "gc"):
        before_kill, stopped = load(directory / (phase + ".before.json"))[0], load(directory / (phase + ".stopped.json"))[0]
        platform.killed(before_kill, stopped)
        name = before_kill["Name"].lstrip("/")
        start = platform.client_command(ledger, name)
        kill = platform.commands(ledger, ["kill", "--signal", "KILL", name])
        following = platform.client_command(ledger, plan["Recovery" if phase == "publication" else "Terminal"])
        require(start < kill < following, "real child reap precedes genuine replacement SDK client")
        require(before_kill["Config"]["Cmd"] == ["fdb-spike", "column-fault-" + ("publication-child" if phase == "publication" else "gc-child")], "actual production-door native child")
    launcher.healthy(load(directory / "healthy-after.json"))
    require([sum(e["Rows"]) for e in state["Sources"]] == [9, 6, 5], "three authentic source EOFs remain distinct from derived collection")
    # Own the exact observed prefix; later launcher cleanup remains separately sealed.
    with (directory / "commands.prefix.jsonl").open("xb") as output: output.write(ledger_data)
    proof = {"Format": FORMAT, "Case": state["Case"], "Coordinator": state["Coordinator"],
             "SetupOperations": 3, "UsefulOperations": 2, "Serial": serial_binding(state, originals, histories),
             "Scope": "real publisher/derived-collector SDK child replacement on one physical host; source KV/history retained; no source-history process-restart, matrix or capacity acceptance",
             "ArtifactsSHA256": {str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(directory.rglob("*")) if p.is_file() and p.name != "proof.json"}}
    with (directory / "proof.json").open("x") as output: json.dump(proof, output, indent=2); output.write("\n")


def main():
    mode, *args = sys.argv[1:]
    if mode == "marker":
        marker(load(args[0]), load(args[1]), platform.admitted_bytes(args[1], 2 << 20))
    elif mode == "gc-marker":
        gc_marker(load(args[0]), load(args[1]))
    elif mode == "ledger-start":
        print(len(platform.records(args[0])))
    elif mode == "finalize":
        finalize(*args)
    else:
        raise ValueError("unsupported explicit publication action")


if __name__ == "__main__":
    main()
