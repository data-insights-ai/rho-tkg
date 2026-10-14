#!/usr/bin/env python3
"""Finite authentic source-history SDK caller-loss and cursor-resume joins.

Private verifier state is limited to this five-COMMIT/nine-effect fixture. This
is not a graph reducer, ABORT authority, capacity proof or full fault matrix.
"""
import base64
import hashlib
import json
from pathlib import Path
import sys

import launcher
import platform_fault_controller as platform
import column_publication_controller as source

FORMAT = "fdb-source-history-faults-v1"
require, load = platform.require, platform.load
blob, go_json, prefix, row_map, native = source.blob, source.go_json, source.prefix, source.row_map, source.native


def key(state, name):
    return prefix(state) + b"source-retention-v1/" + name.encode().hex().encode()


def owner_key(state, request):
    return prefix(state) + b"source-owner-v1/" + request.encode().hex().encode()


def physical(rows, expected):
    actual = {}
    for row in rows or []:
        require(set(row) == {"Group", "Key", "Value"} and type(row["Group"]) is int and row["Group"] in (0, 1), "exact physical SDK row shape")
        identity = row["Group"], blob(row["Key"])
        source.accept_source_row(actual, expected, identity, blob(row["Value"]))
    return actual


def marker(value, state, raw, phase):
    ready = {"owner": "source-owner-live-after-expiration", "gc": "source-history-deletion-page-committed"}[phase]
    require(value == {"Ready": ready, "Format": FORMAT, "Coordinator": state["Coordinator"],
                      "StateSHA256": list(hashlib.sha256(raw).digest())}, "exact caller-established identity/stored-state marker")
    if phase == "gc":
        require(0 < len(state["Pages"]) <= 32, "finite actual collected pages")
        partial(state["Pages"][-1])


def partial(page):
    result, after = page["Result"], page["After"]["State"]
    require(not result["Done"] and result["Phase"] == after["Phase"] == 2 and
            sum(result["Deleted"]) > 0 and len(blob(after["Cursor"])) > 0,
            "actual committed nonterminal source deletion, never a census-only or sealed page")


def authority(state, observation, final_cut=True):
    rows = row_map(observation["Records"])
    require(native(rows, key(state, "binding")) == state["Binding"] and
            native(rows, key(state, "state")) == observation["State"] and
            rows[key(state, "binding")] == go_json(state["Binding"]) and rows[key(state, "state")] == go_json(observation["State"]), "exact native immutable binding and durable state bytes")
    require(state["Binding"] == {"Version": 1, "Config": state["Config"], "Initial": state["Initial"],
            "Policy": {"MaxOwners": 16, "MaxControlBytes": 128 << 10, "PageRows": 1, "PageBytes": 128 << 10,
                       "MaxWorkRows": 1024, "MaxWorkBytes": 8 << 20, "MaxChainSteps": 8, "MaxOutputBytes": 4096}}, "explicit finite policy, not capacity")
    ledger = native(rows, key(state, "ledger"))
    require(0 <= ledger["Records"] <= 16 and 0 < ledger["Bytes"] <= 128 << 10, "cumulative retained control admission")
    for g in (0, 1):
        for name, body in (("config", state["Config"]), ("state", observation["Metadata"][g])):
            require(rows[prefix(state, g) + b"meta/" + name.encode().hex().encode()] == go_json(body), "actual native graph metadata/config bytes")
    for cut in (state["Initial"], state["Old"], *((state["Final"],) if final_cut else ())):
        if cut["Graph"]:
            platform.full_scope(cut["Scope"])
            require(rows[prefix(state) + b"cut/" + go_json(cut).hex().encode().hex().encode()] == go_json(cut), "authentic complete retained cut bytes")
    return rows


def ownership(state, seed):
    require(seed["Owner"] == state["Owner"] and seed["Token"] == state["Token"], "factory identity precedes acquisition unchanged")
    owner, token = state["Owner"], state["Token"]
    require(owner["Kind"] == "read" and owner["State"] == 1 and owner["Cut"] == token["Cut"] == state["Old"] and
            owner["Required"] == state["Initial"] and owner["PinRequest"] == token["Request"] and
            owner["PinDigest"] == list(hashlib.sha256(go_json(token)).digest()) and any(owner["Secret"]), "exact Required prefix/token/secret bindings")
    names = [s["Name"] for s in state["Stages"]]
    require(names == ["factory-intent-before-acquisition", "owner-active-before-update", "held-old-after-update",
                      "expired-new-open-refused-held-owner-live", "replacement-authenticated-held-old", "original-owner-authenticated-close",
                      "retirement-admitted", "guard-refusals-unchanged", "replacement-exact-partial-cursor", "sealed-original-lineage", "terminal-complete"], "exact genuine lifecycle checkpoint inventory")
    for i in (0, 1, 2, 3, 4, 5):
        stage = state["Stages"][i]
        rows = authority(state, stage["Observation"], i >= 2)
        stored = rows[owner_key(state, owner["Request"])]
        if i == 0:
            require(stored is None, "owner intent saved before native acquisition")
        elif i < 5:
            require(stored == go_json(owner), "retained original active identity across genuine caller replacement")
        else:
            require(stored == go_json({**owner, "State": 2, "TerminalRequest": "close"}), "authenticated same-owner terminal Close")
        pin = native(rows, owner_key(state, token["Request"]))
        require(pin["State"] == (1 if i < 3 else 3) and pin["Cut"] == token["Cut"] and pin["Secret"] == token["Secret"], "explicit expiry cannot revoke held reader or authorize new open")
        if i in (1, 2, 3, 4):
            expected = {(g, prefix(state, g) + f'history/{v["Round"]:016x}/'.encode() + name.encode().hex().encode()): go_json(v)
                        for g, group in enumerate(state["Views"][1]["Values"]) for name, v in group.items()}
            require(physical(stage["Rows"], expected) == expected and sum(stage["EOF"]) == 6, "held owner remembers exact old5 after update6 and process replacement")
    require(state["Stages"][3]["Observation"] == state["Stages"][4]["Observation"], "replacement cannot manufacture or alter native ownership authority")
    require(state["Stages"][6]["Observation"] == state["Stages"][7]["Observation"], "two guard refusals create no graph/CDC/owner effects")


def pages(state, expected_history, current):
    previous = state["Stages"][7]["Observation"]
    original_counts = [sum(g == i for g, _ in expected_history) for i in (0, 1)]
    expected_current = {(g, prefix(state, g) + b"current/" + name.encode().hex().encode()): go_json(v)
                        for g, group in enumerate(state["Views"][2]["Values"]) for name, v in group.items()}
    require(physical(previous["History"], expected_history) == expected_history, "complete pre-GC native physical history")
    start = previous["State"]
    require(start["Phase"] == 1 and start["Coordinator"] == start["Group"] == start["Sequence"] == start["WorkRows"] == start["WorkBytes"] == 0 and
            start["Cursor"] is None and start["Originals"] == start["Scanned"] == start["Anchors"] == start["Deleted"] == start["DeletedBytes"] == start["CumulativeDeleted"] == [0, 0] and
            start["Chain"] == [0] * 32 and [m["Versions"] for m in start["Before"]] == original_counts,
            "fresh complete census starts before any physical retirement")
    total_deleted, total_bytes = [0, 0], [0, 0]
    for page in state["Pages"]:
        before, after, result, budget = page["Before"], page["After"], page["Result"], page["Budget"]
        require(before == previous and budget == {"Rows": 1, "Bytes": 128 << 10, "OutputBytes": 4096}, "actual unchanged cursor snapshot and cumulative one-row policy")
        authority(state, before); rows = authority(state, after)
        old = physical(before["History"], expected_history)
        new = physical(after["History"], expected_history)
        require(set(new) <= set(old) and all(old[k] == v for k, v in new.items()), "collection cannot insert/substitute physical originals")
        require(physical(before["Current"], expected_current) == physical(after["Current"], expected_current) == expected_current,
                "source collection preserves exact current/index anchors")
        removed = set(old) - set(new)
        require(len(removed) <= 1 and not removed.intersection(current), "bounded deletion cannot remove a required predecessor/current anchor")
        deleted, debits = [sum(k[0] == g for k in removed) for g in (0, 1)], [sum(len(k[1]) + len(old[k]) for k in removed if k[0] == g) for g in (0, 1)]
        require(result["Phase"] == before["State"]["Phase"] and result["Deleted"] == deleted and result["DeletedBytes"] == debits and
                0 <= result["WorkRows"] <= 1 and 0 <= result["WorkBytes"] <= budget["Bytes"] and
                after["State"]["WorkRows"] == before["State"]["WorkRows"] + result["WorkRows"] <= 1024 and
                after["State"]["WorkBytes"] == before["State"]["WorkBytes"] + result["WorkBytes"] <= 8 << 20,
                "actual page effects/cumulative admitted logical work")
        prior, following = before["State"], after["State"]
        require(following["Before"] == prior["Before"] and following["Owners"] == prior["Owners"] == 0 and
                (result["WorkBytes"] > 0) == (result["WorkRows"] > 0), "fixed retirement metadata/ownership and work admission")
        if prior["Phase"] == 1:
            candidates = [o for o in state["Originals"] if o["Outcome"]["Coordinator"] == prior["Coordinator"] and o["Outcome"]["Sequence"] > prior["Sequence"]]
            candidates.sort(key=lambda o: o["Outcome"]["Sequence"])
            selected = candidates[:1]
            require(not removed and result["WorkRows"] == len(selected) and following["Group"] == prior["Group"] and following["Cursor"] == prior["Cursor"], "census cannot delete or change physical cursor")
            counts, chain = list(prior["Originals"]), bytes(prior["Chain"])
            if selected:
                o = selected[0]; group = json.loads(blob(o["CDC"]["Value"]), object_pairs_hook=platform.object_pairs)
                for write in group["Writes"]: counts[write["Group"]] += 1
                ck, cv = blob(o["CDC"]["Key"]), blob(o["CDC"]["Value"])
                chain = hashlib.sha256(chain + len(ck).to_bytes(8, "big") + ck + len(cv).to_bytes(8, "big") + cv).digest()
            coordinator = prior["Coordinator"] + (not selected)
            require(following["Coordinator"] == coordinator and following["Sequence"] == (selected[0]["Outcome"]["Sequence"] if selected else 0) and
                    following["Originals"] == counts and following["Chain"] == list(chain) and following["Scanned"] == prior["Scanned"] and following["Anchors"] == prior["Anchors"] and
                    following["Phase"] == (2 if coordinator == 2 else 1), "exact authentic coordinator original census progress")
        else:
            require(prior["Phase"] == 2, "only genuine census/deletion pages before seal")
            cursor = b"" if prior["Cursor"] is None else blob(prior["Cursor"])
            candidates = sorted(k for k in old if k[0] == prior["Group"] and k[1] > cursor)
            selected = candidates[:1]
            scanned, anchors = list(prior["Scanned"]), list(prior["Anchors"])
            if selected:
                selected_key = selected[0]; scanned[selected_key[0]] += 1
                if selected_key in current: anchors[selected_key[0]] += 1
                require(removed == (set() if selected_key in current else {selected_key}), "only exact next original predecessor can be deleted")
            group = prior["Group"] + (not selected)
            require(result["WorkRows"] == len(selected) and following["Group"] == group and
                    following["Cursor"] == (base64.b64encode(selected[0][1]).decode() if selected else None) and
                    following["Scanned"] == scanned and following["Anchors"] == anchors and following["Coordinator"] == prior["Coordinator"] and
                    following["Originals"] == prior["Originals"] and following["Chain"] == prior["Chain"] and
                    following["Phase"] == (3 if group == 2 else 2) and result["Done"] == (group == 2), "exact bounded physical-key cursor/anchor/EOF progress")
        for g in (0, 1):
            total_deleted[g] += deleted[g]; total_bytes[g] += debits[g]
            require(after["Metadata"][g] == {**before["Metadata"][g], "Versions": before["Metadata"][g]["Versions"] - deleted[g], "Bytes": before["Metadata"][g]["Bytes"] - debits[g]} and
                    after["State"]["Deleted"][g] == total_deleted[g] and after["State"]["DeletedBytes"][g] == total_bytes[g], "atomic actual physical byte/version debit")
        require(after["State"]["Request"] == "process-final" and after["State"]["Target"] == state["Final"] and
                native(rows, key(state, "intent/process-final")) == {"Request": "process-final", "Target": state["Final"]}, "same immutable native retirement identity")
        if not result["Done"]:
            require(rows[key(state, "seal/process-final")] is None, "partial cursor grants no completed retirement lineage")
        previous = after
    seal = previous["State"]["Completed"]
    require(2 <= len(state["Pages"]) <= 32 and state["Pages"][-1]["Result"]["Done"] and previous["State"]["Phase"] == 3 and
            state["Pages"][-1]["Result"]["Seal"] == seal and native(row_map(previous["Records"]), key(state, "seal/process-final")) == seal and
            seal["Request"] == "process-final" and seal["Cut"] == state["Final"] and seal["Before"] == state["Stages"][7]["Observation"]["Metadata"] and
            seal["After"] == previous["Metadata"] and seal["Originals"] == original_counts and seal["Deleted"] == seal["CumulativeDeleted"] == total_deleted and
            seal["DeletedBytes"] == total_bytes and sum(total_deleted) == 3 and seal["Anchors"] == [sum(g == i for g, _ in current) for i in (0, 1)] and
            physical(previous["History"], expected_history) == current, "complete actual source seal and exact retained anchors, never count-only success")
    chain = bytes(32)
    for original in sorted(state["Originals"], key=lambda o: (o["Outcome"]["Coordinator"], o["Outcome"]["Sequence"])):
        k, value = blob(original["CDC"]["Key"]), blob(original["CDC"]["Value"])
        chain = hashlib.sha256(chain + len(k).to_bytes(8, "big") + k + len(value).to_bytes(8, "big") + value).digest()
    require(seal["CensusSHA256"] == list(chain), "complete original native-order census chain")
    return previous


def post_sources(directory, state, originals, current, metadata):
    expected_current = {(g, prefix(state, g) + b"current/" + name.encode().hex().encode()): go_json(v)
                        for g, group in enumerate(state["Views"][2]["Values"]) for name, v in group.items()}
    expected_cdc = {(g, blob(o["CDC"]["Key"])): blob(o["CDC"]["Value"]) for (g, _), o in originals.items()}
    by_cdc = {(g, blob(o["CDC"]["Key"])): o for (g, _), o in originals.items()}
    shared = None
    for i, (name, expected) in enumerate((("history", current), ("current", expected_current), ("cdc", expected_cdc))):
        data = platform.admitted_bytes(directory / ("fdb-cost-" + name + ".jsonl"), 2 << 20)
        lines = data.splitlines(keepends=True)
        require(2 <= len(lines) <= 11 and all(s.endswith(b"\n") and len(s) <= 512 << 10 for s in lines), "closed raw source framing/EOF")
        frames = [json.loads(s, object_pairs_hook=platform.object_pairs) for s in lines]
        header, footer = frames[0], frames[-1]
        require(set(header) == {"Type", "Format", "Stream", "Config", "InitialCut", "OldCut", "FinalCut", "Policy", "Metadata", "ConfigRecords", "MetadataRecords", "CutRecords"} and header["Type"] == "header" and header["Format"] == FORMAT and header["Stream"] == name and
                header["Config"] == state["Config"] and header["InitialCut"] == state["Initial"] and header["OldCut"] == state["Old"] and
                header["FinalCut"] == state["Final"] and header["Policy"] == {"PageRows": 1, "PageBytes": 128 << 10} and header["Metadata"] == metadata,
                "separate post-retirement native source format/config/cut/policy")
        require(footer == {"Type": "footer", "Rows": state["Sources"][i]["Rows"], "Groups": state["Final"]["Groups"], "Metadata": metadata,
                           "PayloadSHA256": hashlib.sha256(b"".join(lines[:-1])).hexdigest()} and
                state["Sources"][i]["Groups"] == state["Final"]["Groups"] and state["Sources"][i]["Metadata"] == metadata,
                "actual post-GC source EOF/metadata/payload seal")
        common = {k: v for k, v in header.items() if k != "Stream"}
        require(shared is None or common == shared, "mixed native source snapshots"); shared = common
        for g in (0, 1):
            for field, identity, body in (("ConfigRecords", "config", state["Config"]), ("MetadataRecords", "state", metadata[g])):
                record = header[field][g]
                require(blob(record["Key"]) == prefix(state, g) + b"meta/" + identity.encode().hex().encode() and blob(record["Value"]) == go_json(body), "raw native config/metadata identity")
        for record, cut in zip(header["CutRecords"], (state["Initial"], state["Old"], state["Final"]), strict=True):
            platform.full_scope(cut["Scope"])
            require(blob(record["Key"]) == prefix(state) + b"cut/" + go_json(cut).hex().encode().hex().encode() and blob(record["Value"]) == go_json(cut), "exact retained double-encoded native cut key/body")
        actual, order, counts = {}, [], [0, 0]
        for frame in frames[1:-1]:
            require(set(frame) == {"Type", "Row"} and frame["Type"] == "row", "no extra/mixed raw source frame")
            row = frame["Row"]
            require(set(row) == {"Group", "Key", "Value"} | ({"IDKey", "IDValue", "RequestKey", "RequestValue"} if name == "cdc" else set()) and
                    type(row["Group"]) is int and row["Group"] in (0, 1), "exact raw source row fields")
            k = row["Group"], blob(row["Key"])
            source.accept_source_row(actual, expected, k, blob(row["Value"])); counts[row["Group"]] += 1
            if name == "cdc":
                original = by_cdc[k]
                require([blob(row[f]) for f in ("IDKey", "IDValue", "RequestKey", "RequestValue")] ==
                        [blob(original["Originals"][j][f]) for j in (0, 1) for f in ("Key", "Value")], "raw post-GC CDC preserves both original authorities")
                o = original["Outcome"]; order.append((o["Round"], o["Coordinator"], o["Request"]))
            else:
                order.append(k)
        require(actual == expected and counts == footer["Rows"] and order == sorted(order), "exact complete retained source rows/unsigned native enumeration")


def finalize(directory, ledger_path):
    directory = Path(directory)
    terminal, seed, interrupted, recovery, gc = (load(directory / n) for n in ("terminal.json", "seed.json", "owner-state.json", "recovery.json", "gc-state.json"))
    state = terminal["Record"]
    require(terminal["Format"] == state["Format"] == FORMAT and terminal["SetupOperations"] == 3 and terminal["UsefulOperations"] == 2,
            "separate finite source-history process scope")
    require(state["Config"]["Cost"] == {"version": 1, "facts": 1, "crossPercent": 100, "writers": 1} and state["Config"]["Limits"]["Transactions"] == 5,
            "unchanged five-original writer1 binding; no ABORT/headroom widening")
    for previous in (seed, interrupted, recovery, gc):
        require(previous["Config"] == state["Config"] and previous["Coordinator"] == state["Coordinator"] and previous["Binding"] == state["Binding"] and
                previous["Owner"] == state["Owner"] and previous["Token"] == state["Token"], "exact caller-established config/factory owner across processes")
    require(seed["Originals"] == state["Originals"][:4] and interrupted["Originals"] == recovery["Originals"] == gc["Originals"] == state["Originals"] and
            seed["Stages"] == state["Stages"][:1] and interrupted["Stages"] == state["Stages"][:4] and recovery["Stages"] == gc["Stages"] == state["Stages"][:8] and
            not recovery["Pages"] and gc["Pages"] == state["Pages"][:len(gc["Pages"])] and
            state["Stages"][8]["Observation"] == gc["Pages"][-1]["After"], "authentic immutable caller prefix and exact persisted partial cursor resume")
    for phase, record in (("owner", interrupted), ("gc", gc)):
        marker(load(directory / (phase + "-marker.json")), record, platform.admitted_bytes(directory / (phase + "-state.json"), 2 << 20), phase)
    ownership(state, seed)
    originals, histories = source.original_joins(state)
    requests = {(g, o["Outcome"]["Request"]) for (g, _), o in originals.items()}
    require(len(requests) == 5, "unique original request/ID pair inventory")
    for g in (0, 1):
        orders = sorted(o["Outcome"]["Sequence"] for (owner, _), o in originals.items() if owner == g)
        require(orders == list(range(1, state["Final"]["Groups"][g] + 1)), "complete contiguous coordinator CDC/original sequence")
    for cut, subset in ((state["Initial"], []), (state["Old"], state["Originals"][:4]), (state["Final"], state["Originals"])):
        require(cut["Groups"] == [sum(o["Outcome"]["Coordinator"] == g for o in subset) for g in (0, 1)] and
                all(o["Outcome"]["Round"] <= cut["Round"] for o in subset), "exact original membership at each immutable barrier")
    require(state["Originals"][-1]["Outcome"]["Round"] > state["Old"]["Round"], "update is genuinely after the old cut")
    for ordinal, original in enumerate(state["Originals"], 1):
        tx = json.loads(blob(original["Proposal"]), object_pairs_hook=platform.object_pairs)
        require(tx["Lifecycle"]["Provenance"] == ordinal, "separate genuine fixture execution lineage, not shared total CDC round order")
        group = json.loads(blob(original["CDC"]["Value"]), object_pairs_hook=platform.object_pairs)
        for write in group["Writes"]:
            after = write["After"]
            if after["Version"] > 1:
                require(write["Before"] == histories[(write["Group"], write["Key"], after["Version"] - 1)], "exact authenticated predecessor, no same-count before substitution")
    source.source_streams(directory / "before", {**state, "Sources": state["BeforeSources"]}, originals, histories)
    expected_history = {(g, prefix(state, g) + f'history/{cell["Round"]:016x}/'.encode() + name.encode().hex().encode()): go_json(cell)
                        for (g, name, _), cell in histories.items()}
    current = {(g, prefix(state, g) + f'history/{v["Round"]:016x}/'.encode() + name.encode().hex().encode()): go_json(v)
               for g, group in enumerate(state["Views"][2]["Values"]) for name, v in group.items()}
    final = pages(state, expected_history, current)
    require([m["Transactions"] for m in final["Metadata"]] == state["Final"]["Groups"], "COMMIT-only complete retained original inventory")
    post_sources(directory, state, originals, current, final["Metadata"])
    diagnostic = blob(state["Diagnostic"]); tx = json.loads(diagnostic, object_pairs_hook=platform.object_pairs)
    require(len(state["GuardRefusals"]) == 2, "two same immutable graph-level guard refusals")
    for refusal in state["GuardRefusals"]:
        require(refusal["Classification"] == "retirement-busy" and refusal["Request"] == tx["Request"] and refusal["Transaction"] == tx["ID"] and
                refusal["BodySHA256"] == list(hashlib.sha256(diagnostic).digest()) and not refusal["Outcome"]["Commit"] and
                refusal.get("NativeCode") is None and not refusal["Retryable"] and not refusal["MaybeCommitted"] and refusal["SubmitInvocations"] == 1 and
                1 <= refusal["SDKCallbacks"] <= 16 and refusal["Error"], "no invented native code or absence-as-ABORT authority")
        for record, kind, identity in zip(refusal["Originals"], ("id", "request"), (tx["ID"], tx["Request"]), strict=True):
            require(blob(record["Key"]) == prefix(state, tx["Coordinator"]) + kind.encode() + b"/" + identity.encode().hex().encode() and record["Value"] is None, "actual both-absent diagnostic observation only")
    require(len(state["Replays"]) == 5, "one SAME-body replay per actual COMMIT")
    for replay, original in zip(state["Replays"], state["Originals"], strict=True):
        require(replay["Classification"] == "original-commit" and replay["Outcome"] == original["Outcome"] and replay["Originals"] == original["Originals"] and
                replay["Original"] == original["CDC"] and replay["BodySHA256"] == list(hashlib.sha256(blob(original["Proposal"])).digest()) and
                replay["Request"] == original["Outcome"]["Request"] and replay["Transaction"] == original["Outcome"]["ID"] and replay["SubmitInvocations"] == 1 and
                not replay["Error"] and replay.get("NativeCode") is None and not replay["MaybeCommitted"] and not replay["Retryable"], "actual post-GC immutable COMMIT replay")
    ledger_data = platform.admitted_bytes(ledger_path, 16 << 20); ledger = platform.records(ledger_path)
    plan = load(directory / "clients.json"); ledger = ledger[plan["LedgerStart"]:]
    identities = []
    for phase, after in (("owner", plan["Recovery"]), ("gc", plan["Terminal"])):
        before, stopped = load(directory / (phase + ".before.json"))[0], load(directory / (phase + ".stopped.json"))[0]
        platform.killed(before, stopped); identities.append(platform.live(before))
        name = before["Name"].lstrip("/")
        require(before["Config"]["Cmd"] == ["fdb-spike", "history-fault-" + ("owner-child" if phase == "owner" else "gc-child")] and
                platform.client_command(ledger, name) < platform.commands(ledger, ["kill", "--signal", "KILL", name]) < platform.client_command(ledger, after),
                "actual SDK child invocation/reap before genuine replacement client")
    require(identities[0] != identities[1], "distinct actual SDK caller instances")
    launcher.healthy(load(directory / "healthy-after.json"))
    require([sum(e["Rows"]) for e in state["BeforeSources"]] == [9, 6, 5] and [sum(e["Rows"]) for e in state["Sources"]] == [6, 6, 5],
            "physical retirement distinct from complete retained original effects")
    with (directory / "commands.prefix.jsonl").open("xb") as output: output.write(ledger_data)
    proof = {"Format": FORMAT, "Coordinator": state["Coordinator"], "SetupOperations": 3, "UsefulOperations": 2,
             "Serial": source.serial_binding(state, originals, histories),
             "Scope": "actual source-history SDK owner/collector caller loss and same-identity recovery; five COMMITs only; no ABORT/server-loss/power-loss/matrix/capacity acceptance",
             "ArtifactsSHA256": {str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(directory.rglob("*")) if p.is_file() and p.name != "proof.json"}}
    with (directory / "proof.json").open("x") as output: json.dump(proof, output, indent=2); output.write("\n")


def main():
    mode, *args = sys.argv[1:]
    if mode in ("owner-marker", "gc-marker"):
        marker(load(args[0]), load(args[1]), platform.admitted_bytes(args[1], 2 << 20), mode.split("-")[0])
    elif mode == "ledger-start":
        print(len(platform.records(args[0])))
    elif mode == "finalize":
        finalize(*args)
    else:
        raise ValueError("unsupported explicit source-history action")


if __name__ == "__main__":
    main()
