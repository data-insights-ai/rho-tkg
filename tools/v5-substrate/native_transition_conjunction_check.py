#!/usr/bin/env python3
"""Fresh finite P06 × recovery Ready/install source bridge.

Successful replies, unanswered source attempts and actual retained originals are
different inventories. No missing Decide reply is invented. Native/OS receipts
are diagnostics from the Go parent, never independent ACK or power-loss proof.
The existing P06/default batch grammars are deliberately not widened.
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


B = module("p06_conjunction_batch", "native_transition_batch_check.py")
F = module("p06_conjunction_process", "native_fault_family_check.py")
T, C, P = B.T, B.C, B.P
require, integer, fixed = T.require, T.integer, T.fixed
PROFILE = "native-p06-decide-sync-conjunction-v1"
RECOVERY_PROFILE = "native-p06-recovery-sync-conjunction-v2"
TERMINAL_PROFILE = "native-p06-terminal-sync-conjunction-v3"
HEADER = set("Version Profile Case Recipe Run DeadlineUnixNanos Actor FirstVoter Target Successor P06 Attempts UnknownPhases Arm Batch SelectedCommand TargetTransportLoss Processes FirstReopened FaultReopened ReceiptReads ReceiptGrantBytes GateCandidates GateDecodeWork CaptureCandidates CaptureAttempts CaptureWork CaptureOutput CaptureRetained".split())
TERMINAL_HEADER = HEADER | {"Duplicate"}
P06 = set("Version Coordinator Commit Original Manifest Pending Decision FirstYES Processes Refused Closed Changed Duplicates Phases Before Recovered Current Retained From Through Records".split())
RECIPE = {"Coordinator", "Commit", "Kind", "Side"}
LOCAL = {"Partition", "Index", "Proposal", "Outcome", "CDC"}
PHASE = {"Origin", "Partition", "Request", "Reply"}


def recipe(row, version=1):
    require(type(version) is int and version in (1, 2, 3), "implemented conjunction version")
    keys = RECIPE if version == 1 else RECIPE | {"Phase", "Partition"}
    require(type(row) is dict and set(row) == (keys | {"Stage"} if version == 3 else keys), "conjunction recipe grammar")
    require(type(row["Coordinator"]) is int and row["Coordinator"] in (3, 8) and type(row["Commit"]) is bool, "actual coordinator/outcome recipe")
    require(type(row["Kind"]) is int and row["Kind"] in (1, 2) and row["Side"] in ("before", "after"), "implemented Decide native boundary")
    prefix = f'p06-co{row["Coordinator"]}-commit{str(row["Commit"]).lower()}'
    if version == 3:
        require(row["Stage"] == "terminal-replay" and type(row["Phase"]) is int and type(row["Partition"]) is int and (row["Phase"], row["Partition"]) in ((2, 3), (4, 3), (4, 8)), "actual terminal original occurrence")
        return f'{prefix}-terminal{row["Phase"]}-p{row["Partition"]}-k{row["Kind"]}-{row["Side"]}'
    if version == 2:
        require(type(row["Phase"]) is int and row["Phase"] in (2, 4) and type(row["Partition"]) is int and row["Partition"] in (3, 8), "actual additional recovery occurrence")
        return f'{prefix}-recovery{row["Phase"]}-p{row["Partition"]}-k{row["Kind"]}-{row["Side"]}'
    return f'{prefix}-decide-k{row["Kind"]}-{row["Side"]}'


def load(path, *, program=False):
    digest, total, header, footer, records = hashlib.sha256(), 0, None, None, []
    with path.open("rb") as source:
        while line := source.readline((512 << 10)+1):
            total += len(line)
            require(len(line) <= 512 << 10 and total <= 16 << 20 and line.endswith(b"\n"), "finite conjunction frame/artifact")
            row = T.decoded(line)
            require(type(row) is dict and footer is None, "row after conjunction EOF")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and row["Type"] == "header", "conjunction header row")
                header = row["Artifact"]
                require(type(header) is dict and set(header) == (TERMINAL_HEADER if header.get("Version") == 3 else HEADER) | ({"Program"} if program else set()) and type(header["P06"]) is dict and set(header["P06"]) == P06 and header["P06"]["Records"] is None, "fresh grammar cannot inline prefix")
            elif row.get("Type") == "record":
                require(set(row) == {"Type", "Record"} and len(records) < 512, "bounded actual record row")
                records.append(row["Record"])
            else:
                require(set(row) == {"Type", "Rows", "Through", "PayloadSHA256"} and row["Type"] == "footer", "conjunction EOF grammar")
                require(type(row["Rows"]) is int and row["Rows"] == len(records) and row["PayloadSHA256"] == digest.hexdigest(), "complete native prefix hash/census")
                footer = row
            if footer is None:
                digest.update(line)
    require(header is not None and footer is not None and footer["Through"] == header["P06"]["Through"], "missing/foreign actual source EOF")
    header["P06"]["Records"] = records
    return header


def batch_pair(arm, actual, run, deadline, member, kind, side, command):
    """Same native codec; this fresh owner authenticates its ACTUAL leader.

    The old batch checker fixes A1/B4 and must keep that contract unchanged.
    """
    for r in (arm, actual):
        require(type(r) is dict and set(r) == B.RECEIPT and type(r["Version"]) is int and r["Version"] == 1 and r["Profile"] == B.PROFILE, "native receipt grammar/profile")
        require(fixed(r["Run"], 16, True) == run and integer(r["Member"], member, member) and integer(r["PID"], minimum=1), "actual process owner/run")
        require(integer(r["Sequence"], 1, 1) and integer(r["DeadlineUnixNanos"], deadline, 1), "original deadline/sequence")
        c = r["Control"]
        require(type(c) is dict and set(c) == B.CONTROL and type(c["Version"]) is int and c["Version"] == 1 and c["Action"] == "arm" and c["Cookie"] == r["Cookie"] and type(c["Sequence"]) is int and c["Sequence"] == 1 and type(c["Kind"]) is int and c["Kind"] == kind and c["Side"] == side, "exact live native control")
        fixed(r["Cookie"], 16, True)
        require(fixed(c["CommandSHA256"], 32, True) == hashlib.sha256(command).digest(), "selected genuine source command")
        for key, limit in (("Attempts", 2048), ("Work", 1 << 40), ("Entries", 1 << 20), ("Output", 2 << 20)):
            integer(r[key], limit)
        require(type(r["Events"]) is list and len(r["Events"]) == 2 and r["Output"] >= 2*8192, "prepaid serialized native observations")
    require(all(arm[k] == actual[k] for k in ("Cookie", "Run", "Member", "PID", "Sequence", "Control", "DeadlineUnixNanos")), "native owner substitution")
    require(arm["State"] == "armed" and type(arm["Count"]) is int and arm["Count"] == 0 and arm["Attempts"] == arm["Work"] == arm["Entries"] == 0, "arm cannot claim a reached transition")
    for unused in arm["Events"]:
        B.zero_event(unused)
    require(actual["State"] == "observing" and type(actual["Count"]) is int and actual["Count"] == (1 if side == "before" else 2), "unmatched/ambiguous native selector")
    before = B.event(actual["Events"][0], actual["Control"])
    require(type(before["Stage"]) is int and before["Stage"] == 1 and before["Synced"] is False and before["CommandBytes"] == len(command), "actual before Sync event")
    require(actual["Attempts"] >= 1 and actual["Entries"] >= before["Work"]["Entries"] and actual["Work"] >= before["Work"]["Bytes"] and actual["Output"] >= arm["Output"] + before["Work"]["OutputBytes"], "native attempted hashing/serialization prepaid")
    if side == "after":
        require(B.event(actual["Events"][1], actual["Control"]) == {**before, "Stage": 2, "Synced": True}, "same actual batch guaranteed returned Sync")
    else:
        B.zero_event(actual["Events"][1])
    return before


def source_request(row, scopes, actor):
    require(type(row) is dict and set(row) == PHASE and row["Origin"] == actor and row["Partition"] in (3, 8), "actual submitted attempt grammar/origin")
    wire = P.raw(row["Request"])
    require(0 < len(wire) <= 128 << 10 and len(P.raw(row["Reply"])) <= 8192, "bounded source observation")
    command = C._source_command(row["Request"], scopes)
    decoded = C.decision_command(command)
    require(decoded["partition"] == row["Partition"], "actual submission partition")
    q = P.Cursor(P.body(wire, b"GSQ\1"))
    q.number(1)
    sequence, nonce = q.number(), q.take(16)
    return command, decoded, (row["Partition"], sequence, nonce)


def unknown_attempt(row, scopes, actor, coordinator, manifest, phase=3, partition=None):
    command, decoded, correlation = source_request(row, scopes, actor)
    partition = coordinator if partition is None else partition
    require(not P.raw(row["Reply"]) and decoded["phase"] == phase and decoded["partition"] == partition and decoded["manifest"] == manifest, "unanswered submission invented a reply or changed authority")
    return command, correlation


def unknown_invocation(response, transport):
    # This narrow format check is insufficient alone. check() first joins the
    # genuine unanswered source request, selected native batch, actual target
    # typed pipe loss and self-SIGKILL/reap. A generic error is not authority.
    require(type(transport) is str and 0 < len(transport) <= 4096 and (response["code"], response["mask"]) in ((3, 128), (6, 0)), "invalid/limit/corrupt/expiry cannot qualify unknown")


def recovery_order(successful, coordinator, commit, phase, partition):
    order = [(2, 3), (2, 8), (3, coordinator), (4, 3), (4, 8)]
    selected = order.index((phase, partition))
    require(successful[:selected] == order[:selected], "actual successful prefix before selected unknown")
    if phase == 2:
        tails = [order]
    elif phase == 3:
        tails = [order, order[3:]]
    elif commit:
        tails = [order[3:]]  # COMMIT retries both actual Resolve submissions.
    elif partition == 3:
        tails = [order[3:], order[4:]]  # ABORT skips only a stored terminal A.
    else:
        tails = [order[4:], []]  # A already returned; terminal B may be unknown.
    require(successful[selected:] in tails, "bounded genuine retry/terminal-skip order")
    return selected


def program_voters(value):
    # Private context from the full fresh program authenticator, never fields
    # silently adopted by an ordinary old artifact/check entry point.
    require(type(value) is dict and set(value)=={"initial", "selected", "after"}, "exact authenticated P06 role context")
    for pair in value.values():
        require(type(pair) is tuple and len(pair)==2 and type(pair[0]) is type(pair[1]) is int and pair[0] in (1,2) and pair[1] in (4,6), "actual elected P06 voters; no nominal permutation")
    return value


def capture(rows, unknown, scopes, actor, coordinator, manifest, tx, records, commit=True, phase=3, partition=None, *, program_roles=None):
    require(type(rows) is list and 4 <= len(rows) <= 10 and type(unknown) is list and len(unknown) == 1, "actual successes separate from one unknown")
    partition = coordinator if partition is None else partition
    order = [(2, 3), (2, 8), (3, coordinator), (4, 3), (4, 8)]
    selected_position = order.index((phase, partition))
    seen, successful = set(), []
    for n, row in enumerate(rows):
        command, decoded, correlation = source_request(row, scopes, actor)
        require(correlation not in seen, "duplicate correlated source observation")
        seen.add(correlation)
        voter = (2 if row["Partition"] == 3 else 4) if n < selected_position else (1 if partition == 3 else 2) if row["Partition"] == 3 else (4 if partition == 3 else 6)
        if program_roles is not None:
            roles=program_voters(program_roles)
            voter=roles["selected" if n<selected_position else "after"][0 if row["Partition"]==3 else 1]
        _, actual_manifest, status, outcome = P.peer_phase(P.raw(row["Request"]), P.raw(row["Reply"]), scopes, voter)
        require(actual_manifest == decoded["manifest"] == manifest and status["tx"] == tx, "immutable actual source original")
        require((status["partition"], status["index"]) in records and P.raw(records[status["partition"], status["index"]]["Outcome"]) == outcome, "successful reply not in actual retained originals")
        successful.append((decoded["phase"], row["Partition"]))
    recovery_order(successful, coordinator, commit, phase, partition)
    command, correlation = unknown_attempt(unknown[0], scopes, actor, coordinator, manifest, phase, partition)
    require(correlation not in seen, "unknown request duplicated a successful correlated reply")
    return command


def terminal_capture(rows, unknown, scopes, actor, coordinator, manifest, tx, records, phase, partition, prepare, *, program_roles=None):
    require(type(rows) is list and len(rows) == 5 and (unknown is None or type(unknown) is list and not unknown), "terminal owner needs five actual successes, no invented unknown")
    order = [(2, 3), (2, 8), (3, coordinator), (4, 3), (4, 8)]
    seen, selected = set(), prepare if phase == 2 else None
    for row, expected in zip(rows, order):
        command, decoded, correlation = source_request(row, scopes, actor)
        require(correlation not in seen and (decoded["phase"], decoded["partition"]) == expected, "actual initial recovery order/correlation")
        seen.add(correlation)
        voter=2 if row["Partition"] == 3 else 4
        if program_roles is not None:voter=program_voters(program_roles)["selected"][0 if row["Partition"]==3 else 1]
        _, actual_manifest, status, outcome = P.peer_phase(P.raw(row["Request"]), P.raw(row["Reply"]), scopes, voter)
        require(actual_manifest == decoded["manifest"] == manifest and status["tx"] == tx and P.raw(records[status["partition"], status["index"]]["Outcome"]) == outcome, "terminal source command lacks its genuine retained original")
        if expected == (phase, partition):
            require(selected is None or selected == command, "terminal Prepare differs from first YES command")
            selected = command
    require(selected is not None, "terminal command not captured")
    return selected


def terminal_install_original(record, original_wires, partition):
    # Late Prepare returns CURRENT Resolve. Requested phase and returned phase
    # are separate coordinates; retrying does not create another first original.
    require(record is not None and P.raw(record["Outcome"]) == original_wires[partition, 4] and not P.raw(record["CDC"]), "terminal install changed current Resolve original or emitted CDC")


def terminal_transport(row):
    require(type(row) is dict and set(row) == set("Input Reply Transport Accepted InputGrantBytes OutputGrantBytes BeforeImage RecoveredImage StepLoss".split()) and type(row["Accepted"]) is bool and type(row["Transport"]) is str and len(row["Transport"]) <= 4096, "terminal Propose observation grammar")
    require(type(row["InputGrantBytes"]) is int and row["InputGrantBytes"] == (64 << 10)+512 and type(row["OutputGrantBytes"]) is int and row["OutputGrantBytes"] == 8*(2 << 20), "separate prepaid actual IPC observations")
    wire = P.raw(row["Reply"])
    require(len(wire) <= 2 << 20, "actual terminal reply frame")
    if not row["Accepted"]:
        require(len(wire) == 381 and not any(wire) and bool(row["Transport"]) and row["StepLoss"] is None, "unknown Propose requires actual all-zero transport result, no Step ACK")
        return None
    require(not row["Transport"] and row["StepLoss"] is not None, "returned Propose and failed Step must remain distinct")
    response = F.process(wire)
    require(response["code"] == response["mask"] == 0 and not response["detail"] and response["packets"] and not response["outcome"] and not response["changes"] and not response["reads"], "native refused/error response cannot qualify Propose acceptance")
    return response


def terminal_packet(packet, scopes, partition, target, outbound):
    require(type(packet) is dict and set(packet) == {"From", "To", "Snapshot", "Payload"} and type(packet["From"]) is int and type(packet["To"]) is int and packet["Snapshot"] is False, "terminal actual ordinary packet grammar")
    members = (1, 2, 3) if partition == 3 else (4, 5, 6)
    require(packet["From"] in members and packet["To"] in members and packet["From"] != packet["To"] and packet["From" if outbound else "To"] == target, "actual terminal native transport route")
    payload = packet["Payload"] if type(packet["Payload"]) is bytes else P.raw(packet["Payload"])
    require(0 < len(payload) <= 2 << 20, "finite actual Step payload")
    message = F.raft_packet(payload, scopes, partition)
    require(message.get(2) == [packet["To"]] and message.get(3) == [packet["From"]], "native protobuf endpoints differ from transport")
    return payload, message


def terminal_submission(row, scopes, partition, target, selected, event):
    response = terminal_transport(row)
    wire = P.raw(row["Input"])
    require(0 < len(wire) <= row["InputGrantBytes"], "bounded native Propose input")
    c = P.Cursor(wire)
    require(P.body(c.field(156), b"APS\1") == scopes[partition] and c.field(64 << 10) == selected, "actual native Submit scope/command binding")
    c.end()
    before, after = P.raw(row["BeforeImage"]), P.raw(row["RecoveredImage"])
    require(before == after and len(before) == 172 and before[:3] == b"GR\3" and before[3] in (1, 2, 3) and before[4:20] == P.GRAPH and int.from_bytes(before[20:28], "big") == partition and hashlib.sha256(before[:-32]).digest() == before[-32:], "terminal retry must preserve actual native graph image")
    if response is None:
        return
    matches, recipients = 0, set()
    for packet in response["packets"]:
        _, message = terminal_packet(packet, scopes, partition, target, True)
        require(message.get(1) == [3] and message.get(4) == [event["Term"]], "returned Propose packets must be actual same-term append messages")
        for wire in message.get(7, []):
            require(type(wire) is bytes, "native entry protobuf wire")
            entry = F.protobuf_fields(wire)
            if entry.get(3) == [event["Index"]]:
                require(entry.get(2) == [event["Term"]] and entry.get(1, [0]) == [0] and entry.get(4) == [selected], "returned Propose packet changed selected native entry")
                matches += 1
                recipients.add(packet["To"])
    require(1 <= matches <= 2, "returned Propose did not carry selected entry to real peers")
    loss = row["StepLoss"]
    require(type(loss) is dict and set(loss) == {"Packet", "PayloadSHA256", "Transport", "WorkGrant"} and type(loss["Transport"]) is str and 0 < len(loss["Transport"]) <= 4096 and type(loss["WorkGrant"]) is int and loss["WorkGrant"] == 4*(2 << 20)+8192, "separately prepaid real failed Step observation")
    payload, message = terminal_packet(loss["Packet"], scopes, partition, target, False)
    require(loss["Packet"]["From"] in recipients and fixed(loss["PayloadSHA256"], 32, True) == hashlib.sha256(payload).digest() and message.get(1) == [4] and message.get(4) == [event["Term"]] and message.get(6) == [event["Index"]] and message.get(10, [0]) == [0], "failed Step must carry actual nonrejecting selected-entry append response")
    return response


def phase_census(data, tx, digest, coordinator, commit):
    records = F.prefix(data)
    originals, resolved = {}, set()
    local = [data["Refused"], data["Closed"]] + ([data["Changed"]] if data["Changed"] else [])
    require(data["Refused"]["Partition"] == data["Closed"]["Partition"] == 3 and (data["Changed"] is None) is commit and (data["Changed"] is None or data["Changed"]["Partition"] == 8), "exact local conflict/effect participants")
    known = {}
    for row in local:
        require(type(row) is dict and set(row) == LOCAL, "local real original grammar")
        require((row["Partition"], row["Index"]) not in known, "duplicate local original")
        proposal = P.pgq(P.raw(row["Proposal"]))
        outcome = P.body(P.raw(row["Outcome"]), b"GRO\4")
        require(len(outcome) == 84 and outcome[0] == proposal["kind"] and outcome[1:17] == P.GRAPH and int.from_bytes(outcome[17:25], "big") == row["Partition"] == proposal["partition"] and outcome[25:41] == proposal["request"] and outcome[41:73] == hashlib.sha256(P.raw(row["Proposal"])).digest() and int.from_bytes(outcome[73:81], "big") == row["Index"] and outcome[81] == 1, "local native proposal/outcome identity")
        require(records.get((row["Partition"], row["Index"])) == {k: row[k] for k in ("Partition", "Index", "Outcome", "CDC")}, "actual stored local original")
        is_refusal = row is data["Refused"]
        require((int.from_bytes(outcome[82:84], "big") == 11 and not P.raw(row["CDC"])) if is_refusal else (outcome[82:84] == bytes(2) and bool(P.raw(row["CDC"]))), "prepared conflict or actual useful local effect")
        known[row["Partition"], row["Index"]] = row
    for coordinate, row in records.items():
        outcome, cdc = P.raw(row["Outcome"]), P.raw(row["CDC"])
        if not outcome:
            require(not cdc, "empty outcome cannot carry CDC")
            continue  # Both actual output reads succeeded in the Go extractor.
        if outcome[:4] == b"GCS\1":
            s = P.status(outcome)
            require(s["tx"] == tx and s["digest"] == digest and s["partition"] == coordinate[0] and 0 < s["index"] <= coordinate[1], "foreign cross original")
            original = records.get((s["partition"], s["index"]))
            require(original is not None and P.raw(original["Outcome"]) == outcome, "replayed status differs from retained first original")
            require(not cdc or s["phase"] == 4 and s["commit"] and s["index"] == coordinate[1], "duplicate/nonbusiness CDC")
            if s["index"] == coordinate[1]:
                key = (s["partition"], s["phase"])
                require(key not in originals, "duplicate first original phase")
                originals[key] = s
                why = P.body(outcome, b"GCS\1")[98]
                require(why == (8 if key == (8, 2) and not commit else 0), "actual native Prepare refusal cause")
                if s["phase"] == 4:
                    require(s["commit"] == commit and bool(cdc) == commit, "whole actual Resolve outcome/CDC")
                    resolved.add(s["partition"])
            identity, original_hash, index = tx, digest, s["index"]
        elif outcome[:4] == b"GRO\4":
            require(coordinate in known, "unknown local native mutation")
            b = P.body(outcome, b"GRO\4")
            identity, original_hash, index = b[25:41], b[41:73], int.from_bytes(b[73:81], "big")
        else:
            b = P.body(outcome, b"GSO\1")
            require(len(b) == 18 and int.from_bytes(b[10:18], "big") == coordinate[1] and not cdc, "unknown/nonempty cut control")
            continue
        if cdc:
            c = P.Cursor(P.body(cdc, b"GCE\1"))
            c.number(1)
            require(c.take(16) == identity and c.take(32) == original_hash and c.number() == index, "complete CDC/original coordinate")
            logical = P.body(c.field(), b"GCD\5")
            c.end()
            require(logical[:16] == P.GRAPH and int.from_bytes(logical[16:24], "big") == coordinate[0], "actual CDC namespace")
    require(set(originals) == {(coordinator, 1), (3, 2), (8, 2), (coordinator, 3), (3, 4), (8, 4)} and resolved == {3, 8}, "complete six genuine phase originals")
    decision = originals[coordinator, 3]
    require(decision["commit"] == commit and originals[3, 2]["yes"] and originals[8, 2]["yes"] == commit, "actual prepared votes and immutable decision")
    require(all(originals[p, 4]["round"] == decision["round"] for p in (3, 8)), "both participant effects share real decision")
    return records, decision


def literal_and_serial(data, decision):
    before, recovered, current = (P.projected(data[key]) for key in ("Before", "Recovered", "Current"))
    require([r["id"] for r in data["Before"]["Native"]["entityCensus"] if r["found"]] == [1, 17], "exact initial native IDs")
    expected = dict(before)
    transactions, rounds, committed = {}, {}, set()
    if data["Commit"]:
        round_ = decision["round"]
        expected.update({"A/entity/3": dict(kind=2, source=1, target=17, mode=1, type="edge"), "A/presence/3": dict(present=True, null=False, handle=4, revision=round_, provenance=3, kind="life", i64=0), "A/labels/3": [], "A/property/3/p": dict(present=True, null=False, handle=5, revision=round_, provenance=3, kind="i64", i64=5), "A/life/3/4": dict(sourceLife=2, targetLife=18)})
        for key in ("A/posting/1/0", "A/posting/1/2", "B/posting/17/0", "B/posting/17/18"):
            expected[key] = [3]
        transactions["edge"] = {"reads": [P.read_key(before, k) for k in ("A/presence/1", "B/presence/17", "A/entity/3")], "writes": P.writes(before, recovered, ("A/entity/3", "A/presence/3", "A/labels/3", "A/property/3/p", "A/life/3/4", "A/posting/1/0", "A/posting/1/2", "B/posting/17/0", "B/posting/17/18"))}
        rounds["edge"], committed = decision["round"], {"edge"}
    else:
        require(data["Changed"] is not None, "ABORT is not a timeout or fabricated NO")
        cell = recovered["B/presence/17"]
        expected["B/presence/17"] = dict(present=False, null=False, handle=0, revision=cell["revision"], provenance=41, kind="absent", i64=0)
        transactions["close-b"] = {"reads": [P.read_key(before, "B/presence/17")], "writes": P.writes(before, recovered, ("B/presence/17",))}
        rounds["close-b"], committed = integer(cell["revision"], minimum=1), {"close-b"}
    T.semantic(recovered == expected, "p06-useful-original-answer")
    cell = current["A/presence/1"]
    expected["A/presence/1"] = dict(present=False, null=False, handle=0, revision=cell["revision"], provenance=41, kind="absent", i64=0)
    T.semantic(current == expected, "p06-final-endpoint-answer")
    for key in ("Recovered", "Current"):
        native = data[key]["Native"]
        T.semantic([r["id"] for r in native["entityCensus"] if r["found"]] == ([1, 3, 17] if data["Commit"] else [1, 17]), "p06-exact-entity-census")
        entities = {row["id"]: row for row in native["entities"]}
        T.semantic(entities[1]["effective"] is (key != "Current") and entities[17]["effective"] is data["Commit"] and entities[3]["found"] is data["Commit"] and entities[3]["effective"] is (data["Commit"] and key != "Current"), "p06-endpoint-life-effective-answer")
        for row in native["postings"]:
            declared = [3] if data["Commit"] and (row["endpoint"], row["life"]) != (1, 7) else []
            T.semantic((row["candidates"] or []) == declared and (row["declared"] or []) == declared and (row["effective"] or []) == (declared if key != "Current" else []), "p06-exact-posting-answer")
    transactions["close-a"] = {"reads": [P.read_key(recovered, "A/presence/1")], "writes": P.writes(recovered, current, ("A/presence/1",))}
    rounds["close-a"] = integer(cell["revision"], minimum=1)
    committed.add("close-a")
    history = {"name": "P06-native-Decide-sync", "initial": before, "transactions": transactions}
    observations = [{"event": key, "scope": ["A", "B"], "round": P.cut(data[key])[1], "values": P.projected(data[key])} for key in ("Before", "Recovered", "Current", "Retained")]
    witness, detail = P.oracle()(history, committed, rounds, observations, [{"event": "final", "scope": ["A", "B"], "values": current}])
    T.semantic(witness is not None, "independent exact serial/cut witness: " + str(detail))
    return witness


def first_processes(rows, voter=1):
    require(type(rows) is list and len(rows) == 2, "first YES actual process pair")
    killed, reopened = rows
    raw, restored = T.decoded(P.raw(killed["Raw"])), T.decoded(P.raw(reopened["Raw"]))
    require(killed["Action"] == "kill" and killed["Member"] == raw["Member"] == voter and killed["Reaped"] is True and killed["Signal"] == "SIGKILL" and raw["SignalNumber"] == 9 and raw["Signaled"] is True and raw["Reaped"] is True and raw["Success"] is False and raw["Instance"] == killed["Instance"] and raw["Volume"] == killed["Volume"], "actual prepared leader death/reap")
    require(reopened["Action"] == "reopen" and reopened["Member"] == restored["Member"] == voter and reopened["Ready"] is True and reopened["PreviousInstance"] == restored["PreviousInstance"] == killed["Instance"] and reopened["Volume"] == restored["Volume"] == killed["Volume"] and reopened["RecoveryRaw"] == restored["RecoveryRaw"] and raw["PID"] != restored["PID"], "first same-directory restored quorum")
    return F.process(P.raw(restored["LocallyRestoredRaw"]))


def check(a, *, program_roles=None):
    require(type(a) is dict and type(a.get("Version")) is int and a["Version"] in (1, 2, 3), "implemented conjunction version")
    terminal = a["Version"] == 3
    require(set(a) == (TERMINAL_HEADER if terminal else HEADER) and a["Profile"] == {1: PROFILE, 2: RECOVERY_PROFILE, 3: TERMINAL_PROFILE}[a["Version"]] and a["Case"] == recipe(a["Recipe"], a["Version"]), "fresh implemented conjunction grammar")
    d, r = a["P06"], a["Recipe"]
    require(type(d) is dict and set(d) == P06 and type(d["Version"]) is int and d["Version"] == 1 and d["Coordinator"] == r["Coordinator"] and d["Commit"] is r["Commit"], "same real semantic P06 recipe")
    coordinator, commit, kind, side = r["Coordinator"], r["Commit"], r["Kind"], r["Side"]
    phase, partition = (3, coordinator) if a["Version"] == 1 else (r["Phase"], r["Partition"])
    actor = 3 if coordinator == 3 else 5
    target, successor = (2, 1) if partition == 3 else (4, 6)
    first_voter=1
    if program_roles is not None:
        roles=program_voters(program_roles);first_voter=roles["initial"][0]
        target,successor=roles["selected"][0 if partition==3 else 1],roles["after"][0 if partition==3 else 1]
    require((a["Actor"], a["FirstVoter"], a["Target"], a["Successor"]) == (actor, first_voter, target, successor), "actual finite process routing")
    scopes, _ = P.cut(d["Before"])
    first, manifest, yes, yes_wire = P.peer_phase(P.raw(d["FirstYES"]["Request"]), P.raw(d["FirstYES"]["Reply"]), scopes, first_voter)
    require(first == P.raw(d["Manifest"]) and yes["yes"] and yes["partition"] == 3, "genuine first YES source")
    original = C.decision_manifest(P.raw(d["Original"]), manifest, coordinator, scopes, d["Before"])
    tx, digest = original["tx"], original["digest"]
    require(yes["tx"] == tx, "same original first YES")
    records, decision = phase_census(d, tx, digest, coordinator, commit)
    require(P.raw(records[3, yes["index"]]["Outcome"]) == yes_wire, "first YES native retained original")
    decision_wire = P.raw(records[coordinator, decision["index"]]["Outcome"])
    P.resumed(d["Pending"], scopes, original["source"]["request"], tx, P.business_hash(original["wire"], coordinator), 1, b"")
    P.resumed(d["Decision"], scopes, original["source"]["request"], tx, P.business_hash(original["wire"], coordinator), 0, decision_wire)
    if terminal:
        selected = terminal_capture(d["Phases"], a["UnknownPhases"], scopes, actor, coordinator, manifest, tx, records, phase, partition, first, program_roles=program_roles)
    else:
        selected = capture(d["Phases"], a["UnknownPhases"], scopes, actor, coordinator, manifest, tx, records, commit, phase, partition, program_roles=program_roles)
    require(selected == P.raw(a["SelectedCommand"]), "actual captured command differs from native selected command")
    decoded = C.decision_command(selected)
    require(decoded["phase"] == phase and decoded["partition"] == partition and decoded["manifest"] == manifest, "actual selected recovery original")
    original_wires = {(part, P.status(P.raw(row["Outcome"]))["phase"]): P.raw(row["Outcome"]) for (part, index), row in records.items() if P.raw(row["Outcome"])[:4] == b"GCS\1" and P.status(P.raw(row["Outcome"]))["index"] == index}
    proof_keys = [(3, 2), (8, 2)] if phase == 3 else [(coordinator, 3)] if phase == 4 else [(coordinator, 1)]+([(3, 2)] if partition == 8 else [])
    proof_wires = [original_wires[key] for key in proof_keys]
    require(decoded["proofs"] == [P.status(wire) for wire in proof_wires] and decoded["proof_wires"] == proof_wires and decoded["conflict"] is None, "selected native submission bound to actual original authority records")
    run, deadline = fixed(a["Run"], 16, True), integer(a["DeadlineUnixNanos"], (1 << 63)-1, 1)
    event = batch_pair(a["Arm"], a["Batch"], run, deadline, target, kind, side, selected)
    B.processes(a["Processes"], None, a["Case"], a["Batch"])
    first_local = first_processes(d["Processes"], first_voter)
    process_order = [d["Processes"][0], d["Processes"][1], a["Processes"][0], a["Processes"][1]]
    events = [integer(row["EventID"], minimum=1) for row in process_order]
    require(events == sorted(set(events)), "first quorum restored before second actual crash/reopen")
    require(first_local["applied"] >= yes["index"], "first replacement did not read its own durable prepared prefix")
    if kind == 2:
        row = records.get((partition, event["Index"]))
        if terminal:
            terminal_install_original(row, original_wires, partition)
        else:
            require(row is not None and P.raw(row["Outcome"]) == original_wires[partition, phase], "install event not bound to actual retained phase original")
    if terminal:
        submitted = terminal_submission(a["Duplicate"], scopes, partition, target, selected, event)
        if submitted is not None:
            require(a["Processes"][0]["Instance"] == f'voter{target}/pid{a["Batch"]["PID"]}/{submitted["incarnation"].hex()}', "returned Propose belongs to actual killed target incarnation")
    require(type(a["TargetTransportLoss"]) is str and 0 < len(a["TargetTransportLoss"]) <= 4096, "missing typed target loss observation")
    attempts = a["Attempts"]
    require(type(attempts) is list and len(attempts) == 3, "bounded actual unknown/final/replay calls")
    pending_response = F.process(P.raw(d["Pending"]))
    previous_sequence = pending_response["sequence"]
    for i, row in enumerate(attempts):
        unknown_call = not terminal and i == 0
        require(type(row) is dict and set(row) == {"Reply", "Transport", "Pending"} and type(row["Pending"]) is bool and row["Pending"] is unknown_call and type(row["Transport"]) is str and len(row["Transport"]) <= 4096, "actual invocation grammar")
        response = F.process(P.raw(row["Reply"]))
        require(response["incarnation"] == pending_response["incarnation"] and response["sequence"] > previous_sequence, "original caller changed process or replayed IPC sequence")
        previous_sequence = response["sequence"]
        require(not response["outcome"] and not response["changes"] and not response["packets"] and not response["reads"], "coordinator call invented native event outputs")
        if unknown_call:
            unknown_invocation(response, row["Transport"])
        else:
            require(not row["Transport"] and response["code"] == response["mask"] == 0, "success/replay was not genuine success")
        P.resumed(row["Reply"], scopes, original["source"]["request"], tx, P.business_hash(original["wire"], coordinator), int(unknown_call), b"" if unknown_call else decision_wire)
    require(attempts[0 if terminal else 1]["Reply"] == d["Decision"], "final returned original not actual captured response")
    require(type(d["Duplicates"]) is list and len(d["Duplicates"]) == 3, "actual terminal Prepare/two Resolve replays")
    for i, row in enumerate(d["Duplicates"]):
        require(type(row) is dict and set(row) == LOCAL, "terminal duplicate grammar")
        command = C.decision_command(P.raw(row["Proposal"]))
        status = P.status(P.raw(row["Outcome"]))
        require(command["phase"] == (2 if i == 0 else 4) and command["partition"] == (3 if i < 2 else 8) and command["manifest"] == manifest and status["phase"] == 4 and status["tx"] == tx and status["digest"] == digest and status["index"] < row["Index"] and not P.raw(row["CDC"]), "current terminal replay semantics/original/zero CDC")
        require(records.get((row["Partition"], row["Index"])) == {k: row[k] for k in ("Partition", "Index", "Outcome", "CDC")}, "terminal replay missing from actual prefix")
    for actual in (d["Retained"], a["FirstReopened"], a["FaultReopened"]):
        T.semantic(actual == d["Before"], "old-cut:exact-original-certificate-data-index")
    reads = integer(a["ReceiptReads"], 129, 1)
    require(a["ReceiptGrantBytes"] == reads*65536, "receipt read grants/prepay")
    require(integer(a["GateCandidates"], 128, 1)*(4096+32*(2 << 20)) == integer(a["GateDecodeWork"], 128*(4096+32*(2 << 20)), 1), "whole envelope gate decode prepay")
    candidates, count = integer(a["CaptureCandidates"], 256, 1), integer(a["CaptureAttempts"], 32, 1)
    unknown = a["UnknownPhases"] or []  # Only terminal_capture admits nil; old capture requires its list.
    require(candidates >= count == len(d["Phases"])+len(unknown) and a["CaptureWork"] == candidates*(4096+32*(2 << 20)) and a["CaptureOutput"] == count*((128 << 10)+8192), "attempted capture/output grant prepay")
    actual_bytes = sum(len(P.raw(row["Request"]))+len(P.raw(row["Reply"])) for row in d["Phases"]+unknown)
    require(integer(a["CaptureRetained"], 256 << 10) == actual_bytes, "finite owned actual captures")
    witness = literal_and_serial(d, decision)
    return dict(case=a["Case"], records=len(records), successful_replies=len(d["Phases"]), unknown_attempts=len(unknown), serial_witness=witness, scope="P06 recovery/terminal replay Ready/install before/after only; initial occurrences, rotated leaders/messages, other P rows/state compositions and runtime qualification remain open")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+", type=Path)
    args = parser.parse_args()
    require(len(args.artifacts) <= 128, "finite conjunction invocation")
    counts, seen, results = {1: 0, 2: 0, 3: 0}, set(), []
    for path in args.artifacts:
        artifact = load(path)  # One bounded artifact at a time, never128 prefixes.
        result = check(artifact)
        version = artifact["Version"]
        counts[version] += 1
        require(counts[version] <= {1: 16, 2: 64, 3: 48}[version] and artifact["Case"] not in seen, "finite unique versioned conjunction inventory")
        seen.add(artifact["Case"])
        results.append(result)  # Only bounded two/three-transaction summaries.
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
