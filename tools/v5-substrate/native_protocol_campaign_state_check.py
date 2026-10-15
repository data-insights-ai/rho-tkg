#!/usr/bin/env python3
"""Finite P07-P09 exact native Sync, original ownership and complete feed joins.

Native observations remain diagnostic evidence, never a quorum/cursor/pin. The
bridge shares canonical binary readers only; its acceptance joins original
bytes, literal typed C, real process/volume lineage and full Page/ACK vectors.
"""
import argparse
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import struct


def module(name, file):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(file))
    result = importlib.util.module_from_spec(spec); spec.loader.exec_module(result)
    return result


B = module("protocol_state_batch", "native_transition_batch_check.py")
S = module("protocol_state_native_state", "native_transition_state_check.py")
R = module("protocol_state_retention", "native_transition_retention_check.py")
M = module("protocol_state_move", "native_transition_move_check.py")
G = module("protocol_state_feed", "native_feed_check.py")
T, P, F = B.T, B.P, S.F
require, integer, fixed = T.require, T.integer, T.fixed
PROFILE = "native-protocol-owned-state-transition-v1"
SEMANTIC = hashlib.sha256(hashlib.sha256(G.AGREEMENT + b"feed-retention=irreversible-rolling-ack-anchor-v1\0snapshot-reopen=authenticated-expired-v1\0payload-anchor=exact-plm-round-image-v1\0").digest() + b"range-move=compact-recovery-spans-v1\0retention=immutable-native-reference-v1\0replay=bounded-original-batches32-v1\0").digest()
HEADER = set("Version Profile Recipe Run DeadlineUnixNanos Arm Batch StateArm State Command Intent Calls GC BeforeGC AfterGC Before Retained Reopened Processes Original Token Business BusinessReplay BusinessAfterGC LostRequest LostReply From Through Records ReceiptReads ReceiptGrantBytes Refusals ChangedCaller ChangedReply AllocatorBefore AllocatorAfter".split())
CONTROL = set("Version Cookie Sequence Action Request Original Token Boundary Digest".split())
REPLY = set("Version Profile Cookie Run Sequence Calls TerminalCalls GrantedWork Member PID DeadlineUnixNanos Action Result Original Token Page Digest Snapshot Completed".split())


def recipe(r):
    require(type(r) is dict and set(r) == {"Family", "Phase", "Partition", "Page", "Kind", "Side"}, "exact actual state selector")
    phase, part, page, kind = (integer(r[k], 255 if k == "Phase" else 8 if k == "Partition" else 1024 if k == "Page" else 2) for k in ("Phase", "Partition", "Page", "Kind"))
    family, side = r["Family"], r["Side"]
    if family == "gc":
        require((phase, part, page, kind) == (0, 3, 0, 0) and side in ("before", "after"), "actual collector Sync selector")
    elif family == "publication-race":
        require((phase, part, page, kind, side) == (0, 3, 0, 0, "after-gc"), "exact captured-before-publication race")
    else:
        require(kind in (1, 2) and side in ("before", "after"), "actual Ready/application side")
        if family == "move":
            M.recipe(dict(Version=1, Phase=phase, Partition=part, Page=page, Side="before-submit"))
        elif family == "pin":
            require(1 <= phase <= 8 and part in (3, 8) and (phase != 7 or part == 3) and page == 0, "actual native pin participant")
        else:
            require(family == "allocation" and phase in (15, 16) and part == 3 and page == 0, "actual reserve/install")
    return family, phase, part, page, kind, side


def load(path):
    total, digest, header, footer = 0, hashlib.sha256(), None, None
    rows, calls, gc = [], [], []
    with path.open("rb") as stream:
        while line := stream.readline((512 << 10)+1):
            total += len(line)
            require(total <= 16 << 20 and len(line) <= 512 << 10 and line.endswith(b"\n"), "bounded state artifact")
            row = T.decoded(line)
            require(type(row) is dict and footer is None, "state row after EOF")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and row["Type"] == "header", "state header")
                header = row["Artifact"]
                require(type(header) is dict and set(header) == HEADER and all(header[k] is None for k in ("Records", "Calls", "GC")), "owned streamed proof grammar")
            elif row.get("Type") == "record":
                require(set(row) == {"Type", "Record"} and len(rows) < 512, "bounded native prefix"); rows.append(row["Record"])
            elif row.get("Type") in ("call", "gc"):
                kind = row["Type"]; target = calls if kind == "call" else gc; field = "Call" if kind == "call" else "Receipt"
                require(set(row) == {"Type", "Sequence", field} and type(row["Sequence"]) is int and row["Sequence"] == len(target) < (416 if kind == "call" else 2304), "exact bounded owner row sequence")
                target.append(row[field])
            else:
                require(set(row) == {"Type", "Rows", "Calls", "GC", "Through", "PayloadSHA256"} and row["Type"] == "footer" and all(type(row[k]) is int and row[k] == len(v) for k, v in (("Rows", rows), ("Calls", calls), ("GC", gc))) and row["PayloadSHA256"] == digest.hexdigest(), "exact state EOF/hash/census")
                footer = row
            if footer is None: digest.update(line)
    require(header is not None and footer is not None and footer["Through"] == header["Through"], "missing/foreign complete EOF")
    header.update(Records=rows, Calls=calls, GC=gc)
    return header


def cut(read):
    scopes, round_, values = F.range_read(read)
    require(set(scopes) == {3, 8} and all(scope[88:120] == SEMANTIC for scope in scopes.values()), "exact existing span semantic/full scope")
    M.census(read)
    return scopes, round_, values


def definition(encoded, deadline):
    wire = P.raw(encoded); c = P.Cursor(P.body(wire, b"GFD\1")); manifest = c.field(32768)
    policy = (c.number(4), c.number(4), c.number(), c.number(), c.number(4), c.number(4), c.number(4), c.number()); c.end()
    require(policy == (65536, 256, 64, 512, 16384, 16 << 20, 32 << 20, deadline), "same fixed original feed work/deadline")
    c = P.Cursor(P.body(manifest, b"GPM\1")); scope, issuer, request = c.take(120), c.take(16), c.take(16)
    epoch, expires, count = c.number(), c.number(), c.number(4)
    require(scope[88:] == SEMANTIC and int.from_bytes(scope[16:24], "big") in (3, 8) and any(issuer) and any(request) and epoch == 0 and expires == 1 and count == 2, "genuine complete original manifest/issuer")
    parts = {}
    for part in (3, 8):
        cert, start, lineage = c.field(512), c.number(), c.take(32)
        cert_body = P.body(cert, b"GSC\1")
        require(len(cert_body) == 356 and cert_body[88:120] == SEMANTIC and int.from_bytes(cert_body[16:24], "big") == part and start == int.from_bytes(cert_body[176:184], "big")+1 and lineage == hashlib.sha256(b"rho-tkg:retained-pin-source-lineage:v1\0"+cert+struct.pack(">Q", start)).digest(), "actual native certificate/data/feed lineage")
        parts[part] = dict(scope=cert_body[:120], certificate=cert, source=int.from_bytes(cert_body[176:184], "big"), index=int.from_bytes(cert_body[160:168], "big"), start=start)
    c.end()
    return dict(wire=wire, manifest=manifest, scope=scope, issuer=issuer, request=request, id=hashlib.sha256(b"rho-tkg:retained-graph-pin:v1\0"+scope+issuer+request).digest(), digest=hashlib.sha256(manifest).digest(), parts=parts)


def original_cell(wire, axis, kind, value=None):
    """One actual ScopeAll SC cell, not a guessed logical-round revision."""
    c = P.Cursor(bytes.fromhex(wire))
    require(c.take(4) == b"SC"+bytes((1, 2)), "actual retained rational component codec")
    axis_id, definition_, count = c.take(16), c.take(32), c.number(4)
    require(definition_ == bytes.fromhex(axis) and any(axis_id) and count == 1, "exact native single ScopeAll cell")
    scope = c.field(53)
    require(scope == b"TS"+bytes((1, 2))+axis_id+definition_+bytes((6,)), "actual unbounded ScopeAll, no hidden interval")
    present, value_kind = c.number(1), c.number(1)
    handle, size, revision, provenance = (c.number() for _ in range(4)); c.end()
    require(present == 1 and value_kind == 2 and handle > 0 and size == (0 if kind == "life" else 9) and revision > 0 and provenance > 0, "actual present native reference/revision")
    return dict(present=True, null=False, handle=handle, revision=revision, provenance=provenance, kind=kind, i64=0 if value is None else value)


def snapshot(value, d, before):
    require(type(value) is dict and set(value) == {"Cut", "Facts", "Entities"}, "actual held snapshot projection grammar")
    require(type(value["Cut"]) is dict and set(value["Cut"]) == {"Certificates"}
            and [P.raw(x) for x in value["Cut"]["Certificates"]] == [d["parts"][p]["certificate"] for p in (3, 8)], "held C is actual Original capture, not an independent Fresh")
    _, _, native = cut(before)
    facts = value["Facts"]
    require(type(facts) is list and len(facts) == 32, "held snapshot full actual entity census")
    for owner, fact in enumerate(facts, 1):
        require(type(fact) is dict and set(fact) == {"Exists", "Active", "Life", "Source", "Target", "Labels", "Incident"}, "held fact schema")
        found = owner in (1, 3, 17)
        expected = dict(Exists=found, Active=found, Life=owner+1 if found else 0,
                        Source=1 if owner == 3 else 0, Target=17 if owner == 3 else 0,
                        Labels=[], Incident=[3] if owner in (1, 17) else [])
        require(type(fact["Exists"]) is bool and type(fact["Active"]) is bool
                and all(type(fact[k]) is int for k in ("Life", "Source", "Target")) and all(fact[k] is None or type(fact[k]) is list for k in ("Labels", "Incident"))
                and {**fact, "Labels": fact["Labels"] or [], "Incident": fact["Incident"] or []} == expected, "literal held old labels/lives/endpoints/postings, no K/current substitution")
    entities = value["Entities"]
    require(type(entities) is list and len(entities) == 4, "complete held typed entity set")
    zero = dict(present=False, null=False, handle=0, revision=0, provenance=0, kind="", i64=0)
    for entity, owner in zip(entities, (1, 3, 7, 17)):
        require(type(entity) is dict and set(entity) == set("id found kind source target mode type presence effective effectiveLife labels property".split()), "held typed entity grammar")
        found = owner != 7
        expected = dict(id=owner, found=found, kind=2 if owner == 3 else 1 if found else 0,
                        source=1 if owner == 3 else 0, target=17 if owner == 3 else 0,
                        mode=1 if owner == 3 else 0, type="cross-edge" if owner == 3 else "",
                        effective=found, effectiveLife=owner+1 if found else 0, labels=[], presence=zero, property=zero)
        if found:
            row = native[f"{'A' if owner < 17 else 'B'}/fact/{owner}"]
            expected["presence"] = original_cell(row["presence"], row["axis"], "life")
            require(expected["presence"]["handle"] == owner+1, "actual native same-life presence")
            if owner != 3:
                expected["property"] = original_cell(row["property"], row["axis"], "i64", row["value"])
                require(expected["property"]["handle"] == row["valueID"]
                        and expected["property"]["revision"] == row["revision"]
                        and expected["property"]["provenance"] == row["provenance"], "actual held dictionary/reference/original revision join")
        for field in ("presence", "property"):
            cell = entity[field]
            require(type(cell) is dict and set(cell) == set(zero) and type(cell["present"]) is bool and type(cell["null"]) is bool
                    and all(type(cell[k]) is int for k in ("handle", "revision", "provenance", "i64")) and type(cell["kind"]) is str, "strict held cell types, no bool/integer alias")
        require(type(entity["found"]) is bool and type(entity["effective"]) is bool
                and all(type(entity[k]) is int for k in ("id", "kind", "source", "target", "mode", "effectiveLife")) and (entity["labels"] is None or type(entity["labels"]) is list)
                and {**entity, "labels": entity["labels"] or []} == expected, "complete held old typed values/revisions/provenance/presence")


def selected(command, r, scopes):
    family, phase, part, page, _, _ = recipe(r)
    if family == "move":
        c = P.Cursor(P.body(command, b"GMQ\2")); scope, actual, sequence = c.take(120), c.number(1), c.number()
        intent, payload = c.field(1024), c.field((1 << 20)-2048)
        for _ in range(integer(c.number(4), 2)): move_status(c.field(256))
        c.end(); require(scope == scopes[part] and actual == phase and sequence == page, "canonical actual GMR native phase/page")
        P.body(intent, b"GMI\2")
        return hashlib.sha256(intent).digest()
    if family == "pin":
        c = P.Cursor(P.body(command, b"GPL\1")); scope, actual, issuer, action = c.take(120), c.number(1), c.take(16), c.take(16)
        id_, digest = c.take(32), c.take(32); epoch, expected, start = c.number(), c.number(), c.number(); manifest = c.field(32768); c.end()
        require(scope == scopes[part] and actual == phase and any(issuer) and any(action), "canonical actual native pin selector")
        if phase in (1, 7, 8): require(any(manifest) and hashlib.sha256(manifest).digest() == digest and any(id_), "actual complete manifest/cancel binding")
        elif phase == 6: require(not any(id_+digest+manifest) and epoch == expected+1 and start == 0, "actual native epoch transition")
        else: require(any(id_) and any(digest) and not manifest and expected == 0 and (start > 0) == (phase == 2), "actual owner/terminal binding")
        return id_
    c = P.Cursor(P.body(command, b"AQP\1")); actual, graph, part_, request = c.number(1), c.take(16), c.number(), c.take(16)
    require(actual == phase and part_ == part == 3 and graph == P.GRAPH and any(request), "actual native reserve/install original")
    require(len(c.take(len(c.wire))) > 32, "complete nonempty actual allocation command")
    return request


def move_status(wire):
    c = P.Cursor(P.body(wire, b"GMO\2")); graph, part, move, intent = c.take(16), c.number(), c.take(16), c.take(32)
    phase, result = c.number(1), c.number(1)
    index, floor, round_, sequence, rows, size = (c.number() for _ in range(6)); digest = c.take(32); c.end()
    require(graph == P.GRAPH and part in (3, 8) and any(move) and any(intent) and 1 <= phase <= 9 and 1 <= result <= 4 and index > 0, "actual immutable GMO2")
    return dict(partition=part, move=move, intent=intent, phase=phase, result=result, index=index, floor=floor, round=round_, sequence=sequence, rows=rows, size=size, digest=digest)


def batch(a, scopes):
    r = a["Recipe"]; family, phase, part, _, kind, side = recipe(r)
    command = P.raw(a["Command"]); require(0 < len(command) <= 64 << 10, "actual owned encoded command")
    selected(command, r, scopes)
    arm, receipt = a["Arm"], a["Batch"]
    member = 3 if part == 3 else 6
    for value in (arm, receipt):
        require(type(value) is dict and set(value) == B.RECEIPT and value["Profile"] == B.PROFILE and type(value["Version"]) is int and value["Version"] == 1, "actual native batch owner")
        require(value["Member"] == member and fixed(value["Run"], 16, True) == fixed(a["Run"], 16, True) and value["DeadlineUnixNanos"] == a["DeadlineUnixNanos"], "same native member/run/deadline")
        control = value["Control"]
        require(set(control) == B.CONTROL and control["Action"] == "arm" and control["Version"] == control["Sequence"] == value["Sequence"] == 1 and control["Kind"] == kind and control["Side"] == side and control["Cookie"] == value["Cookie"] and fixed(control["CommandSHA256"], 32, True) == hashlib.sha256(command).digest(), "actual native exact command selector")
        integer(value["Attempts"], 2048); integer(value["Work"], 1 << 42); integer(value["Entries"], 1 << 20); integer(value["Output"], 2 << 20)
    for k in ("Cookie", "Run", "Member", "PID", "Sequence", "Control", "DeadlineUnixNanos"): require(arm[k] == receipt[k], "changed exact native owner")
    require(arm["State"] == "armed" and arm["Count"] == 0 and receipt["State"] == "observing" and receipt["Count"] == (1 if side == "before" else 2), "actual native boundary cardinality")
    for e in arm["Events"]: B.zero_event(e)
    e = B.event(receipt["Events"][0], receipt["Control"])
    require(e["Stage"] == 1 and not e["Synced"] and e["CommandBytes"] == len(command), "actual complete native batch")
    require(receipt["Work"] >= e["Work"]["Bytes"] and receipt["Entries"] >= e["Work"]["Entries"] and receipt["Output"] >= arm["Output"]+e["Work"]["OutputBytes"], "native hashing/copy work retired")
    if side == "after": require(B.event(receipt["Events"][1], receipt["Control"]) == {**e, "Stage": 2, "Synced": True}, "exact same Sync completed")
    else: B.zero_event(receipt["Events"][1])
    integer(a["ReceiptReads"], 129, 1)
    return e


def processes(a, receipt):
    rows = a["Processes"]; require(type(rows) is list and 2 <= len(rows) <= 13, "finite actual process inventory")
    failed = rows[0]; actual = T.decoded(P.raw(failed["Raw"]))
    require(actual["Receipt"] == receipt and actual["PID"] == receipt["PID"] and actual["Member"] == failed["Member"] == receipt["Member"] and actual["ExitCode"] == -1 and actual["SignalNumber"] == 9 and actual["Signaled"] is True and actual["Reaped"] is True and failed["Reaped"] is True and failed["Signal"] == "SIGKILL", "actual receipt-selected self-kill/reap")
    matches = [r for r in rows[1:] if r["Member"] == failed["Member"] and r["Ready"] is True]
    require(len(matches) == 1, "actual one same-volume replacement")
    restored = matches[0]
    require(restored["PreviousInstance"] == failed["Instance"] and restored["Volume"] == failed["Volume"] and restored["Instance"] != failed["Instance"], "native same durable image/process lineage")
    R.response(restored["RecoveryRaw"])


def prefix(a):
    rows = {}; by_part = {3: [], 8: []}
    for row in a["Records"]:
        require(type(row) is dict and set(row) == {"Partition", "Index", "Outcome", "CDC"}, "actual native record grammar")
        part, index = integer(row["Partition"], 8, 3), integer(row["Index"], minimum=1)
        require(part in by_part and (part, index) not in rows and row["Outcome"] is not None and row["CDC"] is not None, "present-empty distinct from missing")
        rows[part, index] = row; by_part[part].append(index)
    for i, part in enumerate((3, 8)):
        lo, hi = integer(a["From"][i]), integer(a["Through"][i])
        require(lo <= hi <= lo+256 and by_part[part] == list(range(lo+1, hi+1)), "complete native source prefix/retained diagnostic EOF")
    return rows


def check_calls(a, d, rows):
    owners, pages, acks, chain, business_pages = {}, {}, set(), [], {}
    for item in a["Calls"]:
        require(type(item) is dict and set(item) == {"Member", "Input", "Response"} and item["Member"] in (2, 5), "actual owner pipe")
        q = item["Input"]; require(type(q) is dict and set(q) == CONTROL and type(q["Version"]) is int and q["Version"] == 1, "canonical private owner control")
        response = R.response(item["Response"]); reply = T.decoded(response["aux"])
        require(type(reply) is dict and set(reply) == REPLY and reply["Profile"] == PROFILE and type(reply["Version"]) is int and reply["Version"] == 1 and reply["Member"] == item["Member"] and fixed(reply["Run"], 16, True) == fixed(a["Run"], 16, True) and reply["DeadlineUnixNanos"] == a["DeadlineUnixNanos"] and reply["Action"] == q["Action"] and reply["Sequence"] == q["Sequence"], "actual original owner/operation/deadline")
        key = item["Member"], reply["PID"]; last = owners.get(key, (0, 0, 0, 0))
        sequence, calls, terminals, work = (integer(reply[k], 416 if k == "Sequence" else 384 if k == "Calls" else 32 if k == "TerminalCalls" else 1 << 42) for k in ("Sequence", "Calls", "TerminalCalls", "GrantedWork"))
        require(calls >= last[1] and terminals >= last[2] and work >= last[3] and work == (calls+terminals)*(4*(64 << 20)+2*(2 << 20)), "same owner finite no-refund grants")
        if q["Action"] != "inspect": require(sequence == last[0]+1 and q["Cookie"] == reply["Cookie"], "actual owner sequence/cookie")
        owners[key] = sequence, calls, terminals, work
        if response["code"] != 0:
            require(reply["Result"] in ("pending-or-unavailable", "invalid", "limit", "expired", "released") and reply["Snapshot"] is None, "truthful refusal no fake snapshot")
            continue
        require(reply["Result"] == "applied", "success relabelled refusal")
        if q["Action"] == "snapshot": snapshot(reply["Snapshot"], d, a["Before"])
        if q["Action"] == "page":
            page = G.page(P.raw(reply["Page"])); request = fixed(q["Request"], 16, True)
            require(page["request"] == request and page["pin"] == d["id"] and hashlib.sha256(P.raw(reply["Page"])).digest() == fixed(reply["Digest"], 32, True), "actual complete page original/digest")
            before = pages.get(request)
            require(before is None or before == P.raw(reply["Page"]), "SAME Page changed immutable bytes")
            if before is None:
                require(page["sequence"] == len(chain)+1 and page["from"] == (G.page(chain[-1])["next"] if chain else [d["parts"][p]["start"] for p in (3, 8)])
                        and page["previous"] == (hashlib.sha256(chain[-1]).digest() if chain else bytes(32)), "full-vector gap-free page/ACK chain")
                require(not chain or G.page(chain[-1])["request"] in acks, "next page before complete prior ACK")
                require(page["boundary"] == G.codec.cut(q["Boundary"]), "exact requested full certified boundary")
                chain.append(P.raw(reply["Page"]))
                if page["kind"] != 0:
                    require(page["tx"] not in business_pages, "business/feed lineage emitted twice")
                    business_pages[page["tx"]] = page["kind"]
            pages[request] = P.raw(reply["Page"])

            for frame in page["frames"]:
                key_ = frame["Partition"], frame["Index"]
                require(key_ in rows and P.raw(rows[key_]["Outcome"]) == frame["Outcome"] and P.raw(rows[key_]["CDC"]) == frame["CDC"], "Page comes from actual retained native original")
            if page["kind"] == 3:
                require(len(page["frames"]) == 2 and page["frames"][0]["CDC"] == page["frames"][1]["CDC"] and page["frames"][0]["Decision"] == page["frames"][1]["Decision"] and page["frames"][0]["Intent"] == page["frames"][1]["Intent"], "both exact move activation lineages")
                for f in page["frames"]: F.move_changes(f["CDC"], move_status(f["Outcome"]))
        if q["Action"] == "ack":
            request = fixed(q["Request"], 16, True)
            require(request in pages and fixed(q["Digest"], 32, True) == hashlib.sha256(pages[request]).digest(), "SAME ACK joins real pending page")
            actual_page = G.page(pages[request])
            require(G.codec.cut(reply["Completed"]) == (actual_page["boundary"] if actual_page["eof"] else actual_page["prior"]), "completed-cut never crosses a half transaction")
            acks.add(request)

    require(set(pages) <= acks, "page without completed SAME ACK")
    return business_pages


def k_label_changes(logical, partition, round_, expected_axis, seen):
    """Primary literal GCD5/CC1 reader for this fixed two-label client input."""
    c = P.Cursor(P.body(logical, b"GCD\5"))
    require(c.take(16) == P.GRAPH and c.number() == partition and c.number(1) == 0 and [c.number() for _ in range(3)] == [0, 0, 0] and c.number(4) == 0 and c.number(4) == 1, "label CDC namespace/no schema/one native axis")
    axis_id, profile, version = c.take(16), c.number(1), c.number(2)
    reference, unit = c.field(256), c.field(256)
    require(any(axis_id) and profile == 2 and version == 1 and bool(reference.decode("utf-8").strip()) and bool(unit.decode("utf-8").strip()), "label CDC exact native rational axis descriptor")
    descriptor = axis_id + struct.pack(">BH", profile, version) + struct.pack(">I", len(reference)) + reference + struct.pack(">I", len(unit)) + unit
    definition = hashlib.sha256(b"rho-tkg:axis:v1\0" + descriptor).digest()
    require(definition == expected_axis and [c.number(4) for _ in range(3)] == [0, 0, 0] and c.number(4) == 1, "label CDC no entity/life/value or extra component rows")
    owner, life, kind, name, member = c.number(), c.number(), c.number(1), c.field(256), c.number()
    require(owner == (1 if partition == 3 else 17) and owner not in seen and life == owner + 1 and kind == 2 and name == b"healthy" and member == 0 and c.number(4) == 1, "label CDC complete exact component key/one-based axis")
    scope = b"TS\1" + bytes([profile]) + axis_id + definition + bytes([6])
    require(c.field(53) == scope, "label CDC whole native-domain ownership")
    changes = P.Cursor(c.field())
    c.end()
    require(changes.take(4) == b"CC\1" + bytes([profile]) and changes.take(16) == axis_id and changes.take(32) == definition and changes.number(4) == 1 and changes.field(53) == scope and changes.take(34) == bytes(34), "label CDC exact original absent before image")
    require(changes.number(1) == changes.number(1) == 1 and [changes.number() for _ in range(4)] == [0, 0, round_, 23], "label CDC exact present-null after/revision/provenance")
    changes.end()
    seen.add(owner)


def business(a, scopes, rows):
    command, manifest, lost, lost_wire = P.peer_phase(P.raw(a["LostRequest"]), P.raw(a["LostReply"]), scopes, 4)
    require(lost["partition"] == 8 and lost["phase"] == 3 and lost["commit"], "actual committed K reply withheld, not an invented timeout vote")
    c = P.Cursor(P.body(manifest, b"GCM"+bytes((1,)))); tx, coordinator, original_wire = c.take(16), c.number(), c.field(32768)
    original = P.pgq(original_wire)
    require(coordinator == original["partition"] == 8 and tx == lost["tx"] and lost["digest"] == hashlib.sha256(manifest).digest(), "actual complete K original identity")
    wanted = {(8, 1), (3, 2), (8, 2), (8, 3), (3, 4), (8, 4)}
    phases = {}
    for (part, index), row in rows.items():
        wire, changes = P.raw(row["Outcome"]), P.raw(row["CDC"])
        if wire[:4] != b"GCS"+bytes((1,)): continue
        status = P.status(wire)
        if status["tx"] != tx: continue
        require(status["partition"] == part and status["digest"] == lost["digest"] and 0 < status["index"] <= index, "same K actual immutable original")
        require(P.raw(rows.get((part, status["index"]), {}).get("Outcome")) == wire, "K replay must join first installed native result")
        if status["index"] != index:
            require(not changes, "SAME K retry emitted a second delta"); continue
        key = part, status["phase"]
        require(key in wanted and key not in phases, "exact K native phase roster")
        phases[key] = status
        if status["phase"] == 4:
            require(status["commit"] and status["round"] == lost["round"], "complete K participant resolution")
            e = P.Cursor(P.body(changes, b"GCE"+bytes((1,))))
            require(e.number(1) == 6 and e.take(16) == tx and e.take(32) == lost["digest"] and e.number() == index, "actual K CDC original coordinate")
            logical = e.field(); e.end(); body = P.body(logical, b"GCD"+bytes((5,)))
            require(body[:16] == P.GRAPH and int.from_bytes(body[16:24], "big") == part, "actual complete K business namespace")
            if part == 3:
                _, _, native = cut(a["Before"])
                seen = set(); k_label_changes(logical, part, lost["round"], bytes.fromhex(native["A/fact/1"]["axis"]), seen)
                require(seen == {1}, "literal K healthy label exact owner")
        else: require(not changes, "non-Resolve K emitted CDC")
    require(set(phases) == wanted and phases[3, 2]["yes"] and phases[8, 2]["yes"] and phases[8, 3] == lost, "complete actual K six-phase commit")
    fields = ["Business", "BusinessReplay"]
    if a["Recipe"]["Family"] == "gc" or a["Recipe"]["Family"] == "move" and a["Recipe"]["Phase"] != 9:
        require(a["BusinessAfterGC"] is not None, "SAME K must recover after real collection")
        fields.append("BusinessAfterGC")
    for field in fields:

        P.resumed(a[field], scopes, original["request"], tx, P.business_hash(original_wire), 0, lost_wire)
        require(R.response(a[field])["code"] == 0, "SAME K recovery did not succeed")
    changed = P.pgq(P.raw(a["ChangedCaller"]))
    refusal = R.response(a["ChangedReply"])
    require(changed["request"] == original["request"] and changed["partition"] == coordinator
            and P.business_hash(P.raw(a["ChangedCaller"])) != P.business_hash(original_wire)
            and refusal["code"] != 0 and refusal["mask"] & (1 << 4) and not refusal["outcome"] and not refusal["changes"], "actual changed K body refused through original lookup")


def allocator_checkpoint(encoded):
    response = R.response(encoded); require(response["code"] == response["mask"] == 0, "actual native allocator observation")
    c = P.Cursor(response["aux"]); index, last = c.number(), c.number(); wire, grant = c.field(121), c.field(1024); c.end()
    require(len(wire) == 121 and wire[:5] == b"RIDA"+bytes((1,)) and wire[5:21] == P.GRAPH and any(wire[21:37]) and not grant and index <= last, "actual bounded native allocator checkpoint")
    checksum = 0xffffffff
    for byte in wire[:-4]:
        checksum ^= byte
        for _ in range(8): checksum = (checksum >> 1) ^ (0x82f63b78 if checksum & 1 else 0)
    require((checksum ^ 0xffffffff) == int.from_bytes(wire[-4:], "big"), "actual allocator checkpoint CRC")
    return int.from_bytes(wire[37:45], "big"), int.from_bytes(wire[45:53], "big")


def refusals(a, rows):
    expected = a["Recipe"]["Family"] == "move" and a["Recipe"]["Phase"] != 9
    values = a["Refusals"] or []
    require([v["Kind"] for v in values] == (["old-owner-write", "old-allocator-epoch"] if expected else []), "actual P07 delayed authority refusal inventory")
    for value, magic, why in zip(values, (b"GRO"+bytes((4,)), b"GRO"+bytes((2,))), (9, 2)):
        require(type(value) is dict and set(value) == {"Kind", "Proposal", "Before", "After", "Record"}, "bounded delayed authority proof")
        before, after, record = (R.response(value[k]) for k in ("Before", "After", "Record"))
        body = P.body(record["outcome"], magic)
        require(len(body) == 84 and body[1:17] == P.GRAPH and int.from_bytes(body[17:25], "big") == 3
                and body[41:73] == hashlib.sha256(P.raw(value["Proposal"])).digest()
                and int.from_bytes(body[73:81], "big") == after["applied"] > before["applied"]
                and body[81] == 1 and int.from_bytes(body[82:84], "big") == why
                and before["image"] == after["image"] and not record["changes"]
                and P.raw(rows[3, after["applied"]]["Outcome"]) == record["outcome"], "old owner/allocator cannot acknowledge success or mutate state")
    if expected:
        old_epoch, old_high = allocator_checkpoint(a["AllocatorBefore"])
        new_epoch, new_high = allocator_checkpoint(a["AllocatorAfter"])
        require(new_epoch == old_epoch+1 and old_high == new_high == 32, "real allocator epoch transfer preserves high water; stale reserve issued nothing")


def reclaimed(before, after):
    require(len(before) == len(after) == 6 and all(len(v["stats"]) == 28 for v in before+after), "actual six-voter native statistics grammar")
    require(any(integer(y["stats"][19]) < integer(x["stats"][19]) for x, y in zip(before, after)), "actual eligible physical native records reclaimed")


def gc(a, d):

    require(len(a["BeforeGC"] or []) == len(a["AfterGC"] or []) == 6 and a["GC"], "actual six-voter GC inventory")
    before = [R.response(x) for x in a["BeforeGC"]]; after = [R.response(x) for x in a["AfterGC"]]
    reclaimed(before, after)
    plans, eof = {}, set()
    for r in a["GC"]:
        require(set(r) == R.RECEIPT and r["Profile"] == PROFILE and fixed(r["Run"], 16, True) == fixed(a["Run"], 16, True) and r["DeadlineUnixNanos"] == a["DeadlineUnixNanos"], "actual GC owner/run/deadline")
        member = integer(r["Member"], 6, 1)
        if r["Action"] == "current":
            plan = r["Plan"]; require(set(plan) == R.PLAN, "native actual installed plan")
            plans[3 if member <= 3 else 8] = plan
        if r["Action"] == "collect" and r["Progress"]["Complete"] is True:
            require(fixed(r["Progress"]["PlanID"], 32, True) == hashlib.sha256(P.raw(r["Original"])).digest(), "collector exact native original"); eof.add(member)
    require(eof == set(range(1, 7)) and set(plans) == {3, 8}, "actual resumable native collector EOF")
    if a["Recipe"]["Family"] == "publication-race":
        require(plans[3]["RootFloor"] > d["parts"][3]["source"], "real GC crossed actual unpublished recipe data/root")
        publish = [T.decoded(R.response(x["Response"])["aux"]) for x in a["Calls"] if x["Input"]["Action"] == "publish"]
        require(len(publish) >= 2 and all(r["Result"] == "expired" and not P.raw(r["Token"]) for r in publish), "no post-collection token/current substitution")
    else:
        require(all(plans[p]["RootFloor"] <= d["parts"][p]["source"] and plans[p]["DataFloor"] <= d["parts"][p]["source"] for p in (3, 8)), "live C lineage protected while eligible state reclaimed")


def check(a):
    require(type(a) is dict and set(a) == HEADER and type(a["Version"]) is int and a["Version"] == 1 and a["Profile"] == PROFILE, "fresh D-only state grammar")
    family, _, part, _, _, _ = recipe(a["Recipe"])
    fixed(a["Run"], 16, True); integer(a["DeadlineUnixNanos"], (1 << 63)-1, 1)
    require(integer(a["ReceiptReads"], 129)*(64 << 10) == integer(a["ReceiptGrantBytes"], 129*(64 << 10)), "finite receipt read/decode grants remain retired")
    rows = prefix(a)
    if family == "allocation":
        # This supplement attributes exact native Sync; accepted P08 issuance,
        # stale recipient and exhaustion schedules remain separately accepted.
        command = P.raw(a["Command"]); c = P.Cursor(P.body(command, b"AQP\1")); phase, graph, partition = c.number(1), c.take(16), c.number()
        require(graph == P.GRAPH and phase == a["Recipe"]["Phase"] and partition == 3, "actual allocation command")
        scopes = {}  # Allocation has its own exact graph/partition command; no invented cut.
        e = batch(a, scopes); processes(a, a["Batch"])
        require(R.response(a["Business"])["outcome"] and R.response(a["BusinessReplay"])["outcome"], "actual durable recovered allocation original")
        return
    d = definition(a["Original"], a["DeadlineUnixNanos"])
    scopes, _, _ = cut(a["Before"])
    if family != "publication-race":
        cut(a["Retained"]); cut(a["Reopened"])
        require(a["Before"] == a["Retained"] == a["Reopened"], "full original C exact set/revisions survive native loss/GC")
        business(a, scopes, rows)
        refusals(a, rows)
        if family in ("move", "pin"):
            batch(a, scopes); processes(a, a["Batch"])
        elif family == "gc":
            ident = S.pair(a["StateArm"], a["State"], fixed(a["Run"], 16), a["DeadlineUnixNanos"], member=3)
            require(ident["PlanID"] == a["State"]["Control"]["Filter"]["PlanID"], "actual GC cursor/bank/plan Sync")
            processes(a, a["State"])
    feed_business = check_calls(a, d, rows)
    if family == "gc" or family == "move" and a["Recipe"]["Phase"] != 9:
        _, _, lost, _ = P.peer_phase(P.raw(a["LostRequest"]), P.raw(a["LostReply"]), scopes, 4)
        require(feed_business.get(lost["tx"]) == 2, "complete actual K feed envelope survives original reply loss")
        require(sum(v == 3 for v in feed_business.values()) == (1 if family == "move" else 0), "exact complete move lineage, not a half or duplicate group")

    if family in ("gc", "publication-race") or family == "move" and a["Recipe"]["Phase"] != 9: gc(a, d)


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("artifact", type=Path); args = parser.parse_args()
    check(load(args.artifact)); print("native P07-P09 exact native state joins: source artifact accepted")


if __name__ == "__main__": main()
