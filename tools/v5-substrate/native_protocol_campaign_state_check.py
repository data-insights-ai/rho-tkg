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
X = module("protocol_state_pooled", "native_transfer_check.py")
T, P, F = B.T, B.P, S.F
require, integer, fixed = T.require, T.integer, T.fixed
PROFILE = "native-protocol-owned-state-transition-v1"
SEMANTIC = hashlib.sha256(hashlib.sha256(G.AGREEMENT + b"feed-retention=irreversible-rolling-ack-anchor-v1\0snapshot-reopen=authenticated-expired-v1\0payload-anchor=exact-plm-round-image-v1\0").digest() + b"range-move=compact-recovery-spans-v1\0retention=immutable-native-reference-v1\0replay=bounded-original-batches32-v1\0").digest()
POOLED_PROFILE = "native-protocol-owned-pooled-state-transition-v1"
POOLED_SEMANTIC = hashlib.sha256(hashlib.sha256(SEMANTIC + b"graph=bounded-native-rmp-batches32-component8-family-boundary-v1\0progress=gmp5-native-graph-count-v1\0").digest() + b"range-move=hard-pooled-retained-v1\0undo=before-image-debt-v1\0capacity=nonterminal-exact-original-v1\0").digest()
HEADER = set("Version Profile Recipe Run DeadlineUnixNanos Arm Batch StateArm State Command Intent Calls GC BeforeGC AfterGC Before Retained Reopened Processes Original Token Business BusinessReplay BusinessAfterGC LostRequest LostReply From Through Records ReceiptReads ReceiptGrantBytes Refusals ChangedCaller ChangedReply AllocatorBefore AllocatorAfter".split())
CONTROL = set("Version Cookie Sequence Action Request Original Token Boundary Digest".split())
REPLY = set("Version Profile Cookie Run Sequence Calls TerminalCalls GrantedWork Member PID DeadlineUnixNanos Action Result Original Token Page Digest Snapshot Completed".split())


PROGRAM_PROFILE = "native-protocol-owned-pooled-state-program-v1"


def program_state_roles(context, recipe):
    require(type(context) is dict and set(context) == {"leaders", "target"} and type(context["leaders"]) is tuple
            and context["leaders"] in ((1, 4), (2, 6)) and all(type(v) is int for v in context["leaders"])
            and type(context["target"]) is int, "explicit authenticated native state roles")
    leaders, target = context["leaders"], context["target"]
    part = recipe["Partition"]
    expected = (3 if part == 3 else 6) if leaders == (1, 4) else (1 if part == 3 else 4)
    require(recipe["Family"] != "allocation" and target == expected, "actual nonowner follower loss, no leader/accepted issuance relabelling")
    return leaders, target


def state_profile(a, program_roles=None):
    if type(a.get("Version")) is int and a["Version"] == 3:
        program_state_roles(program_roles, a["Recipe"])
        require(a.get("Profile") == PROGRAM_PROFILE and set(a) == HEADER | {"Pooled", "Program"}, "fresh state program grammar")
        return POOLED_PROFILE, POOLED_SEMANTIC
    fresh = type(a.get("Version")) is int and a["Version"] == 2
    require((fresh and a.get("Profile") == POOLED_PROFILE and set(a) == HEADER | {"Pooled"}) or
            (type(a.get("Version")) is int and a["Version"] == 1 and a.get("Profile") == PROFILE and set(a) == HEADER), "explicit state agreement/version; no inferred upgrade")
    return (POOLED_PROFILE, POOLED_SEMANTIC) if fresh else (PROFILE, SEMANTIC)


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


def load(path, *, program=False):
    total, digest, header, footer = 0, hashlib.sha256(), None, None
    rows, calls, gc, pooled = [], [], [], []
    with path.open("rb") as stream:
        while line := stream.readline((512 << 10)+1):
            total += len(line)
            require(total <= 16 << 20 and len(line) <= 512 << 10 and line.endswith(b"\n"), "bounded state artifact")
            row = T.decoded(line)
            require(type(row) is dict and footer is None, "state row after EOF")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and row["Type"] == "header", "state header")
                header = row["Artifact"]
                require(type(header) is dict, "owned streamed proof object")
                version = header.get("Version")
                require(type(version) is int and (version in (1, 2) or program and version == 3), "explicit admitted state stream version")
                require(set(header) == (HEADER | {"Pooled", "Program"} if version == 3 else HEADER | {"Pooled"} if version == 2 else HEADER), "exact selected state stream header")
                require(all(header[k] is None for k in ("Records", "Calls", "GC")), "owned streamed proof grammar")
                if version in (2, 3):
                    if version == 2: state_profile(header)
                    else: require(header["Profile"] == PROGRAM_PROFILE and type(header["Program"]) is dict, "fresh program is validated before optional semantic routing")
                    p = header["Pooled"]
                    require(type(p) is dict and set(p) == {"Version", "Semantic", "Frames"} and type(p["Version"]) is int and p["Version"] == 1 and fixed(p["Semantic"], 32) == POOLED_SEMANTIC and p["Frames"] is None, "fresh native pooled stream header")
            elif row.get("Type") == "record":
                require(set(row) == {"Type", "Record"} and len(rows) < 512, "bounded native prefix"); rows.append(row["Record"])
            elif row.get("Type") == "pooled":
                require(header["Version"] in (2, 3) and set(row) == {"Type", "Sequence", "Frame"} and type(row["Sequence"]) is int and row["Sequence"] == len(pooled) < 128, "bounded exact pooled frame order")
                pooled.append(row["Frame"])
            elif row.get("Type") in ("call", "gc"):
                kind = row["Type"]; target = calls if kind == "call" else gc; field = "Call" if kind == "call" else "Receipt"
                require(set(row) == {"Type", "Sequence", field} and type(row["Sequence"]) is int and row["Sequence"] == len(target) < (416 if kind == "call" else 2304), "exact bounded owner row sequence")
                target.append(row[field])
            else:
                extra = {"Pooled"} if header["Version"] in (2, 3) else set()
                require(set(row) == {"Type", "Rows", "Calls", "GC", "Through", "PayloadSHA256"} | extra and row["Type"] == "footer" and all(type(row[k]) is int and row[k] == len(v) for k, v in (("Rows", rows), ("Calls", calls), ("GC", gc))) and row["PayloadSHA256"] == digest.hexdigest(), "exact state EOF/hash/census")
                if extra: require(type(row["Pooled"]) is int and row["Pooled"] == len(pooled), "pooled stream complete EOF census")
                footer = row
            if footer is None: digest.update(line)
    require(header is not None and footer is not None and footer["Through"] == header["Through"], "missing/foreign complete EOF")
    header.update(Records=rows, Calls=calls, GC=gc)
    if header["Version"] in (2, 3): header["Pooled"]["Frames"] = pooled
    return header


def cut(read, semantic=SEMANTIC):
    scopes, round_, values = F.range_read(read)
    require(set(scopes) == {3, 8} and all(scope[88:120] == semantic for scope in scopes.values()), "exact selected native semantic/full scope")
    M.census(read)
    return scopes, round_, values


def definition(encoded, deadline, semantic=SEMANTIC):
    wire = P.raw(encoded); c = P.Cursor(P.body(wire, b"GFD\1")); manifest = c.field(32768)
    policy = (c.number(4), c.number(4), c.number(), c.number(), c.number(4), c.number(4), c.number(4), c.number()); c.end()
    require(policy == (65536, 256, 64, 512, 16384, 16 << 20, 32 << 20, deadline), "same fixed original feed work/deadline")
    c = P.Cursor(P.body(manifest, b"GPM\1")); scope, issuer, request = c.take(120), c.take(16), c.take(16)
    epoch, expires, count = c.number(), c.number(), c.number(4)
    require(scope[88:] == semantic and int.from_bytes(scope[16:24], "big") in (3, 8) and any(issuer) and any(request) and epoch == 0 and expires == 1 and count == 2, "genuine complete original manifest/issuer")
    parts = {}
    for part in (3, 8):
        cert, start, lineage = c.field(512), c.number(), c.take(32)
        cert_body = P.body(cert, b"GSC\1")
        require(len(cert_body) == 356 and cert_body[88:120] == semantic and int.from_bytes(cert_body[16:24], "big") == part and start == int.from_bytes(cert_body[176:184], "big")+1 and lineage == hashlib.sha256(b"rho-tkg:retained-pin-source-lineage:v1\0"+cert+struct.pack(">Q", start)).digest(), "actual native certificate/data/feed lineage")
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


def snapshot(value, d, before, semantic=SEMANTIC):
    require(type(value) is dict and set(value) == {"Cut", "Facts", "Entities"}, "actual held snapshot projection grammar")
    require(type(value["Cut"]) is dict and set(value["Cut"]) == {"Certificates"}
            and [P.raw(x) for x in value["Cut"]["Certificates"]] == [d["parts"][p]["certificate"] for p in (3, 8)], "held C is actual Original capture, not an independent Fresh")
    _, _, native = cut(before) if semantic == SEMANTIC else cut(before, semantic)
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
        if scopes[part][88:] == POOLED_SEMANTIC:
            value = pooled_command(command)
            require(value["scope"] == scopes[part] and (value["phase"], value["sequence"]) == (phase, page), "canonical actual pooled phase/page")
            return value["intent"]["digest"]
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


def move_status(wire, semantic=SEMANTIC):
    if semantic == POOLED_SEMANTIC:
        return G.column.pooled_move_status(G.raw, wire)
    c = P.Cursor(P.body(wire, b"GMO\2")); graph, part, move, intent = c.take(16), c.number(), c.take(16), c.take(32)
    phase, result = c.number(1), c.number(1)
    index, floor, round_, sequence, rows, size = (c.number() for _ in range(6)); digest = c.take(32); c.end()
    require(graph == P.GRAPH and part in (3, 8) and any(move) and any(intent) and 1 <= phase <= 9 and 1 <= result <= 4 and index > 0, "actual immutable GMO2")
    return dict(partition=part, move=move, intent=intent, phase=phase, result=result, index=index, floor=floor, round=round_, sequence=sequence, rows=rows, size=size, digest=digest)


def pooled_command(wire):
    require(X.AGREEMENT == POOLED_SEMANTIC, "owning pooled codec agreement is the selected native profile")
    return X.move_command(wire)


def pool_control(value, key, root_index):
    require(type(value) is dict and set(value) == {"Found", "Key", "Index", "Value"}
            and type(value["Found"]) is bool and P.raw(value["Key"]) == key, "actual exact native control key/presence")
    at, wire = integer(value["Index"], root_index), P.raw(value["Value"])
    require((at > 0 if value["Found"] else at == 0 and not wire), "missing control cannot invent empty original/index")
    return (at, wire) if value["Found"] else None


def pool_undo(wire, *, fence=False):
    c = P.Cursor(P.body(wire, b"GMU\1" if fence else b"GMU\2"))
    require(len(wire) <= (1 << 20)-2048, "bounded actual before-image")
    bound = 0 if fence else integer(c.number(), 1 << 40, 1)
    rows, keys = [], set()
    for _ in range(integer(c.number(4), 128)):
        key, value, deleted = c.field(65536), c.field(1 << 20), c.number(1)
        require(key and len(key)+len(value) <= 1 << 20 and deleted in (0, 1) and (not deleted or not value)
                and key not in keys, "complete before-image KVs, no duplicate native write key")
        keys.add(key); rows.append((key, value, deleted))
    c.end()
    # Every before-image byte and native escaped key/version frame must fit the
    # independently funded bound. Native policy also includes progress/status,
    # immutable retry control, outcome, root and maximum RS4/TB1 overhead.
    require(fence or bound >= sum(len(k)+k.count(0)+47+len(v) for k, v, _ in rows), "undo bound cannot discount actual before-image growth")
    return bound, rows


def pool_undo_bound(wire, command):
    bound, rows = pool_undo(wire)
    ns = command["scope"][:24]; move = command["intent"]["move"]
    progress_key, status_key = [b"ga"+bytes((tag,))+ns+move for tag in (19, 18)]
    retry_key = b"GMV\1"+ns+move+struct.pack(">I", 0x40000000)
    # Exact moveUndoBoundWithPolicy: base root/outcome frames (135+172+231),
    # progress padded to32KiB, GMO3 status231, GM/V hash+status263, native RS4
    # with all8 spans and16 TB1 owners (3024+45), plus2*retry-key padding.
    cost = lambda k, value_bytes: len(k)+k.count(0)+47+value_bytes
    expected = (135+172+231 + sum(cost(k,len(v)) for k,v,_ in rows)
                +cost(progress_key,32768)+cost(status_key,231)+cost(retry_key,263)
                +3069+2*len(retry_key))
    require(bound == expected and len(rows)+7 <= 128 and bound <= command["intent"]["pools"][2], "exact native before-image/status/control/maximum RS4-TB1 bound and independent undo grant")
    return bound


def pool_progress(wire, command):
    if wire.startswith(b"GMC\1"):
        c = P.Cursor(P.body(wire, b"GMC\1")); scope, floor = c.take(120), c.number()
        generation, index, hash_, image = c.number(), c.number(), c.take(32), c.field(172); c.end()
        require(scope == command["scope"] and generation > 0 and index > 0 and len(image) == 172 and hashlib.sha256(image).digest() == hash_, "actual capture progress, not fabricated zero-debt GMP")
        return dict(imported=False, next=0, total=0, debt=0, graph=0, applied=0, replay=0)
    require(len(wire) <= 32768, "bounded compact pooled progress")
    c = P.Cursor(P.body(wire, b"GMP\6"))
    descriptor, source, imported = c.field(16384), c.field(129 << 10), c.field((1 << 20)-1024)
    replay, end = c.field(129 << 10), c.field(129 << 10)
    graph, next_, total = c.number(), c.number(), c.number()
    key, header, packet, size, rows = c.field(52), c.take(32), c.take(32), c.number(), c.number()
    replay_pages, applied, debt = c.number(), c.number(), c.number()
    inline, retention = c.number(1), c.field(1 << 20); c.end()
    intent = command["intent"]
    require(key == b"GMH\1"+P.GRAPH+struct.pack(">Q", 3)+intent["move"]+bytes(8)
            and any(header) and any(packet) and 0 < size <= 320 << 10 and 1 <= rows <= 17
            and inline in (0, 1) and bool(retention) == bool(inline), "exact immutable retained-reference identity")
    d = P.Cursor(P.body(descriptor, b"RMD\1"))
    publication, from_, to = d.field(256), d.field(256), d.field(256)
    require(publication == intent["publication"], "progress binds actual HOME publication")
    for wire_, part_, epoch_ in ((from_,3,intent["epoch"]),(to,8,intent["epoch"]+1)):
        ownership = P.body(wire_, b"ROF\1")
        require(len(ownership) == 73 and ownership[:16] == P.GRAPH and struct.unpack_from(">3Q",ownership,16) == (intent["range"],part_,epoch_)
                and ownership[40:72] == P.body(publication,b"RPB\1")[56:88] and ownership[72] == 0, "actual unfenced source/successor placement descriptor")
    for scope in (intent["source"], intent["target"]):
        graph_, part, group, semantic = d.take(16), d.number(), d.take(16), d.take(32)
        generation, index, image, hash_ = d.number(), d.number(), d.take(172), d.take(32)
        require(graph_+struct.pack(">Q", part) == scope[:24] and group == scope[24:40] and semantic == POOLED_SEMANTIC
                and generation == 0 and index > 0 and hashlib.sha256(image).digest() == hash_, "actual graphstore capture has generation0, exact source/target roots")
        G.column.root(G.raw, image, part)
    declaration, configuration, axis = d.take(32), d.take(32), d.field(1024)
    d.number(); d.number(); d.take(32); sealed = d.number(1); d.end()
    require(from_ and to and any(declaration) and any(configuration) and axis and sealed in (0, 1), "complete actual descriptor")
    for cursor_wire in (replay, end):
        if P.body(cursor_wire, b"GEC\1") != bytes(82): X.replay_cursor(cursor_wire)
    require(not (source and imported) and 0 <= next_ <= total <= intent["pages"] and graph+replay_pages <= intent["pages"]
            and applied <= graph and debt <= intent["pools"][2], "actual accepted graph/replay/undo counters")
    if source: P.body(source, b"RMC\1")
    if imported: P.body(imported, b"RMS\1")
    else: require(next_ == total == debt == applied == 0, "source progress cannot create target undo debt")
    require(next_ != total or debt == 0, "completed undo leaves no debt")
    return dict(imported=bool(imported), next=next_, total=total, debt=debt, graph=graph, applied=applied, replay=replay_pages)


def pool_exchange(item, leaders=(1, 4)):
    require(type(item) is dict and set(item) == {"Origin", "Partition", "Request", "Reply", "Before", "After"}, "actual complete pooled Submit observation")
    require(integer(item["Origin"], 6, 1) and item["Partition"] in (3, 8), "real source caller/partition")
    c = P.Cursor(P.body(P.raw(item["Request"]), b"GSQ\1"))
    kind, sequence, nonce = c.number(1), c.number(), c.take(16)
    cookie, maximum, rows = c.take(40), c.number(4), c.number(4)
    keys = [c.field(65536) for _ in range(3)]
    certified, tx, phase = c.field(1 << 20), c.take(16), c.number(1); c.end()
    require(kind == 8 and sequence > 0 and any(nonce) and not any(cookie) and 0 < maximum <= 2 << 20
            and rows == phase == 0 and not any(tx) and not any(keys), "actual sourceSubmit, no synthetic command receipt")
    c = P.Cursor(P.body(certified, b"GSC\1"))
    scope, command = P.body(c.field(256), b"APS\1"), c.field(1 << 20); c.end()
    parsed = pooled_command(command)
    require(scope == parsed["scope"] and int.from_bytes(scope[16:24], "big") == item["Partition"], "actual certified target scope")
    if item["Reply"] is None:
        return command, parsed, b"" # Actual transport-unknown; the native after-proof decides what installed.
    c = P.Cursor(P.body(P.raw(item["Reply"]), b"GSR\1"))
    actual, member, issuer = P.body(c.field(256), b"APS\1"), c.number(), c.take(16)
    reply_kind, reply_sequence, reply_nonce, mask = c.number(1), c.number(), c.take(16), c.number(2)
    result = c.field((2 << 20)-512); c.end()
    require(actual == scope and member == leaders[0 if item["Partition"] == 3 else 1] and any(issuer)
            and (reply_kind, reply_sequence, reply_nonce) == (kind, sequence, nonce), "authenticated source reply association")
    require(not mask or not result, "unavailable submit supplies no invented original")
    if result:
        status = move_status(result, POOLED_SEMANTIC)
        require(status["move"] == parsed["intent"]["move"] and status["intent"] == parsed["intent"]["digest"] and status["partition"] == item["Partition"], "exact submitted immutable original")
    return command, parsed, result


def pool_observation(item, command, parsed, a, leaders=(1, 4)):
    require(type(item) is dict and set(item) == {"Member", "Input", "Response"}, "actual move-proof owner call")
    part = int.from_bytes(parsed["scope"][16:24], "big")
    require(item["Member"] == leaders[0 if part == 3 else 1], "proof executes at actual native leader")
    q = item["Input"]
    require(type(q) is dict and set(q) == CONTROL and q["Action"] == "move-proof" and type(q["Version"]) is int and q["Version"] == 1
            and P.raw(q["Original"]) == command and not P.raw(q["Token"]) and fixed(q["Request"], 16) == bytes(16) and fixed(q["Digest"], 32) == bytes(32), "proof binds actual complete command, no minted authority")
    integer(q["Sequence"], 416, 1); fixed(q["Cookie"], 16, True)
    require(len(command) <= 32 << 10, "actual proof command input cap")
    response = R.response(item["Response"]); r = T.decoded(response["aux"])
    require(response["code"] == response["mask"] == 0 and type(r) is dict and set(r) == REPLY and r["Profile"] == PROFILE
            and type(r["Version"]) is int and r["Version"] == 1 and r["Action"] == q["Action"] and r["Sequence"] == q["Sequence"]
            and r["Cookie"] == q["Cookie"] and r["Member"] == item["Member"] and fixed(r["Run"], 16, True) == fixed(a["Run"], 16, True)
            and r["DeadlineUnixNanos"] == a["DeadlineUnixNanos"] and r["Result"] == "applied" and r["Snapshot"] is None, "same successful bounded native proof owner/run/deadline")
    page = P.raw(r["Page"]); require(len(page) <= 64 << 10, "actual bounded proof Page")
    require(fixed(r["Digest"], 32, True) == hashlib.sha256(page).digest(), "owned proof digest binds exact complete Page")
    p = T.decoded(page)
    names = "Version CommandSHA256 Scope RootIndex Root Frame PreviousRoot IntentControl OriginalControl DecisionControl StatusFound Status ProgressFound Progress UndoIndices Undo Rows Bytes Output Complete".split()
    require(type(p) is dict and set(p) == set(names) and type(p["Version"]) is int and p["Version"] == 1 and p["Complete"] is True
            and fixed(p["CommandSHA256"], 32, True) == hashlib.sha256(command).digest()
            and P.raw(p["Scope"]) == parsed["scope"], "same-view complete requested proof; not whole-prefix EOF")
    root_index = integer(p["RootIndex"], minimum=1)
    G.column.root(G.raw, fixed(p["Root"], 172), part)
    require(integer(p["Rows"], 256) > 0 and integer(p["Bytes"], 16 << 20) > 0 and integer(p["Output"], 32 << 20) > 0, "actual prepaid bounded native control/read/output work, separate from direct historical reads")
    ns = P.GRAPH+struct.pack(">Q", part)
    base = b"GM"; move = parsed["intent"]["move"]
    controls = {}
    for field, tag in (("IntentControl", b"I"), ("DecisionControl", b"D")):
        controls[field] = pool_control(p[field], base+tag+b"\1"+ns+move, root_index)
    phase, seq = parsed["phase"], parsed["sequence"]
    credit = {2:1, 3:0x10000000+seq, 4:0x20000000+seq, 5:0x30000000, 6:0x30000001, 7:0x30000002, 8:0xf0000000, 9:0xf0000000 if seq == 0 else 0x40000000+seq-1}.get(phase)
    intent_original = phase in (1,9) and p["OriginalControl"]["Found"] and P.raw(p["OriginalControl"]["Key"]) == base+b"I\1"+ns+move
    original_key = base+b"I\1"+ns+move if intent_original else base+b"V\1"+ns+move+struct.pack(">I", 0 if phase == 1 else credit)
    controls["OriginalControl"] = pool_control(p["OriginalControl"], original_key, root_index)
    if controls["IntentControl"] is not None: require(controls["IntentControl"][1] == parsed["original"], "observed durable original cannot substitute intent")
    if controls["DecisionControl"] is not None:
        decision = move_status(controls["DecisionControl"][1], POOLED_SEMANTIC)
        require(decision["partition"] == 3 and decision["phase"] == 6 and decision["result"] == 1 and decision["move"] == move
                and decision["intent"] == parsed["intent"]["digest"] and (part != 3 or decision["index"] == controls["DecisionControl"][0]), "actual source decision or authenticated target copy, no synthetic terminal")
    for flag, field in (("StatusFound", "Status"), ("ProgressFound", "Progress")):
        require(type(p[flag]) is bool and (p[flag] or not P.raw(p[field])), "absent mutable state is explicit")
    status = move_status(P.raw(p["Status"]), POOLED_SEMANTIC) if p["StatusFound"] else None
    if status is not None:
        require(status["partition"] == part and status["move"] == move and status["intent"] == parsed["intent"]["digest"] and status["index"] <= root_index, "same native mutable status/intent/root")
    progress = pool_progress(P.raw(p["Progress"]), parsed) if p["ProgressFound"] else dict(imported=False, next=0, total=0, debt=0, graph=0, applied=0, replay=0)
    indices = p["UndoIndices"]
    require(type(indices) is list and 1 <= len(indices) <= 4 and indices[0] == 0 and indices == sorted(set(integer(v, parsed["intent"]["pages"]) for v in indices))
            and (not P.raw(q["Boundary"]) or P.raw(q["Boundary"]) == b"".join(struct.pack(">Q", v) for v in indices))
            and type(p["Undo"]) is list and len(p["Undo"]) == len(indices), "exact bounded requested undo-key set")
    if not P.raw(q["Boundary"]):
        expected = {0}
        if parsed["phase"] == 4 and parsed["sequence"] > 0: expected.add(parsed["sequence"])
        if status is not None and status["phase"] == 4 and status["sequence"] > 0: expected.add(status["sequence"])
        if progress["total"]: expected.add(progress["total"])
        if progress["next"] < progress["total"]: expected.add(progress["total"]-progress["next"])
        require(indices == sorted(expected), "native automatic probe selects current/next exact keys, not caller-chosen omissions")
    undo = {}
    for seq, value in zip(indices, p["Undo"]):
        observed = pool_control(value, base+b"U\1"+ns+move+struct.pack(">Q", seq), root_index)
        undo[seq] = observed
        if observed is not None:
            require(len(observed[1]) <= 16 << 10, "actual bounded GMU diagnostic read")
            if seq == 0: pool_undo(observed[1], fence=True)
            else: pool_undo_bound(observed[1], parsed)
    frame = p["Frame"]
    require((frame is not None) == (controls["OriginalControl"] is not None), "exact requested original, no current-status substitution")
    if frame is not None:
        require(type(frame) is dict and set(frame) == set("Partition Index Root Outcome CDC Registration OriginalOutcome OriginalCDC Rows".split()), "complete genuine native original frame")
        integer(frame["Index"], root_index, 1)
        require(frame["Partition"] == part and frame["Index"] == controls["OriginalControl"][0] and frame["Index"] <= root_index
                and not frame["Rows"], "actual first original frame and zero business rows")
        original = move_status(P.raw(frame["Outcome"]), POOLED_SEMANTIC)
        require(original["index"] == frame["Index"], "proof reads first install, not later replay index")
        expected = parsed["original"] if intent_original else hashlib.sha256(command).digest()+P.raw(frame["Outcome"])
        require(controls["OriginalControl"][1] == expected, "actual immutable GM/I or GM/V command original")
        registration = P.Cursor(P.body(P.raw(frame["Registration"]), b"NCM1"))
        for field in ("IntentControl", "OriginalControl", "DecisionControl"):
            key_, at_, value_ = registration.field(1024), registration.number(), registration.field(2048)
            if field != "DecisionControl" or key_:
                require(controls[field] is not None and (key_,at_,value_) == (P.raw(p[field]["Key"]),*controls[field]), "same-view control observation joins actual original registration")
            else:
                require(at_ == 0 and not value_ and (controls[field] is None or controls[field][0] > frame["Index"]), "only actual later decision is excluded from first-original metadata")
        registration.end()
    return dict(proof=p, reply=r, status=status, progress=progress, undo=undo, frame=frame, controls=controls)


def pool_transition(command, before, after, inventory):
    """Debt is a live prefix; consumed immutable GMUs need not be deleted."""
    frame = after["frame"]
    if frame is None: return
    if before["frame"] is not None:
        require(before["proof"]["Progress"] == after["proof"]["Progress"], "SAME original cannot consume import/undo twice")
        return
    require(before["proof"]["RootIndex"] < frame["Index"] <= after["proof"]["RootIndex"], "new original is installed between these actual views")
    status = move_status(P.raw(frame["Outcome"]), POOLED_SEMANTIC)
    p, q = before["progress"], after["progress"]
    if status["result"] == 5:
        require(before["proof"]["Progress"] == after["proof"]["Progress"], "capacity refusal cannot accept attempted page/undo debt")
        return
    if int.from_bytes(command["scope"][16:24], "big") != 8: return
    phase, seq = command["phase"], command["sequence"]
    if phase == 4 and status["result"] == 1:
        if seq == 0:
            require(q["total"] == q["next"] == q["debt"] == 0, "initial target/fence undo is separately funded")
        else:
            require(seq in inventory and q["total"] == p["total"]+1 == seq and q["next"] == p["next"]
                    and q["debt"] == p["debt"]+pool_undo_bound(inventory[seq][1], command), "one accepted Import adds exactly its independently funded before-image")
    elif phase == 9:
        if seq == 0:
            require(p["next"] == p["total"] and p["debt"] == q["debt"] == 0, "terminal routing/fence rollback cannot skip outstanding undo")
        else:
            original = p["total"]-p["next"]
            require(original in inventory and q["total"] == p["total"] and q["next"] == p["next"]+1 == seq
                    and q["debt"]+pool_undo_bound(inventory[original][1], command) == p["debt"], "actual exact-next reverse tail consumes debt once, not its retained control")


def pooled(a, rows, program_roles=None):
    leaders = (1, 4) if program_roles is None else program_state_roles(program_roles, a["Recipe"])[0]
    p = a["Pooled"]
    require(type(p) is dict and set(p) == {"Version", "Semantic", "Frames"} and type(p["Version"]) is int and p["Version"] == 1
            and fixed(p["Semantic"], 32) == POOLED_SEMANTIC and type(p["Frames"]) is list and len(p["Frames"]) <= 128, "finite actual pooled submit inventory")
    has_move = a["Recipe"]["Family"] == "move" or a["Recipe"]["Family"] == "pin" and a["Recipe"]["Phase"] == 2
    require(bool(p["Frames"]) == has_move, "actual move trace required only where real move runs")
    # The fixture cap128 bounds both original joins and the live undo inventory;
    # this is not a graph-size cache or a replacement executor.
    originals, live, calls, observed_outcomes, commands, installed, unknown = {}, {}, list(a["Calls"]), {}, set(), set(), []
    class Evidence:
        def lookup(self, namespace, key):
            require((namespace, key) in originals, "missing actual earlier command/original")
            return originals[namespace, key]
    evidence = Evidence()
    for item in p["Frames"]:
        wire, command, result = pool_exchange(item, leaders)
        commands.add(wire)
        if item["Reply"] is None:
            require(a["Recipe"]["Family"] == "move" and wire == P.raw(a["Command"]), "missing reply only belongs to actual selected native kill command; never success authority")
            unknown.append(wire)
        before, after = [pool_observation(item[name], wire, command, a, leaders) for name in ("Before", "After")]
        calls.extend((item["Before"], item["After"]))
        require(before["proof"]["UndoIndices"] == after["proof"]["UndoIndices"]
                and before["proof"]["RootIndex"] <= after["proof"]["RootIndex"], "same observed keys and monotone native view")
        require(P.raw(item["After"]["Input"]["Boundary"]) == b"".join(struct.pack(">Q",v) for v in before["proof"]["UndoIndices"]), "after read cannot silently select another native key window")
        part, move = item["Partition"], command["intent"]["move"]
        active = live.setdefault((part, move), {})
        for which, observed in (("before", before), ("after", after)):
            progress, status = observed["progress"], observed["status"]
            if which == "after":
                for seq, actual in observed["undo"].items():
                    if seq == 0: continue
                    prior = active.get(seq)
                    if actual is None:
                        require(seq > progress["total"]-progress["next"], "live before-image cannot disappear")
                    elif prior is None:
                        require(command["phase"] == 4 and command["sequence"] == seq == progress["total"]
                                and observed["frame"] is not None and actual[0] == observed["frame"]["Index"]
                                and move_status(P.raw(observed["frame"]["Outcome"]), POOLED_SEMANTIC)["result"] == 1, "only accepted actual Import adds GMU debt")
                        active[seq] = actual
                    else: require(actual == prior, "immutable undo before-image/index changed")
            for seq, actual in observed["undo"].items():
                if seq and actual is not None: require(active.get(seq) == actual, "observed undo native key differs from complete captured prefix")
            expected = range(1, progress["total"]-progress["next"]+1)
            require(all(seq in active for seq in expected) and sum(pool_undo_bound(active[seq][1], command) for seq in expected) == progress["debt"], "complete live accepted-prefix undo sum; historical GMUs may remain after undo/COMMIT")
            if status is not None and progress["imported"] and status["phase"] in (4, 5, 7): require(progress["total"] == status["sequence"], "refused attempted Import excluded from accepted total")
            frame = observed["frame"]
            if frame is not None:
                key = struct.pack(">QQ", part, frame["Index"])
                actual = rows.get((part, frame["Index"]))
                require(actual is not None and all(actual[k] == frame[k] for k in ("Outcome", "CDC")), "actual first native original/CDC prefix join")
                originals[b"move-command", key] = wire
                G.column.pooled_move_frame(G.raw, frame, fixed(observed["proof"]["PreviousRoot"], 172), POOLED_SEMANTIC, evidence)
                encoded_frame = G.raw.go_json(frame)
                previous = originals.get((b"frame", key))
                require(previous is None or previous == encoded_frame, "reopen/SAME immutable move frame changed")
                originals[b"frame", key] = encoded_frame
                observed_outcomes[part, frame["Index"]] = P.raw(frame["Outcome"])
                installed.add(wire)
        pool_transition(command, before, after, active)
        if result:
            returned = move_status(result, POOLED_SEMANTIC)
            previous = observed_outcomes.get((part, returned["index"]))
            require(previous is None or previous == result, "same returned original cannot change bytes")
            observed_outcomes[part, returned["index"]] = result
            if after["frame"] is None:
                status = move_status(result, POOLED_SEMANTIC)
                require(status["phase"] == command["phase"] == 1 and status["result"] in (2, 3)
                        and before["status"] == after["status"] and before["progress"] == after["progress"], "uninstalled fence refusal creates no original/debt authority")
            else: require(P.raw(after["frame"]["Outcome"]) == result, "actual source reply is requested installed original")
        if before["frame"] is not None: require(before["frame"] == after["frame"], "SAME command replay cannot refresh index/bytes")
    if a["Recipe"]["Family"] == "move": require(P.raw(a["Command"]) in commands, "actual native Sync selector is one captured submitted original")
    require(all(command in installed for command in unknown), "unknown selected attempt needs subsequent SAME installed original, native kill/reopen proof remains mandatory")
    for (part, index), row in rows.items():
        outcome = P.raw(row["Outcome"])
        if not outcome.startswith(b"GMO"): continue
        require(outcome.startswith(b"GMO\3"), "fresh native prefix cannot silently mix old move authority")
        status = move_status(outcome, POOLED_SEMANTIC)
        require(status["partition"] == part and 0 < status["index"] <= index and observed_outcomes.get((part,status["index"])) == outcome, "complete actual GMO3 prefix joins captured exact command original, no erased phase")
        if status["index"] != index: require(not P.raw(row["CDC"]), "SAME native move application cannot publish another activation")
    owners = {}
    for item in calls:
        response = R.response(item["Response"]); r = T.decoded(response["aux"]); q = item["Input"]
        require(type(r) is dict and set(r) == REPLY and r["Profile"] == PROFILE and r["Member"] == item["Member"]
                and fixed(r["Run"], 16, True) == fixed(a["Run"], 16, True) and r["DeadlineUnixNanos"] == a["DeadlineUnixNanos"], "all actual owner calls retain original lifetime")
        integer(r["PID"], minimum=1)
        key = item["Member"], r["PID"], response["incarnation"]
        sequence = integer(r["Sequence"], 416)
        require(sequence == q["Sequence"] and r["Action"] == q["Action"] and (q["Action"] == "inspect" or q["Cookie"] == r["Cookie"]), "actual owner call association")
        fixed(r["Cookie"], 16, True)
        spent = integer(r["Calls"],384)+integer(r["TerminalCalls"],32)
        entries = owners.setdefault(key, {})
        require(spent not in entries or entries[spent] == (q,r), "SAME cached call cannot change reply or retire a second grant")
        entries[spent] = q,r
    for entries in owners.values():
        require(sorted(entries) == list(range(1, len(entries)+1)), "complete proof/feed/inspect spent-work roster, no erased expensive call")
        calls_ = terminals = sequence = 0
        cookie = None
        for _,r in (entries[i] for i in sorted(entries)):
            terminal = r["Action"] in ("snapshot-close", "close", "release", "expire", "cancel", "pin-close")
            calls_ += not terminal; terminals += terminal
            if r["Action"] != "inspect": sequence += 1
            require(r["Sequence"] == (0 if r["Action"] == "inspect" else sequence) and (cookie is None or cookie == r["Cookie"]), "inspect spends work without advancing operation sequence; one native owner cookie")
            cookie = r["Cookie"]
            require(r["Calls"] == calls_ <= 384 and r["TerminalCalls"] == terminals <= 32
                    and r["GrantedWork"] == (calls_+terminals)*(4*(64 << 20)+2*(2 << 20)), "same finite no-refund proof/feed work pool")


def batch(a, scopes, program_roles=None):
    r = a["Recipe"]; family, phase, part, _, kind, side = recipe(r)
    command = P.raw(a["Command"]); require(0 < len(command) <= 64 << 10, "actual owned encoded command")
    selected(command, r, scopes)
    arm, receipt = a["Arm"], a["Batch"]
    member = (3 if part == 3 else 6) if program_roles is None else program_state_roles(program_roles, a["Recipe"])[1]
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


def check_calls(a, d, rows, program_roles=None):
    _, semantic = state_profile(a, program_roles)
    leaders = (1, 4) if program_roles is None else program_state_roles(program_roles, a["Recipe"])[0]
    owners, pages, acks, chain, business_pages = {}, {}, set(), [], {}
    for item in a["Calls"]:
        require(type(item) is dict and set(item) == {"Member", "Input", "Response"}
                and (item["Member"] in (2, 5) or semantic == POOLED_SEMANTIC and item["Member"] in leaders and item["Input"]["Action"] == "inspect"), "actual feed owner or pooled proof owner inspection")
        q = item["Input"]; require(type(q) is dict and set(q) == CONTROL and type(q["Version"]) is int and q["Version"] == 1, "canonical private owner control")
        response = R.response(item["Response"]); reply = T.decoded(response["aux"])
        require(type(reply) is dict and set(reply) == REPLY and reply["Profile"] == PROFILE and type(reply["Version"]) is int and reply["Version"] == 1 and reply["Member"] == item["Member"] and fixed(reply["Run"], 16, True) == fixed(a["Run"], 16, True) and reply["DeadlineUnixNanos"] == a["DeadlineUnixNanos"] and reply["Action"] == q["Action"] and reply["Sequence"] == q["Sequence"], "actual original owner/operation/deadline")
        key = (item["Member"], reply["PID"]) if semantic == SEMANTIC else (item["Member"], reply["PID"], response["incarnation"])
        last = owners.get(key, (0, 0, 0, 0))
        sequence, calls, terminals, work = (integer(reply[k], 416 if k == "Sequence" else 384 if k == "Calls" else 32 if k == "TerminalCalls" else 1 << 42) for k in ("Sequence", "Calls", "TerminalCalls", "GrantedWork"))
        require(calls >= last[1] and terminals >= last[2] and work >= last[3] and work == (calls+terminals)*(4*(64 << 20)+2*(2 << 20)), "same owner finite no-refund grants")
        if q["Action"] != "inspect": require((sequence == last[0]+1 if semantic == SEMANTIC else sequence > last[0]) and q["Cookie"] == reply["Cookie"], "actual owner sequence/cookie")
        owners[key] = sequence, calls, terminals, work
        if response["code"] != 0:
            require(reply["Result"] in ("pending-or-unavailable", "invalid", "limit", "expired", "released") and reply["Snapshot"] is None, "truthful refusal no fake snapshot")
            continue
        require(reply["Result"] == "applied", "success relabelled refusal")
        if q["Action"] == "snapshot": snapshot(reply["Snapshot"], d, a["Before"]) if semantic == SEMANTIC else snapshot(reply["Snapshot"], d, a["Before"], semantic)
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
                for f in page["frames"]: F.move_changes(f["CDC"], move_status(f["Outcome"], semantic))
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


def business(a, scopes, rows, program_roles=None):
    leaders = (1, 4) if program_roles is None else program_state_roles(program_roles, a["Recipe"])[0]
    command, manifest, lost, lost_wire = P.peer_phase(P.raw(a["LostRequest"]), P.raw(a["LostReply"]), scopes, leaders[1])
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
                _, semantic = state_profile(a, program_roles)
                _, _, native = cut(a["Before"]) if semantic == SEMANTIC else cut(a["Before"], semantic)
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


def check(a, *, program_roles=None):
    require(type(a) is dict, "D-only state object")
    _, semantic = state_profile(a, program_roles)
    family, _, part, _, _, _ = recipe(a["Recipe"])
    fixed(a["Run"], 16, True); integer(a["DeadlineUnixNanos"], (1 << 63)-1, 1)
    require(integer(a["ReceiptReads"], 129)*(64 << 10) == integer(a["ReceiptGrantBytes"], 129*(64 << 10)), "finite receipt read/decode grants remain retired")
    rows = prefix(a)
    if semantic == POOLED_SEMANTIC: pooled(a, rows, program_roles)
    if family == "allocation":
        # This supplement attributes exact native Sync; accepted P08 issuance,
        # stale recipient and exhaustion schedules remain separately accepted.
        command = P.raw(a["Command"]); c = P.Cursor(P.body(command, b"AQP\1")); phase, graph, partition = c.number(1), c.take(16), c.number()
        require(graph == P.GRAPH and phase == a["Recipe"]["Phase"] and partition == 3, "actual allocation command")
        scopes = {}  # Allocation has its own exact graph/partition command; no invented cut.
        e = batch(a, scopes); processes(a, a["Batch"])
        require(R.response(a["Business"])["outcome"] and R.response(a["BusinessReplay"])["outcome"], "actual durable recovered allocation original")
        return
    d = definition(a["Original"], a["DeadlineUnixNanos"], semantic)
    scopes, _, _ = cut(a["Before"], semantic)
    if family != "publication-race":
        cut(a["Retained"], semantic); cut(a["Reopened"], semantic)
        require(a["Before"] == a["Retained"] == a["Reopened"], "full original C exact set/revisions survive native loss/GC")
        business(a, scopes, rows, program_roles)
        refusals(a, rows)
        if family in ("move", "pin"):
            batch(a, scopes, program_roles); processes(a, a["Batch"])
        elif family == "gc":
            target = 3 if program_roles is None else program_state_roles(program_roles, a["Recipe"])[1]
            ident = S.pair(a["StateArm"], a["State"], fixed(a["Run"], 16), a["DeadlineUnixNanos"], member=target)
            require(ident["PlanID"] == a["State"]["Control"]["Filter"]["PlanID"], "actual GC cursor/bank/plan Sync")
            processes(a, a["State"])
    feed_business = check_calls(a, d, rows, program_roles)
    if family == "gc" or family == "move" and a["Recipe"]["Phase"] != 9:
        leaders = (1, 4) if program_roles is None else program_state_roles(program_roles, a["Recipe"])[0]
        _, _, lost, _ = P.peer_phase(P.raw(a["LostRequest"]), P.raw(a["LostReply"]), scopes, leaders[1])
        require(feed_business.get(lost["tx"]) == 2, "complete actual K feed envelope survives original reply loss")
        require(sum(v == 3 for v in feed_business.values()) == (1 if family == "move" else 0), "exact complete move lineage, not a half or duplicate group")

    if family in ("gc", "publication-race") or family == "move" and a["Recipe"]["Phase"] != 9: gc(a, d)


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("artifact", type=Path); args = parser.parse_args()
    check(load(args.artifact)); print("native P07-P09 exact native state joins: source artifact accepted")


if __name__ == "__main__": main()
