#!/usr/bin/env python3
"""Actual issued/published pin transitions, complete originals and old sources.

Finite diagnostic replay is not pin authority. The native parent authenticates
Resume/Open through real keepers; this bridge independently joins their raw
originals, full cuts, actual processes and literal business state.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import struct

SPEC = importlib.util.spec_from_file_location("native_pin_components", Path(__file__).with_name("native_transition_check.py"))
T = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(T)
C, P = T.C, T.C.P
require, integer, fixed = T.require, T.integer, T.fixed
PROFILE = "native-owned-pin-transition-v1"
HEADER = set("Version Profile Recipe Run DeadlineUnixNanos Gate Owners Calls Original Token Business Before Restored Retained Current Processes From Through Records".split())
GATE = set("Version Profile Cookie Run Member PID Sequence Control State DeadlineUnixNanos Proposal EntryData Target ProposalSHA256 Index Term Root Outcome CDC AttemptedWork PrepaidOutput".split())
ACTIONS = ("publish", "feed", "release", "expire", "close", "epoch", "issue", "cancel")


def interrupted(result, code, snapshot):
    require(result == "pending-or-unavailable" and type(code) is int and code in (3, 6) and snapshot is None,
            "invalid/limit/corrupt/unexpected error cannot qualify actual interruption")


def terminal(result, code, snapshot):
    require(result in ("expired", "released") and type(code) is int and code in (3, 6) and snapshot is None,
            "terminal refusal must be actual classified expired/released with no source")


def recipe(value):
    require(type(value) is dict and set(value) == {"Version", "Phase", "Partition", "Side"}, "exact pin recipe")
    integer(value["Version"], 1, 1)
    phase, partition = integer(value["Phase"], 8, 1), integer(value["Partition"], 8, 3)
    require(partition in (3, 8) and (phase != 7 or partition == 3) and value["Side"] in ("before-submit", "after-applied"), "feasible actual pin participant/phase/side")
    return phase, partition, value["Side"]


def load(path):
    digest, total, header, footer, rows = hashlib.sha256(), 0, None, None, []
    with path.open("rb") as source:
        while line := source.readline((512 << 10)+1):
            total += len(line)
            require(len(line) <= 512 << 10 and total <= 16 << 20 and line.endswith(b"\n"), "bounded native frame")
            row = T.decoded(line)
            require(type(row) is dict and footer is None, "row after EOF")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and row["Type"] == "header", "header row")
                header = row["Artifact"]
                require(type(header) is dict and set(header) == HEADER and header["Records"] is None, "fresh pin header")
            elif row.get("Type") == "record":
                require(set(row) == {"Type", "Record"} and len(rows) < 512, "native prefix admission")
                rows.append(row["Record"])
            else:
                require(set(row) == {"Type", "Rows", "Through", "PayloadSHA256"} and row["Type"] == "footer", "EOF row")
                footer = row
                require(row["Rows"] == len(rows) and row["PayloadSHA256"] == digest.hexdigest(), "complete prefix hash/census")
            if footer is None: digest.update(line)
    require(header is not None and footer is not None and footer["Through"] == header["Through"], "missing/foreign EOF")
    header["Records"] = rows
    return header


def action(original, request, phase):
    if phase == 1: return request
    value = bytearray(hashlib.sha256(b"rho-tkg:pin-terminal-action:v1\0"+bytes([phase])+original).digest()[:16])
    if not any(value) or value == request: value[0] ^= 1
    return bytes(value)


def manifest(data):
    wire = P.raw(data["Original"])
    c = P.Cursor(P.body(wire, b"GPM\1"))
    publisher, issuer, request = c.take(120), c.take(16), c.take(16)
    epoch, expires, count = c.number(), c.number(), c.number(4)
    scopes, round_ = P.cut(data["Before"])
    require(publisher == scopes[3] and any(issuer) and any(request) and epoch == 0 and expires == 1 and count == 2, "exact original publisher/native epoch/full coverage")
    parts = {}
    for partition in (3, 8):
        cert, feed, lineage = c.field(512), c.number(), c.take(32)
        body = P.body(cert, b"GSC\1")
        require(len(body) == 356 and body[:120] == scopes[partition] and cert == P.raw(data["Before"]["Cut"]["Certificates"][len(parts)])
                and feed == int.from_bytes(body[176:184], "big")+1
                and lineage == hashlib.sha256(b"rho-tkg:retained-pin-source-lineage:v1\0"+cert+struct.pack(">Q", feed)).digest(), "complete genuine certificate/source/feed lineage")
        parts[partition] = dict(certificate=cert, index=int.from_bytes(body[160:168], "big"), feed=feed)
    c.end()
    return dict(wire=wire, publisher=publisher, issuer=issuer, request=request, epoch=epoch, expires=expires, parts=parts,
                id=hashlib.sha256(b"rho-tkg:retained-graph-pin:v1\0"+publisher+issuer+request).digest(), digest=hashlib.sha256(wire).digest(), scopes=scopes, round=round_)


def outcome(encoded):
    c = P.Cursor(P.body(P.raw(encoded), b"GPO\1"))
    result = dict(phase=c.number(1), status=c.number(1), id=c.take(32), digest=c.take(32), action=c.take(16),
                  index=c.number(), publication=c.number(), epoch=c.number(), feed=c.number())
    c.end()
    require(1 <= result["phase"] <= 8 and 1 <= result["status"] <= 5 and result["index"] > 0, "actual pin result codec")
    return result


def prefix(data, m):
    records, originals = {}, {}
    for row in data["Records"]:
        require(type(row) is dict and set(row) == {"Partition", "Index", "Outcome", "CDC"}, "exact native record")
        key = integer(row["Partition"], 8, 3), integer(row["Index"], minimum=1)
        require(key[0] in (3, 8) and key not in records and len(records) < 512, "unique actual prefix")
        records[key] = row
    for i, partition in enumerate((3, 8)):
        lo, hi = integer(data["From"][i], minimum=1), integer(data["Through"][i], minimum=1)
        require(lo <= hi and hi-lo <= 256 and [row["Index"] for row in data["Records"] if row["Partition"] == partition] == list(range(lo+1, hi+1)), "complete exact native prefix")
    for (partition, index), row in records.items():
        raw, changes = P.raw(row["Outcome"]), P.raw(row["CDC"])
        if not raw:
            require(not changes, "empty original emitted changes")
            continue
        if raw[:4] == b"GSO\1":
            c = P.Cursor(P.body(raw, b"GSO\1"))
            require(c.number(1) in (1, 2) and c.number(1) == 1 and c.number() > 0 and c.number() == index and not changes, "genuine cut control")
            c.end(); continue
        if raw[:4] != b"GPO\1":
            require(raw[:4] == b"GCS\1", "unknown native control/business original")
            continue
        result = outcome(row["Outcome"])
        phase = result["phase"]
        require(result["action"] == action(m["wire"], m["request"], phase) and result["index"] <= index
                and (phase != 7 or partition == 3)
                and ((result["id"], result["digest"]) == ((bytes(32), bytes(32)) if phase == 6 else (m["id"], m["digest"]))), "same actual issuer/original/phase")
        if result["index"] < index:
            require(not changes and records.get((partition, result["index"]), {}).get("Outcome") == row["Outcome"], "replay lacks authentic earlier original")
            continue
        require((partition, phase) not in originals and result["status"] == (5 if phase == 8 else 1), "one applied immutable pin phase")
        require(changes == (raw if phase in (1, 2, 3, 4, 5, 6) else b""), "exact GPO control CDC, never business rows")
        if phase == 1: require(result["publication"] == index and result["feed"] == m["parts"][partition]["feed"], "actual first publication/index/feed")
        if phase == 2: require(result["feed"] == m["parts"][partition]["index"]+1, "actual certificate-bounded feed advance")
        if phase == 6: require(result["epoch"] == 1 and result["publication"] == result["feed"] == 0, "actual replicated epoch advance")
        originals[partition, phase] = result
    require((3, 7) in originals and all((p, 8) in originals for p in (3, 8)), "complete issuance and ordered cancellation, no absent-owner terminal shortcut")
    return records, originals


def gate(data, m, records):
    phase, partition, side = recipe(data["Recipe"])
    g = data["Gate"]
    require(type(g) is dict and set(g) == GATE and g["Profile"] == T.LOGICAL_PROFILE and g["State"] == "reached", "matched actual typed gate")
    integer(g["Version"], 1, 1); integer(g["Sequence"], 1, 1); integer(g["PID"], minimum=1)
    require(g["Member"] == (1 if partition == 3 else 4) and fixed(g["Run"], 16, True) == fixed(data["Run"], 16, True), "actual source member/run")
    fixed(g["Cookie"], 16, True)
    require(integer(g["DeadlineUnixNanos"], (1 << 63)-1, 1) <= integer(data["DeadlineUnixNanos"], (1 << 63)-1, 1), "original deadline")
    require(g["Control"] == dict(Version=1, Action="arm", Cookie=g["Cookie"], Sequence=1, Family="pin", Phase=phase, Side=side, Identity=list(action(m["wire"], m["request"], phase)), Partition=partition, Page=0), "exact genuine selected action")
    integer(g["AttemptedWork"], 1 << 50, 1)
    require(g["PrepaidOutput"] == 512 << 10, "prepaid finite observation")
    wire = P.raw(g["Proposal"])
    require(len(wire) <= 17408 and fixed(g["ProposalSHA256"], 32) == hashlib.sha256(wire).digest(), "actual complete selected proposal")
    c = P.Cursor(P.body(wire, b"GPL\1"))
    scope, actual_phase, issuer, actual_action = c.take(120), c.number(1), c.take(16), c.take(16)
    id_, digest, epoch, expected, feed, original = c.take(32), c.take(32), c.number(), c.number(), c.number(), c.field(16384)
    c.end()
    require(scope == m["scopes"][partition] == P.raw(g["Target"]) and actual_phase == phase and issuer == m["issuer"]
            and actual_action == action(m["wire"], m["request"], phase), "full actual target/issuer/action")
    require((id_, digest) == ((bytes(32), bytes(32)) if phase == 6 else (m["id"], m["digest"]))
            and expected == 0 and epoch == (1 if phase in (4, 6) else 0)
            and feed == (m["parts"][partition]["index"]+1 if phase == 2 else 0)
            and original == (m["wire"] if phase in (1, 7, 8) else b""), "complete genuine phase arguments")
    root = g["Root"]
    require(type(root) is dict and set(root) == {"Generation", "Index", "Image", "ImageHash"}, "native root grammar")
    if side == "before-submit":
        require(g["Index"] == g["Term"] == root["Generation"] == root["Index"] == 0 and not any(P.raw(g[k]) for k in ("EntryData", "Outcome", "CDC"))
                and not P.raw(root["Image"]) and fixed(root["ImageHash"], 32) == bytes(32), "unsubmitted command invented application")
    else:
        index = integer(g["Index"], minimum=1); integer(g["Term"], minimum=1); integer(root["Generation"], minimum=1)
        image = P.raw(root["Image"])
        require(root["Index"] == index and P.raw(g["EntryData"]) == wire and len(image) == 172 and image[:3] == b"GR\3" and image[4:20] == P.GRAPH
                and int.from_bytes(image[20:28], "big") == partition and hashlib.sha256(image[:-32]).digest() == image[-32:]
                and fixed(root["ImageHash"], 32) == hashlib.sha256(image).digest(), "actual native Entry/root")
        require(records.get((partition, index)) == dict(Partition=partition, Index=index, Outcome=g["Outcome"], CDC=g["CDC"]), "selected application absent from native prefix")
        result = outcome(g["Outcome"])
        require(result["index"] == index and result["phase"] == phase and result["action"] == actual_action, "selected actual original outcome")
    return g


def projection(snapshot, edge, round_):
    require(type(snapshot) is dict and set(snapshot) == {"Cut", "Entities", "Facts"} and len(snapshot["Entities"]) == 4 and len(snapshot["Facts"]) == 32, "complete actual small graph census")
    require([e["id"] for e in snapshot["Entities"]] == [1, 3, 7, 17], "literal entity read domain")
    wanted = {1, 3, 17} | ({7} if edge else set())
    result = {}
    for id_, f in enumerate(snapshot["Facts"], 1):
        require(type(f) is dict and set(f) == set("Exists Active Life Source Target Labels Incident".split()) and type(f["Exists"]) is type(f["Active"]) is bool, "actual typed point fact")
        require(f["Exists"] == f["Active"] == (id_ in wanted) and f["Life"] == (id_+1 if id_ in wanted else 0)
                and (f["Source"], f["Target"]) == ((1, 17) if id_ == 7 and edge else (0, 0))
                and not f["Labels"] and (f["Incident"] or []) == ([7] if edge and id_ in (1, 17) else []), "literal complete life/endpoint/posting/phantom answer")
        if id_ in wanted: result[("B" if id_ == 17 else "A")+f"/fact/{id_}"] = dict(f, Labels=[], Incident=f["Incident"] or [])
    for e in snapshot["Entities"]:
        id_ = e["id"]
        require(e["found"] is (id_ in wanted), "actual entity found flag")
        if id_ not in wanted:
            require(e["kind"] == e["source"] == e["target"] == e["mode"] == e["effectiveLife"] == 0 and not e["effective"] and not e["type"] and not e["presence"]["present"] and not e["property"]["present"], "absent edge invented current fields")
            continue
        require(e["kind"] == (2 if id_ == 7 else 1) and (e["source"], e["target"]) == ((1, 17) if id_ == 7 else (0, 0))
                and e["mode"] == (1 if id_ == 7 else 0) and e["type"] == ("edge" if id_ == 7 else "")
                and e["effective"] is True and e["effectiveLife"] == id_+1 and not e["labels"], "literal entity/life binding")
        presence = e["presence"]
        require(presence["present"] is True and presence["null"] is False and presence["handle"] == id_+1 and presence["kind"] == "life"
                and presence["revision"] > 0 and presence["provenance"] == {1: 1, 3: 2, 17: 3, 7: 4}[id_], "actual original presence revision/provenance")
        if id_ == 7:
            require(presence["revision"] == round_ and e["property"] == dict(present=True, null=False, handle=5, revision=round_, provenance=4, kind="i64", i64=5), "literal scalar5, no later/current substitution")
        result[("B" if id_ == 17 else "A")+f"/entity/{id_}"] = e
    return result


def processes(data, g):
    rows = data["Processes"]
    require(type(rows) is list and len(rows) == 12 and rows[0]["Member"] == g["Member"], "actual failed source/full cohort replacement")
    killed, reopened = {}, {}
    for row in rows[:6]:
        raw = T.decoded(P.raw(row["Raw"]))
        id_ = integer(row["Member"], 6, 1)
        require(id_ not in killed and row["Action"] == "kill" and row["Reaped"] is True and row["Signal"] == "SIGKILL" and raw["Member"] == id_ and raw["Instance"] == row["Instance"] and raw["Reaped"] is True and raw["Signaled"] is True and raw["SignalNumber"] == 9, "actual unique OS-child kill/reap")
        killed[id_] = row, raw
    require(killed[g["Member"]][1]["PID"] == g["PID"], "selected actual gate source process")
    for row in rows[6:]:
        raw = T.decoded(P.raw(row["Raw"]))
        id_ = integer(row["Member"], 6, 1)
        require(id_ not in reopened and row["Action"] == "reopen" and row["Ready"] is True and raw["Member"] == id_ and raw["PID"] != killed[id_][1]["PID"]
                and row["PreviousInstance"] == raw["PreviousInstance"] == killed[id_][0]["Instance"] and row["Volume"] == raw["Volume"] == killed[id_][0]["Volume"]
                and raw["ReadyKind"] == "native-restored-before-campaign" and raw["RecoveryRaw"] == row["RecoveryRaw"], "same native directory/profile Restore, no invented quorum ACK")
        restored = C.process_response(P.raw(row["RecoveryRaw"]))
        require(restored["code"] == restored["mask"] == 0 and row["Instance"] == f'voter{id_}/pid{raw["PID"]}/{restored["incarnation"].hex()}', "actual restored process frame/incarnation")
        reopened[id_] = row, raw
    require(set(killed) == set(reopened) == set(range(1, 7)), "complete owned process census")
    return killed, reopened


def calls(data, m, phase, killed, reopened):
    require(type(data["Owners"]) is list and len(data["Owners"]) == 2 and type(data["Calls"]) is list and len(data["Calls"]) <= 32, "finite two-owner method budget")
    owners, cumulative = {}, 0
    for o, pair in zip(data["Owners"], (killed[2], reopened[2])):
        row, actual = pair
        pid = actual["PID"]
        require(type(o) is dict and set(o) == GATE and o["Member"] == 2 and o["PID"] == pid and o["State"] == "idle" and o["Profile"] == T.LOGICAL_PROFILE
                and fixed(o["Run"], 16, True) == fixed(data["Run"], 16, True) and integer(o["DeadlineUnixNanos"], (1 << 63)-1, 1) <= data["DeadlineUnixNanos"], "actual bounded original owner lifetime")
        cookie = fixed(o["Cookie"], 16, True)
        require(cookie not in owners, "replacement reused live cookie")
        spent = integer(o["AttemptedWork"], 1 << 50, 1); cumulative += spent
        identity = row["Instance"].split("/")
        require(len(identity) == 3 and identity[:2] == ["voter2", f"pid{pid}"] and len(identity[2]) == 32, "actual method owner process identity")
        owners[cookie] = dict(sequence=0, work=0, maximum=spent, incarnation=identity[2], ipc=0, lastInput=None, lastAux=None)
    require(cumulative <= 1 << 51, "checked cumulative owner grant, no restart refund")
    pending, refused, begun, resumes = 0, [], 0, 0
    for call in data["Calls"]:
        require(set(call) == {"Member", "Input", "Response"} and call["Member"] == 2, "actual factory caller")
        wire = P.raw(call["Input"]); require(len(wire) <= 32768 and wire[:4] == b"GTP1", "bounded diagnostic control")
        q = T.decoded(wire[4:])
        require(type(q) is dict and set(q) == {"Version", "Cookie", "Sequence", "Action", "Original", "Token"} and q["Version"] == 1, "exact request grammar")
        integer(q["Version"], 1, 1)
        cookie = fixed(q["Cookie"], 16, True); require(cookie in owners, "foreign method owner")
        owner = owners[cookie]
        sequence = integer(q["Sequence"], 16, 1)
        cached = sequence == owner["sequence"]
        require(sequence == owner["sequence"]+1 or cached and wire == owner["lastInput"], "no skipped/replaced method sequence")
        if not cached: owner["sequence"] += 1
        response = C.process_response(P.raw(call["Response"]))
        require(response["incarnation"].hex() == owner["incarnation"] and response["sequence"] > owner["ipc"], "same actual source-process frame/IPC sequence")
        owner["ipc"] = response["sequence"]
        r = T.decoded(response["aux"])
        require(set(r) == set("Version Cookie Sequence Action Original Token Actions Result Snapshot AttemptedWork".split()) and r["Version"] == 1 and r["Cookie"] == q["Cookie"] and r["Sequence"] == q["Sequence"] and r["Action"] == q["Action"] and P.raw(r["Original"]) == m["wire"], "same actual original/correlated method result")
        work = integer(r["AttemptedWork"], owner["maximum"], 1); require(work >= owner["work"], "source attempt grant reset"); owner["work"] = work
        require(r["Actions"] == [list(action(m["wire"], m["request"], p)) for p in range(1, 9)], "canonical same-original actions")
        require(not P.raw(r["Token"]) or r["Token"] == data["Token"], "foreign diagnostic token")
        if cached:
            require(response["aux"] == owner["lastAux"], "replay replaced last genuine method reply")
            continue
        owner["lastInput"], owner["lastAux"] = wire, response["aux"]
        if q["Action"] == "begin":
            begun += 1
            request = bytearray(response["incarnation"]); request[8:] = struct.pack(">Q", response["sequence"])
            require(m["request"] == request and not P.raw(q["Original"]) and not P.raw(q["Token"]) and not P.raw(r["Token"]), "actual born factory request, no publication authority")
        elif q["Action"] == "resume":
            resumes += 1; require(P.raw(q["Original"]) == m["wire"] and (not P.raw(q["Token"]) or q["Token"] == data["Token"]), "inert Resume must name exact original/token")
        else: require(not P.raw(q["Original"]) and not P.raw(q["Token"]), "method supplied replacement authority")
        if r["Result"] == "pending-or-unavailable":
            pending += 1; require(q["Action"] == ACTIONS[phase-1], "unknown method invented result")
            interrupted(r["Result"], response["code"], r["Snapshot"])
        elif r["Result"] in ("expired", "released"):
            refused.append(r["Result"]); require(q["Action"] == "pin-read", "unproved token refusal")
            terminal(r["Result"], response["code"], r["Snapshot"])
        else:
            require(r["Result"] == "applied" and response["code"] == response["mask"] == 0, "actual method error labelled success")
        if r["Result"] == "applied" and q["Action"] in ("pin-read", "at-read", "fresh-read"):
            require(r["Snapshot"] == data["Current" if q["Action"] == "fresh-read" else "Before"], "actual method source differs from reported answer")
        else: require(r["Snapshot"] is None, "non-read diagnostic invented source")
    require(begun == 1 and resumes == 2 and pending == 1, "one genuine Begin, bounded original recovery and pending census")
    require(refused == (["expired" if phase in (4, 6) else "released"] if phase in (3, 4, 5, 6) else []), "explicit expired vs released vs unavailable")


def check(data):
    require(type(data) is dict and set(data) == HEADER and data["Version"] == 1 and data["Profile"] == PROFILE, "fresh actual pin artifact")
    integer(data["Version"], 1, 1)
    phase, partition, side = recipe(data["Recipe"])
    m = manifest(data)
    records, pin_originals = prefix(data, m)
    g = gate(data, m, records)
    killed, reopened = processes(data, g)
    calls(data, m, phase, killed, reopened)
    token = P.raw(data["Token"])
    if phase == 8:
        require(not token and all((p, 1) not in pin_originals for p in (3, 8)), "unpublished cancellation invented portable authority")
    else:
        c = P.Cursor(P.body(token, b"GPT\1")); require(c.field(16384) == m["wire"] and c.number(4) == 2, "same complete portable original")
        for p in (3, 8): require((p, 1) in pin_originals and c.number() == pin_originals[p, 1]["publication"], "actual all-participant publication originals")
        c.end()
    b = data["Business"]
    require(b["Engine"] == "native-raft" and b["Classification"] == "commit" and b["ClientID"] == 0 and b["AttemptOrdinal"] == 1 and fixed(b["PreviousRequest"], 16) == bytes(16), "one actual quiet-fixture immutable business attempt")
    original = C.decision_manifest(P.raw(b["Proposal"]), P.raw(b["Manifest"]), 3, m["scopes"], data["Before"])
    require(b["Operation"] == dict(Dataset=list(hashlib.sha256(bytes([1])+struct.pack(">QQ", 5, 100)+b"graph-v2").digest()), Phase="create", Fact=0, Ordinal=4)
            and 1 <= integer(b["SubmitInvocations"], 4, 1) <= 4
            and fixed(b["BusinessSHA256"], 32) == P.business_hash(P.raw(b["Proposal"]), 3), "actual fixed useful operation/whole invocation allowance")
    require(fixed(b["Request"], 16, True) == original["source"]["request"] and fixed(b["Transaction"], 16, True) == original["tx"]
            and fixed(b["BodySHA256"], 32) == hashlib.sha256(P.raw(b["Proposal"])).digest() and fixed(b["DecisionDigest"], 32) == original["digest"], "complete original request/body/manifest")
    statuses = {}
    for key, row in records.items():
        raw, changes = P.raw(row["Outcome"]), P.raw(row["CDC"])
        if raw[:4] != b"GCS\1": continue
        s = P.status(raw)
        require(s["partition"] == key[0] and s["tx"] == original["tx"] and s["digest"] == original["digest"] and s["index"] <= key[1], "unknown business original")
        if s["index"] < key[1]: require(not changes and records.get((key[0], s["index"]), {}).get("Outcome") == row["Outcome"], "authentic earlier business replay"); continue
        require((key[0], s["phase"]) not in statuses and (s["phase"] != 2 or s["yes"]) and (s["phase"] < 3 or s["commit"]), "complete applied business phase")
        require(bool(changes) == (s["phase"] == 4), "exact business CDC phase")
        if changes:
            c = P.Cursor(P.body(changes, b"GCE\1")); require(c.number(1) == 6 and c.take(16) == original["tx"] and c.take(32) == original["digest"] and c.number() == key[1], "actual original CDC identity")
            logical = P.body(c.field(), b"GCD\5"); c.end(); require(logical[:16] == P.GRAPH and int.from_bytes(logical[16:24], "big") == key[0], "actual CDC namespace")
        statuses[key[0], s["phase"]] = s
    require(set(statuses) == {(3, 1), (3, 2), (8, 2), (3, 3), (3, 4), (8, 4)}, "complete original protocol census")
    decision = statuses[3, 3]
    require(P.raw(b["Outcome"]) == P.raw(records[3, decision["index"]]["Outcome"]) and not P.raw(b["CDC"]) and b["OriginalIndex"] == decision["index"] and b["Round"] == decision["round"] > m["round"]
            and statuses[3, 4]["round"] == statuses[8, 4]["round"] == decision["round"], "actual immutable decision and both effects")
    require(data["Before"] == data["Restored"] == data["Retained"], "old native source replaced after later transaction/restart")
    initial = projection(data["Before"], False, decision["round"])
    current = projection(data["Current"], True, decision["round"])
    keys = ("A/entity/7", "A/fact/7", "A/fact/1", "B/fact/17")
    transactions = {"edge": dict(reads=[P.read_key(initial, k) for k in ("A/entity/1", "B/entity/17", "A/entity/7")], writes=P.writes(initial, current, keys))}
    observations = [dict(event=name, scope=["A", "B"], round=P.cut(data[name])[1], values=projection(data[name], name == "Current", decision["round"])) for name in ("Before", "Restored", "Retained", "Current")]
    witness, detail = P.oracle()({"initial": initial, "transactions": transactions}, {"edge"}, {"edge": decision["round"]}, observations, [dict(event="final", scope=["A", "B"], values=current)])
    require(witness is not None, "independent literal serial/old-cut witness: "+str(detail))
    return dict(profile=PROFILE, recipe=data["Recipe"], records=len(records), serialWitness=witness,
                scope="thirty feasible native pin-call transition cells; genuine cohort replacement and authenticated original recovery; no physical IO attribution, new-cut publication or runtime qualification")


def main():
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("artifact", type=Path)
    args = parser.parse_args(); print(json.dumps(check(load(args.artifact)), indent=2))


if __name__ == "__main__": main()
