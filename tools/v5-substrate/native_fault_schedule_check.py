#!/usr/bin/env python3
"""Bounded native fault raw-history bridge; no reducer or process simulator.

The Go owner captures real pipes, native reads, matched IO and OS reaping. This
checker binds that finite capture and the existing independent serial search;
JSON alone grants no physical-failure, consensus or power-loss authority.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import struct

SPEC = importlib.util.spec_from_file_location("native_fault_p06", Path(__file__).with_name("check-p06-history.py"))
P = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(P)
PROFILE = "native-composed-graph-faults-v1"
DECISION_PROFILE = "native-queued-original-decisions-v1"
IO_KINDS = {"short-write": "short write", "write-eio": "input/output error",
            "write-enospc": "no space left on device", "file-sync": "input/output error",
            "directory-sync": "input/output error"}
CASES = {"partial-install-decision-loss", "directory-sync-during-reopen", "frozen-conflict-original-recovery"}
CASES |= {"wal-" + k for k in IO_KINDS if k != "directory-sync"}
CASES |= {f"packet-{k}-0ms" for k in ("drop", "duplicate", "reorder", "partition")}
CASES |= {f"packet-delay-{n}ms" for n in (1, 20, 100)}


def require(value, message):
    if not value:
        raise ValueError("native fault refused: " + message)


def load(path):
    """Stream the exact fixed-case envelope, including its terminal EOF seal."""
    digest, size, records, header, footer = hashlib.sha256(), 0, [], None, None
    with path.open("rb") as source:
        while line := source.readline((512 << 10) + 1):
            size += len(line)
            require(len(line) <= 512 << 10 and size <= 16 << 20 and line.endswith(b"\n"), "artifact byte bound")
            row = json.loads(line)
            require(type(row) is dict and footer is None, "row after terminal EOF")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and row["Type"] == "header", "header grammar")
                header = row["Artifact"]
                require(header["Records"] is None, "header cannot inline an unbounded prefix")
            elif row.get("Type") == "record":
                require(set(row) == {"Type", "Record"} and len(records) < 512, "record grammar/cap")
                records.append(row["Record"])
            else:
                require(set(row) == {"Type", "Rows", "Through", "PayloadSHA256"} and row["Type"] == "footer", "terminal grammar")
                footer = row
                require(row["Rows"] == len(records) and row["PayloadSHA256"] == digest.hexdigest(), "terminal census/hash")
            if footer is None:
                digest.update(line)
    require(header is not None and footer is not None and footer["Through"] == header["Through"], "missing/foreign terminal EOF")
    header["Records"] = records
    return header


def process_response(wire):
    c = P.Cursor(wire)
    incarnation = c.take(16)
    c.take(28 * 8 + 32 + 32 + 8)
    handles = c.number(4)
    require(handles <= 2, "process handle cap")
    c.take(8 * handles)
    aux, ready, sequence, applied, code, mask = c.field(), c.number(1), c.number(), c.number(), c.number(1), c.number(2)
    detail = c.field(4096)
    require(any(incarnation) and sequence > 0 and ready <= 1 and mask < 1 << 16, "process response identity")
    require(c.number(4) == 0 and c.number(4) == 0, "unexpected packets/reads in certified refusal")
    image, outcome, changes = c.field(), c.field(), c.field()
    flags, life, labels = c.number(1), c.number(), c.number(4)
    require(flags <= 3 and labels <= 16, "process fact bounds")
    for _ in range(labels):
        c.field(256)
    c.field(256)
    c.end()
    return dict(incarnation=incarnation, aux=aux, sequence=sequence, applied=applied,
                code=code, mask=mask, detail=detail, image=image, outcome=outcome, changes=changes, life=life)


def partial_reads(rows, tx, coordinator):
    require([r["Mode"] for r in rows] == ["Fresh", "After"], "partial Fresh/After inventory")
    for row, mode in zip(rows, (1, 4)):
        c = P.Cursor(P.body(P.raw(row["Request"]), b"GEQ\1"))
        actual_mode, actual_tx, actual_coordinator, cuts = c.number(1), c.take(16), c.number(), c.number(4)
        c.end()
        require(actual_mode == mode and cuts == 0 and actual_tx == (tx if mode == 4 else bytes(16)) and actual_coordinator == (coordinator if mode == 4 else 0), "certified request binding")
        response = process_response(P.raw(row["Response"]))
        # This is processResponse.causeMask, not the outer graph-wire mask.
        # processCauseMask bit7 is replica.ErrUnavailable; bit3 is ErrLimit.
        # No additional/unknown cause may qualify as the expected outage.
        require(row["Classification"] == "unavailable" and response["code"] == 3 and response["mask"] == 1 << 7 and
                not response["aux"] and not response["outcome"] and not response["changes"], "partial read invented success or timeout authority")


def io_receipt(receipt, case):
    require(type(receipt) is dict and receipt["Version"] == 1 and receipt["Profile"] == PROFILE, "IO profile")
    require(type(receipt["Cookie"]) is list and len(receipt["Cookie"]) == 16 and any(receipt["Cookie"]) and all(type(n) is int and 0 <= n <= 255 for n in receipt["Cookie"]), "IO owner cookie")
    kind, op = receipt["Kind"], receipt["IO"]
    require(kind in IO_KINDS and receipt["Member"] == 1 and receipt["PID"] > 0 and receipt["Sequence"] == receipt["Arms"] == receipt["Nth"] == receipt["Seen"] == 1 and receipt["Matched"] is True and receipt["Armed"] is False, "unmatched/foreign IO owner")
    require(op["Error"] == IO_KINDS[kind] and 0 <= op["Written"] <= op["Requested"] <= 2 << 20, "IO result")
    for field in ("InputSHA256", "WrittenSHA256"):
        require(len(op[field]) == 32 and all(type(n) is int and 0 <= n <= 255 for n in op[field]), "IO digest")
    if case == "directory-sync-during-reopen":
        require(kind == "directory-sync" and op["Directory"] is True and op["Kind"] == "sync" and op["Path"] == ".", "not actual directory-handle Sync")
    else:
        require(case == "wal-" + kind and op["Directory"] is False and op["Path"].endswith(".log") and not Path(op["Path"]).is_absolute() and ".." not in Path(op["Path"]).parts, "foreign WAL operation")
        require(op["Kind"] == ("sync" if kind == "file-sync" else "write"), "IO method")
    if kind == "short-write":
        require(op["Requested"] > 0 and op["Written"] + 1 == op["Requested"], "not actual short prefix")
    else:
        require(op["Written"] == 0, "failed IO claimed native write")


def processes(rows, receipt, case):
    if receipt is None:
        if case != "partial-install-decision-loss":
            require(not rows, "unexpected process events")
            return
        require(len(rows) == 2, "decision leader loss/rejoin census")
        killed, reopened = rows
        actual = json.loads(P.raw(killed["Raw"]))
        require(killed["Action"] == "kill" and killed["Member"] == 4 and killed["Reaped"] is True and killed["Signal"] == "SIGKILL" and actual["SignalNumber"] == 9 and actual["Signaled"] is True and actual["Reaped"] is True, "actual decision-leader kill")
    else:
        io_receipt(receipt, case)
        require(len(rows) == (3 if case == "directory-sync-during-reopen" else 2), "native failure/reopen census")
        failed, reopened = rows[-2:]
        actual = json.loads(P.raw(failed["Raw"]))
        require(failed["Action"] == "native-io-failure" and failed["Reaped"] is True and failed["Member"] == actual["Member"] == receipt["Member"] and actual["PID"] == receipt["PID"] and actual["ExitCode"] == 1 and actual["Signaled"] is False and actual["Reaped"] is True and actual["Receipt"] == receipt and actual["Volume"] == failed["Volume"] and receipt["IO"]["Error"] in actual["Stderr"] + actual["Stdout"], "unreaped/unmatched native failure")
        killed = rows[0]
        if case == "directory-sync-during-reopen":
            os_kill = json.loads(P.raw(killed["Raw"]))
            require(killed["Action"] == "kill" and killed["Member"] == 1 and os_kill["SignalNumber"] == 9 and os_kill["Reaped"] is True and killed["Volume"] == failed["Volume"], "same-directory pre-reopen kill")
    restored = json.loads(P.raw(reopened["Raw"]))
    require(reopened["Action"] == "reopen" and reopened["Ready"] is True and reopened["PreviousInstance"] == killed["Instance"] and reopened["Volume"] == killed["Volume"] and restored["PreviousInstance"] == killed["Instance"] and restored["Volume"] == killed["Volume"] and restored["RecoveryRaw"] == reopened["RecoveryRaw"], "actual same-volume replacement boundary")


def check(data):
    if data.get("Profile") == DECISION_PROFILE:
        return decision_check(data)
    case = data["Case"]
    require(data["Version"] == 1 and data["Profile"] == PROFILE and case in CASES, "fresh finite case contract")
    require((data["IO"] is not None)==(case.startswith("wal-") or case=="directory-sync-during-reopen"),"required actual IO receipt")
    require(bool(data["PartialReads"])==(case=="partial-install-decision-loss"),"partial certification inventory")
    return check_history(data, startup=case == "directory-sync-during-reopen")


def check_history(data, *, startup=False, process_check=processes, semantic_require=require):
    """Shared complete original/prefix/serial checks after strict case admission.

    A fresh embedding must authenticate its own actual process/IO grammar. It
    supplies the real capture unchanged, never substituted legacy receipts.
    """
    case = data["Case"]
    scopes, old_round = P.cut(data["Before"])
    for key in ("Retained", "Reopened"):
        if semantic_require is require:
            require(data[key] == data["Before"], "old cut substituted current state")
    source, manifest, tx, digest, coordinator = None, P.raw(data["Manifest"]), None, None, None
    if not startup:
        source = P.pgq(P.raw(data["Original"]))
        m = P.Cursor(P.body(manifest, b"GCM\1"))
        tx, coordinator, original_wire = m.take(16), m.number(), m.field()
        original = P.pgq(original_wire)
        require(coordinator in (3, 8) and original["partition"] == coordinator and original["request"] == source["request"] and original["configuration"] == source["configuration"] and original["declaration"] == source["declaration"] and original["routing"] == source["routing"], "original identity/configuration")
        # The immutable original frame is already sealed; preserve its complete
        # business bytes while normalizing only coordinator/revision as Run does.
        require(P.business_hash(P.raw(data["Original"]), coordinator) == P.business_hash(original_wire), "complete original operations/claims/axes/provenance")
        configuration,declaration,routing,template=m.take(32),m.take(32),m.take(32),m.take(32)
        require((configuration,declaration,routing)==(original["configuration"],original["declaration"],original["routing"]) and any(template) and m.number(4)==2,"manifest full configuration/participant set")
        for partition in (3,8):
            actual_partition,floor,kernel=m.number(),m.number(),P.Cursor(P.body(m.field(),b"GPK\1"))
            require(actual_partition==partition and kernel.take(16)==tx and kernel.take(16)==P.GRAPH and kernel.number()==partition and kernel.number()>0,"original kernel namespace/ownership")
            require(tuple(kernel.take(32) for _ in range(4))==(configuration,declaration,routing,template),"original kernel configuration/template")
            effect=kernel.take(32)
            require(kernel.number()>floor,"kernel provisional round/floor")
            kernel.number()  # Original provenance is also bound by business_hash.
            require(bool(kernel.field()),"kernel native axis")
            require(hashlib.sha256(kernel.field()).digest()==effect,"kernel actual effect digest")
            kernel.end()
        m.end()
        for encoded in data["Before"]["Cut"]["Certificates"]:
            certificate=P.body(P.raw(encoded),b"GSC\1")
            require(certificate[120:152]==configuration,"original/cut configuration")
        require(tx == hashlib.sha256(b"rho-tkg:cross-transaction-id:v1\0" + P.GRAPH + struct.pack(">Q", coordinator) + original["request"]).digest()[:16], "qualified original TxID")
        digest = hashlib.sha256(manifest).digest()
    else:
        require(not P.raw(data["Original"]) and not manifest, "startup failure claimed graph command")
    records, phases, decision, resolves, resolve_rounds = {}, set(), None, set(), set()
    for record in data["Records"]:
        key = record["Partition"], record["Index"]
        require(key[0] in (3, 8) and key not in records, "duplicate/foreign physical entry")
        records[key] = record
    require(len(records) <= 512, "finite prefix cap")
    for i, partition in enumerate((3, 8)):
        lo, hi = data["From"][i], data["Through"][i]
        require(type(lo) is int and type(hi) is int and 0 < lo <= hi < 1 << 64 and hi-lo <= 256, "actual prefix range")
        require([r["Index"] for r in data["Records"] if r["Partition"] == partition] == list(range(lo+1, hi+1)), "incomplete actual prefix")
    refused = data.get("Refused")
    require((refused is not None)==(case=="frozen-conflict-original-recovery"),"frozen conflict original inventory")
    if refused is not None:
        proposal = P.pgq(P.raw(refused["Proposal"]))
        o = P.body(P.raw(refused["Outcome"]), b"GRO\4")
        require(case == "frozen-conflict-original-recovery" and len(o) == 84 and o[1:17] == P.GRAPH and o[25:41] == proposal["request"] and o[41:73] == hashlib.sha256(P.raw(refused["Proposal"])).digest() and int.from_bytes(o[73:81],"big") == refused["Index"] and int.from_bytes(o[82:84],"big") == 11 and not P.raw(refused["CDC"]), "second frozen original refusal")
        replay = data["RefusedReplay"]
        require(replay["Index"] > refused["Index"] and replay["Proposal"] == refused["Proposal"] and replay["Outcome"] == refused["Outcome"] and not P.raw(replay["CDC"]), "refused original changed after recovery")
        require(records[(refused["Partition"],refused["Index"])]["Outcome"] == refused["Outcome"] and records[(replay["Partition"],replay["Index"])]["Outcome"] == refused["Outcome"], "original refusal/replay not in actual prefix")
    for record in data["Records"]:
        outcome, cdc = P.raw(record["Outcome"]), P.raw(record["CDC"])
        if not outcome:
            require(not cdc, "output missing original")
            continue  # Real successful native ApplicationRecordGet captured empty.
        if outcome[:4] == b"GSO\1":
            control = P.body(outcome,b"GSO\1")
            require(len(control)==18 and 1<=control[0]<=2 and 1<=control[1]<=4 and int.from_bytes(control[2:10],"big")>0 and int.from_bytes(control[10:18],"big")==record["Index"] and not cdc,"control outcome binding")
            continue
        if outcome[:4] == b"GRO\4":
            require(refused is not None and outcome==P.raw(refused["Outcome"]) and record["Partition"]==refused["Partition"] and record["Index"] in (refused["Index"],data["RefusedReplay"]["Index"]) and not cdc,"unknown local output")
            continue
        s=P.status(outcome)
        require(tx is not None and s["partition"]==record["Partition"] and s["tx"]==tx and s["digest"]==digest and s["index"]<=record["Index"],"unknown cross original")
        if s["index"] < record["Index"]:
            require(not cdc and records.get((s["partition"],s["index"]),{}).get("Outcome")==record["Outcome"],"replay lacks retained earlier original")
            continue
        key=s["partition"],s["phase"]
        require(key not in phases,"duplicate original phase")
        phases.add(key)
        require(s["yes"] if s["phase"]==2 else True,"unexpected NO")
        if s["phase"]==3:
            require(s["commit"] and s["round"]>old_round,"new genuine COMMIT")
            decision=s
        if s["phase"]==4:
            require(s["commit"] and bool(cdc),"missing complete participant CDC")
            resolves.add(s["partition"])
            resolve_rounds.add(s["round"])
        else:
            require(not cdc,"control phase business output")
        if cdc:
            envelope=P.Cursor(P.body(cdc,b"GCE\1")); envelope.take(1)
            require(envelope.take(16)==tx and envelope.take(32)==digest and envelope.number()==record["Index"],"CDC original identity")
            logical=P.body(envelope.field(),b"GCD\5"); envelope.end()
            require(logical[:16]==P.GRAPH and int.from_bytes(logical[16:24],"big")==record["Partition"],"CDC namespace")
    transactions, rounds = {}, {}
    initial, current = P.projected(data["Before"]),P.projected(data["Recovered"])
    if tx is not None:
        require(decision is not None and resolves=={3,8} and resolve_rounds=={decision["round"]} and phases=={(coordinator,1),(3,2),(8,2),(coordinator,3),(3,4),(8,4)},"incomplete genuine protocol prefix")
        keys=("A/entity/3","A/presence/3","A/labels/3","A/property/3/p","A/life/3/4","A/posting/1/0","A/posting/1/2","B/posting/17/0","B/posting/17/18")
        transactions["edge"]={"reads":[P.read_key(initial,k) for k in ("A/presence/1","B/presence/17","A/entity/3")],"writes":P.writes(initial,current,keys)}
        rounds["edge"]=decision["round"]
        if case=="partial-install-decision-loss":
            _, actual_manifest, installed,installed_outcome=P.peer_phase(P.raw(data["InstalledA"]["Request"]),P.raw(data["InstalledA"]["Reply"]),scopes,1)
            require(actual_manifest==manifest and installed["phase"]==4 and installed["commit"] and installed["tx"]==tx and installed["round"]==decision["round"] and P.raw(records[(3,installed["index"])]["Outcome"])==installed_outcome,"actual installed A")
            phase,partition,held_manifest=P.gct(_source_command(data["WithheldB"]["Request"],scopes))
            require(phase==4 and partition==8 and held_manifest==manifest,"withheld B original")
            partial_reads(data["PartialReads"],tx,coordinator)
            P.resumed(data["PendingCaller"],scopes,source["request"],tx,P.business_hash(P.raw(data["Original"]),coordinator),1,b"")
    else:
        require(initial==current,"startup fault changed useful graph")
    if semantic_require is not require:
        for key in ("Retained", "Reopened"):
            semantic_require(data[key] == data["Before"], "old-cut:" + key)
    observations=[{"event":key,"scope":["A","B"],"round":P.cut(data[key])[1],"values":P.projected(data[key])} for key in ("Before","Retained","Recovered","Reopened")]
    witness,detail=P.oracle()({"initial":initial,"transactions":transactions},set(transactions),rounds,observations,[{"event":"final","scope":["A","B"],"values":current}])
    semantic_require(witness is not None,"independent exact serial/cut witness: "+str(detail))
    process_check(data["Processes"] or [],data["IO"],case)
    events=data["PacketEvents"] or []
    require(bool(events)==case.startswith("packet-"),"matched packet inventory")
    require(len(events)<=128,"packet event bound")
    for e in events:
        require(e["From"]==1 and e["To"]==2 and e["Ordinal"]>=1 and 0<e["Bytes"]<=2<<20 and len(e["SHA256"])==32 and case==f'packet-{e["Action"]}-{e["ConfiguredDelayMillis"]}ms',"packet identity/schedule")
        require(e["ObservedDelayNanos"]>=e["ConfiguredDelayMillis"]*1_000_000 if e["Action"]=="delay" else e["ObservedDelayNanos"]==0,"actual configured delay")
    return {"case":case,"records":len(records),"serial_witness":witness,"scope":"fixed actual-source bridge; source-only until real process qualification; no power-loss or capacity claim"}


def _source_command(encoded,scopes):
    c=P.Cursor(P.body(P.raw(encoded),b"GSQ\1"))
    require(c.number(1)==8 and c.number()>0 and any(c.take(16)),"withheld source request")
    c.take(48); c.field();c.field();c.field()
    proposal=P.Cursor(P.body(c.field(),b"GSC\1"));target=P.body(proposal.field(156),b"APS\1");command=proposal.field();proposal.end()
    c.take(17);c.end()
    _,partition,_=P.gct(command)
    require(target==scopes[partition],"withheld authenticated source scope")
    return command


def decision_manifest(source_wire, manifest, coordinator, scopes, before):
    """Parse the real immutable original, including both typed effect kernels."""
    source = P.pgq(source_wire)
    m = P.Cursor(P.body(manifest, b"GCM\1"))
    tx, actual_coordinator, original_wire = m.take(16), m.number(), m.field()
    original = P.pgq(original_wire)
    require(actual_coordinator == coordinator == original["partition"] and original["request"] == source["request"], "queued original request/coordinator")
    require(P.business_hash(source_wire, coordinator) == P.business_hash(original_wire), "queued complete original business")
    configuration, declaration, routing, template = (m.take(32) for _ in range(4))
    require((configuration, declaration, routing) == (source["configuration"], source["declaration"], source["routing"]) and any(template) and m.number(4) == 2, "queued complete configuration/participants")
    floors = {}
    for partition in (3, 8):
        actual, floor, k = m.number(), m.number(), P.Cursor(P.body(m.field(), b"GPK\1"))
        floors[partition] = floor
        require(actual == partition and k.take(16) == tx and k.take(16) == P.GRAPH and k.number() == partition and k.number() == int.from_bytes(scopes[partition][40:48], "big"), "queued kernel ownership/namespace")
        require(tuple(k.take(32) for _ in range(4)) == (configuration, declaration, routing, template), "queued kernel configuration/template")
        effect = k.take(32)
        require(k.number() > floor, "queued provisional revision")
        k.number()  # Provenance and native axes remain bound by the original.
        require(bool(k.field()) and hashlib.sha256(k.field()).digest() == effect, "queued kernel native axis/effect")
        k.end()
    m.end()
    for encoded in before["Cut"]["Certificates"]:
        require(P.body(P.raw(encoded), b"GSC\1")[120:152] == configuration, "queued cut configuration")
    expected = hashlib.sha256(b"rho-tkg:cross-transaction-id:v1\0" + P.GRAPH + struct.pack(">Q", coordinator) + source["request"]).digest()[:16]
    require(tx == expected, "queued qualified original identity")
    return dict(tx=tx, digest=hashlib.sha256(manifest).digest(), coordinator=coordinator,
                configuration=configuration, routing=routing, source=source, wire=source_wire, manifest=manifest, floors=floors)


def decision_command(wire):
    require(wire[:4] in (b"GCT\1", b"GCT\2") and len(wire) <= 64 << 10, "queued candidate codec/bound")
    c = P.Cursor(P.body(wire, wire[:4]))
    phase, graph, partition, manifest, count = c.number(1), c.take(16), c.number(), c.field(64 << 10), c.number(4)
    require(1 <= phase <= 4 and graph == P.GRAPH and partition in (3, 8) and count <= 2, "queued phase namespace/proofs")
    proofs, proof_wires = [], []
    for _ in range(count):
        raw_status = b"GCS\1" + c.take(99)
        proof_wire = raw_status + hashlib.sha256(raw_status).digest()
        proof_wires.append(proof_wire)
        proofs.append(P.status(proof_wire))
    conflict = c.take(416) if wire[3] == 2 else None
    c.end()
    return dict(phase=phase, partition=partition, manifest=manifest, proofs=proofs, proof_wires=proof_wires, conflict=conflict)


def decision_queue(queue, order):
    require(queue["Version"] == 1 and queue["Profile"] == DECISION_PROFILE and order in ("commit-first", "abort-first") and queue["Order"] == order, "queued finite order/profile")
    cookie = queue["Cookie"]
    require(type(cookie) is list and len(cookie) == 16 and any(cookie) and all(type(n) is int and 0 <= n <= 255 for n in cookie), "queued live owner cookie")
    require(queue["Sequence"] == 1 and queue["Member"] == 4 and type(queue["PID"]) is int and queue["PID"] > 0 and queue["Mask"] == 3 and queue["Queued"] is True and queue["Closed"] is False, "queued genuine owner lifecycle")
    require(queue["Accepted"] == [True, True] and queue["Errors"] == [0, 0], "both genuine candidates must be admitted")
    require(1 <= queue["Controls"] <= 16 and queue["PrepaidBytes"] == 20 << 20, "queued finite default owner grant")
    applied, before, after = queue["BeforeApplied"], queue["BeforeLastIndex"], queue["AfterLastIndex"]
    require(all(type(n) is int and 0 < n < 1 << 64 for n in (applied, before, after)) and applied == queue["AfterApplied"] <= before and after == before + 2, "queued two persisted inputs before first application")
    require(len(queue["Candidates"]) == len(queue["EntryWires"]) == 2, "queued native input census")
    candidates = [P.raw(w) for w in queue["Candidates"]]
    entries = [P.raw(w) for w in queue["EntryWires"]]
    require(entries == (candidates[::-1] if order == "commit-first" else candidates), "actual native log inputs differ from admitted order")
    abort, commit = map(decision_command, candidates)
    manifest = P.raw(queue["Manifest"])
    require(all(c["phase"] == 3 and c["partition"] == 8 and c["manifest"] == manifest for c in (abort, commit)), "same B original decision candidates")
    require(abort["conflict"] is not None and not abort["proofs"] and commit["conflict"] is None and [s["partition"] for s in commit["proofs"]] == [3, 8], "actual capacity ABORT/all-YES COMMIT forms")
    return abort, commit


def decision_capacity(boundary, scopes, a, b, conflict):
    q = P.Cursor(P.body(P.raw(boundary["Request"]), b"GSQ\1"))
    kind, sequence, nonce = q.number(1), q.number(), q.take(16)
    q.take(48)
    require(not q.field() and not q.field() and not q.field(), "capacity nonrange request")
    question = P.Cursor(P.body(q.field(), b"GCQ\1"))
    proposal = P.Cursor(P.body(question.field(), b"GSC\1"))
    target, prepare_wire = P.body(proposal.field(156), b"APS\1"), proposal.field()
    proposal.end()
    require((question.number(4), question.number(4), question.number(4)) == (128, 128 << 10, 1 << 20), "unchanged actual capacity allowance")
    question.end()
    require(q.take(16) == b["tx"] and q.number(1) == 2, "capacity desired query identity")
    q.end()
    prepare = decision_command(prepare_wire)
    require(prepare["phase"] == 2 and prepare["partition"] == 3 and prepare["manifest"] == b["manifest"] and prepare["conflict"] is None, "capacity actual desired Prepare")
    require(len(prepare["proofs"]) == 1 and prepare["proofs"][0]["tx"] == b["tx"] and prepare["proofs"][0]["digest"] == b["digest"] and prepare["proofs"][0]["phase"] == 1 and prepare["proofs"][0]["partition"] == 8, "capacity genuine B registration")
    r = P.Cursor(P.body(P.raw(boundary["Reply"]), b"GSR\1"))
    scope, voter, issuer = P.body(r.field(156), b"APS\1"), r.number(), r.take(16)
    require(kind == r.number(1) == 6 and sequence == r.number() and sequence > 0 and nonce == r.take(16) and any(nonce) and r.number(2) == 0 and any(issuer), "authenticated capacity correlation")
    require(scope == target == scopes[3] and voter == 1, "configured capacity source/voter")
    answer = P.Cursor(P.body(r.field(), b"GCA\1"))
    r.end()
    actual = P.body(answer.field(512), b"GCF\1")
    rows, read_bytes, output = answer.number(4), answer.number(4), answer.number(4)
    answer.end()
    require(0 < rows <= 128 and 0 < read_bytes <= 128 << 10 and 0 < output <= 1 << 20 and actual == conflict, "complete same genuine capacity observation/work")
    c = P.Cursor(actual)
    require(c.take(120) == scopes[3] and c.take(32) == b["configuration"] and c.take(32) == b["routing"], "capacity scope/configuration/routing")
    generation, index, image = c.number(), c.number(), c.take(32)
    require(generation > 0 and index > 0 and any(image), "actual capacity retained source view")
    require((c.take(16), c.take(32), c.number()) == (b["tx"], b["digest"], 8) and (c.take(16), c.take(32), c.number()) == (a["tx"], a["digest"], 3) and b["tx"] > a["tx"], "same two originals/actual loser ordering")
    require(c.take(32) == hashlib.sha256(prepare_wire).digest() and c.take(16) == nonce and c.take(16) == issuer and c.number() > 0, "completed read identity/nonce/Prepare digest")
    c.end()
    return index, prepare["proof_wires"][0]


def decision_check(data):
    order, queue = data["Case"], data["Queue"]
    require(data["Version"] == 2 and data["Profile"] == DECISION_PROFILE, "fresh queued version")
    abort, commit = decision_queue(queue, order)
    scopes, old_round = P.cut(data["Before"])
    require(data["Retained"] == data["Reopened"] == data["Before"], "queued old cut substituted current")
    a = decision_manifest(P.raw(data["OriginalA"]), P.raw(data["ManifestA"]), 3, scopes, data["Before"])
    b = decision_manifest(P.raw(data["OriginalB"]), P.raw(queue["Manifest"]), 8, scopes, data["Before"])
    require(P.raw(queue["Original"]) == b["wire"] and a["tx"] != b["tx"], "queued same B business original")
    capacity_index, registration_wire = decision_capacity(data["Capacity"], scopes, a, b, abort["conflict"])
    originals = {a["tx"]: a, b["tx"]: b}
    records, phases, statuses = {}, {a["tx"]: {}, b["tx"]: {}}, {}
    for record in data["Records"]:
        key = record["Partition"], record["Index"]
        require(key[0] in (3, 8) and key not in records and len(records) < 512, "queued duplicate/foreign native prefix")
        records[key] = record
    for offset, partition in enumerate((3, 8)):
        lo, hi = data["From"][offset], data["Through"][offset]
        require(type(lo) is int and type(hi) is int and 0 < lo <= hi < 1 << 64 and hi-lo <= 256, "queued finite prefix range")
        require([r["Index"] for r in data["Records"] if r["Partition"] == partition] == list(range(lo+1, hi+1)), "queued incomplete native prefix")
    for record in data["Records"]:
        outcome, cdc = P.raw(record["Outcome"]), P.raw(record["CDC"])
        if not outcome:
            require(not cdc, "queued native output lacks original")
            continue  # Genuine successful present-empty ApplicationRecordGet.
        if outcome[:4] == b"GSO\1":
            control = P.body(outcome, b"GSO\1")
            require(len(control) == 18 and 1 <= control[0] <= 2 and 1 <= control[1] <= 4 and int.from_bytes(control[2:10], "big") > 0 and int.from_bytes(control[10:], "big") == record["Index"] and not cdc, "queued actual control outcome")
            continue
        s = P.status(outcome)
        original = originals.get(s["tx"])
        require(original is not None and s["partition"] == record["Partition"] and s["digest"] == original["digest"] and 0 < s["index"] <= record["Index"], "queued foreign original/status")
        if s["index"] < record["Index"]:
            require(not cdc and records.get((s["partition"], s["index"]), {}).get("Outcome") == record["Outcome"], "queued replay lacks actual earlier original")
            continue
        key = s["partition"], s["phase"]
        require(key not in phases[s["tx"]], "queued duplicate original phase")
        phases[s["tx"]][key], statuses[(s["tx"], *key)] = s, outcome
        expected_commit = s["tx"] == b["tx"] and order == "commit-first"
        if s["phase"] >= 3:
            require(s["commit"] == expected_commit and (s["round"] > old_round if expected_commit else s["round"] == 0), "queued durable first decision/effects")
        if s["phase"] == 2:
            require(s["yes"] == (s["tx"] == b["tx"]), "queued actual A NO/B YES")
        require(bool(cdc) == (s["phase"] == 4 and expected_commit), "queued exact business CDC scope")
        if cdc:
            envelope = P.Cursor(P.body(cdc, b"GCE\1"))
            require(envelope.number(1) == 6, "queued typed graph operation CDC")
            require(envelope.take(16) == b["tx"] and envelope.take(32) == b["digest"] and envelope.number() == record["Index"], "queued original CDC binding")
            logical = P.body(envelope.field(), b"GCD\5")
            envelope.end()
            require(logical[:16] == P.GRAPH and int.from_bytes(logical[16:24], "big") == record["Partition"], "queued typed CDC namespace")
    require(set(phases[a["tx"]]) == {(3, 1), (3, 2), (3, 3), (3, 4)} and set(phases[b["tx"]]) == {(8, 1), (3, 2), (8, 2), (8, 3), (3, 4), (8, 4)}, "queued complete two-original protocol census")
    require(phases[a["tx"]][(3, 1)]["index"] <= capacity_index < phases[a["tx"]][(3, 2)]["index"], "capacity was observed while A genuinely owned completion")
    decision_a, decision_b = P.raw(data["DecisionA"]), P.raw(data["DecisionB"])
    require(decision_a == statuses[(a["tx"], 3, 3)] and decision_b == statuses[(b["tx"], 8, 3)], "queued actual immutable decisions")
    first_index = queue["BeforeLastIndex"] + 1
    require(phases[b["tx"]][(8, 3)]["index"] == first_index and all(P.raw(records[(8, first_index+i)]["Outcome"]) == decision_b and not P.raw(records[(8, first_index+i)]["CDC"]) for i in range(2)), "queued second native decision did not retain first durable original")
    registration = bytearray(P.body(statuses[(b["tx"], 8, 1)], b"GCS\1"))
    require(int.from_bytes(registration[81:89], "big") == b["floors"][8], "retained Register captured coordinator floor")
    # The completed Register lookup projects only floor=0 from its immutable R
    # control. All other bytes must match the genuine retained original exactly.
    registration[81:89] = bytes(8)
    projected_registration = b"GCS\1" + registration
    require(registration_wire == projected_registration + hashlib.sha256(projected_registration).digest(), "capacity Prepare differs from actual retained Register projection")
    for s, wire in zip(commit["proofs"], commit["proof_wires"]):
        require(wire == statuses[(b["tx"], s["partition"], 2)], "all-YES candidate differs from actual retained votes")
    require(len(data["Pending"]) == 2, "queued pending caller census")
    for encoded in data["Pending"]:
        P.resumed(encoded, scopes, b["source"]["request"], b["tx"], P.business_hash(b["wire"], 8), 1, b"")
    require(len(data["Duplicates"]) == 3 and [r["Actor"] for r in data["Duplicates"]] == [3, 4, 4], "same original/replacement retry census")
    for retry, original, decision_wire in zip(data["Duplicates"], (a, b, b), (decision_a, decision_b, decision_b)):
        require(P.raw(retry["Original"]) == original["wire"], "duplicate imported a replacement original")
        P.resumed(retry["Response"], scopes, original["source"]["request"], original["tx"], P.business_hash(original["wire"], original["coordinator"]), 0, decision_wire)
        expected = []
        for offset, partition in enumerate((3, 8)):
            lo, hi = retry["From"][offset], retry["Through"][offset]
            require(data["From"][offset] <= lo <= hi <= data["Through"][offset], "duplicate foreign prefix range")
            expected.extend(records[(partition, index)] for index in range(lo+1, hi+1))
        require((retry["Records"] or []) == expected and all(not P.raw(r["CDC"]) for r in expected), "duplicate incomplete prefix or repeated CDC")
    processes(data["Processes"], None, "partial-install-decision-loss")
    killed = json.loads(P.raw(data["Processes"][0]["Raw"]))
    restored = json.loads(P.raw(data["Processes"][1]["Raw"]))
    require(killed["PID"] == queue["PID"] and restored["PID"] != queue["PID"] and restored["Member"] == 4, "actual owner process/replacement identity")
    require(data["ReopenedCurrent"]["Native"] == data["Current"]["Native"], "replacement changed participant effects")
    initial, current = P.projected(data["Before"]), P.projected(data["Current"])
    transactions, rounds = {}, {}
    if order == "commit-first":
        decision = phases[b["tx"]][(8, 3)]
        require(all(phases[b["tx"]][(partition, 4)]["round"] == decision["round"] for partition in (3, 8)), "both participant COMMIT rounds")
        keys = ("A/entity/3", "A/presence/3", "A/labels/3", "A/property/3/p", "A/life/3/4", "A/posting/1/0", "A/posting/1/2", "B/posting/17/0", "B/posting/17/18")
        transactions["B"] = {"reads": [P.read_key(initial, key) for key in ("A/presence/1", "B/presence/17", "A/entity/3")], "writes": P.writes(initial, current, keys)}
        rounds["B"] = decision["round"]
    else:
        require(initial == current, "two ABORTs changed useful graph")
    observations = [{"event": key, "scope": ["A", "B"], "round": P.cut(data[key])[1], "values": P.projected(data[key])} for key in ("Before", "Retained", "Current", "Reopened", "ReopenedCurrent")]
    witness, detail = P.oracle()({"initial": initial, "transactions": transactions}, set(transactions), rounds, observations, [{"event": "final", "scope": ["A", "B"], "values": current}])
    require(witness is not None, "queued independent serial/cut witness: " + str(detail))
    return {"case": order, "records": len(records), "serial_witness": witness, "scope": "two genuine queued original candidates; source-only pending actual process qualification; no whole D/matrix capacity claim"}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts",nargs="+",type=Path)
    args=parser.parse_args()
    print(json.dumps([check(load(path)) for path in args.artifacts],indent=2))


if __name__ == "__main__":
    main()
