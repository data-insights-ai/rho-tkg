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
        require(row["Classification"] == "unavailable" and response["mask"] & (1 << 3) and not response["mask"] & (1 << 12) and not response["aux"] and not response["outcome"] and not response["changes"], "partial read invented success or timeout authority")


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
    case = data["Case"]
    require(data["Version"] == 1 and data["Profile"] == PROFILE and case in CASES, "fresh finite case contract")
    require((data["IO"] is not None)==(case.startswith("wal-") or case=="directory-sync-during-reopen"),"required actual IO receipt")
    require(bool(data["PartialReads"])==(case=="partial-install-decision-loss"),"partial certification inventory")
    scopes, old_round = P.cut(data["Before"])
    for key in ("Retained", "Reopened"):
        require(data[key] == data["Before"], "old cut substituted current state")
    source, manifest, tx, digest, coordinator = None, P.raw(data["Manifest"]), None, None, None
    if case != "directory-sync-during-reopen":
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
    observations=[{"event":key,"scope":["A","B"],"round":P.cut(data[key])[1],"values":P.projected(data[key])} for key in ("Before","Retained","Recovered","Reopened")]
    witness,detail=P.oracle()({"initial":initial,"transactions":transactions},set(transactions),rounds,observations,[{"event":"final","scope":["A","B"],"values":current}])
    require(witness is not None,"independent exact serial/cut witness: "+str(detail))
    processes(data["Processes"] or [],data["IO"],case)
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


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts",nargs="+",type=Path)
    args=parser.parse_args()
    print(json.dumps([check(load(path)) for path in args.artifacts],indent=2))


if __name__ == "__main__":
    main()
