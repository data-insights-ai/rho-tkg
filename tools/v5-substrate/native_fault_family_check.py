#!/usr/bin/env python3
"""Independent finite bridge for genuine native composed-D source artifacts.

Only actual retained originals, authenticated cuts and complete framed prefixes
enter the serial oracle. A JSON receipt does not establish that a process ran.
The synchronized-image cases explicitly model guaranteed bytes/names; their
pre-kill catalog is lineage, while recovery selects the post-reap sealed image.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath
import struct

SPEC = importlib.util.spec_from_file_location("native_family_components", Path(__file__).with_name("native_fault_schedule_check.py"))
C = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(C)
P = C.P
PROFILE = "native-composed-d-family-v1"
MODEL = "native-vfs-synced-image-v1"
POLICY = dict(Files=1024, Names=2048, CatalogBytes=1 << 20, CopyBytes=64 << 10,
              Operations=65536, ShadowBytes=1 << 30, WorkBytes=1 << 40)
CASES = {"torn-wal-current-disk", "torn-wal-synced-image", "clock-expiry-rollback",
         "durable-native-wal-corruption", "durable-native-manifest-corruption",
         "old-leader-packet", "unequal-ticks", "asymmetric-network",
         "partition-quorum-loss", "coordinator-quorum-loss", "logical-domain-loss",
         "options-rename-during-reopen", "snapshot-chunk-interruption", "snapshot-atomic-kv-sync"}
EXPLORATIONS = {f"explore-A-{phase}-fence-{str(first).lower()}"
                for phase in ("registered", "prepared") for first in (False, True)}
EXPLORATIONS |= {f"explore-B-{phase}-fence-true" for phase in ("registered", "prepared")}
HEADER_KEYS = set("Version Profile Case Original Manifest PendingCaller InstalledA WithheldB PartialReads Before Retained Recovered Reopened Processes From Through Records PacketEvents IO Refused RefusedReplay Ticks Clocks Model Network HeldPacket HeldTerm LeaderBefore LeaderAfter Corruption OpenRefusal Repair CorruptArchive Expiry Snapshot Exploration DiskBytes AllocatedDiskBytes Scope".split())
SENDER_PROFILE = "native-snapshot-inflight-sender-loss-v1"
SENDER_CASES = {"snapshot-sender-before-append", "snapshot-sender-after-append"}
SENDER_FIELDS = set("Version Run DeadlineUnixNanos Side SelectedSnapshot SenderOwner SenderBefore ReceiverBefore ReceiverPartial ReceiverAfterClose SenderReadIndex RestartedReadIndex SenderAfter SenderReadRequest RestartedReadRequest PartialOld StaleRequest StaleReply OriginalReply ReplayReply".split())


def require(value, message):
    if not value:
        raise ValueError("native D family refused: " + message)


def integer(value, maximum=(1 << 64) - 1, minimum=0):
    require(type(value) is int and minimum <= value <= maximum, "finite integer")
    return value


def octets(value, size, nonzero=False):
    require(type(value) is list and len(value) == size and all(type(n) is int and 0 <= n <= 255 for n in value), "fixed byte array")
    wire = bytes(value)
    require(not nonzero or any(wire), "zero owner/digest")
    return wire


def pairs(items):
    require(len({key for key, _ in items}) == len(items), "duplicate JSON field")
    return dict(items)


def decoded(wire):
    return json.loads(wire, object_pairs_hook=pairs, parse_constant=lambda value: require(False, "nonfinite JSON value"))


def load(path):
    digest, total, header, footer = hashlib.sha256(), 0, None, None
    records, snapshots, image = [], [], bytearray()
    phase = 0
    with path.open("rb") as source:
        while line := source.readline((512 << 10) + 1):
            total += len(line)
            require(len(line) <= 512 << 10 and total <= 16 << 20 and line.endswith(b"\n"), "artifact byte ceiling")
            row = decoded(line)
            require(type(row) is dict and footer is None, "row after sealed EOF")
            kind = row.get("Type")
            if header is None:
                require(set(row) == {"Type", "Artifact"} and kind == "header", "header grammar")
                header = row["Artifact"]
                sender = type(header) is dict and header.get("Profile") == SENDER_PROFILE
                require(type(header) is dict and set(header) == (HEADER_KEYS | {"SnapshotSender"} if sender else HEADER_KEYS) and header["Records"] is None and header["Snapshot"] is None, "fresh header grammar/inline census")
                require(header["Model"] is None or header["Model"]["Image"] is None, "inline model")
            elif kind == "model-image":
                require(phase == 0 and set(row) == {"Type", "Sequence", "Data"} and row["Sequence"] == (len(image) + 65535) // 65536, "model page sequence")
                data = P.raw(row["Data"])
                require(0 < len(data) <= 65536 and len(image) + len(data) <= (1 << 20) + 65 and (not image or len(image) % 65536 == 0), "model page bound")
                image.extend(data)
            elif kind == "snapshot-boundary":
                require(phase <= 1 and set(row) == {"Type", "Sequence", "Boundary"} and row["Sequence"] == len(snapshots) and len(snapshots) < 16, "snapshot page order/cap")
                phase = 1
                snapshots.append(row["Boundary"])
            elif kind == "record":
                require(set(row) == {"Type", "Record"} and len(records) < 512, "native prefix frame cap")
                phase = 2
                records.append(row["Record"])
            else:
                require(set(row) == {"Type", "Rows", "Through", "PayloadSHA256"} and kind == "footer", "terminal EOF grammar")
                footer = row
                require(row["Rows"] == len(records) and row["PayloadSHA256"] == digest.hexdigest(), "terminal exact census/hash")
            if footer is None:
                digest.update(line)
    require(header is not None and footer is not None and footer["Through"] == header["Through"], "missing/foreign EOF")
    require(bool(image) == (header["Model"] is not None), "model page census")
    header["Records"], header["Snapshot"] = records, snapshots
    if image:
        header["Model"]["Image"] = bytes(image)
    return header


def packet(c):
    source, target, snap, payload = c.number(), c.number(), c.number(1), c.field(2 << 20)
    require(1 <= source <= 6 and 1 <= target <= 6 and source != target and snap <= 1, "actual packet endpoints")
    return dict(From=source, To=target, Snapshot=bool(snap), Payload=payload)


def process(wire):
    require(len(wire) <= 2 << 20, "process frame ceiling")
    c = P.Cursor(wire)
    incarnation, stats = c.take(16), [c.number() for _ in range(28)]
    cut, manifest, generation = c.take(32), c.take(32), c.number()
    count = c.number(4)
    require(count <= 2, "actual process handle cap")
    handles = [c.number() for _ in range(count)]
    aux, ready, sequence, applied, code, mask = c.field(), c.number(1), c.number(), c.number(), c.number(1), c.number(2)
    detail = c.field(4096)
    count = c.number(4)
    require(count <= 128, "actual packet count")
    packets = [packet(c) for _ in range(count)]
    count = c.number(4)
    require(count <= 16, "actual read result count")
    reads = [(c.number(), c.field(1024)) for _ in range(count)]
    image, outcome, changes = c.field(64 << 10), c.field(64 << 10), c.field()
    flags, life, count = c.number(1), c.number(), c.number(4)
    require(count <= 16 and flags <= 3, "actual process fact shape")
    labels = [c.field(256) for _ in range(count)]
    answer = c.field(256)
    c.end()
    require(any(incarnation) and sequence > 0 and ready <= 1 and code <= 6, "actual process identity/result")
    return dict(incarnation=incarnation, stats=stats, cut=cut, manifest=manifest,
                generation=generation, handles=handles, aux=aux, ready=ready,
                sequence=sequence, applied=applied, code=code, mask=mask, detail=detail,
                packets=packets, reads=reads, image=image, outcome=outcome, changes=changes,
                flags=flags, life=life, labels=labels, answer=answer)


def prefix(data):
    rows = data["Records"]
    require(type(rows) is list and len(rows) <= 512, "prefix count")
    records = {}
    for row in rows:
        p, i = row["Partition"], integer(row["Index"], minimum=1)
        require(p in (3, 8) and (p, i) not in records, "foreign/duplicate actual entry")
        require(set(row) == {"Partition", "Index", "Outcome", "CDC"}, "actual native record grammar")
        records[p, i] = row
    require(len(data["From"]) == len(data["Through"]) == 2, "two physical prefix bounds")
    for ordinal, partition in enumerate((3, 8)):
        lo, hi = integer(data["From"][ordinal], minimum=1), integer(data["Through"][ordinal], minimum=1)
        require(lo <= hi <= lo + 256, "finite actual prefix span")
        require([r["Index"] for r in rows if r["Partition"] == partition] == list(range(lo + 1, hi + 1)), "missing actual prefix entry")
    return records


def move_status(wire):
    c = P.Cursor(P.body(wire, b"GMO\1"))
    graph, partition, move, intent = c.take(16), c.number(), c.take(16), c.take(32)
    phase, result = c.number(1), c.number(1)
    index, floor, round_, sequence, rows, size = (c.number() for _ in range(6))
    digest = c.take(32)
    c.end()
    require(graph == P.GRAPH and partition in (3, 8) and any(move) and any(intent) and 1 <= phase <= 9 and 1 <= result <= 4 and index > 0 and sequence <= 1024, "actual move status")
    return dict(partition=partition, move=move, intent=intent, phase=phase, result=result,
                index=index, floor=floor, round=round_, sequence=sequence, rows=rows, size=size, digest=digest)


def move_changes(wire, status):
    c = P.Cursor(P.body(wire, b"GML\1"))
    require(status["phase"] == 7 and status["result"] == 1 and c.take(16) == status["move"] and c.take(32) == status["intent"], "actual GML activation original")
    rid, source, target, old_epoch, new_epoch, round_ = (c.number() for _ in range(6))
    require((rid, source, target) == (1, 3, 8) and 0 < old_epoch < new_epoch == old_epoch + 1 and round_ == status["round"] > 0 and c.take(32) == status["digest"], "actual GML immutable range/placement/round/complete-source digest")
    c.end()
    return old_epoch


def originals(data, records, descriptors, move=None):
    phases = {tx: {} for tx in descriptors}
    moves = {}
    for (partition, index), row in records.items():
        wire, changes = P.raw(row["Outcome"]), P.raw(row["CDC"])
        if not wire:
            require(not changes, "CDC without genuine outcome")
            continue  # Successful native ApplicationRecord read, not missing=empty.
        if wire[:4] == b"GSO\1":
            body = P.body(wire, b"GSO\1")
            require(len(body) == 18 and body[0] in (1, 2) and 1 <= body[1] <= 4 and int.from_bytes(body[2:10], "big") > 0 and int.from_bytes(body[10:], "big") == index and not changes, "actual cut control index/no output")
            continue
        if wire[:4] == b"GMO\1":
            s = move_status(wire)
            require(move is not None and s["partition"] == partition and (s["move"], s["intent"]) == (move["move"], move["intent"]) and s["index"] <= index, "foreign move original/output")
            if s["index"] < index:
                require(not changes and records.get((partition, s["index"]), {}).get("Outcome") == row["Outcome"], "move replay lacks authentic original")
            else:
                if s["phase"] == 7 and s["result"] == 1:
                    move_changes(changes, s)
                else:
                    require(not changes, "non-activation move phase emitted CDC")
                moves[partition, index] = s
            continue
        s = P.status(wire)
        require(s["partition"] == partition and s["tx"] in descriptors and s["digest"] == descriptors[s["tx"]]["digest"] and 0 < s["index"] <= index, "unknown/foreign actual original")
        if s["index"] < index:
            require(not changes and records.get((partition, s["index"]), {}).get("Outcome") == row["Outcome"], "replay lacks authentic earlier original")
            continue
        key = partition, s["phase"]
        require(key not in phases[s["tx"]], "duplicate original phase")
        phases[s["tx"]][key] = s
        if s["phase"] != 4 or not s["commit"]:
            require(not changes, "non-COMMIT phase business CDC")
        else:
            e = P.Cursor(P.body(changes, b"GCE\1"))
            require(e.number(1) == 6 and e.take(16) == s["tx"] and e.take(32) == s["digest"] and e.number() == index, "actual CDC original envelope")
            body = P.body(e.field(), b"GCD\5")
            e.end()
            require(body[:16] == P.GRAPH and int.from_bytes(body[16:24], "big") == partition, "actual complete CDC namespace")
    return phases, moves


def decision(phases, descriptor, commit, old_round):
    coordinator, tx = descriptor["coordinator"], descriptor["tx"]
    require((coordinator, 1) in phases and (coordinator, 3) in phases, "missing registration/first durable decision")
    result = phases[coordinator, 3]
    require(result["commit"] is commit and (result["round"] > old_round if commit else result["round"] == 0), "durable original decision/round")
    require(set(phases) <= {(coordinator, 1), (3, 2), (8, 2), (coordinator, 3), (3, 4), (8, 4)}, "unexpected protocol phase")
    present = {p for p, phase in phases if phase in (1, 2)}
    require({p for p, phase in phases if phase == 4} == ({3, 8} if commit else present), "missing native resolution of actual present participants")
    for (partition, phase), s in phases.items():
        require(s["tx"] == tx and (phase != 2 or not commit or s["yes"]), "actual Prepare vote")
        if phase == 4:
            require(s["commit"] is commit and s["round"] == result["round"], "resolve disagrees with first decision")
    if commit:
        require({p for p, phase in phases if phase == 2} == {3, 8}, "COMMIT lacks two YES originals")
    return result


def safe_name(name):
    require(type(name) is str and 0 < len(name) <= 1024 and "\\" not in name and str(PurePosixPath(name)) == name and not name.startswith("/") and name != "." and ".." not in PurePosixPath(name).parts, "unsafe native name")
    return name


def receipt_owner(receipt):
    require(receipt["Version"] == 1 and receipt["Profile"] == PROFILE and 1 <= receipt["Member"] <= 6 and integer(receipt["PID"], minimum=1), "family source owner")
    cookie = octets(receipt["Cookie"], 16, True)
    integer(receipt["Controls"], 16, 1)
    integer(receipt["ModelCaptures"], 2)
    integer(receipt["Sequence"], 16)
    integer(receipt["OffsetMillis"], 365 * 86400000, -365 * 86400000)
    return cookie


def model_image(wire, policy):
    require(policy == POLICY and 66 <= len(wire) <= policy["CatalogBytes"] + 65 and wire[-65:-64] == b"\n", "native synchronized catalog envelope/policy")
    body, checksum = wire[:-65], wire[-64:]
    require(hashlib.sha256(body).hexdigest().encode() == checksum, "catalog byte seal")
    image = decoded(body)
    require(set(image) == {"Version", "Profile", "Cookie", "Member", "Policy", "Ledger", "Names", "Files"} and image["Version"] == 1 and image["Profile"] == MODEL and image["Member"] in range(1, 7) and image["Policy"] == policy, "native catalog grammar")
    octets(image["Cookie"], 16, True)
    ledger = image["Ledger"]
    require(set(ledger) == {"Operations", "Files", "Versions", "ShadowBytes", "WorkBytes", "CopyReadBytes", "CopyWriteBytes"}, "native ledger grammar")
    for field, limit in (("Operations", policy["Operations"]), ("Files", policy["Files"]), ("Versions", policy["Operations"]), ("ShadowBytes", policy["ShadowBytes"]), ("WorkBytes", policy["WorkBytes"]), ("CopyReadBytes", policy["ShadowBytes"]), ("CopyWriteBytes", policy["ShadowBytes"])):
        integer(ledger[field], limit)
    require(ledger["Versions"] <= ledger["Operations"] and ledger["CopyReadBytes"] <= ledger["ShadowBytes"] and ledger["CopyWriteBytes"] == ledger["ShadowBytes"], "actual copy ownership/versions")
    require(type(image["Files"]) is list and len(image["Files"]) == ledger["Files"] and len(image["Names"] or []) <= policy["Names"], "finite file/name inventory")
    files, previous, latest = {}, 0, 0
    for file in image["Files"]:
        require(set(file) == {"File", "Operation", "Bytes", "Image", "SHA256"}, "file version grammar")
        fid, op, size = integer(file["File"], minimum=1), integer(file["Operation"], ledger["Operations"]), integer(file["Bytes"], policy["ShadowBytes"])
        require(fid > previous, "file identity ordering/reuse")
        digest = octets(file["SHA256"], 32, op > 0)
        require(file["Image"] == f"{fid:016x}-{op:016x}.image" if op else file["Image"] == "" and size == 0 and not any(digest), "immutable native shadow identity")
        files[fid], previous, latest = file, fid, latest + size
    require(latest <= ledger["ShadowBytes"], "latest versions exceed actual cumulative shadow copies")
    names, kinds, previous = {}, {}, ""
    for name in image["Names"] or []:
        require(set(name) == {"Path", "File", "Directory"}, "synced name grammar")
        path, fid, directory = safe_name(name["Path"]), name["File"], name["Directory"]
        require(path > previous and fid in files and type(directory) is bool and (fid not in kinds or kinds[fid] is directory) and (not directory or files[fid]["Operation"] == 0), "synced name/file lineage")
        names[path], kinds[fid], previous = name, directory, path
    return image, files, names


def recovery(receipt, image, files, names, digest, previous_work):
    require(receipt["Version"] == 1 and receipt["Profile"] == MODEL and receipt["SourceCookie"] == image["Cookie"] and receipt["Member"] == image["Member"] and octets(receipt["CatalogSHA"], 32, True) == digest, "selected post-reap synced image binding")
    require(type(receipt["Directory"]) is str and Path(receipt["Directory"]).is_absolute(), "owned recovery directory")
    visible, copied, name_count, byte_count = {"."}, set(), 0, 0
    for path, name in names.items():
        if str(PurePosixPath(path).parent) not in visible:
            continue
        name_count += 1
        if name["Directory"]:
            visible.add(path)
        elif name["File"] not in copied:
            copied.add(name["File"])
            byte_count += files[name["File"]]["Bytes"]
    grant = 2 * image["Ledger"]["ShadowBytes"] + 4 * POLICY["CatalogBytes"] + 4096 * POLICY["Names"]
    require(receipt["Names"] == name_count and receipt["Files"] == len(copied) and receipt["Bytes"] == byte_count and receipt["WorkBytes"] == previous_work + grant <= POLICY["WorkBytes"], "prepaid recovery work vs actual copied native bytes/links")


def model(data):
    m = data["Model"]
    if m is None:
        return None
    require(set(m) == {"Capture", "Image", "Recovery", "SourceVolume", "VolatileArchive"}, "model source/recovery metadata grammar")
    capture = m["Capture"]
    receipt_owner(capture)
    wire = m["Image"]
    image, files, names = model_image(wire, capture["Policy"])
    digest = hashlib.sha256(wire).digest()
    require(image["Cookie"] == capture["ModelCookie"] and image["Member"] == capture["Member"] == 1 and image["Ledger"]["Operations"] >= capture["ModelLedger"]["Operations"] and image["Ledger"]["WorkBytes"] >= capture["ModelLedger"]["WorkBytes"], "pre-kill capture lineage, not earlier hash equality")
    octets(capture["CatalogSHA"], 32, True)
    recovery(m["Recovery"], image, files, names, digest, image["Ledger"]["WorkBytes"])
    volume, archive = Path(m["SourceVolume"]), Path(m["VolatileArchive"])
    require(volume.is_absolute() and archive == volume.with_name(volume.name + f'.volatile.{capture["PID"]}'), "owned volatile archive lineage")
    require(data["DiskBytes"] >= image["Ledger"]["ShadowBytes"] + m["Recovery"]["Bytes"], "all-child/shadow/recovery disk census")
    return image, files, names, digest


def active_manifest(names):
    selected, iteration = None, -1
    for path in names:
        if "/" in path or not path.startswith("marker."):
            continue
        parts = path.split(".", 3)
        require(len(parts) == 4 and parts[2].isdigit() and int(parts[2]) < 1 << 64, "Pebble marker grammar")
        if parts[1] == "manifest" and int(parts[2]) > iteration:
            selected, iteration = parts[3], int(parts[2])
    require(selected is not None and selected.startswith("MANIFEST-") and selected in names and not names[selected]["Directory"], "actual required active Pebble manifest")
    return selected


def required_wal_outcome(wire, certificate):
    # sourceIndex names the business image, not the later GSO certificate
    # control entry. This fixed fixture's business source is a real Resolve.
    require(len(certificate) == 356 and certificate[:16] == P.GRAPH and int.from_bytes(certificate[16:24], "big") == 3, "required WAL certificate namespace")
    status = P.status(wire)
    body = P.body(wire, b"GCS\1")
    require(status["partition"] == 3 and status["phase"] == 4 and status["commit"] is True and body[98] == 0
            and any(status["tx"]) and any(status["digest"])
            and status["index"] == int.from_bytes(certificate[176:184], "big") > 0
            and status["round"] == int.from_bytes(certificate[168:176], "big") > 0,
            "acknowledged applied COMMIT Resolve at exact certificate source index/round")


def corruption(data, selected):
    case, receipt = data["Case"], data["Corruption"]
    require((receipt is not None) == case.startswith("durable-native-"), "native corruption case inventory")
    if receipt is None:
        require(data["OpenRefusal"] is None and data["Repair"] is None and not data["CorruptArchive"], "orphan native corruption diagnostics")
        return
    require(selected is not None, "native corruption lacks selected synced source")
    image, files, names, digest = selected
    require(receipt["SourceCookie"] == image["Cookie"] and receipt["ActiveManifest"] == active_manifest(names), "native active-file selection")
    path = safe_name(receipt["Path"])
    require("/" not in path and path in names and not names[path]["Directory"], "corrupted actual native required file")
    file = files[names[path]["File"]]
    require(receipt["File"] == file["File"] and receipt["Operation"] == file["Operation"] > 0 and receipt["Size"] == file["Bytes"] >= 32 and receipt["BeforeSHA256"] == file["SHA256"] and octets(receipt["AfterSHA256"], 32, True) != octets(receipt["BeforeSHA256"], 32, True), "actual native byte version before/after")
    require(receipt["Offset"] == 0 and integer(receipt["Before"], 255) ^ 128 == integer(receipt["After"], 255), "declared actual native record corruption")
    if case == "durable-native-wal-corruption":
        require(receipt["EligibleWALs"] == 1 and [n for n in names if not names[n]["Directory"] and n.endswith(".log")] == [path], "ambiguous/obsolete WAL cannot prove required corruption")
        certs = [P.body(P.raw(c), b"GSC\1") for c in data["Before"]["Cut"]["Certificates"]]
        certificate = next(c for c in certs if int.from_bytes(c[16:24], "big") == 3)
        require(P.raw(receipt["RequiredImage"]) == certificate[184:356], "acknowledged actual native image not bound to selected WAL")
        required_wal_outcome(P.raw(receipt["RequiredOutcome"]), certificate)
    else:
        require(path == receipt["ActiveManifest"] and receipt["EligibleWALs"] == 0 and not P.raw(receipt["RequiredImage"]) and not P.raw(receipt["RequiredOutcome"]), "not selected required active manifest")
    expected_work = data["Model"]["Recovery"]["WorkBytes"] + 4 * POLICY["CatalogBytes"] + 8192 + 8 * file["Bytes"] + 2 * POLICY["CopyBytes"] + 8192
    require(receipt["WorkBytes"] == expected_work <= POLICY["WorkBytes"], "prepaid native corruption traversal")
    refusal = data["OpenRefusal"]
    require(refusal["Version"] == 1 and refusal["Profile"] == PROFILE and refusal["Member"] == 1 and integer(refusal["PID"], minimum=1) and refusal["Stage"] == "raftlog.Open-before-Ready" and bool(refusal["Error"]) and (refusal["RaftCorrupt"] is True or refusal["NativeCorrupt"] is True), "literal typed native Open refusal")
    octets(refusal["Cookie"], 16, True)
    recovery(data["Repair"], image, files, names, digest, expected_work)
    volume = Path(data["Model"]["SourceVolume"])
    require(Path(data["CorruptArchive"]) == volume.with_name(volume.name + f'.corrupt.{refusal["PID"]}'), "corrupt attempt archive ownership")


def events(rows, allowed):
    require(type(rows) is list and 0 < len(rows) <= 128, "matched actual packet census")
    for row in rows:
        require((row["From"], row["To"]) in allowed and row["Action"] in ("hold", "release-oldleader", "directed-partition") and integer(row["Ordinal"], 4096, 1) and integer(row["Bytes"], 2 << 20, 1) and row["ConfiguredDelayMillis"] == row["ObservedDelayNanos"] == 0, "actual packet schedule/endpoints")
        octets(row["SHA256"], 32, True)


def protobuf_fields(wire):
    c = P.Cursor(wire)
    def varint():
        result = 0
        for shift in range(0, 70, 7):
            n = c.number(1)
            require(shift < 63 or n <= 1, "protobuf overflow")
            result |= (n & 127) << shift
            if n < 128:
                return result
        raise ValueError("protobuf varint")
    result = {}
    while c.wire:
        key = varint()
        tag, kind = key >> 3, key & 7
        require(tag > 0 and kind in (0, 1, 2, 5), "protobuf field")
        value = varint() if kind == 0 else c.take(8 if kind == 1 else 4 if kind == 5 else varint())
        result.setdefault(tag, []).append(value)
    return result


def raft_packet(payload, scopes, partition):
    c = P.Cursor(payload)
    require(c.take(4) == b"RP\1\0", "native Raft packet codec")
    identity, semantic = c.take(40), c.take(32)
    message = c.field(2 << 20)
    c.end()
    require(identity == scopes[partition][:40] and semantic == scopes[partition][88:120], "native packet full identity/semantic")
    return protobuf_fields(message)


def scheduling(data, scopes):
    case = data["Case"]
    ticks, windows = data["Ticks"] or [], data["Network"] or []
    require(len(ticks) <= 64 and len(windows) <= 1, "finite scheduler diagnostics")
    ordinals, instances = {}, {}
    for tick in ticks:
        member = integer(tick["Member"], 6, 1)
        ordinals[member] = ordinals.get(member, 0) + 1
        require(tick["Ordinal"] == ordinals[member] and 0 <= tick["Before"] <= tick["After"] < 1 << 64, "actual unequal Tick sequence")
        r = process(P.raw(tick["Response"]))
        require(r["code"] == 0 and tick["Before"] <= r["applied"] <= tick["After"], "actual Tick response")
        require(member not in instances or instances[member] == r["incarnation"], "Tick process replaced silently")
        instances[member] = r["incarnation"]
    if case == "unequal-ticks":
        require(ordinals == {2: 7, 5: 4, 6: 1} and not windows, "declared unequal real Tick vector")
    elif case in ("partition-quorum-loss", "coordinator-quorum-loss"):
        member = 1 if case == "partition-quorum-loss" else 4
        require(ordinals == {member: 24} and len(windows) == 1, "actual CheckQuorum Tick vector")
    else:
        require(not ticks, "unexpected Tick schedule")
    if case in ("partition-quorum-loss", "coordinator-quorum-loss", "asymmetric-network"):
        require(len(windows) == 1, "missing actual matched directed window")
        expected = {(1, 2)} if case == "asymmetric-network" else ({(1, 2), (2, 1), (1, 3), (3, 1)} if case == "partition-quorum-loss" else {(4, 5), (5, 4), (4, 6), (6, 4)})
        edges = windows[0]["Edges"]
        require(type(edges) is list and len(edges) == len(expected) and set(map(tuple, edges)) == expected, "directed real loss topology")
        events(windows[0]["Packets"], expected)
        require(all(r["Action"] == "directed-partition" for r in windows[0]["Packets"]), "wrong directed loss operation")
    else:
        require(not windows, "orphan loss window")
    rows = data["PacketEvents"] or []
    if case == "old-leader-packet":
        events(rows, {(1, 3)})
        require([r["Action"] for r in rows] == ["hold", "release-oldleader"] and rows[0]["SHA256"] == rows[1]["SHA256"] and rows[0]["Bytes"] == rows[1]["Bytes"], "held native packet changed/released twice")
        held = data["HeldPacket"]
        payload = P.raw(held["Payload"])
        require(held["From"] == 1 and held["To"] == 3 and held["Snapshot"] is False and len(payload) == rows[0]["Bytes"] and hashlib.sha256(payload).digest() == octets(rows[0]["SHA256"], 32), "actual held packet payload")
        message = raft_packet(payload, scopes, 3)
        require(message.get(2) == [3] and message.get(3) == [1] and message.get(4) == [data["HeldTerm"]], "old term actual protobuf message")
        before, after = process(P.raw(data["LeaderBefore"])), process(P.raw(data["LeaderAfter"]))
        require(data["HeldTerm"] > 0 and before["stats"][2] > data["HeldTerm"] and after["stats"][2] >= before["stats"][2] and before["incarnation"] == after["incarnation"], "old packet revived old leader authority")
    else:
        require(not rows and data["HeldPacket"] is None and data["HeldTerm"] == 0 and not P.raw(data["LeaderBefore"]) and not P.raw(data["LeaderAfter"]), "orphan old-leader schedule")


def clock(data, scopes):
    receipts, expiry = data["Clocks"] or [], data["Expiry"] or []
    if data["Case"] != "clock-expiry-rollback":
        require(not receipts and not expiry, "orphan wall-clock schedule")
        return
    require(len(receipts) == 5 and len(expiry) == 2, "clock/rollback inventory")
    expected = [(4, 1, -86400000), (1, 1, 120000), (1, 2, -86400000), (1, 3, 0), (4, 2, 0)]
    owners = {}
    for r, (member, sequence, offset) in zip(receipts, expected):
        cookie = receipt_owner(r)
        require((r["Member"], r["Sequence"], r["OffsetMillis"]) == (member, sequence, offset), "clock vector changes lifetime only")
        identity = cookie, r["PID"]
        require(member not in owners or owners[member] == identity, "clock owner substituted")
        owners[member] = identity
    responses = [process(P.raw(row["Response"])) for row in expiry]
    require(responses[0]["incarnation"] == responses[1]["incarnation"] and responses[0]["sequence"] < responses[1]["sequence"], "same expired caller owner after rollback")
    for response in responses:
        require(response["code"] != 0 and not response["aux"] and not response["outcome"] and not response["changes"] and not response["mask"] & (1 << 12), "expired original owner revived or timed out")
    require(not P.raw(expiry[1]["Request"]) and not P.raw(expiry[1]["Reply"]), "permanently canceled owner must refuse locally after rollback")
    for row in expiry[:1]:
        q = P.Cursor(P.body(P.raw(row["Request"]), b"GSQ\1"))
        kind, sequence, nonce = q.number(1), q.number(), q.take(16)
        cookie = q.take(40)
        q.take(8)
        q.field(); q.field(); q.field(); q.field(); q.take(17); q.end()
        r = P.Cursor(P.body(P.raw(row["Reply"]), b"GSR\1"))
        scope, voter, issuer = P.body(r.field(156), b"APS\1"), r.number(), r.take(16)
        require(scope == scopes[3] and voter == 1 and any(issuer) and r.number(1) == kind and r.number() == sequence and r.take(16) == nonce and any(nonce) and any(cookie), "expired actual source correlation")
        require(r.number(2) & (1 << 10) and not r.field(), "expired source returned authority/output")
        r.end()
        require(cookie[:16] == issuer, "expired source issuer/cookie")


def io(data):
    case, receipt = data["Case"], data["IO"]
    needed = case.startswith("torn-wal-") or case in ("options-rename-during-reopen", "snapshot-atomic-kv-sync")
    require((receipt is not None) == needed, "matched native IO inventory")
    if receipt is None:
        return
    require(receipt["Version"] == 1 and receipt["Profile"] == C.PROFILE and receipt["Sequence"] == receipt["Arms"] == receipt["Nth"] == receipt["Seen"] == 1 and receipt["Matched"] is True and receipt["Armed"] is False and integer(receipt["PID"], minimum=1), "actual matched IO owner")
    octets(receipt["Cookie"], 16, True)
    op, kind = receipt["IO"], receipt["Kind"]
    path = safe_name(op["Path"])
    require(op["Directory"] is False and 0 <= op["Written"] <= op["Requested"] <= 2 << 20, "real native file operation")
    octets(op["InputSHA256"], 32)
    octets(op["WrittenSHA256"], 32)
    if case.startswith("torn-wal-"):
        require(receipt["Member"] == 1 and kind == "torn-wal-kill" and op["Kind"] == "write" and path.endswith(".log") and op["Requested"] >= 2 and op["Written"] == op["Requested"] // 2 and op["Error"] == "declared native torn WAL prefix before SIGKILL" and integer(op.get("Offset", 0)) >= 0, "actual torn prefix/SIGKILL model")
    elif case == "options-rename-during-reopen":
        destination = safe_name(op["Destination"])
        require(receipt["Member"] == 1 and kind == "options-rename" and op["Kind"] == "rename" and destination.startswith("OPTIONS-") and "/" not in destination and op["Error"] == "input/output error" and op["Written"] == 0, "actual OPTIONS rename failure, not manifest/snapshot rename")
    else:
        require(receipt["Member"] == 3 and kind == "file-sync" and op["Kind"] == "sync" and path.endswith(".log") and op["Error"] == "input/output error" and op["Written"] == 0, "actual atomic-KV activation native WAL sync failure")


def processes(data):
    rows = data["Processes"] or []
    case = data["Case"]
    require(len(rows) <= 6, "finite process lifecycle census")
    active, killed, reopened = {}, [], []
    for row in rows:
        member, action = integer(row["Member"], 6, 1), row["Action"]
        raw = decoded(P.raw(row["Raw"]))
        require(row["Volume"] and (action == "reopen-interrupted" or row["Volume"] == raw["Volume"]), "actual durable directory binding")
        if action == "kill":
            require(row["Reaped"] is True and row["Signal"] == "SIGKILL" and raw["Reaped"] is True and raw["SignalNumber"] == 9 and raw["Signaled"] is True, "actual child SIGKILL/reap")
            killed.append(member)
            active[member] = row
        elif action in ("native-io-failure", "native-torn-prefix-kill"):
            receipt = data["IO"]
            require(receipt is not None and row["Reaped"] is True and raw["Reaped"] is True and raw["Member"] == member == receipt["Member"] and raw["PID"] == receipt["PID"] and raw["Receipt"] == receipt, "matched native operation process failure")
            if action == "native-torn-prefix-kill":
                require(raw["Signaled"] is True and raw["SignalNumber"] == 9, "real torn-prefix SIGKILL")
            else:
                require(raw["Signaled"] is False and raw["ExitCode"] == 1 and receipt["IO"]["Error"] in raw["Stderr"] + raw["Stdout"], "literal native failure and reap")
            if case != "options-rename-during-reopen":
                active[member] = row
            killed.append(member)
        elif action == "native-corrupt-open-refusal":
            refusal = data["OpenRefusal"]
            require(refusal is not None and member == refusal["Member"] and row["Reaped"] is True and raw["PID"] == refusal["PID"] and raw["ExitCode"] == 1 and raw["Signaled"] is False and raw["Reaped"] is True and raw["Receipt"] == refusal and refusal["Error"] in raw["Stderr"] + raw["Stdout"], "actual failed native Open before Ready")
            # This failed pre-Ready process never replaces the prior source identity.
        elif action in ("reopen", "reopen-interrupted"):
            require(member in active and row["Volume"] == active[member]["Volume"], "same-directory native replacement")
            if action == "reopen-interrupted":
                require(row["Instance"] == active[member]["Instance"] and raw["Member"] == member and raw["Ready"] is False and bool(P.raw(raw["Startup"])) and any(r["Instance"] == row["PreviousInstance"] for r in rows if r["Member"] == member and r["Action"] == "kill"), "actual replacement was interrupted after local native restore")
            else:
                require(row["PreviousInstance"] == active[member]["Instance"] and raw["PreviousInstance"] == row["PreviousInstance"] and raw["RecoveryRaw"] == row["RecoveryRaw"], "same-directory restored process lineage")
            require(row["Ready"] is (action == "reopen"), "interrupted replacement cannot claim Ready")
            if action == "reopen":
                reopened.append(member)
            active[member] = row
        else:
            raise ValueError("unknown real process action")
    if case.startswith("torn-wal-") or case.startswith("durable-native-") or case == "options-rename-during-reopen":
        require(reopened == [1] and killed and set(killed) == {1}, "failed source same-directory recovery census")
    elif case == "logical-domain-loss":
        require(sorted(killed) == sorted(reopened) == [2, 5], "actual two-process logical-domain loss")
    elif case in SENDER_CASES:
        require(killed == [3, 1] and reopened == [1, 3] and len(rows) == 4 and not any(r["Action"] == "reopen-interrupted" for r in rows), "real lagged receiver plus in-flight sender loss, no receiver-loss substitution")
    elif case.startswith("snapshot-"):
        require(reopened == [3] and killed and set(killed) == {3} and sum(r["Action"] == "reopen-interrupted" for r in rows) == 1, "interrupted native snapshot receiver/rejoin")
    else:
        require(not rows, "unexpected process failures")


def ordinary(data, records):
    scopes, old_round = P.cut(data["Before"])
    require(data["Retained"] == data["Before"] == data["Reopened"], "historical cut substituted current state")
    descriptor = None
    if P.raw(data["Original"]):
        manifest = P.raw(data["Manifest"])
        m = P.Cursor(P.body(manifest, b"GCM\1"))
        m.take(16)
        coordinator = m.number()
        require(coordinator in (3, 8), "original coordinator")
        descriptor = C.decision_manifest(P.raw(data["Original"]), manifest, coordinator, scopes, data["Before"])
    else:
        require(data["Case"].startswith("durable-native-") or data["Case"] == "options-rename-during-reopen", "missing useful graph original")
        require(not P.raw(data["Manifest"]), "manifest without original")
    descriptors = {} if descriptor is None else {descriptor["tx"]: descriptor}
    phases, _ = originals(data, records, descriptors)
    initial, current = P.projected(data["Before"]), P.projected(data["Recovered"])
    transactions, rounds = {}, {}
    if descriptor:
        d = decision(phases[descriptor["tx"]], descriptor, True, old_round)
        keys = ("A/entity/3", "A/presence/3", "A/labels/3", "A/property/3/p", "A/life/3/4", "A/posting/1/0", "A/posting/1/2", "B/posting/17/0", "B/posting/17/18")
        transactions["edge"] = {"reads": [P.read_key(initial, k) for k in ("A/presence/1", "B/presence/17", "A/entity/3")], "writes": P.writes(initial, current, keys)}
        rounds["edge"] = d["round"]
        if data["Case"] in ("partition-quorum-loss", "coordinator-quorum-loss"):
            C.partial_reads(data["PartialReads"], descriptor["tx"], descriptor["coordinator"])
            P.resumed(data["PendingCaller"], scopes, descriptor["source"]["request"], descriptor["tx"], P.business_hash(descriptor["wire"], descriptor["coordinator"]), 1, b"")
        else:
            require(not data["PartialReads"], "orphan unavailable reads")
    else:
        require(initial == current and not data["PartialReads"] and not P.raw(data["PendingCaller"]), "startup/corruption invented useful effects")
    observations = [dict(event=k, scope=["A", "B"], round=P.cut(data[k])[1], values=P.projected(data[k])) for k in ("Before", "Retained", "Recovered", "Reopened")]
    witness, detail = P.oracle()({"initial": initial, "transactions": transactions}, set(transactions), rounds, observations, [dict(event="final", scope=["A", "B"], values=current)])
    require(witness is not None, "independent exact serial/cut witness: " + str(detail))
    scheduling(data, scopes)
    clock(data, scopes)
    return witness, scopes


def fact(wire):
    c = P.Cursor(wire)
    flags, life, source, target, count = c.number(1), c.number(), c.number(), c.number(), c.number(4)
    require(flags <= 3 and count <= 16, "range observed fact bounds")
    labels = [c.field(256).decode("utf-8") for _ in range(count)]
    count = c.number(4)
    require(count <= 16, "range incident bounds")
    incident = [c.number() for _ in range(count)]
    c.end()
    require(labels == sorted(set(labels)) and incident == sorted(set(incident)), "range exact label/incident set")
    return dict(flags=flags, life=life, source=source, target=target, labels=labels, incident=incident)


def range_read(observation):
    scopes, round_ = P.cut({"Cut": observation["Cut"]})
    require(len(observation["Details"]) == 2, "range oracle complete two native pages")
    result = {}
    for encoded, expected in zip(observation["Details"], ([1, 3, 17], [7])):
        c = P.Cursor(P.raw(encoded))
        require(c.number(4) == len(expected), "range oracle exact ID count")
        for owner in expected:
            require(c.number() == owner, "range oracle ordered exact IDs")
            observed = fact(c.field(64 << 10))
            identity, presence, property_ = c.field(1024), c.field(64 << 10), c.field(64 << 10)
            value_id, revision, provenance, value = (c.number() for _ in range(4))
            row = dict(observed, identity=identity.hex(), presence=presence.hex(), property=property_.hex(), valueID=value_id, revision=revision, provenance=provenance, value=value)
            prefix = "A" if owner < 17 else "B"
            result[f"{prefix}/fact/{owner}"] = row
            if owner == 7:
                require(observed == dict(flags=0, life=0, source=0, target=0, labels=[], incident=[]) and not identity + presence + property_ and value_id == revision == provenance == value == 0, "phantom range fact/cell")
                continue
            require(observed["flags"] == 3 and observed["life"] == owner + 1 and bool(identity) and bool(presence), "range native entity/life/presence")
            native = P.Cursor(identity)
            require(native.number() == owner, "range immutable native identity")
            kind, mode, interpretation, role = (native.number(1) for _ in range(4))
            type_ = native.field(256)
            ns, nt, axis = native.number(), native.number(), native.take(32)
            life_owner, life, source_life, target_life = (native.number() for _ in range(4))
            native.end()
            require(life_owner == owner and life == owner + 1 and any(axis) and kind in (1, 2) and mode in (0, 1, 2), "range exact typed native axes/lives")
            row["axis"] = axis.hex()
            if owner == 3:
                require((kind, mode, interpretation, role, type_) == (2, 1, 6, 2, b"cross-edge") and observed["source"] == ns == 1 and observed["target"] == nt == 17 and source_life == 2 and target_life == 18 and not property_ and value_id == revision == value == 0, "range endpoint-life edge/property identity")
            else:
                want_id, want_value = (5, 7) if owner == 1 else (19, 13)
                require((kind, mode, interpretation, role, type_) == (1, 0, 3, 1, b"") and observed["source"] == observed["target"] == ns == nt == 0 and source_life == target_life == 0 and value_id == want_id and value == want_value and revision > 0 and provenance > 0 and bool(property_) and observed["incident"] == [3], "range exact native typed value/cell/posting")
        c.end()
    return scopes, round_, result


def move_result(encoded):
    response = process(P.raw(encoded))
    c = P.Cursor(response["aux"])
    require(c.take(4) == b"GMR2", "actual move resume codec")
    scope, request, move, intent = P.body(c.field(156), b"APS\1"), c.take(16), c.take(16), c.take(32)
    pending, committed = c.number(1), c.number(1)
    wires = [c.field(256) for _ in range(3)]
    mask = c.number(2)
    c.end()
    require(len(scope) == 120 and scope[:16] == P.GRAPH and int.from_bytes(scope[16:24], "big") == 3 and any(request) and any(intent) and pending <= 1 and committed <= 1 and move == hashlib.sha256(b"rho-tkg:range-move-id:v1\0" + P.GRAPH + struct.pack(">Q", 3) + request).digest()[:16], "actual source-qualified move identity")
    statuses = [move_status(w) if w else None for w in wires]
    require(all(s is None or (s["move"], s["intent"]) == (move, intent) for s in statuses), "move result changed original")
    return dict(response=response, scope=scope, request=request, move=move, intent=intent,
                pending=pending, committed=committed, statuses=statuses, wires=wires, mask=mask)


def placement(encoded):
    c = P.Cursor(P.raw(encoded))
    publication, owner = P.body(c.field(256), b"RPB\1"), P.body(c.field(256), b"ROF\1")
    c.end()
    require(len(publication) == 104 and len(owner) == 73 and publication[:16] == owner[:16] == P.GRAPH, "native placement codecs")
    p, o = P.Cursor(publication[16:]), P.Cursor(owner[16:])
    rid, first, last, initial_partition, initial_epoch = (p.number() for _ in range(5))
    config, source, index = p.take(32), p.number(), p.number()
    p.end()
    owner_id, partition, epoch, owner_config, fenced = o.number(), o.number(), o.number(), o.take(32), o.number(1)
    o.end()
    require(rid == owner_id == 1 and first == last == 1 and initial_partition == source == 3 and initial_epoch > 0 and index > 0 and config == owner_config and any(config) and partition in (3, 8) and epoch >= initial_epoch and fenced <= 1, "immutable split reservation vs mutable placement")
    return publication, dict(partition=partition, epoch=epoch, config=config, fenced=fenced)


def label_changes(logical, partition, round_, expected_axis, seen):
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
    require(owner == (1 if partition == 3 else 17) and owner not in seen and life == owner + 1 and kind == 2 and name == b"family-conflict" and member == 0 and c.number(4) == 1, "label CDC complete exact component key/one-based axis")
    scope = b"TS\1" + bytes([profile]) + axis_id + definition + bytes([6])
    require(c.field(53) == scope, "label CDC whole native-domain ownership")
    changes = P.Cursor(c.field())
    c.end()
    require(changes.take(4) == b"CC\1" + bytes([profile]) and changes.take(16) == axis_id and changes.take(32) == definition and changes.number(4) == 1 and changes.field(53) == scope and changes.take(34) == bytes(34), "label CDC exact original absent before image")
    require(changes.number(1) == changes.number(1) == 1 and [changes.number() for _ in range(4)] == [0, 0, round_, 71], "label CDC exact present-null after/revision/provenance")
    changes.end()
    seen.add(owner)


def exploration(data, records, *, semantic_require=require):
    e, case = data["Exploration"], data["Case"]
    phase = "prepared" if "prepared" in case else "registered"
    first = case.endswith("true")
    b_first = case.startswith("explore-B-")
    order = (["freeze-A-B", "B-" + phase, "fence-blocked-by-original", "B-caller-canceled", "A-native-commit-and-resolve", "B-native-abort"] if b_first else ["freeze-A-B", "A-" + phase] + (["fence-blocked-by-original", "B-native-abort"] if first else ["B-native-abort", "fence-blocked-by-original"]) + ["A-native-commit-and-resolve"])
    order += ["native-fence-capture-transfer-finish"]
    require(e["Order"] == order, "actual finite causal order")
    octets(e["MoveOwnerCookie"], 16, True)
    parsed = {name: range_read(e[name]) for name in ("Before", "Settled", "Transferred", "Held", "Reopened")}
    scopes, old_round, initial = parsed["Before"]
    old_preserved = e["Held"] == e["Reopened"] == e["Before"] and parsed["Held"][2] == parsed["Reopened"][2] == initial
    current = parsed["Settled"][2]
    transferred_preserved = current == parsed["Transferred"][2] and current != initial
    require(len(e["Originals"]) == len(e["Manifests"]) == len(e["Results"]) == 2, "exact two authentic competing originals")
    coordinators = (8, 3) if b_first else (3, 8)
    a, b = [C.decision_manifest(P.raw(source), P.raw(manifest), coordinator, scopes, {"Cut": e["Before"]["Cut"]}) for source, manifest, coordinator in zip(e["Originals"], e["Manifests"], coordinators)]
    require(a["tx"] < b["tx"] and a["tx"] != b["tx"] and P.business_hash(a["wire"], a["coordinator"]) != P.business_hash(b["wire"], b["coordinator"]), "actual qualified competing original priority")
    pending, complete, replay = [move_result(e[name]) for name in ("MovePending", "MoveComplete", "MoveReplay")]
    identity = lambda r: (r["scope"], r["request"], r["move"], r["intent"])
    require(identity(pending) == identity(complete) == identity(replay) and pending["scope"] == scopes[3] and pending["pending"] == 1 and pending["committed"] == 0 and pending["statuses"] == [None] * 3, "pre-fence refusal synthesized capture/decision")
    require(complete["pending"] == complete["mask"] == 0 and complete["committed"] == 1 and replay["pending"] == replay["mask"] == 0 and replay["committed"] == 1 and replay["wires"] == complete["wires"], "same live owned real transfer/replay")
    phases, moves = originals(data, records, {a["tx"]: a, b["tx"]: b}, complete)
    da, db = decision(phases[a["tx"]], a, True, old_round), decision(phases[b["tx"]], b, False, old_round)
    seen_labels, activation_epochs = set(), {}
    require(initial["A/fact/1"]["axis"] == initial["B/fact/17"]["axis"], "original native axis agreement")
    for (partition, index), row in records.items():
        if not P.raw(row["CDC"]):
            continue
        if P.raw(row["Outcome"])[:4] == b"GMO\1":
            require(partition not in activation_epochs, "duplicate activation CDC")
            activation_epochs[partition] = move_changes(P.raw(row["CDC"]), moves[partition, index])
            continue
        status = P.status(P.raw(row["Outcome"]))
        require(status["tx"] == a["tx"] and status["phase"] == 4 and status["index"] == index and status["commit"], "non-original conflict/control business CDC")
        envelope = P.Cursor(P.body(P.raw(row["CDC"]), b"GCE\1"))
        envelope.take(1 + 16 + 32 + 8)
        label_changes(envelope.field(), partition, da["round"], bytes.fromhex(initial["A/fact/1"]["axis"]), seen_labels)
        envelope.end()
    require(seen_labels == {1, 17}, "missing complete two-partition label CDC")
    for descriptor, d, encoded in zip((a, b), (da, db), e["Results"]):
        row = records[descriptor["coordinator"], d["index"]]
        P.resumed(encoded, scopes, descriptor["source"]["request"], descriptor["tx"], P.business_hash(descriptor["wire"], descriptor["coordinator"]), 0, P.raw(row["Outcome"]))
    held = b if b_first else a
    P.resumed(e["Pending"], scopes, held["source"]["request"], held["tx"], P.business_hash(held["wire"], held["coordinator"]), 1, b"")
    gate = e["Gate"]
    if phase == "prepared":
        _, manifest, vote, _ = P.peer_phase(P.raw(gate["Request"]), P.raw(gate["Reply"]), scopes, 1)
        require(manifest == held["manifest"] and vote["phase"] == 2 and vote["yes"] and vote["tx"] == held["tx"], "actual held YES boundary")
    else:
        command = C.decision_command(C._source_command(gate["Request"], scopes))
        require(command["phase"] == 2 and command["partition"] == 3 and command["manifest"] == held["manifest"] and not P.raw(gate["Reply"]), "actual pre-Prepare held boundary")
    source, target, durable_decision = complete["statuses"]
    require(all(s is not None and s["result"] == 1 for s in (source, target, durable_decision)), "real move original status missing")
    require((source["partition"], source["phase"], target["partition"], target["phase"], durable_decision["partition"], durable_decision["phase"]) == (3, 8, 8, 8, 3, 6) and source["round"] == target["round"] == durable_decision["round"] > max(da["round"], db["round"]), "actual source/target finish and first move decision")
    require(source["rows"] == target["rows"] > 0 and source["size"] == target["size"] > 0 and source["digest"] == target["digest"] == durable_decision["digest"] and any(source["digest"]), "real transfer complete-source rows/bytes/digest agreement")
    for s in complete["statuses"]:
        require(moves.get((s["partition"], s["index"])) == s, "move receipt not actual immutable retained original")
    before_publication, before_owner = placement(e["PlacementBefore"])
    require(before_owner["partition"] == 3 and before_owner["fenced"] == 0 and len(e["PlacementAfter"]) == 6 and len(set(e["PlacementAfter"])) == 1, "all-six genuine native placement census")
    after_publication, after_owner = placement(e["PlacementAfter"][0])
    require(before_publication == after_publication and after_owner == dict(before_owner, partition=8, epoch=before_owner["epoch"] + 1), "transfer altered original reservation or failed physical target activation")
    require(activation_epochs == {3: before_owner["epoch"], 8: before_owner["epoch"]}, "both genuine activation CDC originals preserve exact placement lineage")
    require(not data["Ticks"] and not data["Network"] and not data["Clocks"] and not data["Expiry"] and not data["PacketEvents"] and not data["Processes"] and data["Model"] is None
            and data["IO"] is None and data["Corruption"] is None and data["OpenRefusal"] is None and data["Repair"] is None and not data["CorruptArchive"]
            and not data["Snapshot"] and data["HeldPacket"] is None and data["HeldTerm"] == 0 and not P.raw(data["LeaderBefore"]) and not P.raw(data["LeaderAfter"]), "foreign exploration diagnostics")
    # No semantic classification precedes actual original/protocol/CDC/full
    # transfer/placement authentication. Legacy callers retain ValueError.
    semantic_require(old_preserved, "old-cut:composed-transfer")
    semantic_require(transferred_preserved, "composed-transfer-state")
    expected = {k: dict(v, labels=["family-conflict"]) if k in ("A/fact/1", "B/fact/17") else v for k, v in initial.items()}
    semantic_require(current == expected, "composed-conflict-state")
    changes = P.writes(initial, current, ("A/fact/1", "B/fact/17"))
    transactions = {"A": dict(reads=[P.read_key(initial, k) for k in ("A/fact/1", "B/fact/17")], writes=changes), "B": dict(reads=[], writes={})}
    observations = [dict(event=name, scope=["A", "B"], round=round_, values=values) for name, (_, round_, values) in parsed.items()]
    witness, detail = P.oracle()({"initial": initial, "transactions": transactions}, {"A"}, {"A": da["round"], "B": db["round"]}, observations, [dict(event="final", scope=["A", "B"], values=current)])
    semantic_require(witness is not None, "independent exact serial/cut witness: " + str(detail))
    return witness, scopes


def snapshot_manifest(wire, scopes):
    c = P.Cursor(wire)
    require(c.take(4) == b"AS\5\0", "actual AS5 source contract")
    identity, contract = c.take(40), c.take(80)
    numbers = list(struct.unpack(">10Q", contract))
    require(numbers[0] == 3 and all(0 < n <= 64 << 20 for n in numbers), "actual AS5 finite native policy")
    index, term = c.number(), c.number()
    ns_bytes, ns_rows = [c.number() for _ in range(4)], [c.number() for _ in range(4)]
    image_hash, records_hash, cn, size = c.take(32), c.take(32), c.number(4), c.number(4)
    cut, semantic = c.take(32), c.take(32)
    control_bytes, control_rows, recovery_bytes, recovery_rows = (c.number() for _ in range(4))
    root = c.field(65536)
    require(72 <= len(root) <= 65536 and root[:4] == b"RS\1\0" and len(root) == 72 + int.from_bytes(root[68:72], "big") * 24, "actual bounded AS5 recovery ledger")
    require(cn <= 32768 and size == 172 and size <= numbers[3] and index > 0 and term > 0, "actual AS5 image/index/policy")
    conf, image = c.take(cn), c.take(size)
    c.end()
    require(identity == scopes[3][:40] and semantic == scopes[3][88:120] and hashlib.sha256(image).digest() == image_hash and ns_rows[1:] == [index] * 3 and recovery_rows == index, "actual complete retained AS5 cut binding")
    voters = protobuf_fields(conf)
    # Repeated uint64 membership may use packed or unpacked protobuf form.
    actual_voters = []
    for value in voters.get(1, []):
        if type(value) is int:
            actual_voters.append(value)
        else:
            packed = P.Cursor(value)
            while packed.wire:
                number, shift = 0, 0
                while True:
                    octet = packed.number(1)
                    require(shift < 64 and (shift < 63 or octet <= 1), "membership varint overflow")
                    number |= (octet & 127) << shift
                    if octet < 128:
                        break
                    shift += 7
                actual_voters.append(number)
    require(actual_voters == [1, 2, 3] and set(voters) <= {1, 5} and all(v in (0, b"") for v in voters.get(5, [])), "actual native donor configuration")
    recovery_hash = hashlib.sha256(root).digest()
    totals = [sum(ns_bytes) + control_bytes + recovery_bytes, sum(ns_rows) + control_rows + recovery_rows]
    require(all(n <= 1 << 40 for n in ns_bytes + ns_rows + [control_bytes, control_rows, recovery_bytes, recovery_rows] + totals), "AS5 finite retained ledgers")
    cut_body = (b"rho-tkg:published-application-cut:v4\0" + semantic + identity + contract
                + struct.pack(">5Q", index, term, size, *totals)
                + struct.pack(">4Q", control_bytes, control_rows, recovery_bytes, recovery_rows)
                + recovery_hash + image_hash + struct.pack(">Q", cn) + conf)
    require(hashlib.sha256(cut_body).digest() == cut, "actual native published cut hash/configuration")
    manifest_body = (b"rho-tkg:application-snapshot-manifest:v5\0" + semantic + cut + records_hash
                     + struct.pack(">8Q", *ns_bytes, *ns_rows)
                     + struct.pack(">4Q", control_bytes, control_rows, recovery_bytes, recovery_rows) + recovery_hash)
    manifest_id = hashlib.sha256(manifest_body).digest()
    seed = hashlib.sha256(b"rho-tkg:application-snapshot-records:v5\0" + semantic + cut + identity + contract + struct.pack(">2Q", index, term) + image_hash).digest()
    descriptor = (b"AD\4\0" + semantic + identity + contract + struct.pack(">5Q", index, term, size, *totals)
                  + struct.pack(">4Q", control_bytes, control_rows, recovery_bytes, recovery_rows)
                  + recovery_hash + image_hash + cut + manifest_id + struct.pack(">I", cn) + conf)
    require(len(descriptor) <= 512, "actual native snapshot descriptor limit")
    return dict(index=index, term=term, image=image, cut=cut, manifest=manifest_id,
                ns_bytes=ns_bytes, ns_rows=ns_rows, control_bytes=control_bytes, control_rows=control_rows,
                recovery_bytes=recovery_bytes, recovery_rows=recovery_rows, root=root, records_hash=records_hash,
                seed=seed, contract=numbers, descriptor=descriptor, conf=conf)


def snapshot_chunk(wire, manifest, state):
    c = P.Cursor(wire)
    require(c.number(4) == 5 and c.take(32) == manifest["cut"] and c.take(32) == manifest["manifest"] and c.number() == state["sequence"], "actual snapshot chunk original/sequence")
    visited, visited_bytes, final = c.number(), c.number(), c.number(1)
    after, data = c.field(2059), c.field(256 << 10)
    c.end()
    require(visited <= 64 and visited_bytes <= 256 << 10 and final <= 1 and (final or visited > 0) and (after >= state["after"]), "snapshot bounded physical progress")
    entries, page_bytes, raw = 0, 0, P.Cursor(data)
    while raw.wire:
        kn, vn = raw.number(4), raw.number(4)
        require(0 < kn <= 2 * manifest["contract"][1] + 11 and 36 <= vn <= max(manifest["contract"][2], manifest["contract"][3], manifest["contract"][8], manifest["contract"][9]) + 36, "snapshot bounded actual frame")
        key, frame = raw.take(kn), raw.take(vn)
        require(key > state["last"] and frame[:3] == b"AV\1" and frame[3] <= 1 and (not frame[3] or len(frame) == 36) and hashlib.sha256(key + frame[:4] + frame[36:]).digest() == frame[4:36], "snapshot canonical ordering/native frame seal")
        tag = key[0]
        require(tag in (8, 9, 10, 11, 18, 20) and (tag == 8 or frame[3] == 0), "snapshot native namespace/tombstone")
        if tag in (9, 10, 11, 20):
            require(len(key) == 9, "snapshot native prefix index key")
            index = int.from_bytes(key[1:], "big")
            require(index == state["indexes"].get(tag, 0) + 1 <= manifest["index"], "snapshot complete Root/CDC/Outcome/recovery prefix")
            state["indexes"][tag] = index
            if index == manifest["index"] and tag in (9, 20):
                require(frame[36:] == manifest["image" if tag == 9 else "root"], "snapshot final root/recovery authority")
        slot = tag - 8 if tag <= 11 else 4 if tag == 18 else 5
        state["rows"][slot] += 1
        state["bytes"][slot] += kn + vn
        state["digest"] = hashlib.sha256(state["digest"] + struct.pack(">2I", kn, vn) + key + frame).digest()
        state["last"], entries, page_bytes = key, entries + 1, page_bytes + 8 + kn + vn
    require(entries <= visited and page_bytes <= visited_bytes and after >= state["last"] and (not after or after > state["after"] or final), "snapshot declared progress lacks actual bounded contents")
    state["sequence"], state["after"], state["final"] = state["sequence"] + 1, after, bool(final)
    if final:
        require(state["rows"] == manifest["ns_rows"] + [manifest["control_rows"], manifest["recovery_rows"]] and state["bytes"] == manifest["ns_bytes"] + [manifest["control_bytes"], manifest["recovery_bytes"]] and state["digest"] == manifest["records_hash"], "snapshot full genuine EOF and namespace/hash ledgers")
    return final


def snapshots(data, scopes):
    rows, case = data["Snapshot"] or [], data["Case"]
    if not case.startswith("snapshot-"):
        require(not rows, "orphan native snapshot schedule")
        return
    require(3 <= len(rows) <= 16, "actual snapshot interruption/recovery boundary census")
    attempts, current, activated = [], None, False
    for row in rows:
        require(set(row) == {"Phase", "Donor", "Receiver", "Owner", "Manifest", "Chunk", "Packet", "ResponseWire"}, "actual snapshot diagnostic grammar")
        require(row["Donor"] == 1 and row["Receiver"] == 3 and integer(row["Owner"], minimum=1), "actual sender/receiver capability diagnostic")
        manifest_wire, response = P.raw(row["Manifest"]), process(P.raw(row["ResponseWire"]))
        manifest = snapshot_manifest(manifest_wire, scopes)
        packet_ = row["Packet"]
        require(packet_["From"] == 1 and packet_["To"] == 3 and packet_["Snapshot"] is True, "actual MsgSnap envelope")
        message = raft_packet(P.raw(packet_["Payload"]), scopes, 3)
        require(message.get(1) == [7] and message.get(2) == [3] and message.get(3) == [1] and len(message.get(9, [])) == 1, "actual native MsgSnap receiver")
        snapshot = protobuf_fields(message[9][0])
        require(set(snapshot) == {1, 2} and snapshot[1] == [manifest["descriptor"]] and len(snapshot[2]) == 1, "actual MsgSnap bound AS5 descriptor")
        metadata = protobuf_fields(snapshot[2][0])
        require(metadata == {1: [manifest["conf"]], 2: [manifest["index"]], 3: [manifest["term"]]}, "actual MsgSnap native cut/configuration/index/term")
        identity = row["Owner"], response["incarnation"], manifest["manifest"], hashlib.sha256(P.raw(packet_["Payload"])).digest()
        if current is None or current["identity"] != identity:
            require(not activated and row["Phase"] == "after-append" and len(attempts) < 2, "new native snapshot owner must begin with actual Append")
            current = dict(identity=identity, sequence=0, after=b"", last=b"", final=False, prepared=False, rows=[0] * 6, bytes=[0] * 6, indexes={}, digest=manifest["seed"])
            attempts.append(current)
        require(response["code"] == response["mask"] == 0 and not response["outcome"] and not response["changes"], "snapshot diagnostics invented graph result")
        phase = row["Phase"]
        if phase == "after-append":
            require(not current["final"] and not current["prepared"], "Append after complete/verified owner")
            snapshot_chunk(P.raw(row["Chunk"]), manifest, current)
        elif phase == "before-activate":
            require(current["final"] and not current["prepared"] and not P.raw(row["Chunk"]) and response["stats"][11] == response["stats"][23] == response["stats"][24] == 1, "actual verified dormant receiver capability")
            current["prepared"] = True
        elif phase == "after-activate":
            require(current["prepared"] and current["final"] and not activated and not P.raw(row["Chunk"]) and response["stats"][1] == response["stats"][5] == manifest["index"] and response["cut"] == manifest["cut"] and response["manifest"] == manifest["manifest"], "actual native atomic-KV activation checkpoint/audit/cut")
            activated = True
        else:
            raise ValueError("unknown actual snapshot boundary")
    require(len(attempts) == 2 and activated and rows[-1]["Phase"] == "after-activate", "interrupted snapshot lacked complete genuine replacement EOF/activation")
    require(not attempts[0]["final"] if case == "snapshot-chunk-interruption" else attempts[0]["final"] and attempts[0]["prepared"], "interruption at declared actual chunk vs atomic-KV boundary")


def sender_stale_request(encoded, reply, owner):
    q = P.Cursor(P.raw(encoded))
    incarnation, sequence, op, actual_owner, entity = q.take(16), q.number(), q.number(1), q.number(), q.number()
    require(incarnation == reply["incarnation"] and sequence == reply["sequence"] and op == 15 and actual_owner == owner > 0 and entity == 0 and not q.field(), "actual stale processNext request/reply correlation")
    require(q.number() == q.number() == q.number(1) == 0 and not q.field(), "stale sender request invented packet authority")
    q.end()
    require(reply["code"] == 1 and reply["mask"] == 1 << 4 and not reply["handles"] and not reply["aux"] and not reply["outcome"] and not reply["changes"] and not reply["packets"], "old sender handle must actually refuse, not unavailable or renewed authority")


def sender_active_root(before, partial, closed):
    for observed in (before, partial, closed):
        require(observed["code"] == observed["mask"] == 0 and not observed["outcome"] and not observed["changes"], "actual receiver-state diagnostic refused")
    for observed in (partial, closed):
        require(observed["incarnation"] == before["incarnation"] and observed["applied"] == before["applied"] and observed["image"] == before["image"] and observed["cut"] == before["cut"] and observed["stats"][1] == before["stats"][1], "partial import published a mixed/new active root")
    require(before["stats"][23] == closed["stats"][23] == 0 and partial["stats"][23] == 1 and partial["stats"][24] == 0, "genuine live partial import then actual Close, no verified/activated claim")


def sender_leader_read(configuration, state, request):
    # The actual owning API uses Driver.ReadIndex, which refuses a follower.
    # HardState.Vote (stats[3]) supports this observation; it is NOT leader ID.
    q = P.Cursor(P.raw(request))
    require(q.take(16) == configuration["incarnation"] and q.number() == configuration["sequence"] and q.number(1) == 29 and q.number() == 0 and q.number() == 1 and not q.field(), "exact native processCertifiedRead(observeConfiguration) request/reply")
    require(q.number() == q.number() == q.number(1) == 0 and not q.field(), "leader ReadIndex request invented packet")
    q.end()
    require(configuration["code"] == configuration["mask"] == state["code"] == state["mask"] == 0 and state["ready"] == 1 and state["stats"][2] > 0 and state["stats"][3] == 1
            and configuration["incarnation"] == state["incarnation"] and configuration["sequence"] < state["sequence"] and configuration["applied"] <= state["applied"], "actual sender leader-only ReadIndex and exact process ordering")


def sender_published_source(sender, manifest, receiver):
    # processStats.cut/manifest are ApplicationActivationAudit: a locally
    # republished Base explicitly reports zero, even for a genuine publication.
    # The published cut is authenticated independently by snapshot_manifest's
    # full cut hash, the actual MsgSnap descriptor and every imported chunk.
    require(sender["code"] == sender["mask"] == 0 and sender["stats"][5] == 0 and
            sender["cut"] == sender["manifest"] == bytes(32), "locally published donor must have no imported-activation audit")
    require(type(manifest["cut"]) is bytes and len(manifest["cut"]) == 32 and any(manifest["cut"]) and
            sender["stats"][1] == sender["applied"] == manifest["index"] and sender["image"] == manifest["image"] and
            receiver["applied"] < manifest["index"], "published sender source must exceed actual lagged receiver")


def snapshot_sender(data, scopes, records):
    e = data["SnapshotSender"]
    require(type(e) is dict and set(e) == SENDER_FIELDS and type(e["Version"]) is int and e["Version"] == 1, "finite sender evidence grammar")
    octets(e["Run"], 16, True)
    integer(e["DeadlineUnixNanos"], (1 << 63)-1, 1)
    require(e["Side"] in ("before", "after") and data["Case"] == "snapshot-sender-" + e["Side"] + "-append", "only actual in-flight boundaries")
    rows, selected = data["Snapshot"], e["SelectedSnapshot"]
    require(type(rows) is list and 5 <= len(rows) <= 16, "bounded sender/import observation inventory")
    integer(selected, len(rows)-3, 1)
    require(rows[selected]["Phase"] == e["Side"] + "-append" and rows[selected]["SenderOwner"] == integer(e["SenderOwner"], minimum=1), "selected genuine sender owner/boundary")
    before, partial, closed = [process(P.raw(e[k])) for k in ("ReceiverBefore", "ReceiverPartial", "ReceiverAfterClose")]
    sender_active_root(before, partial, closed)
    require(e["PartialOld"] == data["Before"] == data["Reopened"], "partial import changed exact old graph/phantom/index set before retry")
    sender_before, stale = process(P.raw(e["SenderBefore"])), process(P.raw(e["StaleReply"]))
    sender_after = process(P.raw(e["SenderAfter"]))
    sender_leader_read(process(P.raw(e["SenderReadIndex"])), sender_before, e["SenderReadRequest"])
    sender_leader_read(process(P.raw(e["RestartedReadIndex"])), sender_after, e["RestartedReadRequest"])
    sender_stale_request(e["StaleRequest"], stale, e["SenderOwner"])
    events_ = data["Processes"]
    lagged, failed, restarted, receiver = events_
    loss, start, rejoin = [decoded(P.raw(row["Raw"])) for row in (failed, restarted, receiver)]
    require(failed["Role"] == "snapshot-sender" and failed["Member"] == 1 and loss["Member"] == 1 and loss["PID"] > 0 and failed["Instance"].endswith("/" + sender_before["incarnation"].hex()), "actual captured sender process identity")
    local_sender = process(P.raw(start["LocallyRestoredRaw"]))
    local_receiver = process(P.raw(rejoin["LocallyRestoredRaw"]))
    require(local_sender["incarnation"] == stale["incarnation"] != sender_before["incarnation"] and local_sender["sequence"]+1 == stale["sequence"] and local_sender["applied"] >= sender_before["applied"], "old sender handle attempted after actual local Restore before new offers")
    require(sender_after["incarnation"] == stale["incarnation"] and sender_after["sequence"] > stale["sequence"] and sender_after["stats"][2] > sender_before["stats"][2], "real restarted sender leader term/identity after stale-handle refusal")
    require(loss["Instance"] == failed["Instance"] and loss["Volume"] == failed["Volume"] == start["Volume"] and loss["ExitCode"] == -1 and loss["Success"] is False and start["Member"] == 1 and start["PID"] != loss["PID"], "exact sender SIGKILL and same-directory replacement evidence")
    require(local_receiver == before and receiver["PreviousInstance"] == lagged["Instance"], "partial active-state assertion belongs to genuine receiver replacement")
    attempts, current, first_manifest, activated, abandoned = [], None, None, False, False
    for ordinal, row in enumerate(rows):
        require(type(row) is dict and set(row) == {"Phase", "Donor", "Receiver", "Owner", "Manifest", "Chunk", "Packet", "ResponseWire", "SenderOwner"} and row["Donor"] == 1 and row["Receiver"] == 3, "actual sender snapshot diagnostic grammar")
        owner, send_owner = integer(row["Owner"], minimum=1), integer(row["SenderOwner"], minimum=1)
        manifest = snapshot_manifest(P.raw(row["Manifest"]), scopes)
        if first_manifest is None: first_manifest = manifest
        require(manifest == first_manifest, "sender restart changed actual published snapshot cut or full source")
        packet_ = row["Packet"]
        require(set(packet_) == {"From", "To", "Snapshot", "Payload"} and packet_["From"] == 1 and packet_["To"] == 3 and packet_["Snapshot"] is True, "actual donor MsgSnap envelope")
        message = raft_packet(P.raw(packet_["Payload"]), scopes, 3)
        require(message.get(1) == [7] and message.get(2) == [3] and message.get(3) == [1] and len(message.get(9, [])) == 1, "real snapshot packet sender/receiver")
        attempt = len(attempts) + (1 if row["Phase"] == "after-begin" else 0)
        require(attempt in (1,2) and message.get(4) == [(sender_before if attempt == 1 else sender_after)["stats"][2]], "actual initial/restarted leader term on emitted snapshot")
        metadata = protobuf_fields(message[9][0])
        require(set(metadata) == {1, 2} and metadata[1] == [manifest["descriptor"]] and len(metadata[2]) == 1 and protobuf_fields(metadata[2][0]) == {1:[manifest["conf"]], 2:[manifest["index"]], 3:[manifest["term"]]}, "actual descriptor/configuration/index/term")
        phase, wire = row["Phase"], P.raw(row["ResponseWire"])
        response = None if phase == "before-append" else process(wire)
        if response is None:
            require(len(wire) == 381 and not any(wire), "pre-Append fabricated ACK")
        else:
            require(response["incarnation"] == before["incarnation"] and response["code"] == response["mask"] == 0 and not response["outcome"] and not response["changes"], "receiver changed or snapshot diagnostic invented graph result")
        if phase == "after-begin":
            require(current is None and not activated and len(attempts) < 2 and response["handles"] == [owner] and response["aux"] == manifest["manifest"], "actual acknowledged Begin before sender loss/retry")
            current = dict(owner=owner, sender=send_owner, sequence=0, after=b"", last=b"", final=False, prepared=False, rows=[0]*6, bytes=[0]*6, indexes={}, digest=manifest["seed"])
            attempts.append(current)
        require(current is not None and current["owner"] == owner and current["sender"] == send_owner, "snapshot operation outside actual live owner")
        if phase == "before-append":
            require(ordinal == selected and e["Side"] == "before" and len(attempts) == 1 and current["sequence"] == 0, "unselected pre-Append work")
            prospective = {**current, "rows":current["rows"][:], "bytes":current["bytes"][:], "indexes":dict(current["indexes"])}
            require(not snapshot_chunk(P.raw(row["Chunk"]), manifest, prospective), "sender killed only after complete EOF")
        elif phase == "after-append":
            require(not current["final"] and not current["prepared"], "Append after complete/verified import")
            snapshot_chunk(P.raw(row["Chunk"]), manifest, current)
        elif phase == "after-sender-close":
            require(ordinal == selected+1 and len(attempts) == 1 and not abandoned and not current["final"] and not current["prepared"] and response["stats"][23] == 0, "actual abandoned partial owner Close")
            require(current["sequence"] == (0 if e["Side"] == "before" else 1) and send_owner == e["SenderOwner"], "before/after first Append attribution")
            require(response["sequence"] > partial["sequence"] and closed["sequence"] > response["sequence"], "partial read/actual cleanup/closed state ordering")
            abandoned, current = True, None
        elif phase == "before-activate":
            require(abandoned and len(attempts) == 2 and current["final"] and not current["prepared"] and response["stats"][11] == response["stats"][23] == response["stats"][24] == 1, "unverified or partial retry imported state")
            current["prepared"] = True
        elif phase == "after-activate":
            require(abandoned and len(attempts) == 2 and current["final"] and current["prepared"] and ordinal == len(rows)-1 and response["stats"][1] == response["stats"][5] == manifest["index"] and response["cut"] == manifest["cut"] and response["manifest"] == manifest["manifest"], "complete genuine retry Ready activation")
            activated = True
        else:
            require(phase == "after-begin", "unsupported sender-loss boundary")
        if ordinal == selected:
            require(len(attempts) == 1 and not current["final"] and send_owner == e["SenderOwner"] and (phase == "before-append" or current["sequence"] == 1), "selected sender-loss did not interrupt first nonfinal page")
            if response is not None: require(response["sequence"] < partial["sequence"], "Append ACK must precede sender-loss partial-state assertion")
    require(activated and abandoned and len(attempts) == 2 and attempts[1]["final"] and not attempts[0]["final"], "missing complete independent retry EOF")
    sender_published_source(sender_before, first_manifest, before)
    m = P.Cursor(P.body(P.raw(data["Manifest"]), b"GCM\1")); m.take(16); coordinator = m.number()
    descriptor = C.decision_manifest(P.raw(data["Original"]), P.raw(data["Manifest"]), coordinator, scopes, data["Before"])
    phases, _ = originals(data, records, {descriptor["tx"]:descriptor})
    d = decision(phases[descriptor["tx"]], descriptor, True, P.cut(data["Before"])[1])
    original = P.raw(records[coordinator, d["index"]]["Outcome"])
    for name in ("OriginalReply", "ReplayReply"):
        reply = process(P.raw(e[name]))
        require(reply["code"] == reply["mask"] == 0, "same-original reply missing/refused")
        P.resumed(e[name], scopes, descriptor["source"]["request"], descriptor["tx"], P.business_hash(descriptor["wire"], coordinator), 0, original)
    require(process(P.raw(e["OriginalReply"]))["aux"] == process(P.raw(e["ReplayReply"]))["aux"], "same original must keep exact result/IDs after sender restart")


def check(data):
    if type(data) is dict and data.get("Profile") == SENDER_PROFILE:
        require(set(data) == HEADER_KEYS | {"SnapshotSender"} and type(data["Version"]) is int and data["Version"] == 3 and data["Case"] in SENDER_CASES, "fresh closed sender-loss artifact")
        return check_source(data)
    require(type(data) is dict and set(data) == HEADER_KEYS, "fresh closed artifact grammar")
    require(data["Version"] == 1 and data["Profile"] == PROFILE and data["Case"] in CASES | EXPLORATIONS, "fresh finite D artifact contract")
    return check_source(data)


def check_source(data, *, semantic_require=require):
    """Actual-source core; each private embedding admits its own closed format."""
    integer(data["DiskBytes"])
    integer(data["AllocatedDiskBytes"])
    require(type(data["Scope"]) is str and "explicit VFS model" in data["Scope"] and "matrix remains separate" in data["Scope"], "honest qualification boundary")
    require(data["Refused"] is None and data["RefusedReplay"] is None and all(not P.raw(data[name][field]) for name in ("InstalledA", "WithheldB") for field in ("Request", "Reply")), "old-family diagnostic union cannot enter fresh D contract")
    records = prefix(data)
    require((data["Exploration"] is not None) == (data["Case"] in EXPLORATIONS), "actual bounded exploration inventory")
    if data["Exploration"] is not None:
        witness, scopes = exploration(data, records, semantic_require=semantic_require)
    else:
        witness, scopes = ordinary(data, records)
    selected = model(data)
    require((selected is not None) == (data["Case"] == "torn-wal-synced-image" or data["Case"].startswith("durable-native-")), "explicit synchronized-byte model cases")
    corruption(data, selected)
    io(data)
    processes(data)
    if data["Case"] in SENDER_CASES:
        snapshot_sender(data, scopes, records)
    else:
        snapshots(data, scopes)
    return dict(case=data["Case"], records=len(records), serial_witness=witness,
                scope="finite genuine-source D bridge; runtime qualification pending; explicit synced-image model, no physical power-loss or matrix/capacity claim")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path, nargs="+")
    parser.add_argument("--complete-slice", action="store_true", help="require the exact declared finite OS-child case inventory, not exhaustive event permutations")
    args = parser.parse_args()
    require(len(args.artifact) <= len(CASES | EXPLORATIONS), "finite family inventory")
    results, seen = [], set()
    for path in args.artifact:
        result = check(load(path))
        require(result["case"] not in seen, "duplicate qualified finite case")
        seen.add(result["case"])
        results.append(result)
    if args.complete_slice:
        require(seen == CASES | EXPLORATIONS, "missing declared genuine OS-child schedule")
    print(json.dumps(dict(results=results, complete_slice=args.complete_slice,
                         separate_source_component="TestRangeMoveRejectsFirstDelayedCrossPrepareAndSettlesInertLateRegistration uses six genuine native hosts for fence-first stale originals; this checker does not relabel it OS-child evidence"), sort_keys=True))


if __name__ == "__main__":
    main()
