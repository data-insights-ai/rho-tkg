#!/usr/bin/env python3
"""Private genuine native contention/owned-transport artifact verifier.

Only fixed owner metadata and one admitted page are resident. Long-lived raw
entry/original/predecessor joins use the unchanged finite disposable spool.
Actual native hydration is a Go-exporter boundary pending runtime qualification;
checksummed artifact bytes alone are never portable source or decision authority.
"""
import base64
from contextlib import nullcontext
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import struct
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from comparison_spool import Policy, Spool, u64
import native_columns_check as columns_codec

_spec = importlib.util.spec_from_file_location("native_contention_raw", ROOT / "compare-cost-cdc.py")
raw = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(raw)
require, fields, integer, blob, encoded = raw.require, raw.fields, raw.integer, raw.raw_bytes, raw.go_json
LINE = 512 << 10
FORMAT, FAMILY = "native-contention-empty-range-v1", "contention-empty-range"
WORK = columns_codec.WORK
WORK_MAX = "MaxWorkRecords MaxWorkBytes MaxDecodedRows MaxOutputBytes MaxRPCs MaxWireBytes".split()
AGREEMENT = hashlib.sha256(b"rho-tkg:V2:durable-native-change-columns:v2\0exact-initial-final;retained-original-keeper;dependency-min;explicit-generation-retirement\0").digest()
MAX_SECONDS = 365*24*60*60


def owner_limits(end):
    seconds = integer(end["BudgetSeconds"], 1, MAX_SECONDS)
    return seconds//60+13, 32+seconds//60+4, seconds//60


def causal_minimum(s, rtt_millis):
    shape(s)
    require(type(rtt_millis) is int and rtt_millis in (0, 1, 20, 100), "actual native RTT profile")
    remote = s["facts"]*s["crossPercent"]//100
    return (s["facts"]+max(remote, s["facts"]-remote)+3)*rtt_millis


def admission_digest(end):
    seconds = integer(end["BudgetSeconds"], 1, MAX_SECONDS)
    shaped = end["Transport"][0]["Spec"]
    floor = causal_minimum(end["spec"], shaped["targetRTTMillis"])
    require(end["CausalMinimumMillis"] == floor and seconds*1000 > floor, "fixed chosen budget exceeds necessary causal RTT floor only; no throughput promise")
    integer(end["DeadlineUnixNanos"], 1, (1 << 63)-1)
    a = {"Version": 1, "Spec": end["spec"], "Run": end["run"], "Transport": shaped,
         "BudgetSeconds": seconds, "CausalMinimumMillis": floor, "DeadlineUnixNanos": end["DeadlineUnixNanos"]}
    return digest(b"rho-tkg:native-contention-run-admission:v1\0"+encoded(a))


def decoded(body):
    require(type(body) is bytes and 0 < len(body) <= LINE, "bounded native artifact body")
    value = json.loads(body, object_pairs_hook=raw.pairs,
                       parse_constant=lambda _: require(False, "nonfinite native artifact"))
    require(encoded(value) == body, "canonical native artifact bytes")
    return value


def lines(path, maximum):
    integer(maximum, 1)
    with Path(path).open("rb") as source:
        for _ in range(maximum):
            body = source.readline(LINE + 1)
            if not body:
                return
            require(len(body) <= LINE and body.endswith(b"\n"), "bounded complete native artifact line")
            yield decoded(body[:-1])
        require(not source.read(1), "native artifact whole-prefix work quota")


def array(value, length):
    return columns_codec.array(raw, value, length)


def digest(body):
    return hashlib.sha256(body).digest()


def checked(body, magic, maximum=32768):
    return columns_codec.checked_body(raw, body, magic, maximum)


def cursor(body):
    return columns_codec.Cursor(raw, body)


def optional_bytes(value):
    return b"" if value is None else blob(value)


def shape(s):
    fields(s, "version family facts crossPercent writers layout")
    require(s["version"] == 1 and type(s["version"]) is int and s["family"] == FAMILY
            and integer(s["facts"], 20, 1_000_000) % 20 == 0
            and integer(s["crossPercent"]) in (0, 10, 50, 100)
            and integer(s["writers"]) in (1, 8, 32) and s["layout"] in ("kv", "columns"), "fresh native family profile")
    return s


def frame_limit(s):
    # Same finite final algorithm bound as the actual bootstrap. This is a
    # contiguous retain-all contract; sparse/collected prefixes cannot reuse it.
    n = shape(s)["facts"]
    return 16384 + (5*n+3)*8*10 + 14*n + 1024*32 + 16


def cross(s, fact):
    return (fact+1)*s["crossPercent"]//100 > fact*s["crossPercent"]//100


def target(s, phase, fact):
    if phase == "seed":
        integer(fact, 0, s["facts"]//2-1)
        return fact
    if phase == "claim":
        integer(fact, 0, s["facts"]//2-1)
        return s["facts"]//2+fact
    require(phase == "hot", "actual logical family phase")
    integer(fact, 0, s["facts"]-1)
    return next(i for i in range(10) if cross(s, i) == cross(s, fact))


def operation(s, value):
    fields(value, "Dataset Family Phase Fact Claimant Ordinal")
    phase, fact, claimant = value["Phase"], integer(value["Fact"]), integer(value["Claimant"])
    n, competitors = s["facts"], max(2, s["writers"])
    if phase == "setup":
        integer(fact, 0, 2)
        require(claimant == 0, "setup claimant")
        ordinal = fact+1
    elif phase == "seed":
        target(s, phase, fact)
        require(claimant == 0, "seed claimant")
        ordinal = 4+fact
    elif phase == "hot":
        target(s, phase, fact)
        require(claimant == 0, "hot claimant")
        ordinal = 4+n//2+fact
    else:
        target(s, phase, fact)
        integer(claimant, 0, competitors-1)
        ordinal = 4+3*n//2+fact*competitors+claimant
    dataset = digest(b"\x01"+struct.pack(">QQ", n, s["crossPercent"])+b"graph-v2")
    require(value["Family"] == FAMILY and array(value["Dataset"], 32) == dataset and value["Ordinal"] == ordinal, "native immutable logical operation")
    return phase, fact, claimant, ordinal


def cut(value):
    c = cursor(checked(blob(value), b"GEQ\1", 16384))
    require(c.number(1) == 3 and not any(c.take(16)) and c.number() == 0 and c.number(4) == 2, "complete actual At cut")
    certs = [columns_codec.certificate(raw, base64.b64encode(c.field(512)).decode()) for _ in range(2)]
    c.done()
    require([v["partition"] for v in certs] == [3, 8], "both actual cut partitions")
    return certs


def policy(p, s):
    fields(p, " ".join(columns_codec.POLICY)+" Version Admission")
    for name, value in p.items():
        if name != "Admission": integer(value, 1)
    require(p["Version"] == 1 and p["ChunkRecords"] == p["ChunkRows"] == 16
            and p["ChunkBytes"] == 65536 and p["FrameBytes"] == 32768 and p["MaxGenerations"] == 2
            and p["MaxChunks"] == 2*frame_limit(s) and p["MaxRetainedBytes"] == 8*16384+2*frame_limit(s)*(65536+128)
            and p["MaxRetainedBytes"] <= 1 << 62 and p["CacheBytes"] == 2 << 20
            and p["MemTableBytes"] == 1 << 20 and p["Lifetime"] == 60_000_000_000,
            "fresh finite moderate chunk/storage policy")


def binding(b, end, certs, group):
    s = end["spec"]
    fields(b, "TerminalAttempts Version Layout Agreement Coverage Peers Configurations Policy")
    policy(b["Policy"], s)
    require(array(b["Policy"]["Admission"], 32) == admission_digest(end), "source policy authenticates exact run/spec/transport/chosen budget/floor/original deadline")
    require(b["Version"] == 2 and b["TerminalAttempts"] == 4 and b["Layout"] == columns_codec.LAYOUT
            and array(b["Agreement"], 32) == AGREEMENT and b["Coverage"] == 1 << group
            and len(b["Peers"]) == len(b["Configurations"]) == 2, "actual durable disjoint native binding")
    axes = []
    for g in range(2):
        c = cursor(checked(blob(b["Peers"][g]), b"NCP2", 256))
        scope = checked(c.field(256), b"APS\1", 156)
        c.done()
        require(scope == certs[g]["scope"], "full actual source scope/semantic/routing/config binding")
        config = blob(b["Configurations"][g])
        body = checked(config, b"GAC\3", 4096)
        require(len(body) == 214 and body[:16].hex() == raw.GRAPH and scope[48:56] == body[16:24]
                and scope[56:88] == body[24:56]
                and digest(b"rho-tkg:partition-genesis:v1\0"+config[:-32]) == certs[g]["configuration"], "complete actual native configuration")
        axis = body[104:182]
        a = cursor(axis)
        require(any(a.take(16)) and a.number(1) == 2 and a.number(2) == 1
                and a.field(256) == raw.SCOPE["reference"].encode() and a.field(256) == raw.SCOPE["canonicalUnit"].encode(), "exact rational native axis descriptor")
        a.done()
        axes.append(axis)
    require(b["Configurations"][0] == b["Configurations"][1] and axes[0] == axes[1], "complete two-partition configuration agreement")
    return axes[group]


def monotone(a, b, p):
    for name, maximum in zip(WORK, WORK_MAX, strict=True):
        require(integer(a[name]) <= integer(b[name]) <= p[maximum], "cumulative source/decode/RPC/wire/output grant refund/exhaustion")


def cross_status(body):
    c = cursor(checked(body, b"GCS\1", 135))
    graph, part, tx, manifest = c.take(16), c.number(), c.take(16), c.take(32)
    phase, index, floor, round_, flags, why = c.number(1), c.number(), c.number(), c.number(), c.number(1), c.number(1)
    c.done()
    require(graph.hex() == raw.GRAPH and part in (3, 8) and any(tx) and any(manifest)
            and phase in (1, 2, 3, 4) and index > 0 and flags <= 3 and why in (0, 1, 2, 3, 4, 5, 6, 8),
            "actual cross status namespace/enum/original")
    return {"part": part, "tx": tx.hex(), "digest": manifest.hex(), "phase": phase, "index": index,
            "floor": floor, "round": round_, "yes": bool(flags & 1), "commit": bool(flags & 2), "why": why}


def manifest(body):
    c = cursor(checked(body, b"GCM\2"))
    base, condition = c.field(32768), c.field(32768)
    c.done()
    b = cursor(checked(base, b"GCM\1"))
    tx, coordinator, proposal = b.take(16), b.number(), b.field(32768)
    configuration, declaration, routing, template = [b.take(32) for _ in range(4)]
    count = b.number(4)
    require(1 <= count <= 2 and coordinator in (3, 8), "genuine native manifest participant shape")
    parts = []
    for _ in range(count):
        part, floor, kernel = b.number(), b.number(), b.field(32768)
        require(part in (3, 8) and (not parts or part > parts[-1][0]) and len(kernel) > 32, "ordered actual native prepared kernels")
        # The native Go factory/inspection authenticates full prepared kernels;
        # this verifier binds those exact immutable bytes, never reissues them.
        parts.append((part, floor, digest(kernel).hex()))
    b.done()
    q = cursor(checked(condition, b"GUC\1"))
    require(q.number(4) == count, "condition covers every actual manifest participant")
    for part, _, _ in parts:
        cert = columns_codec.certificate(raw, base64.b64encode(q.field(512)).decode())
        prefix, authority = q.number(), q.take(32)
        require(cert["partition"] == part and cert["configuration"] == configuration and cert["scope"][56:88] == declaration
                and prefix >= cert["index"] and any(authority), "full original condition/prefix/authority association")
    q.done()
    require(any(configuration) and any(declaration) and any(routing) and any(template), "complete manifest descriptors")
    return {"tx": tx.hex(), "coordinator": coordinator, "proposal": base64.b64encode(proposal).decode(),
            "condition": base64.b64encode(condition).decode(), "parts": parts, "digest": digest(body).hex()}


def frame_digest(f):
    fields(f, "Partition Index Root Outcome CDC Registration OriginalOutcome OriginalCDC Rows")
    body = u64(integer(f["Partition"]))+u64(integer(f["Index"]))+array(f["Root"], 172)
    for name in ("Outcome", "CDC", "Registration", "OriginalOutcome", "OriginalCDC"):
        value = optional_bytes(f[name])
        require(len(value) <= 32768, "raw original frame field admission")
        body += struct.pack(">I", len(value))+value
    normalized = []
    for row in f["Rows"] or []:
        fields(row, " ".join(columns_codec.ROW))
        normalized.append({name: ({key: row[name][key] for key in columns_codec.CELL} if name in ("Before", "After") else row[name])
                           for name in columns_codec.ROW})
    return digest(body+encoded(normalized))


def business_cdc(cdc, status, image, prior_root):
    c = cursor(checked(cdc, b"GCE\1"))
    require(c.number(1) == 6 and c.take(16).hex() == status["tx"] and c.take(32).hex() == status["digest"]
            and c.number() == status["index"], "actual retained CDC envelope binds SAME native original/status")
    logical = c.field(32768); c.done()
    body = checked(logical, b"GCD\5")
    require(body[:16].hex() == raw.GRAPH and body[16:24] == u64(status["part"])
            and len(body) >= 53 and body[24:53] == bytes(29), "exact native non-bootstrap logical envelope namespace")
    expected = digest(b"rho-tkg:graph-effects:v1\0"+prior_root[52:84]+u64(len(logical))+logical)
    require(image[52:84] == expected, "independent raw logical CDC to native root effect-chain binding")


def authenticate_frame(f, prior_root, spool, fresh):
    part, index = f["Partition"], integer(f["Index"], 1)
    image = array(f["Root"], 172)
    columns_codec.root(raw, image, part)
    out, cdc, registration = [optional_bytes(f[n]) for n in ("Outcome", "CDC", "Registration")]
    require(f["Outcome"] is not None and f["CDC"] is not None, "missing originals cannot become present-empty")
    rows = f["Rows"] or []
    require(type(rows) is list and len(rows) <= 16, "actual bounded decoded chunk rows")
    if out.startswith(b"GCS\1"):
        status = cross_status(out)
        m = manifest(registration)
        require(status["part"] == part and status["index"] <= index and status["tx"] == m["tx"] and status["digest"] == m["digest"], "cross status/actual immutable registration")
        txkey = bytes.fromhex(m["tx"])
        stored = spool.find(b"manifest", txkey)
        if stored is None:
            require(fresh, "manifest absent from authentic initial source")
            spool.insert(b"manifest", txkey, registration)
        else:
            require(stored == registration, "immutable manifest changed across phase/voter/reopen")
        if status["index"] < index:
            require(not rows and not cdc and image == prior_root and optional_bytes(f["OriginalOutcome"]) == out
                    and digest(optional_bytes(f["OriginalCDC"])) == spool.lookup(b"original-cdc", u64(part)+u64(status["index"])), "actual later replay/earlier original sidecars")
        elif fresh:
            require(not optional_bytes(f["OriginalOutcome"]) and not optional_bytes(f["OriginalCDC"]), "invented original replay sidecars")
            spool.insert(b"phase", u64(part)+txkey+bytes([status["phase"]]), out)
            spool.insert(b"original-cdc", u64(part)+u64(index), digest(cdc))
            if status["phase"] == 4 and status["commit"]:
                require(status["round"] > 0 and cdc, "committed genuine Resolve missing present envelope")
                business_cdc(cdc, status, image, prior_root)
            else:
                require(not cdc and not rows and image == prior_root, "protocol/ABORT duplicated business changes or changed graph image")
        return status, m
    require(not rows, "unknown nonbusiness outcome supplied business rows")
    if out.startswith(b"GPO\1"):
        c = cursor(checked(out, b"GPO\1", 256))
        phase, status, pin, pin_digest, action = c.number(1), c.number(1), c.take(32), c.take(32), c.take(16)
        original, publication, epoch, feed = [c.number() for _ in range(4)]
        c.done()
        require(1 <= phase <= 8 and 1 <= status <= 5 and any(pin) and any(pin_digest) and any(action)
                and original <= index and image == prior_root, "genuine pin control unchanged graph image")
        if status == 2 and original == index:
            require(not registration and not cdc, "actual pending pin stage union")
        else:
            a = cursor(checked(registration, b"NCO1"))
            key, at, value = a.field(4096), a.number(), a.field(32768)
            a.done()
            native = checked(value, b"GPX\1", 512)
            require(key[:3] == b"ga\x18" and len(key) == 59 and key[3:19].hex() == raw.GRAPH
                    and key[19:27] == u64(part) and key[-16:] == action and at == original
                    and any(native[:32]) and native[32:] == out, "actual immutable pin original control sidecar")
            require(not cdc or original == index and status == 1 and cdc == out, "pin control CDC union")
        if fresh and original == index:
            spool.insert(b"pin-result", u64(part)+u64(index), encoded({"phase": phase, "status": status, "pin": pin.hex(), "digest": pin_digest.hex(), "publication": publication, "epoch": epoch, "feed": feed}))
            if phase in (3, 4, 5, 8) and status in (1, 4, 5):
                raw.put_raw(spool, b"pin-terminal", u64(part)+pin+pin_digest, out)
        elif original < index:
            require(optional_bytes(f["OriginalOutcome"]) == out and not cdc
                    and spool.lookup(b"original-bytes", u64(part)+u64(original)) == digest(out)+digest(optional_bytes(f["OriginalCDC"])),
                    "actual pin replay sidecars bind earlier full native entry")
        return None, None
    require(not registration and not optional_bytes(f["OriginalOutcome"]) and not optional_bytes(f["OriginalCDC"]), "unknown nonbusiness original sidecar")
    if out.startswith(b"GSO\1"):
        b = raw.fixed_body(out, b"GSO\1", 54)
        phase, status, round_, original = struct.unpack(">BBQQ", b)
        require(phase in (1, 2) and status == 1 and round_ > 0 and original == index and not cdc and image == prior_root, "actual read-control prefix")
    elif out.startswith(b"GRO\2"):
        c = cursor(checked(out, b"GRO\2", 120))
        kind, graph, namespace, identity, body_hash = c.number(1), c.take(16), c.number(), c.take(16), c.take(32)
        original, disposition, reason = c.number(), c.number(1), c.number(2)
        c.done()
        require(graph.hex() == raw.GRAPH and namespace == part and any(identity) and any(body_hash)
                and original <= index and disposition in (1, 2, 3, 4) and reason == 0 and not cdc,
                "actual allocation/recipient protocol original, not a graph change")
        observed = spool.find(b"grant-entry", u64(part)+u64(index))
        if observed is not None:
            require(observed == digest(out+cdc+image), "authentic before-submit grant original entry")
        if original < index:
            require(disposition == 2 and image == prior_root, "actual control replay changed image/original")
            expected = bytearray(spool.lookup(b"control-original", u64(part)+u64(original)))
            expected[85] = 2; expected[-32:] = digest(expected[:-32])
            require(out == bytes(expected), "actual recipient/grant replay binds exact earlier original")
        elif fresh:
            spool.insert(b"control-original", u64(part)+u64(index), out)
    else:
        require(not out and not cdc and image == prior_root, "unknown native empty/control outcome")
    return None, None


def proposal(body, request, ordinal=None):
    c = cursor(checked(body, b"PGQ\1", 32768))
    kind, graph, coordinator, identity = c.number(1), c.take(16), c.number(), c.take(16)
    descriptors = [c.take(32) for _ in range(3)]
    inner = c.field(32768)
    c.done()
    q = cursor(checked(inner, b"PGO\1", 32768))
    require(q.number(1) == kind and q.take(16) == graph and q.number() == coordinator and q.take(16) == identity
            and identity == request and graph.hex() == raw.GRAPH and coordinator in (3, 8)
            and all(any(v) for v in descriptors), "actual typed factory request/body/configuration identity")
    revision, provenance = q.number(), q.number()
    require(revision > 0 and provenance > 0 and (ordinal is None or provenance == ordinal), "actual immutable typed operation provenance")
    # Remaining typed payload is inspected by the actual Go factory and its
    # retained original. Keep its full hash; never decode it into replacement
    # authority or assume a PGQ label makes an operation genuine.
    return coordinator


def factory_revision(body):
    """Re-encode only the actual nested native revision, as the Go guard does."""
    c = cursor(checked(body, b"PGQ\1", 32768))
    c.take(1+16+8+16+3*32)
    n = c.number(4)
    start = 4+c.offset
    inner = bytearray(c.take(n)); c.done()
    checked(bytes(inner), b"PGO\1", 32768)
    require(len(inner) >= 4+1+16+8+16+16+32, "actual nested revision span")
    inner[45:53] = u64(1)
    inner[-32:] = digest(inner[:-32])
    normalized = bytearray(body)
    normalized[start:start+n] = inner
    normalized[-32:] = digest(normalized[:-32])
    return bytes(normalized)


def operations(directory, end, spool):
    s = end["spec"]
    counts = {"commit": 0, "native-abort": 0, "pre-prepare-stale": 0, "frozen": 0, "cohort-open": 0, "cohort-close": 0}
    owner, frozen_slots, finished = None, set(), set()  # <=32 entries in one real cohort
    actors, requests, native_calls = set(), 0, 0  # actors has at most two member IDs
    admission_nanos, call_nanos = 0, 0
    for row in lines(directory / "operations.jsonl", 8*64*(3+2*s["facts"]*max(2, s["writers"]))):
        fields(row, "Kind Actor Reply ElapsedNanos AdmissionNanos Invocations" + (" Error" if "Error" in row else ""))
        kind, r = row["Kind"], row["Reply"]
        require(row["Actor"] in (2, 4) and r["Run"] == end["run"] and not row.get("Error"), "complete actual workload operation trace")
        integer(row["ElapsedNanos"]); integer(row["AdmissionNanos"]); integer(row["Invocations"], 0, 8)
        admission_nanos += row["AdmissionNanos"]; call_nanos += row["ElapsedNanos"]
        require(max(admission_nanos, call_nanos) <= (1 << 63)-1, "checked cumulative actual caller timing")
        if kind == "cohort-open":
            require(owner is None and r["Class"] == "captured-native-prefix" and r["Cookie"]["Owner"] > 0
                    and any(array(r["Cookie"]["Request"], 16)) and 0 < r["DeadlineUnixNanos"] <= end["DeadlineUnixNanos"], "one genuine bounded cohort owner")
            owner = r
            frozen_slots.clear(); finished.clear()
            counts[kind] += 1
        elif kind == "cohort-close":
            require(owner is not None and r["Class"] == "closed-native-prefix" and r["Cookie"] == owner["Cookie"]
                    and finished == frozen_slots and 1 <= len(finished) <= 32, "drained exact frozen native cohort")
            owner = None
            counts[kind] += 1
        else:
            require(owner is not None and r["Cookie"] == owner["Cookie"], "unowned actual factory/Run result")
            slot = integer(r["Slot"], 1, 32)
            request = array(r["Request"], 16)
            phase, fact, claimant, ordinal = operation(s, r["Operation"])
            job = r["Job"]
            fields(job, "Phase Fact Claimant Client Attempt Previous")
            require((phase, fact, claimant) == (job["Phase"], job["Fact"], job["Claimant"])
                    and 0 <= integer(job["Client"]) < s["writers"] and 1 <= integer(job["Attempt"]) <= 64,
                    "actual independent client/job lineage")
            if kind == "frozen":
                require(slot not in frozen_slots and not finished and any(request), "one actual immutable factory per frozen slot")
                body = blob(r["Business"])
                require(0 < len(body) <= 8192 and digest(body) == array(r["BodySHA256"], 32)
                        and r["Capture"]["Cookie"] == owner["Cookie"] and r["Capture"]["Condition"] == owner["Capture"]["Condition"], "genuine frozen body/read-owner binding")
                coordinator = proposal(body, request, ordinal)
                require(coordinator == (8 if phase == "setup" and fact == 2 else 3), "actual request coordinator")
                previous = spool.find(b"attempt-chain", u64(ordinal))
                if job["Attempt"] == 1:
                    require(previous is None and not any(array(job["Previous"], 16)), "first immutable attempt")
                else:
                    prior = decoded(previous) if previous is not None else None
                    require(prior is not None and prior["Job"]["Attempt"]+1 == job["Attempt"]
                            and prior["Request"] == job["Previous"] and prior["Class"] in ("native-abort", "pre-prepare-stale"), "re-read/replan only after authentic known prior refusal")
                spool.insert(b"factory", request, encoded(r))
                frozen_slots.add(slot); counts[kind] += 1; requests += 1
            else:
                require(kind == "terminal" and slot in frozen_slots and slot not in finished, "one terminal result per actual frozen job")
                factory = decoded(spool.lookup(b"factory", request))
                require(all(r[k] == factory[k] for k in ("Cookie", "Slot", "Job", "Operation", "Request", "BodySHA256")), "same original across uncertain retries")
                classification = r["Class"]
                require(classification in ("commit", "native-abort", "pre-prepare-stale"), "unknown native outcome cannot complete/replan")
                calls = integer(r["Calls"], 1, 8)
                require(row["Invocations"] >= calls, "actual IPC invocation counters cannot omit native attempts")
                native_calls += calls
                if classification == "pre-prepare-stale":
                    require(r["Proof"] is None and r["Error"], "actual pre-prepare refusal, not a fabricated ABORT")
                else:
                    proof = r["Proof"]
                    fields(proof, "Manifest Outcome CDC")
                    wire = blob(proof["Manifest"])
                    m, decision = manifest(wire), cross_status(blob(proof["Outcome"]))
                    tx = digest(b"rho-tkg:cross-transaction-id:v1\0"+bytes.fromhex(raw.GRAPH)+u64(m["coordinator"])+request)[:16]
                    require(m["tx"] == tx.hex() and decision["tx"] == m["tx"] and decision["digest"] == m["digest"]
                            and decision["part"] == m["coordinator"] and decision["phase"] == 3
                            and decision["commit"] == (classification == "commit") and not blob(proof["CDC"])
                            and m["condition"] == factory["Capture"]["Condition"], "actual first-D/original/manifest/complete condition")
                    # Rebased planned revision may differ from the factory's1;
                    # actual Go validates canonical re-encoding of the rest.
                    proposal(blob(m["proposal"]), request, ordinal)
                    require(factory_revision(blob(m["proposal"])) == blob(factory["Business"]), "actual planned original equals SAME frozen typed body after only revision rebase")
                    spool.insert(b"terminal", tx, encoded({"Operation": r["Operation"], "Class": classification, "Decision": proof["Outcome"], "Manifest": proof["Manifest"]}))
                    if classification == "commit":
                        coordinate = u64(ordinal) if phase != "claim" else b"claim"+u64(fact)
                        spool.insert(b"useful", coordinate, request)
                raw.put_raw(spool, b"attempt-chain", u64(ordinal), encoded(r))
                counts[classification] += 1; finished.add(slot); actors.add(job["Client"])
    m = end["Metrics"]
    require(owner is None and counts["cohort-open"] == counts["cohort-close"] == m["Cohorts"]
            and counts["commit"] == m["Commits"] == 3+2*s["facts"] and counts["native-abort"] == m["NativeAborts"]
            and counts["pre-prepare-stale"] == m["PrePrepareRefusals"] and native_calls == m["NativeRunInvocations"]
            and m["AdmissionNanos"] == admission_nanos and m["CallNanos"] == call_nanos
            and m["Callers"] == s["writers"] and m["RunAdmissions"] == 2 and 1 <= m["MaximumActive"] <= 2,
            "complete useful work and truthful actual admission/refusal counters")
    require(actors == set(range(s["writers"])), "each declared independent caller actually reaches native Run")
    for phase, count in (("setup", 3), ("seed", s["facts"]//2), ("hot", s["facts"])):
        for fact in range(count):
            ordinal = fact+1 if phase == "setup" else 4+fact+(s["facts"]//2 if phase == "hot" else 0)
            spool.lookup(b"useful", u64(ordinal))
    for fact in range(s["facts"]//2): spool.lookup(b"useful", b"claim"+u64(fact))
    return counts


def grants(directory, spool):
    pending, count = None, 0
    for row in lines(directory / "grants.jsonl", 36):
        fields(row, "Phase Action Partition Sequence Count Proposal Outcome CDC Root Index")
        if row["Phase"] == "pending-original":
            require(pending is None and row["Index"] == 0 and row["Outcome"] is None and row["CDC"] is None, "genuine proposal saved before native grant submission")
            pending = row
        else:
            require(pending is not None and row["Phase"] == "complete-original" and all(row[k] == pending[k] for k in ("Action", "Partition", "Sequence", "Count", "Proposal")), "exact submitted native grant original lineage")
            out, cdc, image = blob(row["Outcome"]), blob(row["CDC"]), blob(row["Root"])
            body = checked(out, b"GRO\2", 120)
            proposal_ = cursor(blob(row["Proposal"]))
            scope, command = checked(proposal_.field(256), b"APS\1", 156), proposal_.field(32768)
            proposal_.done()
            require(not cdc and body[1:17].hex() == raw.GRAPH and int.from_bytes(body[17:25], "big") == row["Partition"]
                    and int.from_bytes(body[73:81], "big") == row["Index"] and body[81] in (1, 4)
                    and body[82:] == b"\0\0" and body[41:73] == digest(command)
                    and scope[:24] == body[1:25], "actual immutable grant outcome namespace/index/body/disposition")
            columns_codec.root(raw, image, row["Partition"])
            spool.insert(b"grant-entry", u64(row["Partition"])+u64(row["Index"]), digest(out+cdc+image))
            spool.insert(b"grant-original", digest(blob(row["Proposal"])), encoded(row))
            pending = None; count += 1
    require(pending is None and 4 <= count <= 18, "complete finite actual grant inventory")
    return count


def grant_replays(directory, end, spool):
    count = 0
    for row in lines(directory / "grant-replays.jsonl", 18):
        require(row["Phase"] == "actual-replay-entry", "genuine later grant replay entry")
        original = decoded(spool.lookup(b"grant-original", digest(blob(row["Proposal"]))))
        before, after = blob(original["Outcome"]), blob(row["Outcome"])
        expected = bytearray(before)
        expected[85] = 2
        expected[-32:] = digest(expected[:-32])
        require(after == bytes(expected) and row["Index"] > original["Index"] and not blob(row["CDC"])
                and all(row[k] == original[k] for k in ("Action", "Partition", "Sequence", "Count", "Proposal")), "actual entry index differs from immutable original index")
        spool.insert(b"grant-entry", u64(row["Partition"])+u64(row["Index"]), digest(after+blob(row["CDC"])+blob(row["Root"])))
        count += 1
    require(count == end["GrantReplays"], "complete grant replay actual-prefix census")
    return count


def row_key(row):
    return b"".join(u64(row[k]) for k in ("Owner", "Life", "Component"))+row["Name"].encode()


def fold_rows(frame, terminal, axis, s, old, spool):
    op = terminal["Operation"]
    phase, fact, _, ordinal = operation(s, op)
    destination = 5*s["facts"]+65
    expected_owner = (1, 3, destination)[fact] if phase == "setup" else 7+2*target(s, phase, fact)
    all_scope = b"TS\1\2"+axis[:16]+digest(b"rho-tkg:axis:v1\0"+axis)+b"\6"
    for row in frame["Rows"] or []:
        fields(row, " ".join(columns_codec.ROW))
        for name in ("Owner", "Life", "Source", "Target", "SourceLife", "TargetLife", "Component", "Kind", "Mode", "Interpretation", "Role"):
            integer(row[name])
        require(row["Owner"] == expected_owner and row["Component"] in (1, 3)
                and blob(row["Axis"]) == axis and blob(row["Scope"]) == all_scope, "actual change owner/component/full native scope")
        after, before = row["After"], row["Before"]
        for cell in (before, after):
            fields(cell, " ".join(columns_codec.CELL))
            require(type(cell["Present"]) is bool and type(cell["Null"]) is bool, "typed native cell flags")
            for name in ("Handle", "Revision", "Provenance"): integer(cell[name])
            integer(cell["Value"], -(1 << 63), (1 << 63)-1)
        require(after["Present"] and not after["Null"] and after["Provenance"] == ordinal and after["Revision"] > 0,
                "actual immutable original provenance/revision")
        if phase == "setup":
            require(row["Kind"] == 1 and row["Component"] == 1 and row["Life"] == 0 and row["Name"] == row["Type"] == ""
                    and row["Source"] == row["Target"] == row["SourceLife"] == row["TargetLife"] == 0
                    and row["Interpretation"] == 3 and row["Role"] == 1 and after["Handle"] == expected_owner+1,
                    "genuine setup node presence")
        else:
            edge = target(s, phase, fact)
            require(row["Kind"] == 2 and row["Mode"] == 1 and row["Type"] == "edge" and row["Interpretation"] == 6 and row["Role"] == 2
                    and row["Source"] == 1 and row["SourceLife"] == 2 and row["Target"] == (destination if cross(s, edge) else 3)
                    and row["TargetLife"] == row["Target"]+1, "genuine endpoint-life relationship binding")
            if row["Component"] == 1:
                require(phase != "hot" and row["Life"] == 0 and row["Name"] == "" and after["Handle"] == expected_owner+1,
                        "genuine empty-ID initial presence")
            else:
                require(row["Life"] == expected_owner+1 and row["Name"] == "p" and 5 <= after["Value"] <= s["facts"]+5
                        and after["Handle"] == 4*s["facts"]+32+after["Value"], "exact typed scalar value identity")
                require(after["Value"] == before["Value"]+1 if phase == "hot" else after["Value"] == 5, "useful genuine hot increment")
        key, body = row_key(row), encoded(row)
        prior = spool.find(b"state", key)
        absent = {"Present": False, "Null": False, "Handle": 0, "Value": 0, "Revision": 0, "Provenance": 0}
        require(before == (decoded(prior)["After"] if prior is not None else absent)
                and (prior is None or after["Revision"] > before["Revision"]), "independent actual serial predecessor chain")
        raw.put_raw(spool, b"state", key, body)
        if old: raw.put_raw(spool, b"old-state", key, body)
        coordinate = bytes.fromhex(cross_status(blob(terminal["Decision"]))["tx"])+key
        spool.insert(b"business-row", coordinate, digest(body))


def sources(directory, end, spool):
    s, first, old = end["spec"], cut(end["Initial"]), cut(end["Old"])
    finals = {"before-stop": cut(end["BeforeStop"]), "reopened-replayed": cut(end["Replayed"])}
    order = [(stage, member) for stage in finals for member in range(1, 7)]
    step, owner, previous, sequence, positions, root_image = 0, None, None, 0, None, None
    frames, business_rows, controls = 0, 0, 0
    for trace in lines(directory / "sources.jsonl", 12*(frame_limit(s)+3)):
        fields(trace, "Stage Partition Member Incarnation Action Receipt")
        require(step < len(order) and (trace["Stage"], trace["Member"]) == order[step], "exact two-cohort/all-six source inventory")
        stage, member = order[step]
        g, part = (member-1)//3, 3 if member <= 3 else 8
        r, action = trace["Receipt"], trace["Action"]
        require(trace["Partition"] == part and any(array(trace["Incarnation"], 16)) and r["Run"] == end["run"], "actual source/process run binding")
        fields(r, "Run Cookie Sequence Binding Initial Final Positions Frames EOF Closed Charged Starts Expirations FailedStarts AttemptedPages FailedPages RPCs RequestBytes ReplyBytes DeadlineUnixNanos")
        certs = [columns_codec.certificate(raw, v) for v in r["Initial"]]
        final_certs = [columns_codec.certificate(raw, v) for v in r["Final"]]
        require(certs == first and final_certs == finals[stage] and r["DeadlineUnixNanos"] == end["DeadlineUnixNanos"], "same full original cuts and fixed whole deadline")
        axis = binding(r["Binding"], end, first, g)
        for name in ("Charged",): fields(r[name], " ".join(WORK))
        if action == "open":
            require(owner is None and r["Sequence"] == 0 and not r["EOF"] and not r["Closed"] and r["Frames"] == [], "genuine source opening")
            owner, previous, sequence = r, None, 0
            positions = [v["sourceIndex"] for v in first]
            root_image = first[g]["image"]
        else:
            require(owner is not None and r["Cookie"] == owner["Cookie"] and r["Binding"] == owner["Binding"], "private actual source cookie/configuration substituted")
        starts, failed, expirations = owner_limits(end)
        require(1 <= integer(r["Starts"]) <= starts and 0 <= integer(r["Expirations"]) <= expirations
                and 0 <= integer(r["FailedStarts"]) <= 4 and 0 <= integer(r["FailedPages"]) <= failed
                and r["Expirations"]+r["FailedStarts"] <= r["Starts"], "whole-query cumulative capture/failed-page bounds")
        if previous is not None:
            monotone(previous["Charged"], r["Charged"], r["Binding"]["Policy"])
            for name in ("Starts", "Expirations", "FailedStarts", "AttemptedPages", "FailedPages", "RPCs", "RequestBytes", "ReplyBytes"):
                require(integer(r[name]) >= previous[name], "actual source attempted/counter refund")
        require(r["RPCs"] <= r["Charged"]["RPCs"] and r["RequestBytes"]+r["ReplyBytes"] <= r["Charged"]["WireBytes"], "actual transport above prepaid source grant")
        if action == "next":
            sequence += 1
            require(r["Sequence"] == sequence and not r["Closed"] and type(r["Frames"]) is list and len(r["Frames"]) <= 16,
                    "genuine monotone private whole-page source cursor")
            for f in r["Frames"]:
                require(not previous["EOF"] and f["Partition"] == part and f["Index"] == positions[g]+1, "complete source prefix omitted/duplicated entry")
                positions[g] += 1
                key, hashed = u64(part)+u64(f["Index"]), frame_digest(f)
                prior = spool.find(b"frame", key)
                fresh = prior is None
                require(fresh or prior == hashed, "actual prefix/typed originals changed on voter/reopen")
                status, m = authenticate_frame(f, root_image, spool, fresh)
                if fresh:
                    require(member in (1, 4), "later voter cannot supply missing first source")
                    spool.insert(b"frame", key, hashed)
                    spool.insert(b"original-bytes", key, digest(optional_bytes(f["Outcome"]))+digest(optional_bytes(f["CDC"])))
                    if status is not None and status["index"] == f["Index"] and status["phase"] == 4 and status["commit"]:
                        terminal = decoded(spool.lookup(b"terminal", bytes.fromhex(status["tx"])))
                        require(terminal["Class"] == "commit" and terminal["Manifest"] == f["Registration"], "genuine Resolve joins independent actual caller original")
                        fold_rows(f, terminal, axis, s, f["Index"] <= old[g]["sourceIndex"], spool)
                        business_rows += len(f["Rows"] or [])
                    else:
                        require(not f["Rows"], "unjoined original business output")
                        controls += 1
                root_image = array(f["Root"], 172)
                frames += 1
            require(r["Positions"] == positions and (not r["EOF"] or not r["Frames"] and positions[g] == finals[stage][g]["sourceIndex"] and root_image == finals[stage][g]["image"]), "actual full source EOF/root equality")
        elif action == "close":
            require(previous["EOF"] and r["Closed"] and not r["EOF"] and r["Frames"] == [] and r["Positions"] == positions,
                    "source Close cannot manufacture completion")
            spool.insert(b"source-eof", stage.encode()+bytes([member]), encoded({"Incarnation": trace["Incarnation"], "Binding": r["Binding"], "Final": r["Final"]}))
            owner = None; step += 1
        else:
            require(action == "open", "unknown genuine source action")
        previous = r
    require(owner is None and step == len(order) and business_rows == 3+3*s["facts"], "complete source inventory/actual business change census")
    # Every actual R/D/Prepare/Resolve must join the same caller original and
    # exact first immutable decision; both commit effects and ABORT non-effects.
    original_count = 0
    for tx, terminal_body in spool.walk(b"terminal"):
        terminal = decoded(terminal_body)
        m = manifest(blob(terminal["Manifest"]))
        decision = cross_status(blob(terminal["Decision"]))
        require(spool.lookup(b"manifest", tx) == blob(terminal["Manifest"])
                and spool.lookup(b"phase", u64(m["coordinator"])+tx+b"\3") == blob(terminal["Decision"]), "actual immutable first-D source join")
        for part, floor, _ in m["parts"]:
            prepared_wire = spool.find(b"phase", u64(part)+tx+b"\2")
            resolved_wire = spool.find(b"phase", u64(part)+tx+b"\4")
            prepared = None if prepared_wire is None else cross_status(prepared_wire)
            resolved = None if resolved_wire is None else cross_status(resolved_wire)
            # An authentic first NO stops later Prepare. Native ABORT only
            # resolves participants actually registered/prepared; absent later
            # partitions remain absent instead of synthetic FDB-style ABORTs.
            if decision["commit"]:
                require(prepared is not None and prepared["yes"] and resolved is not None, "COMMIT requires every genuine participant YES/effects")
            else:
                require(decision["round"] == 0 and (resolved is not None) == (prepared is not None or part == m["coordinator"]), "actual ABORT participant presence/non-effects")
            if prepared is not None:
                require(prepared["tx"] == tx.hex() and prepared["digest"] == m["digest"] and prepared["floor"] >= floor, "same actual Prepare authority")
            if resolved is not None:
                require(resolved["tx"] == tx.hex() and resolved["digest"] == m["digest"] and resolved["commit"] == decision["commit"]
                        and resolved["round"] == decision["round"], "actual Resolve/first-decision lineage")
        cross_status(spool.lookup(b"phase", u64(m["coordinator"])+tx+b"\1"))
        original_count += 1
    require(original_count == end["Metrics"]["Commits"]+end["Metrics"]["NativeAborts"], "unique genuine registered originals, not reply cardinality")
    for key, _ in spool.walk(b"manifest"):
        spool.lookup(b"terminal", key)
    return {"completeVoterSources": step, "framesAcrossVotersAndCohorts": frames, "businessChanges": business_rows, "controls": controls, "uniqueOriginals": original_count}


def expected_value(s, fact):
    candidate = next(i for i in range(10) if cross(s, i) == cross(s, fact))
    if candidate != fact: return 5
    count = s["facts"]*s["crossPercent"]//100
    return 5+(count if cross(s, fact) else s["facts"]-count)


def points(directory, end, spool):
    s, count = end["spec"], 0
    order = (("old-after-mutation", "Old", False), ("final", "Final", True), ("reopened-old", "Old", False), ("reopened-final", "Final", True))
    for row in lines(directory / "points.jsonl", 4*(s["facts"]+1)):
        fields(row, "Stage At Fact Entity")
        stage, name, updated = order[count//(s["facts"]+1)]
        fact = count % (s["facts"]+1)
        require(row["Stage"] == stage and row["At"] == end[name] and row["Fact"] == fact, "complete ordered retained/reopened point inventory")
        entity, present = row["Entity"], fact < s["facts"] and (updated or fact < s["facts"]//2)
        owner = 7+2*fact if fact < s["facts"] else 5
        require(entity["id"] == owner and entity["found"] == entity["effective"] == present, "old/final exact entity absence/presence")
        if present:
            require(entity["kind"] == 2 and entity["mode"] == 1 and entity["type"] == "edge" and entity["source"] == 1
                    and entity["target"] == (5*s["facts"]+65 if cross(s, fact) else 3) and entity["effectiveLife"] == owner+1,
                    "actual effective endpoint-life point binding")
            namespace = b"state" if updated else b"old-state"
            for component, life, name_ in ((1, 0, "presence"), (3, owner+1, "property")):
                key = u64(owner)+u64(life)+u64(component)+(b"p" if component == 3 else b"")
                actual = decoded(spool.lookup(namespace, key))["After"]
                expected = {"present": actual["Present"], "null": actual["Null"], "handle": actual["Handle"], "revision": actual["Revision"],
                            "provenance": actual["Provenance"], "kind": "i64" if component == 3 else "life", "i64": actual["Value"]}
                require(entity[name_] == expected, "point equals complete raw native serial predecessor fold")
            require(entity["property"]["i64"] == (expected_value(s, fact) if updated else 5), "all N useful hot increments")
        else:
            require(not entity["presence"]["present"] and not entity["property"]["present"], "phantom/current state leak")
        count += 1
    require(count == 4*(s["facts"]+1), "missing exact old/final/reopened point EOF")
    return count


def generation_manifest(m, b, initial, final, g):
    fields(m, "Generation Chunks Frames Rows Bytes Positions Chain")
    generation = m["Generation"]
    fields(generation, "ID Coverage Binding Sources Pin")
    require(any(array(generation["ID"], 16)) and generation["Coverage"] == 1 << g
            and array(generation["Binding"], 32) == digest(columns_codec.sealed_json(raw, b))
            and any(array(generation["Pin"], 32)), "actual immutable durable native generation")
    require(len(generation["Sources"]) == 2, "full two-source generation binding")
    for group, boundary in enumerate(generation["Sources"]):
        fields(boundary, "Initial Final Generation")
        require(columns_codec.certificate(raw, boundary["Initial"]) == initial[group]
                and columns_codec.certificate(raw, boundary["Final"]) == final[group]
                and integer(boundary["Generation"], 1) > 0, "exact original retained cut/bank generation")
    positions = [v["sourceIndex"] for v in initial]
    positions[g] = final[g]["sourceIndex"]
    require(m["Positions"] == positions and m["Frames"] == final[g]["sourceIndex"]-initial[g]["sourceIndex"]
            and 0 < integer(m["Chunks"]) <= min(m["Frames"], b["Policy"]["MaxChunks"])
            and m["Frames"] <= m["Chunks"]*16 and integer(m["Rows"]) <= 16*m["Chunks"]
            and 0 < integer(m["Bytes"]) <= b["Policy"]["MaxRetainedBytes"] and any(array(m["Chain"], 32)), "complete immutable disjoint source EOF census")


def retention_receipt(r, m, spool, terminal=False):
    fields(r, "Version State Attempts Generation Pin Original Token")
    require(r["Version"] == 1 and r["Generation"] == m["Generation"]["ID"] and r["Pin"] == m["Generation"]["Pin"]
            and integer(r["Attempts"], 0, 4) <= 4 and r["State"] == (5 if terminal else 3)
            and digest(blob(r["Original"])) == array(r["Pin"], 32), "same genuine durable original/pin/lifetime")
    wire = blob(r["Original"])
    original = cursor(checked(wire, b"GPM\1", 16384))
    scope, issuer, request = original.take(120), original.take(16), original.take(16)
    require(any(issuer) and any(request), "native pin issuer/request")
    pin = digest(b"rho-tkg:retained-graph-pin:v1\0"+scope+issuer+request)
    epoch, expires, parts = original.number(), original.number(), original.number(4)
    require(epoch < expires == 20 and parts == 2, "explicit unchanged native replicated-epoch policy")
    for g in range(2):
        cert = original.field(512)
        feed, lineage = original.number(), original.take(32)
        lower = columns_codec.certificate(raw, m["Generation"]["Sources"][g]["Initial"])
        require(cert == blob(m["Generation"]["Sources"][g]["Initial"]) and 0 < feed <= lower["sourceIndex"]+1
                and lineage == digest(b"rho-tkg:retained-pin-source-lineage:v1\0"+cert+u64(feed)), "actual pin covers exact initial lower bound and earlier-original dependency minimum")
        if terminal: spool.lookup(b"pin-terminal", u64(lower["partition"])+pin+digest(wire))
    original.done()
    require(terminal or optional_bytes(r["Token"]), "live generation lacks actual issued native token")
    if optional_bytes(r["Token"]):
        token = cursor(checked(blob(r["Token"]), b"GPT\1", 17408))
        require(token.field(16384) == wire and token.number(4) == 2, "same genuine native pin token/original")
        for g in range(2):
            part = 3 if g == 0 else 8
            publication = token.number()
            result = decoded(spool.lookup(b"pin-result", u64(part)+u64(publication)))
            require(result["phase"] == result["status"] == 1 and result["publication"] == publication and result["pin"] == pin.hex()
                    and result["digest"] == digest(wire).hex() and result["epoch"] == epoch, "actual both-participant native publication originals, not journal authority")
        token.done()


def column_artifacts(directory, end, spool):
    s, initial = end["spec"], cut(end["Initial"])
    path = directory / "columns.jsonl"
    if s["layout"] == "kv":
        require(not path.exists() and "Columns" not in end and "ColumnBindings" not in end, "ordinary fresh KV has no fabricated column receipt")
        return {"walks": 0}
    require(len(end["Columns"]) == len(end["ColumnBindings"]) == 2, "two actual immutable generations")
    for generation in range(2):
        final = cut(end["Old" if generation == 0 else "Final"])
        for g in range(2):
            binding(end["ColumnBindings"][generation][g], end, initial, g)
            generation_manifest(end["Columns"][generation][g], end["ColumnBindings"][generation][g], initial, final, g)
        require(end["Columns"][generation][0]["Generation"]["Sources"] == end["Columns"][generation][1]["Generation"]["Sources"]
                and end["ColumnBindings"][generation][0] | {"Coverage": 3} == end["ColumnBindings"][generation][1] | {"Coverage": 3}, "both genuine sealed EOF/full configuration/policy/source-generation join")
        require(sum(m["Rows"] for m in end["Columns"][generation]) == (3+s["facts"] if generation == 0 else 3+3*s["facts"]), "full old/final native column source census")
    expected = {"old-seal/native", "old-seal/cache-fill", "old-seal/cached", "final-seal/native", "final-seal/cache-fill", "final-seal/cached", "old-after-mutation",
                "reopened-0-cached-false", "reopened-0-cached-true", "reopened-1-cached-false", "reopened-1-cached-true"}
    owners, completed, published, closed = [None, None], set(), set(), set()
    failures, expired = [0, 0], [0, 0]
    terminal_cache = [None, None]
    starts_limit, failed_limit, expiry_limit = owner_limits(end)
    for trace in lines(path, 20*(2*frame_limit(s)+failed_limit+starts_limit)+100):
        fields(trace, "Stage Member Input"+(" Receipt" if "Receipt" in trace else "")+(" Creation" if "Creation" in trace else "")+(" Error" if "Error" in trace else ""))
        stage, member, q = trace["Stage"], trace["Member"], trace["Input"]
        require(member in (1, 4), "actual partition-local publisher member")
        g, action = (member-1)//3, integer(q["Action"], 1, 6)
        generation = 1 if stage.startswith(("final-seal", "reopened-1", "retire-1")) else 0
        manifest_, binding_ = end["Columns"][generation][g], end["ColumnBindings"][generation][g]
        r = trace.get("Receipt")
        if r is None:
            require(trace.get("Error") and (owners[g] is None and action == 1 or owners[g] is not None and q["Cookie"] == owners[g]["receipt"]["Cookie"]), "failed request retains genuine query identity")
            failure = trace.get("Creation")
            if failure is not None and failure["FullTTL"] and failure["Expired"]:
                require(failure["DeadlineUnixNanos"]-failure["IssuedUnixNanos"] == 60_000_000_000, "actual complete owner TTL only")
                expired[g] += 1
            else: failures[g] += 1
            require(expired[g] <= expiry_limit and failures[g] <= failed_limit, "additive chosen-budget whole-query failed/expiry allowance")
            continue
        require(not trace.get("Error") and r["Binding"] == binding_ and r["DeadlineUnixNanos"] == end["DeadlineUnixNanos"]
                and 1 <= r["SourceStarts"] <= starts_limit and 0 <= r.get("SourceExpirations", 0) <= r["SourceStarts"], "actual fixed source/query binding/deadline")
        if action == 1:
            require(owners[g] is None and q["Initial"] == end["Initial"] and q["Final"] == end["Old" if generation == 0 else "Final"]
                    and q["RetainedExpires"] == 20 and q["Generation"] == manifest_["Generation"]["ID"] and r["Sequence"] == 0
                    and not r["EOF"] and not r["Closed"], "actual original bounded column owner")
            owners[g] = {"receipt": r, "sequence": 0, "generation": generation, "cursor": None, "replayed": False}
        else:
            o = owners[g]
            require(o is not None and o["generation"] == generation and q["Cookie"] == r["Cookie"] == o["receipt"]["Cookie"], "same private column query owner")
            monotone(o["receipt"]["Charged"], r["Charged"], binding_["Policy"])
            monotone(o["receipt"]["DurableWork"], r["DurableWork"], binding_["Policy"])
            if action not in (5, 6):
                require(q["Sequence"] == r["Sequence"] == o["sequence"]+1, "commit cursor/sequence only after complete actual page")
                o["sequence"] += 1
        o = owners[g]
        for name in ("RPCs", "RequestBytes", "ReplyBytes"): integer(r[name])
        require(r["RPCs"] <= r["Charged"]["RPCs"] and r["RequestBytes"]+r["ReplyBytes"] <= r["Charged"]["WireBytes"], "actual column transport grant")
        ledger = r["CacheLedger"]
        if ledger["Version"]:
            require(ledger["Version"] == 1 and 32768 <= ledger["Charged"] <= ledger["Maximum"] == binding_["Policy"]["MaxRetainedBytes"] <= 1 << 62, "actual finite source-policy-bound persisted cache ledger")
            terminal_cache[g] = ledger
        # The durable generation ledger must remain monotone even when a live
        # source expires or a new process reacquires the exact original cuts.
        work_key = bytes([g, generation])
        prior = spool.find(b"column-work", work_key)
        if prior is not None: monotone(decoded(prior), r["DurableWork"], binding_["Policy"])
        raw.put_raw(spool, b"column-work", work_key, encoded(r["DurableWork"]))
        if action == 2 and r["EOF"]:
            require(r["Manifest"] == manifest_, "genuine immutable publication EOF")
            retention_receipt(r["Retention"], manifest_, spool)
            published.add((generation, g))
        elif action == 3:
            require(stage in expected and (stage, g) not in completed and r["Manifest"] == manifest_ and not r["EOF"]
                    and q["At"] == end["Old" if generation == 0 else "Final"] and not optional_bytes(r["Body"]) and not r["Rows"], "actual column read begins at exact sealed cut")
            require(r["Cached"] == q["Cached"] and r["Fill"] == q["Fill"] and not (r["Cached"] and r["Fill"]), "actual declared cache/native read mode")
            o["cursor"] = {"stage": stage, "ordinal": 0, "chain": bytes(32), "position": initial[g]["sourceIndex"], "frames": 0, "rows": 0, "bytes": 0, "cached": q["Cached"], "fill": q["Fill"]}
            if q["Cached"]: columns_codec.cache_manifest(raw, Path(end["Directory"]), g, manifest_)
        elif action == 4:
            c = o["cursor"]
            require(c is not None and c["stage"] == stage and r["Ordinal"] == c["ordinal"] and r["Manifest"] == manifest_
                    and r["Cached"] == c["cached"] and r["Fill"] == c["fill"], "whole genuine private column chunk continuation")
            if r["EOF"]:
                require(not optional_bytes(r["Body"]) and not r["Rows"] and c["ordinal"] == manifest_["Chunks"] and c["chain"] == array(manifest_["Chain"], 32)
                        and c["position"] == manifest_["Positions"][g] and c["frames"] == manifest_["Frames"] and c["rows"] == manifest_["Rows"] and c["bytes"] == manifest_["Bytes"], "actual full column EOF, not Close/partial page")
                completed.add((stage, g)); o["cursor"] = None
            else:
                body = blob(r["Body"])
                frames = columns_codec.chunk(raw, body, manifest_["Generation"], c["ordinal"], c["chain"], binding_["Policy"])
                selected = []
                for f in frames:
                    f = {**f, "Root": list(f["Root"]), **{name: base64.b64encode(f[name]).decode() for name in ("Outcome", "CDC", "Registration", "OriginalOutcome", "OriginalCDC")}}
                    require(f["Partition"] == (3 if g == 0 else 8) and f["Index"] == c["position"]+1
                            and frame_digest(f) == spool.lookup(b"frame", u64(f["Partition"])+u64(f["Index"])), "actual binary columns equal authenticated full raw source frame")
                    c["position"] += 1; c["frames"] += 1; c["rows"] += len(f["Rows"])
                    selected.extend(f["Rows"])
                require(selected == (r["Rows"] or []), "actual selected typed row columns")
                columns_codec.cache_chunk(raw, Path(end["Directory"]), g, manifest_, c["ordinal"], body, spool)
                # Cache key is the actual generation ID rendered as raw hex.
                key = b"generation/"+array(manifest_["Generation"]["ID"], 16).hex().encode()+b"/chunk/"+f'{c["ordinal"]:016x}'.encode()
                c["bytes"] += len(key)+len(body)
                c["chain"] = digest(c["chain"]+digest(body)); c["ordinal"] += 1
        elif action in (5, 6):
            require(r["Closed"] and not r["EOF"] and o["cursor"] is None, "Close/retire cannot certify unfinished column read")
            if action == 6: retention_receipt(r["Retention"], manifest_, spool, True)
            closed.add((stage, g)); owners[g] = None
        else:
            require(action == 1 or action == 2 and not r["EOF"], "unknown actual column transition")
            if action == 2:
                require(not any(array(r["Manifest"]["Generation"]["ID"], 16)) and not r["Rows"] and not optional_bytes(r["Body"]), "partial publisher cannot supply a seal or data walk")
        if owners[g] is not None: owners[g]["receipt"] = r
    require(owners == [None, None] and published == {(generation, g) for generation in range(2) for g in range(2)}
            and completed == {(stage, g) for stage in expected for g in range(2)}
            and all((f"retire-{generation}", g) in closed for generation in range(2) for g in range(2)), "complete actual publication/cache/old/reopened/retirement inventory")
    for g in range(2): columns_codec.cache_terminal(raw, Path(end["Directory"]), g, terminal_cache[g], spool)
    return {"walks": len(completed), "genuineDisjointSeals": len(published)}


def coordinator_result(value):
    c = cursor(blob(value))
    require(c.take(4) == b"GCR1", "actual native original recovery envelope")
    scope = checked(c.field(256), b"APS\1", 156)
    request, tx, business, pending = c.take(16), c.take(16), c.take(32), c.number(1)
    decision = c.field(256); c.done()
    require(any(request) and any(tx) and any(business) and pending == 0 and decision, "genuine completed original, not timeout authority")
    status = cross_status(decision)
    require(status["tx"] == tx.hex() and status["part"] == int.from_bytes(scope[16:24], "big"), "same original coordinator namespace")
    return scope, request, tx, business, decision


def process_state(value):
    c = cursor(blob(value))
    incarnation = c.take(16)
    stats = [c.number() for _ in range(28)]
    c.take(64); c.number()
    count = c.number(4)
    require(count <= 2, "bounded actual native state handles")
    for _ in range(count): c.number()
    c.field(1 << 20)
    ready, sequence, applied, code, cause = c.number(1), c.number(), c.number(), c.number(1), c.number(2)
    detail = c.field(4096)
    require(c.number(4) == 0 and c.number(4) == 0, "drained actual native pre-stop/reopen state")
    image, outcome, changes = c.field(65536), c.field(65536), c.field(1 << 20)
    flags, life, labels = c.number(1), c.number(), c.number(4)
    require(labels <= 16, "bounded state fact labels")
    for _ in range(labels): c.field(256)
    c.field(256); c.done()
    require(any(incarnation) and sequence > 0 and applied > 0 and code == cause == 0 and not detail
            and ready in (0, 1) and stats[7] == stats[22] == 0, "actual native source-free restore state")
    return {"incarnation": incarnation, "stats": stats, "applied": applied, "image": image}


def processes(directory, end, spool):
    previous, restored = [None]*6, [None]*6
    count = 0
    for row in lines(directory / "restore.jsonl", 12):
        fields(row, "Stage Member Raw")
        member = count % 6+1
        stage = "before-stop" if count < 6 else "restored-before-campaign"
        require(row["Member"] == member and row["Stage"] == stage, "all-six exact same-directory restoration inventory")
        actual = process_state(row["Raw"])
        columns_codec.root(raw, actual["image"], 3 if member <= 3 else 8)
        if count < 6:
            previous[member-1] = actual
        else:
            prior = previous[member-1]
            require(actual["incarnation"] != prior["incarnation"] and actual["applied"] == prior["applied"]
                    and actual["image"] == prior["image"] and actual["stats"][0] == prior["stats"][0]
                    and actual["stats"][4] == prior["stats"][4], "actual pre-election physical restore changed acknowledged authority")
            restored[member-1] = actual
        count += 1
    require(count == 12, "missing actual pre-election restore proof")
    identities, count = set(), 0  # exactly12 process incarnations, not a dataset map
    for row in lines(directory / "processes.jsonl", 12):
        fields(row, "Cohort Member PID Incarnation Volume WaitStatus ExitCode Reaped")
        cohort, member = count//6+1, count % 6+1
        require(row["Cohort"] == cohort and row["Member"] == member and row["Reaped"] is True
                and row["WaitStatus"] == row["ExitCode"] == 0 and integer(row["PID"], 1) > 0
                and Path(row["Volume"]) == Path(end["Directory"])/str(member), "actual same-volume native OS-process wait/cleanup proof")
        expected = (previous if cohort == 1 else restored)[member-1]["incarnation"]
        require(array(row["Incarnation"], 16) == expected and expected not in identities, "actual stopped process incarnation differs from restored/source owner")
        for stage in ("before-stop" if cohort == 1 else "reopened-replayed",):
            source = decoded(spool.lookup(b"source-eof", stage.encode()+bytes([member])))
            require(array(source["Incarnation"], 16) == expected, "genuine source EOF came from another process")
        identities.add(expected); count += 1
    require(count == end["ProcessInstances"] == 12 and end["Reaped"] == 6, "all actual OS children reaped")
    return count


def transport(receipts):
    require(type(receipts) is list and len(receipts) == 2, "both actual transport cohorts")
    selected = None
    for r in receipts:
        fields(r, "Version Spec Raft SourceRequest SourceReply MaximumHeldFrames MaximumHeldBytes MetadataBytes PipeWrites PipeReads Scope")
        s = r["Spec"]
        fields(s, "kind targetRTTMillis jitterMillis seed")
        require(r["Version"] == 1 and integer(s["seed"], 1) > 0 and s["jitterMillis"] == 0
                and (s["kind"] == "local" and s["targetRTTMillis"] == 0 or s["kind"] == "injected-rtt" and s["targetRTTMillis"] in (1, 20, 100)), "actual owned directed native transport configuration")
        require(selected is None or selected == s, "restart changed configured transport")
        selected = s
        require(r["MetadataBytes"] == 32768 and 0 <= r["MaximumHeldFrames"] <= 128 and 0 <= r["MaximumHeldBytes"] <= 8 << 20
                and "not native delivery/ACK" in r["Scope"] and "no physical network claim" in r["Scope"], "finite retained queue and honest native delay scope")
        for name in ("Raft", "SourceRequest", "SourceReply"):
            l = r[name]
            fields(l, "Admitted Completed Canceled Bytes ConfiguredNanos ObservedNanos DeviationNanos MinimumObservedNanos MaximumObservedNanos")
            for value in l.values(): integer(value)
            require(l["Completed"]+l["Canceled"] == l["Admitted"] and l["ConfiguredNanos"] == l["Admitted"]*s["targetRTTMillis"]*500000
                    and l["MinimumObservedNanos"] <= l["MaximumObservedNanos"] <= l["ObservedNanos"], "actual settled half-RTT counts; no accidental doubled RTT")
        for name in ("PipeWrites", "PipeReads"):
            l = r[name]
            fields(l, "AttemptedFrames CompleteFrames FailedFrames PrepaidBytes ActualBytes")
            for value in l.values(): integer(value)
            require(l["CompleteFrames"]+l["FailedFrames"] == l["AttemptedFrames"] and 0 < l["ActualBytes"] <= l["PrepaidBytes"], "actual full framed IO including partial/error/cleanup reads")
    return selected


def disk_accounting(end):
    root = Path(end["Directory"])
    total, allocated, work = 0, 0, 0
    def walk(path, depth):
        nonlocal total, allocated, work
        work += 1
        require(depth <= 64 and work <= 1 << 40, "finite streaming actual filesystem accounting")
        info = path.lstat()
        if stat.S_ISDIR(info.st_mode):
            with os.scandir(path) as entries:
                for entry in entries: walk(Path(entry.path), depth+1)
        else:
            total += integer(info.st_size)
            allocated += integer(info.st_blocks)*512
            integer(total); integer(allocated)
    for member in range(1, 7):
        child = root/str(member)
        require(child.is_dir() and not child.is_symlink(), "actual owned voter directory missing/foreign")
        walk(child, 0)
    require(total == end["PersistedFileBytes"] and allocated == end["AllocatedFileBytes"], "all-six actual voter/derived/cache disk costs, symlink metadata only")
    return {"fileBytes": total, "allocatedBytes": allocated, "entriesVisited": work}


def answer_digest(s, spool):
    answer = hashlib.sha256()
    for namespace in (b"old-state", b"state"):
        for fact in range(s["facts"]):
            owner = 7+2*fact
            value = spool.find(namespace, u64(owner)+u64(owner+1)+u64(3)+b"p")
            cell = None if value is None else decoded(value)["After"]
            answer.update(u64(fact)+bytes([cross(s, fact), cell is not None]))
            if cell is not None: answer.update(struct.pack(">q", cell["Value"]))
    return answer.hexdigest()


def verify(directory, spool_directory, policy, *, binary_sha=None, expected_spec=None, expected_run=None, expected_deadline=None, expected_seconds=None, shared_spool=None):
    directory = Path(directory).resolve(strict=True)
    with (directory / "result.json").open("rb") as source: body = source.read(LINE+1)
    require(len(body) <= LINE and body.endswith(b"\n"), "bounded complete fresh native result")
    end = decoded(body[:-1])
    require(end["version"] == 1 and type(end["version"]) is int and end["format"] == FORMAT and end["engine"] == "native-raft", "explicit fresh native family artifact grammar")
    s = shape(end["spec"])
    run = array(end["run"], 16)
    require(any(run) and (expected_spec is None or raw.same(s, expected_spec)) and (expected_run is None or run.hex() == expected_run)
            and (binary_sha is None or end["binarySHA256"] == binary_sha) and (expected_deadline is None or end["DeadlineUnixNanos"] == expected_deadline)
            and (expected_seconds is None or end["BudgetSeconds"] == expected_seconds), "actual selected binary/spec/fresh run/chosen budget/deadline binding")
    require(Path(end["ArtifactDirectory"]).resolve(strict=True) == directory and not Path(end["Directory"]).is_symlink()
            and Path(end["Directory"]).resolve(strict=True).parent == directory, "owned genuine durable/artifact roots")
    cuts = {name: cut(end[name]) for name in ("Initial", "Old", "Final", "BeforeStop", "Replayed")}
    for g in range(2):
        require(all(cuts[name][g]["scope"] == cuts["Initial"][g]["scope"] and cuts[name][g]["configuration"] == cuts["Initial"][g]["configuration"] for name in cuts)
                and [cuts[name][g]["sourceIndex"] for name in cuts] == sorted(cuts[name][g]["sourceIndex"] for name in cuts)
                and cuts["Replayed"][g]["sourceIndex"] <= frame_limit(s), "full original native cuts/finite retain-all frame contract")
    selected = transport(end["Transport"])
    admission_digest(end)
    require(policy is not None, "explicit unchanged shared finite comparison spool policy")
    policy.validate()
    with (Spool(spool_directory, policy) if shared_spool is None else nullcontext(shared_spool)) as spool:
        actual_grants = grants(directory, spool)
        require(grant_replays(directory, end, spool) == actual_grants, "every actual grant original replayed exactly once")
        attempts = operations(directory, end, spool)
        original = end["OriginalReplay"]
        recovered = end["ReopenedOriginal"]
        require(original["Class"] == "commit" and original["Proof"] is not None and recovered["Error"] == ""
                and not any(recovered[k] for k in ("Conflict", "Closed", "Unavailable", "Expired", "Invalid"))
                and coordinator_result(original["Result"]) == coordinator_result(recovered["Result"]), "genuine SAME immutable original after OS replacement")
        census = sources(directory, end, spool)
        retained_points = points(directory, end, spool)
        columns = column_artifacts(directory, end, spool)
        children = processes(directory, end, spool)
        disk = disk_accounting(end)
        answers = answer_digest(s, spool)
        report = spool.report()
    return {"format": FORMAT, "family": FAMILY, "spec": s, "run": run.hex(), "transport": selected,
            "BudgetSeconds": end["BudgetSeconds"], "CausalMinimumMillis": end["CausalMinimumMillis"], "DeadlineUnixNanos": end["DeadlineUnixNanos"],
            "actualOriginalClasses": attempts, "source": census, "retainedPoints": retained_points,
            "columns": columns, "processInstances": children, "disk": disk, "spool": report,
            "logicalAnswersSHA256": answers,
            "scope": "one genuine native family cell; Go exporter raw-codec hydration remains a runtime qualification boundary; no portable proof, FDB RTT, whole matrix, capacity or seeded-D acceptance"}


if __name__ == "__main__":
    try:
        require(len(sys.argv) == 5, "usage: native_contention_check.py ARTIFACTS OWNED_SPOOL_DIR DISK_BYTES CACHE_BYTES")
        print(json.dumps(verify(sys.argv[1], sys.argv[2], Policy(int(sys.argv[3]), int(sys.argv[4]), 4096, LINE)), indent=2))
    except (ValueError, KeyError, TypeError, OSError) as error:
        print("native workload evidence refused: "+str(error), file=sys.stderr)
        sys.exit(1)
