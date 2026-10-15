"""Fixed V2 native binary-column receipts, sharing the caller's finite spool.

This checks actual source artifacts, not portable source authority. Only fixed
metadata and one admitted chunk are live; no graph/history reconstruction map.
The native adapter remains responsible for its genuine Host/retained cut checks.
"""
import base64
import hashlib
import json
import os
from pathlib import Path
import struct

LAYOUT = "native-component-i64-change-columns-v1"
RUNNER = "independent-clients-request-bound-ack-columns-v1"
AGREEMENT = hashlib.sha256(b"rho-tkg:V2:partition-local-native-change-columns:v1\0authenticated-full-cuts;explicit-local-prefix-coverage;complete-native-originals;typed-i64;exact-native-scope;local-quiescence;retain-all;no-authority\0").digest()
WORK = "Records Bytes DecodedRows OutputBytes RPCs WireBytes".split()
ROW = "Owner Life Source Target SourceLife TargetLife Component Kind Mode Interpretation Role Name Type Axis Scope Before After".split()
CELL = "Present Null Handle Value Revision Provenance".split()
RECEIPT = "Cookie Sequence Binding Manifest Ordinal Body Rows EOF Closed Cached Fill Charged DurableWork Ledger CacheLedger RPCs RequestBytes ReplyBytes DeadlineUnixNanos ElapsedNanos Accounting".split()
POLICY = "ChunkRecords ChunkRows ChunkBytes FrameBytes MaxGenerations MaxChunks MaxRetainedBytes MaxWorkRecords MaxWorkBytes MaxDecodedRows MaxOutputBytes MaxRPCs MaxWireBytes CacheBytes MemTableBytes Lifetime".split()


def sealed_json(api, value):
    body = api.go_json(value)
    api.require(len(body) + 32 <= 16384, "native column metadata byte bound")
    return body + hashlib.sha256(body).digest()


def array(api, value, length):
    api.require(type(value) is list and len(value) == length, "native column fixed array")
    return bytes(api.integer(v, 0, 255) for v in value)


class Cursor:
    def __init__(self, api, body):
        self.api, self.body, self.offset = api, body, 0

    def take(self, count):
        self.api.require(type(count) is int and 0 <= count <= len(self.body) - self.offset,
                         "native column truncated span")
        value = self.body[self.offset:self.offset + count]
        self.offset += count
        return value

    def number(self, size=8):
        return int.from_bytes(self.take(size), "big")

    def field(self, maximum):
        size = self.number(4)
        self.api.require(size <= maximum, "native column arena field admission")
        return self.take(size)

    def done(self):
        self.api.require(self.offset == len(self.body), "native column trailing bytes")


def checked_body(api, body, magic, maximum):
    api.require(len(magic) + 32 <= len(body) <= maximum and body[:len(magic)] == magic
                and hashlib.sha256(body[:-32]).digest() == body[-32:], "native column native codec/seal")
    return body[len(magic):-32]


def certificate(api, encoded):
    body = api.fixed_body(api.raw_bytes(encoded), b"GSC\1", 392)
    scope, configuration = body[:120], body[120:152]
    round_, index, source_round, source_index = struct.unpack_from(">QQQQ", body, 152)
    partition = struct.unpack_from(">Q", scope, 16)[0]
    api.require(scope[:16].hex() == api.GRAPH and partition in api.PARTITIONS and any(configuration)
                and 0 < source_index < index and 0 < round_ and source_round <= round_, "native column actual certificate union")
    root(api, body[184:], partition)
    return {"scope": scope, "configuration": configuration, "round": round_, "index": index,
            "sourceRound": source_round, "sourceIndex": source_index, "image": body[184:], "partition": partition}


def root(api, image, partition):
    api.require(len(image) == 172 and image[:3] == b"GR\3" and image[3] in (1, 2, 3)
                and image[4:20].hex() == api.GRAPH and image[20:28] == struct.pack(">Q", partition)
                and hashlib.sha256(image[:-32]).digest() == image[-32:], "native column actual native root")


def policy(api, p):
    api.fields(p, " ".join(POLICY))
    for name, value in p.items():
        api.integer(value, 1)
    api.require(p["ChunkRecords"] == 16 and p["ChunkRows"] == 2 and p["ChunkBytes"] == 65536
                and p["FrameBytes"] == 32768 and p["MaxGenerations"] == 2 and p["MaxChunks"] <= 1 << 40
                and 65536 <= p["MaxRetainedBytes"] <= 1 << 62 and p["CacheBytes"] == 2 << 20
                and p["MemTableBytes"] == 1 << 20 and p["Lifetime"] == 60_000_000_000,
                "native column fixed comparison policy, not capacity evidence")


def binding(api, b, partition):
    api.fields(b, "Version Layout Agreement Coverage Peers Configurations Policy")
    policy(api, b["Policy"])
    api.require(type(b["Version"]) is int and b["Version"] == 1 and b["Layout"] == LAYOUT
                and array(api, b["Agreement"], 32) == AGREEMENT
                and type(b["Coverage"]) is int and b["Coverage"] == 1 << api.PARTITIONS[partition]
                and type(b["Peers"]) is list and type(b["Configurations"]) is list
                and len(b["Peers"]) == len(b["Configurations"]) == 2,
                "native column actual source binding/disjoint coverage")
    configurations, scopes = [], []
    for g, part in enumerate(api.PARTITIONS):
        peer = api.raw_bytes(b["Peers"][g])
        c = Cursor(api, checked_body(api, peer, b"NCP1", 256))
        scope = checked_body(api, c.field(256), b"APS\1", 156)
        voter = c.number(); c.done()
        api.require(len(scope) == 120 and scope[:16].hex() == api.GRAPH and scope[16:24] == struct.pack(">Q", part)
                    and voter == (1 if part == 3 else 4), "native column configured peer/scope association")
        cfg = api.raw_bytes(b["Configurations"][g])
        body = checked_body(api, cfg, b"GAC\3", 4096)
        api.require(len(body) == 214 and body[:16].hex() == api.GRAPH and scope[48:56] == body[16:24]
                    and scope[56:88] == body[24:56], "native column complete native genesis/declaration")
        axis = body[104:182]
        c = Cursor(api, axis)
        axis_id, profile, version = c.take(16), c.number(1), c.number(2)
        reference, unit = c.field(256), c.field(256); c.done()
        api.require(any(axis_id) and profile == 2 and version == 1
                    and reference == api.SCOPE["reference"].encode() and unit == api.SCOPE["canonicalUnit"].encode(),
                    "native column exact rational/native axis descriptor")
        configurations.append((cfg, hashlib.sha256(b"rho-tkg:partition-genesis:v1\0" + cfg[:-32]).digest(), axis))
        scopes.append(scope)
    api.require(configurations[0] == configurations[1], "native column full configuration agreement")
    return configurations, scopes


def capture(api, row, spool):
    """Called only AFTER authentic native original/prefix validation."""
    key = struct.pack(">QQ", row["Partition"], row["Index"])
    spool.insert(b"native-column-source", key, api.go_json(row))
    for entity in row["Entities"] or []:
        spool.insert(b"native-column-entity", struct.pack(">Q", entity["id"]), api.go_json(entity))
    for life in row["Lives"] or []:
        spool.insert(b"native-column-life", struct.pack(">QQ", life["owner"], life["life"]), api.go_json(life))


def chunk(api, body, generation, ordinal, chain, p):
    """Decode genuine NCC1 arenas and contiguous numeric columns, one chunk."""
    c = Cursor(api, checked_body(api, body, b"NCC1", p["ChunkBytes"]))
    api.require(c.take(32) == hashlib.sha256(sealed_json(api, generation)).digest()
                and c.number() == ordinal and c.take(32) == chain, "native binary generation/ordinal/chain")
    count = c.number(4)
    api.require(1 <= count <= p["ChunkRecords"], "native binary original frame count")
    frames, rows = [], []
    for _ in range(count):
        part, index, image = c.number(), c.number(), c.take(172)
        originals = [c.field(p["FrameBytes"]) for _ in range(5)]
        n = c.number(4)
        api.require(n <= p["ChunkRows"] - len(rows), "native binary decoded row admission")
        frame_rows = [{} for _ in range(n)]
        rows.extend(frame_rows)
        frames.append({"Partition":part, "Index":index, "Root":image,
                       **dict(zip(("Outcome","CDC","Registration","OriginalOutcome","OriginalCDC"), originals)), "Rows":frame_rows})
    for field in ("Component","Kind","Mode","Interpretation","Role","BeforeFlags","AfterFlags"):
        for row in rows: row[field] = c.number(1)
    numbers = "Owner Life Source Target SourceLife TargetLife BeforeHandle BeforeValue BeforeRevision BeforeProvenance AfterHandle AfterValue AfterRevision AfterProvenance".split()
    for field in numbers:
        for row in rows:
            value = c.number()
            row[field] = value - (1 << 64) if field.endswith("Value") and value >= 1 << 63 else value
    encode = lambda raw: base64.b64encode(raw).decode()
    for row in rows:
        row["Name"], row["Type"] = c.field(256).decode("utf-8"), c.field(256).decode("utf-8")
        row["Axis"], row["Scope"] = encode(c.field(p["FrameBytes"])), encode(c.field(p["FrameBytes"]))
        for prefix in ("Before", "After"):
            flag = row.pop(prefix + "Flags")
            api.require(flag in (0, 1, 3), "native column typed cell flags")
            row[prefix] = {"Present":bool(flag & 1), "Null":bool(flag & 2),
                           **{name:row.pop(prefix + name) for name in ("Handle","Value","Revision","Provenance")}}
    c.done()
    return frames


def source_prefixes(api, directory, result, spool):
    """Original raw voter-prefix receipts remain separate from graph authority."""
    initial = [certificate(api,v) for v in result["columnInitialCut"]["Certificates"]]
    final = [certificate(api,v) for v in result["FinalCut"]["Certificates"]]
    # Current retain-all profile has Root+Outcome+CDC per absolute prefix index;
    # future sparse AS7/GC must not reuse this bound or this source grammar.
    facts = api.integer(result["spec"]["facts"],1,1_000_000)
    frames = (65536 + 512*facts)//3
    pages = (frames+15)//16 # Products/additions fit uint64 under the fixed required-spec bound.
    for g,part in enumerate(api.PARTITIONS):
        last, end, image = 0, None, None
        for page,_ in api.parallel_lines(directory / f"original-prefix-{g}.jsonl", pages):
            api.fields(page,"Partition End Next Image Records")
            api.require(page["Partition"] == part and type(page["Records"]) is list
                        and 1 <= len(page["Records"]) <= 16, "native column real raw prefix page")
            this_end = api.integer(page["End"],1,frames)
            this_image = api.raw_bytes(page["Image"])
            root(api,this_image,part)
            if end is None: end,image = this_end,this_image
            api.require(this_end == end and this_image == image and len(page["Records"]) == min(16,end-last), "native column fixed raw source endpoint")
            for row in page["Records"]:
                api.fields(row,"Partition Index Outcome CDC Changes Entities Lives Values Original CDCClass RawHashes Logical SourceBoundary")
                api.require(row["Partition"] == part and type(row["Index"]) is int and row["Index"] == last+1
                            and row["Outcome"] is not None and row["CDC"] is not None,
                            "native column complete present raw prefix")
                last += 1
                if initial[g]["sourceIndex"] < last <= final[g]["sourceIndex"]:
                    raw = {name:row[name] for name in ("Outcome","CDC")}
                    for value in raw.values(): api.raw_bytes(value)
                    spool.insert(b"native-column-prefix",struct.pack(">QQ",part,last),api.go_json(raw))
            api.require(type(page["Next"]) is int and page["Next"] == last, "native column raw prefix continuation")
        api.require(end is not None and last == end >= final[g]["index"]
                    and image == final[g]["image"], "native column raw original prefix genuine complete EOF/root")
    return initial,final


def typed_rows(api,frame,axis,spool,initial,baseline,generation):
    key = struct.pack(">QQ",frame["Partition"],frame["Index"])
    raw = json.loads(spool.lookup(b"native-column-prefix",key),object_pairs_hook=api.pairs)
    api.require(frame["Outcome"] == api.raw_bytes(raw["Outcome"]) and frame["CDC"] == api.raw_bytes(raw["CDC"]),
                "native column frame is not actual retained original bytes")
    source = spool.find(b"native-column-source",key)
    if source is None:
        # Only the genuine initial certificate's protocol tail precedes CDCFrom;
        # no omitted business row can be reclassified as empty or synthesized.
        api.require(initial["sourceIndex"] < frame["Index"] <= baseline and frame["Root"] == initial["image"]
                    and not frame["CDC"] and not frame["Rows"] and not frame["Registration"]
                    and not frame["OriginalOutcome"] and not frame["OriginalCDC"], "native column pre-baseline business omission")
        if frame["Outcome"]:
            body = api.fixed_body(frame["Outcome"],b"GSO\1",54)
            phase,status,round_,index = struct.unpack(">BBQQ",body)
            api.require(phase in (1,2) and status == 1 and round_ > 0 and index == frame["Index"], "native column genuine initial read-control tail")
        return []
    source = json.loads(source,object_pairs_hook=api.pairs)
    api.require(source["SourceBoundary"]["Generation"] == generation
                and hashlib.sha256(frame["Root"]).hexdigest() == source["SourceBoundary"]["ImageSHA256"],
                "native column substituted source root at actual entry")
    replay = source.get("Replay")
    api.require(frame["OriginalOutcome"] == (api.raw_bytes(replay["Outcome"]) if replay else b"")
                and frame["OriginalCDC"] == (api.raw_bytes(replay["CDC"]) if replay else b""),
                "native column original/replay sidecars differ from authenticated entry")
    original = source["Original"]
    if original["Kind"] == "cross":
        registration = frame["Registration"]
        api.require(hashlib.sha256(registration).hexdigest() == original["Digest"], "native column genuine immutable registration digest")
        c = Cursor(api,checked_body(api,registration,b"GCM\1",32768))
        transaction,coordinator,proposal = c.take(16),c.number(),c.field(32768)
        api.require(transaction.hex() == original["Transaction"] and coordinator == original["Coordinator"], "native column registration original request association")
        api.native_parallel_manifest(registration,bytes.fromhex(original["Request"]),transaction,bytes.fromhex(original["Digest"]),proposal)
        if replay: api.require(registration == api.raw_bytes(replay["Registration"]), "native column replay registration replaced")
    else:
        api.require(not frame["Registration"], "native column non-cross registration union")
    expected = []
    encoded_axis = base64.b64encode(axis).decode()
    all_scope = b"TS\1\2" + axis[:16] + hashlib.sha256(b"rho-tkg:axis:v1\0"+axis).digest() + b"\6"
    for change in source["Changes"] or []:
        entity = json.loads(spool.lookup(b"native-column-entity",struct.pack(">Q",change["Owner"])),object_pairs_hook=api.pairs)
        life = change["Life"] if change["Component"] != 1 else change["After"]["handle"] or change["Before"]["handle"]
        actual_life = json.loads(spool.lookup(b"native-column-life",struct.pack(">QQ",change["Owner"],life)),object_pairs_hook=api.pairs)
        api.require(api.same(change["AllScope"],api.SCOPE) and api.raw_bytes(change["Scope"]) == all_scope,
                    "native column exact native scope/rational axis")
        value = {"Owner":change["Owner"],"Life":change["Life"],"Source":entity["source"],"Target":entity["target"],
                 "SourceLife":actual_life["sourceLife"],"TargetLife":actual_life["targetLife"],
                 "Component":change["Component"],"Kind":entity["kind"],"Mode":entity["mode"],
                 "Interpretation":3 if entity["kind"] == 1 else 6,"Role":1 if entity["kind"] == 1 else 2,
                 "Name":change["Name"],"Type":entity["type"],"Axis":encoded_axis,"Scope":change["Scope"]}
        for name in ("Before","After"):
            cell = change[name]
            value[name] = {"Present":cell["present"],"Null":cell["null"],"Handle":cell["handle"],
                           "Value":cell["i64"],"Revision":cell["revision"],"Provenance":cell["provenance"]}
        expected.append(value)
    api.require(api.same(frame["Rows"],expected), "native numeric/descriptor/cell columns differ from actual raw source changes")
    return expected


def manifest(api,m,b,initial,final,partition):
    api.fields(m,"Generation Chunks Frames Rows Bytes Positions Chain")
    g = m["Generation"]
    api.fields(g,"ID Coverage Binding Sources")
    api.require(any(array(api,g["ID"],16)) and type(g["Coverage"]) is int and g["Coverage"] == b["Coverage"]
                and array(api,g["Binding"],32) == hashlib.sha256(sealed_json(api,b)).digest()
                and type(g["Sources"]) is list and len(g["Sources"]) == 2, "native column sealed generation/binding")
    cfg,scopes = binding(api,b,partition)
    for n,boundary in enumerate(g["Sources"]):
        api.fields(boundary,"Initial Final Generation")
        first,last = certificate(api,boundary["Initial"]),certificate(api,boundary["Final"])
        api.integer(boundary["Generation"],1)
        api.require(first == initial[n] and last == final[n] and first["scope"] == last["scope"] == scopes[n]
                    and first["configuration"] == last["configuration"] == cfg[n][1]
                    and first["round"] <= last["round"] and first["sourceIndex"] <= last["sourceIndex"],
                    "native column complete original cut/config/source-generation binding")
    for name in ("Chunks","Frames","Rows","Bytes"): api.integer(m[name])
    api.require(type(m["Positions"]) is list and len(m["Positions"]) == 2, "native column EOF source positions")
    covered = api.PARTITIONS[partition]
    positions = [v["sourceIndex"] for v in initial]
    positions[covered] = final[covered]["sourceIndex"]
    p = b["Policy"]
    api.require(api.same(m["Positions"],positions) and m["Frames"] == final[covered]["sourceIndex"]-initial[covered]["sourceIndex"]
                and m["Chunks"] <= min(p["MaxChunks"],m["Frames"]) and m["Frames"] <= m["Chunks"]*p["ChunkRecords"]
                and m["Rows"] <= m["Frames"]*p["ChunkRows"] and m["Bytes"] <= p["MaxRetainedBytes"], "native column complete covered-prefix census")
    chain = array(api,m["Chain"],32)
    api.require(bool(m["Frames"]) == bool(m["Chunks"]) == bool(any(chain)), "native column nonempty seal")
    return cfg[covered][2]


def work(api,r,previous,spool,partition,generation):
    api.require(previous is None or r["ElapsedNanos"] >= previous["ElapsedNanos"], "native column monotone fixed-deadline elapsed time")
    for name in ("Charged","DurableWork"):
        api.fields(r[name]," ".join(WORK))
        for key in WORK:
            v = api.integer(r[name][key])
            cap = r["Binding"]["Policy"][["MaxWorkRecords","MaxWorkBytes","MaxDecodedRows","MaxOutputBytes","MaxRPCs","MaxWireBytes"][WORK.index(key)]]
            api.require(v <= cap and (previous is None or v >= previous[name][key]), "native column cumulative accounting refund/exhaustion")
    if generation is not None:
        key = struct.pack(">Q",partition)+array(api,generation["ID"],16)
        prior = spool.find(b"native-column-work",key)
        if prior is not None:
            prior = json.loads(prior,object_pairs_hook=api.pairs)
            api.require(all(r["DurableWork"][k] >= prior[k] for k in WORK), "native column restart refunded durable generation work")
        api.put_raw(spool,b"native-column-work",key,api.go_json(r["DurableWork"]))
    for key in ("RPCs","RequestBytes","ReplyBytes"):
        api.integer(r[key])
        api.require(previous is None or r[key] >= previous[key], "native column actual dispatched/received counter refund")
    api.require(r["RPCs"] <= r["Charged"]["RPCs"] and r["RequestBytes"]+r["ReplyBytes"] <= r["Charged"]["WireBytes"], "native column peer dispatch beyond prepaid grant")
    api.fields(r["Ledger"],"Bytes Chunks Generations")
    for key in r["Ledger"]: api.integer(r["Ledger"][key])
    p = r["Binding"]["Policy"]
    api.require(r["Ledger"]["Bytes"] <= p["MaxRetainedBytes"] and r["Ledger"]["Chunks"] <= p["MaxChunks"] and r["Ledger"]["Generations"] <= 2,
                "native column actual derived ledger exceeded policy")
    api.fields(r["CacheLedger"],"Version Maximum Charged")
    cache = r["CacheLedger"]
    for value in cache.values(): api.integer(value)
    api.require(cache == {"Version":0,"Maximum":0,"Charged":0} and not r["Cached"] and not r["Fill"]
                or cache["Version"] == 1 and cache["Maximum"] == 1 << 40 and 32768 <= cache["Charged"] <= cache["Maximum"],
                "native column actual cache finite ledger")


def verify(api,path,result,spool,bounds):
    api.require(type(result.get("version")) is int and result["version"] == 2 and result.get("layout") == "columns"
                and result.get("runner") == RUNNER, "fresh native column artifact contract required")
    api.fields(result["columnInitialCut"],"Certificates")
    api.require(len(result["columnInitialCut"]["Certificates"]) == 2 and type(result["columnGenerations"]) is list
                and len(result["columnGenerations"]) == 2 and all(type(v) is list and len(v)==2 for v in result["columnGenerations"]),
                "native two-generation/two-partition seal inventory")
    for pair in result["columnGenerations"]:
        for m in pair:
            api.fields(m,"Generation Chunks Frames Rows Bytes Positions Chain")
            for name in ("Chunks","Frames","Rows","Bytes"): api.integer(m[name])
    facts = api.integer(result["spec"]["facts"],1,1_000_000)
    api.require(sum(m["Rows"] for m in result["columnGenerations"][0]) == 3+2*facts
                and sum(m["Rows"] for m in result["columnGenerations"][1]) == 3+3*facts,
                "native complete old/final business row census")
    path = Path(path)
    initial,final = source_prefixes(api,path.parent,result,spool)
    old = [certificate(api,v) for v in result["OldCut"]["Certificates"]]
    api.require(all(initial[g]["sourceIndex"] <= bounds["from"][g] < old[g]["sourceIndex"] <= final[g]["sourceIndex"] for g in range(2)),
                "native column exact initial/baseline/old/final prefix")
    owners = [None,None]
    bindings = [[None,None],[None,None]]
    published, completed, closed = set(),set(),set() # Fixed four/22/fourteen metadata entries only.
    expected_walks = {"old-seal/native","old-seal/cache-fill","old-seal/cached","final-seal/native","final-seal/cache-fill","final-seal/cached",
                      "old-after-mutation","reopen-0-cached-false","reopen-0-cached-true","reopen-1-cached-false","reopen-1-cached-true"}
    allowed_open = {"old-seal":0,"final-seal":1,"old-after-mutation":0,"reopen-0-cached-false":0,"reopen-0-cached-true":0,
                    "reopen-1-cached-false":1,"reopen-1-cached-true":1}
    total_rows,actual_control_requests,actual_control_replies = 0,0,0
    last_cache = [None,None]
    for trace,_ in api.parallel_lines(path, 64 + 32*sum(m["Chunks"] for pair in result["columnGenerations"] for m in pair)):
        api.fields(trace,"Stage Partition Action ControlRequestBytes ControlReplyBytes Receipt")
        part,stage,action = trace["Partition"],trace["Stage"],trace["Action"]
        api.require(type(part) is int and part in api.PARTITIONS and type(stage) is str and type(action) is int and 1 <= action <= 5,
                    "native column actual child action/partition")
        g = api.PARTITIONS[part]
        actual_control_requests += api.integer(trace["ControlRequestBytes"],1,16384+5)
        actual_control_replies += api.integer(trace["ControlReplyBytes"],1,1<<20)
        r = trace["Receipt"]; api.fields(r," ".join(RECEIPT))
        api.fields(r["Cookie"],"Owner Request")
        api.integer(r["Cookie"]["Owner"],1)
        api.require(any(array(api,r["Cookie"]["Request"],16)), "native column genuine owner cookie")
        for key in ("EOF","Closed","Cached","Fill"): api.require(type(r[key]) is bool,"native column boolean flags")
        api.require(not (r["Cached"] and r["Fill"]),"native column cache mode union")
        api.integer(r["DeadlineUnixNanos"],1,(1<<63)-1); api.integer(r["ElapsedNanos"],0,(1<<63)-1)
        api.integer(r["Sequence"]); api.integer(r["Ordinal"])
        api.require(type(r["Accounting"]) is str and "not actual bytes/RSS" in r["Accounting"], "native column accounting scope")
        binding(api,r["Binding"],part)
        if action == 1:
            api.require(stage in allowed_open and owners[g] is None and r["Sequence"] == 0 and not r["EOF"] and not r["Closed"], "native column fresh owner admission")
            name = allowed_open[stage]
            api.require((stage,g) not in closed,"native column owner reopening same stage")
            owner = {"stage":stage,"name":name,"sequence":0,"previous":None,"receipt":r,"cursor":None,"replayed":False,"lastAction":action,"lastStage":stage}
            owners[g] = owner
            if bindings[name][g] is not None: api.require(api.same(bindings[name][g],r["Binding"]),"native column restart changed full binding")
            bindings[name][g] = r["Binding"]
        else:
            owner = owners[g]
            api.require(owner is not None and api.same(r["Cookie"],owner["receipt"]["Cookie"])
                        and api.same(r["Binding"],owner["receipt"]["Binding"])
                        and r["DeadlineUnixNanos"] == owner["receipt"]["DeadlineUnixNanos"],"native column unowned/replaced source cookie/config/deadline")
            if action != 5 and r["Sequence"] == owner["sequence"]:
                api.require(not owner["replayed"] and owner["lastAction"] == action and owner["lastStage"] == stage
                            and api.same(r,owner["previous"]),"native only last genuine chunk may replay once without refund")
                owner["replayed"] = True
                continue
            api.require(r["Sequence"] == (0 if action==5 else owner["sequence"]+1),"native column omitted/duplicate invocation sequence")
            if action != 5: owner["sequence"],owner["replayed"] = r["Sequence"],False
        name = owner["name"]
        m = result["columnGenerations"][name][g]
        axis = manifest(api,m,r["Binding"],initial,old if name==0 else final,part)
        work(api,r,owner["previous"],spool,part,m["Generation"])
        body = b"" if r["Body"] is None else api.raw_bytes(r["Body"])
        api.require(len(body) <= r["Binding"]["Policy"]["ChunkBytes"] and (r["Rows"] is None or type(r["Rows"]) is list)
                    and len(r["Rows"] or []) <= 2,"native column admitted output chunk")
        if action != 5:
            api.require(not r["Closed"] and r["ElapsedNanos"] <= r["Binding"]["Policy"]["Lifetime"],"native column expired owner supplied data/EOF")
        if action == 2:
            api.require(stage == owner["stage"] in ("old-seal","final-seal") and owner["cursor"] is None and not body and not r["Rows"],"native column bounded publication action")
            if r["EOF"]:
                api.require((name,g) not in published and api.same(r["Manifest"],m),"native genuine local source EOF/seal mismatch")
                published.add((name,g))
        elif action == 3:
            api.require(stage in expected_walks and (name,g) in published and api.same(r["Manifest"],m) and not r["EOF"]
                        and not body and not r["Rows"] and (owner["cursor"] is None or owner["cursor"]["done"]),"native source-bound read admission")
            expected_cached = stage.endswith("/cached") or stage.endswith("cached-true")
            api.require(r["Cached"] == expected_cached and r["Fill"] == stage.endswith("/cache-fill"), "native actual cache/native read dispatch")
            owner["cursor"] = {"stage":stage,"cached":r["Cached"],"fill":r["Fill"],"ordinal":0,"index":initial[g]["sourceIndex"],"chain":bytes(32),"image":initial[g]["image"],"frames":0,"rows":0,"bytes":0,"done":False}
        elif action == 4:
            c = owner["cursor"]
            api.require(c is not None and not c["done"] and c["stage"] == stage and r["Ordinal"] == c["ordinal"]
                        and r["Cached"] == c["cached"] and r["Fill"] == c["fill"]
                        and api.same(r["Manifest"],m),"native original continuation cursor")
            if r["EOF"]:
                api.require(not body and not r["Rows"] and c["ordinal"] == m["Chunks"] and c["frames"] == m["Frames"]
                            and c["rows"] == m["Rows"] and c["bytes"] == m["Bytes"] and c["index"] == m["Positions"][g]
                            and c["image"] == (old if name==0 else final)[g]["image"]
                            and c["chain"] == array(api,m["Chain"],32) and (stage,g) not in completed,"native complete source/read EOF census")
                c["done"] = True; completed.add((stage,g))
                if r["Fill"] or r["Cached"]:
                    cache_manifest(api,path.parent,g,m)
            else:
                api.require(c["ordinal"] < m["Chunks"],"native data beyond immutable EOF")
                frames = chunk(api,body,m["Generation"],c["ordinal"],c["chain"],r["Binding"]["Policy"])
                expected = []
                for frame in frames:
                    api.require(frame["Partition"] == part and frame["Index"] == c["index"]+1,"native complete actual source prefix index")
                    root(api,frame["Root"],part)
                    if not frame["Outcome"] or frame["Outcome"].startswith(b"GSO\1") or frame["OriginalOutcome"]:
                        api.require(frame["Root"] == c["image"], "native empty/control/replay frame changed original native root")
                    expected.extend(typed_rows(api,frame,axis,spool,initial[g],bounds["from"][g],m["Generation"]["Sources"][g]["Generation"]))
                    c["index"] += 1
                    c["image"] = frame["Root"]
                    c["frames"] += 1; c["rows"] += len(frame["Rows"])
                api.require(api.same(r["Rows"],expected),"native selected column chunk receipt differs from actual numeric columns")
                if r["Cached"] or r["Fill"]: cache_chunk(api,path.parent,g,m,c["ordinal"],body,spool)
                c["bytes"] += 66 + len(body)
                c["chain"] = hashlib.sha256(c["chain"]+hashlib.sha256(body).digest()).digest()
                c["ordinal"] += 1; total_rows += len(expected)
        elif action == 5:
            api.require(r["Closed"] and not r["EOF"] and not body and not r["Rows"] and owner["cursor"] is not None
                        and owner["cursor"]["done"],"native owner cleanup before complete read EOF")
            closed.add((owner["stage"],g)); owners[g] = None
            if r["CacheLedger"]["Version"]: last_cache[g] = r["CacheLedger"]
        owner["previous"] = r
        owner["lastAction"],owner["lastStage"] = action,stage
    api.require(owners == [None,None] and published == {(n,g) for n in range(2) for g in range(2)}
                and completed == {(s,g) for s in expected_walks for g in range(2)}
                and closed == {(s,g) for s in allowed_open for g in range(2)},"native missing/partial owner/publisher/read/cleanup EOF set")
    for name in range(2):
        left,right = bindings[name]
        api.require(api.same(left | {"Coverage":3},right | {"Coverage":3})
                    and api.same(result["columnGenerations"][name][0]["Generation"]["ID"],result["columnGenerations"][name][1]["Generation"]["ID"])
                    and api.same(result["columnGenerations"][name][0]["Generation"]["Sources"],result["columnGenerations"][name][1]["Generation"]["Sources"]),
                    "native disjoint genuine EOF full-cut/config/peer/policy/source-generation join")
    for g in range(2): cache_terminal(api,path.parent,g,last_cache[g],spool)
    return {"format":RUNNER,"layout":LAYOUT,"sealedLocalEOFs":4,"completeReadEOFs":22,"closedOwners":14,
            "binaryRowsChecked":total_rows,"controlPayloadRequestBytes":actual_control_requests,"controlPayloadReplyBytes":actual_control_replies,
            "scope":"actual native typed source/cut/original/replay binding and real cache bytes; unchanged finite spool; no runtime/capacity/storage-efficiency acceptance"}


def cache_read(api,directory,g,name,maximum):
    path = directory / str(1+3*g) / "native-column-cache" / name
    api.require(path.is_file() and not path.is_symlink() and path.stat().st_size <= maximum,"native actual bounded cache file missing/oversized")
    with path.open("rb") as source: body = source.read(maximum+1)
    api.require(len(body) <= maximum,"native actual cache byte admission")
    return body


def cache_manifest(api,directory,g,m):
    name = "manifest-"+hashlib.sha256(sealed_json(api,m["Generation"])).hexdigest()+".bin"
    api.require(cache_read(api,directory,g,name,16384) == sealed_json(api,m),"native actual cache manifest differs from genuine immutable seal")


def cache_chunk(api,directory,g,m,ordinal,body,spool):
    name = f"chunk-{hashlib.sha256(sealed_json(api,m['Generation'])).hexdigest()}-{ordinal:016x}.bin"
    api.require(cache_read(api,directory,g,name,65536) == body,"native actual cached binary columns differ from source")
    key = bytes([g])+name.encode()
    prior = spool.find(b"native-column-cache",key)
    value = struct.pack(">Q",len(body))+hashlib.sha256(body).digest()
    if prior is None: spool.insert(b"native-column-cache",key,value)
    else: api.require(prior == value,"native immutable cache filename/body changed")


def cache_terminal(api,directory,g,ledger,spool):
    api.require(ledger is not None,"native complete actual cache ledger missing")
    body = cache_read(api,directory,g,"LEDGER",16384)
    api.require(body == sealed_json(api,ledger),"native terminal actual cache ledger changed")
    charged,files = 32768,0
    for key,value in spool.walk(b"native-column-cache"):
        if key[0] != g: continue
        charged += len(key)-1+struct.unpack_from(">Q",value)[0]; files += 1
    # Each of the two sealed generation manifests is a real cache object too.
    cache_dir = directory / str(1+3*g) / "native-column-cache"
    observed = 0
    with os.scandir(cache_dir) as entries:
        for entry in entries:
            if entry.name in ("LOCK","LEDGER"): continue
            api.require(not entry.is_symlink() and entry.is_file(follow_symlinks=False),"native foreign cache entry")
            observed += 1
            if entry.name.startswith("manifest-"):
                size = entry.stat(follow_symlinks=False).st_size
                api.require(len(entry.name)==77 and size <= 16384,"native actual cache manifest shape")
                charged += len(entry.name)+size
            else:
                api.require(spool.find(b"native-column-cache",bytes([g])+entry.name.encode()) is not None,"native undeclared cached chunk")
    api.require(observed == files+2 and charged == ledger["Charged"],"native complete cache EOF/name/byte ledger")


def pooled_move_status(api,wire):
    """Fresh GMO3 only. Capacity refusal remains active, never a COMMIT."""
    c=Cursor(api,checked_body(api,wire,b"GMO\3",256))
    graph,part,move,intent=c.take(16),c.number(),c.take(16),c.take(32)
    phase,result=c.number(1),c.number(1)
    index,floor,round_,sequence,rows,size=[c.number() for _ in range(6)]
    hash_=c.take(32);failed_phase,failed_sequence,failed_command=c.number(1),c.number(),c.take(32);c.done()
    api.require(graph.hex()==api.GRAPH and part in (3,8) and any(move) and any(intent) and 1<=phase<=9 and 1<=result<=5 and index>0 and sequence<=1<<24,"canonical pooled move original")
    follows=(phase==2 and sequence==0 and failed_phase==3 and failed_sequence==0 or phase in (3,4) and failed_phase==phase and failed_sequence==sequence+1)
    api.require((result==5 and follows and any(failed_command) and failed_sequence<=1<<24) or result!=5 and failed_phase==failed_sequence==0 and not any(failed_command),"exact nonterminal attempted phase/refusal identity")
    return dict(partition=part,move=move,intent=intent,phase=phase,result=result,index=index,floor=floor,round=round_,sequence=sequence,rows=rows,size=size,digest=hash_,failedPhase=failed_phase,failedSequence=failed_sequence,failedCommand=failed_command)


def pooled_move_intent(api,wire,semantic):
    c=Cursor(api,checked_body(api,wire,b"GMI\3",1024))
    request,source,target=c.take(16),c.take(120),c.take(120)
    publication=checked_body(api,c.field(256),b"RPB\1",256)
    epoch=c.number();pages,rows,bytes_=[c.number(4) for _ in range(3)]
    pools=[c.number() for _ in range(3)];c.done()
    api.require(any(request) and source[:16].hex()==api.GRAPH and source[16:24]==struct.pack(">Q",3) and target[16:24]==struct.pack(">Q",8)
                and source[:16]==target[:16] and source[48:]==target[48:] and source[88:]==semantic
                and len(publication)==104 and publication[:16]==source[:16] and 0<epoch<(1<<64)-1
                and 1<=pages<=1<<24 and 1<=rows<=32 and 8192<=bytes_<=65536 and 65536<pools[0]<=1<<40 and 65536<pools[1]<=1<<40 and 0<pools[2]<=1<<40,"exact fresh source/target/publication/pool binding")
    rid,first,last,home,initial_epoch=struct.unpack_from(">QQQQQ",publication,16)
    api.require(0<rid and 0<first<=last and home==3 and 0<initial_epoch<=epoch and any(publication[56:88]),"actual immutable HOME range publication")
    return dict(request=request,source=source,target=target,publication=publication,range=rid,first=first,last=last,epoch=epoch,pages=pages,rows=rows,bytes=bytes_,pools=pools,
                move=hashlib.sha256(b"rho-tkg:range-move-id:v1\0"+source[:24]+request).digest()[:16],digest=hashlib.sha256(wire).digest())


def pooled_move_frame(api,frame,previous,semantic,spool):
    """Authenticate I/V/D plus the actual complete command; movement has zero rows."""
    wire=api.raw_bytes(frame["Outcome"]);s=pooled_move_status(api,wire)
    api.require(s["partition"]==frame["Partition"] and s["index"]<=frame["Index"] and not frame["Rows"],"move record namespace/original/zero-business census")
    c=Cursor(api,checked_body(api,api.raw_bytes(frame["Registration"]),b"NCM1",32768))
    records=[(c.field(1024),c.number(),c.field(2048)) for _ in range(3)];c.done()
    (ik,ii,iv),(vk,vi,vv),(dk,di,dv)=records
    intent=pooled_move_intent(api,iv,semantic)
    prefix=b"GM";ns=bytes.fromhex(api.GRAPH)+struct.pack(">Q",s["partition"])
    api.require(ik==prefix+b"I\1"+ns+s["move"] and 0<ii<=s["index"] and intent["move"]==s["move"] and intent["digest"]==s["intent"],"actual same immutable intent control")
    phase,sequence=(s["failedPhase"],s["failedSequence"]) if s["result"]==5 else (s["phase"],s["sequence"])
    command=spool.lookup(b"move-command",struct.pack(">QQ",s["partition"],s["index"]))
    cmd=Cursor(api,checked_body(api,command,b"GMQ\3",1<<20))
    scope,command_phase,command_sequence=cmd.take(120),cmd.number(1),cmd.number()
    command_intent=cmd.field(1024);cmd.field((1<<20)-2048)
    proof_count=cmd.number(4);api.require(proof_count<=2,"bounded exact command proofs")
    for _ in range(proof_count):pooled_move_status(api,cmd.field(256))
    cmd.done()
    api.require(scope==(intent["source"] if s["partition"]==3 else intent["target"]) and (command_phase,command_sequence)==(phase,sequence)
                and command_intent==iv,"actual command scope/immutable phase/intent, not caller selected hash")
    command_hash=hashlib.sha256(command).digest()
    if vk==ik:
        api.require(vi==ii==s["index"] and vv==iv and phase in (1,9),"only native uncredited fence/terminal original uses I")
    else:
        credit={2:1,3:0x10000000+sequence,4:0x20000000+sequence,5:0x30000000,6:0x30000001,7:0x30000002,8:0xf0000000,9:0xf0000000 if sequence==0 else 0x40000000+sequence-1}.get(phase)
        api.require(credit is not None and vk==prefix+b"V\1"+ns+s["move"]+struct.pack(">I",credit) and vi==s["index"] and vv==command_hash+wire,"GM/V exact full GMQ3 hash, phase and first-install index")
    api.require(s["result"]!=5 or s["failedCommand"]==command_hash,"refusal is the SAME attempted command, not a later status")
    cdc=api.raw_bytes(frame["CDC"])
    if s["index"]<frame["Index"]:
        api.require(not cdc and api.raw_bytes(frame["OriginalOutcome"])==wire and array(api,frame["Root"],172)==previous,"SAME move replay cannot publish another lineage/data root")
        first=json.loads(spool.lookup(b"frame",struct.pack(">QQ",s["partition"],s["index"])),object_pairs_hook=api.pairs)
        api.require(api.raw_bytes(frame["OriginalCDC"]) in (b"",api.raw_bytes(first["CDC"])) and api.raw_bytes(first["Outcome"])==wire,"exact retained first move original even below ACK floor")
    elif cdc:
        c=Cursor(api,checked_body(api,cdc,b"GML\1",256))
        api.require(c.take(16)==s["move"] and c.take(32)==s["intent"],"lineage immutable move/intent")
        rid,source,target,old,new,round_=[c.number() for _ in range(6)];hash_=c.take(32);c.done()
        api.require(s["phase"]==7 and s["result"]==1 and (rid,source,target,old,new,round_,hash_)==(intent["range"],3,8,intent["epoch"],intent["epoch"]+1,s["round"],s["digest"]) and round_>0
                    and dk==prefix+b"D\1"+ns+s["move"] and 0<di<=s["index"] and pooled_move_status(api,dv)["phase"]==6,"genuine both-owner activation, not business GCD")
        decision=pooled_move_status(api,dv)
        api.require(decision["partition"]==3 and (di==decision["index"] if s["partition"]==3 else di==s["index"])
                    and decision["move"]==s["move"] and decision["intent"]==s["intent"] and decision["round"]==s["round"] and decision["result"]==1,
                    "activation exact source COMMIT; target control copy has its genuine target install index")
    else:
        api.require(s["phase"]!=7 or s["result"]!=1,"activation cannot omit GML")
        api.require(array(api,frame["Root"],172)==previous,"nonactivation control changed graph state")
    return s,intent
