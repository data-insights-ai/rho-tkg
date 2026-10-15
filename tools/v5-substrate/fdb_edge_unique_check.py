#!/usr/bin/env python3
"""Private effective-uniqueness/life workload oracle, never a storage backend.

Only raw codec primitives and ONE caller-granted disposable spool are reused.
Every expected key/value follows the admitted operation recipe; observed After
is checked against that recipe and its own retained Before, never made the model.
"""
import hashlib
import importlib.util
import json
from pathlib import Path

_spec = importlib.util.spec_from_file_location("edge_unique_raw", Path(__file__).resolve().parent / "fdb/fdb_contention_check.py")
raw = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(raw)
codec = raw._codec
require, fields, integer = codec.require, codec.fields, codec.integer
blob, go_json, decoded = codec.raw_bytes, codec.go_json, raw.decoded
Spool, Policy, u64 = raw.Spool, raw.Policy, raw.u64
FAMILY, FORMAT = "cross-edge-endpoint-uniqueness", "graph-edge-endpoint-unique-v1"


def metadata(value):
    fields(value, "Floor Generation Versions Transactions Certificates Bytes Groups")
    for counter in value.values():
        integer(counter)
    return value


def writer_census(spec, completed):
    for name in ("version", "facts", "crossPercent", "writers"):
        require(type(spec[name]) is int, "integer workload dimensions exclude bool")
    require(type(completed) is list and len(completed) == 32
            and all(type(value) is int and value >= 0 for value in completed),
            "strict bounded per-caller completion counters")
    total, writers = 6*spec["facts"]+7, spec["writers"]
    require(writers in (1, 8, 32) and sum(completed) == total
            and all(completed[i] == (total+writers-1-i)//writers for i in range(writers))
            and all(value == 0 for value in completed[writers:])
            and all(value > 0 for value in completed[:writers]),
            "every declared independent caller completed its exact ordinal assignment")


def endpoint(p, remote=False, reopened=False):
    n = p["facts"]
    return {"Owner": int(remote), "Node": 5*n+8 if remote else 3,
            "Life": 5*n+9+int(reopened) if remote else (4*n+7 if reopened else 4)}


def edge(p, fact, candidate, reopened=False):
    integer(fact, 0, p["facts"]-1)
    integer(candidate, 0, 1)
    return {"ID": 7+4*fact+2*candidate, "Source": {"Owner": 0, "Node": 1, "Life": 2},
            "Target": endpoint(p, raw.cross(p, fact), reopened), "Value": fact+5}


def life_key(e):
    return f'node/{e["Node"]:016x}/life/{e["Life"]:016x}'


def edge_key(kind, e):
    return f'{kind}/{e["ID"]:016x}/{e["Source"]["Node"]:016x}/{e["Source"]["Life"]:016x}/{e["Target"]["Node"]:016x}/{e["Target"]["Life"]:016x}'


def unique_key(fact):
    return f"unique/p/i64/{fact+5:016x}"


def operation(p, ordinal):
    n = p["facts"]
    integer(ordinal, 1, 6*n+7)
    if ordinal <= 3:
        return "setup", ordinal-1, 0
    if ordinal < 4+2*n:
        return "claim", (ordinal-4)//2, (ordinal-4)%2
    for phase, start in (("stale", 4+2*n), ("replace", 4+3*n), ("frozen-close", 4+4*n), ("close", 4+5*n)):
        if start <= ordinal < start+n:
            return phase, ordinal-start, None
    return "life", ordinal-4-6*n, 0


def contract(p, ordinal, candidate, release=False):
    phase, fact, fixed = operation(p, ordinal)
    if fixed is not None:
        require(candidate == fixed, "operation candidate identity")
    zero = {"Owner": 0, "Node": 0, "Life": 0}
    empty_edge = {"ID": 0, "Source": zero, "Target": zero, "Value": 0}
    lifecycle = {"Provenance": ordinal}
    effects, reads = {}, {}
    if phase in ("setup", "life"):
        if phase == "setup":
            node = ({"Owner": 0, "Node": 1, "Life": 2}, endpoint(p), endpoint(p, True))[fact]
            kind = "node-create"
        else:
            node, kind = endpoint(p, fact >= 2, fact % 2 == 1), "node-create" if fact % 2 else "node-close"
        command = {"Kind": kind, "Node": node, "Edge": empty_edge}
        effects[node["Owner"]] = [{"Key": life_key(node), "Value": int(kind == "node-create"), "Delete": kind == "node-close"}]
        reads[node["Owner"]] = [life_key(node)]
        return phase, fact, command, lifecycle, effects, reads
    e = edge(p, fact, candidate, phase == "replace")
    closing = phase in ("close", "frozen-close")
    if closing:
        e["Value"] = 0
    command = {"Kind": "edge-close" if closing else "edge-create", "Node": zero, "Edge": e}
    lifecycle["EdgeLife"] = e["ID"]+1
    if not closing:
        lifecycle["Definition"] = {"ID": 4*p["facts"]+8+fact, "Kind": "i64", "I64": fact+5}
    for kind, g in (("edge", 0), ("out", 0), ("in", e["Target"]["Owner"])):
        effects.setdefault(g, []).append({"Key": edge_key(kind, e), "Value": e["Value"], "Delete": closing})
    if not closing or release:
        effects[0].append({"Key": unique_key(fact), "Value": 0 if closing else e["ID"], "Delete": closing})
    reads[0] = [edge_key("edge", e), unique_key(fact)]
    if closing:
        reads[0].append(edge_key("out", e))
        reads.setdefault(e["Target"]["Owner"], []).append(edge_key("in", e))
    else:
        reads[0].append(life_key(e["Source"]))
        reads.setdefault(e["Target"]["Owner"], []).append(life_key(e["Target"]))
    return phase, fact, command, lifecycle, {g: sorted(rows, key=lambda r: r["Key"]) for g, rows in effects.items()}, {g: sorted(rows) for g, rows in reads.items()}


def current(spool, g, logical, namespace=b"current"):
    body = spool.find(namespace, u64(g)+logical.encode())
    return None if body is None else decoded(body)


def put_current(spool, namespace, g, logical, value):
    k, body = u64(g)+logical.encode(), go_json(value)
    prior = spool.find(namespace, k)
    if prior is None:
        spool.insert(namespace, k, body)
    else:
        spool.replace(namespace, k, prior, body)


def live(spool, e):
    v = current(spool, e["Owner"], life_key(e))
    return v is not None and not v["Deleted"] and v["Value"] == 1


def candidate_from_command(p, command):
    identity = integer(command["Edge"]["ID"], 7)
    require((identity-7) % 2 == 0 and (identity-7)//4 < p["facts"], "declared actual relationship identity")
    return ((identity-7) % 4)//2


def capture(a, tx, end, reads):
    """Bind the real initial read transaction to this original's whole footprint."""
    observation, c = a["Capture"], end["Config"]
    fields(observation, "Metadata ReadVersion Reads UniqueRange Begin End")
    integer(observation["ReadVersion"], 1)
    require(len(observation["Metadata"]) == len(observation["Reads"]) == 2,
            "complete native capture")
    by_group = {part["Group"]: part for part in tx["Participants"]}
    for g in range(2):
        metadata(observation["Metadata"][g])
        records = observation["Reads"][g] or []
        wanted = reads.get(g, [])
        require(len(records) == len(wanted), "captured exact point footprint")
        for record, logical in zip(records, wanted, strict=True):
            fields(record, "Key Value")
            require(blob(record["Key"]) == raw.key(c, g, "current", logical),
                    "captured native point key")
            body = None if record["Value"] is None else blob(record["Value"])
            v = None if body is None else decoded(body)
            version = 0 if v is None else integer(v["Version"], 1)
            if v is not None:
                fields(v, "Value Version Round Deleted TxID")
                require(type(v["Deleted"]) is bool and integer(v["Round"], 1)
                        and isinstance(v["TxID"], str) and v["TxID"], "actual captured value")
            part = by_group[g]
            require(part["Generation"] == observation["Metadata"][g]["Generation"]
                    and next(r["Version"] for r in part["Reads"] if r["Key"] == logical) == version,
                    "proposal binds native captured revision/generation")
    command = tx["GraphChange"]
    rows = observation["UniqueRange"] or []
    if command["Kind"] == "edge-create":
        key = raw.key(c, 0, "current", unique_key(a["Fact"]))
        require(blob(observation["Begin"]) == key and blob(observation["End"]) == key+b"\0"
                and len(rows) <= 1, "actual singleton native unique range")
        point = next(record for record in observation["Reads"][0]
                     if blob(record["Key"]) == key)
        require(rows == ([] if point["Value"] is None else [point]),
                "empty/range answer agrees with same native read transaction")
    else:
        require(not rows and observation["Begin"] is None and observation["End"] is None,
                "non-create cannot fabricate unique range observation")


def attempt(a, end, spool):
    fields(a, "Family Engine Phase PreviousRequest Fact Candidate Ordinal Client Attempt ReadCallbacks Capture Proposal Digest Submissions Proof Classification Error")
    p, c = end["spec"], end["Config"]
    phase, fact, fixed = operation(p, a["Ordinal"])
    candidate = integer(a["Candidate"], 0, 1)
    require(a["Family"] == FAMILY and a["Engine"] == "foundationdb" and a["Phase"] == phase and a["Fact"] == fact, "fresh family operation")
    client, number = integer(a["Client"], 0, p["writers"]-1), integer(a["Attempt"], 1, 64)
    require(client == (a["Ordinal"]-1) % p["writers"], "actual declared writer assignment")
    integer(a["ReadCallbacks"], 2, 16*(1+8))
    body, proof = blob(a["Proposal"]), a["Proof"]
    require(0 < len(body) <= 8192 and hashlib.sha256(body).digest() == codec.sha_array(a["Digest"]), "exact frozen body/hash")
    tx = decoded(body)
    fields(tx, "Graph Topology ID Request Coordinator Dependency Participants GraphChange Lifecycle")
    release = any(effect["Key"] == unique_key(fact) for part in tx["Participants"] for effect in part["Effects"]) if phase in ("close", "frozen-close") else False
    _, _, command, lifecycle, effects, reads = contract(p, a["Ordinal"], candidate, release)
    require(tx["Graph"] == c["Graph"] and tx["Topology"] == c["Topology"] and tx["Dependency"] == 0 and tx["GraphChange"] == command and tx["Lifecycle"] == lifecycle, "canonical business recipe/lives/scalar definition")
    expected_id = f"edge-unique-{phase}-{fact}-{candidate}-c{client}-a{number}"
    require(tx["ID"] == expected_id and tx["Request"] == "request/"+expected_id and tx["Coordinator"] == (command["Node"]["Owner"] if phase in ("setup", "life") else 0), "original namespace/identity")
    require([part["Group"] for part in tx["Participants"]] == sorted(effects), "complete participant footprint")
    for part in tx["Participants"]:
        fields(part, "Group Epoch Generation Reads Effects")
        g = integer(part["Group"], 0, 1)
        require(part["Epoch"] == c["Epochs"][g] and part["Effects"] == effects[g] and [r["Key"] for r in part["Reads"]] == reads[g], "immutable full read/effect footprint")
        for r in part["Reads"]:
            fields(r, "Key Version")
            integer(r["Version"])
    capture(a, tx, end, reads)
    chain_key = u64(a["Ordinal"])
    previous_raw = spool.find(b"attempt-chain", chain_key)
    previous = None if previous_raw is None else decoded(previous_raw)
    require(previous is None and number == 1 and a["PreviousRequest"] == "" or previous is not None and number == previous["number"]+1 and a["PreviousRequest"] == previous["request"] and previous["reason"] in ("generation", "revision"), "only authentic ABORT permits linked replan")
    fields(proof, "Pair Original Group Outcome")
    o = proof["Outcome"]
    fields(o, "Commit Round Digest ID Request Reason Coordinator Sequence")
    require(o["Digest"] == a["Digest"] and o["ID"] == tx["ID"] and o["Request"] == tx["Request"] and o["Coordinator"] == tx["Coordinator"] and len(proof["Pair"]) == 2, "native paired original identity")
    for record, kind, name in zip(proof["Pair"], ("id", "request"), (tx["ID"], tx["Request"]), strict=True):
        raw.raw_record(record, raw.key(c, tx["Coordinator"], kind, name), go_json(o))
        spool.insert(b"original-"+kind.encode(), blob(record["Key"]), blob(record["Value"]))
    require(a["Error"] == "" and type(o["Commit"]) is bool and 1 <= len(a["Submissions"]) <= 8, "actual completed attempt")
    for index, sub in enumerate(a["Submissions"], 1):
        fields(sub, "Ordinal Callbacks Outcome Error Retryable MaybeCommitted")
        require(sub["Ordinal"] == index and integer(sub["Callbacks"], 1, 16), "bounded actual SDK callbacks")
        if not sub["Error"]:
            require(sub["Outcome"] == o and index == len(a["Submissions"]), "successful return matches retained original")
        elif index < len(a["Submissions"]):
            require(sub["Retryable"] or sub["MaybeCommitted"], "same-body recovery only after classified ambiguity")
    if o["Commit"]:
        require(a["Classification"] == "committed" and o["Reason"] == "" and integer(o["Round"], 1) and integer(o["Sequence"], 1) and phase not in ("stale", "frozen-close"), "genuine useful COMMIT")
        group = decoded(blob(proof["Original"]["Value"]))
        require(group["Tx"] == tx and group["Outcome"] == o and group == proof["Group"], "full native CDC original")
        raw.raw_record(proof["Original"], raw.prefix(c, o["Coordinator"])+f'changes/{o["Sequence"]:016x}/'.encode()+o["Request"].encode().hex().encode(), go_json(group))
        spool.insert(b"operation", chain_key, go_json(group))
    else:
        require(a["Classification"] == "aborted" and o["Reason"] in ("generation", "revision", "unique", "endpoint-life") and o["Round"] == o["Sequence"] == 0 and proof["Original"] == {"Key": None, "Value": None}, "real ABORT, not unknown or CDC")
    value = go_json({"number": number, "request": tx["Request"], "reason": o["Reason"]})
    if previous_raw is None:
        spool.insert(b"attempt-chain", chain_key, value)
    else:
        spool.replace(b"attempt-chain", chain_key, previous_raw, value)
    prior_metrics = spool.find(b"timing-attempts", chain_key)
    metrics = {"Attempts": 0, "Submissions": 0, "ReadCallbacks": 0, "SDKCallbacks": 0} if prior_metrics is None else decoded(prior_metrics)
    metrics["Attempts"] += 1
    metrics["Submissions"] += len(a["Submissions"])
    metrics["ReadCallbacks"] += a["ReadCallbacks"]
    metrics["SDKCallbacks"] += sum(sub["Callbacks"] for sub in a["Submissions"])
    metrics["Commit"] = o["Commit"]
    if prior_metrics is None:
        spool.insert(b"timing-attempts", chain_key, go_json(metrics))
    else:
        spool.replace(b"timing-attempts", chain_key, prior_metrics, go_json(metrics))
    return o


def timing_row(row, end, spool):
    fields(row, "Phase Fact Candidate Ordinal Client StartNanos StopNanos ElapsedNanos AdmissionNanos Attempts Submissions ReadCallbacks SDKCallbacks Commit")
    phase, fact, fixed = operation(end["spec"], integer(row["Ordinal"], 1))
    require(row["Phase"] == phase and row["Fact"] == fact
            and row["Candidate"] in (0, 1) and (fixed is None or row["Candidate"] == fixed)
            and row["Client"] == (row["Ordinal"]-1) % end["spec"]["writers"],
            "logical latency is bound to the admitted operation/caller")
    for field in ("Fact", "Candidate", "Client", "StartNanos", "AdmissionNanos"):
        integer(row[field])
    integer(row["StopNanos"], 1, end["TotalNanos"])
    integer(row["ElapsedNanos"], 1)
    require(row["StopNanos"]-row["StartNanos"] == row["ElapsedNanos"]
            and row["AdmissionNanos"] <= row["ElapsedNanos"],
            "monotone complete-operation elapsed/admission interval")
    metrics = decoded(spool.lookup(b"timing-attempts", u64(row["Ordinal"])))
    for field in ("Attempts", "Submissions", "ReadCallbacks", "SDKCallbacks"):
        require(integer(row[field], 1) == metrics[field], "latency includes every same-original/replan SDK invocation")
    require(type(row["Commit"]) is bool and row["Commit"] == metrics["Commit"], "actual final logical decision")
    spool.insert(b"timing-seen", u64(row["Ordinal"]), b"1")
    spool.insert(b"latency-order", u64(row["ElapsedNanos"])+u64(row["Ordinal"]), b"1")


def timings(directory, end, spool):
    digest, count = hashlib.sha256(), 0
    for row, line in raw.lines(directory/"fdb-workload-latency.jsonl", 6*end["spec"]["facts"]+8):
        timing_row(row, end, spool)
        count += 1
        digest.update(line)
    require(count == end["LogicalOperations"] == 6*end["spec"]["facts"]+7
            and digest.hexdigest() == end["LatencyPayloadSHA256"], "complete hash-bound logical timing stream")
    ranks = {name: (count*percent+99)//100 for name, percent in (("p50", 50), ("p95", 95), ("p99", 99))}
    result = {}
    for index, (key, _) in enumerate(spool.walk(b"latency-order"), 1):
        for name, rank in ranks.items():
            if index == rank:
                result[name+"Nanos"] = int.from_bytes(key[:8], "big")
    require(len(result) == 3, "bounded spool percentile ranks")
    return {**result, "logicalOperations": count,
            "usefulTransactionsPerSecond": end["Committed"]*1_000_000_000/end["BusinessNanos"],
            "scope": "queue admission through final retained original, including same-original/replan waits; frozen-plan capture is separately inside business-phase time"}


def raw_stream(directory, name, end):
    digest, header, footer, counts, previous = hashlib.sha256(), None, False, [0, 0], None
    for value, line in raw.lines(directory/("fdb-cost-"+name+".jsonl"), 11*end["spec"]["facts"]+10):
        require(not footer, "raw data after EOF")
        if header is None:
            header = value
            fields(header, "Type Format Stream Config InitialCut OldCut FinalCut Policy Metadata ConfigRecords MetadataRecords CutRecords")
            require(header["Type"] == "header" and header["Format"] == "fdb-graph-cost-raw-v2" and header["Stream"] == name and header["Config"] == end["Config"] and header["InitialCut"] == end["Initial"] and header["OldCut"] == end["Old"] and header["FinalCut"] == end["Final"] and header["Policy"] == {"PageRows":128,"PageBytes":16<<20}, "exact source/config/cuts")
            for g in range(2):
                metadata(header["Metadata"][g])
                raw.raw_record(header["ConfigRecords"][g], raw.key(end["Config"],g,"meta","config"), go_json(end["Config"]))
                raw.raw_record(header["MetadataRecords"][g], raw.key(end["Config"],g,"meta","state"), go_json(header["Metadata"][g]))
            for record, cut in zip(header["CutRecords"], (end["Initial"],end["Old"],end["Final"]), strict=True):
                raw.raw_record(record,raw.key(end["Config"],0,"cut",go_json(cut).hex()),go_json(cut))
            digest.update(line)
            yield "header",header
        elif value.get("Type") == "footer":
            require(value == {"Type":"footer","Rows":counts,"Groups":end["Final"]["Groups"],"Metadata":header["Metadata"],"PayloadSHA256":digest.hexdigest()}, "raw exact EOF/hash")
            require(end["Sources"][("history","current","cdc").index(name)] == {"Rows":counts,"Groups":end["Final"]["Groups"],"Metadata":header["Metadata"]}, "source receipt/EOF")
            footer=True
        else:
            fields(value,"Type Row")
            require(value["Type"] == "row", "unknown raw frame")
            row=value["Row"]
            fields(row,"Group Key Value"+(" IDKey IDValue RequestKey RequestValue" if name=="cdc" else ""))
            row={k:v if k=="Group" else blob(v) for k,v in row.items()}
            g=integer(row["Group"],0,1)
            order=(decoded(row["Value"])["Outcome"]["Round"],g,decoded(row["Value"])["Outcome"]["Request"]) if name=="cdc" else (g,row["Key"])
            require(previous is None or previous<order,"duplicate/reordered native source")
            previous=order;counts[g]+=1;digest.update(line)
            yield "row",row
    require(header is not None and footer,"truncated raw EOF")


def fold(directory,end,spool):
    p,c=end["spec"],end["Config"]
    sequences,versions,generations=[0,0],[0,0],[0,0]
    for kind,row in raw_stream(directory,"cdc",end):
        if kind=="header":
            continue
        group=decoded(row["Value"]);tx,o=group["Tx"],group["Outcome"]
        ordinal=tx["Lifecycle"]["Provenance"]
        require(row["Value"] == spool.lookup(b"operation",u64(ordinal)),"CDC exact observed useful original")
        phase,fact,_=operation(p,ordinal)
        candidate=0 if phase in ("setup","life") else candidate_from_command(p,tx["GraphChange"])
        release=False
        if phase=="close":
            head=current(spool,0,unique_key(fact))
            require(head is not None and head["Value"] != tx["GraphChange"]["Edge"]["ID"],"old close cannot own new-life head")
        _,_,command,lifecycle,effects,_=contract(p,ordinal,candidate,release)
        require(tx["GraphChange"]==command and tx["Lifecycle"]==lifecycle,"fixed scalar/life operation")
        if phase in ("claim","replace"):
            e=command["Edge"]
            require(live(spool,e["Source"]) and live(spool,e["Target"]),"actual exact endpoint lives")
            head=current(spool,0,unique_key(fact))
            if phase=="claim":
                require(head is None,"genuine empty unique range winner")
                spool.insert(b"old-winner",u64(fact),u64(candidate))
            else:
                old_candidate=int.from_bytes(spool.lookup(b"old-winner",u64(fact)),"big")
                prior=edge(p,fact,old_candidate)
                require(candidate==1-old_candidate and head is not None and head["Value"]==prior["ID"] and not live(spool,prior["Target"]),"ABA replacement only after actual old support ends")
        if phase=="life" and fact%2:
            require(not live(spool,endpoint(p,fact>=2,False)),"newLife reopen cannot overlap old support")
        expected=[(g,effect) for g,rows in sorted(effects.items()) for effect in rows]
        require(len(group["Writes"])==len(expected),"no collateral or omitted writes")
        for write,(g,effect) in zip(group["Writes"],expected,strict=True):
            fields(write,"Group Key Before After" if "Before" in write else "Group Key After")
            prior=current(spool,g,effect["Key"])
            require(write["Group"]==g and write["Key"]==effect["Key"] and write.get("Before")==prior,"authentic predecessor/full footprint")
            after={"Value":effect["Value"],"Version":1 if prior is None else prior["Version"]+1,"Round":o["Round"],"Deleted":effect["Delete"],"TxID":tx["ID"]}
            require(write["After"]==after,"exact recipe scalar/tombstone/revision, no observed-After model")
            put_current(spool,b"current",g,effect["Key"],after)
            if o["Round"]<=end["Old"]["Round"]:
                put_current(spool,b"old",g,effect["Key"],after)
            hkey=raw.prefix(c,g)+f'history/{o["Round"]:016x}/'.encode()+effect["Key"].encode().hex().encode()
            spool.insert(b"history",hkey,go_json(after));versions[g]+=1
        for part in tx["Participants"]:
            generations[part["Group"]]+=1
        sequences[o["Coordinator"]]+=1
        require(o["Sequence"]==sequences[o["Coordinator"]],"complete coordinator sequence")
        for name,field in (("id","ID"),("request","Request")):
            require(row[field+"Key"] == raw.key(c,o["Coordinator"],name,o[field]) and row[field+"Value"] == spool.lookup(b"original-"+name.encode(),row[field+"Key"]) == go_json(o),"native CDC/paired originals")
        spool.insert(b"cdc-seen",u64(ordinal),row["Value"])
    require(sum(sequences)==3*p["facts"]+7 and sequences==end["Final"]["Groups"] and sum(versions)==11*p["facts"]+7,"full lifecycle CDC/source census")
    for phase in ("old","current"):
        for fact in range(p["facts"]):
            old_candidate=int.from_bytes(spool.lookup(b"old-winner",u64(fact)),"big")
            wanted=edge(p,fact,old_candidate if phase=="old" else 1-old_candidate,phase=="current")
            head=current(spool,0,unique_key(fact),phase.encode())
            require(head is not None and not head["Deleted"] and head["Value"]==wanted["ID"],"exact old/current uniqueness owner")
            if phase=="current":
                prior=edge(p,fact,old_candidate)
                require(current(spool,0,edge_key("edge",prior))["Deleted"],"old incumbent stays closed after same-original replay")
    for name,count in (("history",11*p["facts"]+7),("current",7*p["facts"]+5)):
        observed=0
        for kind,row in raw_stream(directory,name,end):
            if kind=="header": continue
            if name=="history":
                expected=spool.lookup(b"history",row["Key"])
            else:
                prefix=raw.prefix(c,row["Group"])+b"current/"
                require(row["Key"].startswith(prefix),"actual current namespace")
                logical=bytes.fromhex(row["Key"][len(prefix):].decode()).decode()
                expected=go_json(current(spool,row["Group"],logical))
            require(row["Value"]==expected,"complete raw exact set, no phantom/tombstone omission")
            spool.insert(b"raw-seen-"+name.encode(),row["Key"],b"1");observed+=1
        require(observed==count,"physical raw set census")
    for g,m in enumerate(end["FinalMetadata"]):
        require(m["Versions"]==versions[g] and m["Generation"]==generations[g] and m["Groups"]==sequences[g],"native metadata matches actual full fold")


def typed_row(group):
    tx,o=group["Tx"],group["Outcome"]
    c,m=tx["GraphChange"],tx["Lifecycle"]
    row={"Kind":0,"Owner":0,"Life":0,"SourceNode":0,"SourceLife":0,"TargetNode":0,"TargetLife":0,"Value":0,"ValueID":0,"Provenance":m["Provenance"],"Round":o["Round"],"Sequence":o["Sequence"],"Coordinator":o["Coordinator"],"Original":None}
    if c["Kind"] in ("node-create","node-close"):
        row.update(Kind=1 if c["Kind"]=="node-create" else 4,Owner=c["Node"]["Node"],Life=c["Node"]["Life"],Value=int(c["Kind"]=="node-create"))
    else:
        e=c["Edge"]
        row.update(Kind=2 if c["Kind"]=="edge-create" else 5,Owner=e["ID"],Life=m["EdgeLife"],SourceNode=e["Source"]["Node"],SourceLife=e["Source"]["Life"],TargetNode=e["Target"]["Node"],TargetLife=e["Target"]["Life"],Value=e["Value"],ValueID=m["Definition"]["ID"] if c["Kind"]=="edge-create" else 0)
    return row


def columns(directory,end,spool):
    path=directory/"fdb-workload-columns.jsonl"
    if end["spec"]["layout"]=="kv":
        require(not path.exists() and all(m["Chunks"]==0 for m in end["Columns"]),"KV cannot claim columns")
        return
    n=end["spec"]["facts"];groups=3*n+7
    policy={"ChunkRows":32,"ChunkBytes":65536,"MaxChunks":2*((groups+6)//7),"MaxGenerations":2,"MaxRetainedBytes":(64<<20)+2*groups*(65536+1024),"MaxWorkRows":groups,"MaxWorkBytes":(64<<20)+groups*(256<<10),"Version":1}
    name=None;counts=[0,0];completed=0
    for frame,_ in raw.lines(path,8*groups+4):
        if frame["Type"]=="header":
            require(name is None and completed<2,"column header/previous EOF")
            fields(frame,"Type Binding Manifest")
            name=("old","final")[completed];binding,m=frame["Binding"],frame["Manifest"]
            fields(binding,"Version Layout Schema Config Scope Policy")
            require(binding=={"Version":2,"Layout":"effective-unique-edge-life-i64-columns-v1","Schema":"node-life-create-close;edge-life-bound-create-close;property-p:i64:effective-unique;canonical-original:v1","Config":end["Config"],"Scope":codec.SCOPE,"Policy":policy},"fresh layout/policy agreement")
            fields(m,"Generation Metadata Chunks Rows Bytes History Chain")
            require(m==end["Columns"][completed] and m["Generation"]=={"Version":1,"ID":name,"Binding":list(hashlib.sha256(go_json(binding)).digest()),"Initial":end["Initial"],"Cut":end["Old" if name=="old" else "Final"]},"genuine exact generation/cut")
            require(m["Metadata"]==end["OldMetadata" if name=="old" else "FinalMetadata"] and m["Rows"]==(n+3 if name=="old" else groups) and m["History"]==[item["Versions"] for item in m["Metadata"]],"complete publication source census")
            chain,rows,retained,sequences,previous=bytes(32),0,0,[0,0],None
            for ordinal in range(integer(m["Chunks"],1,policy["MaxChunks"])):
                filename=f'column-{hashlib.sha256(go_json(m["Generation"])).hexdigest()}-{ordinal:016x}.bin'
                cache=directory/filename
                require(cache.is_file() and not cache.is_symlink(),"owned bounded binary cache")
                with cache.open("rb") as source:
                    body=source.read(65537)
                require(114<=len(body)<=65536 and body[:8]==b"RHOCLM02" and body[8:40]==bytes(m["Generation"]["Binding"]) and body[40:72]==hashlib.sha256(go_json(m["Generation"])).digest() and int.from_bytes(body[72:80],"big")==ordinal and body[80:112]==chain,"binary layout/binding/generation/chain")
                count=int.from_bytes(body[112:114],"big");require(1<=count<=32,"binary row admission")
                arena=114+98*count;require(arena<=len(body),"column arrays admission")
                offset=0
                for i in range(count):
                    numbers=[int.from_bytes(body[114+2*count+(column*count+i)*8:114+2*count+(column*count+i+1)*8],"big") for column in range(11)]
                    spans=114+2*count+88*count
                    start=int.from_bytes(body[spans+i*4:spans+(i+1)*4],"big")
                    length=int.from_bytes(body[spans+count*4+i*4:spans+count*4+(i+1)*4],"big")
                    require(start==offset and 0<length<=len(body)-arena-offset,"canonical admitted original span")
                    original=body[arena+offset:arena+offset+length];offset+=length
                    group=decoded(original);tx,o=group["Tx"],group["Outcome"]
                    require(original==spool.lookup(b"operation",u64(tx["Lifecycle"]["Provenance"])),"column exact authentic CDC original")
                    wanted=typed_row(group)
                    values=[wanted[k] for k in ("Owner","Life","SourceNode","SourceLife","TargetNode","TargetLife","Value","ValueID","Provenance","Round","Sequence")]
                    require(body[114+i]==wanted["Kind"] and body[114+count+i]==wanted["Coordinator"] and numbers==[v%(1<<64) for v in values],"binary typed life/scalar/closure exact original")
                    order=o["Round"],o["Coordinator"],o["Request"]
                    require((previous is None or previous<order) and o["Round"]<=m["Generation"]["Cut"]["Round"] and o["Sequence"]==sequences[o["Coordinator"]]+1,"binary native original enumeration")
                    previous=order;sequences[o["Coordinator"]]+=1
                    spool.insert(b"column-row-"+name.encode(),u64(rows),go_json(group));rows+=1
                require(arena+offset==len(body),"no hidden column arena bytes")
                chain=hashlib.sha256(chain+hashlib.sha256(body).digest()).digest()
                native_key=raw.prefix(end["Config"])+b"columns-v1/generation/"+name.encode().hex().encode()+f"/chunk/{ordinal:016x}".encode()
                retained+=len(native_key)+len(body)
            require(chain==bytes(m["Chain"]) and rows==m["Rows"] and sequences==m["Generation"]["Cut"]["Groups"] and retained==m["Bytes"],"complete binary native EOF/census")
            counts=[0,0]
        elif frame["Type"]=="row":
            fields(frame,"Type Generation Cached Row")
            require(name is not None and frame["Generation"]==name and type(frame["Cached"]) is bool,"column walk scope")
            index=int(frame["Cached"])
            original=blob(frame["Row"]["Original"]);group=decoded(original)
            require(original==spool.lookup(b"column-row-"+name.encode(),u64(counts[index])),"native/cache walk matches exact binary original order")
            expected=typed_row(group);expected["Original"]=frame["Row"]["Original"]
            require(frame["Row"]==expected,"native/cache typed row exact scalar/life/tombstone")
            counts[index]+=1
        else:
            fields(frame,"Type Generation Counts")
            require(frame["Type"]=="footer" and name is not None and frame["Generation"]==name and counts==[m["Rows"],m["Rows"]] and frame["Counts"]==[2*v for v in m["Generation"]["Cut"]["Groups"]],"both complete native/cache EOF walks")
            name=None;completed+=1
    require(completed==2 and name is None,"truncated columns")


def views(directory,end,spool):
    active=None;counts=[0,0];finished=0
    for frame,_ in raw.lines(directory/"fdb-workload-views.jsonl",8*end["spec"]["facts"]+10):
        if frame["Type"]=="header":
            require(active is None and frame["Index"]==finished and finished<2,"native At view ordering")
            active=finished;counts=[0,0]
            selection=frame["Selection"];at=end["Old" if active==0 else "Final"]
            require(selection["Request"]=={"Mode":"At","Scope":[0,1],"At":at} and selection["Selected"]==at,"exact authenticated old/current At selection")
            owner=decoded(blob(selection["AcquiredOwner"]["Value"]))
            require(owner["Cut"]==at and owner["State"]==1,"actual native active view owner")
        elif frame["Type"]=="row":
            require(active is not None and frame["Index"]==active,"view row scope")
            row=frame["Row"];fields(row,"Group Key Value Current History Chain")
            g,logical=integer(row["Group"],0,1),row["Key"]
            expected=current(spool,g,logical,b"old" if active==0 else b"current")
            require(expected is not None and not expected["Deleted"] and row["Value"]==expected,"actual retained old/current answer, not current substitution")
            raw.raw_record(row["Current"],raw.key(end["Config"],g,"current",logical),go_json(current(spool,g,logical)))
            hkey=raw.prefix(end["Config"],g)+f'history/{expected["Round"]:016x}/'.encode()+logical.encode().hex().encode()
            raw.raw_record(row["History"],hkey,go_json(expected))
            require(1<=len(row["Chain"])<=16,"bounded original predecessor chain")
            head=current(spool,g,logical)
            for index,record in enumerate(row["Chain"]):
                group=decoded(blob(record["Value"]));ordinal=group["Tx"]["Lifecycle"]["Provenance"]
                require(blob(record["Value"])==spool.lookup(b"cdc-seen",u64(ordinal)),"At selected from actual immutable original")
                matching=[w for w in group["Writes"] if w["Group"]==g and w["Key"]==logical and w["After"]==head]
                require(len(matching)==1,"exact native predecessor link")
                o=group["Outcome"]
                raw.raw_record({"Key":record["Key"],"Value":record["Value"]},raw.prefix(end["Config"],o["Coordinator"])+f'changes/{o["Sequence"]:016x}/'.encode()+o["Request"].encode().hex().encode(),go_json(group))
                if index==len(row["Chain"])-1:
                    require(head==expected and head["Round"]<=at["Round"] and o["Sequence"]<=at["Groups"][o["Coordinator"]],"latest authentic revision at own cut")
                else:
                    require(head["Round"]>at["Round"] and matching[0].get("Before") is not None,"no skipped post-cut predecessor")
                    before=matching[0]["Before"]
                    require(before["Version"]+1==head["Version"] and before["Round"]<head["Round"],"strict predecessor chain")
                    head=before
            spool.insert(b"view-seen-"+u64(active),u64(g)+logical.encode(),b"1");counts[g]+=1
        else:
            require(active is not None and frame["Type"]=="footer" and frame["Index"]==active and frame["End"]==end["Views"][active] and frame["End"]["Rows"]==counts and sum(counts)==3+4*end["spec"]["facts"],"complete exact old/current view EOF")
            active=None;finished+=1
    require(finished==2 and active is None,"truncated At views")


def verify(directory,spool_directory,policy=None):
    directory=Path(directory)
    end=decoded((directory/"fdb-workload-result.json").read_bytes().rstrip(b"\n"))
    fields(end,"version format spec Config Binding Initial Old Final Attempts Aborted Committed Completed OldMetadata FinalMetadata Sources Columns OldHeldRows Views AttemptPayloadSHA256 LatencyPayloadSHA256 LogicalOperations SetupNanos BusinessNanos DiagnosticNanos TotalNanos")
    p=end["spec"];fields(p,"version family facts crossPercent writers layout")
    require(end["version"]==p["version"]==1 and end["format"]==FORMAT and p["family"]==FAMILY and p["writers"] in (1,8,32) and p["crossPercent"] in (0,10,50,100) and p["layout"] in ("kv","columns") and integer(p["facts"],20,1_000_000)%20==0,"fresh family result/spec")
    c=end["Config"];require(c["Cost"]=={**{k:p[k] for k in ("facts","crossPercent","writers")},"version":4},"fresh immutable profile")
    n=p["facts"]
    binding={"Version":1,"Family":FAMILY,"Config":c,"Window":32,"MaxAttempts":64,"Policy":"effective relationship p:UniqueScalar; exact endpoint lives; distinct contenders; retained canonical old relations and originals; unknown never authorizes replan"}
    raw.raw_record(end["Binding"],raw.key(c,0,"workload-edge-unique-v1","binding"),go_json(binding))
    writer_census(p, end["Completed"])
    for name in ("OldMetadata", "FinalMetadata"):
        require(len(end[name]) == 2, "both native metadata participants")
        for value in end[name]:
            metadata(value)
    for field in ("version", "Attempts", "Aborted", "Committed", "LogicalOperations", "SetupNanos", "DiagnosticNanos"):
        integer(end[field])
    integer(end["BusinessNanos"], 1)
    integer(end["TotalNanos"], 1)
    require(end["SetupNanos"]+end["BusinessNanos"]+end["DiagnosticNanos"] == end["TotalNanos"],
            "explicit setup/business/diagnostic wall-time partition")
    require(len(end["OldHeldRows"]) == 2 and all(type(v) is int and v >= 0 for v in end["OldHeldRows"])
            and sum(end["OldHeldRows"]) == 3+4*n, "complete held old-cut source EOF")
    require(c["Limits"]=={"Bytes":(64<<20)+n*(256<<10)+(7+258*n)*2048,"Transactions":7+258*n,"Versions":7+11*n,"Certificates":1024},"full finite admitted schedule")
    run=c["Graph"].removeprefix("graph-workload-edge-unique-v1-"+p["layout"]+"-")
    require(len(run)==32 and bytes.fromhex(run).hex()==run and bytes.fromhex(run)!=bytes(16),"fresh nonzero run binding")
    with Spool(spool_directory,policy or Policy(1<<30,8<<20,4096,512<<10)) as spool:
        digest,header,footer,attempts,commits,aborts,replays=hashlib.sha256(),False,False,0,0,0,0
        for value,line in raw.lines(directory/"fdb-workload-attempts.jsonl",7+259*n+3):
            require(not footer,"attempt after EOF")
            if not header:
                require(value["Type"]=="header" and value["End"]["Config"]==c and value["End"]["spec"]==p,"attempt header binding");header=True;digest.update(line)
            elif value["Type"]=="footer":
                require(value["End"]==end and end["AttemptPayloadSHA256"]==digest.hexdigest(),"attempt exact EOF/hash");footer=True
            elif value["Type"]=="old-close-replay":
                fields(value, "Type Before After Metadata Submission")
                require(value["Before"]==value["After"] and value["Before"]["Outcome"]["Commit"],"exact original close replay")
                tx=value["Before"]["Group"]["Tx"]
                require(operation(p,tx["Lifecycle"]["Provenance"])[0]=="close"
                        and go_json(value["Before"]["Group"]) == spool.lookup(b"operation", u64(tx["Lifecycle"]["Provenance"]))
                        and value["Before"]["Outcome"] == value["Before"]["Group"]["Outcome"],"replay exact observed immutable close original")
                for record, kind in zip(value["Before"]["Pair"], ("id", "request"), strict=True):
                    require(blob(record["Value"]) == spool.lookup(b"original-"+kind.encode(), blob(record["Key"])),
                            "replayed native paired original bytes")
                sub = value["Submission"]
                fields(sub, "Ordinal Callbacks Outcome Error Retryable MaybeCommitted")
                require(integer(sub["Callbacks"], 1, 16) and (sub["Outcome"] == value["Before"]["Outcome"] if not sub["Error"] else sub["Retryable"] or sub["MaybeCommitted"]),
                        "actual duplicate submit uncertainty cannot invent a new result")
                spool.insert(b"replayed",u64(tx["Lifecycle"]["Provenance"]),b"1");replays+=1;digest.update(line)
            else:
                require(value["Type"]=="attempt","unknown attempt frame")
                o=attempt(value["Attempt"],end,spool);attempts+=1;commits+=int(o["Commit"]);aborts+=int(not o["Commit"]);digest.update(line)
        require(header and footer and attempts==end["Attempts"]==commits+aborts and commits==end["Committed"]==3*n+7 and aborts==end["Aborted"] and replays==n,"complete attempts/original replay census")
        fold(directory,end,spool)
        views(directory,end,spool)
        # Binary columns are independently checked by the family-specific
        # decoder below, never the legacy5/6 acceptance wrapper.
        columns(directory,end,spool)
        timing = timings(directory, end, spool)
        return {"spec":p,"runGraph":c["Graph"],"commits":commits,"aborts":aborts,"sourceVersions":11*n+7,"timing":timing,"scope":"one real family cell; matrix/native parity/transport/capacity remain separately qualified"}
