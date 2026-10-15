#!/usr/bin/env python3
"""Private, exclusively owned FDB ingress/IFB deployment and streamed evidence.

No import-time effects. A policy or command listing is never transport evidence.
All actual tc/ip/probe processes execute inside the existing five capped engine
containers. Packet limits do not establish a byte or kernel-memory/RSS bound.
"""
import base64
from decimal import Decimal
import hashlib
import importlib.util
import ipaddress
import json
import math
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import tempfile
import time

_monitor_spec=importlib.util.spec_from_file_location("fdb_owned_resource_monitor",Path(__file__).with_name("cost_monitor.py"))
cost_monitor=importlib.util.module_from_spec(_monitor_spec)
_monitor_spec.loader.exec_module(cost_monitor)

FORMAT = "fdb-owned-ingress-transport-v2"
MAX_SECONDS, MAX_SAMPLES, MAX_BYTES = 31_536_000, 31_536_002, 1 << 44
# A conservative declaration, not allocated storage/RSS or capacity evidence.
# Each sample funds five <=128KiB native readbacks, their command envelopes,
# one <=768KiB network frame and one <=64KiB aggregate process/cgroup frame.
FRAME, FIXED = 2 << 20, 16 << 20
SOURCE_SHA = "bbd141ef7b5d0127cc2152843ba61f274dc32814fa3e0f13e7d07a080bef53d9"
ROLES = ("zone0", "zone1", "zone2", "zone3", "client")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, low, high):
    require(type(value) is int and low <= value <= high, "finite integer policy")
    return value


def canonical(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def admitted(path, limit):
    with Path(path).open("rb") as source:
        body = source.read(limit + 1)
    require(len(body) <= limit, "bounded delivered artifact")
    return body


def sha_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        while chunk := source.read(64 << 10):
            digest.update(chunk)
    return digest.hexdigest()


def causal_minimum(spec, transport):
    n, writers = spec["facts"], spec["writers"]
    return (5 + 2 * ((n - 1 + writers - 1) // writers) + (n + 7) // 8) * transport["targetRTTMillis"]


def validate_input(value):
    require(type(value) is dict and set(value) == set("Version Spec Run Transport BudgetSeconds QueuePackets SampleMillis MaxSamples MaxArtifactBytes ToolsDirectory".split()), "explicit fresh transport input")
    require(value["Version"] == 2 and type(value["Version"]) is int, "fresh version2 required")
    spec, t = value["Spec"], value["Transport"]
    require(type(spec) is dict and set(spec) == set("version family facts crossPercent writers layout".split()), "exact feed spec")
    n = integer(spec["facts"], 20, 1_000_000)
    writers = integer(spec["writers"], 1, 32)
    require(spec["version"] == 2 and type(spec["version"]) is int and spec["family"] == "snapshot-to-feed" and n % 20 == 0 and writers in (1, 8, 32) and n >= writers + 1 and type(spec["crossPercent"]) is int and spec["crossPercent"] in (0, 10, 50, 100) and spec["layout"] in ("kv", "columns"), "actual fresh feed-only transport adapter")
    require(type(t) is dict and set(t) == set("kind targetRTTMillis jitterMillis seed".split()) and type(t["targetRTTMillis"]) is int and type(t["jitterMillis"]) is int and type(t["seed"]) is int and t["jitterMillis"] == 0 and t["seed"] == 20261009 and (t["kind"] == "local" and t["targetRTTMillis"] == 0 or t["kind"] == "injected-rtt" and t["targetRTTMillis"] in (1, 20, 100)), "actual local or fixed injected delay")
    run = value["Run"]
    require(type(run) is str and len(run) == 32 and bytes.fromhex(run).hex() == run and bytes.fromhex(run) != bytes(16), "same fresh run identity")
    seconds = integer(value["BudgetSeconds"], 1, MAX_SECONDS)
    require(seconds * 1000 > causal_minimum(spec, t), "budget cannot cover even necessary causal delay; not a performance guarantee")
    integer(value["QueuePackets"], 1, 4096)
    interval = integer(value["SampleMillis"], 1000, 60000)
    samples = integer(value["MaxSamples"], 1, MAX_SAMPLES)
    require(samples >= (seconds * 1000 + interval - 1) // interval + 2, "finite sample count funds original lifetime")
    grant = integer(value["MaxArtifactBytes"], FIXED, MAX_BYTES)
    require(samples <= (grant - FIXED) // FRAME, "finite streamed sample+fixed control reservation")
    require(type(value["ToolsDirectory"]) is str and value["ToolsDirectory"], "explicit authenticated read-only tool bundle")
    return value


def tools(directory):
    directory = Path(directory).resolve(strict=True)
    entries=[]
    for entry in directory.iterdir():
        require(len(entries)<5,"fixed private tooling inventory")
        entries.append(entry.name)
    require(set(entries)=={"tc","ip","manifest.json","build.txt","elf.txt"},"complete exact prebuilt tooling bundle")
    require((directory/"elf.txt").is_file() and not (directory/"elf.txt").is_symlink() and (directory/"elf.txt").stat().st_size<=1<<20,"finite static ELF observation")
    manifest = json.loads(admitted(directory / "manifest.json", 4096))
    require((directory/"build.txt").is_file() and not (directory/"build.txt").is_symlink() and (directory/"build.txt").stat().st_size<=1<<20,"finite immutable build provenance")
    require(type(manifest) is dict and set(manifest) == {"Version", "TC", "IP", "SourceSHA256", "BuildSHA256"} and manifest["Version"] == "iproute2-6.12.0" and manifest["SourceSHA256"] == SOURCE_SHA and sha_file(directory / "build.txt") == manifest["BuildSHA256"], "pinned tool/source/build manifest")
    for name, field in (("tc", "TC"), ("ip", "IP")):
        path = directory / name
        require(not path.is_symlink() and path.is_file() and path.stat().st_size <= 64 << 20 and os.access(path, os.X_OK), "finite executable tool file, no symlink")
        require(type(manifest[field]) is str and len(manifest[field]) == 64 and bytes.fromhex(manifest[field]).hex() == manifest[field] and sha_file(path) == manifest[field], "exact declared tool binary bytes")
    return directory, manifest


def environment_input():
    raw = os.environ.get("RHO_FDB_TRANSPORT_INPUT", "")
    require(len(raw) <= 8192, "finite input envelope")
    value = validate_input(json.loads(raw))
    require(value["Spec"] == json.loads(os.environ["RHO_GRAPH_WORKLOAD_SPEC"]) and value["Run"] == os.environ["RHO_GRAPH_WORKLOAD_RUN"] and os.environ.get("RHO_GRAPH_WORKLOAD_TRANSPORT") == FORMAT and os.environ.get("RHO_GRAPH_WORKLOAD_SECONDS") == str(value["BudgetSeconds"]), "request/launcher source binding")
    require(value["ToolsDirectory"] == "@pinned-prebuild", "repository-owned pinned prebuild required")
    return value


class Evidence:
    """One bounded streaming writer; no duration-sized lists or command map."""
    def __init__(self, directory, grant):
        self.directory, self.grant, self.used = Path(directory), grant, 0
        self.commands,self.command_hash=0,hashlib.sha256()
        self.ledger = (self.directory / "transport-commands.jsonl").open("xb")

    def bytes(self, body):
        require(len(body) <= self.grant - self.used, "transport artifact grant exhausted")
        self.used += len(body)

    def write(self, name, value, native=False):
        body = canonical(value) + (b"" if native else b"\n")
        self.bytes(body)
        with (self.directory / name).open("xb") as output:
            output.write(body)
        return hashlib.sha256(body).hexdigest()

    def record(self, value):
        body = canonical(value) + b"\n"
        require(len(body) <= 512 << 10, "finite command frame")
        self.bytes(body)
        self.ledger.write(body)
        self.ledger.flush()
        self.commands+=1;self.command_hash.update(body)

    def command(self, args, deadline, allowed=(0,), wall_limit=15):
        remaining = deadline - time.monotonic()
        require(remaining > 0, "original monotonic run deadline")
        started = time.monotonic_ns()
        # Output goes to a private finite temporary handle, never an unbounded
        # capture_output buffer. Fixed known commands may fail the output cap.
        with tempfile.TemporaryFile(dir=self.directory) as output:
            process = subprocess.Popen(args, stdout=output, stderr=subprocess.STDOUT, start_new_session=True, preexec_fn=output_limit)
            expired=False
            try:
                result = process.wait(timeout=min(wall_limit, remaining))
            except subprocess.TimeoutExpired:
                expired=True
                result=terminate_host(process)
            output.seek(0)
            body = output.read((128 << 10) + 1)
        require(len(body) <= 128 << 10, "actual command output cap")
        self.record({"Command": args, "Exit": result, "TimedOut":expired,"LimitSeconds":wall_limit,"StartNanos": started, "EndNanos": time.monotonic_ns(), "Output": base64.b64encode(body).decode()})
        require(not expired and result in allowed, "actual command failed/timeout; owned namespace cleanup required")
        return body

    def docker(self, args, deadline):
        return self.command(["docker", *args], deadline)

    def native(self, container, args, deadline):
        # A stopped Docker CLI alone cannot stop a target exec. The Linux
        # command has its own fixed deadline; any uncertainty fails the cell,
        # whose owning launcher must remove all five containers before reuse.
        probe=args[:2]==["/usr/local/bin/transport-probe","measure"]
        return self.command(["docker","exec", container, "timeout", "--signal=KILL", "28s" if probe else "10s", *args], deadline,wall_limit=30 if probe else 15)

    def close(self):
        self.ledger.close()


def inspect_peer(evidence, name, role, network, deadline):
    row = json.loads(evidence.docker(["inspect", name], deadline))[0]
    state, host = row["State"], row["HostConfig"]
    require(state["Running"] and not state["OOMKilled"] and state["Pid"] > 0 and not host["Privileged"] and host["NetworkMode"] != "host" and "NET_ADMIN" in (host["CapAdd"] or []), "owned capped live network namespace")
    networks = row["NetworkSettings"]["Networks"]
    require(set(networks) == {network}, "exact single owned network")
    linked = networks[network]
    ip = str(ipaddress.IPv4Address(linked["IPAddress"]))
    netns = evidence.native(name, ["readlink", "/proc/self/ns/net"], deadline).decode().strip()
    addresses = json.loads(evidence.native(name, ["/owned-tools/ip", "-j", "address", "show", "dev", "eth0"], deadline))
    require(len(addresses) == 1 and any(a["family"] == "inet" and a["local"] == ip for a in addresses[0]["addr_info"]), "actual peer IP/interface binding")
    return {"Role": role, "Container": row["Id"], "Image": row["Image"], "IP": ip, "NetNS": netns, "Network": linked["NetworkID"], "PID": state["Pid"]}


def directed(peers):
    require(len(peers) == 5 and tuple(p["Role"] for p in peers) == ROLES, "exact owned peer set")
    require(len({p["Container"] for p in peers}) == len({p["IP"] for p in peers}) == len({p["NetNS"] for p in peers}) == 5 and len({p["Network"] for p in peers}) == 1, "distinct actual namespaces/addresses, same owned network")
    return [(source, target) for target in range(5) for source in range(5) if source != target]


def shaping_commands(peers, target, packets, half_us):
    """Exact receiver ingress -> one IFB per source. No foreign replace/host IO."""
    require(half_us in (500, 10000, 50000), "explicit added one-way delay")
    local = peers[target]
    yield ["/owned-tools/tc", "qdisc", "add", "dev", "eth0", "clsact"]
    for source in range(5):
        if source == target:
            continue
        device, handle = f"rhoi{source}", f"{100 + source}:"
        yield ["/owned-tools/ip", "link", "add", device, "type", "ifb", "numtxqueues", "1", "numrxqueues", "1"]
        yield ["/owned-tools/ip", "link", "set", "dev", device, "up"]
        yield ["/owned-tools/tc", "qdisc", "add", "dev", device, "root", "handle", handle, "netem", "limit", str(packets), "delay", f"{half_us}us", "seed", "20261009"]
        yield ["/owned-tools/tc", "filter", "add", "dev", "eth0", "ingress", "protocol", "ip", "pref", str(100 + source), "flower", "skip_hw", "src_ip", peers[source]["IP"] + "/32", "dst_ip", local["IP"] + "/32", "action", "mirred", "egress", "redirect", "dev", device]


def readback(evidence, names, peers, network, deadline):
    out = []
    for name, peer in zip(names, peers, strict=True):
        # One bounded exec/peer. No re-enumeration proportional to run length.
        body = evidence.native(name, ["/bin/bash", "-ec", 'readlink /proc/self/ns/net; /owned-tools/tc -j -s qdisc show; /owned-tools/tc -j -s filter show dev eth0 ingress; /owned-tools/ip -j -s link show'], deadline)
        raw = body.splitlines()
        require(len(raw) == 4 and raw[0].decode() == peer["NetNS"], "same actual namespace and complete native readback")
        out.append({"Peer": peer, "Qdisc": json.loads(raw[1]), "Filters": json.loads(raw[2]), "Links": json.loads(raw[3]), "NativeSHA256":hashlib.sha256(body).hexdigest()})
    return out


def validate_readback(rows, peers, policy, armed):
    require(len(rows) == 5, "complete actual readback")
    injected = policy["Transport"]["kind"] == "injected-rtt" and armed
    drops = 0
    for target, row in enumerate(rows):
        require(row["Peer"] == peers[target], "same exact observed peer")
        qdiscs, filters = row["Qdisc"], row["Filters"]
        require(type(qdiscs) is list and type(filters) is list and len(qdiscs) <= 7 and len(row["Links"]) <= 6, "fixed native namespace inventory")
        expected = {f"rhoi{source}" for source in range(5) if source != target} if injected else set()
        qdisc_pairs={("noqueue","lo"),("noqueue","eth0")}|({("clsact","eth0")}|{("netem",dev) for dev in expected} if injected else set())
        require(len(qdiscs)==len(qdisc_pairs) and {(q["kind"],q["dev"]) for q in qdiscs}==qdisc_pairs,"exact nonduplicated root/hook qdisc inventory")
        netems = {q["dev"]: q for q in qdiscs if q["kind"] == "netem"}
        require(set(netems) == expected, "complete exact source-specific IFBs")
        for q in qdiscs:
            require(q["kind"] == "noqueue" and q["dev"] in ("lo", "eth0") or injected and (q["kind"] == "clsact" and q["dev"] == "eth0" or q["kind"] == "netem" and q["dev"] in expected), "no foreign qdisc adopted")
        require(sum(q["kind"] == "clsact" for q in qdiscs) == (1 if injected else 0), "actual receiver ingress hook")
        require({link["ifname"] for link in row["Links"]} == {"lo","eth0"}|expected, "complete exclusively owned link inventory")
        for device, q in netems.items():
            options = q["options"]
            require(q.get("root") is True and q["handle"]==str(100+int(device[4:]))+":","exact owned root netem handle, no child substitute")
            require(set(options) == {"limit", "delay", "seed", "ecn", "gap"} and options["limit"] == policy["QueuePackets"] and options["seed"] == 20261009 and options["ecn"] is False and options["gap"] == 0 and set(options["delay"]) == {"delay", "jitter"} and Decimal(str(options["delay"]["delay"])) == Decimal(policy["Transport"]["targetRTTMillis"]) / 2000 and options["delay"]["jitter"] == 0, "actual iproute2-v6.12 JSON seconds/packet limit, no hidden impairment")
            drops += integer(q["drops"], 0, (1 << 64) - 1)
            integer(q["backlog"], 0, (1 << 64) - 1)
        require(len(filters) == (4 if injected else 0), "exact ingress filters, no foreign rules")
        for source in range(5):
            if source == target or not injected:
                continue
            matches = [f for f in filters if f["kind"] == "flower" and f["pref"] == 100 + source]
            require(len(matches) == 1, "exact directed flower")
            options = matches[0]["options"]
            require(set(options["keys"]) == {"eth_type","src_ip","dst_ip"} and options["keys"]["eth_type"] == "ipv4" and options["keys"]["src_ip"] in (peers[source]["IP"], peers[source]["IP"] + "/32") and options["keys"]["dst_ip"] in (peers[target]["IP"], peers[target]["IP"] + "/32") and options.get("skip_hw") is True, "actual full host src/dst software filter; no narrowing transport/port match")
            actions = options["actions"]
            require(len(actions) == 1 and actions[0]["kind"] == "mirred" and actions[0]["to_dev"] == f"rhoi{source}" and actions[0]["mirred_action"] == "redirect" and actions[0]["direction"] == "egress", "actual one-level owned IFB redirection")
        for link in row["Links"]:
            if link["ifname"].startswith("rhoi"):
                require(link["ifname"] in expected, "foreign derived interface")
            if link["ifname"] != "lo":
                drops += integer(link["stats64"]["rx"]["dropped"], 0, (1 << 64) - 1)
                drops += integer(link["stats64"]["tx"]["dropped"], 0, (1 << 64) - 1)
    return drops


def output_limit():
    # Actual temporary output cannot grow until a later post-read rejection.
    resource.setrlimit(resource.RLIMIT_FSIZE, (128 << 10, 128 << 10))


def terminate_host(process):
    if process.poll() is None:
        try: os.killpg(process.pid,signal.SIGKILL)
        except ProcessLookupError: pass
    return process.wait(timeout=3)


def stream_line(output, value, evidence, cap=768 << 10):
    body = canonical(value) + b"\n"
    require(len(body) <= cap, "finite delivered control/sample frame")
    evidence.bytes(body)
    output.write(body)
    output.flush()
    return body


def array_hash(hexadecimal):
    require(type(hexadecimal) is str and len(hexadecimal) == 64 and bytes.fromhex(hexadecimal).hex() == hexadecimal and bytes.fromhex(hexadecimal) != bytes(32), "canonical nonzero SHA256")
    return list(bytes.fromhex(hexadecimal))


def validate_policy(p):
    require(type(p) is dict and list(p) == "Version Spec Run Transport BudgetSeconds CausalMinimumMillis AdmittedUnixNanos DeadlineUnixNanos QueuePackets SampleMillis MaxSamples MaxArtifactBytes ProbeSamples ProbePayloadBytes ProbeTimeoutMillis Tools Peers ArmedSHA256".split(), "exact canonical native transport policy")
    require(len(canonical(p))<=16<<10,"admitted native policy bytes")
    require(type(p["Run"]) is list and len(p["Run"])==16 and all(type(n) is int and 0<=n<=255 for n in p["Run"]),"native unsigned run bytes")
    run = bytes(p["Run"])
    validate_input({"Version": p["Version"], "Spec": p["Spec"], "Run": run.hex(), "Transport": p["Transport"], **{k: p[k] for k in ("BudgetSeconds", "QueuePackets", "SampleMillis", "MaxSamples", "MaxArtifactBytes")}, "ToolsDirectory": "@pinned-prebuild"})
    require(len(run) == 16 and integer(p["CausalMinimumMillis"],0,(1<<64)-1) == causal_minimum(p["Spec"], p["Transport"]) and integer(p["AdmittedUnixNanos"], 1, (1 << 63)-1) + p["BudgetSeconds"]*1_000_000_000 == integer(p["DeadlineUnixNanos"], 1, (1 << 63)-1), "same original absolute finite deadline")
    require((p["ProbeSamples"], p["ProbePayloadBytes"], p["ProbeTimeoutMillis"]) == (9, 64, 3000) and set(p["Tools"]) == {"Version", "TC", "IP", "Probe", "Source", "Build"} and p["Tools"]["Version"] == "iproute2-6.12.0" and bytes(p["Tools"]["Source"]).hex() == SOURCE_SHA, "exact source/tool/probe policy")
    for digest in [p["ArmedSHA256"], *(p["Tools"][k] for k in ("TC", "IP", "Probe", "Source", "Build"))]:
        require(type(digest) is list and len(digest) == 32 and all(type(n) is int and 0 <= n <= 255 for n in digest) and bytes(digest) != bytes(32), "actual digest bytes")
    directed(p["Peers"])
    for peer in p["Peers"]:
        require(set(peer) == set("Role Container Image IP NetNS Network PID".split()) and str(ipaddress.IPv4Address(peer["IP"])) == peer["IP"] and 7<=len(peer["NetNS"])<=64 and peer["NetNS"].startswith("net:[") and peer["NetNS"].endswith("]") and peer["NetNS"][5:-1].isascii() and peer["NetNS"][5:-1].isdecimal() and int(peer["NetNS"][5:-1])<1<<64, "actual peer namespace binding")
        integer(peer["PID"], 1, (1 << 64)-1)
        array_hash(peer["Container"]); array_hash(peer["Network"])
        require(peer["Image"].startswith("sha256:"), "actual image content identity")
        array_hash(peer["Image"][7:])
    return p


def probes(e, names, peers, token, deadline, phase):
    samples = []  # Fixed twenty directed pairs x nine samples, never dataset-sized.
    for source, target in directed(peers):
        body = e.native(names[source], ["/usr/local/bin/transport-probe", "measure", peers[source]["IP"], peers[target]["IP"], token, str(deadline[1])], deadline[0])
        rows = [json.loads(line) for line in body.splitlines()]
        require(len(rows) == 9, "complete actual probe samples")
        for ordinal, row in enumerate(rows):
            require(row == {"Source": peers[source]["IP"], "Target": peers[target]["IP"], "TokenSHA256": hashlib.sha256(token.encode()).hexdigest(), "Ordinal": ordinal, "Nanos": integer(row["Nanos"], 1, 3_000_000_000), "Bytes": 64}, "actual directed TCP connect+echo sample; base/queue latency retained")
        samples.append({"Source": source, "Target": target, "Samples": rows})
    e.write("transport-probes-"+phase+".json", samples)


def launch(value, directory, tag, image, connection, client_ip, client_mib):
    """Own only this launcher's five namespaces; no host tc, SDK sleep or proxy."""
    directory = Path(directory).resolve(strict=True)
    tool_dir, tool = tools(directory / "owned-tools")
    probe = directory / "transport-probe"
    require(probe.is_file() and not probe.is_symlink() and probe.stat().st_size<=64<<20, "actual finite pinned prebuilt static probe")
    admitted_at = time.time_ns()
    absolute = admitted_at + value["BudgetSeconds"] * 1_000_000_000
    require(absolute <= (1 << 63)-1, "checked UnixNano lifetime")
    deadline = time.monotonic() + value["BudgetSeconds"]
    names = [tag+"-"+role for role in ROLES[:-1]]+[tag+"-workload"]
    # Controller owns 3/4 of the declared output grant; monitor owns 1/4. No
    # phase reset or silent growth; both debit bytes BEFORE streaming a frame.
    monitor_grant = value["MaxArtifactBytes"] // 4
    e = Evidence(directory, value["MaxArtifactBytes"] - monitor_grant)
    helpers, monitor, created = [], None, []
    completed = False
    try:
        spec = canonical(value["Spec"]).decode()
        command = ["run", "-d", "--name", names[-1], "--network", tag, "--ip", client_ip, "--cap-add", "NET_ADMIN", "--memory", str(integer(int(client_mib), 1, 32768))+"m", "--cpus", "0.5", "--mount", f"type=bind,source={directory},target=/evidence", "--mount", f"type=bind,source={tool_dir},target=/owned-tools,readonly"]
        for key, content in {"FDB_CONNECTION_STRING": connection, "RHO_GRAPH_WORKLOAD_SPEC": spec, "RHO_GRAPH_WORKLOAD_RUN": value["Run"], "RHO_GRAPH_WORKLOAD_TRANSPORT": FORMAT, "RHO_GRAPH_WORKLOAD_SECONDS": str(value["BudgetSeconds"]), "RHO_GRAPH_WORKLOAD_DEADLINE": str(absolute), "FDB_COST_ARTIFACT": "/evidence"}.items():
            command += ["--env", key+"="+content]
        # Driver cannot mutate before genuine native namespace readback and
        # probes. The absolute deadline is set BEFORE this container is released.
        command += [image, "/bin/bash", "-c", 'while [ ! -f /evidence/transport-admission.json ]; do sleep 0.05; done; fdb-spike workload-cell > /evidence/workload-result.stdout 2>/evidence/workload-result.stderr; result=$?; printf "%s\\n" "$result" > /evidence/transport-driver-exit; while [ ! -f /evidence/transport-release ]; do sleep 0.05; done; exit "$result"']
        e.docker(command, deadline)
        peers = [inspect_peer(e, name, role, tag, deadline) for name, role in zip(names, ROLES, strict=True)]
        directed(peers)
        for name in names:
            body=e.native(name,["sha256sum","/owned-tools/tc","/owned-tools/ip","/usr/local/bin/transport-probe"],deadline)
            expected="".join(f"{digest}  {path}\n" for digest,path in ((tool["TC"],"/owned-tools/tc"),(tool["IP"],"/owned-tools/ip"),(sha_file(probe),"/usr/local/bin/transport-probe"))).encode()
            require(body==expected,"actual mounted/image executable identities before shaping/workload")
        e.write("transport-input.json", value)
        # Policy order is identical to the Go typed wire; it is persisted later
        # by an actual SDK write and post-commit observation, never a label only.
        p = {"Version": 2, "Spec": value["Spec"], "Run": list(bytes.fromhex(value["Run"])), "Transport": value["Transport"], "BudgetSeconds": value["BudgetSeconds"], "CausalMinimumMillis": causal_minimum(value["Spec"], value["Transport"]), "AdmittedUnixNanos": admitted_at, "DeadlineUnixNanos": absolute, **{k: value[k] for k in ("QueuePackets", "SampleMillis", "MaxSamples", "MaxArtifactBytes")}, "ProbeSamples": 9, "ProbePayloadBytes": 64, "ProbeTimeoutMillis": 3000, "Tools": {"Version": tool["Version"], "TC": array_hash(tool["TC"]), "IP": array_hash(tool["IP"]), "Probe": array_hash(sha_file(probe)), "Source": array_hash(tool["SourceSHA256"]), "Build": array_hash(tool["BuildSHA256"])}, "Peers": peers, "ArmedSHA256": [1]*32}
        # Monitor starts before any probe/shaping helper so their process-tree
        # RSS is included; cgroup peak/stat additionally capture unsampled work.
        monitor_args = [sys.executable, "-B", str(Path(__file__).with_name("cost_monitor.py")), "--servers", *names[:4], "--client", names[-1], "--output", str(directory/"transport-rss.jsonl"), "--seconds", str(p["BudgetSeconds"]), "--absolute-deadline", str(absolute), "--sample-millis", str(p["SampleMillis"]), "--max-samples", str(p["MaxSamples"]), "--max-bytes", str(monitor_grant),"--terminal-marker",str(directory/"transport-release")]
        monitor_log = (directory/"transport-monitor.log").open("xb")
        monitor = subprocess.Popen(monitor_args, stdout=monitor_log, stderr=subprocess.STDOUT, start_new_session=True)
        token = hashlib.sha256(bytes.fromhex(value["Run"])+b"owned-tcp-probe-v2").hexdigest()
        for name, peer in zip(names, peers, strict=True):
            log = (directory/("transport-helper-"+peer["Role"]+".jsonl")).open("xb+")
            proc = subprocess.Popen(["docker", "exec", name, "/usr/local/bin/transport-probe", "serve", peer["IP"], peer["IP"], token, str(absolute)], stdout=log, stderr=subprocess.STDOUT, start_new_session=True, preexec_fn=output_limit)
            helpers.append((name, proc, log, peer))
            while log.seek(0) == 0:
                line = log.readline(4097)
                if line.endswith(b"\n"):
                    ready = json.loads(line)
                    require(ready["Ready"] == "owned-tcp-echo" and ready["Address"] == peer["IP"]+":4501" and integer(ready["PID"],1,(1<<31)-1), "actual bounded helper readiness/PID")
                    break
                require(proc.poll() is None and time.monotonic() < deadline, "probe helper failed before readiness")
                time.sleep(0.01)
        baseline = readback(e,names,peers,tag,deadline)
        require(validate_readback(baseline,peers,p,False)==0,"baseline has foreign/drop state")
        e.write("transport-baseline.json", baseline)
        probes(e,names,peers,token,(deadline,absolute),"before")
        if p["Transport"]["kind"] == "injected-rtt":
            # Do not let first IFB use autoload a module whose default devices
            # could be created in the host initial namespace. This adapter only
            # uses an already available kernel IFB implementation; no modprobe.
            for name in names: e.native(name,["test","-d","/sys/module/ifb"],deadline)
            for target,name in enumerate(names):
                for args in shaping_commands(peers,target,p["QueuePackets"],p["Transport"]["targetRTTMillis"]*500):
                    e.native(name,args,deadline)
                    if args[1:3] == ["link","add"]: created.append((name,args[3]))
                    if args[1:3] == ["qdisc","add"] and args[-1] == "clsact": created.append((name,"clsact"))
        armed = readback(e,names,peers,tag,deadline)
        require(validate_readback(armed,peers,p,True)==0,"drops before workload release")
        p["ArmedSHA256"] = array_hash(e.write("transport-armed.json",armed))
        probes(e,names,peers,token,(deadline,absolute),"armed")
        validate_policy(p)
        # Atomic publication gates PID1. No partial file is a valid admission.
        body = canonical(p); e.bytes(body)
        with (directory/"transport-admission.pending").open("xb") as out: out.write(body)
        os.link(directory/"transport-admission.pending",directory/"transport-admission.json")
        (directory/"transport-admission.pending").unlink()
        count = 0;network_hash=hashlib.sha256()
        with (directory/"transport-samples.jsonl").open("xb") as output:
            while True:
                require(count < p["MaxSamples"] and time.monotonic() < deadline,"original sample/run budget exhausted")
                rows = readback(e,names,peers,tag,deadline)
                drops = validate_readback(rows,peers,p,True)
                require(drops == 0,"actual packet drops invalidate delay-only comparison")
                network_hash.update(stream_line(output,{"Ordinal":count,"AtUnixNanos":time.time_ns(),"Rows":rows,"Drops":drops},e))
                count += 1
                marker = directory/"transport-driver-exit"
                if marker.exists():
                    require(admitted(marker,16)==b"0\n","actual workload failed; no transport EOF")
                    break
                time.sleep(min(p["SampleMillis"]/1000,max(0,deadline-time.monotonic())))
            probes(e,names,peers,token,(deadline,absolute),"after")
            terminal = readback(e,names,peers,tag,deadline)
            require(validate_readback(terminal,peers,p,True)==0,"actual terminal drops/foreign shaping")
            e.write("transport-terminal.json",terminal)
            network_hash.update(stream_line(output,{"Type":"footer","Samples":count,"DeadlineUnixNanos":absolute},e))
        stopped = stop_helpers(e,helpers,deadline)
        for name,device in reversed(created):
            e.native(name,["/owned-tools/tc","qdisc","del","dev","eth0","clsact"] if device=="clsact" else ["/owned-tools/ip","link","del","dev",device],deadline)
        created.clear()
        restored = readback(e,names,peers,tag,deadline)
        require(validate_readback(restored,peers,p,False)==0,"owned qdisc/interface cleanup not exact")
        e.write("transport-restored.json",restored)
        actual = [inspect_peer(e,n,r,tag,deadline) for n,r in zip(names,ROLES,strict=True)]
        require(actual==peers,"actual incarnation changed during run")
        e.write("transport-release",{"Release":True})
        require(e.docker(["wait",names[-1]],deadline)==b"0\n","actual released client exit")
        require(monitor.wait(timeout=min(40,max(0.1,deadline-time.monotonic())))==0,"actual aggregate monitor terminal within SAME original deadline")
        monitor_log.close()
        state=json.loads(e.docker(["inspect",names[-1]],deadline))[0]["State"]
        require(not state["Running"] and state["ExitCode"]==0 and not state["OOMKilled"],"actual client terminal")
        e.write("transport-complete.json",{"Version":2,"PolicySHA256":hashlib.sha256(body).hexdigest(),"Samples":count,"NetworkSHA256":network_hash.hexdigest(),"Commands":e.commands,"CommandSHA256":e.command_hash.hexdigest(),"Helpers":stopped,"MonitorExit":0,"DriverExit":0,"Restored":True,"UsedControllerBytes":e.used,"MonitorGrant":monitor_grant,"DeadlineUnixNanos":absolute,"HostUsage":host_usage(),"Scope":"one-host actual configured ingress delay; TCP connect+echo distributions include native baseline/queues; packet cap is not byte/RSS cap"})
        completed=True
    finally:
        # Failure never emits completion. Known created devices remain owned by
        # these exclusively allocated namespaces; launcher removal is mandatory.
        cleanup_deadline=time.monotonic()+30
        if helpers:
            try: stop_helpers(e,helpers,cleanup_deadline)
            except (ValueError,OSError,subprocess.SubprocessError): pass
        for _,proc,log,_ in helpers:
            terminate_host(proc)
            log.close()
        if monitor is not None and monitor.poll() is None:
            terminate_host(monitor)
        if monitor is not None: monitor_log.close()
        if not completed:
            for name,device in reversed(created):
                try: e.native(name,["/owned-tools/tc","qdisc","del","dev","eth0","clsact"] if device=="clsact" else ["/owned-tools/ip","link","del","dev",device],cleanup_deadline)
                except (ValueError,OSError,subprocess.SubprocessError): pass
        e.close()


def host_usage():
    def usage(who):
        u=resource.getrusage(who)
        return {"UserSeconds":u.ru_utime,"SystemSeconds":u.ru_stime,"MaxRSS":u.ru_maxrss}
    return {"Platform":sys.platform,"Self":usage(resource.RUSAGE_SELF),"ReapedChildren":usage(resource.RUSAGE_CHILDREN),"RSSUnits":"bytes" if sys.platform=="darwin" else "KiB","Scope":"reaped-child CPU cumulative; child MaxRSS is largest child, not simultaneous aggregate; engine/helper aggregate sampled RSS and cgroup peaks reported separately"}


def stop_helpers(e, helpers, deadline):
    rows=[]
    for name,proc,log,peer in helpers:
        log.seek(0); ready=json.loads(log.readline(4097))
        pid=integer(ready["PID"],1,(1<<31)-1)
        # Exact live helper command includes this run's unique token and PID.
        # SIGTERM is tooling lifecycle only, never graph decision authority.
        expected=" ".join(proc.args[3:])+" "
        e.native(name,["/bin/bash","-ec",'cmd=$(tr "\\0" " " < "/proc/$1/cmdline"); [ "$cmd" = "$2" ]; kill -TERM "$1"',"--",str(pid),expected],deadline)
        result=proc.wait(timeout=5)
        require(result==0,"probe helper exit/reap")
        log.close()
        rows.append({"Container":peer["Container"],"PID":pid,"HostPID":proc.pid,"Exit":result,"Reaped":True})
    helpers.clear()
    return rows


def frames(path, maximum, cap):
    with Path(path).open("rb") as source:
        count=0
        while line:=source.readline(cap+1):
            require(len(line)<=cap and line.endswith(b"\n") and count<maximum,"bounded complete transport frame/EOF")
            count+=1
            yield json.loads(line),line


def verify(directory,p,spool):
    """Join actual raw kernel/command/probe/resource EOFs using caller's spool.

    No packet schedule is inferred from desired policy. Samples establish
    observed states/distributions, not every-packet latency or machine capacity.
    """
    validate_policy(p)
    directory=Path(directory)
    require(admitted(directory/"transport-admission.json",16<<10)==canonical(p),"same canonical admission artifact/native SDK binding")
    value=json.loads(admitted(directory/"transport-input.json",8192))
    validate_input(value)
    require(value["Spec"]==p["Spec"] and bytes(p["Run"]).hex()==value["Run"] and all(p[k]==value[k] for k in ("Transport","BudgetSeconds","QueuePackets","SampleMillis","MaxSamples","MaxArtifactBytes")),"same requested lifetime/grants/transport")
    tool_dir,tool=tools(directory/"owned-tools")
    require(all(bytes(p["Tools"][field]).hex()==tool[key] for field,key in (("TC","TC"),("IP","IP"),("Source","SourceSHA256"),("Build","BuildSHA256"))) and sha_file(directory/"transport-probe")==bytes(p["Tools"]["Probe"]).hex(),"actual immutable tooling bytes/provenance")
    used=0;commands=0;command_hash=hashlib.sha256();observed_peers=set();aliases={};mutations=[];actual_tools=set();memory_limits={}
    # Max command count is a finite checked consequence of fixed setup/probes
    # plus five exec readbacks per admitted sample, not an unbounded input loop.
    for row,line in frames(directory/"transport-commands.jsonl",2048+5*p["MaxSamples"],512<<10):
        require(set(row)=={"Command","Exit","TimedOut","LimitSeconds","StartNanos","EndNanos","Output"} and integer(row["Exit"],0,0)==0 and row["TimedOut"] is False and integer(row["LimitSeconds"],1,30) and integer(row["StartNanos"],1,(1<<64)-1)<=integer(row["EndNanos"],1,(1<<64)-1),"actual terminal command")
        args=row["Command"]
        require(type(args) is list and 2<=len(args)<=80 and args[0]=="docker" and args[1] in ("run","inspect","exec","wait"),"owned Docker commands only")
        body=base64.b64decode(row["Output"],validate=True)
        require(len(body)<=128<<10,"actual bounded command output")
        if args[1]=="inspect":
            actual=json.loads(body)
            require(type(actual) is list and len(actual)==1,"actual single peer inspection")
            native=actual[0]
            matches=[peer for peer in p["Peers"] if peer["Container"]==native["Id"]]
            require(len(matches)==1,"foreign actual container inspection")
            peer=matches[0];state=native["State"];host=native["HostConfig"]
            alias=native["Name"].removeprefix("/")
            require(alias==args[2] and (alias not in aliases or aliases[alias]==p["Peers"].index(peer)),"same owned alias/incarnation")
            aliases[alias]=p["Peers"].index(peer)
            require(native["Image"]==peer["Image"] and not state["OOMKilled"] and not host["Privileged"] and host["NetworkMode"]!="host" and "NET_ADMIN" in host["CapAdd"] and host["Memory"]>0 and host["NanoCpus"]>0,"actual capped non-host owned incarnation")
            limit=integer(host["Memory"],1,(1<<63)-1)
            require(peer["Container"] not in memory_limits or memory_limits[peer["Container"]]==limit,"unchanged actual container memory cap")
            memory_limits[peer["Container"]]=limit
            networks=list(native["NetworkSettings"]["Networks"].values())
            require(len(networks)==1 and networks[0]["NetworkID"]==peer["Network"] and networks[0]["IPAddress"] in ((peer["IP"],) if state["Running"] else (peer["IP"],"")),"actual directed native network/address")
            if state["Running"]:
                require(state["Pid"]==peer["PID"],"actual live PID identity")
                observed_peers.add(peer["Container"])
            else:
                require(peer["Role"]=="client" and state["ExitCode"]==0,"actual sole terminal client identity")
        elif args[1]=="exec":
            require(args[2] in aliases and args[3:5]==["timeout","--signal=KILL"] and args[5] in ("10s","28s"),"owned bounded native helper invocation")
            native_args=args[6:]
            if native_args[:2] in (["/owned-tools/tc","qdisc"],["/owned-tools/tc","filter"],["/owned-tools/ip","link"]):
                mutations.append((aliases[args[2]],native_args))
                require(len(mutations)<=110,"fixed shaping/cleanup command inventory")
            if native_args==["sha256sum","/owned-tools/tc","/owned-tools/ip","/usr/local/bin/transport-probe"]:
                expected="".join(f"{bytes(p['Tools'][k]).hex()}  {path}\n" for k,path in (("TC","/owned-tools/tc"),("IP","/owned-tools/ip"),("Probe","/usr/local/bin/transport-probe"))).encode()
                require(body==expected,"actual five-namespace executable SHA256")
                actual_tools.add(aliases[args[2]])
        elif args[1]=="wait":
            require(args[2] in aliases and aliases[args[2]]==4 and body==b"0\n","actual terminal client wait authority")
        digest=hashlib.sha256(body).digest()
        if spool.find(b"transport-command-output",digest) is None: spool.insert(b"transport-command-output",digest,body)
        used+=len(line);commands+=1;command_hash.update(line)
    require(commands>=100 and actual_tools==set(range(5)) and observed_peers=={peer["Container"] for peer in p["Peers"]},"missing actual setup/probe/tool/native incarnation inventory")
    expected_mutations=[];created=[]
    if p["Transport"]["kind"]=="injected-rtt":
        for target in range(5):
            for args in shaping_commands(p["Peers"],target,p["QueuePackets"],p["Transport"]["targetRTTMillis"]*500):
                expected_mutations.append((target,args))
                if args[1:3]==["link","add"]: created.append((target,args[3]))
                if args[1:3]==["qdisc","add"] and args[-1]=="clsact": created.append((target,"clsact"))
        for target,device in reversed(created):
            expected_mutations.append((target,["/owned-tools/tc","qdisc","del","dev","eth0","clsact"] if device=="clsact" else ["/owned-tools/ip","link","del","dev",device]))
    require(mutations==expected_mutations,"complete exact owned shaping/deletion command order, no foreign replacement/rearming")
    def readback_file(name,armed):
        nonlocal used
        body=admitted(directory/name,768<<10);used+=len(body)
        rows=json.loads(body)
        join_rows(rows)
        require(validate_readback(rows,p["Peers"],p,armed)==0,"actual drops/foreign shaping")
        return body
    def join_rows(rows):
        require(type(rows) is list and len(rows)==5,"actual full native readback")
        for row in rows:
            require(set(row)=={"Peer","Qdisc","Filters","Links","NativeSHA256"},"native readback grammar")
            body=spool.lookup(b"transport-command-output",bytes.fromhex(row["NativeSHA256"]))
            lines=body.splitlines()
            require(len(lines)==4 and lines[0].decode()==row["Peer"]["NetNS"] and [json.loads(b) for b in lines[1:]]==[row["Qdisc"],row["Filters"],row["Links"]],"exact raw kernel command/source artifact join")
    readback_file("transport-baseline.json",False)
    require(hashlib.sha256(readback_file("transport-armed.json",True)).digest()==bytes(p["ArmedSHA256"]),"native persisted policy binds exact armed readback")
    readback_file("transport-terminal.json",True)
    readback_file("transport-restored.json",False)
    count=0;footer=False;previous=p["AdmittedUnixNanos"];network_hash=hashlib.sha256()
    for row,line in frames(directory/"transport-samples.jsonl",p["MaxSamples"]+1,768<<10):
        used+=len(line)
        network_hash.update(line)
        require(not footer,"network sample after EOF")
        if row.get("Type")=="footer":
            require(row=={"Type":"footer","Samples":count,"DeadlineUnixNanos":p["DeadlineUnixNanos"]} and count>0,"actual network sample EOF")
            footer=True;continue
        require(set(row)=={"Ordinal","AtUnixNanos","Rows","Drops"} and integer(row["Ordinal"],0,p["MaxSamples"])==count and previous<=integer(row["AtUnixNanos"],p["AdmittedUnixNanos"],p["DeadlineUnixNanos"]) and integer(row["Drops"],0,0)==0,"ordered admitted actual network samples")
        previous=row["AtUnixNanos"];join_rows(row["Rows"])
        require(validate_readback(row["Rows"],p["Peers"],p,True)==0,"actual sample qdisc/drop authority")
        count+=1
    require(footer,"missing network sample EOF")
    token=hashlib.sha256(bytes(p["Run"])+b"owned-tcp-probe-v2").hexdigest()
    distributions={}
    for phase in ("before","armed","after"):
        body=admitted(directory/("transport-probes-"+phase+".json"),128<<10);used+=len(body)
        pairs=json.loads(body)
        require(type(pairs) is list and len(pairs)==20,"all directed actual TCP probes")
        summary=[]
        for pair,(source,target) in zip(pairs,directed(p["Peers"]),strict=True):
            require(set(pair)=={"Source","Target","Samples"} and pair["Source"]==source and pair["Target"]==target and len(pair["Samples"])==9,"exact directed probe identities")
            native=b"".join(canonical(s)+b"\n" for s in pair["Samples"])
            spool.lookup(b"transport-command-output",hashlib.sha256(native).digest())
            values=[]
            for i,s in enumerate(pair["Samples"]):
                require(s=={"Source":p["Peers"][source]["IP"],"Target":p["Peers"][target]["IP"],"TokenSHA256":hashlib.sha256(token.encode()).hexdigest(),"Ordinal":i,"Nanos":integer(s["Nanos"],1,3_000_000_000),"Bytes":64},"exact raw probe bytes and real duration")
                values.append(s["Nanos"])
            values.sort() # Always exactly nine, never dataset/run-sized.
            summary.append({"source":source,"target":target,"min":values[0],"p50":values[4],"p95":values[8],"p99":values[8],"max":values[-1]})
        distributions[phase]=summary
    last=None;rss_count=0;rss_footer=False;rss_hash=hashlib.sha256();rss_bytes=0;previous_end=0;peaks=[0]*5;faults=[{} for _ in range(5)];sampled_peak=0;live_client=False;host_cpu={}
    ordered_names=[next(name for name,index in aliases.items() if index==i) for i in range(5)]
    for row,line in frames(directory/"transport-rss.jsonl",p["MaxSamples"]+1,64<<10):
        used+=len(line)
        require(not rss_footer,"resource frame after EOF")
        if row.get("type")=="footer":
            require(row=={"type":"footer","samples":rss_count,"payloadBytes":rss_bytes,"payloadSHA256":rss_hash.hexdigest(),"deadlineUnixNanos":p["DeadlineUnixNanos"]},"complete actual resource EOF/hash")
            rss_footer=True;continue
        rss_bytes+=len(line);rss_hash.update(line)
        require(integer(row["ordinal"],0,p["MaxSamples"])==rss_count and row["deadlineUnixNanos"]==p["DeadlineUnixNanos"] and len(row["containers"])==len(row["cgroups"])==5,"complete actual aggregate/helper resource sample")
        starts=[];ends=[]
        for i,(processes,group) in enumerate(zip(row["containers"],row["cgroups"],strict=True)):
            require(processes["container"]==group["container"]==ordered_names[i],"actual ordered same five resource identities")
            for observation in (processes,group):
                start=integer(observation["startNanos"],previous_end,(1<<64)-1);end=integer(observation["endNanos"],start,(1<<64)-1)
                starts.append(start);ends.append(end)
            if processes["state"]=="live":
                require(cost_monitor.process_rows(processes["rawTop"])==processes["processes"] and any(proc["pid"]==p["Peers"][i]["PID"] for proc in processes["processes"]),"raw actual process census includes same original container root PID")
                if i==4: live_client=True
            else:
                require(i==4 and processes["state"]=="stopped" and processes["processes"]==[] and not processes["inspection"]["Running"] and not processes["inspection"]["OOMKilled"] and processes["inspection"]["ExitCode"]==0,"only genuine zero-exit client may stop")
            if group["state"]=="live":
                current,peak,stats=cost_monitor.parse_cgroup_raw(group["raw"])
                require(peak>=peaks[i] and peak<=memory_limits[p["Peers"][i]["Container"]],"same actual cumulative cgroup peak/cap; source cap not RSS inference")
                for name in ("pgfault","pgmajfault"):
                    if name in stats:
                        require(stats[name]>=faults[i].get(name,0),"same live cgroup cumulative fault counter")
                        faults[i][name]=stats[name]
                peaks[i]=peak
            else:
                require(i==4 and group["state"]=="stopped" and not group["inspection"]["Running"] and not group["inspection"]["OOMKilled"] and group["inspection"]["ExitCode"]==0,"actual terminal cgroup distinction")
        require(row["samplingSkewNanos"]==max(ends)-min(starts),"actual whole sample IO skew includes raw cgroup observations")
        previous_end=max(ends)
        usage=row["hostController"]
        require(usage["platform"] in ("darwin","linux") and usage["rssUnits"]==("bytes" if usage["platform"]=="darwin" else "KiB"),"actual host CPU/RSS platform units")
        for who in ("self","reapedChildren"):
            integer(usage[who]["maxRSS"],0,(1<<64)-1)
            for metric in ("userSeconds","systemSeconds"):
                value=usage[who][metric]
                require(type(value) in (int,float) and math.isfinite(value) and value>=host_cpu.get((who,metric),0),"actual cumulative host CPU; child maxRSS is not aggregate peak")
                host_cpu[who,metric]=value
        pids=[proc["pid"] for c in row["containers"] for proc in c["processes"]]
        require(len(set(pids))==len(pids) and row["rssKiB"]==sum(proc["rssKiB"] for c in row["containers"] for proc in c["processes"]),"same-PID nonduplicated actual aggregate RSS")
        sampled_peak=max(sampled_peak,row["rssKiB"])
        require(row["sampledPeakRSSKiB"]==sampled_peak,"actual cumulative sampled RSS peak")
        require(row["elapsedSeconds"]>=0 and (last is None or row["elapsedSeconds"]>=last["elapsedSeconds"]),"actual resource timeline")
        last=row;rss_count+=1
    require(rss_footer and live_client and all(peaks) and last is not None and last["containers"][-1]["state"]=="stopped" and last["containers"][-1]["inspection"]["ExitCode"]==0,"resource stream ends only on genuine client terminal; preceding live workload/helper cgroup peaks required")
    complete=json.loads(admitted(directory/"transport-complete.json",64<<10))
    require(complete["Version"]==2 and complete["PolicySHA256"]==hashlib.sha256(canonical(p)).hexdigest() and complete["Commands"]==commands and complete["CommandSHA256"]==command_hash.hexdigest() and complete["Samples"]==count and complete["MonitorExit"]==complete["DriverExit"]==0 and complete["Restored"] is True and complete["DeadlineUnixNanos"]==p["DeadlineUnixNanos"] and complete["MonitorGrant"]==p["MaxArtifactBytes"]//4 and len(complete["Helpers"])==5,"complete same-lifetime controller/helper/reap receipt")
    require(complete["NetworkSHA256"]==network_hash.hexdigest(),"actual network stream EOF/hash")
    for helper,peer in zip(complete["Helpers"],p["Peers"],strict=True):
        require(helper["Container"]==peer["Container"] and helper["Exit"]==0 and helper["Reaped"] is True,"genuine helper terminal identity")
        integer(helper["PID"],1,(1<<31)-1);integer(helper["HostPID"],1,(1<<64)-1)
        ready=json.loads(admitted(directory/("transport-helper-"+peer["Role"]+".jsonl"),4096))
        require(ready=={"Ready":"owned-tcp-echo","PID":helper["PID"],"Address":peer["IP"]+":4501"},"original helper readiness/PID joins terminal reap")
    require(used<=p["MaxArtifactBytes"],"actual combined evidence grant")
    assets=sum((tool_dir/name).stat().st_size for name in ("tc","ip","build.txt","manifest.json","elf.txt"))+(directory/"transport-probe").stat().st_size
    return {"policySHA256":complete["PolicySHA256"],"networkSamples":count,"rssSamples":rss_count,"observedTCPConnectEchoNanos":distributions,"configuredAddedOneWayMicros":p["Transport"]["targetRTTMillis"]*500,"drops":0,"sampledAggregateRSSKiB":sampled_peak,"sameContainerCgroupPeakBytes":peaks,"toolingArtifactFileBytes":assets,"scope":"actual inter-namespace receiver ingress, one host; intra-namespace/co-resident traffic and native baseline unchanged; process/cgroup/tooling counters distinct, no equal-RTT/every-packet or capacity promise"}


def main():
    mode,*args=sys.argv[1:]
    value=environment_input()
    if mode=="preflight": return
    require(mode=="launch" and len(args)==6,"actual owned launch arguments required")
    launch(value,*args)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as error:
        raise SystemExit("owned transport refused: " + str(error)) from error
