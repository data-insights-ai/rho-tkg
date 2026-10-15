#!/usr/bin/env python3
"""Join existing complete SDK fault proofs to simultaneous owned role loss.

This is a finite qualification harness, not a database reducer or authority.
Source-held and derived-column-held fixtures are separate native namespaces.
No progress/minority-availability assertion is borrowed from this schedule:
the unchanged platform schedule owns those inside-outage observations.
"""
import hashlib
import json
from pathlib import Path
import sys
import time

import launcher
import platform_fault_controller as platform
import column_publication_controller as columns
import history_retirement_controller as history

FORMAT = "fdb-composed-platform-faults-v1"
FAULTS = ("transaction", "log", "storage", "one-zone", "two-zones", "network")
SEAMS = ("chunk", "source-eof", "manifest-issued", "owner", "gc")
CASE_SECONDS, TOTAL_SECONDS = 600, 36000
require, load = platform.require, platform.load


def shape(kind, seam, fault, coordinator):
    require(type(coordinator) is str and coordinator in ("A", "B") and
            fault in FAULTS and seam in SEAMS and
            kind == ("source" if seam in ("owner", "gc") else "column"),
            "exact approved fixture/seam/domain/orientation")
    return {"Kind": kind, "Seam": seam, "Fault": fault, "Coordinator": coordinator}


def cases():
    return [shape("source" if seam in ("owner", "gc") else "column", seam, fault, coordinator)
            for fault in FAULTS for seam in SEAMS for coordinator in ("A", "B")]


def name(case):
    return "composed-{Kind}-{Fault}-{Seam}-{Coordinator}".format(**case)


def phase(case):
    return case["Seam"] if case["Kind"] == "source" else "publication"


def save(path, value):
    with Path(path).open("x") as output:
        json.dump(value, output, indent=2)
        output.write("\n")


def sha(path, limit=2 << 20, *, allow_empty=False):
    path = Path(path)
    require(path.stat().st_size <= limit and (allow_empty or path.stat().st_size > 0), "file hash admission")
    digest, size = hashlib.sha256(), 0
    with path.open("rb") as source:
        while chunk := source.read(64 << 10):
            size += len(chunk)
            require(size <= limit, "file grew beyond admitted hash bytes")
            digest.update(chunk)
    require(allow_empty or size > 0, "missing nonempty authority artifact")
    return digest.hexdigest()


def admitted(root):
    a = load(root / "composition-admission.json")
    require(set(a) == {"Format", "Cases", "CaseSeconds", "TotalSeconds", "StartedMonotonicNanos", "DeadlineMonotonicNanos", "ProfileSHA256"} and
            a["Format"] == FORMAT and a["Cases"] == cases() and
            type(a["CaseSeconds"]) is int and a["CaseSeconds"] == CASE_SECONDS and
            type(a["TotalSeconds"]) is int and a["TotalSeconds"] == TOTAL_SECONDS and
            type(a["StartedMonotonicNanos"]) is int and a["StartedMonotonicNanos"] > 0 and
            a["DeadlineMonotonicNanos"] == a["StartedMonotonicNanos"] + TOTAL_SECONDS * 10**9 and
            a["ProfileSHA256"] == sha(root / "profile.json"), "immutable finite composition admission/profile")
    return a


def start(directory):
    s = load(directory / "composition-start.json")
    c = shape(**{key.lower(): s["Case"][key] for key in ("Kind", "Seam", "Fault", "Coordinator")})
    a = admitted(directory.parent)
    require(s["Case"] == c and directory.name == name(c) and
            set(s) == {"Case", "AdmissionSHA256", "StartedMonotonicNanos", "DeadlineMonotonicNanos", "LedgerStart"} and
            s["AdmissionSHA256"] == sha(directory.parent / "composition-admission.json") and
            type(s["LedgerStart"]) is int and s["LedgerStart"] >= 0 and
            type(s["StartedMonotonicNanos"]) is int and a["StartedMonotonicNanos"] <= s["StartedMonotonicNanos"] < a["DeadlineMonotonicNanos"] and
            s["DeadlineMonotonicNanos"] == min(s["StartedMonotonicNanos"] + CASE_SECONDS * 10**9, a["DeadlineMonotonicNanos"]),
            "original case budget, never restart-relative renewal")
    return s


def marker(directory, case):
    p = phase(case)
    raw = platform.admitted_bytes(directory / (p + "-state.json"), 2 << 20)
    state = json.loads(raw, object_pairs_hook=platform.object_pairs)
    m = load(directory / (p + "-marker.json"))
    require(state["Coordinator"] == "AB".index(case["Coordinator"]), "actual SDK coordinator orientation")
    if case["Kind"] == "column":
        require(state["Case"] == case["Seam"] and state["Config"]["Graph"].startswith("column-publication-v1-") and
                len(state["Stages"]) == 3 and [s["Name"] for s in state["Stages"][:2]] == ["owner-active-before-update", "held-old-after-update"],
                "actual held-column/publisher prefix; not a source-held namespace")
        columns.marker(m, state, raw)
    else:
        require(state["Config"]["Graph"].startswith("source-history-fault-v1-"), "actual source-held namespace")
        history.marker(m, state, raw, p)
        require(len(state["Stages"]) == (4 if p == "owner" else 8), "authentic held source or exact partial collector prefix")
    return {"StateSHA256": hashlib.sha256(raw).hexdigest(),
            "MarkerSHA256": sha(directory / (p + "-marker.json")), "Phase": p}


def select(directory, network, kind, seam, fault, coordinator):
    directory = Path(directory)
    s = start(directory)
    c = shape(kind, seam, fault, coordinator)
    require(c == s["Case"] and time.monotonic_ns() < s["DeadlineMonotonicNanos"], "same admitted case before fault")
    bound = marker(directory, c)
    sdk = load(directory / (bound["Phase"] + ".before.json"))[0]
    platform.live(sdk)
    command = "column-fault-publication-child" if kind == "column" else "history-fault-" + ("owner-child" if seam == "owner" else "gc-child")
    require(sdk["Config"]["Cmd"] == ["fdb-spike", command], "actual unchanged native SDK executable")
    launcher.resource(load(directory.parent / "profile.json"), [sdk], "client")
    rows = platform.inventory(load(directory / "composition-healthy-before.json"),
                              load(directory / "composition-owned.before.json"), network, directory)
    for row in rows:
        launcher.resource(load(directory.parent / "profile.json"), [row["Inspect"]], "server")
    targets = platform.targets(rows, fault)
    plan = {"Format": FORMAT, "Case": c, "Network": network, "Marker": bound,
            "SDK": sdk, "Inventory": rows, "Targets": targets,
            "StartSHA256": sha(directory / "composition-start.json")}
    save(directory / "composition-plan.json", plan)
    for row in targets:
        print(row["Name"], row["Address"].split(":")[0])


def chronology(ledger, sdk, targets, network, fault, replacement):
    """Only genuine successful owned commands can establish event ordering."""
    sdk_name = sdk["Name"].lstrip("/")
    issued = platform.client_command(ledger, sdk_name)
    killed = platform.commands(ledger, ["kill", "--signal", "KILL", sdk_name])
    recovered = platform.client_command(ledger, replacement)
    logs = [i for i, row in enumerate(ledger) if row["command"] == ["docker", "logs", sdk_name] and
            row["exit"] == 0 and not row["deadline_expired"]]
    faults, heals = [], []
    for target in targets:
        n, ip = target["Name"], target["Address"].split(":")[0]
        faults.append(platform.commands(ledger, ["network", "disconnect", network, n] if fault == "network" else ["kill", "--signal", "KILL", n]))
        heals.append(platform.commands(ledger, ["network", "connect", "--ip", ip, network, n] if fault == "network" else ["start", n]))
        inspected = [i for i, row in enumerate(ledger) if row["command"] == ["docker", "inspect", n] and
                     row["exit"] == 0 and not row["deadline_expired"]]
        require(len(inspected) == 3 and inspected[0] < faults[-1] < inspected[1] < killed < heals[-1] < inspected[2] < recovered,
                "actual before/lost/healed target inspections surround SDK reap")
    require(faults and issued < min(faults) and any(issued < i < min(faults) for i in logs) and
            max(faults) < killed < min(heals) and max(heals) < recovered,
            "genuine marker then ALL target faults < SDK reap < ALL heals < replacement")
    return {"SDKStart": issued, "Faults": faults, "SDKKill": killed, "Heals": heals, "Replacement": recovered}


def native_proof(directory, plan, ledger_path):
    """Run the complete unchanged finalizer, then bind its actual proof bytes.

    A preexisting descriptive proof is never authority to skip native checks.
    The existing finalizer exclusively creates its original proof/prefix files.
    No graph states or decision outcomes are fabricated by this join.
    """
    # Admit the complete owned artifact set BEFORE the old finalizer hashes it.
    # Reserve its full command-prefix copy and bounded proof output as well.
    members, total = 0, 0
    for path in directory.rglob("*"):
        require(not path.is_symlink(), "no foreign artifact alias")
        if path.is_file():
            members += 1
            total += path.stat().st_size
            require(members <= 1024 and path.stat().st_size <= 16 << 20, "finite native proof inputs")
    require(total + Path(ledger_path).stat().st_size + (8 << 20) <= 128 << 20,
            "full native finalizer temporary/output ownership admission")
    if plan["Case"]["Kind"] == "column":
        columns.finalize(directory, ledger_path)
    else:
        history.finalize(directory, ledger_path)
    proof = load(directory / "proof.json", 8 << 20)
    kind = plan["Case"]["Kind"]
    require(proof["Format"] == (columns.FORMAT if kind == "column" else history.FORMAT) and
            proof["Coordinator"] == "AB".index(plan["Case"]["Coordinator"]) and
            proof["SetupOperations"] == 3 and proof["UsefulOperations"] == 2 and
            proof["Serial"]["Witness"] is not None, "complete genuine existing terminal/serial proof")
    hashes = proof["ArtifactsSHA256"]
    required = {"terminal.json", "seed.json", "recovery.json", "clients.json", "proof.json"}
    # proof.json excludes itself by construction. The immutable raw prefix hash
    # and all actual source/cache/GC members remain required in that exact proof.
    required.remove("proof.json")
    required |= {"commands.prefix.jsonl", "fdb-cost-history.jsonl", "fdb-cost-current.jsonl", "fdb-cost-cdc.jsonl"}
    require(required <= set(hashes), "no terminal/raw EOF/original/cursor member omitted")
    require(0 < len(hashes) <= 1024, "finite proof inventory")
    total = 0
    for key, digest in hashes.items():
        rel = Path(key)
        require(not rel.is_absolute() and rel.parts and key == rel.as_posix() and all(p not in ("", ".", "..") for p in rel.parts), "owned proof path")
        path = directory / rel
        require(not path.is_symlink() and all(not (directory / parent).is_symlink() for parent in rel.parents) and
                path.is_file() and path.resolve().is_relative_to(directory.resolve()), "owned genuine artifact")
        total += path.stat().st_size
        require(total <= 128 << 20 and sha(path, 16 << 20, allow_empty=True) == digest, "complete bounded native proof inventory/hash")
    return proof


def finalize(directory, ledger_path):
    directory = Path(directory)
    s = start(directory)
    plan = load(directory / "composition-plan.json")
    require(plan["Format"] == FORMAT and plan["Case"] == s["Case"] and
            plan["StartSHA256"] == sha(directory / "composition-start.json") and
            plan["Marker"] == marker(directory, s["Case"]), "same authentic prefault marker/state/owner")
    rows = platform.inventory(load(directory / "composition-healthy-before.json"),
                              load(directory / "composition-owned.before.json"), plan["Network"], directory)
    require(plan["Inventory"] == rows and plan["Targets"] == platform.targets(rows, s["Case"]["Fault"]), "actual requested/co-resident role set")
    p = phase(s["Case"])
    before, stopped = load(directory / (p + ".before.json"))[0], load(directory / (p + ".stopped.json"))[0]
    require(before == plan["SDK"], "same actual SDK caller through server fault")
    platform.killed(before, stopped)
    for target in plan["Targets"]:
        n = target["Name"]
        b, a = load(directory / (n + ".composition-before.json"))[0], load(directory / (n + ".composition-after.json"))[0]
        require(b == target["Inspect"] and platform.live(b) == tuple(target["Instance"]), "actual target unchanged before fault")
        if s["Case"]["Fault"] == "network":
            detached = load(directory / (n + ".composition-detached.json"))[0]
            require(platform.live(detached) == platform.live(b) == platform.live(a) and
                    plan["Network"] not in detached["NetworkSettings"]["Networks"] and
                    a["NetworkSettings"]["Networks"][plan["Network"]]["IPAddress"] + ":4500" == target["Address"] and
                    platform.volume(b) == platform.volume(a), "actual same-process symmetric domain isolation/heal")
        else:
            platform.killed(b, load(directory / (n + ".composition-stopped.json"))[0])
            require(b["Id"] == a["Id"] and platform.live(b) != platform.live(a) and
                    platform.volume(b) == platform.volume(a), "actual changed-incarnation same-volume native recovery")
    launcher.healthy(load(directory / "composition-healthy-after.json"))
    full = platform.records(ledger_path)
    require(s["LedgerStart"] < len(full), "actual admitted command prefix")
    ledger = full[s["LedgerStart"]:]
    clients = load(directory / "clients.json")
    replacement = clients["Terminal" if p == "gc" else "Recovery"]
    order = chronology(ledger, before, plan["Targets"], plan["Network"], s["Case"]["Fault"], replacement)
    native = native_proof(directory, plan, ledger_path)
    finished = time.monotonic_ns()
    require(s["StartedMonotonicNanos"] < finished <= s["DeadlineMonotonicNanos"], "same finite case deadline includes verification")
    proof = {"Format": FORMAT, "Case": s["Case"], "Plan": plan, "Chronology": order,
             "StartedMonotonicNanos": s["StartedMonotonicNanos"], "FinishedMonotonicNanos": finished,
             "NativeProofSHA256": sha(directory / "proof.json", 8 << 20),
             "NativeFormat": native["Format"], "NativeSerial": native["Serial"],
             "Scope": "actual simultaneous co-resident-role/domain loss plus unchanged SDK owner/publication/collector recovery; source-held and column-held namespaces distinct; no inside-outage progress, packet-reorder, power-loss, matrix, capacity or V2 acceptance"}
    save(directory / "composition-proof.json", proof)


def complete(root):
    root = Path(root)
    a = admitted(root)
    actual = sorted(p.name for p in root.glob("composed-*") if p.is_dir())
    require(actual == sorted(name(c) for c in cases()), "all and only sixty admitted compositions")
    proofs, previous = [], a["StartedMonotonicNanos"]
    for c in cases():
        directory = root / name(c)
        s, proof = start(directory), load(directory / "composition-proof.json", 8 << 20)
        require(proof["Format"] == FORMAT and proof["Case"] == c and
                previous <= s["StartedMonotonicNanos"] == proof["StartedMonotonicNanos"] < proof["FinishedMonotonicNanos"] <= s["DeadlineMonotonicNanos"] and
                proof["NativeProofSHA256"] == sha(directory / "proof.json", 8 << 20), "original ordered case/deadline/proof lineage")
        previous = proof["FinishedMonotonicNanos"]
        proofs.append({"Case": c, "ProofSHA256": sha(directory / "composition-proof.json", 8 << 20)})
    finished = time.monotonic_ns()
    require(previous <= finished <= a["DeadlineMonotonicNanos"], "fixed total budget without phase renewal")
    save(root / "composition-complete.json", {"Format": FORMAT, "Admission": a, "Cases": proofs,
                                              "FinishedMonotonicNanos": finished})


def main():
    mode, *args = sys.argv[1:]
    if mode == "admit":
        root = Path(args[0]); now = time.monotonic_ns()
        save(root / "composition-admission.json", {"Format": FORMAT, "Cases": cases(), "CaseSeconds": CASE_SECONDS,
             "TotalSeconds": TOTAL_SECONDS, "StartedMonotonicNanos": now,
             "DeadlineMonotonicNanos": now + TOTAL_SECONDS * 10**9, "ProfileSHA256": sha(root / "profile.json")})
    elif mode == "begin":
        directory, kind, seam, fault, coordinator, ledger = args
        directory = Path(directory); a = admitted(directory.parent); now = time.monotonic_ns()
        require(now < a["DeadlineMonotonicNanos"], "original total deadline")
        save(directory / "composition-start.json", {"Case": shape(kind, seam, fault, coordinator),
             "AdmissionSHA256": sha(directory.parent / "composition-admission.json"), "StartedMonotonicNanos": now,
             "DeadlineMonotonicNanos": min(now + CASE_SECONDS * 10**9, a["DeadlineMonotonicNanos"]), "LedgerStart": len(platform.records(ledger))})
    elif mode == "select":
        select(*args)
    elif mode == "finalize":
        finalize(*args)
    elif mode == "complete":
        complete(*args)
    else:
        raise ValueError("unsupported explicit composed action")


if __name__ == "__main__":
    main()
