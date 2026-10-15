#!/usr/bin/env python3
"""Fresh actual-child recorded replay and bounded semantic counterexample deletion.

No binary is built here. Recipes name actions only; every trial obtains genuine
native originals from fresh children and preserves those originals through loss.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time

SPEC = importlib.util.spec_from_file_location("native_recorded_semantics", Path(__file__).with_name("native_transition_check.py"))
C = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(C)
MAX_TRIALS = 16
MAX_TRIAL_BYTES = 2 << 30
MAX_SECONDS = 1800
WATCHDOG = Path(__file__).with_name("process_watchdog.py")


def require(value, message):
    if not value:
        raise ValueError("native recorded replay refused: " + message)


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as source:
        while block := source.read(65536): h.update(block)
    return h.hexdigest()


def canonical(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def write(path, value):
    wire = (json.dumps(value, indent=2, ensure_ascii=False)+"\n").encode()
    require(len(wire) <= 65536, "bounded controller receipt")
    with path.open("xb") as stream:
        stream.write(wire)


def bounded_json(path):
    with path.open("rb") as stream: wire = stream.read(65537)
    require(0 < len(wire) <= 65536, "bounded actual terminal receipt")
    return C.decoded(wire)


def seeded_recipe(seed):
    C.integer(seed, minimum=1)
    # Fixed xorshift64 actions; this is a reproducible scheduler choice, never
    # a request/authority/randomness source. All exact actions are recorded.
    state = seed
    def next_():
        nonlocal state
        state ^= (state << 13) & ((1 << 64)-1)
        state ^= state >> 7
        state ^= (state << 17) & ((1 << 64)-1)
        return state
    choices = [(phase, part, side) for phase in (1, 2, 3, 4) for part in (3, 8)
               if phase not in (1, 3) or part == 8
               for side in ("before-submit", "after-applied")]
    phase, part, side = choices[next_() % len(choices)]
    actions = [dict(Member=1+next_()%6, Ticks=1+next_()%3) for _ in range(4)]
    value = dict(Version=1, Seed=seed, Phase=phase, Partition=part, Side=side, Actions=actions)
    C.recipe(value)
    return value


def footprint(root):
    """Finite actual child/artifact files, no directory-sized list or symlinks."""
    total, files, directories = 0, 0, [root]
    while directories:
        directory = directories.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                files += 1
                require(files <= 8192 and not entry.is_symlink(), "owned replay file/name admission")
                if entry.is_dir(follow_symlinks=False):
                    require(len(directories) < 1024, "owned replay directory work")
                    directories.append(Path(entry.path))
                else:
                    require(entry.is_file(follow_symlinks=False), "foreign owned-file kind")
                    n = entry.stat(follow_symlinks=False).st_size
                    require(type(n) is int and 0 <= n <= MAX_TRIAL_BYTES-total, "actual replay disk quota")
                    total += n
    return dict(files=files, bytes=total)


def trial(root, number, recipe, binary, binary_sha, watchdog, seconds, deadline, memory_mib):
    require(0 <= number < MAX_TRIALS and time.time_ns() < deadline, "fixed whole-replay trial/deadline grant exhausted")
    C.recipe(recipe)
    require(digest(binary) == binary_sha, "actual selected binary unchanged before fresh cohort")
    path = root/f"trial-{number:02d}"
    path.mkdir(mode=0o700)
    temporary = path/"tmp"; temporary.mkdir(mode=0o700)
    run = secrets.token_hex(16)
    env = {**os.environ, "TMPDIR": str(temporary), "GOTMPDIR": str(temporary), "GOWORK": "off", "GOTOOLCHAIN": "local",
           "GOMAXPROCS": "2", "GOMEMLIMIT": "2GiB", "RHO_GRAPH_TRANSITION_RECIPE": canonical(recipe),
           "RHO_GRAPH_TRANSITION_OUTPUT": str(path/"engine"), "RHO_GRAPH_TRANSITION_RUN": run,
           "RHO_GRAPH_TRANSITION_DEADLINE": str(deadline)}
    for name in ("RHO_GRAPH_COST_SPEC", "RHO_GRAPH_NATIVE_WORKLOAD", "RHO_GRAPH_NATIVE_FAULT_PROCESS", "RHO_GRAPH_NATIVE_FAULT_START",
                 "RHO_GRAPH_RANGE_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS", "RHO_GRAPH_DURABLE_COLUMN_PROCESS",
                 "RHO_GRAPH_FAULT_FAMILY_PROCESS", "RHO_GRAPH_DURABLE_MODEL", "RHO_GRAPH_DECISION_FAULT_PROCESS"):
        env[name] = ""
    remaining = max(1, (deadline-time.time_ns()+999_999_999)//1_000_000_000)
    actual = [str(binary), "-test.run=^TestNativeTransitionRecordedReplayCell$", "-test.count=1", "-test.v", f"-test.timeout={remaining}s"]
    command = [sys.executable, str(watchdog), "--rss-mib", str(memory_mib), "--seconds", str(seconds),
               "--deadline-unix-nanos", str(deadline), "--owned-dir", str(path), "--disk-bytes", str(MAX_TRIAL_BYTES),
               "--report", str(path/"watchdog.json"), "--", *actual]
    request = dict(version=1, recipe=recipe, recipeSHA256=hashlib.sha256(canonical(recipe).encode()).hexdigest(), run=run,
                   binarySHA256=binary_sha, watchdogSHA256=digest(watchdog), deadlineUnixNanos=deadline,
                   seconds=seconds, aggregateRSSMiB=memory_mib, command=command,
                   admission=dict(trials=MAX_TRIALS, liveEngineProcesses=6, totalEngineCreationsPerTrial=7,
                                  actions=8, ticks=16, artifactBytes=16 << 20, trialDiskBytes=MAX_TRIAL_BYTES))
    write(path/"request.json", request)
    receipt = dict(version=1, requestSHA256=digest(path/"request.json"), recipe=recipe, run=run, binarySHA256=binary_sha,
                   deadlineUnixNanos=deadline, exit=None, monitor=None, monitorSHA256=None, artifact=None,
                   artifactSHA256=None, classification="unavailable", semanticFailure=None, proof=None, disk=None)
    try:
        with (path/"stdout.txt").open("xb") as out, (path/"stderr.txt").open("xb") as error:
            result = subprocess.run(command, env=env, stdout=out, stderr=error, check=False)
        receipt["exit"] = result.returncode
        meter = bounded_json(path/"watchdog.json")
        receipt["monitor"], receipt["monitorSHA256"] = meter, digest(path/"watchdog.json")
        if result.returncode != 0 or meter["reason"] != "completed" or meter["exit_code"] != 0:
            receipt["classification"] = "driver-or-resource-refused"
            return receipt
        require(meter["command"] == actual and meter["deadline_unix_nanos"] == deadline and meter["wall_limit_s"] == seconds
                and meter["rss_limit_mib"] == memory_mib and 0 < meter["peak_tree_rss_mib"] <= memory_mib
                and meter["all_owned_exited"] is True, "actual fixed-budget process-owner terminal")
        artifact_path = None
        with os.scandir(path/"engine") as entries:
            for entry in entries:
                require(artifact_path is None and entry.is_file(follow_symlinks=False) and not entry.is_symlink()
                        and entry.name.startswith("native-transition-cross-"), "one genuine trial capture")
                artifact_path = Path(entry.path)
        require(artifact_path is not None and digest(binary) == binary_sha, "complete trial capture/unchanged actual binary")
        artifact = C.load(artifact_path)
        require(artifact["Recipe"] == recipe and bytes(artifact["Run"]).hex() == run and artifact["DeadlineUnixNanos"] == deadline
                and bytes(artifact["RecipeSHA256"]).hex() == request["recipeSHA256"], "actual fresh source/recipe/original deadline")
        receipt["artifact"], receipt["artifactSHA256"] = str(artifact_path.relative_to(root)), digest(artifact_path)
        try:
            receipt["proof"] = C.check(artifact)
            receipt["classification"] = "passed"
        except C.SemanticFailure as failure:
            receipt["classification"] = "semantic-failure"
            receipt["semanticFailure"] = dict(code=failure.code, detail=failure.detail)
        except (ValueError, KeyError, TypeError) as failure:
            receipt["classification"], receipt["diagnostic"] = "source-refused", str(failure)[:4096]
        return receipt
    except (OSError, ValueError, KeyError, TypeError) as failure:
        receipt["classification"], receipt["diagnostic"] = "source-refused", str(failure)[:4096]
        return receipt
    finally:
        try:
            receipt["disk"] = footprint(path)
        except (OSError, ValueError) as error:
            receipt["classification"], receipt["diagnostic"] = "driver-or-resource-refused", str(error)[:4096]
        write(path/"receipt.json", receipt)


def minimize(recipe, invoke, expected_code, *, used=2, maximum=MAX_TRIALS):
    """Single-action deletion, fresh invoke for EVERY candidate; no simulation."""
    C.recipe(recipe)
    require(2 <= used <= maximum <= MAX_TRIALS and type(expected_code) is str and expected_code, "prepaid replay/minimization pool")
    current = {**recipe, "Actions": list(recipe["Actions"] or [])}
    index, attempts, accepted, unresolved = 0, [], [], []
    while index < len(current["Actions"]) and used < maximum:
        candidate = {**current, "Actions": current["Actions"][:index]+current["Actions"][index+1:]}
        outcome = invoke(candidate, used)
        used += 1
        same = outcome.get("classification") == "semantic-failure" and (outcome.get("semanticFailure") or {}).get("code") == expected_code
        conclusive = outcome.get("classification") in ("passed", "semantic-failure")
        attempts.append(dict(trial=used-1, removed=index, sameSemanticFailure=same,
                             independentlyClassified=conclusive))
        if same:
            accepted.append(used-1)
            current, index, unresolved = candidate, 0, []
        else:
            if not conclusive:
                unresolved.append(used-1)
            index += 1
    return dict(recipe=current, used=used, attempts=attempts, acceptedTrials=accepted,
                unresolvedTrials=unresolved,
                deletionOneMinimal=index == len(current["Actions"]) and not unresolved,
                minimality="single-action deletion under the fixed mandatory selector/recovery recipe; no global minimum or infinite-state claim")


def execute(output, binary, watchdog, seconds, memory_mib, recipe):
    C.recipe(recipe)
    require(type(seconds) is int and 1 <= seconds <= MAX_SECONDS and type(memory_mib) is int and 4096 <= memory_mib <= 1 << 20, "finite caller-selected whole-replay resources")
    binary, watchdog = Path(binary).resolve(strict=True), Path(watchdog).resolve(strict=True)
    require(binary.is_file() and os.access(binary, os.X_OK) and watchdog.is_file(), "supplied actual native binary/repository watchdog")
    destination = Path(output).absolute()
    require(destination.parent.resolve(strict=True) == destination.parent and not destination.exists(), "fresh owned output parent")
    now = time.time_ns(); deadline = now+seconds*1_000_000_000
    require(deadline <= (1 << 63)-1 and MAX_TRIALS*seconds*1_000_000_000 <= (1 << 63)-1, "checked fixed deadline/cumulative trial time")
    destination.mkdir(mode=0o700)
    binary_sha = digest(binary)
    write(destination/"plan.json", dict(version=1, recipe=recipe, binarySHA256=binary_sha, watchdogSHA256=digest(watchdog),
                                       seconds=seconds, deadlineUnixNanos=deadline, aggregateRSSMiB=memory_mib,
                                       maxTrials=MAX_TRIALS, maxDiskBytes=MAX_TRIALS*MAX_TRIAL_BYTES))
    receipts = []
    def invoke(value, number):
        require(number == len(receipts) and len(receipts) < MAX_TRIALS, "monotone whole-query trial accounting")
        receipt = trial(destination, number, value, binary, binary_sha, watchdog, seconds, deadline, memory_mib)
        receipts.append(receipt)
        return receipt
    result = dict(version=1, planSHA256=digest(destination/"plan.json"), receipts=[], reproduced=False, minimization=None,
                  scope="fixed recorded real six-child replay; semantic-failure-only bounded deletion; source-only until executed qualification")
    try:
        first = invoke(recipe, 0)
        # Replaying the recorded exact schedule always uses a NEW actual child
        # cohort and original factory; no captured proposal is resubmitted as
        # authority to a fake/reconstructed Host.
        again = invoke(recipe, 1)
        code = (first.get("semanticFailure") or {}).get("code")
        if code and again.get("classification") == "semantic-failure" and (again.get("semanticFailure") or {}).get("code") == code:
            result["reproduced"] = True
            result["minimization"] = minimize(recipe, invoke, code)
        return result
    finally:
        result["receipts"] = [{"trial": n, "receiptSHA256": digest(destination/f"trial-{n:02d}"/"receipt.json"),
                               "classification": row["classification"], "artifactSHA256": row["artifactSHA256"]} for n, row in enumerate(receipts)]
        write(destination/"result.json", result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--watchdog", type=Path, default=WATCHDOG)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--seconds", required=True, type=int)
    parser.add_argument("--memory-mib", required=True, type=int)
    parser.add_argument("--seed", required=True, type=int)
    args = parser.parse_args()
    print(json.dumps(execute(args.output, args.binary, args.watchdog, args.seconds, args.memory_mib, seeded_recipe(args.seed)), indent=2))


if __name__ == "__main__":
    main()
