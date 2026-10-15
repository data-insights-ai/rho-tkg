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
CAMPAIGN_CONTROL_BYTES = 4 * 65536
CAMPAIGN_TRIAL_BYTES = MAX_TRIAL_BYTES-2*65536
WATCHDOG = Path(__file__).with_name("process_watchdog.py")
FIXTURE_FLAGS = ("RHO_GRAPH_COST_SPEC", "RHO_GRAPH_COST_BLOCK_SIZE", "RHO_GRAPH_NATIVE_WORKLOAD",
                 "RHO_GRAPH_NATIVE_FAULT_PROCESS", "RHO_GRAPH_NATIVE_FAULT_START", "RHO_GRAPH_NATIVE_FAULT_FAMILY",
                 "RHO_GRAPH_RANGE_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS",
                 "RHO_GRAPH_DURABLE_COLUMN_PROCESS", "RHO_GRAPH_FAULT_FAMILY_PROCESS", "RHO_GRAPH_DURABLE_MODEL",
                 "RHO_GRAPH_DECISION_FAULT_PROCESS", "RHO_GRAPH_TRANSITION_PROCESS", "RHO_GRAPH_FEED_PROCESS",
                 "RHO_GRAPH_FEED_RETAINED_PROCESS", "RHO_GRAPH_SELECTORS_PROCESS", "RHO_GRAPH_CREATE_UPDATE_PROCESS",
                 "RHO_GRAPH_IMMUTABLE_COLUMNS_PROCESS", "RHO_GRAPH_LATE_CORRECTION_PROCESS", "RHO_GRAPH_EDGE_UNIQUE_PROCESS",
                 "RHO_GRAPH_STATE_RETENTION_PROCESS", "RHO_GRAPH_STABLE_STORAGE_PROCESS", "RHO_GRAPH_STABLE_STATE_PROCESS",
                 "RHO_GRAPH_PROTOCOL_CAMPAIGN", "RHO_GRAPH_PROTOCOL_STATE_PROCESS", "RHO_GRAPH_PROTOCOL_STATE_RECOVERY",
                 "RHO_GRAPH_TRANSFER_PROCESS")


def components(profile):
    if profile == "cross":
        return C
    require(profile == "composed", "exact supported fresh recorded profile")
    spec = importlib.util.spec_from_file_location("native_recorded_composed", Path(__file__).with_name("native_transition_composed_check.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def composed_sources(watchdog):
    directory = Path(__file__).resolve().parent
    names = ("native_transition_replay.py", "native_transition_composed_replay.py", "native_transition_composed_check.py",
             "native_fault_family_check.py", "native_fault_schedule_check.py", "native_transition_check.py", "check-p06-history.py")
    result = {name: digest(directory/name) for name in names}
    result["independent-oracle"] = digest(directory.parents[1]/"docs/v5/reference/independent/protocol/oracle.py")
    result["owned-watchdog"] = digest(watchdog)
    return result


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


def footprint(root, *, physical=False):
    """Finite actual child/artifact files, no directory-sized list or symlinks."""
    total, allocated, files, directories = 0, 0, 0, [root]
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
                    info = entry.stat(follow_symlinks=False)
                    n = info.st_size
                    require(type(n) is int and 0 <= n <= MAX_TRIAL_BYTES-total, "actual replay disk quota")
                    total += n
                    if physical:
                        allocated += info.st_blocks * 512
                        require(0 <= allocated <= MAX_TRIAL_BYTES, "actual allocated replay file quota")
    result = dict(files=files, bytes=total)
    if physical: result["allocatedFileBytes"] = allocated
    return result


def semantic_code(receipt):
    """A stale diagnostic payload cannot override a later owner refusal."""
    if receipt.get("classification") != "semantic-failure":
        return None
    value = (receipt.get("semanticFailure") or {}).get("code")
    return value if type(value) is str and 0 < len(value) <= 4096 else None


def terminal_result(meter, wrapper_exit, profile):
    require(profile in ("cross", "composed"), "exact fixed replay profile")
    require(type(wrapper_exit) is int and type(meter["exit_code"]) is int, "actual integer process exit statuses")
    completed = wrapper_exit == 0 and meter["reason"] == "completed" and meter["exit_code"] == 0
    failed_verdict = profile == "composed" and wrapper_exit == 124 and meter["reason"] == "command-failed" and meter["exit_code"] == 1
    return completed, failed_verdict


def trial_environment(temporary, engine, recipe, run, deadline, profile, *, inherited=None):
    require(profile in ("cross","composed"),"actual replay environment profile")
    env=dict(os.environ if inherited is None else inherited)
    for name in FIXTURE_FLAGS:env[name]=""
    env.update(TMPDIR=str(temporary),GOTMPDIR=str(temporary),GOWORK="off",GOTOOLCHAIN="local",GOMAXPROCS="2",GOMEMLIMIT="2GiB",
               RHO_GRAPH_TRANSITION_RUN=run,RHO_GRAPH_TRANSITION_DEADLINE=str(deadline),
               RHO_GRAPH_TRANSITION_RECIPE=canonical(recipe) if profile=="cross" else "",
               RHO_GRAPH_TRANSITION_OUTPUT=str(engine) if profile=="cross" else "",
               RHO_GRAPH_COMPOSED_RECIPE=canonical(recipe) if profile=="composed" else "",
               RHO_GRAPH_COMPOSED_OUTPUT=str(engine) if profile=="composed" else "")
    return env


def trial(root, number, recipe, binary, binary_sha, watchdog, seconds, deadline, memory_mib, *, profile="cross", source_hashes=None, campaign=False):
    require(0 <= number < MAX_TRIALS and time.time_ns() < deadline, "fixed whole-replay trial/deadline grant exhausted")
    if profile == "composed": require(source_hashes is not None and composed_sources(watchdog) == source_hashes, "same admitted independent checker/controller/watchdog sources")
    checker = components(profile)
    checker.recipe(recipe)
    require(digest(binary) == binary_sha, "actual selected binary unchanged before fresh cohort")
    path = root/f"trial-{number:02d}"
    path.mkdir(mode=0o700)
    temporary = path/"tmp"; temporary.mkdir(mode=0o700)
    run = secrets.token_hex(16)
    env = trial_environment(temporary,path/"engine",recipe,run,deadline,profile)
    remaining = max(1, (deadline-time.time_ns()+999_999_999)//1_000_000_000)
    parent = "TestNativeComposedTransitionRecordedReplayCell" if profile == "composed" else "TestNativeTransitionRecordedReplayCell"
    actual = [str(binary), "-test.run=^"+parent+"$", "-test.count=1", "-test.v", f"-test.timeout={remaining}s"]
    disk_limit = MAX_TRIAL_BYTES-CAMPAIGN_CONTROL_BYTES if campaign else MAX_TRIAL_BYTES
    command = [sys.executable, str(watchdog), "--rss-mib", str(memory_mib), "--seconds", str(seconds),
               "--deadline-unix-nanos", str(deadline), "--owned-dir", str(path), "--disk-bytes", str(disk_limit),
               "--report", str(path/"watchdog.json"), "--", *actual]
    request = dict(version=1, recipe=recipe, recipeSHA256=hashlib.sha256(canonical(recipe).encode()).hexdigest(), run=run,
                   binarySHA256=binary_sha, watchdogSHA256=digest(watchdog), deadlineUnixNanos=deadline,
                   seconds=seconds, aggregateRSSMiB=memory_mib, command=command,
                   admission=dict(trials=MAX_TRIALS, liveEngineProcesses=6, totalEngineCreationsPerTrial=7,
                                  actions=8, ticks=16, artifactBytes=16 << 20, trialDiskBytes=MAX_TRIAL_BYTES))
    if profile == "composed":
        request.update(version=2, profile=checker.PROFILE, sourceSHA256=source_hashes)
        request["admission"].update(totalEngineCreationsPerTrial=6, duplicates=4)
    if campaign:
        require(profile == "composed", "fresh campaign uses the actual composed adapter")
        request["admission"].update(controllerReserveBytes=CAMPAIGN_CONTROL_BYTES,
                                     monitoredDiskBytes=disk_limit)
    write(path/"request.json", request)
    receipt = dict(version=1, requestSHA256=digest(path/"request.json"), recipe=recipe, run=run, binarySHA256=binary_sha,
                   deadlineUnixNanos=deadline, exit=None, monitor=None, monitorSHA256=None, artifact=None,
                   artifactSHA256=None, classification="unavailable", semanticFailure=None, proof=None, disk=None)
    if profile == "composed": receipt.update(version=2, profile=checker.PROFILE)
    try:
        with (path/"stdout.txt").open("xb") as out, (path/"stderr.txt").open("xb") as error:
            result = subprocess.run(command, env=env, stdout=out, stderr=error, check=False)
        receipt["exit"] = result.returncode
        meter = bounded_json(path/"watchdog.json")
        receipt["monitor"], receipt["monitorSHA256"] = meter, digest(path/"watchdog.json")
        engine_exit = meter["exit_code"]
        completed, failed_verdict = terminal_result(meter, result.returncode, profile)
        if profile == "composed": receipt["engineExit"] = engine_exit
        if not (completed or failed_verdict):
            receipt["classification"] = "driver-or-resource-refused"
            return receipt
        require(meter["command"] == actual and meter["deadline_unix_nanos"] == deadline and meter["wall_limit_s"] == seconds
                and meter["rss_limit_mib"] == memory_mib and 0 < meter["peak_tree_rss_mib"] <= memory_mib
                and meter["all_owned_exited"] is True and meter.get("forced_cleanup") is False, "actual fixed-budget process-owner terminal")
        artifact_path = None
        with os.scandir(path/"engine") as entries:
            for entry in entries:
                require(artifact_path is None and entry.is_file(follow_symlinks=False) and not entry.is_symlink()
                        and entry.name.startswith("native-composed-" if profile == "composed" else "native-transition-cross-"), "one genuine trial capture")
                artifact_path = Path(entry.path)
        require(artifact_path is not None and digest(binary) == binary_sha, "complete trial capture/unchanged actual binary")
        if profile == "composed": require(composed_sources(watchdog) == source_hashes, "source changed during genuine child replay")
        artifact = checker.load(artifact_path)
        owner = artifact["Replay"] if profile == "composed" else artifact
        require(owner["Recipe"] == recipe and bytes(owner["Run"]).hex() == run and owner["DeadlineUnixNanos"] == deadline
                and bytes(owner["RecipeSHA256"]).hex() == request["recipeSHA256"], "actual fresh source/recipe/original deadline")
        receipt["artifact"], receipt["artifactSHA256"] = str(artifact_path.relative_to(root)), digest(artifact_path)
        try:
            receipt["proof"] = checker.check(artifact)
            receipt["classification"] = "passed" if completed else "driver-or-resource-refused"
        except checker.SemanticFailure as failure:
            # Fresh Go preserves a complete capture, then FAILS its own
            # independent verdict. A passing Go cell cannot hide a failed
            # checker; an unrelated failed test plus a passing proof is refused.
            if profile == "composed" and not failed_verdict:
                receipt["classification"] = "source-refused"
            else:
                receipt["classification"] = "semantic-failure"
                receipt["semanticFailure"] = dict(code=failure.code, detail=failure.detail)
        except (ValueError, KeyError, TypeError) as failure:
            receipt["classification"], receipt["diagnostic"] = "source-refused", str(failure)[:4096]
        if campaign:
            require(time.time_ns() < deadline, "independent oracle exceeded original whole-campaign deadline")
        return receipt
    except (OSError, ValueError, KeyError, TypeError) as failure:
        receipt["classification"], receipt["diagnostic"] = "source-refused", str(failure)[:4096]
        return receipt
    finally:
        if profile == "composed":
            try:
                require(composed_sources(watchdog) == source_hashes, "source changed during independent semantic verdict")
            except (OSError, ValueError) as error:
                receipt["classification"], receipt["semanticFailure"], receipt["diagnostic"] = "source-refused", None, str(error)[:4096]
        try:
            receipt["disk"] = footprint(path, physical=campaign)
            if campaign:
                require(time.time_ns() < deadline, "native/source finalizer exceeded original whole-campaign deadline")
                require(max(receipt["disk"]["bytes"],receipt["disk"]["allocatedFileBytes"]) <= CAMPAIGN_TRIAL_BYTES-65536,
                        "prepaid terminal controller receipt cannot fit")
        except (OSError, ValueError) as error:
            receipt["classification"], receipt["diagnostic"] = "driver-or-resource-refused", str(error)[:4096]
        if campaign and receipt["classification"] not in ("passed", "semantic-failure"):
            receipt["semanticFailure"], receipt["proof"] = None, None
        write(path/"receipt.json", receipt)


def minimize(recipe, invoke, expected_code, *, used=2, maximum=MAX_TRIALS, validator=None):
    """Single-action deletion, fresh invoke for EVERY candidate; no simulation."""
    (C.recipe if validator is None else validator)(recipe)
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


def campaign_admission(value, checker):
    """Closed six-order + recorded-seed domain, not a model executor."""
    require(type(value) is dict and set(value)=={"Version","Enumeration","Recorded"} and type(value["Version"]) is int and value["Version"]==1,
            "explicit finite composed campaign")
    enumeration,recorded=value["Enumeration"],value["Recorded"]
    require(type(enumeration) is list and len(enumeration)==6 and type(recorded) is list and 1<=len(recorded)<=2,
            "six actual causal orders precede one or two recorded seeds")
    seen,seeds=set(),set()
    for r in enumeration:
        checker.recipe(r)
        identity=(r["First"],r["Phase"],r["FenceFirst"])
        require(r["Actions"]==[] and identity not in seen,"one empty-action actual base per causal order")
        seen.add(identity)
    expected={(first,phase,fence) for first in ("A","B") for phase in ("registered","prepared") for fence in (True,False) if first=="A" or fence}
    require(seen==expected,"missing real two-conflict/fence/transfer causal order")
    for r in recorded:
        checker.recipe(r)
        require(r["Seed"] not in seeds,"recorded seeds are explicit and distinct")
        seeds.add(r["Seed"])
    require(all(r["Seed"]==recorded[0]["Seed"] for r in enumeration) and 6+2*len(recorded)<=MAX_TRIALS,
            "one original enumeration seed and prepaid fresh exact replays")
    return value


def campaign_sequence(result, campaign, invoke, receipts, checker):
    """Every invoke is the existing real six-child trial and full oracle."""
    def run(r, stage):
        number=len(receipts)
        result["schedule"].append(dict(trial=number,stage=stage,recipeSHA256=hashlib.sha256(canonical(r).encode()).hexdigest()))
        return invoke(r,number)
    def reproduce(r, first, stage, again=None):
        code=semantic_code(first)
        if not code:
            result["classification"]="semantic-failure-unreproduced" if again is not None and semantic_code(again) else "source-refused"
            return
        again=run(r,stage+"-exact-replay") if again is None else again
        if semantic_code(again)!=code:
            result["classification"]="semantic-failure-unreproduced"
            return
        result["reproduced"],result["classification"]=True,"semantic-failure"
        result["failureCode"],result["counterexampleRecipe"]=code,r
        result["minimization"]=minimize(r,invoke,code,used=len(receipts),validator=checker.recipe)
    bases=[]
    for r in campaign["Enumeration"]:
        bases.append(run(r,"causal-enumeration"))
    result["enumerationComplete"]=True
    result["enumerationPassed"]=all(row["classification"]=="passed" for row in bases)
    if not result["enumerationPassed"]:
        for r,row in zip(campaign["Enumeration"],bases):
            if semantic_code(row):
                reproduce(r,row,"causal-enumeration")
                return
        result["classification"]="source-refused"
        return
    for r in campaign["Recorded"]:
        first=run(r,"recorded-seed")
        again=run(r,"recorded-seed-exact-replay")
        result["recordedSeedsAttempted"]+=1
        if first["classification"]==again["classification"]=="passed":
            result["recordedSeedsPassed"]+=1
        else:
            reproduce(r,first,"recorded-seed",again)
            return
    result["classification"]="passed"


def campaign_trial_finalizer(receipt,path,deadline):
    receipt["capturedClassification"]=receipt["classification"]
    try:
        receipt["finalizedDisk"]=footprint(path,physical=True)
        require(time.time_ns()<deadline and max(receipt["finalizedDisk"]["bytes"],receipt["finalizedDisk"]["allocatedFileBytes"])<=CAMPAIGN_TRIAL_BYTES,
                "late or over-budget terminal trial finalizer")
    except (OSError,ValueError) as failure:
        receipt.update(classification="driver-or-resource-refused",semanticFailure=None,proof=None,finalizationRefusal=str(failure)[:4096])
    return receipt


def execute(output, binary, watchdog, seconds, memory_mib, recipe, *, profile="cross", deadline_unix_nanos=None, campaign=None):
    checker = components(profile)
    checker.recipe(recipe)
    if campaign is not None:
        require(profile=="composed","campaign requires the actual composed native source adapter")
        campaign=campaign_admission(campaign,checker)
        require(recipe==campaign["Recorded"][0],"plan's original recorded recipe must match its actual campaign")
    require(type(seconds) is int and 1 <= seconds <= MAX_SECONDS and type(memory_mib) is int and 4096 <= memory_mib <= 1 << 20, "finite caller-selected whole-replay resources")
    binary, watchdog = Path(binary).resolve(strict=True), Path(watchdog).resolve(strict=True)
    require(binary.is_file() and os.access(binary, os.X_OK) and watchdog.is_file(), "supplied actual native binary/repository watchdog")
    destination = Path(output).absolute()
    require(destination.parent.resolve(strict=True) == destination.parent and not destination.exists(), "fresh owned output parent")
    now = time.time_ns(); deadline = now+seconds*1_000_000_000 if deadline_unix_nanos is None else deadline_unix_nanos
    require(type(deadline) is int and now < deadline <= now+seconds*1_000_000_000, "same admitted original absolute deadline")
    require(deadline <= (1 << 63)-1 and MAX_TRIALS*seconds*1_000_000_000 <= (1 << 63)-1, "checked fixed deadline/cumulative trial time")
    destination.mkdir(mode=0o700)
    binary_sha = digest(binary)
    source_hashes = composed_sources(watchdog) if profile == "composed" else None
    plan = dict(version=1, recipe=recipe, binarySHA256=binary_sha, watchdogSHA256=digest(watchdog),
                                       seconds=seconds, deadlineUnixNanos=deadline, aggregateRSSMiB=memory_mib,
                                       maxTrials=MAX_TRIALS, maxDiskBytes=MAX_TRIALS*MAX_TRIAL_BYTES)
    if profile == "composed": plan.update(version=2, profile=checker.PROFILE, sourceSHA256=source_hashes)
    if campaign is not None:
        plan.update(version=3,campaign=campaign,
                    cumulativeAdmission=dict(trials=MAX_TRIALS,engineCreations=6*MAX_TRIALS,observedProcessIdentities=128*MAX_TRIALS,
                                             actions=8*MAX_TRIALS,ticks=16*MAX_TRIALS,duplicates=4*MAX_TRIALS,
                                             trialFileBytes=CAMPAIGN_TRIAL_BYTES,maxFileBytes=MAX_TRIALS*MAX_TRIAL_BYTES,
                                             controllerTerminalBytes=2*65536,ownedNames=8192*MAX_TRIALS+2))
    write(destination/"plan.json", plan)
    receipts = []
    def invoke(value, number):
        require(number == len(receipts) and len(receipts) < MAX_TRIALS, "monotone whole-query trial accounting")
        if campaign is not None and len(result["schedule"])==number:
            result["schedule"].append(dict(trial=number,stage="semantic-action-deletion",recipeSHA256=hashlib.sha256(canonical(value).encode()).hexdigest()))
        receipt = trial(destination, number, value, binary, binary_sha, watchdog, seconds, deadline, memory_mib, profile=profile, source_hashes=source_hashes,
                        **({"campaign":True} if campaign is not None else {}))
        if campaign is not None:
            # The terminal trial receipt is itself charged. Later finalizer
            # refusal overrides its captured verdict in the owning campaign;
            # the original receipt/hash stays preserved, never rewritten.
            campaign_trial_finalizer(receipt,destination/f"trial-{number:02d}",deadline)
        receipts.append(receipt)
        return receipt
    result = dict(version=1, planSHA256=digest(destination/"plan.json"), receipts=[], reproduced=False, minimization=None,
                  scope="fixed recorded real six-child replay; semantic-failure-only bounded deletion; source-only until executed qualification")
    if profile == "composed": result.update(version=2, profile=checker.PROFILE)
    if campaign is not None:
        result.update(version=3,classification="source-refused",schedule=[],enumerationComplete=False,enumerationPassed=False,
                      recordedSeedsAttempted=0,recordedSeedsPassed=0,
                      scope="finite actual six-order enumeration before recorded seeded fresh-cohort replay; one original deadline/trial/work/file pool and independent full raw oracle. Only reproduced SAME semantic failures admit deletion; reported minimality is deletion1-minimal only, not global minimum, sustained soak or B/C/D qualification")
    try:
        if campaign is not None:
            campaign_sequence(result,campaign,invoke,receipts,checker)
            return result
        first = invoke(recipe, 0)
        # Replaying the recorded exact schedule always uses a NEW actual child
        # cohort and original factory; no captured proposal is resubmitted as
        # authority to a fake/reconstructed Host.
        again = invoke(recipe, 1)
        code = semantic_code(first)
        if code and semantic_code(again) == code:
            result["reproduced"] = True
            result["minimization"] = minimize(recipe, invoke, code, validator=checker.recipe)
        return result
    except (OSError,ValueError,KeyError,TypeError) as failure:
        if campaign is None: raise
        result.update(classification="source-refused",diagnostic=str(failure)[:4096],minimization=None)
        return result
    finally:
        result["receipts"] = [{"trial": n, "receiptSHA256": digest(destination/f"trial-{n:02d}"/"receipt.json"),
                               "classification": row["classification"], "artifactSHA256": row["artifactSHA256"]} for n, row in enumerate(receipts)]
        if profile == "composed" and campaign is None:
            first_two = [row["classification"] for row in receipts[:2]]
            result["classification"] = ("passed" if first_two == ["passed", "passed"] else
                                        "semantic-failure" if result["reproduced"] else
                                        "semantic-failure-unreproduced" if "semantic-failure" in first_two else "source-refused")
        if campaign is not None:
            result["receipts"]=[dict(row, capturedClassification=receipts[n].get("capturedClassification"),
                                      finalizationRefusal=receipts[n].get("finalizationRefusal"),finalizedDisk=receipts[n].get("finalizedDisk"))
                                for n,row in enumerate(result["receipts"])]
            result["trialsUsed"]=len(receipts)
            result["observedFileBytes"]=sum(max((r.get("finalizedDisk") or {}).get("bytes",0),(r.get("finalizedDisk") or {}).get("allocatedFileBytes",0)) for r in receipts)
            result["unmeasuredTrials"]=[n for n,r in enumerate(receipts) if r.get("finalizedDisk") is None]
            result["chargedFileBytes"]=result["observedFileBytes"]+len(result["unmeasuredTrials"])*CAMPAIGN_TRIAL_BYTES
            try:
                require(time.time_ns()<deadline and composed_sources(watchdog)==source_hashes and result["chargedFileBytes"]+2*65536<=MAX_TRIALS*MAX_TRIAL_BYTES,
                        "whole-campaign source/deadline/cumulative finalizer refusal")
                require(not result["unmeasuredTrials"],"failed final catalog measurement is missing evidence, never zero-cost or NA")
                require(result["classification"]!="passed" or result["enumerationPassed"] and result["recordedSeedsPassed"]==len(campaign["Recorded"]),
                        "missing causal/seed adapter or evidence is not a passing campaign")
            except (OSError,ValueError) as failure:
                result.update(classification="source-refused",diagnostic=str(failure)[:4096],minimization=None)
        write(destination/"result.json", result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--watchdog", type=Path, default=WATCHDOG)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--seconds", required=True, type=int)
    parser.add_argument("--memory-mib", required=True, type=int)
    parser.add_argument("--seed", required=True, type=int)
    parser.add_argument("--deadline-unix-nanos", type=int)
    args = parser.parse_args()
    print(json.dumps(execute(args.output, args.binary, args.watchdog, args.seconds, args.memory_mib, seeded_recipe(args.seed), deadline_unix_nanos=args.deadline_unix_nanos), indent=2))


if __name__ == "__main__":
    main()
