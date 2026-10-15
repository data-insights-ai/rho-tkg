#!/usr/bin/env python3
"""Routine v5 CI: explicit affected packages/parents, never release acceptance."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
MODULES = {
    ".": {"module": ".", "go_mod": "go.mod", "config": ".golangci.yml", "only_new": True},
    "v5": {"module": "v5", "go_mod": "v5/go.mod", "config": "../.golangci.yml", "only_new": False},
}
ROOT_LARGE = {
    "pkg/graph": "graph", "pkg/graph/internal/core": "core",
    "pkg/graph/store/badger": "badger", "pkg/graph/store/memory": "memory",
    "pkg/graph/store/tiered": "tiered",
}
MODES = ("normal", "race", "coverage")
# Reviewed integration guards, including the old A contract which caught D's
# cancellation regression. They supplement changed/companion tests, not replace
# them. Every name is checked against the current compiled inventory.
GRAPH_COMPAT = [
    "TestGraphReplacementAdmissionNativeSurvivingQuorumAllAgreements",
    "TestGraphReplacementActivationNativeHistoryAllocatorAndLifetime",
    "TestGraphReplacementRevocationFencesHostAndSourceBeforeAsyncClose",
    "TestPartitionWritesBothGroupsPreserveTypedAuthorityHistoryAndReopen",
    "TestCertifiedGraphFollowerBarrierReopenAndHeldCutHistory",
    "TestCertifiedGraphActualAS5ActivationKeepsHeldCertifiedSourceAndReopensCut",
    "TestRangeMovePartialActivationRestoresOriginalCommitAndOldCertifiedCut",
    "TestRangeMoveCompletedHandoffCopiesNativeCrossDecisionsAndDedupWithKeeperOffline",
    "TestGraphSourceDirectCloseCancelsPausedNativeData",
]
PARENTS = re.compile(r"^\s*func\s+((?:Test|Fuzz)\w+)\s*\(", re.MULTILINE)
DEFINITIONS = re.compile(r"^(func(?:\s+\([^\n]*\))?|type|var|const)\s+(\w+)", re.MULTILINE)
# Shared cost fixtures feed both ordinary and refill paths. Their reviewed finite
# scope excludes the env-gated large cell; an explicitly edited parent is still
# selected below and retains its independent short-mode guard.
COST_FIXTURES = {
    "native_graph_cost_test.go", "native_graph_cost_harness_test.go",
    "native_graph_cost_grants_harness_test.go", "native_graph_cost_reopen_harness_test.go",
    "native_graph_cost_incident_harness_test.go",
    "native_graph_cost_columns_harness_test.go",
    "native_graph_cost_parallel_harness_test.go", "native_graph_cost_runner_test.go",
}
COST_PARENTS = [
    "TestNativeGraphCostSmallActualProcesses", "TestNativeGraphCostProfileAndRouting",
    "TestNativeGraphCostAckPollMarkerRequiresCostChildAndKeepsDefaultDiagnostics",
    "TestNativeGraphCostMillionFactProfileUsesBoundedGrantBlocks",
    "TestNativeGraphCostMultipleGrantsSmallActualProcesses",
    "TestNativeGraphCostPhysicalReopenAndIndexesSmallActualProcesses",
    "TestNativeGraphCostReopenIndexPagesRejectMalformedAndOversizedInput",
    "TestNativeGraphCostIncidentOwnerRetainsOldCutAndRefusesExhaustedLateCookies",
    "TestNativeGraphCostIncidentEnvelopeRejectsUnownedSequenceAndExtraInput",
    "TestNativeGraphCostIndependentClientsSmallActualProcesses",
    "TestNativeGraphCostRequestACKFindsOriginalBehindLaterAppliedEntry",
    "TestNativeGraphCostAttemptLineageAndWindowAreBoundedAndRequestJoined",
    "TestNativeGraphCostThirtyTwoCommandWindowRejectsOtherWaveOriginals",
    "TestNativeColumnsPublishResumeCacheAndRetainedCut",
    "TestNativePartitionColumnsBindDisjointGenuinePrefixesAndRetainedReadCursor",
    "TestNativeColumnRequiredCostProfilesPrepayEveryFinitePass",
    "TestNativeColumnsDurableGenerationOwnsExactPinAcrossReopenAndRetirement",
    "TestNativeColumnIssuanceAndCancellationPreserveInterruptedCreation",
]
FAULT_PARENTS = [
    "TestNativeStorageTransitionSelectorsKeepGuaranteesAndFiniteOwnership",
    "TestNativeStorageTransitionWALBeforeAfterRecoversSameOriginalAndOldCut",
    "TestNativeComposedFaultQueuedCapacityDecisionsKeepFirstOriginal",
    "TestNativeDecisionSubmissionSeamChecksContextAfterCallbackWithoutHostLock",
    "TestNativeDecisionOwnerCloseRetiresActualCandidateWithoutImportingWire",
    "TestNativeComposedFaultPartialInstallSurvivesDecisionLeaderLossAndDelayedResolve",
    "TestNativeFaultChildFilesystemRecordsOnlyActualMatchedOperations",
    "TestNativeFaultHeldAndDuplicatePacketsStayWithinOriginalOwnerBudget",
    "TestNativeComposedFaultActualWALFailureRecoversOriginalDecision",
    "TestNativeComposedFaultDirectorySyncFailsGenuineSameDirectoryReopen",
    "TestNativeComposedFaultActualPacketsPreserveWholeTransactionAndOldCut",
    "TestNativeComposedFaultFrozenConflictKeepsOriginalRefusalThroughRecovery",
    "TestNativeFaultFamilyTornWALRecoversActualOriginalFromDiskAndSyncedImage",
    "TestNativeFaultFamilyTwoConflictingOriginalsFenceAndRealTransferExploreCausalOrders",
    "TestNativeFaultFamilySnapshotInterruptionAndAtomicKVPublicationRecoverWholeCut",
    "TestNativeFaultFamilyChildClockExpiryAndRollbackKeepOldAuthorityDead",
    "TestNativeFaultFamilyCorruptedDurableNativeRecordsRefuseBeforeReady",
    "TestNativeFaultFamilyOldLeaderPacketStaysChargedAcrossGenuineElection",
    "TestNativeFaultFamilyDirectedLossAndUnequalTicksPreserveOriginalAndOldCut",
    "TestNativeFaultFamilyOptionsPublicationRenameFailsActualReopen",
    "TestNativeFaultFamilyLeaseClockCannotExtendOrReviveNativeOwner",
    "TestNativeDurableImageUsesSyncedIdentityAndNamesInsteadOfCurrentDisk",
    "TestNativeDurableImageRefusesForeignCorruptAndUnfundedRecovery",
]
WORKLOAD_PARENTS = [
    "TestNativeContentionRequiredProfileAndWholeGenerationGrantAreFinite",
    "TestNativeContentionColumnDependencyPrepassSurvivesExpiryAndReopen",
    "TestNativeWorkloadTransportOwnsFiniteQueueAndCancelsHeldFrames",
    "TestNativeWorkloadTransportRejectsUnfundedProfilesAndOrdersReadyFrames",
]
FIXTURE_COMPAT = {
    **dict.fromkeys(COST_FIXTURES, COST_PARENTS),
    "native_graph_cost_columns_harness_test.go": [*COST_PARENTS, *WORKLOAD_PARENTS],
    "native_graph_contention_harness_test.go": WORKLOAD_PARENTS,
    "native_graph_contention_runner_test.go": WORKLOAD_PARENTS,
    "native_graph_workload_transport_harness_test.go": WORKLOAD_PARENTS,
    "paired_graph_fault_harness_test.go": ["TestNativePairedGraphFaultSixOperationsActualProcessesAndRetainedCuts", "TestNativePairedGraphFaultNoOpUsesActualRetainedNormalEntry"],
    "paired_graph_snapshot_harness_test.go": ["TestNativePairedGraphFaultSixOperationsActualProcessesAndRetainedCuts"],
    "prepared_lock_p06_harness_test.go": ["TestPreparedLockP06FirstYESLeaderKillOriginalRecovery"],
    "coordinator_loss_p03_harness_test.go": ["TestNativeCoordinatorLeaderLossAfterDurableCommitBeforeCallerReply"],
    "prepare_fence_p02_harness_test.go": ["TestPrepareFenceP02ActualOrderingAcrossElection"],
    "native_graph_fault_schedule_harness_test.go": FAULT_PARENTS,
    "native_graph_fault_decision_harness_test.go": FAULT_PARENTS,
    "native_graph_fault_family_harness_test.go": FAULT_PARENTS,
    "native_graph_fault_durable_fs_test.go": FAULT_PARENTS,
    "native_graph_transition_harness_test.go": FAULT_PARENTS,
    "native_graph_columns_retention_test.go": ["TestNativeColumnsDurableGenerationOwnsExactPinAcrossReopenAndRetirement", "TestNativeColumnIssuanceAndCancellationPreserveInterruptedCreation"],
    "native_graph_columns_process_harness_test.go": ["TestNativeColumnsDurableOSChildCreateReopenAndRetire"],
    "replicated_process_test.go": ["TestReplicatedProcessProtocolBoundaries", "TestReplicatedProcessShutdownReapsFailedCloseRPC", *FAULT_PARENTS, *WORKLOAD_PARENTS],
    "graph_source_process_test.go": ["TestGraphSourceProcessEnvelopeRejectsUnboundAndExtraInput", "TestGraphSourceProcessObservationBatchBoundsOrderAndOwnedFacts", *COST_PARENTS, *FAULT_PARENTS, *WORKLOAD_PARENTS],
}
WORKLOAD_INPUTS = {"tools/v5-substrate/native_contention_check.py", "tools/v5-substrate/native_contention_check_test.py",
                   "tools/v5-substrate/native_workload_cell.py", "tools/v5-substrate/native_workload_cell_test.py",
                   "tools/v5-substrate/workload_matrix.py", "tools/v5-substrate/workload_matrix_test.py"}
FAULT_INPUTS = {"tools/v5-substrate/native_fault_schedule_check.py", "tools/v5-substrate/native_fault_schedule_check_test.py",
                "tools/v5-substrate/native_transition_check.py", "tools/v5-substrate/native_transition_check_test.py",
                "tools/v5-substrate/native_fault_family_check.py", "tools/v5-substrate/native_fault_family_check_test.py",
                "tools/v5-substrate/check-p06-history.py", "docs/v5/reference/independent/protocol/oracle.py"}
# The complete-CDC replay tests consume these two frozen FDB archives directly.
# Keep this finite dependency list in sync with compare_cost_cdc_test.py.
COST_CDC_INPUTS = {
    "tools/v5-substrate/compare-cost-cdc.py",
    "tools/v5-substrate/cost-cell.py",
    "tools/v5-substrate/compare_cost_cdc_test.py",
    "tools/v5-substrate/native_columns_check.py",
    "tools/v5-substrate/cost_cell_test.py",
    "tools/v5-substrate/fdb_retention_check.py",
    "tools/v5-substrate/fdb_retention_check_test.py",
    "tasks/evidence/v2-fdb-cost-profile-20261013/raw.tar.gz",
    "tasks/evidence/v2-fdb-cost-final-source-20261013/raw.tar.gz",
}


def command(args, cwd=None):
    return subprocess.check_output(args, cwd=ROOT if cwd is None else cwd, text=True)


def fail(message):
    raise ValueError(message + "; use workflow_dispatch full_validation=true for explicit full scope")


def changed_paths(base, head):
    # --no-renames deliberately reports BOTH sides of a rename/deletion.
    for ref in (base, head):
        command(["git", "rev-parse", "--verify", ref + "^{commit}"])
    return sorted(set(command(["git", "diff", "--no-renames", "--name-only", "-z", base, head]).split("\0")) - {""})


def plan(paths, full=False):
    if full:
        return {"full": True, "paths": paths, "matrix": {"include": list(MODULES.values())}, "root_behavior": False, "comparison": True, "migration": True,
                "external": any(p.startswith("tools/v5-substrate/fdb/") and "/evidence/" not in p and not p.endswith(".md") for p in paths)}
    if any(p in ("go.mod", "go.sum", "v5/go.mod", "v5/go.sum") for p in paths):
        fail("Dependency/toolchain changes require reviewed full scope")
    modules = set()
    comparison = migration = external = root_behavior = False
    for path in paths:
        p = Path(path)
        if path.startswith("tools/graph-compare/"):
            comparison |= p.suffix != ".md" and "/evidence/" not in path
        elif path.startswith("tools/v5-migrate/"):
            migration |= p.suffix != ".md" and "/evidence/" not in path
        elif path.startswith("tools/v5-substrate/fdb/"):
            external |= p.suffix != ".md" and "/evidence/" not in path
        elif path.endswith(".go") or path in ("go.mod", "go.sum", "v5/go.mod", "v5/go.sum") or "testdata" in p.parts:
            if path.startswith("tools/"):
                fail("No routine Go policy for external tool module: " + path)
            module = "v5" if path.startswith("v5/") else "."
            modules.add(module)
            root_behavior |= module == "."
            if module == "." and path.startswith("pkg/") and not path.endswith("_test.go"):
                migration = True  # local replacement selects the current v4 reader
            if module == "v5" and path.startswith("v5/pkg/") and not path.endswith("_test.go"):
                migration = True
        elif path in ("Makefile", "v5/Makefile", ".golangci.yml", ".github/workflows/ci.yml", "scripts/test-race.sh"):
            modules.update(MODULES)  # static/build checks; no unchanged root suites
    return {"full": full, "paths": paths, "matrix": {"include": [MODULES[m] for m in sorted(modules)]},
            "root_behavior": root_behavior, "comparison": comparison, "migration": migration, "external": external}


def json_objects(text):
    decoder = json.JSONDecoder()
    while text.strip():
        value, end = decoder.raw_decode(text.lstrip())
        yield value
        text = text.lstrip()[end:]


def affected_packages(packages, seeds, all_packages=False):
    known = {p["ImportPath"]: p for p in packages}
    affected = set(known) if all_packages else set(seeds)
    while True:
        added = {name for name, p in known.items()
                 if set(p.get("Imports", []) + p.get("TestImports", []) + p.get("XTestImports", [])) & affected}
        if added <= affected:
            return sorted(affected & known.keys())
        affected |= added


def parent_names(path):
    return set(PARENTS.findall(path.read_text())) if path.is_file() else set()


def changed_definitions(path, base):
    text = path.read_text()
    definitions = list(DEFINITIONS.finditer(text))
    spans = [(m[1], m[2], text.count("\n", 0, m.start()) + 1,
              text.count("\n", 0, definitions[i + 1].start()) if i + 1 < len(definitions) else text.count("\n") + 1)
             for i, m in enumerate(definitions)]
    diff = command(["git", "diff", "-U0", base, "--", str(path.relative_to(ROOT))])
    ranges = [(int(a), int(a) + max(int(n or 1), 1) - 1)
              for a, n in re.findall(r"@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@", diff)]
    return [(kind, name) for kind, name, first, last in spans if any(a <= last and b >= first for a, b in ranges)]


def graph_selection(directory, changed, definitions):
    tests = set()
    changed_tests = [p for p in changed if p.name.endswith("_test.go") and p.is_file()]
    for path in changed_tests:
        edited = definitions[path]
        own = {name for _, name in edited if name in parent_names(path)}
        tests |= own
        # Only actually edited top-level helper functions get consumer lookup.
        # Shared types/methods require a reviewed finite fixture guard list.
        helpers = {name for kind, name in edited if kind == "func" and name not in parent_names(path)}
        if helpers and (path.name in COST_FIXTURES or path.name in ("graph_source_process_test.go", "native_graph_fault_schedule_harness_test.go", "native_graph_fault_decision_harness_test.go", "native_graph_fault_family_harness_test.go", "native_graph_fault_durable_fs_test.go", "native_graph_transition_harness_test.go", "native_graph_contention_harness_test.go", "native_graph_contention_runner_test.go", "native_graph_workload_transport_harness_test.go")):
            tests |= set(FIXTURE_COMPAT[path.name])
        else:
            for candidate in directory.glob("*_test.go"):
                if helpers and any(re.search(r"\b" + re.escape(name) + r"\b", candidate.read_text()) for name in helpers):
                    tests |= parent_names(candidate)
        unknown = [(kind, name) for kind, name in edited if kind != "func"]
        if unknown and not own:
            if path.name not in FIXTURE_COMPAT:
                fail("Unknown changed graphapply fixture scope: " + path.name)
            tests |= set(FIXTURE_COMPAT[path.name])
    changed_behavior = bool(tests)
    if changed_tests and not changed_behavior:
        fail("Changed graphapply fixture has no runnable parents or named consumers")
    if any("testdata" in p.parts for p in changed) and not changed_behavior:
        fail("Changed graphapply test inputs need explicit consumer test parents")
    for path in changed:
        if path.suffix != ".go" or path.name.endswith("_test.go"):
            continue
        companions = list(directory.glob(path.stem + "_test.go")) + list(directory.glob(path.stem + "_*_test.go"))
        names = set().union(*(parent_names(p) for p in companions))
        if not names and not changed_behavior:
            fail("Missing changed/companion behavior selectors for " + str(path))
        tests |= names
    return sorted(tests | set(GRAPH_COMPAT))


def validate_inventory(names, text, package):
    lines = [x for x in text.splitlines() if x]
    compiled = {x for x in lines if re.fullmatch(r"(?:Test|Fuzz|Example)\w+", x)}
    footers = [x for x in lines if x.startswith("ok\t") or x.startswith("ok ")]
    if len(footers) != 1 or footers[0].split()[1] != package:
        fail("Invalid compiled inventory footer for " + package)
    missing = set(names) - compiled
    if missing:
        fail("Required compiled selectors missing: " + ", ".join(sorted(missing)))


def execute(module, paths, output, base):
    if module == ".":
        fail("Root behavior requires the frozen root-plan/root-run partition gate")
    folder = ROOT / module
    packages = list(json_objects(command(["go", "list", "-json", "./..."], folder)))
    prefix = re.search(r"^module\s+(\S+)", (folder / "go.mod").read_text(), re.MULTILINE)[1]
    local = [Path(p).relative_to(module) if module != "." else Path(p) for p in paths
             if (p.startswith("v5/") if module == "v5" else not p.startswith(("v5/", "tools/", "docs/", "tasks/")))]
    seeds = set()
    for path in local:
        if path.suffix == ".go":
            seeds.add(prefix + ("/" + path.parent.as_posix() if path.parent != Path(".") else ""))
        elif "testdata" in path.parts:
            parent = Path(*path.parts[:path.parts.index("testdata")])
            seeds.add(prefix + ("/" + str(parent) if parent != Path(".") else ""))
    affected = affected_packages(packages, seeds, any(str(p) in ("go.mod", "go.sum") for p in local))
    graph = prefix + "/internal/graphapply" if module == "v5" else None
    selections = []
    small = [p for p in affected if p != graph]
    if small:
        selections.append({"packages": small, "parents": None})
    if graph in affected:
        directory = folder / "internal/graphapply"
        changed = [folder / p for p in local if "internal/graphapply" in p.as_posix()]
        definitions = {p: changed_definitions(p, base) for p in changed if p.name.endswith("_test.go") and p.is_file()}
        names = graph_selection(directory, changed, definitions)
        inventory = command(["go", "test", "-short", "-list", "^(Test|Fuzz|Example)", graph], folder)
        validate_inventory(names, inventory, graph)
        selections.append({"packages": [graph], "parents": names})
    output.mkdir(parents=True, exist_ok=True)
    (output / "selection.json").write_text(json.dumps({"module": module, "paths": paths, "selections": selections}, indent=2) + "\n")
    if not selections:
        print("No Go behavior changed in this module; static/build gates only.")
    # Identical selection once in each mode. Whole parents preserve subtests;
    # subprocess parents keep their independently compiled child entry points.
    for mode in ("normal", "race", "coverage"):
        for index, selected in enumerate(selections):
            args = ["go", "test", "-short", "-count=1", "-json", "-timeout=10m"]
            if mode == "race":
                args.append("-race")
            if mode == "coverage":
                args += ["-covermode=atomic", "-coverprofile=" + str(output / ("coverage-" + str(index) + ".out"))]
            if selected["parents"]:
                args += ["-run", "^(" + "|".join(re.escape(n) for n in selected["parents"]) + ")$"]
            args += selected["packages"]
            print("Focused", module, mode, " ".join(args), flush=True)
            with (output / (mode + "-" + str(index) + ".jsonl")).open("w") as log:
                subprocess.run(args, cwd=folder, stdout=log, check=True)
    print("Focused checks passed; scoped coverage is diagnostic, not the full-module/release coverage gate.")


def compiled_root_names(package):
    # Same race/platform build constraints and sorted whole-parent assignment as
    # the full race gate. -list compiles but does not run the tests.
    text = command(["go", "test", "-short", "-race", "-count=1", "-list", "^(Test|Fuzz|Example)", package])
    names = [line for line in text.splitlines() if re.fullmatch(r"(?:Test|Fuzz|Example)\w+", line)]
    validate_inventory(names, text, package)
    other = [line for line in text.splitlines() if line and line not in names and not line.startswith(("ok\t", "ok "))]
    if other or len(names) != len(set(names)) or len(names) < 4:
        fail("Incomplete/duplicate root compiled inventory")
    return sorted(names)


def root_inputs(head):
    if command(["git", "rev-parse", "HEAD"]).strip() != head:
        fail("Root plan checkout does not match its head")
    # Include untracked inputs too: a newly injected Go/testdata file cannot
    # silently change compilation after the plan was frozen.
    paths = command(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"]).split("\0")
    selected = [p for p in set(paths) if p and not p.startswith(("v5/", "tools/")) and
                (p.endswith(".go") or "testdata" in Path(p).parts or
                 p in ("go.mod", "go.sum", "Makefile", "scripts/ci-changes.py", ".github/workflows/ci.yml"))]
    return {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in sorted(selected)}


def root_partitions(affected, core, inventory):
    partitions = []
    large = {core.rsplit("/pkg/", 1)[0] + "/" + path: name for path, name in ROOT_LARGE.items()}
    other = [p for p in affected if p not in large]
    if other:
        partitions.append({"part": "other", "packages": other, "parents": None})
    for package, name in sorted(large.items()):
        if package not in affected:
            continue
        if name == "core":
            if len(inventory) < 4 or inventory != sorted(set(inventory)):
                fail("Root core needs a complete unique compiled inventory")
            for index in range(4):
                partitions.append({"part": "core-" + str(index + 1), "packages": [core], "parents": inventory[index::4]})
        else:
            partitions.append({"part": name, "packages": [package], "parents": None})
    return partitions


def prepare_root(paths, base, head, output):
    inputs = root_inputs(head)
    packages = list(json_objects(command(["go", "list", "-race", "-json", "./..."])))
    prefix = re.search(r"^module\s+(\S+)", (ROOT / "go.mod").read_text(), re.MULTILINE)[1]
    seeds = set()
    for name in paths:
        path = Path(name)
        if name.startswith(("v5/", "tools/", "docs/", "tasks/")):
            continue
        if path.suffix == ".go" or "testdata" in path.parts:
            parent = Path(*path.parts[:path.parts.index("testdata")]) if "testdata" in path.parts else path.parent
            seeds.add(prefix + ("/" + parent.as_posix() if parent != Path(".") else ""))
    known = [p["ImportPath"] for p in packages]
    if len(known) != len(set(known)) or not seeds <= set(known):
        fail("Unknown/duplicate affected root package scope")
    affected = affected_packages(packages, seeds)
    core = prefix + "/pkg/graph/internal/core"
    inventory = compiled_root_names(core) if core in affected else []
    data = {"version": 1, "head": head, "base": base, "paths": paths, "inputs": inputs,
            "affected": affected, "core": core, "inventory": inventory, "modes": list(MODES),
            "no_test_packages": sorted(p["ImportPath"] for p in packages if p["ImportPath"] in affected
                                       and not p.get("TestGoFiles") and not p.get("XTestGoFiles")),
            "core_scope": "exhaustive whole parents; no reviewed narrow root production selector",
            "partitions": root_partitions(affected, core, inventory)}
    if root_inputs(head) != inputs:
        fail("Root source changed while preparing its plan")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(data, indent=2) + "\n")
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    matrix = {"include": [{"part": p["part"]} for p in data["partitions"]]}
    if os.environ.get("GITHUB_OUTPUT"):
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as f:
            f.write("matrix=" + json.dumps(matrix, separators=(",", ":")) + "\nplan_sha=" + digest + "\n")
    print(json.dumps({"matrix": matrix, "plan_sha": digest}, indent=2))
    return data


def run_root(plan_path, digest, part, output):
    raw = plan_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != digest:
        fail("Frozen root plan digest mismatch")
    data = json.loads(raw)
    if data["version"] != 1 or data["modes"] != list(MODES) or root_inputs(data["head"]) != data["inputs"]:
        fail("Frozen root plan source/modes mismatch")
    if data["affected"] != sorted(set(data["affected"])) or data["partitions"] != root_partitions(data["affected"], data["core"], data["inventory"]):
        fail("Root plan omits/duplicates packages or compiled parents")
    selected = [p for p in data["partitions"] if p["part"] == part]
    if len(selected) != 1:
        fail("Unknown root partition: " + part)
    selected = selected[0]
    if selected["parents"] and compiled_root_names(data["core"]) != data["inventory"]:
        fail("Root compiled inventory changed after planning")
    output.mkdir(parents=True, exist_ok=True)
    (output / "selection.json").write_text(json.dumps({"plan_sha256": digest, "head": data["head"], "part": part,
                                                     "selection": selected, "modes": list(MODES)}, indent=2) + "\n")
    for mode in MODES:
        args = ["go", "test", "-short", "-count=1", "-json", "-timeout=10m"]
        if mode == "race":
            args.append("-race")
        if mode == "coverage":
            args += ["-covermode=atomic", "-coverprofile=" + str(output / "coverage.out")]
        if selected["parents"]:
            args += ["-run", "^(" + "|".join(re.escape(n) for n in selected["parents"]) + ")$"]
        args += selected["packages"]
        print("Focused root", part, mode, " ".join(args), flush=True)
        with (output / (mode + ".jsonl")).open("w") as log:
            subprocess.run(args, cwd=ROOT, stdout=log, check=True)
        # A successful invocation with no matching parents is not qualification.
        package_terminals, terminals, skipped = {}, set(), set()
        with (output / (mode + ".jsonl")).open() as log:
            for line in log:
                e = json.loads(line)
                if e["Action"] in ("pass", "skip") and "Test" not in e:
                    package_terminals[e.get("Package")] = e["Action"]
                if e["Action"] in ("pass", "skip") and "Test" in e and e.get("Package") in selected["packages"]:
                    terminals.add(e.get("Test"))
                    if e["Action"] == "skip":
                        skipped.add(e["Test"])
        no_tests = set(data["no_test_packages"])
        bad_packages = any(status != "pass" and package not in no_tests for package, status in package_terminals.items())
        if set(package_terminals) != set(selected["packages"]) or bad_packages or not set(selected["parents"] or []) <= terminals:
            fail("Root partition has missing package/parent terminals in " + mode)
        if root_inputs(data["head"]) != data["inputs"]:
            fail("Root source changed during " + mode)
        (output / (mode + "-summary.json")).write_text(json.dumps({"mode": mode, "package_terminals": package_terminals,
            "selected_parents": selected["parents"], "skipped_tests": sorted(skipped), "scope": "focused partition; short-mode skips are not executed proof"}, indent=2) + "\n")
    print("Focused root partition passed; its single scoped coverage profile is diagnostic, not full/release coverage.")


def external_qualification(paths):
    """FDB needs its pinned native headers/runtime; this job does NOT run Go."""
    folder = ROOT / "tools/v5-substrate/fdb"
    changed = [ROOT / p for p in paths if p.startswith("tools/v5-substrate/fdb/") and "/evidence/" not in p and not p.endswith(".md")]
    required = {}
    for source in changed:
        relative = source.relative_to(folder).as_posix()
        if not source.is_file():
            raise ValueError("FDB source deletion needs its specific external requalification: " + relative)
        required[relative] = hashlib.sha256(source.read_bytes()).hexdigest()
    match = None
    for receipt in sorted((folder / "evidence").glob("*.json")):
        data = json.loads(receipt.read_text())
        gates = data.get("gates", {})
        terminal = data.get("runs", {}).get("passing_normal_race", {})
        if terminal.get("exit") != 0 or terminal.get("cleanupExit") != 0:
            continue
        if not all(isinstance(gates.get(mode), dict) and gates[mode].get("passedParents", 0) > 0 and gates[mode].get("failedParents") == 0 for mode in ("normal", "race")):
            continue
        hashes = data.get("finalSourceSHA256", data.get("source_sha256", {}))
        if all(hashes.get(relative) == digest for relative, digest in required.items()):
            match = receipt
            break
    if required and match is None:
        raise ValueError("FDB native-header module requires one coherent recorded normal/race qualification with terminal exit/cleanup 0 for all changed files: " +
                         ", ".join(required) + "; run its Docker/native gate and record exact hashes. full_validation does not build this external module")
    for relative, digest in required.items():
        print("FDB recorded-source check:", relative, digest, match.relative_to(ROOT))
    print("Recorded external source check only; no FDB execution, new fault/cost or engine acceptance claimed.")


def docs(paths):
    for path in paths:
        p = ROOT / path
        if p.is_file() and p.suffix == ".json":
            json.loads(p.read_text())
    if any(p.startswith("docs/v5/reference/") or p == "docs/v5/PLAN.md" for p in paths):
        subprocess.run([sys.executable, "-B", "docs/v5/reference/independent/check.py"], cwd=ROOT, check=True)
    cdc(paths)


def cdc(paths):
    if COST_CDC_INPUTS.intersection(paths):
        subprocess.run([sys.executable, "-B", "tools/v5-substrate/compare_cost_cdc_test.py"], cwd=ROOT, check=True)
        if {"tools/v5-substrate/cost-cell.py", "tools/v5-substrate/native_columns_check.py", "tools/v5-substrate/cost_cell_test.py"}.intersection(paths):
            subprocess.run([sys.executable, "-B", "tools/v5-substrate/cost_cell_test.py"], cwd=ROOT, check=True)
        if {"tools/v5-substrate/compare-cost-cdc.py", "tools/v5-substrate/fdb_retention_check.py", "tools/v5-substrate/fdb_retention_check_test.py"}.intersection(paths):
            subprocess.run([sys.executable, "-B", "tools/v5-substrate/fdb_retention_check_test.py"], cwd=ROOT, check=True)
    if FAULT_INPUTS.intersection(paths):
        subprocess.run([sys.executable, "-B", "tools/v5-substrate/native_fault_schedule_check_test.py"], cwd=ROOT, check=True)
        subprocess.run([sys.executable, "-B", "tools/v5-substrate/native_fault_family_check_test.py"], cwd=ROOT, check=True)
        subprocess.run([sys.executable, "-B", "tools/v5-substrate/native_transition_check_test.py"], cwd=ROOT, check=True)
    if WORKLOAD_INPUTS.intersection(paths):
        subprocess.run([sys.executable, "-B", "tools/v5-substrate/native_contention_check_test.py"], cwd=ROOT, check=True)
        subprocess.run([sys.executable, "-B", "tools/v5-substrate/native_workload_cell_test.py"], cwd=ROOT, check=True)
        subprocess.run([sys.executable, "-B", "tools/v5-substrate/workload_matrix_test.py"], cwd=ROOT, check=True)


def focused_dispatch_base(event, kind, head):
    """Structured manual input only; automatic push/PR scope is unchanged."""
    base = event.get("inputs", {}).get("focused_base", "") if kind == "workflow_dispatch" else ""
    if base == "":
        return None
    if not isinstance(base, str) or not re.fullmatch(r"[0-9a-fA-F]{40}", base):
        fail("focused_base requires a full 40-hex commit SHA")
    try:
        if command(["git", "cat-file", "-t", base]).strip() != "commit":
            fail("focused_base must name a genuine commit")
        dispatched = command(["git", "rev-parse", "--verify", head + "^{commit}"]).strip()
        if base.lower() == dispatched.lower():
            fail("focused_base must differ from dispatched head")
        command(["git", "merge-base", "--is-ancestor", base, dispatched])
    except subprocess.CalledProcessError as error:
        raise ValueError("focused_base must be an existing ancestor commit of dispatched head") from error
    return base.lower()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "run", "root-plan", "root-run", "docs", "cdc", "external"))
    parser.add_argument("--base")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--module", choices=tuple(MODULES))
    parser.add_argument("--output", default="ci-focused")
    parser.add_argument("--plan-file")
    parser.add_argument("--plan-sha")
    parser.add_argument("--part")
    args = parser.parse_args()
    if args.action == "plan":
        event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text()) if os.environ.get("GITHUB_EVENT_PATH") else {}
        kind = os.environ.get("GITHUB_EVENT_NAME", "push")
        full = os.environ.get("GITHUB_REF") == "refs/heads/main" or event.get("pull_request", {}).get("base", {}).get("ref") == "main" or kind == "workflow_dispatch" and str(event.get("inputs", {}).get("full_validation", "true")).lower() == "true"
        focused_base = focused_dispatch_base(event, kind, args.head)
        base = args.base or focused_base or event.get("before") or event.get("pull_request", {}).get("base", {}).get("sha") or "HEAD^"
        if kind == "pull_request":
            base = command(["git", "merge-base", base, args.head]).strip()
        paths = changed_paths(base, args.head)
        result = plan(paths, full)
        result.update(base=base, head=args.head)
        print(json.dumps(result, indent=2))
        if os.environ.get("GITHUB_OUTPUT"):
            with Path(os.environ["GITHUB_OUTPUT"]).open("a") as f:
                for key in ("full", "matrix", "root_behavior", "comparison", "migration", "external", "base", "head"):
                    if key == "external" and key not in result:
                        result[key] = False
                    value = json.dumps(result[key], separators=(",", ":")) if key not in ("base", "head") else result[key]
                    f.write(key + "=" + value + "\n")
    elif args.action == "root-run":
        if not args.plan_file or not args.plan_sha or not args.part:
            fail("root-run needs plan-file, plan-sha and part")
        run_root(Path(args.plan_file), args.plan_sha, args.part, Path(args.output).resolve())
    else:
        paths = changed_paths(args.base, args.head)
        if args.action == "run":
            execute(args.module, paths, Path(args.output).resolve(), args.base)
        elif args.action == "root-plan":
            head = command(["git", "rev-parse", args.head]).strip()
            prepare_root(paths, args.base, head, Path(args.output).resolve())
        elif args.action == "docs":
            docs(paths)
        elif args.action == "cdc":
            cdc(paths)
        else:
            external_qualification(paths)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, subprocess.CalledProcessError) as error:
        print("Change-aware CI refused:", error, file=sys.stderr)
        sys.exit(1)
