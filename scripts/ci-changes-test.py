#!/usr/bin/env python3
"""Small dispatch/selection guards; no Go/compiler or external runtime."""
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("ci_changes", Path(__file__).with_name("ci-changes.py"))
CI = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CI)


class Planning(unittest.TestCase):
    def docs_action(self, paths):
        with patch.object(sys, "argv", ["ci-changes.py", "docs", "--base", "fixture"]), patch.object(CI, "changed_paths", return_value=paths):
            CI.main()

    def test_dispatch_defaults_and_automatic_full_policy_are_preserved(self):
        for kind, ref, event, full, base in [
            ("workflow_dispatch", "refs/heads/v5", {}, True, "HEAD^"),
            ("workflow_dispatch", "refs/heads/v5", {"inputs": {"full_validation": False}}, False, "HEAD^"),
            ("workflow_dispatch", "refs/heads/main", {"inputs": {"full_validation": False}}, True, "HEAD^"),
            ("push", "refs/heads/v5", {"before": "push-base", "inputs": {"focused_base": "invalid"}}, False, "push-base"),
            ("pull_request", "refs/pull/1/merge", {"pull_request": {"base": {"ref": "main", "sha": "pr-base"}}}, True, "merged-base"),
        ]:
            with self.subTest(kind=kind, event=event), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "event.json"
                path.write_text(json.dumps(event))
                with patch.dict(os.environ, {"GITHUB_EVENT_PATH": str(path), "GITHUB_EVENT_NAME": kind, "GITHUB_REF": ref}, clear=True), \
                     patch.object(sys, "argv", ["ci-changes.py", "plan"]), \
                     patch.object(CI, "command", return_value="merged-base"), \
                     patch.object(CI, "changed_paths", return_value=[] ) as changed, patch.object(sys, "stdout", io.StringIO()) as out:
                    CI.main()
                changed.assert_called_once_with(base, "HEAD")
                self.assertEqual(json.loads(out.getvalue())["full"], full)

    def test_docs_runs_complete_cdc_for_each_exact_input_once(self):
        inputs = ["tools/v5-substrate/compare-cost-cdc.py", "tools/v5-substrate/cost-cell.py",
                  "tools/v5-substrate/compare_cost_cdc_test.py",
                  "tasks/evidence/v2-fdb-cost-profile-20261013/raw.tar.gz",
                  "tasks/evidence/v2-fdb-cost-final-source-20261013/raw.tar.gz"]
        for paths in [[p] for p in inputs] + [inputs, [inputs[0], inputs[0]]]:
            with self.subTest(paths=paths), patch.object(CI.subprocess, "run") as run:
                self.docs_action(paths)
                run.assert_called_once_with([sys.executable, "-B", "tools/v5-substrate/compare_cost_cdc_test.py"], cwd=CI.ROOT, check=True)
                p = CI.plan(paths)
                self.assertEqual(p["matrix"]["include"], [])
                self.assertFalse(p["comparison"] or p["migration"] or p["external"])

    def test_docs_ignores_unrelated_evidence_and_preserves_reference_gate(self):
        unrelated = ["tools/v5-substrate/README.md", "tasks/evidence/v2-fdb-cost-profile-20261013/README.md",
                     "tasks/evidence/v2-fdb-cost-statics-20261013/raw.tar.gz",
                     "tools/v5-substrate/fdb/evidence/README.md"]
        with patch.object(CI.subprocess, "run") as run:
            self.docs_action(unrelated)
            run.assert_not_called()
            self.docs_action(["docs/v5/PLAN.md", "tools/v5-substrate/cost-cell.py"])
            self.assertEqual([c.args[0] for c in run.call_args_list], [
                [sys.executable, "-B", "docs/v5/reference/independent/check.py"],
                [sys.executable, "-B", "tools/v5-substrate/compare_cost_cdc_test.py"]])
            for call in run.call_args_list:
                self.assertEqual(call.kwargs, {"cwd": CI.ROOT, "check": True})

    def test_docs_propagates_complete_cdc_failure(self):
        with patch.object(CI.subprocess, "run", side_effect=subprocess.CalledProcessError(1, ["cdc-test"])):
            with self.assertRaises(subprocess.CalledProcessError):
                self.docs_action(["tools/v5-substrate/compare-cost-cdc.py"])

    def test_full_cdc_action_runs_only_its_exact_replay_without_reference_or_go(self):
        with patch.object(sys, "argv", ["ci-changes.py", "cdc", "--base", "fixture"]), \
             patch.object(CI, "changed_paths", return_value=["docs/v5/PLAN.md", "tools/v5-substrate/cost-cell.py"]), \
             patch.object(CI.subprocess, "run") as run:
            CI.main()
            run.assert_called_once_with([sys.executable, "-B", "tools/v5-substrate/compare_cost_cdc_test.py"], cwd=CI.ROOT, check=True)

    def test_docs_evidence_do_not_run_go_or_tools(self):
        p = CI.plan(["docs/v5/PLAN.md", "tasks/evidence/receipt.json", "tools/v5-migrate/evidence/old.json"])
        self.assertEqual(p["matrix"]["include"], [])
        self.assertFalse(p["comparison"] or p["migration"] or p["external"])

    def test_v5_only_and_root_reader_modules(self):
        p = CI.plan(["v5/internal/replica/receiving.go"])
        self.assertEqual([m["module"] for m in p["matrix"]["include"]], ["v5"])
        self.assertFalse(p["root_behavior"])
        self.assertFalse(p["migration"])
        p = CI.plan(["pkg/graph/internal/core/temporal.go"])
        self.assertEqual([m["module"] for m in p["matrix"]["include"]], ["."])
        self.assertTrue(p["migration"])
        self.assertTrue(p["root_behavior"])
        self.assertFalse(CI.plan([".github/workflows/ci.yml"])["root_behavior"])

    def test_dependency_shift_requires_explicit_full(self):
        with self.assertRaisesRegex(ValueError, "full_validation"):
            CI.plan(["v5/go.mod"])
        p = CI.plan(["v5/go.mod"], full=True)
        self.assertEqual({m["module"] for m in p["matrix"]["include"]}, {".", "v5"})
        self.assertTrue(p["comparison"] and p["migration"])

    def test_external_fdb_is_not_library_or_false_full_fallback(self):
        p = CI.plan(["tools/v5-substrate/fdb/main.go"])
        self.assertTrue(p["external"])
        self.assertEqual(p["matrix"]["include"], [])
        with tempfile.TemporaryDirectory() as tmp, patch.object(CI, "ROOT", Path(tmp)):
            folder = Path(tmp) / "tools/v5-substrate/fdb"
            (folder / "evidence").mkdir(parents=True)
            (folder / "main.go").write_text("current source")
            with self.assertRaisesRegex(ValueError, "full_validation does not build"):
                CI.external_qualification(["tools/v5-substrate/fdb/main.go"])
            digest = CI.hashlib.sha256((folder / "main.go").read_bytes()).hexdigest()
            receipt = {"runs": {"passing_normal_race": {"exit": 0, "cleanupExit": 0}}, "finalSourceSHA256": {"main.go": digest}, "gates": {mode: {"passedParents": 1, "failedParents": 0} for mode in ("normal", "race")}}
            (folder / "evidence/qualified.json").write_text(json.dumps(receipt))
            CI.external_qualification(["tools/v5-substrate/fdb/main.go"])

    def test_external_fdb_refuses_mixed_receipt_hashes(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(CI, "ROOT", Path(tmp)):
            folder = Path(tmp) / "tools/v5-substrate/fdb"
            (folder / "evidence").mkdir(parents=True)
            for name in ("first.go", "second.go"):
                (folder / name).write_text(name)
                data = {"finalSourceSHA256": {name: CI.hashlib.sha256((folder / name).read_bytes()).hexdigest()},
                        "gates": {mode: {"passedParents": 1, "failedParents": 0} for mode in ("normal", "race")},
                        "runs": {"passing_normal_race": {"exit": 0, "cleanupExit": 0}}}
                (folder / ("evidence/" + name + ".json")).write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, "one coherent"):
                CI.external_qualification(["tools/v5-substrate/fdb/first.go", "tools/v5-substrate/fdb/second.go"])

    def test_external_fdb_refuses_incomplete_or_failed_terminal(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(CI, "ROOT", Path(tmp)):
            folder = Path(tmp) / "tools/v5-substrate/fdb"
            (folder / "evidence").mkdir(parents=True)
            (folder / "main.go").write_text("source")
            data = {"finalSourceSHA256": {"main.go": CI.hashlib.sha256((folder / "main.go").read_bytes()).hexdigest()},
                    "gates": {mode: {"passedParents": 1, "failedParents": 0} for mode in ("normal", "race")}}
            for terminal in ({}, {"exit": 0}, {"exit": 1, "cleanupExit": 0}, {"exit": 0, "cleanupExit": 1}):
                with self.subTest(terminal=terminal):
                    data["runs"] = {"passing_normal_race": terminal}
                    (folder / "evidence/receipt.json").write_text(json.dumps(data))
                    with self.assertRaisesRegex(ValueError, "terminal exit/cleanup 0"):
                        CI.external_qualification(["tools/v5-substrate/fdb/main.go"])

    def test_runner_modes_share_short_scope_without_running_go(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(CI, "ROOT", Path(tmp)):
            folder = Path(tmp) / "v5"
            folder.mkdir()
            (folder / "go.mod").write_text("module m\n")
            packages = json.dumps({"ImportPath": "m/internal/replica"})
            with patch.object(CI, "command", return_value=packages), patch.object(CI.subprocess, "run") as run:
                CI.execute("v5", ["v5/internal/replica/receiver.go"], Path(tmp) / "out", "HEAD")
            calls = [c.args[0] for c in run.call_args_list]
            self.assertEqual(len(calls), 3)
            for args in calls:
                self.assertIn("-short", args)
                self.assertIn("-count=1", args)
                self.assertEqual(args[-1], "m/internal/replica")
            self.assertNotIn("-race", calls[0])
            self.assertIn("-race", calls[1])
            self.assertIn("-covermode=atomic", calls[2])

    def test_reverse_imports_include_test_consumers(self):
        packages = [{"ImportPath": "m/a"}, {"ImportPath": "m/b", "Imports": ["m/a"]}, {"ImportPath": "m/c", "TestImports": ["m/b"]}, {"ImportPath": "m/d"}]
        self.assertEqual(CI.affected_packages(packages, {"m/a"}), ["m/a", "m/b", "m/c"])

    def test_required_inventory_missing_name_and_foreign_footer_fail(self):
        CI.validate_inventory(["TestA"], "TestA\nok\tm/pkg\t0.1s\n", "m/pkg")
        with self.assertRaisesRegex(ValueError, "Required compiled selectors missing"):
            CI.validate_inventory(["TestA", "TestMissing"], "TestA\nok\tm/pkg\t0.1s\n", "m/pkg")
        with self.assertRaisesRegex(ValueError, "footer"):
            CI.validate_inventory(["TestA"], "TestA\nok\tforeign\t0.1s\n", "m/pkg")


class RealGitSelection(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.patch = patch.object(CI, "ROOT", self.root)
        self.patch.start()
        self.directory = self.root / "v5/internal/graphapply"
        self.directory.mkdir(parents=True)
        self.git("init", "-q")
        self.git("config", "user.name", "CI fixture")
        self.git("config", "user.email", "ci@example.invalid")

    def tearDown(self):
        self.patch.stop()
        self.tmp.cleanup()

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root, text=True).strip()

    def commit(self):
        self.git("add", ".")
        self.git("commit", "-qm", "fixture")
        return self.git("rev-parse", "HEAD")

    def test_dispatch_focused_base_uses_real_ancestor_and_refuses_invalid_scope(self):
        path = self.directory / "native_graph_cost_test.go"
        path.write_text("package graphapply\n")
        base = self.commit()
        path.write_text("package graphapply\nfunc TestCost(t *testing.T) {}\n")
        middle = self.commit()
        (self.root / "note.md").write_text("later CI repair")
        head = self.commit()
        self.git("checkout", "-q", "--detach", base)
        (self.root / "foreign.md").write_text("foreign branch")
        foreign = self.commit()
        self.git("checkout", "-q", "--detach", head)
        event_path = self.root / "event.json"
        blob = self.git("hash-object", "-w", "note.md")
        def dispatch(focused, dispatched=head):
            event_path.write_text(json.dumps({"inputs": {"full_validation": False, "focused_base": focused}}))
            with patch.dict(os.environ, {"PATH": os.environ["PATH"], "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_REF": "refs/heads/v5", "GITHUB_EVENT_PATH": str(event_path)}, clear=True), \
                 patch.object(sys, "argv", ["ci-changes.py", "plan", "--head", dispatched]), patch.object(sys, "stdout", io.StringIO()) as out:
                CI.main()
            return json.loads(out.getvalue())
        result = dispatch(base)
        self.assertEqual(result["base"], base)
        self.assertFalse(result["full"])
        self.assertIn("v5/internal/graphapply/native_graph_cost_test.go", result["paths"])
        self.assertNotIn("v5/internal/graphapply/native_graph_cost_test.go", dispatch("")["paths"])
        for invalid, dispatched in [("HEAD^", head), (base[:8], head), ("x"*40, head), ("0"*40, head),
                                     (blob, head), (head, head), (foreign, head), (head, middle)]:
            with self.subTest(base=invalid, head=dispatched), self.assertRaises((ValueError, subprocess.CalledProcessError)):
                dispatch(invalid, dispatched)

    def test_rename_reports_both_packages(self):
        path = self.directory / "old.go"
        path.write_text("package graphapply\n")
        base = self.commit()
        target = self.root / "v5/internal/replica/new.go"
        target.parent.mkdir(parents=True)
        path.rename(target)
        head = self.commit()
        self.assertEqual(CI.changed_paths(base, head), ["v5/internal/graphapply/old.go", "v5/internal/replica/new.go"])

    def test_actual_a_companion_always_selected_for_admission_source(self):
        source = self.directory / "graph_replacement_admission.go"
        source.write_text("package graphapply\nfunc check() {}\n")
        companion = self.directory / "graph_replacement_admission_test.go"
        companion.write_text("package graphapply\nfunc TestGraphReplacementAdmissionNativeSurvivingQuorumAllAgreements(t *testing.T) {}\n")
        names = CI.graph_selection(self.directory, [source], {})
        self.assertIn("TestGraphReplacementAdmissionNativeSurvivingQuorumAllAgreements", names)

    def test_helper_edit_uses_changed_definition_not_unchanged_shared_type(self):
        path = self.directory / "fixture_test.go"
        path.write_text("package graphapply\ntype processRequest struct{}\nfunc editedHelper() int { return 1 }\nfunc TestOwn(t *testing.T) {}\n")
        (self.directory / "used_test.go").write_text("package graphapply\nfunc TestUsed(t *testing.T) { editedHelper() }\n")
        (self.directory / "unrelated_test.go").write_text("package graphapply\nfunc TestUnrelated(t *testing.T) { _ = processRequest{} }\n")
        base = self.commit()
        path.write_text(path.read_text().replace("return 1", "return 2"))
        definitions = CI.changed_definitions(path, base)
        self.assertEqual(definitions, [("func", "editedHelper")])
        names = CI.graph_selection(self.directory, [path], {path: definitions})
        self.assertIn("TestUsed", names)
        self.assertNotIn("TestUnrelated", names)

    def test_reviewed_paired_fixture_types_have_finite_real_parent_scope(self):
        fixtures = {
            "paired_graph_fault_harness_test.go": {"TestNativePairedGraphFaultSixOperationsActualProcessesAndRetainedCuts", "TestNativePairedGraphFaultNoOpUsesActualRetainedNormalEntry"},
            "paired_graph_snapshot_harness_test.go": {"TestNativePairedGraphFaultSixOperationsActualProcessesAndRetainedCuts"},
            "prepared_lock_p06_harness_test.go": {"TestPreparedLockP06FirstYESLeaderKillOriginalRecovery"},
            "coordinator_loss_p03_harness_test.go": {"TestNativeCoordinatorLeaderLossAfterDurableCommitBeforeCallerReply"},
            "prepare_fence_p02_harness_test.go": {"TestPrepareFenceP02ActualOrderingAcrossElection"},
        }
        for name, parents in fixtures.items():
            with self.subTest(fixture=name):
                path = self.directory / name
                path.write_text("package graphapply\ntype pairedFixture struct{}\n")
                names = CI.graph_selection(self.directory, [path], {path: [("type", "pairedFixture")]})
                self.assertEqual(set(names), parents | set(CI.GRAPH_COMPAT))

    def test_shared_cost_fixtures_select_all_finite_consumers_only(self):
        parents = {"TestNativeGraphCostSmallActualProcesses", "TestNativeGraphCostProfileAndRouting",
                   "TestNativeGraphCostAckPollMarkerRequiresCostChildAndKeepsDefaultDiagnostics",
                   "TestNativeGraphCostMillionFactProfileUsesBoundedGrantBlocks",
                   "TestNativeGraphCostMultipleGrantsSmallActualProcesses",
                   "TestNativeGraphCostPhysicalReopenAndIndexesSmallActualProcesses",
                   "TestNativeGraphCostReopenIndexPagesRejectMalformedAndOversizedInput",
                   "TestNativeGraphCostIndependentClientsSmallActualProcesses",
                   "TestNativeGraphCostRequestACKFindsOriginalBehindLaterAppliedEntry",
                   "TestNativeGraphCostAttemptLineageAndWindowAreBoundedAndRequestJoined",
                   "TestNativeGraphCostThirtyTwoCommandWindowRejectsOtherWaveOriginals"}
        for fixture in ("native_graph_cost_test.go", "native_graph_cost_harness_test.go",
                        "native_graph_cost_grants_harness_test.go", "native_graph_cost_reopen_harness_test.go",
                        "native_graph_cost_parallel_harness_test.go", "native_graph_cost_runner_test.go"):
            with self.subTest(fixture=fixture):
                path = self.directory / fixture
                path.write_text("package graphapply\ntype sharedCost struct{}\nfunc sharedRunner() int { return 1 }\n")
                (self.directory / "large_test.go").write_text("package graphapply\nfunc TestNativeGraphCostCell(t *testing.T) { sharedRunner() }\n")
                base = self.commit()
                path.write_text(path.read_text().replace("struct{}", "struct{ block uint64 }").replace("return 1", "return 2"))
                head = self.commit()
                changed = [self.root / p for p in CI.changed_paths(base, head)]
                definitions = {p: CI.changed_definitions(p, base) for p in changed}
                self.assertEqual(set(CI.graph_selection(self.directory, changed, definitions)),
                                 parents | set(CI.GRAPH_COMPAT))

    def test_shared_child_cost_delta_keeps_finite_cost_and_transport_consumers(self):
        path = self.directory / "graph_source_process_test.go"
        path.write_text("package graphapply\ntype graphProcessHub struct{}\n")
        selected = set(CI.graph_selection(self.directory, [path], {path: [("type", "graphProcessHub")]}))
        self.assertEqual(selected, set(CI.COST_PARENTS) | set(CI.GRAPH_COMPAT) |
                         {"TestGraphSourceProcessEnvelopeRejectsUnboundAndExtraInput",
                          "TestGraphSourceProcessObservationBatchBoundsOrderAndOwnedFacts"})
        self.assertNotIn("TestNativeGraphCostCell", selected)

    def test_cost_fixture_guard_preserves_changed_large_parent(self):
        fixture = self.directory / "native_graph_cost_harness_test.go"
        fixture.write_text("package graphapply\ntype costFixture struct{}\n")
        names = CI.graph_selection(self.directory, [fixture], {fixture: [("type", "costFixture")]})
        self.assertEqual(set(names), set(CI.COST_PARENTS) | set(CI.GRAPH_COMPAT))
        parent = self.directory / "native_graph_cost_test.go"
        parent.write_text("package graphapply\nfunc TestNativeGraphCostCell(t *testing.T) {}\n")
        names = CI.graph_selection(self.directory, [fixture, parent], {
            fixture: [("type", "costFixture")], parent: [("func", "TestNativeGraphCostCell")]})
        self.assertIn("TestNativeGraphCostCell", names)

    def test_unknown_fixture_type_or_production_scope_requires_full(self):
        fixture = self.directory / "unknown_test.go"
        fixture.write_text("package graphapply\ntype unknown struct{}\n")
        with self.assertRaisesRegex(ValueError, "Unknown changed graphapply fixture scope"):
            CI.graph_selection(self.directory, [fixture], {fixture: [("type", "unknown")]})
        source = self.directory / "new_behavior.go"
        source.write_text("package graphapply\n")
        with self.assertRaisesRegex(ValueError, "Missing changed/companion behavior"):
            CI.graph_selection(self.directory, [source], {})


class RootPartitionCLI(unittest.TestCase):
    """Real git/CLI/artifact flow; every Go call uses a fake executable."""
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / "scripts").mkdir()
        (self.root / "bin").mkdir()
        self.script = self.root / "scripts/ci-changes.py"
        self.script.write_bytes(Path(CI.__file__).read_bytes())
        (self.root / "go.mod").write_text("module m\ngo 1.26.9\n")
        self.core = "m/pkg/graph/internal/core"
        self.names = sorted(["TestNode", "TestRel", "TestHistory", "TestGeneric", "TestNamed", "FuzzWire", "ExampleCore", "TestProcess"])
        self.packages = [{"ImportPath": self.core, "TestGoFiles": ["core_test.go"]}] + [{"ImportPath": "m/" + p, "Imports": [self.core], "TestGoFiles": ["api_test.go"]}
            for p in ["pkg/graph", "pkg/graph/store/badger", "pkg/graph/store/memory", "pkg/graph/store/tiered", "pkg/small"]]
        source = self.root / "pkg/graph/internal/core/core.go"
        source.parent.mkdir(parents=True)
        source.write_text("package core\n")
        fake = self.root / "bin/go"
        fake.write_text("#!/usr/bin/env python3\nimport json,os,re,sys\nfrom pathlib import Path\n" +
            "packages=" + repr(self.packages) + "\nnames=" + repr(self.names) + "\ncore=" + repr(self.core) + r"""
args=sys.argv[1:]
with Path(os.environ["FAKE_LOG"]).open("a") as f: f.write(json.dumps(args)+"\n")
if args[0]=="list":
    for p in packages:
        if p["ImportPath"]==os.environ.get("FAKE_NO_TEST"): p.pop("TestGoFiles",None)
    print("\n".join(json.dumps(p) for p in packages)); sys.exit(0)
if "-list" in args:
    if os.environ.get("FAKE_BAD_INVENTORY"): names.append("TestNode")
    if os.environ.get("FAKE_DRIFT_INVENTORY"): names.append("TestNewParent")
    print("\n".join(names)+"\nok\t"+core+"\t0.001s"); sys.exit(0)
mode="race" if "-race" in args else "coverage" if "-covermode=atomic" in args else "normal"
if os.environ.get("FAKE_FAIL_MODE")==mode: sys.exit(1)
pattern=args[args.index("-run")+1] if "-run" in args else ".*"
for p in [a for a in args if a.startswith("m/")]:
    if p==core:
        selected=[n for n in names if re.search(pattern,n)]
        if os.environ.get("FAKE_OMIT_PARENT"): selected=selected[1:]
        for n in selected: print(json.dumps({"Action":"pass","Package":p,"Test":n}))
    if not os.environ.get("FAKE_OMIT_PACKAGE"):
        print(json.dumps({"Action":"skip" if p==os.environ.get("FAKE_NO_TEST") else "pass","Package":p}))
for a in args:
    if a.startswith("-coverprofile="): Path(a.split("=",1)[1]).write_text("mode: atomic\n")
""")
        fake.chmod(0o755)
        self.env = dict(os.environ, PATH=str(self.root / "bin") + os.pathsep + os.environ["PATH"],
                        FAKE_LOG=str(self.root / "fake-calls.jsonl"), GITHUB_OUTPUT=str(self.root / "outputs"))
        for args in [("init", "-q"), ("config", "user.name", "CI fixture"), ("config", "user.email", "ci@example.invalid"),
                     ("add", "."), ("commit", "-qm", "base")]:
            self.git(*args)
        self.base = self.git("rev-parse", "HEAD")
        source.write_text("package core\nfunc Changed() {}\n")
        self.git("add", "."); self.git("commit", "-qm", "production change")
        self.head = self.git("rev-parse", "HEAD")
        self.plan = self.root / "plan.json"
        result = self.cli("root-plan", "--base", self.base, "--head", self.head, "--output", str(self.plan))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.data = json.loads(self.plan.read_text())
        self.digest = CI.hashlib.sha256(self.plan.read_bytes()).hexdigest()

    def tearDown(self):
        self.tmp.cleanup()

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root, text=True).strip()

    def cli(self, *args, **env):
        return subprocess.run([sys.executable, "-B", str(self.script), *args], cwd=self.root,
                              env=dict(self.env, **env), text=True, capture_output=True)

    def run_part(self, part="core-1", **env):
        return self.cli("root-run", "--plan-file", str(self.plan), "--plan-sha", self.digest,
                        "--part", part, "--output", str(self.root / "out" / part), **env)

    def rewrite_plan(self):
        self.plan.write_text(json.dumps(self.data))
        self.digest = CI.hashlib.sha256(self.plan.read_bytes()).hexdigest()

    def test_exact_affected_packages_and_all_compiled_parent_modes(self):
        parts = self.data["partitions"]
        self.assertEqual({p["part"] for p in parts}, {"other", "graph", "badger", "memory", "tiered", "core-1", "core-2", "core-3", "core-4"})
        assigned = [p for part in parts if part["parents"] is None for p in part["packages"]]
        self.assertEqual(sorted(assigned), sorted(p["ImportPath"] for p in self.packages if p["ImportPath"] != self.core))
        self.assertEqual(sorted(n for p in parts if p["parents"] for n in p["parents"]), self.names)
        for part in parts:
            result = self.run_part(part["part"])
            self.assertEqual(result.returncode, 0, result.stderr)
        calls = [json.loads(x) for x in (self.root / "fake-calls.jsonl").read_text().splitlines()]
        self.assertTrue(all("-race" in c for c in calls if c[0] == "list" or "-list" in c))
        runs = [c for c in calls if c[0] == "test" and "-json" in c]
        self.assertEqual(len(runs), len(parts) * 3)
        for i in range(0, len(runs), 3):
            group = runs[i:i+3]
            self.assertTrue(all("-short" in c and "-timeout=10m" in c for c in group))
            self.assertNotIn("-race", group[0]); self.assertIn("-race", group[1])
            self.assertIn("-covermode=atomic", group[2])
            scopes = [([a for a in c if a.startswith("m/")], c[c.index("-run")+1] if "-run" in c else None) for c in group]
            self.assertEqual(scopes[0], scopes[1]); self.assertEqual(scopes[0], scopes[2])

    def test_plan_omission_duplicate_and_bogus_parent_are_refused(self):
        for change in ("omission", "duplicate", "parent"):
            with self.subTest(change=change):
                self.rewrite_plan()
                data = json.loads(self.plan.read_text())
                if change == "omission": data["partitions"].pop()
                elif change == "duplicate": data["partitions"].append(data["partitions"][0])
                else: next(p for p in data["partitions"] if p["parents"])["parents"].append("TestBogus")
                raw = json.dumps(data).encode()
                self.plan.write_bytes(raw)
                self.digest = CI.hashlib.sha256(raw).hexdigest()
                self.assertIn("omits/duplicates", self.run_part().stderr)

    def test_plan_digest_source_and_unknown_part_cannot_drift(self):
        self.assertIn("Unknown root partition", self.run_part("unknown").stderr)
        self.assertIn("compiled inventory changed", self.run_part(FAKE_DRIFT_INVENTORY="1").stderr)
        self.plan.write_text(self.plan.read_text() + " ")
        self.assertIn("digest mismatch", self.run_part().stderr)
        self.rewrite_plan()
        (self.root / "pkg/graph/internal/core/injected.go").write_text("package core\n")
        self.assertIn("source/modes mismatch", self.run_part().stderr)

    def test_failure_and_missing_parents_never_complete_or_run_coverage(self):
        result = self.run_part(FAKE_FAIL_MODE="race")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("partition passed", result.stdout)
        self.assertFalse((self.root / "out/core-1/coverage.jsonl").exists())
        result = self.run_part(FAKE_OMIT_PARENT="1")
        self.assertIn("missing package/parent terminals", result.stderr)
        self.assertNotIn("partition passed", result.stdout)
        result = self.run_part(FAKE_OMIT_PACKAGE="1")
        self.assertIn("missing package/parent terminals", result.stderr)

    def test_unknown_source_scope_and_duplicate_inventory_refuse_plan(self):
        result = self.cli("root-plan", "--base", self.base, "--head", self.head, "--output", str(self.plan), FAKE_BAD_INVENTORY="1")
        self.assertIn("duplicate root compiled inventory", result.stderr)
        path = self.root / "pkg/unknown.go"
        path.write_text("package unknown\n")
        self.git("add", "."); self.git("commit", "-qm", "unknown package")
        result = self.cli("root-plan", "--base", self.head, "--head", self.git("rev-parse", "HEAD"), "--output", str(self.plan))
        self.assertIn("Unknown/duplicate affected root package scope", result.stderr)

    def test_no_test_package_skip_is_explicit_and_not_behavior_proof(self):
        result = self.cli("root-plan", "--base", self.base, "--head", self.head, "--output", str(self.plan), FAKE_NO_TEST="m/pkg/small")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.data = json.loads(self.plan.read_text()); self.rewrite_plan()
        result = self.run_part("other", FAKE_NO_TEST="m/pkg/small")
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads((self.root / "out/other/normal-summary.json").read_text())
        self.assertEqual(summary["package_terminals"], {"m/pkg/small": "skip"})
        self.assertIn("not executed proof", summary["scope"])


class CurrentRepository(unittest.TestCase):
    def test_checked_compatibility_names_exist_in_current_sources(self):
        directory = CI.ROOT / "v5/internal/graphapply"
        names = set().union(*(CI.parent_names(p) for p in directory.glob("*_test.go")))
        self.assertTrue(set(CI.GRAPH_COMPAT) | set(CI.COST_PARENTS) <= names)
        self.assertIn("TestGraphReplacementAdmissionNativeSurvivingQuorumAllAgreements", CI.GRAPH_COMPAT)

    def test_explicit_cost_cell_is_short_guarded_before_profile(self):
        text = (CI.ROOT / "v5/internal/graphapply/native_graph_cost_test.go").read_text()
        cell = text.split("func TestNativeGraphCostCell(t *testing.T) {", 1)[1].split("\nfunc ", 1)[0]
        self.assertLess(cell.index("testing.Short()"), cell.index("nativeGraphCostSpecFromEnv()"))
        self.assertIn('t.Skip("explicit cost cell is excluded from routine short CI")', cell)

    def test_root_behavior_has_independent_partition_jobs(self):
        text = (CI.ROOT / ".github/workflows/ci.yml").read_text()
        self.assertIn("  focused-root-plan:\n", text)
        self.assertIn("  focused-root-behavior:\n", text)
        self.assertIn("--plan-sha", text)
        self.assertIn("if: matrix.module == 'v5'", text)
        self.assertIn("matrix: ${{ fromJSON(needs.focused-root-plan.outputs.matrix) }}", text)
        self.assertIn("name: focused-behavior-root-${{ matrix.part }}", text)
        self.assertIn("if: steps.plan.outputs.full == 'true'", text)
        self.assertIn("scripts/ci-changes.py cdc --base", text)

    def test_workflow_keeps_full_gates_and_empty_lint_guard(self):
        text = (CI.ROOT / ".github/workflows/ci.yml").read_text()
        self.assertIn("workflow_dispatch:", text)
        self.assertIn("full_validation:", text)
        self.assertIn("      focused_base:\n", text)
        self.assertIn("Optional full commit SHA ancestor for focused manual revalidation", text)
        self.assertNotIn("${{ inputs.focused_base", text)
        self.assertNotIn("${{ github.event.inputs.focused_base", text)
        self.assertIn("make cover-gate", text)
        self.assertIn("Root race tests with exhaustive core batches", text)
        self.assertIn("  race-plan:\n    needs: changes\n    if: needs.changes.outputs.full == 'true'", text)
        self.assertIn("  race:\n    needs: [changes, race-plan]\n    if: needs.changes.outputs.full == 'true'", text)
        self.assertIn("RACE_SHARD: ${{ matrix.part }}", text)
        self.assertIn("v5 race tests with exhaustive graphapply batches", text)
        self.assertIn("if: needs.changes.outputs.matrix !=", text)


if __name__ == "__main__":
    unittest.main()
