#!/usr/bin/env python3
"""Small dispatch/selection guards; no Go/compiler or external runtime."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("ci_changes", Path(__file__).with_name("ci-changes.py"))
CI = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CI)


class Planning(unittest.TestCase):
    def test_docs_evidence_do_not_run_go_or_tools(self):
        p = CI.plan(["docs/v5/PLAN.md", "tasks/evidence/receipt.json", "tools/v5-migrate/evidence/old.json"])
        self.assertEqual(p["matrix"]["include"], [])
        self.assertFalse(p["comparison"] or p["migration"] or p["external"])

    def test_v5_only_and_root_reader_modules(self):
        p = CI.plan(["v5/internal/replica/receiving.go"])
        self.assertEqual([m["module"] for m in p["matrix"]["include"]], ["v5"])
        self.assertFalse(p["migration"])
        p = CI.plan(["pkg/graph/internal/core/temporal.go"])
        self.assertEqual([m["module"] for m in p["matrix"]["include"]], ["."])
        self.assertTrue(p["migration"])

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
        for name in ("paired_graph_fault_harness_test.go", "paired_graph_snapshot_harness_test.go"):
            with self.subTest(fixture=name):
                path = self.directory / name
                path.write_text("package graphapply\ntype pairedFixture struct{}\n")
                names = CI.graph_selection(self.directory, [path], {path: [("type", "pairedFixture")]})
                self.assertIn("TestNativePairedGraphFaultSixOperationsActualProcessesAndRetainedCuts", names)
        self.assertIn("TestNativePairedGraphFaultNoOpUsesActualRetainedNormalEntry", CI.FIXTURE_COMPAT["paired_graph_fault_harness_test.go"])

    def test_cost_fixture_guard_preserves_changed_large_parent(self):
        fixture = self.directory / "native_graph_cost_harness_test.go"
        fixture.write_text("package graphapply\ntype costFixture struct{}\n")
        names = CI.graph_selection(self.directory, [fixture], {fixture: [("type", "costFixture")]})
        self.assertTrue({"TestNativeGraphCostSmallActualProcesses", "TestNativeGraphCostProfileAndRouting"} <= set(names))
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


class CurrentRepository(unittest.TestCase):
    def test_checked_compatibility_names_exist_in_current_sources(self):
        directory = CI.ROOT / "v5/internal/graphapply"
        names = set().union(*(CI.parent_names(p) for p in directory.glob("*_test.go")))
        self.assertTrue(set(CI.GRAPH_COMPAT) <= names)
        self.assertIn("TestGraphReplacementAdmissionNativeSurvivingQuorumAllAgreements", CI.GRAPH_COMPAT)

    def test_explicit_cost_cell_is_short_guarded_before_profile(self):
        text = (CI.ROOT / "v5/internal/graphapply/native_graph_cost_test.go").read_text()
        cell = text.split("func TestNativeGraphCostCell(t *testing.T) {", 1)[1].split("\nfunc ", 1)[0]
        self.assertLess(cell.index("testing.Short()"), cell.index("nativeGraphCostSpecFromEnv()"))
        self.assertIn('t.Skip("explicit cost cell is excluded from routine short CI")', cell)

    def test_workflow_keeps_full_gates_and_empty_lint_guard(self):
        text = (CI.ROOT / ".github/workflows/ci.yml").read_text()
        self.assertIn("workflow_dispatch:", text)
        self.assertIn("full_validation:", text)
        self.assertIn("make cover-gate", text)
        self.assertIn("Root race tests with exhaustive core batches", text)
        self.assertIn("  race-plan:\n    needs: changes\n    if: needs.changes.outputs.full == 'true'", text)
        self.assertIn("  race:\n    needs: [changes, race-plan]\n    if: needs.changes.outputs.full == 'true'", text)
        self.assertIn("RACE_SHARD: ${{ matrix.part }}", text)
        self.assertIn("v5 race tests with exhaustive graphapply batches", text)
        self.assertIn("if: needs.changes.outputs.matrix !=", text)


if __name__ == "__main__":
    unittest.main()
