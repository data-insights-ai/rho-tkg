"""Adversarial reproduction admission; no vendor service or network execution."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest

import reproduce

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[1]


class ReproductionRefusals(unittest.TestCase):
    def test_ambient_override_cannot_select_another_build(self):
        for key, value in {"GOFLAGS": "-overlay=foreign.json", "GOWORK": "../go.work",
                           "GOOS": "linux", "GOARCH": "amd64", "GOEXPERIMENT": "fieldtrack",
                           "GOROOT": "/another/compiler", "GOTOOLCHAIN": "auto"}.items():
            with self.subTest(key=key), self.assertRaises(reproduce.Refusal):
                reproduce.validate_environment({key: value})
        reproduce.validate_environment({"GOWORK": "off", "GOTOOLCHAIN": "local"})

    def test_self_consistent_lock_rewrite_is_not_authority(self):
        source, reference = reproduce.load_locks(ROOT)
        self.assertEqual(source["file_count"], 1438)
        self.assertEqual(len(reference["files_sha256"]), 114)
        with tempfile.TemporaryDirectory() as tmp:
            altered = Path(tmp)
            shutil.copytree(ROOT / "pins", altered / "pins")
            path = altered / "pins/source.json"
            rewritten = json.loads(path.read_text())
            rewritten["commit"] = "0" * 40
            path.write_text(json.dumps(rewritten))
            with self.assertRaises(reproduce.Refusal):
                reproduce.load_locks(altered)
            shutil.copy2(ROOT / "pins/source.json", path)
            path = altered / "pins/reference.json"
            rewritten = json.loads(path.read_text())
            rewritten["files_sha256"]["normalize.py"] = "0" * 64
            path.write_text(json.dumps(rewritten))
            with self.assertRaises(reproduce.Refusal):
                reproduce.load_locks(altered)

    def test_release_is_exact_commit_tree_and_content_not_head_or_tar_receipt(self):
        source, _ = reproduce.load_locks(ROOT)
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "release"
            receipt = reproduce.extract_release(REPO, target, source)
            self.assertEqual(receipt["commit"], source["commit"])
            self.assertEqual(receipt["tree"], source["tree"])
            self.assertEqual(receipt["file_count"], 1438)
            self.assertIn("go 1.26.7", (target / "go.mod").read_text())
            # Content authority does not require the original tar/manifest receipt.
            alternative = dict(source, historical_archive_sha256="0" * 64,
                               historical_source_manifest_sha256="0" * 64)
            reproduce.verify_release(target, alternative)
            path = target / "go.mod"
            original = path.read_bytes()
            path.write_bytes(original + b"\n// foreign engine\n")
            with self.assertRaises(reproduce.Refusal):
                reproduce.verify_release(target, source)
            path.write_bytes(original)
            (target / "extra.go").write_text("package foreign\n")
            with self.assertRaises(reproduce.Refusal):
                reproduce.verify_release(target, source)
            (target / "extra.go").unlink()
            path.unlink()
            with self.assertRaises(reproduce.Refusal):
                reproduce.verify_release(target, source)
        with tempfile.TemporaryDirectory() as tmp:
            bad = dict(source, commit="0" * 40)
            with self.assertRaises(reproduce.Refusal):
                reproduce.extract_release(REPO, Path(tmp) / "absent", bad)
            bad = dict(source, tree="0" * 40)
            with self.assertRaises(reproduce.Refusal):
                reproduce.extract_release(REPO, Path(tmp) / "wrong-tree", bad)

    def test_all_reference_files_including_extra_and_symlink_are_bound(self):
        _, pin = reproduce.load_locks(ROOT)
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "reference"
            original = REPO / "docs/v5/reference/independent/comparison-inputs"
            reproduce.copy_reference(original, target, pin)
            self.assertEqual(len(reproduce.hash_tree(target)), 114)
            path = target / "normalize.py"
            contents = path.read_bytes()
            path.write_bytes(contents + b"\n# foreign reference\n")
            with self.assertRaises(reproduce.Refusal):
                reproduce.verify_reference(target, pin)
            path.write_bytes(contents)
            (target / "extra").write_text("undeclared")
            with self.assertRaises(reproduce.Refusal):
                reproduce.verify_reference(target, pin)
            (target / "extra").unlink()
            (target / "link").symlink_to(path)
            with self.assertRaises(reproduce.Refusal):
                reproduce.verify_reference(target, pin)
            (target / "link").unlink()
            path.unlink()
            with self.assertRaises(reproduce.Refusal):
                reproduce.verify_reference(target, pin)

    def test_output_ownership_never_removes_or_overwrites_caller_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            existing = parent / "existing"
            existing.mkdir()
            sentinel = existing / "sentinel"
            sentinel.write_bytes(b"prior evidence\n")
            with self.assertRaises(reproduce.Refusal):
                with reproduce.owned_workspace(existing):
                    self.fail("existing output was admitted")
            self.assertEqual(sentinel.read_bytes(), b"prior evidence\n")
            fresh = parent / "refused"
            with self.assertRaisesRegex(RuntimeError, "injected"):
                with reproduce.owned_workspace(fresh) as owned:
                    (owned / "partial").write_bytes(b"not success")
                    raise RuntimeError("injected")
            self.assertFalse(fresh.exists())
            self.assertEqual(sorted(p.name for p in parent.iterdir()), ["existing"])
            # Empty preexisting destinations also belong to the caller.
            empty = parent / "empty"
            empty.mkdir()
            with self.assertRaises(reproduce.Refusal):
                with reproduce.owned_workspace(empty):
                    pass
            with self.assertRaises(reproduce.Refusal):
                with reproduce.owned_workspace(sentinel / "child"):
                    pass

    def test_compiler_admission_refuses_default_or_missing_binaries(self):
        with self.assertRaises(reproduce.Refusal):
            reproduce.compiler("/missing/go1.26.9", {})
        # The test may run on a host whose default Go is the required compiler.
        candidate = shutil.which("go")
        if candidate:
            import subprocess
            version = subprocess.run([candidate, "version"], capture_output=True, text=True).stdout
            if "go1.26.9 " not in version:
                with self.assertRaises(reproduce.Refusal):
                    reproduce.compiler(candidate, {})

    def test_gate_dispatch_does_not_skip_nested_module_or_duplicate_ci_checks(self):
        make = (REPO / "Makefile").read_text()
        ci = (REPO / ".github/workflows/ci.yml").read_text()
        import subprocess
        dispatch = subprocess.run(["make", "-n", "compare-ci"], cwd=REPO, capture_output=True, text=True)
        self.assertEqual(dispatch.returncode, 0, dispatch.stderr)
        self.assertIn("python3 -B reproduce.py", dispatch.stdout)
        self.assertIn(":!:tools/graph-compare/**", make)
        self.assertIn("go-version-file: tools/graph-compare/go.mod", ci)
        self.assertIn("fetch-depth: 0", ci)
        self.assertEqual(ci.count("make compare-ci"), 1)
        self.assertIn("GOTOOLCHAIN: local", ci)
        # Root v4/v5 jobs explicitly retain their own gates without repeating tool jobs.
        self.assertIn('COMPARE_DISPATCH: "0"', ci)

    def test_portable_receipts_never_include_host_paths_or_raw_logs(self):
        receipt = reproduce.portable_receipt(["/host/private/go", "test", "./..."],
                                             "go", 0, b"warning with /Users/private/path\n")
        self.assertEqual(receipt["command"], ["go", "test", "./..."])
        self.assertEqual(receipt["exit_code"], 0)
        self.assertEqual(receipt["output_sha256"], hashlib.sha256(b"warning with /Users/private/path\n").hexdigest())
        self.assertNotIn("/Users", json.dumps(receipt))
        self.assertNotIn("warning", json.dumps(receipt))

    def test_interruption_and_late_destination_collision_never_publish_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "cancelled"
            with self.assertRaises(KeyboardInterrupt):
                with reproduce.owned_workspace(output) as owned:
                    (owned / "completion.json").write_text("not complete")
                    raise KeyboardInterrupt
            self.assertFalse(output.exists())
            output = Path(tmp) / "collision"
            with self.assertRaises(reproduce.Refusal):
                with reproduce.owned_workspace(output) as owned:
                    (owned / "completion.json").write_text("runner proof")
                    output.mkdir()
                    (output / "caller").write_text("late caller bytes")
            self.assertEqual((output / "caller").read_text(), "late caller bytes")
            self.assertFalse((output / "completion.json").exists())

    def test_wrong_compiler_is_refused_even_when_default_is_the_right_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            wrong = Path(tmp) / "go"
            wrong.write_text("#!/bin/sh\necho 'go version go1.27.1 darwin/arm64'\n")
            wrong.chmod(0o700)
            with self.assertRaises(reproduce.Refusal):
                reproduce.compiler(str(wrong), {})


if __name__ == "__main__":
    unittest.main()
