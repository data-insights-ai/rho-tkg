import importlib.util
from pathlib import Path
import unittest
import subprocess
import tempfile
import sys

spec = importlib.util.spec_from_file_location("affected", Path(__file__).with_name("affected-tests.py"))
affected = importlib.util.module_from_spec(spec)
spec.loader.exec_module(affected)


class Selection(unittest.TestCase):
    def test_transitive_reverse_test_dependencies(self):
        graph = {"a": {"fmt"}, "b": {"a"}, "c": {"b"}, "d": {"os"}}
        selected, _ = affected.select(["pkg/a/testdata/input.json"], graph, graph, {"pkg/a": "a"})
        self.assertEqual(selected, ["a", "b", "c"])

    def test_graph_change_falls_back(self):
        self.assertEqual(affected.select(["pkg/a/a.go"], {"a": set()}, {"a": {"fmt"}}, {"pkg/a": "a"})[0], ["./..."])

    def test_dependency_change_falls_back(self):
        self.assertEqual(affected.select(["go.sum"], {}, {}, {})[0], ["./..."])

    def test_package_inputs_include_nested_fixtures_and_new_go_packages(self):
        self.assertTrue(affected.owns_package("pkg/a/testdata/nested/input.bin", {"pkg/a"}))
        self.assertTrue(affected.owns_package("new/package/new.go", set()))
        self.assertFalse(affected.owns_package("tasks/unrelated-note.md", {"."}))

    def test_deleted_package_falls_back(self):
        self.assertEqual(affected.select(["pkg/a/a.go"], {"a": set()}, {}, {})[0], ["./..."])

    def test_external_docs_select_their_readers_and_reverse_dependents(self):
        graph = {"core": set(), "graph": {"core"}, "app": {"graph"}, "unrelated": set()}
        dirs = {"pkg/graph": "graph", "pkg/graph/internal/core": "core"}
        self.assertEqual(affected.select(["docs/errors.md"], graph, graph, dirs)[0], ["app", "core", "graph"])
        self.assertTrue(affected.owns_package("README.md", set()))
        self.assertEqual(affected.select(["tasks/unrelated-note.md"], {}, {}, {})[0], [])


class ActualGraph(unittest.TestCase):
    def test_cli_selects_reverse_closure_and_fails_closed_on_changed_imports(self):
        with tempfile.TemporaryDirectory(prefix="rho-ci-selector-test-") as temp:
            root = Path(temp)
            def command(*args):
                result = subprocess.run(args, cwd=root, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                self.assertEqual(result.returncode, 0, result.stdout)
                return result.stdout
            def write(name, text):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text)
            command("git", "init", "-q")
            command("git", "config", "user.name", "CI selector test")
            command("git", "config", "user.email", "ci@example.invalid")
            write("go.mod", "module example.invalid/selector\ngo 1.26.9\n")
            write("a/a.go", "package a\nfunc Value() int { return 1 }\n")
            write("b/b.go", 'package b\nimport "example.invalid/selector/a"\nfunc Value() int { return a.Value() }\n')
            write("c/c.go", "package c\n")
            # c depends on b ONLY through a test, so production-only closure misses it.
            write("c/c_test.go", 'package c\nimport ("testing"; "example.invalid/selector/b")\nfunc TestValue(t *testing.T) { if b.Value()!=2 { t.Fatal("reverse test dependency was not exercised") } }\n')
            write("d/d.go", "package d\n")
            write("d/d_test.go", 'package d\nimport "testing"\nfunc TestUnrelated(t *testing.T) { t.Fatal("full-suite fallback probe") }\n')
            command("git", "add", ".")
            command("git", "commit", "-qm", "baseline")
            base = command("git", "rev-parse", "HEAD").strip()
            write("a/a.go", "package a\nfunc Value() int { return 2 }\n")
            command("git", "add", ".")
            command("git", "commit", "-qm", "change a without changing graph")
            script = Path(__file__).with_name("affected-tests.py").resolve()
            result = command(sys.executable, str(script), base)
            self.assertIn("example.invalid/selector/a, example.invalid/selector/b, example.invalid/selector/c", result)
            self.assertNotIn("example.invalid/selector/d", result)
            write("a/a.go", 'package a\nimport "strings"\nfunc Value() int { return len(strings.Repeat("x",2)) }\n')
            command("git", "add", ".")
            command("git", "commit", "-qm", "change imports")
            result = subprocess.run([sys.executable, str(script), base], cwd=root, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("full normal fallback", result.stdout)
            self.assertIn("full-suite fallback probe", result.stdout)


if __name__ == "__main__":
    unittest.main()
