#!/usr/bin/env python3
"""Select changed Go packages and reverse test dependents; fail closed on graph changes."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile


def run(*args, cwd=None):
    return subprocess.check_output(args, cwd=cwd, text=True)


def graph(root):
    root = Path(root).resolve()
    module = run("go", "list", "-m", cwd=root).strip()
    raw = run("go", "list", "-deps", "-test", "-json", "./...", cwd=root)
    decoder, at, edges, dirs = json.JSONDecoder(), 0, {}, {}
    while at < len(raw):
        while at < len(raw) and raw[at].isspace():
            at += 1
        if at == len(raw):
            break
        value, at = decoder.raw_decode(raw, at)
        owner = value.get("ForTest") or value["ImportPath"].split(" [")[0]
        if not (owner == module or owner.startswith(module + "/")) or owner.endswith(".test"):
            continue
        imports = edges.setdefault(owner, set())
        for field in ("Imports", "TestImports", "XTestImports"):
            imports.update(x.split(" [")[0] for x in value.get(field, []))
        if value.get("Dir") and not value.get("ForTest"):
            dirs[Path(value["Dir"]).resolve().relative_to(root).as_posix()] = owner
    return edges, dirs


def external_inputs(path):
    # These tests consume repository documents outside their Go package.
    # Keep the dependency explicit rather than assuming docs cannot affect tests.
    if path in ("README.md", "AGENTS.md", "CHANGELOG.md") or path.startswith("docs/"):
        return ["pkg/graph/__external_input__", "pkg/graph/internal/core/__external_input__"]
    return []


def select(changed, old, current, dirs):
    if any(x in ("go.mod", "go.sum") for x in changed) or old != current:
        return ["./..."], "full normal fallback: module or package/test dependency graph changed"
    selected = set()
    inputs = [p for x in changed for p in [x, *external_inputs(x)]]
    for path in inputs:
        folder = Path(path).parent
        while folder.as_posix() != ".":
            if folder.as_posix() in dirs:
                selected.add(dirs[folder.as_posix()])
                break
            folder = folder.parent
        if folder.as_posix() == "." and "." in dirs and path.endswith(".go"):
            selected.add(dirs["."])
    while True:
        expanded = selected | {p for p, deps in current.items() if deps & selected}
        if expanded == selected:
            return sorted(selected), "changed packages plus reverse production/test dependency closure"
        selected = expanded


def owns_package(path, dirs):
    # Package-local fixtures and auxiliary files affect tests too. A newly
    # introduced Go package must trigger graph comparison/full fallback.
    if path.endswith(".go") or external_inputs(path):
        return True
    folder = Path(path).parent
    while folder.as_posix() != ".":
        if folder.as_posix() in dirs:
            return True
        folder = folder.parent
    return False


def main():
    base = sys.argv[1]
    root = Path(run("git", "rev-parse", "--show-toplevel").strip())
    changed = run("git", "diff", "--name-only", base, "HEAD", cwd=root).splitlines()
    if any(x in ("go.mod", "go.sum") for x in changed):
        packages, reason = ["./..."], "full normal fallback: module dependencies changed"
    else:
        tracked = run("git", "ls-files", "*.go", cwd=root).splitlines()
        package_dirs = {Path(x).parent.as_posix() for x in tracked}
        relevant = any(owns_package(x, package_dirs) for x in changed)
        if not relevant:
            print("no package inputs changed", flush=True)
            return
        current, dirs = graph(root)
        with tempfile.TemporaryDirectory(prefix="rho-ci-base-") as temp:
            baseline = Path(temp) / "base"
            subprocess.run(["git", "worktree", "add", "--detach", str(baseline), base], cwd=root, check=True)
            try:
                old, _ = graph(baseline)
            finally:
                subprocess.run(["git", "worktree", "remove", "--force", str(baseline)], cwd=root, check=True)
        packages, reason = select(changed, old, current, dirs)
    print(reason + ": " + (", ".join(packages) or "no package inputs changed"), flush=True)
    if packages:
        subprocess.run(["go", "test", "-short", "-count=1", *packages], cwd=root, check=True)


if __name__ == "__main__":
    main()
