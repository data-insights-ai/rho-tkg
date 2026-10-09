#!/usr/bin/env python3
"""Validate portable dataset source/tests and deterministic reference fixture."""
import argparse
import hashlib
import json
import platform
import re
import sqlite3
import subprocess
import tempfile
from pathlib import Path

import generate
import mutants
from format import canonical, parse

ROOT = Path(__file__).resolve().parent
CORE = ("generate.py", "oracle.py", "format.py", "normalize.py", "test_reference.py", "check.py", "README.md", "contract-pin.json", "profile-basic-small.json", "mutants.py")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def dataset_bytes(folder):
    # Runtime oracle costs are intentionally separate from canonical input bytes.
    return {str(path.relative_to(folder)): path.read_bytes() for path in folder.rglob("*") if path.is_file() and path.name != "oracle-costs.json"}


def validate():
    test = subprocess.run(["python3", "-B", "-m", "unittest", "-v"], cwd=ROOT, capture_output=True, text=True)
    if test.returncode:
        raise AssertionError("independent tests failed: " + test.stderr)
    matched = re.search(r"Ran (\d+) tests", test.stderr)
    if matched is None:
        raise AssertionError("unit-test count unavailable")
    profile = parse((ROOT / "profile-basic-small.json").read_text())
    def emit(path):
        return generate.generate(path, profile["nodes"], profile["edges"], profile["seed"], profile["source_signal_rows"], profile["max_depth"], profile["max_output_rows"])
    with tempfile.TemporaryDirectory(prefix="graph-fixture-check-") as scratch:
        first, second = Path(scratch) / "first", Path(scratch) / "second"
        manifest = emit(first)
        emit(second)
        expected = dataset_bytes(ROOT / "fixtures/basic-small")
        if dataset_bytes(first) != dataset_bytes(second) or dataset_bytes(first) != expected:
            raise AssertionError("nondeterministic/stale canonical fixture")
        cost = parse((first / "oracle-costs.json").read_text())
    files = {name: digest(ROOT / name) for name in CORE}
    for path in sorted((ROOT / "fixtures/basic-small").rglob("*")):
        if path.is_file() and path.name != "oracle-costs.json":
            relative = str(path.relative_to(ROOT))
            if re.search(r"/(?:Users|home|private/var|tmp)/", path.read_text()):
                raise AssertionError("private path in portable fixture")
            files[relative] = digest(path)
    mutations = mutants.run()
    return {"schema_version": 1, "mapping_version": "basic-graph-v1", "status": "external-reference-validation-only", "python_version": platform.python_version(), "sqlite_version": sqlite3.sqlite_version,
        "commands": [{"cwd": ".", "command": "python3 -B -m unittest -v", "exit": 0, "tests": int(matched[1])}],
        "determinism": "two independent generations byte-identical to canonical small fixture; runtime oracle-cost record excluded", "complete_query_outputs": len(parse((ROOT / "fixtures/basic-small/queries.json").read_text())),
        "initial": manifest["revisions"][0], "current": manifest["revisions"][-1], "source_signal_rows": manifest["source_signal_rows"],
        "oracle_generation_cost_record": cost, "source_and_canonical_fixture_sha256": dict(sorted(files.items())), "mutants": mutations,
        "limits": ["No vendor adapter/installation/license or benchmark harness/results", "No native vendor integer-range/label/multiedge/durability support claim", "Basic numeric-zero semantics; source bits and present-null are separate feature cases", "SQLite/oracle workspace and output costs are not future engine costs; peaks remain unmeasured", "Small exact tests and Python allocation guard do not establish large-data/RSS/capacity or V0/V7 acceptance"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-validation", action="store_true")
    args = parser.parse_args()
    record = validate()
    target = ROOT / "validation.json"
    if args.write_validation:
        target.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    else:
        expected = parse(target.read_text())
        for name in ("python_version", "sqlite_version", "oracle_generation_cost_record"):
            expected.pop(name)
        comparable = dict(record)
        for name in ("python_version", "sqlite_version", "oracle_generation_cost_record"):
            comparable.pop(name)
        if expected != comparable:
            raise AssertionError("validation record is stale")
    print(canonical({"result": "passed", "tests": record["commands"][0]["tests"], "complete_query_outputs": record["complete_query_outputs"], "mutants_killed": len(record["mutants"]), "initial_nodes_edges": [record["initial"]["current_nodes"], record["initial"]["current_edges"]], "current_nodes_edges": [record["current"]["current_nodes"], record["current"]["current_edges"]], "V7_complete": False}))


if __name__ == "__main__":
    main()
