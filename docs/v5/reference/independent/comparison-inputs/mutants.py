"""Run deliberate reference defects against independent literal assertions."""
import hashlib
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCES = ("generate.py", "oracle.py", "format.py", "normalize.py", "test_reference.py", "contract-pin.json")
CASES = (
    ("collapse-parallel-adjacency", "oracle.py", "SELECT id,source,target,type FROM edges WHERE {column}=? ORDER BY id", "SELECT MIN(id),source,target,type FROM edges WHERE {column}=? GROUP BY source,target,type ORDER BY MIN(id)"),
    ("round-int64-through-float", "format.py", 'return f"{i64(value) + (1 << 63):016x}"', 'return f"{int(float(i64(value))) + (1 << 63):016x}"'),
    ("omit-false-projection", "oracle.py", 'if key in row["properties"] else {"presence": "absent"}', 'if key in row["properties"] and row["properties"][key].get("value") is not False else {"presence": "absent"}'),
)
TEST = "test_reference.OracleTests.test_literal_integer_range_and_complete_walk_multiset"


def run():
    hashes = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES}
    results = []
    for name, filename, original, replacement in CASES:
        with tempfile.TemporaryDirectory(prefix="graph-reference-mutant-") as scratch:
            folder = Path(scratch)
            for source in SOURCES:
                shutil.copyfile(ROOT / source, folder / source)
            path = folder / filename
            text = path.read_text()
            if text.count(original) != 1:
                raise AssertionError("mutant target drifted: " + name)
            path.write_text(text.replace(original, replacement))
            mutant_sha = hashlib.sha256(path.read_bytes()).hexdigest()
            result = subprocess.run(["python3", "-B", "-m", "unittest", "-v", TEST], cwd=folder, capture_output=True, text=True)
            if result.returncode == 0 or "FAIL: test_literal_integer_range_and_complete_walk_multiset" not in result.stderr or "AssertionError" not in result.stderr:
                raise AssertionError("mutant survived or failed without the literal assertion: " + name)
            results.append({"name": name, "mutated_file": filename, "mutated_file_sha256": mutant_sha, "test": TEST, "exit": result.returncode, "result": "killed-by-literal-assertion", "base_source_sha256": hashes})
    return results
