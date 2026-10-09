#!/usr/bin/env python3
"""Compare complete adapter exports without coercion or row/multiedge collapse."""
import argparse
import hashlib
import itertools
import re
import sqlite3
import tempfile
from pathlib import Path

from format import ContractError, MAPPING_VERSION, canonical, entity, external_id, fields, parse, result_row


def normalized_row(row, shape):
    if shape != "revision":
        return result_row(row, shape)
    fields(row, ["revision", "kind", "id", "deleted", "row", "property_changes"])
    if type(row["revision"]) is not int or row["revision"] < 0 or row["kind"] not in ("node", "edge") or type(row["deleted"]) is not bool or type(row["property_changes"]) is not dict:
        raise ContractError("invalid retained revision")
    external_id(row["id"], row["kind"])
    if row["deleted"]:
        if row["row"] is not None or row["property_changes"] != {"entity_deleted": True}:
            raise ContractError("deleted revision is not a tombstone")
    else:
        row = dict(row)
        row["row"] = entity(row["row"])
        if row["row"]["id"] != row["id"] or row["row"]["kind"] != row["kind"]:
            raise ContractError("revision identity mismatch")
    return row


def checksum(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        while data := source.read(64 << 10):
            digest.update(data)
    return digest.hexdigest()


def compare_files(expected, actual, shape, max_rows=5_000_000):
    # SQLite sorts on disk; each Python row is discarded before the next read.
    with tempfile.TemporaryDirectory(prefix="graph-normalize-") as workspace:
        db = sqlite3.connect(Path(workspace) / "rows.sqlite")
        try:
            db.executescript("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=FILE; PRAGMA cache_size=-4096; CREATE TABLE rows(side INTEGER,wire TEXT COLLATE BINARY);")
            counts = []
            for side, path in enumerate((expected, actual)):
                count = 0
                with Path(path).open(encoding="utf-8") as source:
                    for line in source:
                        count += 1
                        if count > max_rows:
                            raise ContractError("complete export exceeds declared normalization row budget")
                        row = normalized_row(parse(line), shape)
                        db.execute("INSERT INTO rows VALUES(?,?)", (side, canonical(row)))
                counts.append(count)
            db.commit()
            expected_rows = db.execute("SELECT wire FROM rows WHERE side=0 ORDER BY wire")
            actual_rows = db.execute("SELECT wire FROM rows WHERE side=1 ORDER BY wire")
            digest = hashlib.sha256()
            missing = object()
            for index, (left, right) in enumerate(itertools.zip_longest(expected_rows, actual_rows, fillvalue=missing)):
                if left is missing or right is missing or left != right:
                    raise ContractError(f"complete output differs at canonical multiset row {index}; counts {counts[0]}/{counts[1]}")
                digest.update((right[0] + "\n").encode("utf-8"))
            return {"rows": counts[1], "normalized_multiset_sha256": digest.hexdigest(), "result": "equal-canonical-output", "native_vendor_support": "not established by normalization"}
        finally:
            db.close()


def validate_dataset(dataset):
    """Verify the complete supplied reference before attributing its input ID."""
    dataset = Path(dataset)
    manifest = parse((dataset / "manifest.json").read_text())
    if manifest.get("mapping_version") != MAPPING_VERSION or manifest.get("schema_version") != 1:
        raise ContractError("dataset mapping/schema version mismatch")
    files = manifest.get("files")
    required = {"nodes.jsonl", "edges.jsonl", "changes.jsonl", "queries.json",
                "current-nodes.jsonl", "current-edges.jsonl", "retained-revisions.jsonl",
                "feature-cases.json"}
    if type(files) is not dict or not required <= files.keys():
        raise ContractError("missing canonical dataset declarations")
    for name, info in files.items():
        if type(name) is not str or name not in required and not re.fullmatch(r"expected/[A-Za-z0-9_.-]+\.jsonl", name):
            raise ContractError("unexpected canonical dataset declaration")
        if type(info) is not dict or set(info) != ({"bytes", "sha256", "rows"} if name.endswith(".jsonl") else {"bytes", "sha256"}):
            raise ContractError("invalid canonical file metadata")
        if type(info["bytes"]) is not int or info["bytes"] < 0 or type(info["sha256"]) is not str or not re.fullmatch(r"[0-9a-f]{64}", info["sha256"]):
            raise ContractError("invalid canonical file size/checksum")
        if "rows" in info and (type(info["rows"]) is not int or info["rows"] < 0):
            raise ContractError("invalid canonical file row count")
        path = dataset / name
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(dataset.resolve()):
            raise ContractError("missing or redirected canonical dataset file: " + name)
        # One streaming pass verifies bytes, complete JSONL rows and checksum.
        digest, size, rows, last = hashlib.sha256(), 0, 0, b""
        with path.open("rb") as source:
            while data := source.read(64 << 10):
                digest.update(data)
                size += len(data)
                rows += data.count(b"\n")
                last = data[-1:]
        if size != info["bytes"] or digest.hexdigest() != info["sha256"] or "rows" in info and (rows != info["rows"] or size and last != b"\n"):
            raise ContractError("canonical dataset file metadata mismatch: " + name)
    actual = {str(path.relative_to(dataset)) for path in dataset.rglob("*") if path.is_file() or path.is_symlink()}
    # The disclosed runtime workspace record is deliberately outside input identity.
    allowed = set(files) | {"manifest.json", "oracle-costs.json"}
    if not actual <= allowed or not set(files) <= actual:
        raise ContractError("missing or extra canonical dataset artifact")
    initial = manifest.get("declared_initial_graph")
    fields(initial, ["nodes", "edges"])
    seed = manifest.get("seed")
    if any(type(initial[key]) is not int or not 0 <= initial[key] <= (1 << 63) - 3 for key in ("nodes", "edges")) or type(seed) is not int or not 0 <= seed < 1 << 64:
        raise ContractError("invalid declared canonical input counts/seed")
    if initial["nodes"] != files["nodes.jsonl"]["rows"] or initial["edges"] != files["edges.jsonl"]["rows"]:
        raise ContractError("canonical input count mismatch")
    identity = hashlib.sha256(canonical({"mapping_version": MAPPING_VERSION, "seed": seed,
        "nodes": initial["nodes"], "edges": initial["edges"],
        "nodes_sha256": files["nodes.jsonl"]["sha256"], "edges_sha256": files["edges.jsonl"]["sha256"],
        "changes_sha256": files["changes.jsonl"]["sha256"]}).encode("utf-8")).hexdigest()
    if manifest.get("canonical_input_sha256") != identity:
        raise ContractError("canonical input identity mismatch")
    queries = parse((dataset / "queries.json").read_text())
    if type(queries) is not list:
        raise ContractError("query specification must be a list")
    identities, answers = set(), set()
    for query in queries:
        fields(query, ["id", "revision", "lane", "request", "shape", "expected_file", "expected"])
        if type(query["id"]) is not str or query["id"] in identities or type(query["expected_file"]) is not str:
            raise ContractError("invalid or repeated query identity")
        identities.add(query["id"])
        name = query["expected_file"]
        if name not in files or not name.endswith(".jsonl") or query["expected"] != files[name]:
            raise ContractError("stale query expected-answer metadata")
        if name in answers:
            raise ContractError("repeated query answer file")
        answers.add(name)
    if answers != {name for name in files if name.startswith("expected/")} | {"retained-revisions.jsonl"}:
        raise ContractError("missing or extra declared query answer")
    return manifest, queries


def compare_export(dataset, query_id, actual, metadata):
    dataset = Path(dataset)
    manifest, queries = validate_dataset(dataset)
    matches = [query for query in queries if query["id"] == query_id]
    if len(matches) != 1:
        raise ContractError("unknown or repeated query identity")
    query = matches[0]
    fields(metadata, ["mapping_version", "dataset_sha256", "query_id", "lane"])
    if metadata != {"mapping_version": MAPPING_VERSION, "dataset_sha256": manifest["canonical_input_sha256"], "query_id": query_id, "lane": query["lane"]}:
        raise ContractError("adapter export mapping/query/lane mismatch")
    expected = dataset / query["expected_file"]
    if checksum(expected) != query["expected"]["sha256"]:
        raise ContractError("expected-answer checksum mismatch")
    result = compare_files(expected, actual, query["shape"], manifest["limits"]["max_complete_output_rows_per_query"])
    if result["rows"] != query["expected"]["rows"]:
        raise ContractError("expected-answer row count mismatch")
    return {"mapping_version": MAPPING_VERSION, "dataset_sha256": manifest["canonical_input_sha256"], "query_id": query_id, "lane": query["lane"], **result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--actual", required=True)
    parser.add_argument("--metadata", required=True)
    args = parser.parse_args()
    try:
        result = compare_export(args.dataset, args.query, args.actual, parse(Path(args.metadata).read_text()))
    except (ContractError, OSError, ValueError, sqlite3.Error) as error:
        parser.exit(1, "normalization refused: " + str(error) + "\n")
    print(canonical(result))


if __name__ == "__main__":
    main()
