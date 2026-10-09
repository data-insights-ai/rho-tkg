#!/usr/bin/env python3
"""Stream portable synthetic graph inputs; delegate answers to an independent oracle."""
import argparse
import hashlib
import json
import shutil
import sqlite3
import tempfile
from pathlib import Path

from format import ContractError, I64_MAX, MAPPING_VERSION, canonical, f64_cell
from oracle import Oracle

BOUNDARIES = (-(1 << 63), -(1 << 53) - 1, -(1 << 53), -(1 << 31) - 1,
              -(1 << 31), -1, 0, 1, (1 << 31) - 1, 1 << 31,
              1 << 53, (1 << 53) + 1, (1 << 63) - 1)
MASK = (1 << 64) - 1


def mix(value):
    value = (value + 0x9E3779B97F4A7C15) & MASK
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK
    return value ^ (value >> 31)


def identifier(kind, number):
    return ("n:" if kind == "node" else "e:") + f"{number:016x}"


def properties(kind, number, seed):
    result = {"p_bool": {"type": "bool", "value": bool((number + seed) % 2)},
              "p_i64": {"type": "i64", "value": str(BOUNDARIES[(number + seed) % len(BOUNDARIES)])},
              "p_f64": f64_cell((0.0, 1.5, -2.5, 1 / 1024, (1 << 30) + 0.25)[(number + seed) % 5]),
              "p_text": {"type": "text", "value": f"{kind}:{number}:seed={seed}:café\nquoted=\"yes\""}}
    if number % 3:
        result["p_optional"] = {"type": "i64", "value": str(number % 17)}
    return result


def node(number, seed):
    labels = ["Entity", f"Label{(number + seed) % 3}"]
    if number % 4 == 0:
        labels.append("Tagged")
    return {"kind": "node", "id": identifier("node", number), "labels": sorted(labels), "properties": properties("node", number, seed)}


def edge(number, nodes, seed):
    if number < 2:
        source, target, typename = 0, min(1, nodes - 1), "HOP"
    elif number == 2:
        source, target, typename = 0, 0, "HOP"
    else:
        source = mix(seed ^ (number * 2)) % nodes
        target = mix(seed ^ (number * 2 + 1)) % nodes
        typename = ("HOP", "REL1", "REL2")[number % 3]
    return {"kind": "edge", "id": identifier("edge", number), "source": identifier("node", source), "target": identifier("node", target), "type": typename, "properties": properties("edge", 0 if number < 2 else number, seed)}


def appended_edge(number, nodes, seed):
    return {"kind": "edge", "id": identifier("edge", number), "source": identifier("node", nodes), "target": identifier("node", 0 if nodes else nodes), "type": "HOP", "properties": properties("edge", number, seed)}


def mutations(nodes, edges, seed):
    yield {"revision": 1, "op": "append", "row": node(nodes, seed)}
    for number in (edges, edges + 1):
        yield {"revision": 1, "op": "append", "row": appended_edge(number, nodes, seed)}
    if nodes:
        replacement = (1 << 53) + 1
        if properties("node", 0, seed)["p_i64"]["value"] == str(replacement):
            replacement = -replacement
        yield {"revision": 2, "op": "update", "kind": "node", "id": identifier("node", 0), "set": {"p_i64": {"type": "i64", "value": str(replacement)}}, "remove": ["p_bool"]}
    if edges:
        yield {"revision": 2, "op": "update", "kind": "edge", "id": identifier("edge", 0), "set": {"p_text": {"type": "text", "value": "edge updated café"}}, "remove": []}
    upsert = node(nodes, seed)
    upsert["properties"]["p_text"] = {"type": "text", "value": "upserted text 🙂"}
    yield {"revision": 2, "op": "upsert", "row": upsert}
    if edges > 1:
        yield {"revision": 3, "op": "delete", "kind": "edge", "id": identifier("edge", 1)}
    if nodes:
        deleted = identifier("node", nodes - 1)
        for number in range(edges + 2):
            if number == 1 and edges > 1:
                continue
            row = edge(number, nodes, seed) if number < edges else appended_edge(number, nodes, seed)
            if deleted in (row["source"], row["target"]):
                yield {"revision": 3, "op": "delete", "kind": "edge", "id": row["id"]}
        yield {"revision": 3, "op": "delete", "kind": "node", "id": deleted}


def workloads(nodes, edges, seed, max_depth):
    specs = [
        ("node.lookup.first", "entity", {"op": "lookup", "kind": "node", "id": identifier("node", 0)}),
        ("node.lookup.deleted", "entity", {"op": "lookup", "kind": "node", "id": identifier("node", max(0, nodes - 1))}),
        ("node.lookup.absent", "entity", {"op": "lookup", "kind": "node", "id": identifier("node", nodes + 1)}),
        ("edge.lookup.first", "entity", {"op": "lookup", "kind": "edge", "id": identifier("edge", 0)}),
        ("edge.lookup.parallel", "entity", {"op": "lookup", "kind": "edge", "id": identifier("edge", 1)}),
        ("edge.lookup.absent", "entity", {"op": "lookup", "kind": "edge", "id": identifier("edge", edges + 2)}),
        ("node.label", "id", {"op": "label", "label": "Label1"}),
        ("edge.type", "id", {"op": "type", "type": "HOP"}),
        ("node.eq.bool", "id", {"op": "equality", "kind": "node", "key": "p_bool", "value": {"type": "bool", "value": True}}),
        ("node.eq.false", "id", {"op": "equality", "kind": "node", "key": "p_bool", "value": {"type": "bool", "value": False}}),
        ("node.eq.zero", "id", {"op": "equality", "kind": "node", "key": "p_f64", "value": f64_cell(0.0)}),
        ("node.eq.i64", "id", {"op": "equality", "kind": "node", "key": "p_i64", "value": {"type": "i64", "value": str((1 << 53) + 1)}}),
        ("edge.eq.i64", "id", {"op": "equality", "kind": "edge", "key": "p_i64", "value": {"type": "i64", "value": str(-(1 << 63))}}),
        ("node.eq.text", "id", {"op": "equality", "kind": "node", "key": "p_text", "value": properties("node", 0, seed)["p_text"]}),
        ("node.range.i64", "id", {"op": "range", "kind": "node", "key": "p_i64", "low": {"type": "i64", "value": str(-(1 << 53))}, "high": {"type": "i64", "value": str(1 << 53)}}),
        ("edge.range.f64", "id", {"op": "range", "kind": "edge", "key": "p_f64", "low": f64_cell(-3.0), "high": f64_cell(2.0)}),
        ("node.adjacency.out", "adjacency", {"op": "adjacency", "direction": "out", "id": identifier("node", 0)}),
        ("node.adjacency.in", "adjacency", {"op": "adjacency", "direction": "in", "id": identifier("node", 0)}),
        ("node.expand", "walk", {"op": "expand", "id": identifier("node", 0), "max_depth": max_depth}),
        ("node.project", "projection", {"op": "projection", "kind": "node", "keys": ["p_bool", "p_i64", "p_optional", "p_never"]}),
        ("edge.project", "projection", {"op": "projection", "kind": "edge", "keys": ["p_i64", "p_text", "p_never"]}),
        ("node.full", "entity", {"op": "scan", "kind": "node"}),
        ("edge.full", "entity", {"op": "scan", "kind": "edge"}),
    ]
    return [{"name": name, "shape": shape, "request": request} for name, shape, request in specs]


def write_rows(path, rows, limit=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    count, size = 0, 0
    with path.open("wb") as output:
        for row in rows:
            count += 1
            if limit is not None and count > limit:
                raise ContractError("complete output exceeds declared row budget; dataset is not published")
            data = (canonical(row) + "\n").encode("utf-8")
            output.write(data)
            digest.update(data)
            size += len(data)
    return {"rows": count, "bytes": size, "sha256": digest.hexdigest()}


def write_json(path, value):
    data = (canonical(value) + "\n").encode("utf-8")
    path.write_bytes(data)
    return {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def generate(output, nodes, edges, seed=0, source_signal_rows=None, max_depth=2, max_output_rows=5_000_000):
    if any(type(value) is not int or value < 0 for value in (nodes, edges, seed)) or nodes > I64_MAX - 2 or edges > I64_MAX - 2 or seed > MASK or (edges and not nodes) or type(max_depth) is not int or not 1 <= max_depth <= 4 or type(max_output_rows) is not int or max_output_rows < 1:
        raise ContractError("invalid graph count/seed/declared workload bound")
    if source_signal_rows is not None and (type(source_signal_rows) is not int or source_signal_rows < 0):
        raise ContractError("source signal count must be explicitly nonnegative or unavailable")
    output = Path(output)
    if output.exists():
        raise ContractError("refuse to overwrite an existing output directory")
    output.parent.mkdir(parents=True, exist_ok=True)
    source = Path(__file__).resolve().parent
    source_names = ("generate.py", "oracle.py", "format.py", "normalize.py", "contract-pin.json")
    source_hashes = {name: hashlib.sha256((source / name).read_bytes()).hexdigest() for name in source_names}
    temporary = Path(tempfile.mkdtemp(prefix=".graph-input-", dir=output.parent))
    oracle = None
    try:
        files = {}
        files["nodes.jsonl"] = write_rows(temporary / "nodes.jsonl", (node(number, seed) for number in range(nodes)))
        files["edges.jsonl"] = write_rows(temporary / "edges.jsonl", (edge(number, nodes, seed) for number in range(edges)))
        files["changes.jsonl"] = write_rows(temporary / "changes.jsonl", mutations(nodes, edges, seed))
        oracle = Oracle(temporary / "oracle.sqlite")
        oracle.load(temporary / "nodes.jsonl", temporary / "edges.jsonl")
        revisions, queries = [], []
        change_source = (temporary / "changes.jsonl").open(encoding="utf-8")
        try:
            next_event = json.loads(next(change_source))
            for revision in range(4):
                while next_event is not None and next_event["revision"] <= revision:
                    oracle.apply(next_event)
                    line = next(change_source, None)
                    next_event = json.loads(line) if line is not None else None
                oracle.commit()
                revisions.append({"revision": revision, **oracle.ledger()})
                for spec in workloads(nodes, edges, seed, max_depth):
                    query_id = f"r{revision}.{spec['name']}"
                    filename = "expected/" + query_id + ".jsonl"
                    files[filename] = write_rows(temporary / filename, oracle.answer(spec["request"]), max_output_rows)
                    queries.append({"id": query_id, "revision": revision, "lane": "basic-graph-current", "request": spec["request"], "shape": spec["shape"], "expected_file": filename, "expected": files[filename]})
        finally:
            change_source.close()
        files["retained-revisions.jsonl"] = write_rows(temporary / "retained-revisions.jsonl", oracle.answer({"op": "history"}), max_output_rows)
        queries.append({"id": "history.retained-versions", "revision": 3, "lane": "retained-history-feature", "request": {"op": "history"}, "shape": "revision", "expected_file": "retained-revisions.jsonl", "expected": files["retained-revisions.jsonl"]})
        for kind, name in (("node", "current-nodes.jsonl"), ("edge", "current-edges.jsonl")):
            files[name] = write_rows(temporary / name, oracle.answer({"op": "scan", "kind": kind}), max_output_rows)
        oracle.close()
        oracle = None
        workspace_bytes = (temporary / "oracle.sqlite").stat().st_size
        (temporary / "oracle.sqlite").unlink()
        write_json(temporary / "oracle-costs.json", {"schema_version": 1, "scope": "generator/oracle workspace only; not graph-engine storage", "sqlite_version": sqlite3.sqlite_version, "final_oracle_database_bytes": workspace_bytes, "peak_disk_bytes": None, "peak_memory_bytes": None, "temporary_sort_bytes": None, "canonical_input_identity": False})
        feature_cases = {"mapping_version": MAPPING_VERSION, "lane": "additional-feature-reference", "not_in_basic_graph_counts": True, "native_vendor_support": "unverified; lossless explicit mappings required", "cases": [
            {"id": "presence-states", "ordered_states": [{"presence": "absent"}, {"presence": "present", "value": {"type": "null"}}, {"presence": "removed"}], "requirement": "never equate present-null, never-present and explicit removal"},
            {"id": "source-negative-zero-bits", "source_bits": "8000000000000000", "basic_numeric_zero_bits": "0000000000000000", "requirement": "source bit preservation is separate from common native numeric-zero semantics"}], "temporal_scope": "integer workload revision history only; no certified cuts, rationals, uncertainty/lives/provenance adapter acceptance"}
        files["feature-cases.json"] = write_json(temporary / "feature-cases.json", feature_cases)
        files["queries.json"] = write_json(temporary / "queries.json", queries)
        if source_hashes != {name: hashlib.sha256((source / name).read_bytes()).hexdigest() for name in source_names}:
            raise ContractError("generator/reference source changed during emission")
        input_identity = hashlib.sha256(canonical({"mapping_version": MAPPING_VERSION, "seed": seed, "nodes": nodes, "edges": edges, "nodes_sha256": files["nodes.jsonl"]["sha256"], "edges_sha256": files["edges.jsonl"]["sha256"], "changes_sha256": files["changes.jsonl"]["sha256"]}).encode("utf-8")).hexdigest()
        manifest = {"canonical_input_sha256": input_identity, "schema_version": 1, "mapping_version": MAPPING_VERSION, "status": "synthetic-input-and-exact-answer-reference-only", "seed": seed,
            "fixture_guarantees": {"parallel_equal_payload_pair": bool(edges >= 2), "self_edge": bool(edges >= 3), "int64_boundaries_all_present_in_nodes": bool(nodes >= len(BOUNDARIES))},
            "declared_initial_graph": {"nodes": nodes, "edges": edges}, "source_signal_rows": {"count": source_signal_rows, "status": "explicitly-declared-inventory-no-conversion" if source_signal_rows is not None else "unavailable-graph-only-fixture", "synthday_equivalence_claimed": False},
            "revisions": revisions, "files": dict(sorted(files.items())), "generator_source_sha256": source_hashes, "source_contract_pin": json.loads((source / "contract-pin.json").read_text()),
            "query_semantics": {"order": "complete multiset, order-independent normalization", "i64_range": "closed bounds; exact signed int64 biased TEXT ordering, no REAL parsing", "f64": "finite binary64, common numeric zero; source-bit lane separate", "expansion": "directed walks distinguished by complete edge-ID sequences; nodes/edges may repeat; depths 1..max_depth", "projection": "requested columns always explicit present/absent; removal reason lives in retained revisions"},
            "limits": {"max_expansion_depth": max_depth, "max_complete_output_rows_per_query": max_output_rows, "sqlite_cache_target_kib": 4096},
            "accounting": {"property_payload_bytes": "bool=1, signed i64=8, finite f64=8, text=exact UTF-8 value bytes; excludes keys/IDs/labels/wire metadata", "property_keys_and_label_type_names": "exact UTF-8 bytes separately", "current_vs_history": "all emitted revisions/tombstones retained; separate feature lane, never counted equivalent to current-only engines", "oracle_cost_record": "oracle-costs.json", "oracle_cost_record_excluded_from_canonical_identity": True, "workspace_policy": "discardable SQLite disk/index/temporary work and expected exports are generator/oracle costs, not future engine costs", "physical_vendor_reified_reverse_auxiliary_records": None, "per_copy_replica_engine_totals": None},
            "vendor_support": {name: "native ranges/mappings/durability unverified; adapter validation required" for name in ("rho-v5", "rho-v4", "Neo4j", "TigerGraph", "Memgraph")},
            "benchmark_harness_implemented": False, "vendor_adapters_implemented": False, "benchmark_runs": [], "V7_complete": False}
        write_json(temporary / "manifest.json", manifest)
        temporary.rename(output)
        return manifest
    except Exception:
        if oracle is not None:
            oracle.close()
        shutil.rmtree(temporary)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    parser.add_argument("--nodes", type=int, required=True)
    parser.add_argument("--edges", type=int, required=True)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--source-signal-rows", type=int)
    parser.add_argument("--max-depth", type=int, default=2)
    parser.add_argument("--max-output-rows", type=int, default=5_000_000)
    args = parser.parse_args()
    try:
        manifest = generate(args.out, args.nodes, args.edges, args.seed, args.source_signal_rows, args.max_depth, args.max_output_rows)
    except (ContractError, OSError, sqlite3.Error) as error:
        parser.exit(1, "generation refused: " + str(error) + "\n")
    print(canonical({"mapping_version": manifest["mapping_version"], "initial": manifest["revisions"][0], "current": manifest["revisions"][-1], "status": manifest["status"]}))



if __name__ == "__main__":
    main()
