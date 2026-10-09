"""Literal reference cases and adversarial output faults, not mirrored generator tests."""
import copy
import hashlib
from itertools import islice
import json
import sqlite3
import shutil
import tempfile
import tracemalloc
import unittest
from pathlib import Path

import generate
from format import ContractError, I64_MAX, I64_MIN, canonical, cell, f64_cell, int_key, parse
from normalize import compare_export, compare_files
from oracle import Oracle

N = ["n:0000000000000000", "n:0000000000000001", "n:0000000000000002", "n:0000000000000003"]
E = [f"e:{index:016x}" for index in range(6)]


def literal_rows():
    nodes = []
    for identifier, label, rank, flag, name in [(N[0], "A", I64_MIN, False, "α"), (N[1], "B", 1 << 53, True, "β"), (N[2], "C", (1 << 53) + 1, False, "γ")]:
        props = {"rank": {"type": "i64", "value": str(rank)}, "flag": {"type": "bool", "value": flag}, "name": {"type": "text", "value": name}, "weight": f64_cell(1.5)}
        if identifier == N[2]:
            props["limit"] = {"type": "i64", "value": str(I64_MAX)}
        nodes.append({"kind": "node", "id": identifier, "labels": ["Entity", label], "properties": props})
    edges = [{"kind": "edge", "id": identifier, "source": source, "target": target, "type": "HOP", "properties": {"w": {"type": "i64", "value": "1"}}} for identifier, source, target in [(E[0], N[0], N[1]), (E[1], N[0], N[1]), (E[2], N[0], N[0]), (E[3], N[1], N[2]), (E[4], N[1], N[0])]]
    return nodes, edges


def write(path, rows):
    Path(path).write_text("".join(canonical(row) + "\n" for row in rows), encoding="utf-8")


class OracleTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.nodes, self.edges = literal_rows()
        write(self.root / "nodes.jsonl", self.nodes)
        write(self.root / "edges.jsonl", self.edges)
        self.oracle = Oracle(self.root / "oracle.sqlite")
        self.addCleanup(self.oracle.close)
        self.oracle.load(self.root / "nodes.jsonl", self.root / "edges.jsonl")

    def test_literal_integer_range_and_complete_walk_multiset(self):
        exact = list(self.oracle.answer({"op": "range", "kind": "node", "key": "rank", "low": {"type": "i64", "value": str((1 << 53) + 1)}, "high": {"type": "i64", "value": str((1 << 53) + 1)}}))
        self.assertEqual(exact, [{"kind": "node", "id": N[2]}])
        out = list(self.oracle.answer({"op": "adjacency", "direction": "out", "id": N[0]}))
        self.assertEqual([row["id"] for row in out], E[:3])
        incoming = list(self.oracle.answer({"op": "adjacency", "direction": "in", "id": N[0]}))
        self.assertEqual([row["id"] for row in incoming], [E[2], E[4]])
        walks = list(self.oracle.answer({"op": "expand", "id": N[0], "max_depth": 2}))
        expected = {(E[0],), (E[0], E[3]), (E[0], E[4]), (E[1],), (E[1], E[3]), (E[1], E[4]), (E[2],), (E[2], E[0]), (E[2], E[1]), (E[2], E[2])}
        self.assertEqual({tuple(row["edge_ids"]) for row in walks}, expected)
        self.assertEqual(len(walks), 10)
        self.assertEqual([row["id"] for row in self.oracle.answer({"op": "label", "label": "Entity"})], N[:3])
        self.assertEqual([row["id"] for row in self.oracle.answer({"op": "type", "type": "HOP"})], E[:5])
        self.assertEqual(list(self.oracle.answer({"op": "lookup", "kind": "node", "id": N[3]})), [])
        initial_projection = list(self.oracle.answer({"op": "projection", "kind": "node", "keys": ["flag", "never"]}))[0]
        self.assertEqual(initial_projection["columns"], {"flag": {"presence": "present", "value": {"type": "bool", "value": False}}, "never": {"presence": "absent"}})

    def test_literal_exact_ledger_values_not_wire_or_source_rows(self):
        ledger = self.oracle.ledger()
        self.assertEqual((ledger["current_nodes"], ledger["current_edges"], ledger["label_memberships"], ledger["type_memberships"], ledger["self_edges"], ledger["parallel_edge_excess"]), (3, 5, 6, 5, 1, 1))
        self.assertEqual(ledger["property_entries"], {"node": {"bool": 3, "f64": 3, "i64": 4, "text": 3}, "edge": {"i64": 5}})
        self.assertEqual(ledger["property_payload_bytes"], {"node": {"bool": 3, "f64": 24, "i64": 32, "text": 6}, "edge": {"i64": 40}})
        self.assertEqual(ledger["property_key_utf8_bytes"], {"node": 59, "edge": 5})
        self.assertEqual(ledger["label_type_utf8_bytes"], 36)
        self.assertEqual(ledger["retained_revisions"], 8)
        self.assertEqual(ledger["retained_revisions_by_kind"], {"edge": 5, "node": 3})
        self.assertEqual(ledger["label_memberships_by_label"], {"A": 1, "B": 1, "C": 1, "Entity": 3})
        self.assertEqual(ledger["edge_identities_by_type"], {"HOP": 5})
        self.assertEqual(ledger["retained_property_entries"], ledger["property_entries"])
        self.assertEqual(ledger["retained_property_payload_bytes"], ledger["property_payload_bytes"])
        self.assertEqual(self.oracle.db.execute("SELECT typeof(id) FROM entities LIMIT 1").fetchone()[0], "text")
        self.assertEqual(self.oracle.db.execute("SELECT typeof(int_order) FROM properties WHERE type='i64' LIMIT 1").fetchone()[0], "text")
        self.assertEqual(self.oracle.db.execute("PRAGMA cache_size").fetchone()[0], -4096)

    def test_mutation_history_removal_and_deleted_parallel_phantom(self):
        self.oracle.apply({"revision": 1, "op": "delete", "kind": "edge", "id": E[1]})
        self.oracle.apply({"revision": 1, "op": "update", "kind": "node", "id": N[0], "set": {"rank": {"type": "i64", "value": str((1 << 53) + 1)}}, "remove": ["flag"]})
        current = list(self.oracle.answer({"op": "lookup", "kind": "node", "id": N[0]}))[0]
        self.assertNotIn("flag", current["properties"])
        self.assertEqual(current["properties"]["rank"]["value"], str((1 << 53) + 1))
        versions = [row for row in self.oracle.answer({"op": "history"}) if row["kind"] == "node" and row["id"] == N[0]]
        self.assertEqual(len(versions), 2)
        self.assertEqual(versions[0]["row"], {**self.nodes[0], "labels": ["A", "Entity"]})
        self.assertEqual(versions[1]["property_changes"]["remove"], ["flag"])
        projection = list(self.oracle.answer({"op": "projection", "kind": "node", "keys": ["flag", "never"]}))[0]
        self.assertEqual(projection["columns"], {"flag": {"presence": "absent"}, "never": {"presence": "absent"}})
        self.assertEqual(list(self.oracle.answer({"op": "lookup", "kind": "edge", "id": E[1]})), [])
        out = list(self.oracle.answer({"op": "adjacency", "direction": "out", "id": N[0]}))
        self.assertEqual([row["id"] for row in out], [E[0], E[2]])
        self.assertEqual(self.oracle.ledger()["retained_revisions"], 10)
        self.assertEqual(self.oracle.ledger()["deleted_edge_identities"], 1)
        expected, actual = self.root / "expected.jsonl", self.root / "actual.jsonl"
        write(expected, out)
        write(actual, [*out, {key: self.edges[1][key] for key in ("id", "source", "target", "type")}])
        with self.assertRaises(ContractError):
            compare_files(expected, actual, "adjacency")

    def test_rejected_mutation_rolls_back_without_inventing_identity(self):
        before = list(self.oracle.answer({"op": "scan", "kind": "node"}))
        with self.assertRaises(ContractError):
            self.oracle.apply({"revision": 1, "op": "delete", "kind": "node", "id": N[0]})
        self.assertEqual(list(self.oracle.answer({"op": "scan", "kind": "node"})), before)
        self.oracle.apply({"revision": 1, "op": "update", "kind": "node", "id": N[0], "set": {"rank": {"type": "i64", "value": "1"}}, "remove": []})
        committed = list(self.oracle.answer({"op": "scan", "kind": "node"}))
        with self.assertRaises(sqlite3.IntegrityError):
            self.oracle.apply({"revision": 1, "op": "update", "kind": "node", "id": N[0], "set": {"rank": {"type": "i64", "value": "2"}}, "remove": []})
        self.assertEqual(list(self.oracle.answer({"op": "scan", "kind": "node"})), committed)
        with self.assertRaises(ContractError):
            self.oracle.apply({"revision": 2, "op": "delete", "kind": "edge", "id": E[5]})
        self.assertEqual(self.oracle.ledger()["retained_revisions"], 9)
        rewired = copy.deepcopy(self.edges[0])
        rewired["target"] = N[2]
        with self.assertRaises(ContractError):
            self.oracle.apply({"revision": 2, "op": "upsert", "row": rewired})
        self.assertEqual(list(self.oracle.answer({"op": "lookup", "kind": "edge", "id": E[0]}))[0]["target"], N[1])


class NormalizationTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.expected = self.root / "expected.jsonl"
        self.actual = self.root / "actual.jsonl"

    def test_reorder_native_exact_int_and_numeric_zero_allowed(self):
        nodes, _ = literal_rows()
        write(self.expected, nodes)
        native = copy.deepcopy(nodes)
        for row in native:
            row["properties"]["rank"]["value"] = int(row["properties"]["rank"]["value"])
        write(self.actual, reversed(native))
        self.assertEqual(compare_files(self.expected, self.actual, "entity")["rows"], 3)
        a = copy.deepcopy(nodes[:1])
        b = copy.deepcopy(a)
        a[0]["properties"]["weight"] = {"type": "f64", "bits": "0000000000000000"}
        b[0]["properties"]["weight"] = {"type": "f64", "bits": "8000000000000000"}
        write(self.expected, a)
        write(self.actual, b)
        self.assertEqual(compare_files(self.expected, self.actual, "entity")["rows"], 1)
        a[0]["properties"]["name"] = {"type": "text", "value": ""}
        b[0]["properties"]["name"] = {"type": "text", "value": ""}
        write(self.expected, a)
        write(self.actual, b)
        self.assertEqual(compare_files(self.expected, self.actual, "entity")["rows"], 1)
        del b[0]["properties"]["name"]
        write(self.actual, b)
        with self.assertRaises(ContractError):
            compare_files(self.expected, self.actual, "entity")

    def test_kill_parallel_collapse_omitted_false_and_deleted_phantom_faults(self):
        nodes, edges = literal_rows()
        for fault in ("collapse", "omit-edge", "duplicate-edge", "omit-false", "phantom"):
            with self.subTest(fault=fault):
                expected = edges if fault in ("collapse", "omit-edge", "duplicate-edge") else nodes
                actual = copy.deepcopy(expected)
                if fault == "collapse":
                    actual = list({(row["source"], row["target"], row["type"]): row for row in actual}.values())
                elif fault == "omit-edge":
                    actual.pop(0)
                elif fault == "duplicate-edge":
                    actual.append(actual[0])
                elif fault == "omit-false":
                    del actual[0]["properties"]["flag"]
                else:
                    actual.append({"kind": "node", "id": N[3], "labels": [], "properties": {}})
                write(self.expected, expected)
                write(self.actual, actual)
                with self.assertRaises(ContractError):
                    compare_files(self.expected, self.actual, "entity")

    def test_kill_wrong_signed_integer_precision_and_type_faults(self):
        nodes, _ = literal_rows()
        for value in (float(I64_MIN), True, str(1 << 63), "-0", "01"):
            with self.subTest(value=value):
                actual = copy.deepcopy(nodes)
                actual[0]["properties"]["rank"]["value"] = value
                write(self.expected, nodes)
                write(self.actual, actual)
                with self.assertRaises(ContractError):
                    compare_files(self.expected, self.actual, "entity")
        actual = copy.deepcopy(nodes)
        actual[2]["properties"]["rank"]["value"] = str(1 << 53)
        write(self.expected, nodes)
        write(self.actual, actual)
        with self.assertRaises(ContractError):
            compare_files(self.expected, self.actual, "entity")
        actual[2]["properties"]["rank"] = f64_cell(float((1 << 53) + 1))
        write(self.actual, actual)
        with self.assertRaises(ContractError):
            compare_files(self.expected, self.actual, "entity")

    def test_reject_null_presence_nonfinite_duplicate_json_keys(self):
        self.assertRaises(ContractError, cell, {"type": "null", "value": None})
        for bits in ("7ff0000000000000", "7ff8000000000000", "fff0000000000000"):
            self.assertRaises(ContractError, cell, {"type": "f64", "bits": bits})
        self.assertRaises(ContractError, parse, '{"id":"one","id":"two"}')
        self.assertRaises(ContractError, parse, '{"value":NaN}')
        write(self.expected, [{"kind": "node", "id": N[0], "columns": {"flag": {"presence": "absent"}}}])
        write(self.actual, [{"kind": "node", "id": N[0], "columns": {"flag": {"presence": "present", "value": {"type": "null"}}}}])
        with self.assertRaises(ContractError):
            compare_files(self.expected, self.actual, "projection")

    def test_int64_order_is_literal_total_order_without_real_conversion(self):
        values = [I64_MIN, -(1 << 53) - 1, -(1 << 53), -1, 0, 1, 1 << 53, (1 << 53) + 1, I64_MAX]
        self.assertEqual(sorted(values, key=int_key), values)
        self.assertEqual(len({int_key(value) for value in values}), len(values))
        self.assertEqual(int_key(I64_MIN), "0000000000000000")
        self.assertEqual(int_key(I64_MAX), "ffffffffffffffff")


class DatasetIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.dataset = self.root / "original"
        self.manifest = generate.generate(self.dataset, 2, 3)
        self.metadata = {"mapping_version": "basic-graph-v1", "dataset_sha256": self.manifest["canonical_input_sha256"],
                         "query_id": "r3.edge.full", "lane": "basic-graph-current"}
        self.actual = self.root / "adapter.jsonl"
        shutil.copyfile(self.dataset / "expected/r3.edge.full.jsonl", self.actual)

    def clone(self, name):
        target = self.root / name
        shutil.copytree(self.dataset, target)
        return target

    def compare(self, dataset):
        return compare_export(dataset, "r3.edge.full", self.actual, self.metadata)

    def test_changed_or_missing_bound_files_refuse_before_equal_export(self):
        self.assertEqual(self.compare(self.dataset)["rows"], 3)
        for name in self.manifest["files"]:
            for mode in ("changed", "missing"):
                with self.subTest(name=name, mode=mode):
                    target = self.clone(str(len(list(self.root.iterdir()))))
                    file = target / name
                    if mode == "changed":
                        file.write_bytes(file.read_bytes() + b"\n")
                    else:
                        file.unlink()
                    with self.assertRaises((ContractError, OSError)):
                        self.compare(target)

    def test_extra_undeclared_or_extra_declared_data_refuses(self):
        for declared in (False, True):
            with self.subTest(declared=declared):
                target = self.clone("extra-" + str(declared))
                extra = target / "unexpected.jsonl"
                extra.write_text("{}\n")
                if declared:
                    manifest = parse((target / "manifest.json").read_text())
                    manifest["files"][extra.name] = {"rows": 1, "bytes": 3, "sha256": hashlib.sha256(extra.read_bytes()).hexdigest()}
                    (target / "manifest.json").write_text(canonical(manifest) + "\n")
                with self.assertRaises(ContractError):
                    self.compare(target)

    def test_rewritten_input_file_ledger_cannot_keep_old_input_identity(self):
        target = self.clone("rewritten-input")
        file = target / "nodes.jsonl"
        rows = [parse(line) for line in file.read_text().splitlines()]
        rows[0]["properties"]["p_text"]["value"] = "changed source payload"
        write(file, rows)
        manifest = parse((target / "manifest.json").read_text())
        manifest["files"]["nodes.jsonl"] = {"rows": len(rows), "bytes": file.stat().st_size, "sha256": hashlib.sha256(file.read_bytes()).hexdigest()}
        (target / "manifest.json").write_text(canonical(manifest) + "\n")
        with self.assertRaises(ContractError):
            self.compare(target)

    def test_stale_row_counts_input_counts_and_query_metadata_refuse(self):
        for fault in ("row-count", "input-count", "query-expected", "extra-answer"):
            with self.subTest(fault=fault):
                target = self.clone(fault)
                manifest = parse((target / "manifest.json").read_text())
                if fault == "row-count":
                    manifest["files"]["nodes.jsonl"]["rows"] += 1
                elif fault == "input-count":
                    manifest["declared_initial_graph"]["nodes"] += 1
                elif fault == "query-expected":
                    file = target / "queries.json"
                    queries = parse(file.read_text())
                    queries[0]["expected"]["rows"] += 1
                    file.write_text(canonical(queries) + "\n")
                    manifest["files"]["queries.json"] = {"bytes": file.stat().st_size, "sha256": hashlib.sha256(file.read_bytes()).hexdigest()}
                else:
                    file = target / "expected/extra.jsonl"
                    file.write_text("")
                    manifest["files"]["expected/extra.jsonl"] = {"rows": 0, "bytes": 0, "sha256": hashlib.sha256(b"").hexdigest()}
                (target / "manifest.json").write_text(canonical(manifest) + "\n")
                with self.assertRaises(ContractError):
                    self.compare(target)


class GeneratorTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)

    def test_deterministic_all_bytes_exact_counts_and_source_inventory(self):
        a = generate.generate(self.root / "a", 26, 40, seed=11)
        b = generate.generate(self.root / "b", 26, 40, seed=11)
        files_a = {str(path.relative_to(self.root / "a")): hashlib.sha256(path.read_bytes()).hexdigest() for path in (self.root / "a").rglob("*") if path.is_file()}
        files_b = {str(path.relative_to(self.root / "b")): hashlib.sha256(path.read_bytes()).hexdigest() for path in (self.root / "b").rglob("*") if path.is_file()}
        self.assertEqual(files_a, files_b)
        self.assertEqual(a, b)
        self.assertEqual((a["revisions"][0]["current_nodes"], a["revisions"][0]["current_edges"]), (26, 40))
        self.assertGreaterEqual(a["revisions"][0]["parallel_edge_excess"], 1)
        self.assertGreaterEqual(a["revisions"][0]["self_edges"], 1)
        with (self.root / "a/edges.jsonl").open(encoding="utf-8") as source:
            pair = [parse(line) for line in islice(source, 2)]
        self.assertNotEqual(pair[0]["id"], pair[1]["id"])
        self.assertEqual({key: value for key, value in pair[0].items() if key != "id"}, {key: value for key, value in pair[1].items() if key != "id"})
        self.assertIsNone(a["source_signal_rows"]["count"])
        self.assertFalse(a["source_signal_rows"]["synthday_equivalence_claimed"])
        declared = generate.generate(self.root / "declared", 2, 3, source_signal_rows=790_123)
        self.assertEqual(declared["source_signal_rows"]["count"], 790_123)
        self.assertEqual(declared["declared_initial_graph"], {"nodes": 2, "edges": 3})
        rows = [parse(line) for line in (self.root / "a/nodes.jsonl").read_text().splitlines()]
        self.assertEqual({int(row["properties"]["p_i64"]["value"]) for row in rows}, set(generate.BOUNDARIES))
        self.assertEqual(len(json.loads((self.root / "a/queries.json").read_text())), 93)
        for filename, entry in a["files"].items():
            data = (self.root / "a" / filename).read_bytes()
            self.assertEqual(entry["sha256"], hashlib.sha256(data).hexdigest())
            self.assertEqual(entry["bytes"], len(data))
            if "rows" in entry:
                self.assertEqual(entry["rows"], len(data.splitlines()))

    def test_post_update_delete_expected_rows_and_retained_presence(self):
        manifest = generate.generate(self.root / "data", 2, 3, seed=0)
        base, current = manifest["revisions"][0], manifest["revisions"][-1]
        self.assertEqual((base["current_nodes"], base["current_edges"]), (2, 3))
        self.assertEqual((current["current_nodes"], current["current_edges"], current["deleted_node_identities"], current["deleted_edge_identities"]), (2, 3, 1, 2))
        versions = [parse(line) for line in (self.root / "data/retained-revisions.jsonl").read_text().splitlines()]
        self.assertEqual(current["retained_revisions"], len(versions))
        removed = [row for row in versions if row["property_changes"].get("remove") == ["p_bool"]]
        self.assertEqual(len(removed), 1)
        self.assertEqual((self.root / "data/expected/r3.node.lookup.deleted.jsonl").read_text(), "")
        self.assertEqual((self.root / "data/expected/r3.edge.lookup.parallel.jsonl").read_text(), "")
        expected = self.root / "data/expected/r3.edge.full.jsonl"
        result = compare_export(self.root / "data", "r3.edge.full", expected, {"mapping_version": "basic-graph-v1", "dataset_sha256": manifest["canonical_input_sha256"], "query_id": "r3.edge.full", "lane": "basic-graph-current"})
        self.assertEqual(result["rows"], 3)
        self.assertEqual(result["dataset_sha256"], manifest["canonical_input_sha256"])
        with self.assertRaises(ContractError):
            compare_export(self.root / "data", "r3.edge.full", expected, {"mapping_version": "basic-graph-v1", "dataset_sha256": "0" * 64, "query_id": "r3.edge.full", "lane": "basic-graph-current"})
        with self.assertRaises(ContractError):
            compare_export(self.root / "data", "r3.edge.full", expected, {"mapping_version": "wrong", "dataset_sha256": manifest["canonical_input_sha256"], "query_id": "r3.edge.full", "lane": "basic-graph-current"})

    def test_empty_graph_bounds_atomic_output_budget_and_no_overwrite(self):
        empty = generate.generate(self.root / "empty", 0, 0)
        self.assertEqual((empty["revisions"][0]["current_nodes"], empty["revisions"][0]["current_edges"]), (0, 0))
        self.assertEqual((empty["revisions"][-1]["current_nodes"], empty["revisions"][-1]["current_edges"]), (1, 2))
        for nodes, edges, seed in ((0, 1, 0), (-1, 0, 0), (1, -1, 0), (1, 0, -1), (1, 0, 1 << 64)):
            with self.assertRaises(ContractError):
                generate.generate(self.root / "invalid", nodes, edges, seed)
        with self.assertRaises(ContractError):
            generate.generate(self.root / "budget", 4, 8, max_output_rows=1)
        self.assertFalse((self.root / "budget").exists())
        self.assertEqual(list(self.root.glob(".graph-input-*")), [])
        before = (self.root / "empty/manifest.json").read_bytes()
        with self.assertRaises(ContractError):
            generate.generate(self.root / "empty", 1, 0)
        self.assertEqual((self.root / "empty/manifest.json").read_bytes(), before)

    def test_python_generation_memory_does_not_retain_large_row_maps(self):
        tracemalloc.start()
        try:
            manifest = generate.generate(self.root / "bounded", 500, 2000, seed=23)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(manifest["revisions"][0]["current_edges"], 2000)
        self.assertLess(peak, 12 << 20)
        self.assertFalse((self.root / "bounded/oracle.sqlite").exists())
        self.assertGreater(json.loads((self.root / "bounded/oracle-costs.json").read_text())["final_oracle_database_bytes"], 0)
        # Python allocation guard only; not RSS or a graph-engine memory claim.


if __name__ == "__main__":
    unittest.main()
