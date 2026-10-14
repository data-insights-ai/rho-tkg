"""Compact retained grammar guards; no engine, generated archive or qualification."""
import base64
import copy
import hashlib
from pathlib import Path
import tempfile
import unittest

from compare_cost_cdc_test import fdb_replay, module, raw_fixture
import fdb_retention_check as retained


class RetainedOriginals(unittest.TestCase):
    def test_retired_row_requires_its_exact_genuine_current_ancestor(self):
        c = module("retention-ancestor-guard", "compare-cost-cdc.py")
        groups, baseline, _ = fdb_replay(True)
        result, _, streams = raw_fixture(groups, baseline)
        result["Retention"] = {}
        with tempfile.TemporaryDirectory() as tmp, c.Spool(tmp, c.Policy(2 << 20, 16 << 10, 4096, 512 << 10)) as spool:
            for raw in streams["cdc"]:
                group = c.decoded_body(c.raw_bytes(raw["Value"]))
                o = group["Outcome"]
                spool.insert(b"retention-cdc", c.u64(o["Coordinator"]) + c.u64(o["Sequence"]), c.raw_bytes(raw["Value"]))
                for kind in ("id", "request"):
                    spool.insert(b"fdb-" + kind.encode(), c.raw_bytes(raw["IDKey" if kind == "id" else "RequestKey"]), c.raw_bytes(raw["IDValue"]))
            for raw in streams["current"]:
                spool.insert(b"fdb-current", c.raw_bytes(raw["Key"]), c.raw_bytes(raw["Value"]))
            old = next(w for w in groups[3]["Writes"] if w["Key"].startswith("edge/"))
            final = next(w for w in groups[5]["Writes"] if w["Key"] == old["Key"])
            g, logical = old["Group"], old["Key"]
            history_key = lambda value: (f"g{g}/{result['Config']['Graph'].encode().hex()}/history/{value['Round']:016x}/" + logical.encode().hex()).encode()
            v = retained.Verification(c.FDB_RETENTION_API, result, spool)
            self.assertTrue(v.deletion(history_key(old["After"]), c.go_json(old["After"]))[1])
            self.assertFalse(v.deletion(history_key(final["After"]), c.go_json(final["After"]))[1])
            # Another genuine final original is still foreign to this key.
            other = next(w for w in groups[6]["Writes"] if w["Key"].startswith("edge/"))
            key = c.fdb_key(result["Config"], g, "current", logical)
            spool.replace(b"fdb-current", key, c.go_json(final["After"]), c.go_json(other["After"]))
            with self.assertRaisesRegex(ValueError, "genuine original"):
                v.deletion(history_key(old["After"]), c.go_json(old["After"]))

    def test_genuine_original_sidecar_refuses_other_request_and_empty_commit(self):
        c = module("retention-original-guard", "compare-cost-cdc.py")
        groups, baseline, _ = fdb_replay(True)
        result, _, _ = raw_fixture(groups, baseline)
        result["Retention"] = {}
        encoded = lambda body: base64.b64encode(body).decode()
        with tempfile.TemporaryDirectory() as tmp:
            with c.Spool(tmp, c.Policy(1 << 20, 16 << 10, 4096, 512 << 10)) as spool:
                first = groups[0]
                o, tx = first["Outcome"], first["Tx"]
                g = o["Coordinator"]
                pair = []
                for kind, identity in (("id", o["ID"]), ("request", o["Request"])):
                    key = c.fdb_key(result["Config"], g, kind, identity)
                    body = c.go_json(o)
                    spool.insert(b"fdb-" + kind.encode(), key, body)
                    pair.append({"Key": encoded(key), "Value": encoded(body)})
                spool.insert(b"retention-cdc", c.u64(g) + c.u64(o["Sequence"]), c.go_json(first))
                v = retained.Verification(c.FDB_RETENTION_API, result, spool)
                proof = {"Pair": pair, "Original": {"Key": encoded(v.change_key(o)), "Value": encoded(c.go_json(first))}, "Group": first, "Outcome": o}
                key, actual, body = v.proof(proof, inventory=True)
                self.assertEqual(actual, o)
                self.assertEqual(hashlib.sha256(body).digest(), bytes(o["Digest"]))
                self.assertEqual(key, c.fdb_key(result["Config"], g, "id", tx["ID"]))
                for mutate in (lambda p: p["Pair"][1].__setitem__("Key", encoded(b"foreign-request")),
                               lambda p: p["Original"].__setitem__("Value", None),
                               lambda p: p["Group"]["Outcome"].__setitem__("Round", o["Round"] + 1),
                               lambda p: p["Outcome"].__setitem__("Commit", False)):
                    bad = copy.deepcopy(proof)
                    mutate(bad)
                    with self.assertRaises(ValueError): v.proof(bad, inventory=True)
            self.assertEqual(list(Path(tmp).glob(".cost-spool-*")), [])

    def test_census_page_cannot_understate_authentic_original_read_work(self):
        c = module("retention-work-guard", "compare-cost-cdc.py")
        groups, baseline, _ = fdb_replay(True)
        result, header, streams = raw_fixture(groups, baseline)
        # This is one genuine archived CDC coordinator prefix, with explicit
        # synthetic progress wrappers. It is not a fresh SDK/retention receipt.
        result["Retention"] = {"Seal": {"Before": header["Metadata"]}}
        with tempfile.TemporaryDirectory() as tmp, c.Spool(tmp, c.Policy(2 << 20, 16 << 10, 4096, 512 << 10)) as spool:
            for raw in streams["cdc"]:
                group = c.decoded_body(c.raw_bytes(raw["Value"]))
                o = group["Outcome"]
                spool.insert(b"retention-cdc", c.u64(o["Coordinator"]) + c.u64(o["Sequence"]), c.raw_bytes(raw["Value"]))
                for kind in ("id", "request"):
                    spool.insert(b"fdb-" + kind.encode(), c.raw_bytes(raw["IDKey" if kind == "id" else "RequestKey"]), c.raw_bytes(raw["IDValue"]))
            for raw in streams["history"]:
                spool.insert(b"fdb-history", c.raw_bytes(raw["Key"]), c.raw_bytes(raw["Value"]))
            v = retained.Verification(c.FDB_RETENTION_API, result, spool)
            v.stages, v.guards = 4, 2
            v.state = retained.zero_state() | {"Phase": 1, "Request": "source-final", "Target": result["FinalCut"], "Before": header["Metadata"]}
            row = {"Type": "collect", "Budget": {"Rows": 128, "Bytes": 4 << 20, "OutputBytes": 4096},
                   "Page": {"Phase": 1, "WorkRows": 6, "WorkBytes": 0, "Deleted": [0, 0], "DeletedBytes": [0, 0], "Done": False, "Seal": retained.zero_seal()}, "Observation": {}}
            with self.assertRaisesRegex(ValueError, "exact native retirement page"):
                v.collect(row)
            self.assertEqual(v.pages, 0)
            self.assertGreater(v.state["WorkBytes"], 0)
            self.assertEqual(v.state["Deleted"], [0, 0])
            self.assertEqual(v.state["Completed"], retained.zero_seal())


if __name__ == "__main__": unittest.main()
