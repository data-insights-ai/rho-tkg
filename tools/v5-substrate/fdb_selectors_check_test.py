#!/usr/bin/env python3
"""Authored private-oracle guards; synthetic bytes are not SDK evidence."""
import base64
import copy
import hashlib
from pathlib import Path
import tempfile
import unittest

import fdb_selectors_check as check
from comparison_spool import Policy, Spool, SpoolError


def b64(body):
    return base64.b64encode(body).decode()


def fixture():
    p = {"version": 1, "family": check.FAMILY, "facts": 20, "crossPercent": 50, "writers": 1, "layout": "kv"}
    c = {"Graph": "fixture-selectors", "Topology": 1, "Epochs": [7, 11]}
    command, lifecycle, effects, reads = check.codec.fdb_contract(p, 1)
    tx = {"Graph": c["Graph"], "Topology": 1, "ID": "selectors-setup-0-c0-a1", "Request": "request/selectors-setup-0-c0-a1", "Coordinator": 0, "Dependency": 0,
          "Participants": [{"Group": 0, "Epoch": 7, "Generation": 0, "Reads": [{"Key": reads[0][0], "Version": 0}], "Effects": effects[0]}], "GraphChange": command, "Lifecycle": lifecycle}
    body = check.go_json(tx)
    outcome = {"Commit": True, "Round": 2, "Digest": list(hashlib.sha256(body).digest()), "ID": tx["ID"], "Request": tx["Request"], "Reason": "", "Coordinator": 0, "Sequence": 1}
    v = {"Value": 1, "Version": 1, "Round": 2, "Deleted": False, "TxID": tx["ID"]}
    group = {"Tx": tx, "Outcome": outcome, "Writes": [{"Group": 0, "Key": reads[0][0], "After": v}]}
    original = {"Key": b64(check.prefix(c) + b"changes/0000000000000001/" + tx["Request"].encode().hex().encode()), "Value": b64(check.go_json(group))}
    op = {"Dataset": list(hashlib.sha256(b"\x01" + check.u64(20) + check.u64(50) + b"graph-v2").digest()), "Phase": "setup", "Fact": 0, "Ordinal": 1}
    a = {"Operation": op, "Engine": "foundationdb", "ClientID": 0, "AttemptOrdinal": 1, "PreviousRequest": "", "Request": tx["Request"], "Transaction": tx["ID"], "BodySHA256": outcome["Digest"], "Proposal": b64(body),
         "Submissions": [{"Ordinal": 1, "Callbacks": 1, "Outcome": outcome, "Error": "", "Retryable": False, "MaybeCommitted": False}], "ReadCallbacks": 2, "Classification": "committed", "Error": "",
         "Originals": [{"Key": b64(check.key(c, 0, kind, identity)), "Value": b64(check.go_json(outcome))} for kind, identity in (("id", tx["ID"]), ("request", tx["Request"]))], "Original": original}
    cut = {"Graph": c["Graph"], "Topology": 1, "Epochs": [7, 11], "Scope": "AAE=", "Round": 2, "Groups": [1, 0]}
    return {"Spec": p, "Config": c, "Old": cut, "Binding": {"Read": {"ChainSteps": 128}}}, a, group, reads[0][0], v


class SelectorOracleGuards(unittest.TestCase):
    def test_actual_original_identity_and_exact_source_anchor(self):
        end, attempt, group, logical, value = fixture()
        policy = Policy(4 << 20, 64 << 10, 4096, 512 << 10)
        with tempfile.TemporaryDirectory() as directory:
            with Spool(directory, policy) as spool:
                self.assertEqual(check.attempt(attempt, end, spool)[0], 1)
                with self.assertRaises((ValueError, SpoolError)):
                    check.attempt(attempt, end, spool)
            for field, wrong in (("Classification", "pending"), ("PreviousRequest", "unproved-abort"), ("BodySHA256", [0] * 32)):
                changed = copy.deepcopy(attempt)
                changed[field] = wrong
                with Spool(directory, policy) as spool, self.assertRaises(ValueError):
                    check.attempt(changed, end, spool)
            with Spool(directory, policy) as spool:
                check.attempt(attempt, end, spool)
                current = check.key(end["Config"], 0, "current", logical)
                history = check.prefix(end["Config"]) + b"history/0000000000000002/" + logical.encode().hex().encode()
                body = check.go_json(value)
                spool.insert(b"fdb-old", current, body)
                spool.insert(b"fdb-history", history, body)
                spool.insert(b"native-cdc", check.blob(attempt["Original"]["Key"]), check.go_json(group))
                chain = {"Group": 0, **attempt["Original"], "IDKey": attempt["Originals"][0]["Key"], "IDValue": attempt["Originals"][0]["Value"], "RequestKey": attempt["Originals"][1]["Key"], "RequestValue": attempt["Originals"][1]["Value"]}
                row = {"Group": 0, "Key": logical, "Value": value, "Current": {"Key": b64(current), "Value": b64(body)}, "History": {"Key": b64(history), "Value": b64(body)}, "Chain": [chain]}
                check.snapshot_row(row, end["Old"], end, spool, "old-fresh")
                changed = copy.deepcopy(row)
                changed["History"]["Value"] = b64(check.go_json(value | {"Value": 2}))
                with self.assertRaises(ValueError):
                    check.snapshot_row(changed, end["Old"], end, spool, "old-scope-0")
                changed = copy.deepcopy(row)
                changed["Chain"][0]["IDKey"] = b64(b"foreign")
                with self.assertRaises(ValueError):
                    check.snapshot_row(changed, end["Old"], end, spool, "old-scope-0")
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_native_numeric_inventory_scope_and_incomplete_proof(self):
        end, _, _, _, _ = fixture()
        for round_ in (9, 10, (1 << 64) - 1):
            cut = end["Old"] | {"Round": round_}
            body = check.go_json(cut)
            record = {"Key": b64(check.key(end["Config"], 0, "cut", body.hex())), "Value": b64(body)}
            self.assertEqual(check.certificate(end["Config"], record, b"\0\1"), cut)
            with self.assertRaises(ValueError):
                check.certificate(end["Config"], record, b"\0")
            record["Key"] = b64(check.prefix(end["Config"]) + b"cut/" + body.hex().encode())
            with self.assertRaises(ValueError):
                check.certificate(end["Config"], record)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "prefix.jsonl"
            path.write_bytes(b'{"Type":"row"}')
            with self.assertRaises(ValueError):
                list(check.lines(path, 1))
            with Spool(directory, Policy(4 << 20, 64 << 10, 4096, 512 << 10)) as spool, self.assertRaises(ValueError):
                check.codec.column_stage({"Records": []}, 0, {"Config": end["Config"]}, {}, {}, {}, None, None, spool)
            with Spool(directory, Policy(4 << 20, 64 << 10, 4096, 512 << 10)) as spool:
                with self.assertRaises((ValueError, SpoolError)):
                    check.selector_timings(spool)  # No timing result from partial EOFs.
                for name in check.OBSERVATIONS:
                    sample = {"Mode": "Stable", "Cut": end["Old"], "Rows": [1, 0], "Nanos": 10}
                    spool.insert(b"selector-scan", name.encode(), check.go_json(sample))
                    if name != "held-old-after-update":
                        spool.insert(b"selector-open", name.encode(), check.go_json(sample | {"Nanos": 20}))
                spool.insert(b"selector-points", b"fresh-current", check.go_json({"Mode": "Fresh", "Cut": end["Old"], "Probes": 3, "Found": 2, "Nanos": 30}))
                timing = check.selector_timings(spool)
                self.assertEqual(len(timing["samples"]), 22)
                self.assertEqual(timing["nearestRank"][0]["p99Nanos"], 20)


if __name__ == "__main__":
    unittest.main()
