#!/usr/bin/env python3
"""Authored bounded oracle guards; synthetic rows are not real SDK evidence."""
import base64
import copy
import hashlib
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch
import fdb_contention_check as check
from comparison_spool import Policy, Spool, SpoolError


def b64(raw):
    return base64.b64encode(raw).decode()


def setup_attempt(value=1):
    p = {"version": 1, "family": check.FAMILY, "facts": 20, "crossPercent": 50, "writers": 1, "layout": "kv"}
    c = {"Graph": "fixture-native-binding", "Topology": 1, "Epochs": [7, 11]}
    command, effects, reads = check.contract(p, "setup", 0, 0)
    op = {"Dataset": list(hashlib.sha256(b"\x01" + (20).to_bytes(8, "big") + (50).to_bytes(8, "big") + b"graph-v2").digest()), "Phase": "setup", "Fact": 0, "Ordinal": 1}
    tx = {"Graph": c["Graph"], "Topology": 1, "ID": "contention-setup-0-c0-q0-a1", "Request": "request/contention-setup-0-c0-q0-a1", "Coordinator": 0, "Dependency": 0,
          "Participants": [{"Group": 0, "Epoch": 7, "Generation": 0, "Reads": [{"Key": reads[0][0], "Version": 0}], "Effects": effects[0]}],
          "GraphChange": command, "Lifecycle": {"Provenance": 1}}
    body = check.go_json(tx)
    outcome = {"Commit": True, "Round": 2, "Digest": list(hashlib.sha256(body).digest()), "ID": tx["ID"], "Request": tx["Request"], "Reason": "", "Coordinator": 0, "Sequence": 1}
    group = {"Tx": tx, "Outcome": outcome, "Writes": [{"Group": 0, "Key": reads[0][0], "After": {"Value": value, "Version": 1, "Round": 2, "Deleted": False, "TxID": tx["ID"]}}]}
    proof = {"Outcome": outcome, "Pair": [{"Key": b64(check.key(c, 0, kind, identity)), "Value": b64(check.go_json(outcome))} for kind, identity in (("id", tx["ID"]), ("request", tx["Request"]))],
             "Original": {"Key": b64(check.prefix(c) + b"changes/0000000000000001/" + tx["Request"].encode().hex().encode()), "Value": b64(check.go_json(group))}, "Group": group}
    metadata = [{"Floor": 1, "Generation": 0, "Versions": 0, "Transactions": 0, "Certificates": 1 if g == 0 else 0, "Bytes": 8192, "Groups": 0} for g in range(2)]
    row = {"Operation": op, "Family": check.FAMILY, "Engine": "foundationdb", "ClientID": 0, "AttemptOrdinal": 1, "PreviousRequest": "", "Request": tx["Request"], "Transaction": tx["ID"], "Target": 0, "Claimant": 0,
           "Proposal": b64(body), "BodySHA256": outcome["Digest"], "Capture": {"Metadata": metadata, "Records": [{"Key": b64(check.key(c, g, "meta", "state")), "Value": b64(check.go_json(m))} for g, m in enumerate(metadata)], "ReadVersion": 1},
           "ReadCallbacks": 2, "Submissions": [{"Ordinal": 1, "Callbacks": 1, "Outcome": outcome, "Error": "", "Retryable": False, "MaybeCommitted": False}], "Classification": "committed", "Error": "", "Proof": proof, "ElapsedNanos": 1}
    return p, c, row, group


class ContentionOracleGuards(unittest.TestCase):
    def test_exact_original_and_physical_predecessor(self):
        p, c, row, group = setup_attempt()
        policy = Policy(4 << 20, 64 << 10, 4096, check.LINE)
        with tempfile.TemporaryDirectory() as directory:
            with Spool(directory, policy) as spool:
                check.authenticate_attempt(row, p, c, spool)
                spool.insert(b"operation-file", bytes(8) + group["Tx"]["ID"].encode(), check.go_json(group))
                self.assertEqual(check.fold_group(group, p, c, spool, [0, 0], [0, 0], {"Groups": [1, 0]}), "setup")
                with self.assertRaises((ValueError, SpoolError)):
                    check.authenticate_attempt(row, p, c, spool)
            self.assertEqual(list(Path(directory).iterdir()), [])
            p, c, wrong, wrong_group = setup_attempt(value=2)
            with Spool(directory, policy) as spool:
                check.authenticate_attempt(wrong, p, c, spool)
                spool.insert(b"operation-file", bytes(8) + wrong_group["Tx"]["ID"].encode(), check.go_json(wrong_group))
                with self.assertRaises(ValueError):
                    check.fold_group(wrong_group, p, c, spool, [0, 0], [0, 0], {"Groups": [1, 0]})
            foreign = copy.deepcopy(row)
            foreign["Proof"]["Pair"][1]["Value"] = b64(b"{}")
            with Spool(directory, policy) as spool:
                with self.assertRaises(ValueError):
                    check.authenticate_attempt(foreign, p, c, spool)
            unknown = copy.deepcopy(row)
            unknown["Classification"] = "pending"
            with Spool(directory, policy) as spool:
                with self.assertRaises(ValueError):
                    check.authenticate_attempt(unknown, p, c, spool)
            linked = copy.deepcopy(row)
            linked["Operation"].update(Phase="hot", Ordinal=14)
            linked.update(AttemptOrdinal=2, PreviousRequest=row["Request"])
            with Spool(directory, policy) as spool:
                with self.assertRaises(ValueError):
                    check.authenticate_attempt(linked, p, c, spool)  # No authenticated prior ABORT.
            with Spool(directory, policy) as spool:
                with self.assertRaises(ValueError):
                    check._codec.column_stage({"Records": []}, 0, {"Config": c}, {}, {}, {}, None, None, spool)
                with self.assertRaises(ValueError):
                    check.column_refusal({"Type": "refusal", "Name": "expired-new-open", "Generation": "old", "Error": "fdb-column: retained generation or token expired", "Input": {}, "Result": None, "Records": []},
                                         {"Config": c}, {}, b"held", {}, spool, 0, 0, 0, 2)

    def test_optional_shared_spool_owns_no_new_arena_or_close(self):
        # Ownership-only wrapper guard: pipeline mocks supply NO protocol or
        # SDK acceptance evidence. Real pair uses every full unmocked pass.
        end={"spec":{"facts":20,"writers":1},"format":"fixture","version":1,"Config":{"Graph":"fixture"},"Attempts":0,"Committed":0,"Aborted":0}
        policy=Policy(4<<20,65536,4096,check.LINE)
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory)/"fdb-workload-result.json").write_bytes(check.go_json(end)+b"\n")
            with Spool(directory,policy) as spool:
                spool.insert(b"existing",b"owner",b"same")
                with patch.object(check,"Spool",side_effect=AssertionError("new arena")),patch.object(check,"validate_result"),patch.object(check,"attempts") as attempts,patch.object(check,"verify_sources") as source,patch.object(check,"columns") as columns:
                    check.verify(directory,directory,policy,shared_spool=spool)
                    for call in (attempts,source,columns):self.assertIs(call.call_args.args[-1],spool)
                self.assertEqual(spool.lookup(b"existing",b"owner"),b"same")

    def test_family_identity_and_missing_or_trailing_eof(self):
        p, _, row, _ = setup_attempt()
        for op in ({**row["Operation"], "Ordinal": 2}, {**row["Operation"], "Phase": "unknown"}):
            with self.assertRaises(ValueError):
                check.operation(p, op, 0)
        with self.assertRaises(ValueError):
            check.transport.family_record({}, {**p, "version": 2}, {})
        # Transport choices must not alter the actual business dataset/ordinal.
        self.assertEqual(check.operation({**p, "version": 2}, row["Operation"], 0), check.operation(p, row["Operation"], 0))
        self.assertEqual(check.target(p, "hot", 0), 0)
        self.assertEqual(check.target(p, "hot", 1), 1)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "one.jsonl"
            path.write_bytes(b'{"Type":"row"}')
            with self.assertRaises(ValueError):
                list(check.lines(path, 1))
            path.write_bytes(b'{"Type":"row"}\n{"Type":"row"}\n')
            with self.assertRaises(ValueError):
                list(check.lines(path, 1))


if __name__ == "__main__":
    unittest.main()
