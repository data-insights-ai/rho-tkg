#!/usr/bin/env python3
"""Authored offline guards only; synthetic bytes are never SDK/feed evidence."""
import base64
import copy
import hashlib
from pathlib import Path
import tempfile
import unittest

import fdb_feed_check as check
from comparison_spool import Policy, Spool, SpoolError


def b64(body):
    return base64.b64encode(body).decode()


def fixture():
    c = {"Graph": "guard-feed", "Topology": 1, "Epochs": [7, 11]}
    cut = {"Graph": c["Graph"], "Topology": 1, "Epochs": [7, 11], "Scope": "AAE=", "Round": 2, "Groups": [1, 0]}
    final = cut | {"Round": 4, "Groups": [3, 0]}
    end = {"Config": c, "Old": cut, "Final": final, "Spec": {"facts": 20}, "Binding": {"Read": {}},
           "Feed": {"Token": {"Original": [9] * 32}, "Original": {"Policy": {"PageRows": 8, "PageBytes": 64 << 10}}}}
    rows, old = [], []
    for i in range(2):
        logical = f"edge/guard-{i}"
        before = {"Value": 5, "Version": 1, "Round": 2, "Deleted": False, "TxID": f"create-{i}"}
        after = before | {"Value": 6, "Version": 2, "Round": 3 + i, "TxID": f"update-{i}"}
        outcome = {"Commit": True, "Round": 3 + i, "Digest": [i + 1] * 32, "ID": f"update-{i}", "Request": f"request/update-{i}", "Reason": "", "Coordinator": 0, "Sequence": i + 2}
        group = {"Tx": {"ID": outcome["ID"], "Request": outcome["Request"]}, "Outcome": outcome,
                 "Writes": [{"Group": 0, "Key": logical, "Before": before, "After": after}]}
        row = {"Group": 0, "Key": b64(check.prefix(c) + f'changes/{i + 2:016x}/'.encode() + outcome["Request"].encode().hex().encode()), "Value": b64(check.go_json(group)),
               "IDKey": b64(check.key(c, 0, "id", outcome["ID"])), "IDValue": b64(check.go_json(outcome)),
               "RequestKey": b64(check.key(c, 0, "request", outcome["Request"])), "RequestValue": b64(check.go_json(outcome))}
        rows.append(row)
        old.append((check.key(c, 0, "current", logical), check.go_json(before)))
    page = {"Version": 1, "Request": "page-0", "Original": [9] * 32, "From": [1, 0], "Next": [3, 0], "Boundary": final,
            "Last": check.decoded(check.blob(rows[-1]["Value"]))["Outcome"], "Rows": rows, "EOF": True}
    return end, {"Page": page, "Record": {"Key": b64(check.feed_key(c, "page/workload", "page-0")), "Value": b64(check.go_json(page))}, "Replay": False}, old


def seed(spool, delivery, old):
    zero = {"Commit": False, "Round": 0, "Digest": [0] * 32, "ID": "", "Request": "", "Reason": "", "Coordinator": 0, "Sequence": 0}
    spool.insert(b"feed-control-state", b"last", check.go_json(zero))
    for key, value in old:
        spool.insert(b"feed-replica", key, value)
    for row in delivery["Page"]["Rows"]:
        body = check.blob(row["Value"])
        outcome = check.decoded(body)["Outcome"]
        spool.insert(b"native-cdc", check.blob(row["Key"]), body)
        spool.insert(b"feed-sequence", b"\0" + check.u64(outcome["Sequence"]), body)
        spool.insert(b"attempt-id", check.blob(row["IDKey"]), check.blob(row["IDValue"]))
        spool.insert(b"attempt-request", check.blob(row["RequestKey"]), check.blob(row["RequestValue"]))


class FeedOracleGuards(unittest.TestCase):
    def test_business_wall_seed_original_join_and_parallel_percentiles(self):
        phases=[{"Phase":"create","StartNanos":10,"StopNanos":110,"WallNanos":100,"Useful":2},
                {"Phase":"update","StartNanos":200,"StopNanos":400,"WallNanos":200,"Useful":2}]
        end={"Spec":{"facts":2},"Committed":7,"BusinessAccounting":{"Version":1,"Phases":phases,"BusinessNanos":300,"TotalNanos":500,"Access":check.BUSINESS_ACCESS}}
        self.assertIsNone(check.business_accounting({}))
        self.assertEqual(check.business_accounting(end)["BusinessNanos"],300)
        for change in ({"BusinessNanos":301},{"TotalNanos":399},{"Version":True}):
            with self.assertRaises(ValueError):check.business_accounting(end|{"BusinessAccounting":end["BusinessAccounting"]|change})
        # Synthetic clock/original bindings test only the offline timing join;
        # the full verifier still requires authentic SDK originals/raw EOF first.
        rows=[]
        for phase,times in (("setup",((1,2),(3,4),(5,6))),("create",((11,31),(20,70))),("update",((201,221),(210,280)))):
            for fact,(start,stop) in enumerate(times):
                rows.append({"Phase":phase,"Fact":fact,"Client":0,"Nanos":stop-start,"StartNanos":start,"StopNanos":stop,"Access":"actual admission through business attempt/replan/verification; not selector point latency"})
        for wrong in (False,True):
            with tempfile.TemporaryDirectory() as directory:
                selected=copy.deepcopy(rows)
                if wrong:selected[3].update(StartNanos=9,StopNanos=29)  # SAME duration, but seed precedes its measured phase.
                Path(directory,"fdb-feed-latency.jsonl").write_bytes(b"".join(check.go_json(row)+b"\n" for row in selected))
                with Spool(directory,Policy(4<<20,64<<10,4096,512<<10)) as spool:
                    for row in selected:
                        phase,fact=row["Phase"],row["Fact"]
                        ordinal=1+fact if phase=="setup" else 4+fact+(2 if phase=="update" else 0)
                        spool.insert(b"useful-attempt",check.u64(ordinal),check.go_json({"Operation":{"Phase":phase,"Fact":fact},"ClientID":0}))
                    if wrong:
                        with self.assertRaises(ValueError):check.latencies(Path(directory),end,spool)
                    else:
                        check.latencies(Path(directory),end,spool)
                        with self.assertRaises(SpoolError):check.business_timings(end,spool)
                        for p in phases:spool.insert(b"feed-business-phase",p["Phase"].encode(),check.go_json(p))
                        measured=check.business_timings(end,spool)["phases"][0]
                        self.assertEqual((measured["p50Nanos"],measured["p95Nanos"],measured["p99Nanos"]),(20,50,50))
                        self.assertEqual(measured["usefulTransactionsPerSecond"],2*1_000_000_000/100)

    def test_transport_policy_cannot_replace_native_original_or_deadline(self):
        with self.assertRaises(ValueError): check.transport.validate_policy({"Version":2})
        with self.assertRaises(ValueError): check.child_input({"Admission":{}},{"Version":2})
        with self.assertRaises(ValueError): check.validate_result({"Version":2,"Format":check.FORMAT})

    def test_complete_exact_page_and_explicit_same_original_replay(self):
        end, delivery, old = fixture()
        with tempfile.TemporaryDirectory() as directory:
            with Spool(directory, Policy(4 << 20, 64 << 10, 4096, 512 << 10)) as spool:
                seed(spool, delivery, old)
                page, fresh = check.feed_page(delivery, end, spool, [1, 0])
                self.assertTrue(fresh)
                self.assertEqual(page["Next"], [3, 0])
                replay, fresh = check.feed_page(delivery | {"Replay": True}, end, spool)
                self.assertFalse(fresh)
                self.assertEqual(replay, page)
                with self.assertRaises(ValueError):
                    check.feed_page(delivery, end, spool)  # Duplicate must declare replay.
                changed = copy.deepcopy(delivery)
                changed["Replay"] = True
                changed["Page"]["Next"] = [2, 0]
                changed["Record"]["Value"] = b64(check.go_json(changed["Page"]))
                with self.assertRaises(ValueError):
                    check.feed_page(changed, end, spool)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_equal_count_wrong_before_pair_order_boundary_and_truncation(self):
        end, delivery, old = fixture()
        with tempfile.TemporaryDirectory() as directory:
            changes = []
            wrong = copy.deepcopy(delivery)
            group = check.decoded(check.blob(wrong["Page"]["Rows"][0]["Value"]))
            group["Writes"][0]["Before"]["Value"] = 99
            wrong["Page"]["Rows"][0]["Value"] = b64(check.go_json(group))
            changes.append(wrong)
            wrong = copy.deepcopy(delivery)
            wrong["Page"]["Rows"][0]["RequestValue"] = b64(b"foreign-original")
            changes.append(wrong)
            wrong = copy.deepcopy(delivery)
            wrong["Page"]["Rows"].reverse()
            changes.append(wrong)
            wrong = copy.deepcopy(delivery)
            wrong["Page"]["Rows"][1] = copy.deepcopy(wrong["Page"]["Rows"][0])
            changes.append(wrong)
            wrong = copy.deepcopy(delivery)
            wrong["Page"]["Boundary"]["Groups"] = [4, 0]
            changes.append(wrong)
            for changed in changes:
                changed["Record"]["Value"] = b64(check.go_json(changed["Page"]))
                with Spool(directory, Policy(4 << 20, 64 << 10, 4096, 512 << 10)) as spool:
                    seed(spool, delivery, old)
                    with self.assertRaises((ValueError, SpoolError)):
                        check.feed_page(changed, end, spool, [1, 0])
            missing = Path(directory) / "partial.jsonl"
            missing.write_bytes(b'{"Type":"feed-page"}')
            with self.assertRaises(ValueError):
                list(check.lines(missing, 1))
            with self.assertRaises(ValueError):
                check.child_input({}, end)


if __name__ == "__main__":
    unittest.main()
