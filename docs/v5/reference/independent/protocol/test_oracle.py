from __future__ import annotations

import copy
import unittest

from corpus import (active_transfer_trace, corpus, cross_trace, fresh_unacked_trace, historical_trace,
                    interleavings, ownership_move_trace, physical_lock_trace,
                    history, put, Trace, transaction)
from oracle import Evidence, check, conflict, observe, serial_witness


class IndependentHistoryOracleTests(unittest.TestCase):
    def test_named_fault_traces_and_expected_weakened_counterexamples(self):
        positives, negatives = corpus()
        for history in positives:
            with self.subTest(history=history["name"]):
                self.assertTrue(check(history)["accepted"], check(history))
        for history, expected_code in negatives:
            with self.subTest(history=history["name"]):
                result = check(history)
                self.assertFalse(result["accepted"])
                self.assertIn(expected_code, {v["code"] for v in result["violations"]})

    def test_all_seventy_interleavings_preserve_commuting_same_round(self):
        schedules = interleavings()
        self.assertEqual(70, len(schedules))
        for history in schedules:
            self.assertTrue(check(history)["accepted"], history["name"])

    def test_mathematical_oracle_rejects_write_skew_without_any_protocol_guards(self):
        _, negatives = corpus()
        for history, _ in negatives:
            if "write-skew" not in history["name"]:
                continue
            # Bypass Evidence entirely. Whole effects/decisions are atomic;
            # reads of absence are nevertheless impossible in either serial order.
            witness, detail = serial_witness(history, {"t1", "t2"}, {"t1": 10, "t2": 20}, [], [{"event": "final", "scope": ["A", "B"], "values": {"A/unique/1": 1, "B/unique/2": 2}}])
            self.assertIsNone(witness, detail)
            self.assertEqual(2, detail["orders_tried"])

    def test_mathematical_oracle_rejects_lost_update(self):
        h = {"initial": {"A/x": 0}, "transactions": {t: {"reads": [{"kind": "key", "key": "A/x", "exists": True, "value": 0}], "writes": {"A/x": {"op": "put", "value": 1}}} for t in ("t1", "t2")}}
        witness, detail = serial_witness(h, {"t1", "t2"}, {"t1": 10, "t2": 20}, [], [])
        self.assertIsNone(witness, detail)

    def test_absence_is_distinct_from_stored_null(self):
        read = {"kind": "key", "key": "A/x"}
        self.assertEqual({"exists": False, "value": None}, observe({}, {}, read))
        self.assertEqual({"exists": True, "value": None}, observe({"A/x": None}, {"A/x": "base"}, read))

    def test_revision_observation_detects_life_aba(self):
        h = {"initial": {"A/life": 1}, "transactions": {
            "a": {"reads": [], "writes": {"A/life": {"op": "put", "value": 2}}},
            "b": {"reads": [{"kind": "key", "key": "A/life", "exists": True, "value": 2}], "writes": {"A/life": {"op": "put", "value": 1}}},
            "c": {"reads": [{"kind": "key", "key": "A/life", "exists": True, "value": 1, "revision": "base"}], "writes": {"A/dependent": {"op": "put", "value": 3}}},
        }}
        # Commit rounds force a->b->c for conflicting operations; same value
        # after reopening is insufficient to validate the original life.
        witness, detail = serial_witness(h, {"a", "b", "c"}, {"a": 10, "b": 20, "c": 30}, [], [])
        self.assertIsNone(witness, detail)

    def test_conflict_in_b_does_not_false_reject_disjoint_a_prepares(self):
        result = check(physical_lock_trace())
        self.assertTrue(result["accepted"], result)

    def test_transfer_keeps_key_identity_and_historical_lineage(self):
        history = ownership_move_trace()
        result = check(history)
        self.assertTrue(result["accepted"], result)
        reads = [e for e in history["events"] if e["kind"] == "read"]
        self.assertEqual(set(reads[0]["values"]), set(reads[1]["values"]))
        self.assertNotEqual(reads[0]["values"], reads[1]["values"])
        cuts = [e["cut"] for e in history["events"] if e["kind"] == "certify"]
        self.assertEqual({"A": "A"}, cuts[0]["owners"])
        self.assertEqual({"A": "B"}, cuts[1]["owners"])

    def test_active_intent_transfer_declines_before_ownership_mutation(self):
        for observed, declared in ((True, True), (True, False), (False, True)):
            with self.subTest(observed=observed, declared=declared):
                history = active_transfer_trace()
                if not observed:
                    history["events"] = [e for e in history["events"] if e["kind"] != "prepare"]
                if not declared:
                    history["events"][-1]["prepared"] = []
                result = check(history)
                self.assertFalse(result["accepted"])
                self.assertIn("unsupported_active_intent_transfer", {v["code"] for v in result["violations"]})
                evidence = Evidence(history)
                for ordinal, event in enumerate(history["events"]):
                    evidence.ordinal = ordinal
                    evidence.event(event)
                self.assertEqual(1, evidence.topology)
                self.assertEqual("A", evidence.ownership[1]["A"])
                self.assertEqual(1, evidence.epochs["A"])
                if observed:
                    self.assertIn(("t1", "A"), evidence.prepared)

    def test_fresh_includes_unacked_durable_decision_and_requires_new_barrier(self):
        history = fresh_unacked_trace()
        self.assertFalse(any(e["kind"] == "ack" and e["tx"] == "t2" for e in history["events"]))
        read = next(e for e in history["events"] if e["kind"] == "read")
        read.update(cut="old", values={"A/x": 1})
        result = check(history)
        codes = {v["code"] for v in result["violations"]}
        self.assertIn("fresh_missing_durable_decision", codes)
        self.assertIn("fresh_reuses_pre_request_barrier", codes)
        self.assertIsNotNone(result["serial_witness"])  # At(old) contents remain valid.

    def test_fresh_cannot_forge_fence_ordinal(self):
        history = fresh_unacked_trace()
        cert = next(e for e in history["events"] if e["kind"] == "certify" and e["cut"]["id"] == "new")
        cert["fence_events"]["A"] = 9999
        self.assertIn("fence_evidence", {v["code"] for v in check(history)["violations"]})

    def test_input_version_and_resource_limits_are_explicit(self):
        history = cross_trace()
        history["schema_version"] = 2
        with self.assertRaisesRegex(ValueError, "schema"):
            check(history)
        history = cross_trace()
        history["transactions"] = {str(i): {} for i in range(9)}
        with self.assertRaisesRegex(ValueError, "limit"):
            check(history)

    def test_recovery_retains_every_durable_commit_and_abort_even_without_ack(self):
        for outcome in ("commit", "abort"):
            for acknowledged in (False, True):
                with self.subTest(outcome=outcome, acknowledged=acknowledged):
                    h = history("durable-decision-recovery", txs={"t": transaction({"A/x": put(1), "B/y": put(1)})})
                    t = Trace(h)
                    t.register("t")
                    t.prepare("t", "A")
                    t.prepare("t", "B")
                    t.decision("t", outcome, round=10)
                    if acknowledged:
                        t.ack("t", outcome, round=10)
                    t.add("crash", group="B", replica="B1")
                    t.add("restart", group="B", replica="B1")
                    t.add("recover", group="B", prepared=["t"], decisions=["t"], installed=[], floor=0, epoch=1)
                    self.assertTrue(check(h)["accepted"], check(h))
                    h["events"][-1]["decisions"] = []
                    result = check(h)
                    self.assertFalse(result["accepted"], result)
                    self.assertIn("lost_durable_decision", {v["code"] for v in result["violations"]})

    def test_local_commit_honors_prepared_mutation_and_read_locks(self):
        for prepared_writes in (True, False):
            for local_access in ("write", "key-read", "range-read"):
                for local_before_resolution in (True, False):
                    with self.subTest(prepared_writes=prepared_writes, local_access=local_access, local_before_resolution=local_before_resolution):
                        a = transaction({"A/x": put(1), "B/y": put(1)}) if prepared_writes else transaction(
                            {"B/y": put(1)}, reads=[{"kind": "key", "key": "A/x", "exists": True, "value": 0}], participants=["A", "B"])
                        reads = [] if local_access == "write" else ([{"kind": "key", "key": "A/x", "exists": True, "value": 0}]
                            if local_access == "key-read" else [{"kind": "range", "lo": "A/x", "hi": "A/y", "values": {"A/x": 0}}])
                        b = transaction({"A/x" if local_access == "write" else "A/z": put(2)}, reads=reads, coordinator="A")
                        b["request_key"] = "local"
                        h = history("local-versus-prepared-lock", {"A/x": 0}, {"prepared": a, "local": b})
                        t = Trace(h)
                        t.register("prepared")
                        t.prepare("prepared", "A")
                        t.prepare("prepared", "B")
                        if local_before_resolution:
                            t.durable("local_commit", tx="local", epoch=1, round=5)
                        t.decision("prepared", "abort")
                        t.install("prepared", "A", aborted=True)
                        t.install("prepared", "B", aborted=True)
                        if not local_before_resolution:
                            t.durable("local_commit", tx="local", epoch=1, round=5)
                        t.add("final", scope=["A", "B"], values={"A/x": 2} if local_access == "write" else {"A/x": 0, "A/z": 2})
                        result = check(h)
                        violates = local_before_resolution and (prepared_writes or local_access == "write")
                        self.assertEqual(result["accepted"], not violates, result)
                        if violates:
                            self.assertIn("prepared_conflict", {v["code"] for v in result["violations"]})

    def test_local_commit_checks_logical_bucket_epochs_after_move(self):
        for bucket_epoch in (1, 2):
            with self.subTest(bucket_epoch=bucket_epoch):
                h = history("local-after-bucket-move")
                t = Trace(h)
                t.durable("transfer", "A", bucket="A", **{"from": "A", "to": "B"},
                          old_epoch=1, new_epoch=2, retained_lineage=True,
                          target_replicas=["B1", "B2"], installed=[], prepared=[], floor=0)
                s = transaction({"A/x": put(1)}, participants=["B"], coordinator="B")
                s["epochs"] = {"A": bucket_epoch}
                h["transactions"]["local"] = s
                t.durable("local_commit", tx="local", epoch=1, round=5)
                t.add("final", scope=["A"], values={"A/x": 1})
                result = check(h)
                self.assertEqual(result["accepted"], bucket_epoch == 2, result)
                if bucket_epoch != 2:
                    self.assertIn("local_routing", {v["code"] for v in result["violations"]})

    def test_local_commit_routes_and_fences_full_key_and_range_read_footprint(self):
        for read in ({"kind": "key", "key": "B/y", "exists": True, "value": 0},
                     {"kind": "range", "lo": "A/", "hi": "B0", "values": {"B/y": 0}}):
            for variant in ("complete", "remote-read", "missing-read-epoch", "stale-read-epoch"):
                with self.subTest(read=read, variant=variant):
                    s = transaction({"A/x": put(1)}, reads=[read], participants=["A"], coordinator="A")
                    s["epochs"] = {"A": 1, "B": 2}
                    h = history("local-complete-read-footprint", {"B/y": 0}, {"local": s})
                    h["ownership"] = {"A": "A", "B": "A"}
                    h["epochs"]["B"] = 2
                    if variant == "remote-read":
                        h["ownership"]["B"] = "B"
                    elif variant == "missing-read-epoch":
                        del s["epochs"]["B"]
                    elif variant == "stale-read-epoch":
                        s["epochs"]["B"] = 1
                    t = Trace(h)
                    t.durable("local_commit", tx="local", epoch=1, round=5)
                    t.add("final", scope=["A", "B"], values={"A/x": 1, "B/y": 0})
                    result = check(h)
                    self.assertEqual(result["accepted"], variant == "complete", result)
                    if variant != "complete":
                        self.assertIn("local_routing", {v["code"] for v in result["violations"]})

    def test_local_historical_write_requires_revalidation(self):
        for validated in (False, True):
            with self.subTest(validated=validated):
                s = transaction({"A/x": put(1)}, reads=[{"kind": "key", "key": "A/x", "exists": True, "value": 0}], coordinator="A", historical=True)
                h = history("local-historical-write", {"A/x": 0}, {"local": s})
                t = Trace(h)
                if validated:
                    t.add("validate", tx="local", reads=s["reads"])
                t.durable("local_commit", tx="local", epoch=1, round=5)
                t.add("final", scope=["A"], values={"A/x": 1})
                result = check(h)
                self.assertEqual(result["accepted"], validated, result)
                if not validated:
                    self.assertIn("historical_write_unvalidated", {v["code"] for v in result["violations"]})

    def test_private_input_is_not_mutated_by_checker(self):
        history = historical_trace()
        original = copy.deepcopy(history)
        check(history)
        self.assertEqual(original, history)


if __name__ == "__main__":
    unittest.main()
