"""Direct guard tests and, when supplied, adversarial checks of actual exported traces.

Without artifact arguments these tests qualify representations only. They do not
construct a purported valid fault trace or establish paired runtime acceptance.
"""
import argparse
import base64
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import shutil
import unittest


CHECKER = Path(__file__).with_name("compare-graph-fault.py")
spec = importlib.util.spec_from_file_location("paired_fault_checker", CHECKER)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)
NATIVE = FDB = None
FDB_ROOT = None


def encoded(value):
    return base64.b64encode(json.dumps(value, separators=(",", ":")).encode()).decode()


class RepresentationGuards(unittest.TestCase):
    def test_paired_eight_mib_bound_and_unchanged_legacy_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace.json"
            path.write_bytes(b"")
            with self.assertRaises(checker.Refusal): checker.load(path)
            path.write_bytes(b"{}" + b" " * (checker.PAIRED_TRACE_BYTES - 2))
            self.assertEqual(checker.load(path)[0], {})
            with self.assertRaises(checker.Refusal): checker.lc.load(path)
            with path.open("ab") as source: source.write(b" ")
            with self.assertRaises(checker.Refusal): checker.load(path)
            path.write_bytes(b"{broken}")
            with self.assertRaises(ValueError): checker.load(path)
    def test_contract_counts_and_unused_identity(self):
        self.assertEqual(checker.SPEC["counts"], {"logicalOperations": 6, "completeLogicalCDCGroups": 6,
                                               "componentChanges": 7, "FDBPhysicalEffectWrites": 14})
        self.assertEqual(len(checker.SPEC["identityRoles"]), 9)
        self.assertEqual([o.get("I64") for o in checker.OPS[2:]], [5, 6, 5, 6])

    def test_identity_owner_alias_and_operation_order_refuse(self):
        trace = {"schemaVersion": 1, "fixtureID": "graph-fault-local-v1", "engine": "native-raft",
                 "identityMapping": [{"Logical": n, "Role": role, "Owner": owner, "Graph": "guard", "Raw": i + 1}
                                     for i, (n, role, owner) in enumerate(checker.SPEC["identityRoles"])],
                 "operationMapping": [{"Logical": o["Logical"], "Coordinator": {"A": 3, "B": 8}[o["Coordinator"]],
                                       "Request": o["Logical"], "Transaction": o["Logical"], "Round": i + 1}
                                      for i, o in enumerate(checker.OPS)],
                 "steps": [{"Operation": o["Logical"], "Coordinator": o["Coordinator"]} for o in checker.OPS]}
        checker.mapping(trace)  # Inert metadata only, no observed graph/faults.
        for mutate in (lambda t: t["identityMapping"][1].update(Owner="edge"),
                       lambda t: t["identityMapping"][1].update(Raw=1),
                       lambda t: t["operationMapping"][3].update(Round=1),
                       lambda t: t["steps"].reverse()):
            bad = copy.deepcopy(trace); mutate(bad)
            with self.assertRaises(checker.Refusal):
                checker.mapping(bad)

    def test_exact_original_coordinates_and_cdc_refuse(self):
        original = {"Logical": "guard", "Coordinator": 8, "Request": "key", "Transaction": "tx", "Round": 7}
        checker.coordinates(original, original)
        for key in ("Coordinator", "Request", "Transaction", "Round", "Logical"):
            changed = dict(original); changed[key] = "different"
            with self.assertRaises(checker.Refusal):
                checker.coordinates(changed, original)
        with self.assertRaises(checker.Refusal):
            checker.new_records({"engine": "native-raft"}, [{"Partition": 3, "Index": 1, "Outcome": encoded({"diagnostic": True}), "CDC": encoded({"effect": True})}])
        with self.assertRaises(checker.Refusal):
            checker.new_records({"engine": "foundationdb"}, [{"unexpected": "group"}])

    def test_partial_wrong_volume_and_false_reap_refuse(self):
        event = {"EventID": 1, "Action": "stop", "Role": "replica", "Member": 1,
                 "Instance": "guard-only", "Volume": "guard-volume", "Reaped": True,
                 "Signal": "clean-exit", "OOMKilled": None, "Raw": encoded({"inert": "guard"})}
        for mutate in (lambda e: e.update(Reaped=False), lambda e: e.update(OOMKilled=True),
                       lambda e: e.update(EventID=0), lambda e: e.update(Signal="expected-but-unobserved"),
                       lambda e: e.update(Raw="")):
            bad = copy.deepcopy(event); mutate(bad)
            with self.assertRaises(checker.Refusal):
                checker.process_event(bad, "native-raft")
        reopen = {"EventID": 2, "Action": "reopen", "Role": "replica", "Member": 1,
                  "Instance": "new", "PreviousInstance": "new", "Volume": "guard-volume",
                  "Ready": True, "Raw": encoded({"inert": "guard"}), "RecoveryRaw": encoded({"inert": "guard"})}
        with self.assertRaises(checker.Refusal):
            checker.process_event(reopen, "native-raft")
        checker.old_checkpoint(2, True, 8, 12, 4)
        with self.assertRaisesRegex(checker.Refusal, "held owner died"):
            checker.old_checkpoint(5, True, 8, 12, 4)
        with self.assertRaises(checker.Refusal):
            checker.old_checkpoint(12, False, 8, 12, 4)

    def test_native_transparent_os_schema_guards_only(self):
        # Inert direct API guards, not a fabricated accepted native trace.
        instance = "voter1/pid21/" + "ab" * 16
        raw = {"Member": 1, "PID": 21, "Instance": instance, "Volume": "/guard/1", "Reaped": True,
               "Signaled": True, "Signal": "SIGKILL", "SignalNumber": 9, "WaitStatus": 9,
               "ExitCode": -1, "Success": False, "OOMKilled": None, "Error": "signal: killed"}
        event = {"EventID": 1, "Action": "kill", "Role": "replica", "Member": 1, "Instance": instance,
                 "Volume": "/guard/1", "Reaped": True, "Signal": "SIGKILL", "OOMKilled": None, "Raw": encoded(raw)}
        checker.process_event(event, "native-raft")
        for key, value in (("PID", 22), ("Member", 2), ("WaitStatus", 0), ("Success", True), ("Signaled", False), ("OOMKilled", True), ("SignalNumber", 15), ("ExitCode", 0), ("Error", "")):
            bad = copy.deepcopy(event); changed = dict(raw); changed[key] = value; bad["Raw"] = encoded(changed)
            with self.assertRaises(checker.Refusal): checker.process_event(bad, "native-raft")
        clean = dict(raw, Signaled=False, Signal="clean-exit", WaitStatus=0, ExitCode=0, Success=True)
        stopped = dict(event, Action="stop", Signal="clean-exit", Raw=encoded(clean))
        checker.process_event(stopped, "native-raft")
        bad = dict(stopped); bad["Raw"] = encoded(dict(clean, WaitStatus=9))
        with self.assertRaises(checker.Refusal): checker.process_event(bad, "native-raft")
        replacement = "voter1/pid22/" + "cd" * 16
        startup = {"Member": 1, "PID": 22, "Instance": replacement, "PreviousInstance": instance,
                   "Volume": "/guard/1", "Ready": True, "RecoveryRaw": encoded({"inert": "startup bytes"})}
        reopened = {"EventID": 2, "Action": "reopen", "Role": "replica", "Member": 1, "Instance": replacement,
                    "PreviousInstance": instance, "Volume": "/guard/1", "Ready": True,
                    "RecoveryRaw": startup["RecoveryRaw"], "Raw": encoded(startup)}
        checker.process_event(reopened, "native-raft")
        bad = dict(reopened); bad["Raw"] = encoded(dict(startup, RecoveryRaw=encoded({"wrong": "startup"})))
        with self.assertRaises(checker.Refusal): checker.process_event(bad, "native-raft")

    def test_cli_missing_bad_json_duplicate_key_and_engine_order(self):
        for arguments in ([], ["/nonexistent/native", "/nonexistent/fdb"]):
            result = subprocess.run([sys.executable, str(CHECKER), *arguments], capture_output=True, text=True, check=False)
            self.assertNotEqual(result.returncode, 0)
        with self.assertRaises(checker.Refusal):
            checker.lc.parse('{"engine":"native-raft","engine":"foundationdb"}')
        with self.assertRaises(checker.Refusal):
            checker.compare({"engine": "foundationdb"}, {"engine": "native-raft"})


class ActualArtifactChecks(unittest.TestCase):
    """Only loaded native/FDB exports enter positive runtime comparison."""
    def refuse(self, mutate, engine="foundationdb", message=None):
        native, fdb = copy.deepcopy(NATIVE), copy.deepcopy(FDB)
        mutate(native if engine == "native-raft" else fdb)
        context = self.assertRaises(checker.Refusal) if message is None else self.assertRaisesRegex(checker.Refusal, message)
        with context:
            checker.compare(native, fdb, FDB_ROOT)

    def test_actual_pair_and_cli(self):
        result = checker.compare(NATIVE, FDB, FDB_ROOT)
        self.assertTrue(result["equal"])
        self.assertEqual(result["componentChanges"], 7)
        self.assertEqual(result["faultCases"], 4)
        result = subprocess.run([sys.executable, "-B", str(CHECKER), ARGS.native, ARGS.fdb, "--fdb-artifact-root", str(FDB_ROOT)], capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_fault_classification_pending_and_original_payload(self):
        self.refuse(lambda t: t["faults"][0].update(CallerClassification="committed"))
        self.refuse(lambda t: t["faults"][0]["OriginalRequest"].update(Payload=encoded({"new": "request"})))
        self.refuse(lambda t: t["faults"][0]["Retry"].update(Payload=encoded({"new": "request"})))
        self.refuse(lambda t: t["faults"][0]["Intermediate"].update(Classification="pending"))
        self.refuse(lambda t: t["faults"][0]["Intermediate"].update(View=t["steps"][2]["After"]), "native-raft")

    def test_exact_postings_census_lives_definitions_and_aba(self):
        self.refuse(lambda t: t["steps"][5]["After"]["Postings"][-1].update(Effective=[]))
        self.refuse(lambda t: t["steps"][5]["After"].update(PhantomFound=[True, False]))
        self.refuse(lambda t: t["steps"][5]["After"]["Lives"][-1].update(SourceLife="life-a2"))
        self.refuse(lambda t: t["steps"][5]["After"]["Entities"][-1]["Property"].update(RevisionOperation="precommit-update"))
        self.refuse(lambda t: t["steps"][4].update(NewValues=t["steps"][2]["NewValues"]))
        self.refuse(lambda t: t["steps"][4]["LogicalCDC"].pop())

    def test_raw_witness_history_current_and_old_cut(self):
        self.refuse(lambda t: t["steps"][4]["RawRecords"][0].update(Value=encoded({"tampered": True})))
        self.refuse(lambda t: t["steps"][5]["RawObservation"]["History"].pop())
        self.refuse(lambda t: t["faults"][0]["Retry"]["After"]["RawCurrent"].pop())
        self.refuse(lambda t: t["oldReads"][0]["Cut"].update(Round=0))
        self.refuse(lambda t: t["oldReads"][0].update(EventID=0))
        self.refuse(lambda t: next(r for r in t["oldReads"] if r["Mode"] == "held-old").update(EventID=next(e["EventID"] for e in t["faults"][2]["ProcessEvidence"] if e["Action"] == "kill") + 1))
        self.refuse(lambda t: t["oldReads"].__setitem__(1, copy.deepcopy(t["oldReads"][0])))

    def test_actual_prekill_commit_receipt_not_later_observer(self):
        self.refuse(lambda t: t["faults"][1].update(MarkerRaw=encoded({"ready": "after-native-commit-before-reply"})))
        self.refuse(lambda t: next(e for e in t["faults"][1]["ProcessEvidence"] if e["Action"] == "commit")["Result"].update(Round=0))
        self.refuse(lambda t: t["faults"][1]["ChangedPayload"].update(Classification="invalid"))
        self.refuse(lambda t: t["faults"][1]["ChangedPayload"].update(Payload=t["faults"][1]["OriginalRequest"]["Payload"]))
        self.refuse(lambda t: t["faults"][1]["ChangedPayload"].update(NewRawRecords=[{"new": "group"}]))

    def test_exact_cohort_while_down_and_post_reopen_replay(self):
        self.refuse(lambda t: t["faults"][3]["ProcessEvidence"].pop())
        self.refuse(lambda t: next(e for e in t["faults"][3]["ProcessEvidence"] if e["Action"] == "reopen").update(Volume="different-volume"))
        self.refuse(lambda t: next(e for e in t["faults"][3]["ProcessEvidence"] if e["Action"] == "reopen").update(Reaped=True))
        self.refuse(lambda t: next(e for e in t["faults"][2]["ProcessEvidence"] if e["Action"] == "commit").update(EventID=1))
        self.refuse(lambda t: t["faults"][3]["ReopenRetries"].pop())
        self.refuse(lambda t: t["faults"][3]["ReopenRetries"][0].update(EventID=1))
        self.refuse(lambda t: t["faults"][3]["ReopenRetries"][0]["Result"].update(Request="new-key"))


class ActualFDBChecks(unittest.TestCase):
    """Real FDB component, independently qualified while native pair is pending."""
    def refuse(self, mutate, semantic=False, message=None):
        bad = copy.deepcopy(FDB); mutate(bad)
        with self.assertRaises(checker.Refusal) if message is None else self.assertRaisesRegex(checker.Refusal, message):
            if semantic: checker.analyze(bad)
            else: checker.validate(bad, FDB_ROOT)

    def test_real_fdb_component_and_loader_cli(self):
        normalized, inputs, evidence = checker.validate(FDB, FDB_ROOT)
        self.assertEqual(len(normalized["steps"]), 6)
        self.assertEqual(len(inputs), 6)
        self.assertEqual(evidence["sealedCommandEvents"], 221)
        self.assertGreater(Path(ARGS.fdb_only or ARGS.fdb).stat().st_size, 2 << 20)
        for extra, success in ((["--fdb-artifact-root", str(FDB_ROOT)], True), ([], False)):
            result = subprocess.run([sys.executable, "-B", str(CHECKER), "--fdb-only", ARGS.fdb_only or ARGS.fdb, *extra], capture_output=True, text=True, check=False)
            self.assertEqual(result.returncode == 0, success, result.stderr)
            if success: self.assertFalse(json.loads(result.stdout)["fullPairAccepted"])

    def test_frozen_backend_and_controller_copied_fields_refuse(self):
        for mutate in (
            lambda t: t["steps"][5]["After"]["Entities"][-1]["Property"].update(Provenance=91),
            lambda t: t["faults"][0].update(MarkerRaw=encoded({"ready": "after-native-commit-before-reply"})),
            lambda t: t["faults"][0].update(CallerRaw=encoded({"invented": "committed"})),
            lambda t: t["faults"][1].update(MarkerRaw=encoded({"ready": "after-native-commit-before-reply"})),
            lambda t: t["oldReads"][0].update(EventID=1),
            lambda t: t["faults"][3]["ReopenRetries"][0].update(EventID=1),
            lambda t: t["controllerCommandLedger"].update(Bytes=1),
            lambda t: t["backendTraceArtifact"].update(SHA256="0" * 64),
        ):
            self.refuse(mutate)
        bad = copy.deepcopy(FDB)
        event = bad["faults"][0]["ProcessEvidence"][0]
        raw = checker.lc.parse(checker.blob(event["Raw"])); raw["BeforeInspect"][0]["State"]["Pid"] += 1
        event["Raw"] = encoded(raw)
        with self.assertRaisesRegex(checker.Refusal, "client process inspections changed"):
            checker.validate(bad, FDB_ROOT)

    def test_artifact_hash_path_and_exact_prefix_refuse(self):
        with self.assertRaises(checker.Refusal): checker.validate(FDB)
        self.refuse(lambda t: t["controllerArtifacts"].update({"../outside": "0" * 64}))
        names = [*FDB["controllerArtifacts"], FDB["backendTraceArtifact"]["Artifact"], "commands.jsonl"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in names: shutil.copyfile(FDB_ROOT / name, root / name)
            path = root / "before-child-marker.json"; original = path.read_bytes()
            path.write_bytes(original + b" ")
            with self.assertRaisesRegex(checker.Refusal, "artifact SHA256 mismatch"):
                checker.validate(FDB, root)
            path.write_bytes(original)
            path = root / "commands.jsonl"; original = path.read_bytes()
            path.write_bytes(b" " + original)
            with self.assertRaisesRegex(checker.Refusal, "exact final command prefix"):
                checker.validate(FDB, root)
            path.write_bytes(original + b"later append is allowed\n")
            checker.validate(FDB, root)

    def test_semantic_components_and_retained_aba_tampering(self):
        for mutate in (
            lambda t: t["steps"][5]["After"]["Postings"][-1].update(Effective=[]),
            lambda t: t["steps"][5]["After"].update(PhantomFound=[True, False]),
            lambda t: t["steps"][5]["After"]["Lives"][-1].update(SourceLife="life-a2"),
            lambda t: t["steps"][5]["After"]["Entities"][-1]["Property"].update(RevisionOperation="precommit-update"),
            lambda t: t["steps"][4].update(NewValues=t["steps"][2]["NewValues"]),
            lambda t: t["steps"][4]["LogicalCDC"].pop(),
            lambda t: t["steps"][4]["RawRecords"][0].update(Value=encoded({"wrong": "original"})),
            lambda t: t["steps"][5]["RawObservation"]["History"].pop(),
            lambda t: t["faults"][0]["Retry"]["After"]["RawCurrent"].pop(),
            lambda t: t["oldReads"][0]["Cut"].update(Round=0),
            lambda t: t["oldReads"].__setitem__(1, copy.deepcopy(t["oldReads"][0])),
            lambda t: t["faults"][3]["ReopenRetries"].pop(),
            lambda t: t["faults"][1]["ChangedPayload"].update(Payload=t["faults"][1]["OriginalRequest"]["Payload"]),
        ):
            self.refuse(mutate, semantic=True)

    def test_source_process_scope_and_recovery_tampering(self):
        for case in (0, 2, 3):
            self.refuse(lambda t, case=case: t["faults"][case]["ProcessEvidence"][0].update(EventID=1))
        self.refuse(lambda t: next(e for e in t["faults"][3]["ProcessEvidence"] if e["Action"] == "reopen").update(Volume="other"), semantic=True)
        self.refuse(lambda t: next(e for e in t["faults"][3]["ProcessEvidence"] if e["Action"] == "reopen").update(Reaped=True), semantic=True)
        self.refuse(lambda t: t["faults"][0].update(CallerClassification="committed"), semantic=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("native", nargs="?"); parser.add_argument("fdb", nargs="?")
    parser.add_argument("--fdb-only")
    parser.add_argument("--fdb-artifact-root", type=Path)
    ARGS = parser.parse_args()
    if bool(ARGS.native) != bool(ARGS.fdb) or ARGS.fdb_only and ARGS.native:
        parser.error("supply both actual artifacts or neither")
    classes = [RepresentationGuards]
    if ARGS.fdb_only or ARGS.fdb:
        FDB, _ = checker.load(ARGS.fdb_only or ARGS.fdb)
        FDB_ROOT = ARGS.fdb_artifact_root
        if FDB_ROOT is None: parser.error("actual FDB tests require artifact root")
        classes.append(ActualFDBChecks)
    if ARGS.native:
        NATIVE, _ = checker.load(ARGS.native)
        classes.append(ActualArtifactChecks)
    suite = unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(c) for c in classes)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
