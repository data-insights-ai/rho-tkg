#!/usr/bin/env python3
"""Authored bounded decoder guards, not real role-loss or SDK evidence."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import composed_fault_controller as controller


def command(args):
    return {"command": ["docker", *args], "exit": 0, "deadline_expired": False}


def observed_commands(network=False):
    row = {"Name": "owned-zone0", "Address": "10.0.0.10:4500"}
    sdk = {"Name": "/owned-sdk"}
    fault = ["network", "disconnect", "owned", row["Name"]] if network else ["kill", "--signal", "KILL", row["Name"]]
    heal = ["network", "connect", "--ip", "10.0.0.10", "owned", row["Name"]] if network else ["start", row["Name"]]
    records = [command(["run", "-d", "--name", "owned-sdk", "actual-image", "fdb-spike", "column-fault-publication-child"]),
               command(["logs", "owned-sdk"]), command(["inspect", row["Name"]]), command(fault),
               command(["inspect", row["Name"]]), command(["kill", "--signal", "KILL", "owned-sdk"]),
               command(heal), command(["inspect", row["Name"]]),
               command(["run", "--name", "replacement", "actual-image", "fdb-spike", "column-fault-recover"])]
    return sdk, [row], records


class ComposedFaultSourceGuards(unittest.TestCase):
    def test_exact_sixty_disjoint_cases_no_nominal_or_boolean_orientation(self):
        cases = controller.cases()
        self.assertEqual(len(cases), 60)
        self.assertEqual(len({controller.name(c) for c in cases}), 60)
        self.assertEqual(sum(c["Kind"] == "source" for c in cases), 24)
        self.assertEqual(sum(c["Kind"] == "column" for c in cases), 36)
        for case in cases:
            self.assertIn(controller.phase(case), ("owner", "gc", "publication"))
        for args in (("column", "owner", "storage", "A"), ("source", "chunk", "log", "A"),
                     ("source", "gc", "commit-issued", "B"), ("column", "chunk", "network", True)):
            with self.assertRaises(ValueError):
                controller.shape(*args)
        self.assertEqual((controller.CASE_SECONDS, controller.TOTAL_SECONDS), (600, 36000))

    def test_all_actual_faults_precede_sdk_reap_then_heal_then_replacement(self):
        for network in (False, True):
            sdk, targets, records = observed_commands(network)
            fault = "network" if network else "log"
            order = controller.chronology(records, sdk, targets, "owned", fault, "replacement")
            self.assertEqual(order, {"SDKStart": 0, "Faults": [3], "SDKKill": 5, "Heals": [6], "Replacement": 8})
            for left, right in ((1, 4), (3, 5), (5, 6), (6, 8)):
                changed = copy.deepcopy(records)
                changed[left], changed[right] = changed[right], changed[left]
                with self.assertRaises(ValueError):
                    controller.chronology(changed, sdk, targets, "owned", fault, "replacement")
            changed = copy.deepcopy(records); changed[3]["exit"] = 1
            with self.assertRaises(ValueError):
                controller.chronology(changed, sdk, targets, "owned", fault, "replacement")
            with self.assertRaises(ValueError):
                controller.chronology(records, sdk, [], "owned", fault, "replacement")

    def test_changed_prefault_state_cannot_reuse_a_publication_marker(self):
        with tempfile.TemporaryDirectory() as path:
            root = Path(path)
            state = {"Case": "manifest-issued", "Coordinator": 1,
                     "Config": {"Graph": "column-publication-v1-owned"}, "Initial": None, "Final": {"Graph": "owned"},
                     "OldManifest": {"Generation": {"Binding": [1] * 32}},
                     "Stages": [{"Name": "owner-active-before-update"}, {"Name": "held-old-after-update"}, {"Name": "native-manifest-commit-issued"}]}
            raw = json.dumps(state).encode()
            marker = {"Ready": "native-manifest-commit-issued", "Format": controller.columns.FORMAT,
                      "Case": state["Case"], "Coordinator": 1,
                      "Generation": {"Version": 1, "ID": "final", "Binding": [1] * 32, "Initial": None, "Cut": state["Final"]},
                      "StateSHA256": list(hashlib.sha256(raw).digest())}
            (root / "publication-state.json").write_bytes(raw)
            (root / "publication-marker.json").write_text(json.dumps(marker))
            case = controller.shape("column", "manifest-issued", "two-zones", "B")
            self.assertEqual(controller.marker(root, case)["StateSHA256"], hashlib.sha256(raw).hexdigest())
            state["Final"]["Graph"] = "different"
            (root / "publication-state.json").write_text(json.dumps(state))
            with self.assertRaises(ValueError):
                controller.marker(root, case)

    def test_preexisting_descriptive_proof_never_skips_existing_full_oracle(self):
        # This mock tests delegation/ownership only, not native protocol success.
        with tempfile.TemporaryDirectory() as path:
            root = Path(path)
            (root / "proof.json").write_text('{"caller":"not authority"}')
            ledger = root / "actual-ledger.jsonl"; ledger.write_text('{}\n')
            for kind, module in (("column", controller.columns), ("source", controller.history)):
                with patch.object(module, "finalize", side_effect=ValueError("genuine raw original refused")) as actual:
                    with self.assertRaisesRegex(ValueError, "genuine raw original refused"):
                        controller.native_proof(root, {"Case": {"Kind": kind}}, ledger)
                    actual.assert_called_once_with(root, ledger)
            (root / "foreign").symlink_to(ledger)
            with patch.object(controller.columns, "finalize", side_effect=AssertionError("alias reached native verifier")):
                with self.assertRaises(ValueError):
                    controller.native_proof(root, {"Case": {"Kind": "column"}}, ledger)

    def test_copied_start_cannot_renew_original_admission_or_change_profile(self):
        with tempfile.TemporaryDirectory() as path:
            root = Path(path); case = controller.shape("source", "gc", "storage", "A")
            directory = root / controller.name(case); directory.mkdir()
            (root / "profile.json").write_text('{"declared":"profile"}')
            a = {"Format": controller.FORMAT, "Cases": controller.cases(), "CaseSeconds": 600, "TotalSeconds": 36000,
                 "StartedMonotonicNanos": 100, "DeadlineMonotonicNanos": 36000 * 10**9 + 100,
                 "ProfileSHA256": controller.sha(root / "profile.json")}
            controller.save(root / "composition-admission.json", a)
            started = {"Case": case, "AdmissionSHA256": controller.sha(root / "composition-admission.json"),
                       "StartedMonotonicNanos": 200, "DeadlineMonotonicNanos": 600 * 10**9 + 200, "LedgerStart": 0}
            controller.save(directory / "composition-start.json", started)
            self.assertEqual(controller.start(directory), started)
            changed = dict(started, DeadlineMonotonicNanos=started["DeadlineMonotonicNanos"] + 1)
            (directory / "composition-start.json").write_text(json.dumps(changed))
            with self.assertRaises(ValueError):
                controller.start(directory)
            (directory / "composition-start.json").write_text(json.dumps(started))
            (root / "profile.json").write_text('{"declared":"foreign"}')
            with self.assertRaises(ValueError):
                controller.start(directory)

    def test_empty_diagnostic_is_hashable_but_never_nonempty_native_authority(self):
        with tempfile.TemporaryDirectory() as path:
            diagnostic = Path(path) / "stderr.txt"
            diagnostic.write_bytes(b"")
            self.assertEqual(controller.sha(diagnostic, allow_empty=True), hashlib.sha256(b"").hexdigest())
            with self.assertRaises(ValueError):
                controller.sha(diagnostic)


if __name__ == "__main__":
    unittest.main()
