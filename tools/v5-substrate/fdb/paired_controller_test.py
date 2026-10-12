"""Direct controller guard tests; these do not claim real process faults."""
import base64
import copy
import hashlib
import json
import unittest

import paired_controller as controller


def process(pid=21, started="2026-10-12T10:00:00Z"):
    return [{"Id": "owned-container", "Name": "/owned-zone0",
             "State": {"Running": True, "Pid": pid, "StartedAt": started,
                       "OOMKilled": False, "ExitCode": 0},
             "Mounts": [{"Type": "volume", "Destination": "/var/fdb/data",
                         "Name": "owned-data0"}]}]


def stopped():
    record = process()
    record[0]["State"].update(Running=False, Pid=0, ExitCode=137)
    return record


def healthy():
    return {"client": {"database_status": {"healthy": True},
                       "coordinators": {"coordinators": [1, 2, 3]}},
            "cluster": {"configuration": {"redundancy_mode": "triple"},
                        "full_replication": True,
                        "data": {"state": {"name": "healthy", "min_replicas_remaining": 3}},
                        "recovery_state": {"name": "fully_recovered"},
                        "processes": dict.fromkeys(range(4)),
                        "machines": {i: {"locality": {"zoneid": f"zone{i}"}} for i in range(4)}}}


class Contracts(unittest.TestCase):
    def test_paired_marker_binds_exact_child_receipt_before_kill(self):
        receipt = b'{"actual":"committed-original-records"}'
        marker = {"ready": "after-native-commit-before-reply",
                  "receipt_sha256": hashlib.sha256(receipt).hexdigest()}
        controller.committed_marker(json.dumps(marker).encode(), receipt)
        for bad_receipt in (b"", receipt + b" "):
            with self.assertRaises(AssertionError):
                controller.committed_marker(json.dumps(marker).encode(), bad_receipt)
        for bad_marker in ({"ready": marker["ready"]}, {**marker, "extra": True},
                           {**marker, "ready": "before-native-commit"}):
            with self.assertRaises(AssertionError):
                controller.committed_marker(json.dumps(bad_marker).encode(), receipt)

    def test_kill_uses_actual_instance_exit_and_oom_not_signal_intent(self):
        before, after = process(), stopped()
        command = {"command": ["docker", "kill", "--signal", "KILL", "owned-zone0"],
                   "exit": 0, "deadline_expired": False}
        event = controller.killed(before, after, command, "storage", 0, 7)
        self.assertEqual(event["EventID"], 7)
        self.assertTrue(event["Reaped"])
        self.assertEqual(event["Volume"], "owned-data0")
        self.assertEqual(json.loads(base64.b64decode(event["Raw"]))["AfterInspect"], after)
        for key, value in [("ExitCode", 0), ("Running", True), ("OOMKilled", True)]:
            bad = copy.deepcopy(after)
            bad[0]["State"][key] = value
            with self.assertRaises(AssertionError):
                controller.killed(before, bad, command, "storage", 0, 7)
        for records in ([], [after[0], after[0]]):
            with self.assertRaises(AssertionError):
                controller.killed(before, records, command, "storage", 0, 7)

    def test_reopen_requires_distinct_ready_process_same_actual_volume(self):
        before, after = process(), process(32, "2026-10-12T11:00:00Z")
        event = controller.reopened(before, stopped(), after, {}, healthy(), 0, 12)
        self.assertTrue(event["Ready"])
        self.assertNotIn("Reaped", event)
        self.assertNotEqual(event["Instance"], event["PreviousInstance"])
        for bad in (before, process()):
            with self.assertRaises(AssertionError):
                controller.reopened(before, stopped(), bad, {}, healthy(), 0, 12)
        bad = copy.deepcopy(after)
        bad[0]["Mounts"][0]["Name"] = "replacement-empty-volume"
        with self.assertRaises(AssertionError):
            controller.reopened(before, stopped(), bad, {}, healthy(), 0, 12)
        bad_status = healthy()
        bad_status["cluster"]["full_replication"] = False
        with self.assertRaises(AssertionError):
            controller.reopened(before, stopped(), after, {}, bad_status, 0, 12)

    def test_exact_owned_command_refuses_absence_duplicate_or_timeout(self):
        c = {"command": ["docker", "kill", "--signal", "KILL", "owned-client"],
             "exit": 0, "deadline_expired": False}
        self.assertEqual(controller.command([c], ["kill", "--signal", "KILL"], "owned-client"), (0, c))
        for ledger in ([], [c, c]):
            with self.assertRaises(AssertionError):
                controller.command(ledger, ["kill", "--signal", "KILL"], "owned-client")
        for key, value in [("exit", 124), ("deadline_expired", True)]:
            bad = copy.deepcopy(c)
            bad[key] = value
            with self.assertRaises(AssertionError):
                controller.command([bad], ["kill", "--signal", "KILL"], "owned-client")


if __name__ == "__main__":
    unittest.main()
