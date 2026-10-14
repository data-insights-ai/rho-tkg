#!/usr/bin/env python3
"""Compact authored-only guards: no platform fault is reconstructed here."""
import unittest
import platform_fault_controller as controller


class PlatformFaultBindingGuards(unittest.TestCase):
    def test_role_and_domain_selection_refuse_missing_or_foreign_process(self):
        rows = [{"Name": f"zone{i}", "Zone": f"zone{i}", "Address": f"10.0.0.{i+10}:4500",
                 "Roles": ["storage", "log"] if i else ["commit_proxy", "storage"],
                 "Coordinator": i < 3} for i in range(4)]
        self.assertEqual(controller.targets(rows, "transaction"), [rows[0]])
        self.assertEqual(controller.targets(rows, "two-zones"), rows[:2])
        rows[0]["Roles"] = ["storage"]
        with self.assertRaises(ValueError):
            controller.targets(rows, "transaction")
        rows[2]["Coordinator"] = False
        with self.assertRaises(ValueError):
            controller.targets(rows, "two-zones")
        with self.assertRaises(ValueError):
            controller.targets(rows, "unproved-packet-reorder")

    def test_unavailable_is_actual_native_error_not_timeout_or_abort(self):
        row = {"Classification": "native-error", "NativeCode": 1031, "Error": "native timeout",
               "Outcome": {"Commit": False}}
        controller.unavailable(row)
        for bad in ({**row, "NativeCode": None}, {**row, "Classification": "caller-error"},
                    {**row, "Outcome": {"Commit": True}}, {**row, "Classification": "aborted"}):
            with self.assertRaises(ValueError):
                controller.unavailable(bad)


if __name__ == "__main__":
    unittest.main()
