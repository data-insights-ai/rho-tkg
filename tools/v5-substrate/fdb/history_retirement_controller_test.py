#!/usr/bin/env python3
"""Compact source guards; these do not establish real process-loss evidence."""
import base64
import hashlib
import unittest
import history_retirement_controller as controller


class SourceHistoryProcessGuards(unittest.TestCase):
    def test_exact_owner_and_partial_native_state_binding(self):
        raw = b"genuine state before actual child kill"
        state = {"Coordinator": 1}
        marker = {"Ready": "source-owner-live-after-expiration", "Format": controller.FORMAT,
                  "Coordinator": 1, "StateSHA256": list(hashlib.sha256(raw).digest())}
        controller.marker(marker, state, raw, "owner")
        with self.assertRaises(ValueError):
            controller.marker(marker, state, raw + b"substitution", "owner")
        with self.assertRaises(ValueError):
            controller.marker({**marker, "Ready": "timeout-implies-release"}, state, raw, "owner")
        row = lambda key, value: {"Group": 0, "Key": base64.b64encode(key).decode(), "Value": base64.b64encode(value).decode()}
        expected = {(0, b"actual-anchor"): b"6"}
        self.assertEqual(controller.physical([row(b"actual-anchor", b"6")], expected), expected)
        for bad in ([row(b"actual-anchor", b"5")], [row(b"actual-anchor", b"6")] * 2,
                    [row(b"other-authentic-old", b"5")]):
            with self.assertRaises(ValueError):
                controller.physical(bad, expected)
        with self.assertRaises(ValueError):
            controller.partial({"Result": {"Done": True, "Phase": 2, "Deleted": [1, 0]}, "After": {"State": {"Phase": 2, "Cursor": "YQ=="}}})
        with self.assertRaises(ValueError):
            controller.partial({"Result": {"Done": False, "Phase": 1, "Deleted": [0, 0]}, "After": {"State": {"Phase": 1, "Cursor": None}}})


if __name__ == "__main__":
    unittest.main()
