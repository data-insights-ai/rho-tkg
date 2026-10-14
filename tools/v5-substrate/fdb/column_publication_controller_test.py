#!/usr/bin/env python3
"""Authored decoder guards only; none establishes a real native fault."""
import hashlib
import unittest
import column_publication_controller as controller


class PublicationBindingGuards(unittest.TestCase):
    def test_native_invocation_marker_never_substitutes_for_stored_manifest(self):
        state = {"Format": controller.FORMAT, "Case": "manifest-issued", "Coordinator": 1,
                 "Final": {"Graph": "g"}, "OldManifest": {"Generation": {"Binding": [1] * 32}}}
        marker = {"Ready": "native-manifest-commit-issued", "Format": controller.FORMAT,
                  "Case": "manifest-issued", "Coordinator": 1,
                  "Generation": {"Version": 1, "ID": "final", "Binding": [1] * 32,
                                 "Initial": None, "Cut": state["Final"]},
                  "StateSHA256": list(hashlib.sha256(b"actual retained state").digest())}
        state["Initial"] = None
        controller.marker(marker, state, b"actual retained state")
        with self.assertRaises(ValueError):
            controller.marker(marker, state, b"substituted state")
        with self.assertRaises(ValueError):
            controller.marker({**marker, "Ready": "commit_unknown_result"}, state, b"actual retained state")
        expected = {(0, b"authentic-a"): b"5", (1, b"authentic-b"): b"6"}
        actual = {}
        controller.accept_source_row(actual, expected, (0, b"authentic-a"), b"5")
        with self.assertRaises(ValueError):
            controller.accept_source_row(actual, expected, (1, b"authentic-b"), b"5")
        with self.assertRaises(ValueError):
            controller.accept_source_row(actual, expected, (0, b"authentic-a"), b"5")
        self.assertEqual(actual, {(0, b"authentic-a"): b"5"})
        # A truncated/foreign cache must never acquire a chunks-EOF classification.
        with self.assertRaises(ValueError):
            controller.chunk(b"RHOCLM01", {}, 0, bytes(32), {}, {})


if __name__ == "__main__":
    unittest.main()
