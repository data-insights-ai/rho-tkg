#!/usr/bin/env python3
"""Compact authored recipe/codec refusals, not generated native authority."""
import unittest
import native_transition_cut_check as cut


class CutTransitionGuards(unittest.TestCase):
    def test_actual_cut_phase_side_inventory_is_finite(self):
        accepted = []
        for phase in (1, 2):
            for partition in (3, 8):
                for side in ("before-submit", "after-applied"):
                    value = dict(Version=1, Phase=phase, Partition=partition, Side=side)
                    accepted.append(cut.recipe(value))
        self.assertEqual(len(set(accepted)), 8)
        valid = dict(Version=1, Phase=1, Partition=3, Side="before-submit")
        for update in ({"Version": True}, {"Phase": 3}, {"Partition": 4}, {"Side": "after-submit"}, {"Page": 0}):
            with self.subTest(update=update), self.assertRaises(ValueError):
                cut.recipe({**valid, **update})

    def test_diagnostic_or_partial_cut_cannot_supply_returned_certificate(self):
        for data in ({}, dict(Version=3, Profile=cut.PROFILE), dict.fromkeys(cut.HEADER)):
            with self.assertRaises(ValueError):
                cut.check(data)
        with self.assertRaises(ValueError):
            cut.P.body(b"GSQ\1" + bytes(173), b"GSQ\1")
        with self.assertRaises(ValueError):
            cut.T.decoded('{"Phase":1,"Phase":2}')


if __name__ == "__main__":
    unittest.main()
