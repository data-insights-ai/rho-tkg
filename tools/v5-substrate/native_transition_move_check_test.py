#!/usr/bin/env python3
"""Compact authored transfer recipe/codec refusals; no generated source proof."""
import unittest
import native_transition_move_check as move


class MoveTransitionGuards(unittest.TestCase):
    def test_actual_phase_side_and_page_inventory_is_finite(self):
        accepted = []
        for phase in range(1, 10):
            for partition in (3, 8):
                for page in (0, 1, 3):
                    for side in ("before-submit", "after-applied"):
                        value = dict(Version=1, Phase=phase, Partition=partition, Page=page, Side=side)
                        try:
                            move.recipe(value)
                        except ValueError:
                            continue
                        accepted.append((phase, partition, page, side))
        self.assertEqual(len(accepted), 34)
        self.assertIn((1, 3, 0, "before-submit"), accepted)
        self.assertIn((7, 8, 0, "after-applied"), accepted)
        for value in (dict(Version=1, Phase=6, Partition=8, Page=0, Side="after-applied"),
                      dict(Version=1, Phase=4, Partition=8, Page=2, Side="before-submit"),
                      dict(Version=1, Phase=1, Partition=3, Page=0, Side="after-submit")):
            with self.assertRaises(ValueError):
                move.recipe(value)

    def test_diagnostic_or_partial_prefix_cannot_become_native_move_authority(self):
        for data in ({}, dict(Version=1, Profile=move.PROFILE, Recipe={}),
                     dict.fromkeys(move.HEADER)):
            with self.assertRaises(ValueError):
                move.check(data)
        with self.assertRaises(ValueError):
            move.F.prefix(dict(From=[1, 1], Through=[3, 1], Records=[dict(Partition=3, Index=3, Outcome=None, CDC=None)]))
        for read in (dict(Cut={}, Details=[], Census=[]), dict(Cut={}, Details=[], Census=[None]*11)):
            with self.assertRaises(ValueError):
                move.census(read)


if __name__ == "__main__":
    unittest.main()
