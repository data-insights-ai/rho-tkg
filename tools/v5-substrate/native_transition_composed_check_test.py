#!/usr/bin/env python3
"""Compact closed-grammar refusals; no fabricated native qualification."""
import copy
import unittest
import native_transition_composed_check as composed


def recipe():
    return dict(Version=1, Seed=7, First="A", Phase="registered", FenceFirst=True,
                Actions=[dict(At="held", Kind="tick", Original="", Member=3, Ticks=1),
                         dict(At="transferred", Kind="duplicate", Original="A", Member=0, Ticks=0)])


class ComposedSourceGuards(unittest.TestCase):
    def test_six_causal_orders_cannot_be_replaced_by_an_inert_recipe(self):
        accepted = []
        for first in ("A", "B"):
            for phase in ("registered", "prepared"):
                for fence in (False, True):
                    value = dict(Version=1, Seed=1, First=first, Phase=phase, FenceFirst=fence, Actions=[])
                    if first == "B" and not fence:
                        with self.assertRaises(ValueError): composed.recipe(value)
                    else:
                        composed.recipe(value); accepted.append((first, phase, fence))
        self.assertEqual(len(accepted), 6)
        for changes in ({"Seed": True}, {"First": "current"}, {"FenceFirst": 1}, {"Phase": "decided"}, {"Proposal": "inert"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                composed.recipe({**recipe(), **changes})

    def test_optional_actions_cannot_rewrite_order_import_authority_or_exceed_grant(self):
        composed.recipe(recipe())
        for field, value in (("At", "held"), ("Original", "current"), ("Ticks", 1), ("Member", 1)):
            bad = copy.deepcopy(recipe()); bad["Actions"][1][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError): composed.recipe(bad)
        bad = recipe(); bad["Actions"].reverse()
        with self.assertRaises(ValueError): composed.recipe(bad)
        bad = recipe(); bad["Actions"] = [dict(At="held", Kind="tick", Original="", Member=1, Ticks=3)] * 6
        with self.assertRaises(ValueError): composed.recipe(bad)
        for data in ({}, dict(Version=2, Profile=composed.PROFILE), dict.fromkeys(composed.HEADER)):
            with self.assertRaises(ValueError): composed.check(data)


if __name__ == "__main__":
    unittest.main()
