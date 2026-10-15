#!/usr/bin/env python3
"""Authored finite recipe/minimizer guards; qualification uses fresh real trials."""
import unittest
import native_transition_replay as replay


class RecordedReplayGuards(unittest.TestCase):
    def test_fixed_seed_recipe_and_finite_native_action_domain(self):
        value = replay.seeded_recipe(20261015)
        self.assertEqual(value, replay.seeded_recipe(20261015))
        replay.C.recipe(value)
        self.assertEqual(len(value["Actions"]), 4)
        self.assertLessEqual(sum(row["Ticks"] for row in value["Actions"]), 16)
        for seed in (0, True, 1 << 64):
            with self.assertRaises(ValueError):
                replay.seeded_recipe(seed)

    def test_only_same_independent_semantic_failure_allows_deletion(self):
        recipe = {**replay.seeded_recipe(37), "Actions": [{"Member": 1, "Ticks": 1}, {"Member": 2, "Ticks": 1}]}
        calls = []
        def invoke(candidate, number):
            calls.append((candidate, number))
            # These are minimizer refusal guards, never database evidence.
            return {"classification": "driver-or-resource-refused", "semanticFailure": {"code": "serial-cut"}}
        result = replay.minimize(recipe, invoke, "serial-cut")
        self.assertEqual(result["recipe"], recipe)
        self.assertEqual(result["acceptedTrials"], [])
        self.assertFalse(result["deletionOneMinimal"])
        self.assertEqual(result["unresolvedTrials"], [2, 3])
        self.assertEqual([number for _, number in calls], [2, 3])
        result = replay.minimize(recipe, lambda _value, _number: {"classification": "semantic-failure", "semanticFailure": {"code": "different"}}, "serial-cut")
        self.assertEqual(result["recipe"], recipe)
        self.assertTrue(result["deletionOneMinimal"])
        result = replay.minimize(recipe, lambda _value, _number: {"classification": "semantic-failure", "semanticFailure": {"code": "serial-cut"}}, "serial-cut")
        self.assertEqual(result["recipe"]["Actions"], [])
        self.assertTrue(result["deletionOneMinimal"])
        limited = replay.minimize(recipe, invoke, "serial-cut", maximum=2)
        self.assertFalse(limited["deletionOneMinimal"])


if __name__ == "__main__":
    unittest.main()
