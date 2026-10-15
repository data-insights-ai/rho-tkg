#!/usr/bin/env python3
"""Authored finite recipe/minimizer guards; qualification uses fresh real trials."""
import unittest
from unittest.mock import patch
import native_transition_replay as replay


class RecordedReplayGuards(unittest.TestCase):
    def test_replay_environment_clears_actual_sibling_profiles_and_retains_original_recipe(self):
        inherited={name:"1" for name in replay.FIXTURE_FLAGS}
        inherited.update(RHO_GRAPH_PROTOCOL_STATE_RECOVERY="4",KEEP_NON_FIXTURE="opaque-test-value")
        composed=dict(Version=1,Seed=7,First="A",Phase="registered",FenceFirst=True,Actions=[])
        for profile,value in (("cross",replay.seeded_recipe(7)),("composed",composed)):
            env=replay.trial_environment("owned/tmp","owned/engine",value,"12"*16,12345,profile,inherited=inherited)
            self.assertTrue(all(env[name]=="" for name in replay.FIXTURE_FLAGS))
            self.assertEqual(env["KEEP_NON_FIXTURE"],"opaque-test-value")
            self.assertEqual(env["RHO_GRAPH_TRANSITION_RUN"],"12"*16)
            self.assertEqual(env["RHO_GRAPH_TRANSITION_DEADLINE"],"12345")
            chosen="RHO_GRAPH_COMPOSED_RECIPE" if profile=="composed" else "RHO_GRAPH_TRANSITION_RECIPE"
            other="RHO_GRAPH_TRANSITION_RECIPE" if profile=="composed" else "RHO_GRAPH_COMPOSED_RECIPE"
            self.assertEqual(env[chosen],replay.canonical(value));self.assertEqual(env[other],"")

    def test_late_terminal_finalizer_cannot_preserve_semantic_success_or_refill_budget(self):
        receipt={"classification":"semantic-failure","semanticFailure":{"code":"serial-cut"},"proof":{"captured":True}}
        with patch.object(replay,"footprint",return_value={"files":8,"bytes":4096,"allocatedFileBytes":8192}),patch.object(replay.time,"time_ns",return_value=100):
            replay.campaign_trial_finalizer(receipt,"owned/trial",100)
        self.assertEqual(receipt["capturedClassification"],"semantic-failure")
        self.assertEqual(receipt["classification"],"driver-or-resource-refused")
        self.assertIsNone(receipt["semanticFailure"]);self.assertIsNone(receipt["proof"])
        self.assertIsNone(replay.semantic_code(receipt))
        self.assertIsNone(replay.semantic_code({"classification":"source-refused","semanticFailure":{"code":"serial-cut"}}))

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
