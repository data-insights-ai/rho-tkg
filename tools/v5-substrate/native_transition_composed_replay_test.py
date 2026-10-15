#!/usr/bin/env python3
"""Pure recorded-controller refusal guards, never database counterexamples."""
import unittest
import native_transition_composed_replay as replay


class ComposedReplayGuards(unittest.TestCase):
    def test_seed_records_complete_causal_selector_and_bounded_actual_actions(self):
        value = replay.seeded_recipe(20261015)
        self.assertEqual(value, replay.seeded_recipe(20261015))
        replay.C.recipe(value)
        self.assertEqual(len(value["Actions"]), 4)
        self.assertEqual(sum(a["Kind"] == "duplicate" for a in value["Actions"]), 2)
        for seed in (0, True, 1 << 64):
            with self.assertRaises(ValueError): replay.seeded_recipe(seed)

    def test_monitor_timeout_or_leak_is_not_an_explicit_failed_verdict(self):
        meter = dict(reason="command-failed", exit_code=1)
        self.assertEqual(replay.R.terminal_result(meter, 124, "composed"), (False, True))
        self.assertEqual(replay.R.terminal_result(meter, 124, "cross"), (False, False))
        for reason in ("deadline", "rss-limit", "descendants-after-parent", "cleanup-refused", "monitor-refused"):
            with self.subTest(reason=reason):
                self.assertEqual(replay.R.terminal_result(dict(meter, reason=reason), 124, "composed"), (False, False))
        self.assertEqual(replay.R.terminal_result(dict(reason="completed", exit_code=0), 0, "composed"), (True, False))

    def test_deletion_preserves_full_causal_body_and_requires_same_independent_failure(self):
        recipe = replay.seeded_recipe(19)
        used = []
        def invoke(value, number):
            replay.C.recipe(value)
            self.assertEqual({k: v for k, v in value.items() if k != "Actions"}, {k: v for k, v in recipe.items() if k != "Actions"})
            used.append(number)
            return dict(classification="source-refused", semanticFailure=dict(code="old-cut:composed-transfer"))
        result = replay.R.minimize(recipe, invoke, "old-cut:composed-transfer", validator=replay.C.recipe)
        self.assertEqual(result["recipe"], recipe)
        self.assertEqual(result["acceptedTrials"], [])
        self.assertFalse(result["deletionOneMinimal"])
        self.assertEqual(used, [2, 3, 4, 5])
        limited = replay.R.minimize(recipe, invoke, "old-cut:composed-transfer", validator=replay.C.recipe, maximum=2)
        self.assertFalse(limited["deletionOneMinimal"])


if __name__ == "__main__":
    unittest.main()
