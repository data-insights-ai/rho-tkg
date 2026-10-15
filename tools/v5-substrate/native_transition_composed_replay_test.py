#!/usr/bin/env python3
"""Pure recorded-controller refusal guards, never database counterexamples."""
import unittest
import native_transition_composed_replay as replay


class ComposedReplayGuards(unittest.TestCase):
    def test_campaign_admits_every_actual_causal_order_before_distinct_recorded_seeds(self):
        plan=replay.campaign([20261015,20261016])
        self.assertEqual(len(plan["Enumeration"]),6)
        names={f'explore-{r["First"]}-{r["Phase"]}-fence-{str(r["FenceFirst"]).lower()}' for r in plan["Enumeration"]}
        self.assertEqual(names,replay.C.F.EXPLORATIONS)
        self.assertTrue(all(r["Actions"]==[] for r in plan["Enumeration"]))
        self.assertEqual(plan["Recorded"],[replay.seeded_recipe(20261015),replay.seeded_recipe(20261016)])
        for seeds in ([],[1,1],[1,2,3],[True],[0]):
            with self.assertRaises(ValueError):replay.campaign(seeds)
        missing={**plan,"Enumeration":plan["Enumeration"][:-1]}
        with self.assertRaises(ValueError):replay.R.campaign_admission(missing,replay.C)
        receipts=[];seen=[]
        def invoke(value,number):
            self.assertEqual(number,len(receipts));replay.C.recipe(value);seen.append(value)
            row={"classification":"passed"};receipts.append(row);return row
        result={"schedule":[],"recordedSeedsAttempted":0,"recordedSeedsPassed":0}
        replay.R.campaign_sequence(result,plan,invoke,receipts,replay.C)
        self.assertEqual(seen[:6],plan["Enumeration"])
        self.assertEqual(seen[6:],[plan["Recorded"][0]]*2+[plan["Recorded"][1]]*2)
        self.assertEqual(len(receipts),10);self.assertEqual(result["classification"],"passed")

    def test_campaign_deletion_spends_same_trial_pool_after_fresh_exact_failure_replay(self):
        plan=replay.campaign([19]);receipts=[];seen=[]
        def invoke(value,number):
            self.assertEqual(number,len(receipts));self.assertLess(number,replay.R.MAX_TRIALS)
            replay.C.recipe(value);seen.append((value,number))
            if number<6:row={"classification":"passed"}
            elif number<8:row={"classification":"semantic-failure","semanticFailure":{"code":"old-cut:composed-transfer"}}
            else:
                self.assertEqual({k:v for k,v in value.items() if k!="Actions"},{k:v for k,v in plan["Recorded"][0].items() if k!="Actions"})
                row={"classification":"source-refused","semanticFailure":{"code":"old-cut:composed-transfer"}}
            receipts.append(row);return row
        result={"schedule":[],"recordedSeedsAttempted":0,"recordedSeedsPassed":0,"reproduced":False,"minimization":None}
        replay.R.campaign_sequence(result,plan,invoke,receipts,replay.C)
        self.assertTrue(result["reproduced"])
        self.assertEqual([number for _,number in seen],list(range(12)))
        self.assertEqual(result["minimization"]["used"],12)
        self.assertEqual(result["minimization"]["acceptedTrials"],[])
        self.assertFalse(result["minimization"]["deletionOneMinimal"])

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
