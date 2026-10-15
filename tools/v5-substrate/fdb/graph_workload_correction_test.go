package main

import "testing"

// Authored only; no matrix/SDK/timing execution or qualification is claimed.
func TestGraphCorrectionActualHalfCohortAdmission(t *testing.T) {
	for _, writers := range []int{1, 8, 32} {
		spec := graphSelectorsSpec{1, graphCorrectionFamily, 80, 50, writers, "columns"}
		c := graphCorrectionConfig(spec, "01020304050607080910111213141516")
		if !validGraphCostProfile(c) || c.Cost.Version != 7 || c.Limits.Transactions != 3+120*64 || c.Limits.Versions != 363 || validGraphColumnsConfig(c) {
			t.Fatal("actual correction counts/namespace", c)
		}
		if graphCorrectionValue(c.Cost, 0, "old") != 5 || graphCorrectionValue(c.Cost, 39, "final") != 6 || graphCorrectionValue(c.Cost, 40, "final") != 5 || graphCorrectionValue(c.Cost, 79, "final") != 5 {
			t.Fatal("unchanged cohort disguised as useful correction")
		}
		if (&graphSelectorsArtifacts{end: graphSelectorsEnd{Spec: spec}}).filename("indexes.jsonl") != "fdb-correction-indexes.jsonl" {
			t.Fatal("fresh artifact namespace")
		}
	}
	bad := graphSelectorsSpec{1, graphCorrectionFamily, 40, 50, 32, "kv"}
	if validGraphCorrectionConfig(graphCorrectionConfig(bad, "01020304050607080910111213141516")) {
		t.Fatal("not enough real correction jobs for32 callers")
	}
	if len(graphCorrectionPredicates()) != 6 {
		t.Fatal("fixed bounded out/in predicate union")
	}
	legacy := graphColumnsConfig(graphSelectorsSpec{1, graphColumnsFamily, 80, 50, 32, "columns"}, "01020304050607080910111213141516")
	if legacy.Limits.Transactions != 3+160*64 || !validGraphColumnsConfig(legacy) {
		t.Fatal("prior family defaults changed")
	}
}
