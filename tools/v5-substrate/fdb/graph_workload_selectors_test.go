package main

import (
	"errors"
	"testing"
)

// Authored only. The direct SDK API parent is in graph_snapshot_test.go; these
// finite dispatcher guards confer no concurrent SDK or matrix qualification.
func TestGraphSelectorsFreshFamilyAdmissionAndPolicy(t *testing.T) {
	spec := graphSelectorsSpec{1, graphSelectorsFamily, 40, 50, 32, "columns"}
	c := graphSelectorsConfig(spec, "01020304050607080910111213141516")
	if !validGraphCostProfile(c) || c.Limits.Transactions != 3+2*spec.Facts*64 || c.Limits.Versions != 3+6*spec.Facts {
		t.Fatal("fresh actual writer/attempt/headroom policy")
	}
	policy := graphSelectorsPolicy(c)
	if policy.Read.validate() != nil || policy.History.validate() != nil || policy.Window != 32 || policy.Attempts != 64 || policy.Callbacks != 16 || policy.Submissions != 8 {
		t.Fatal("finite policy mismatch")
	}
	for _, writers := range []int{1, 8, 32} {
		spec.Writers = writers
		fresh := graphSelectorsConfig(spec, "01020304050607080910111213141516")
		if !validGraphSelectorsConfig(fresh) || graphSelectorsPolicy(fresh).Dataset != policy.Dataset {
			t.Fatal("writer count changed dataset identity")
		}
	}
	for _, bad := range []graphSelectorsSpec{{1, graphSelectorsFamily, 20, 50, 32, "kv"}, {1, graphSelectorsFamily, 40, 50, 2, "kv"}, {1, graphSelectorsFamily, 40, 50, 8, "foreign"}} {
		if validGraphSelectorsConfig(graphSelectorsConfig(bad, "01020304050607080910111213141516")) {
			t.Fatal("unimplemented profile admitted", bad)
		}
	}
	if validGraphSelectorsConfig(graphSelectorsConfig(spec, "00000000000000000000000000000000")) {
		t.Fatal("missing run identity")
	}
	legacy := graphCostConfig("graph-cost-v1", graphCostProfile{1, 40, 50, 1})
	if validGraphSelectorsConfig(legacy) || legacy.Limits.Transactions != 83 {
		t.Fatal("legacy profile widened")
	}
	if err := (graphSnapshotRequest{Mode: "Stable", Scope: []uint8{0, 1}, At: certificate{Round: 1}}).validate(); !errors.Is(err, errInvalid) {
		t.Fatal("caller At disguised as Stable", err)
	}
}
